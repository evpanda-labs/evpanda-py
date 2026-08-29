"""Adapter — OCPI outbound (host → partner), httpx.

Wraps any httpx transport and returns one, so it drops onto a client's
``transport=`` and captures every call made through it — including calls
made by a generated OCPI client, as long as it accepts an ``httpx.Client``.

The only change it makes to a request is stripping the SDK's own
``X-EVPanda-*`` identity headers before dispatch, so a partner never
receives them. The response is never altered: bodies are recorded as the
caller reads them, not by buffering them up front.

Requires ``httpx`` (``pip install evpanda[httpx]``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import httpx

from .._types import RoamingIdentity
from ._adapter import (
    IDENTITY_HEADERS,
    IDENTITY_KEY,
    CappedBody,
    Capturer,
    Exchange,
    RequestInfo,
    Resolver,
    capturing,
    guard,
    header_map,
    resolve,
    ship,
)


class HTTPXTransport(httpx.BaseTransport):
    """A synchronous httpx transport that captures the OCPI calls it makes.

    ::

        panda = evpanda.start_ocpi()
        client = httpx.Client(transport=HTTPXTransport(panda))

        with ocpi.use_identity(partner_identity):
            response = client.post(f"{partner.url}/sessions", json=payload)

    Identity comes from :func:`~evpanda.ocpi.default_resolver` unless
    ``resolver`` overrides it: the request's ``extensions`` first (under
    the ``evpanda.identity`` key), then the
    :func:`~evpanda.ocpi.use_identity` ContextVar, then the ``X-EVPanda-*``
    headers. Per-request, without the ContextVar::

        client.get(url, extensions={"evpanda.identity": partner_identity})

    Capture completes when the response body is exhausted or closed —
    which httpx does for you on a non-streaming call. A transport error
    propagates unchanged and captures nothing: there is no exchange to
    record.
    """

    def __init__(
        self,
        client: Capturer,
        base: httpx.BaseTransport | None = None,
        *,
        resolver: Resolver | None = None,
    ) -> None:
        self._client = client
        self._base = base if base is not None else httpx.HTTPTransport()
        self._resolver = resolver

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # Re-checked per call: close drops the worker, and a closed client
        # reverts to an untouched transport rather than capturing into a
        # no-op. The identity headers are still stripped, so a client that
        # closes mid-flight does not suddenly start leaking them.
        max_bytes = capturing(self._client)
        # Resolve against the headers the caller set, then strip them: the
        # resolver still sees the identity the partner never will.
        info = _request_info(request, _identity_extension(request))
        _strip_identity_headers(request)

        identity = None if max_bytes is None else resolve(self._resolver, info)
        if identity is None or max_bytes is None:
            return self._base.handle_request(request)

        exchange = _begin(request, max_bytes)
        if not _record_loaded_body(exchange, request):
            request.stream = _TeeStream(request.stream, exchange.request_body)

        response = self._base.handle_request(request)
        _record_response_head(exchange, response)

        finish = _finisher(self._client, identity, exchange)
        if _record_loaded_body(exchange, response, request=False):
            finish()
        else:
            response.stream = _TeeStream(response.stream, exchange.response_body, finish)
        return response

    def close(self) -> None:
        self._base.close()


class AsyncHTTPXTransport(httpx.AsyncBaseTransport):
    """The asynchronous twin of :class:`HTTPXTransport`.

    ::

        client = httpx.AsyncClient(transport=AsyncHTTPXTransport(panda))
    """

    def __init__(
        self,
        client: Capturer,
        base: httpx.AsyncBaseTransport | None = None,
        *,
        resolver: Resolver | None = None,
    ) -> None:
        self._client = client
        self._base = base if base is not None else httpx.AsyncHTTPTransport()
        self._resolver = resolver

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        max_bytes = capturing(self._client)
        info = _request_info(request, _identity_extension(request))
        _strip_identity_headers(request)

        identity = None if max_bytes is None else resolve(self._resolver, info)
        if identity is None or max_bytes is None:
            return await self._base.handle_async_request(request)

        exchange = _begin(request, max_bytes)
        if not _record_loaded_body(exchange, request):
            request.stream = _AsyncTeeStream(request.stream, exchange.request_body)

        response = await self._base.handle_async_request(request)
        _record_response_head(exchange, response)

        finish = _finisher(self._client, identity, exchange)
        if _record_loaded_body(exchange, response, request=False):
            finish()
        else:
            response.stream = _AsyncTeeStream(response.stream, exchange.response_body, finish)
        return response

    async def aclose(self) -> None:
        await self._base.aclose()


# ── Shared plumbing ──────────────────────────────────────────────────────


def _identity_extension(request: httpx.Request) -> RoamingIdentity | None:
    """The identity carried on the request's ``extensions``, if any."""
    value = request.extensions.get(IDENTITY_KEY)
    return value if isinstance(value, RoamingIdentity) else None


def _strip_identity_headers(request: httpx.Request) -> None:
    """Remove the SDK's identity headers so the partner never sees them."""
    for name in IDENTITY_HEADERS:
        if name in request.headers:
            del request.headers[name]


def _request_info(request: httpx.Request, identity: RoamingIdentity | None) -> RequestInfo:
    """Everything a resolver might want about the call about to be made.

    Headers are read after the identity ones are stripped, which is why the
    hint is passed separately.
    """
    return RequestInfo(
        method=request.method,
        url=str(request.url),
        headers=header_map(request.headers.items()),
        identity=identity,
        context=request,
    )


def _begin(request: httpx.Request, max_bytes: int) -> Exchange:
    """Snapshot the request before dispatch.

    The transport underneath is free to add headers of its own, and those
    are not part of what we set out to send.
    """
    return Exchange(
        method=request.method,
        url=str(request.url),
        request_headers=header_map(request.headers.items()),
        request_body=CappedBody(max_bytes),
        response_body=CappedBody(max_bytes),
    )


def _record_loaded_body(
    exchange: Exchange,
    message: httpx.Request | httpx.Response,
    request: bool = True,
) -> bool:
    """Record a body httpx has already materialized, and say whether it did.

    A request built from ``content=`` or ``json=``, and a response a
    transport returns whole (a mock, a cache), both arrive with their bytes
    in hand and their stream spent. Reading the attribute is both cheaper
    and safer than teeing a stream that will never be iterated again.
    """
    if not hasattr(message, "_content"):
        return False
    body = exchange.request_body if request else exchange.response_body
    body.push(message.content)
    return True


def _record_response_head(exchange: Exchange, response: httpx.Response) -> None:
    exchange.status_code = response.status_code
    exchange.response_headers = header_map(response.headers.items())


def _finisher(
    client: Capturer, identity: RoamingIdentity, exchange: Exchange
) -> Callable[[], None]:
    """The callback the response stream fires once, when it is done."""

    def finish() -> None:
        guard(lambda: ship(client, identity, exchange, inbound=False))

    return finish


class _TeeStream(httpx.SyncByteStream):
    """Records what passes through a sync stream, then fires ``on_done`` once.

    Exhaustion and close both count as done — a caller that reads a
    response to the end and forgets to close it still gets its exchange
    captured.
    """

    def __init__(
        self,
        stream: Any,
        body: CappedBody,
        on_done: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._body = body
        self._on_done = on_done
        self._done = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self._stream:
            self._body.push(chunk)
            yield chunk
        self._finish()

    def close(self) -> None:
        close = getattr(self._stream, "close", None)
        if close is not None:
            close()
        self._finish()

    def _finish(self) -> None:
        if self._done or self._on_done is None:
            return
        self._done = True
        self._on_done()


class _AsyncTeeStream(httpx.AsyncByteStream):
    """The asynchronous twin of :class:`_TeeStream`."""

    def __init__(
        self,
        stream: Any,
        body: CappedBody,
        on_done: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._body = body
        self._on_done = on_done
        self._done = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._stream:
            self._body.push(chunk)
            yield chunk
        self._finish()

    async def aclose(self) -> None:
        aclose = getattr(self._stream, "aclose", None)
        if aclose is not None:
            await aclose()
        self._finish()

    def _finish(self) -> None:
        if self._done or self._on_done is None:
            return
        self._done = True
        self._on_done()


__all__ = ["AsyncHTTPXTransport", "HTTPXTransport"]
