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

try:
    import httpx
except ImportError as exc:  # pragma: no cover - exercised by the bare install
    raise ImportError(
        "evpanda.ocpi.httpx_transport needs `httpx` — pip install 'evpanda[httpx]'"
    ) from exc

from .._types import Platform
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

    Pass ``base`` when your client configures the transport it would
    otherwise have built. httpx applies ``verify``, ``cert``, ``limits``,
    ``proxy`` and ``http2`` only to a transport it constructs itself, so
    supplying ``transport=`` drops them — silently, including the TLS
    ones::

        base = httpx.HTTPTransport(verify=ssl_context)
        client = httpx.Client(transport=HTTPXTransport(panda, base))

    Client-level settings that are not transport arguments — ``timeout``,
    ``follow_redirects``, ``headers``, ``auth`` — are unaffected.

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
        prepared = _prepare(self._client, self._resolver, request)
        if prepared is None:
            return self._base.handle_request(request)
        response = self._base.handle_request(request)
        _attach(self._client, prepared, response)
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
        prepared = _prepare(self._client, self._resolver, request)
        if prepared is None:
            return await self._base.handle_async_request(request)
        response = await self._base.handle_async_request(request)
        _attach(self._client, prepared, response)
        return response

    async def aclose(self) -> None:
        await self._base.aclose()


# ── Shared plumbing ──────────────────────────────────────────────────────
#
# The sync and async transports differ by two `await`s, so everything that
# is not an await lives here and both call it. The alternative — two
# transports each carrying its own copy — is four more places for the two
# to drift apart.


def _prepare(
    client: Capturer, resolver: Resolver | None, request: httpx.Request
) -> tuple[Platform, Exchange] | None:
    """Resolve the partner, strip the identity headers, start recording.

    Returns None when the call is not being captured — an inert or closed
    client, or no resolvable identity. The headers are stripped either way,
    so a client that closes mid-flight does not suddenly start leaking them
    to partners; the resolver still sees them, because the request is read
    before they are removed.
    """
    max_bytes = capturing(client)
    info = RequestInfo(
        method=request.method,
        url=str(request.url),
        headers=header_map(request.headers.items()),
        identity=_identity_extension(request),
        context=request,
    )
    for name in IDENTITY_HEADERS:
        if name in request.headers:
            del request.headers[name]

    if max_bytes is None:
        return None
    identity = resolve(resolver, info)
    if identity is None:
        return None

    # Snapshot the request as it will go out: the transport underneath is
    # free to add headers of its own, and those are not part of what we set
    # out to send.
    exchange = Exchange(
        method=request.method,
        url=str(request.url),
        request_headers=header_map(request.headers.items()),
        request_body=CappedBody(max_bytes),
        response_body=CappedBody(max_bytes),
    )
    if not _record_loaded_body(exchange.request_body, _content_or_none(request)):
        request.stream = _TeeStream(request.stream, exchange.request_body)
    return identity, exchange


def _attach(
    client: Capturer,
    prepared: tuple[Platform, Exchange],
    response: httpx.Response,
) -> None:
    """Record the response head, and arrange for the exchange to ship."""
    identity, exchange = prepared
    exchange.status_code = response.status_code
    exchange.response_headers = header_map(response.headers.items())

    def finish() -> None:
        guard(lambda: ship(client, identity, exchange, inbound=False))

    if _record_loaded_body(exchange.response_body, _content_or_none(response)):
        finish()
    else:
        response.stream = _TeeStream(response.stream, exchange.response_body, finish)


def _identity_extension(request: httpx.Request) -> Platform | None:
    """The identity carried on the request's ``extensions``, if any."""
    value = request.extensions.get(IDENTITY_KEY)
    return value if isinstance(value, Platform) else None


def _content_or_none(message: httpx.Request | httpx.Response) -> bytes | None:
    """The body httpx has already materialized, or None if it is still a stream.

    A request built from ``content=`` or ``json=``, and a response a
    transport returns whole (a mock, a cache), both arrive with their bytes
    in hand and their stream spent. Reading the attribute is cheaper and
    safer than teeing a stream that will never be iterated again.
    """
    return message.content if hasattr(message, "_content") else None


def _record_loaded_body(body: CappedBody, content: bytes | None) -> bool:
    """Record an already-materialized body, and say whether there was one."""
    if content is None:
        return False
    body.push(content)
    return True


class _TeeStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """Records what passes through a stream, then fires ``on_done`` once.

    One class for both protocols, the way httpx's own ``ByteStream`` is:
    the sync and async halves differ by four lines, and a request stream is
    handed to whichever transport is running.

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
