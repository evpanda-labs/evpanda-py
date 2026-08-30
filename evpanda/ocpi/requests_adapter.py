"""Adapter — OCPI outbound (host → partner), requests.

A drop-in ``HTTPAdapter`` that captures every OCPI call a session makes.
The only change it makes to a request is stripping the SDK's own
``X-EVPanda-*`` identity headers before dispatch, so a partner never
receives them; the response handed back is the one requests built.

Requires ``requests`` (``pip install evpanda[requests]``).
"""

from __future__ import annotations

from typing import Any

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError as exc:  # pragma: no cover - exercised by the bare install
    raise ImportError(
        "evpanda.ocpi.requests_adapter needs `requests` — pip install 'evpanda[requests]'"
    ) from exc

from .._types import RoamingIdentity
from ._adapter import (
    IDENTITY_HEADERS,
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


class RequestsAdapter(HTTPAdapter):
    """A ``requests`` transport adapter that captures the OCPI calls it makes.

    ::

        panda = evpanda.start_ocpi()
        session = requests.Session()
        instrument_session(session, panda)

        with ocpi.use_identity(partner_identity):
            response = session.post(f"{partner.url}/sessions", json=payload)

    Identity comes from :func:`~evpanda.ocpi.default_resolver` unless
    ``resolver`` overrides it: the :func:`~evpanda.ocpi.use_identity`
    ContextVar first, then the ``X-EVPanda-*`` headers — requests has no
    per-request extension mapping, so per-call attribution goes through the
    headers::

        session.get(url, headers={"X-EVPanda-Platform-Id": partner.id,
                                  "X-EVPanda-Platform-Name": partner.name})

    A streamed call (``stream=True``) is captured without its response
    body: reading it here would consume the stream the caller asked to
    handle itself.

    Any keyword arguments are passed to ``HTTPAdapter``, so pool sizing and
    retries work as they always did.
    """

    def __init__(
        self,
        client: Capturer,
        *,
        resolver: Resolver | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._client = client
        self._resolver = resolver

    def send(  # type: ignore[override]
        self, request: requests.PreparedRequest, stream: bool = False, **kwargs: Any
    ) -> requests.Response:
        # Re-checked per call: close drops the worker, and a closed client
        # reverts to an untouched adapter rather than capturing into a
        # no-op. The identity headers are still stripped, so a client that
        # closes mid-flight does not suddenly start leaking them.
        max_bytes = capturing(self._client)
        headers = header_map(request.headers.items())
        _strip_identity_headers(request)

        identity = (
            None if max_bytes is None else resolve(self._resolver, _request_info(request, headers))
        )
        if identity is None or max_bytes is None:
            return super().send(request, stream=stream, **kwargs)

        exchange = Exchange(
            method=request.method or "",
            url=request.url or "",
            request_headers=header_map(request.headers.items()),
            request_body=CappedBody(max_bytes),
            response_body=CappedBody(max_bytes),
        )
        guard(lambda: exchange.request_body.push(_body_bytes(request.body)))

        response = super().send(request, stream=stream, **kwargs)
        guard(lambda: self._capture(identity, exchange, response, stream))
        return response

    def _capture(
        self,
        identity: RoamingIdentity,
        exchange: Exchange,
        response: requests.Response,
        stream: bool,
    ) -> None:
        """Record the response and ship the exchange.

        Reading ``response.content`` here caches it on the response, so the
        caller still gets the body it was going to get.
        """
        exchange.status_code = response.status_code
        exchange.response_headers = header_map(response.headers.items())
        if not stream:
            exchange.response_body.push(response.content)
        ship(self._client, identity, exchange, inbound=False)


def instrument_session(
    session: requests.Session, client: Capturer, *, resolver: Resolver | None = None
) -> requests.Session:
    """Mount a :class:`RequestsAdapter` on a session for http and https.

    Returns the same session, so it composes::

        session = instrument_session(requests.Session(), panda)
    """
    adapter = RequestsAdapter(client, resolver=resolver)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _strip_identity_headers(request: requests.PreparedRequest) -> None:
    """Remove the SDK's identity headers so the partner never sees them."""
    for name in IDENTITY_HEADERS:
        request.headers.pop(name, None)


def _request_info(request: requests.PreparedRequest, headers: dict[str, str]) -> RequestInfo:
    """Everything a resolver might want about the call about to be made.

    The headers are the ones the caller set, captured before the identity
    headers were stripped.
    """
    return RequestInfo(
        method=request.method or "",
        url=request.url or "",
        headers=headers,
        context=request,
    )


def _body_bytes(body: Any) -> bytes:
    """The request body as bytes, or empty for a streamed or file-like one.

    A generator or file body is left alone: consuming it here would take it
    away from the request that is about to send it.
    """
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    return b""


__all__ = ["RequestsAdapter", "instrument_session"]
