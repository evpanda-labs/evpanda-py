"""Adapter — OCPI inbound (partner → host), ASGI.

Wraps any ASGI application: FastAPI, Starlette, Litestar, Django ASGI, or
a bare coroutine callable. It resolves identity, records the request body
as your application receives it, tees the response as it is sent, and ships
one message when the response is done.

A request with no resolvable identity is served exactly as it would have
been — it just is not captured. Non-HTTP scopes (WebSocket, lifespan) pass
straight through untouched.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, MutableMapping
from typing import Any

from ._adapter import (
    CappedBody,
    Capturer,
    Exchange,
    RequestInfo,
    Resolver,
    capturing,
    guard,
    header_map,
    identity_from,
    resolve,
    ship,
)

#: The ASGI shapes. Spelled out rather than depending on an ASGI types
#: package, which would be a runtime dependency for four aliases.
type Scope = MutableMapping[str, Any]
type ASGIMessage = MutableMapping[str, Any]
type Receive = Callable[[], Awaitable[ASGIMessage]]
type Send = Callable[[ASGIMessage], Awaitable[None]]
type ASGIApplication = Callable[[Scope, Receive, Send], Awaitable[None]]


class ASGIMiddleware:
    """Captures inbound OCPI exchanges served by an ASGI application.

    ::

        from evpanda.ocpi.asgi import ASGIMiddleware

        panda = evpanda.start_ocpi()
        app.add_middleware(ASGIMiddleware, client=panda)   # Starlette / FastAPI
        # or, framework-agnostic:
        app = ASGIMiddleware(app, panda)

    Identity comes from :func:`~evpanda.ocpi.default_resolver` unless
    ``resolver`` overrides it: the scope first (see
    :func:`~evpanda.ocpi.set_identity`), then the
    :func:`~evpanda.ocpi.use_identity` ContextVar, then the ``X-EVPanda-*``
    headers. It is read when the response finishes, so a dependency or an
    inner middleware can stamp the scope and still be seen — mount order
    does not matter.

    Bodies are capped at ``max_capture_bytes``; an oversize body on either
    side drops the whole message rather than storing a truncated one.
    """

    __slots__ = ("_app", "_client", "_resolver")

    def __init__(
        self,
        app: ASGIApplication,
        client: Capturer,
        *,
        resolver: Resolver | None = None,
    ) -> None:
        self._app = app
        self._client = client
        self._resolver = resolver

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Re-checked per request: close drops the worker, and a closed
        # client must stop doing capture work rather than record into a
        # no-op.
        max_bytes = capturing(self._client)
        if scope.get("type") != "http" or max_bytes is None:
            await self._app(scope, receive, send)
            return

        exchange = Exchange(
            method=scope.get("method", ""),
            url=request_url(scope),
            request_headers=decode_headers(scope.get("headers", ())),
            request_body=CappedBody(max_bytes),
            response_body=CappedBody(max_bytes),
        )

        async def capturing_receive() -> ASGIMessage:
            message = await receive()
            if message.get("type") == "http.request":
                guard(lambda: exchange.request_body.push(message.get("body", b"")))
            return message

        async def capturing_send(message: ASGIMessage) -> None:
            guard(lambda: _record_response(exchange, message))
            await send(message)

        try:
            await self._app(scope, capturing_receive, capturing_send)
        finally:
            # An exchange the application blew up in is still worth having.
            guard(lambda: self._ship(scope, exchange))

    def _ship(self, scope: Scope, exchange: Exchange) -> None:
        """Resolve the partner and hand the finished exchange to the SDK."""
        identity = resolve(
            self._resolver,
            RequestInfo(
                method=exchange.method,
                url=exchange.url,
                headers=exchange.request_headers,
                identity=identity_from(scope),
                context=scope,
            ),
        )
        if identity is None:
            return
        ship(self._client, identity, exchange, inbound=True)


def _record_response(exchange: Exchange, message: ASGIMessage) -> None:
    """Take the status, headers and body chunks on their way to the client."""
    kind = message.get("type")
    if kind == "http.response.start":
        exchange.status_code = message.get("status")
        exchange.response_headers = decode_headers(message.get("headers", ()))
    elif kind == "http.response.body":
        exchange.response_body.push(message.get("body", b""))


def request_url(scope: Scope) -> str:
    """The path and query string as they arrived, e.g. ``/ocpi/2.2/cdrs?limit=10``."""
    path = f"{scope.get('root_path', '')}{scope.get('path', '')}"
    query = scope.get("query_string", b"")
    if not query:
        return path
    return f"{path}?{query.decode('latin-1')}"


def decode_headers(headers: Iterable[tuple[bytes, bytes]]) -> dict[str, str]:
    """ASGI's byte header pairs as the SDK's string mapping."""
    return header_map((key.decode("latin-1"), value.decode("latin-1")) for key, value in headers)


__all__ = ["ASGIMiddleware"]
