"""Adapter — OCPI inbound (partner → host), WSGI.

Wraps any WSGI application: Flask, Django, Bottle, Pyramid, Falcon, or a
bare callable. It resolves identity, records the request body, tees the
response as the server writes it, and ships one message when the response
is done.

A request with no resolvable identity is served exactly as it would have
been — it just is not captured. The middleware never alters a response,
never blocks a request, and never raises on the SDK's account.
"""

from __future__ import annotations

import io
from collections.abc import Callable, Iterable, Iterator
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

#: The WSGI shapes, spelled out rather than imported: ``wsgiref.types`` is
#: 3.11+, but naming them here keeps the signature readable without tying
#: the module to it.
type StartResponse = Callable[..., Any]
type WSGIEnvironment = dict[str, Any]
type WSGIApplication = Callable[[WSGIEnvironment, StartResponse], Iterable[bytes]]


class WSGIMiddleware:
    """Captures inbound OCPI exchanges served by a WSGI application.

    ::

        from evpanda.ocpi.wsgi import WSGIMiddleware

        panda = evpanda.start_ocpi()
        app.wsgi_app = WSGIMiddleware(app.wsgi_app, panda)

    Identity comes from :func:`~evpanda.ocpi.default_resolver` unless
    ``resolver`` overrides it: the environ first (see
    :func:`~evpanda.ocpi.set_identity`), then the
    :func:`~evpanda.ocpi.use_identity` ContextVar, then the ``X-EVPanda-*``
    headers. It is read when the response finishes, so an auth layer inside
    this middleware can stamp the environ and still be seen — mount order
    does not matter.

    Bodies are capped at ``max_capture_bytes``; an oversize body on either
    side drops the whole message rather than storing a truncated one, and
    is never read into memory in the first place.
    """

    __slots__ = ("_app", "_client", "_resolver")

    def __init__(
        self,
        app: WSGIApplication,
        client: Capturer,
        *,
        resolver: Resolver | None = None,
    ) -> None:
        self._app = app
        self._client = client
        self._resolver = resolver

    def __call__(self, environ: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
        # Re-checked per request: close drops the worker, and a closed
        # client must stop doing capture work rather than record into a
        # no-op.
        max_bytes = capturing(self._client)
        if max_bytes is None:
            return self._app(environ, start_response)

        exchange = Exchange(
            method=environ.get("REQUEST_METHOD", ""),
            url=request_url(environ),
            request_headers=headers_from_environ(environ),
            request_body=CappedBody(max_bytes),
            response_body=CappedBody(max_bytes),
        )

        _take_request_body(environ, exchange.request_body, max_bytes)

        def capturing_start_response(
            status: str, headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Any:
            guard(lambda: _record_response_head(exchange, status, headers))
            return start_response(status, headers, exc_info)

        try:
            result = self._app(environ, capturing_start_response)
        except BaseException:
            # An exchange the application blew up in is still worth having.
            self._ship(environ, exchange)
            raise
        return _CapturingIterable(
            result, exchange.response_body, lambda: self._ship(environ, exchange)
        )

    def _ship(self, environ: WSGIEnvironment, exchange: Exchange) -> None:
        """Resolve the partner and hand the finished exchange to the SDK."""
        guard(lambda: self._do_ship(environ, exchange))

    def _do_ship(self, environ: WSGIEnvironment, exchange: Exchange) -> None:
        identity = resolve(
            self._resolver,
            RequestInfo(
                method=exchange.method,
                url=exchange.url,
                headers=exchange.request_headers,
                identity=identity_from(environ),
                context=environ,
            ),
        )
        if identity is None:
            return
        ship(self._client, identity, exchange, inbound=True)


def _record_response_head(exchange: Exchange, status: str, headers: list[tuple[str, str]]) -> None:
    """Take the status line and headers on their way to the client."""
    exchange.status_code = parse_status(status)
    exchange.response_headers = header_map(headers)


class _CapturingIterable:
    """Tees the response body, then ships exactly once.

    A WSGI server calls ``close`` when it is done with the iterable — the
    spec requires it — but this ships on exhaustion or abandonment too, so
    a server that iterates to the end and forgets, or a client that
    disconnects mid-response, still gets its exchange.
    """

    __slots__ = ("_body", "_finish", "_finished", "_result")

    def __init__(
        self, result: Iterable[bytes], body: CappedBody, finish: Callable[[], None]
    ) -> None:
        self._result = result
        self._body = body
        self._finish = finish
        self._finished = False

    def __iter__(self) -> Iterator[bytes]:
        # The finally is what covers a server (or a test client) that walks
        # away mid-response: the generator is closed rather than exhausted,
        # and a partial exchange is still worth having.
        try:
            for chunk in self._result:
                self._body.push(chunk)
                yield chunk
        finally:
            self._done()

    def close(self) -> None:
        close = getattr(self._result, "close", None)
        if close is not None:
            close()
        self._done()

    def _done(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._finish()


def _take_request_body(environ: WSGIEnvironment, body: CappedBody, max_bytes: int) -> None:
    """Record the request body, and hand the application a copy of it.

    The Go SDK tees ``r.Body`` and records the body as the handler reads it,
    because Go's ``io.ReadCloser`` has exactly one read method. Python's file
    protocol has six — ``read``, ``read1``, ``readinto``, ``readline``,
    ``readlines`` and iteration — a framework may use any of them, and a
    wrapper that forwards one of them untouched records an empty body while
    the host works perfectly. Werkzeug uses ``readinto``; Django uses
    ``read``. Reading the body here and giving the application a
    :class:`io.BytesIO` of it removes that whole class of bug: BytesIO is
    the standard library's own implementation of the protocol, so there is
    nothing left to get wrong.

    The read is bounded before it happens. A body larger than the cap is
    never pulled into memory — it is marked oversize, which drops the
    exchange exactly as an oversize body does everywhere else — and a
    request with no ``Content-Length`` (a chunked upload, which no OCPI
    client sends) is passed through untouched and captured without a body.
    """
    stream = environ.get("wsgi.input")
    if stream is None:
        return
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        return
    if length <= 0:
        return
    if length > max_bytes:
        body.overflow()
        return
    data = stream.read(length)
    body.push(data)
    environ["wsgi.input"] = io.BytesIO(data)


def request_url(environ: WSGIEnvironment) -> str:
    """The path and query string as they arrived, e.g. ``/ocpi/2.2/cdrs?limit=10``."""
    path = f"{environ.get('SCRIPT_NAME', '')}{environ.get('PATH_INFO', '')}"
    query = environ.get("QUERY_STRING", "")
    return f"{path}?{query}" if query else path


def headers_from_environ(environ: WSGIEnvironment) -> dict[str, str]:
    """The request headers a WSGI environ carries, keys lowercased."""
    pairs: list[tuple[str, str]] = []
    for key, value in environ.items():
        if not isinstance(value, str):
            continue
        if key.startswith("HTTP_"):
            pairs.append((key[5:].replace("_", "-"), value))
        elif key in ("CONTENT_TYPE", "CONTENT_LENGTH"):
            pairs.append((key.replace("_", "-"), value))
    return header_map(pairs)


def parse_status(status: str) -> int | None:
    """The numeric part of a WSGI status line, or None if it is not one."""
    try:
        return int(status.split(" ", 1)[0])
    except (ValueError, AttributeError):
        return None


__all__ = ["WSGIMiddleware"]
