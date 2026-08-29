"""Adapter — OCPI inbound (partner → host), WSGI.

Wraps any WSGI application: Flask, Django, Bottle, Pyramid, Falcon, or a
bare callable. It resolves identity, records the request body as your
application reads it, tees the response as the server writes it, and ships
one message when the response is done.

A request with no resolvable identity is served exactly as it would have
been — it just is not captured. The middleware never alters a response,
never blocks a request, and never raises on the SDK's account.
"""

from __future__ import annotations

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

    The request body is recorded as your application reads it, so an
    application that ignores the body records none. Bodies are capped at
    ``max_capture_bytes``; an oversize body on either side drops the whole
    message rather than storing a truncated one.
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

        stream = environ.get("wsgi.input")
        if stream is not None:
            environ["wsgi.input"] = _TeeInput(stream, exchange.request_body)

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


class _TeeInput:
    """Records what the application reads from ``wsgi.input``.

    Every read path a WSGI application might take is implemented here, not
    only ``read``: Werkzeug — and so Flask — reads through ``readinto``,
    Django through ``read`` and ``readline``, and others iterate the stream.
    A wrapper that delegates one of those straight to the underlying stream
    records an empty body while the host works perfectly, which is the worst
    shape of bug this SDK can have. Anything that is not a read is delegated
    untouched.
    """

    __slots__ = ("_body", "_stream")

    def __init__(self, stream: Any, body: CappedBody) -> None:
        self._stream = stream
        self._body = body

    def read(self, size: int = -1) -> bytes:
        chunk: bytes = self._stream.read(size)
        self._body.push(chunk)
        return chunk

    def read1(self, size: int = -1) -> bytes:
        read1 = getattr(self._stream, "read1", None)
        if read1 is None:
            return self.read(size)
        chunk: bytes = read1(size)
        self._body.push(chunk)
        return chunk

    def readinto(self, buffer: Any) -> int:
        readinto = getattr(self._stream, "readinto", None)
        if readinto is None:
            chunk = self.read(len(buffer))  # teed by read
            buffer[: len(chunk)] = chunk
            return len(chunk)
        count: int = readinto(buffer)
        if count:
            self._body.push(bytes(memoryview(buffer)[:count]))
        return count

    def readinto1(self, buffer: Any) -> int:
        readinto1 = getattr(self._stream, "readinto1", None)
        if readinto1 is None:
            return self.readinto(buffer)
        count: int = readinto1(buffer)
        if count:
            self._body.push(bytes(memoryview(buffer)[:count]))
        return count

    def readline(self, size: int = -1) -> bytes:
        chunk: bytes = self._stream.readline(size)
        self._body.push(chunk)
        return chunk

    def readlines(self, hint: int = -1) -> list[bytes]:
        lines: list[bytes] = self._stream.readlines(hint)
        for line in lines:
            self._body.push(line)
        return lines

    def readall(self) -> bytes:
        return self.read()

    def __iter__(self) -> Iterator[bytes]:
        for line in self._stream:
            self._body.push(line)
            yield line

    def readable(self) -> bool:
        return True

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


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
