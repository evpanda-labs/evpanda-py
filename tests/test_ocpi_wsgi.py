"""The WSGI middleware."""

from __future__ import annotations

import io
from collections.abc import Iterable
from typing import Any
from wsgiref.util import setup_testing_defaults

import pytest

import evpanda
from conftest import FakeCapturer
from evpanda.ocpi import RequestInfo, set_identity, use_identity
from evpanda.ocpi.wsgi import WSGIMiddleware

PARTNER = evpanda.Platform(id="acme", name="Acme Mobility")


def environ_for(body: bytes = b"", **overrides: Any) -> dict[str, Any]:
    environ: dict[str, Any] = {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": "/ocpi/2.2/cdrs",
        "QUERY_STRING": "limit=10",
        "CONTENT_TYPE": "application/json",
        "CONTENT_LENGTH": str(len(body)),
        "HTTP_AUTHORIZATION": "Token secret",
        "wsgi.input": io.BytesIO(body),
    }
    setup_testing_defaults(environ)
    environ.update(overrides)
    return environ


def echo_app(response: bytes = b'{"status_code":1000}', status: str = "201 Created") -> Any:
    """A WSGI app that reads the request body and answers with ``response``."""

    def app(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        environ["wsgi.input"].read()
        start_response(status, [("Content-Type", "application/json")])
        return [response]

    return app


def run(app: Any, environ: dict[str, Any]) -> tuple[str, bytes]:
    """Drive a WSGI app the way a server would, and return status and body."""
    captured: dict[str, Any] = {}

    def start_response(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
        captured["status"] = status
        captured["headers"] = headers
        return lambda chunk: None

    result = app(environ, start_response)
    body = b"".join(result)
    close = getattr(result, "close", None)
    if close is not None:
        close()
    return captured["status"], body


def test_an_identified_exchange_is_captured() -> None:
    client = FakeCapturer()
    environ = environ_for(b'{"id":"cdr-1"}')
    set_identity(environ, PARTNER)

    status, body = run(WSGIMiddleware(echo_app(), client), environ)

    assert (status, body) == ("201 Created", b'{"status_code":1000}')
    identity, data = client.inbound[0]
    assert identity == PARTNER
    assert data.method == "POST"
    assert data.url == "/ocpi/2.2/cdrs?limit=10"
    assert data.status_code == 201
    assert data.request_body == b'{"id":"cdr-1"}'
    assert data.response_body == b'{"status_code":1000}'
    assert data.request_headers["content-type"] == "application/json"
    assert data.response_headers == {"content-type": "application/json"}


def test_identity_stamped_by_an_inner_layer_is_still_seen() -> None:
    """Mount order does not matter: the environ is read when the response ends."""
    client = FakeCapturer()

    def app(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        set_identity(environ, PARTNER)  # an auth layer inside the middleware
        start_response("200 OK", [])
        return [b"{}"]

    run(WSGIMiddleware(app, client), environ_for())

    assert client.inbound[0][0] == PARTNER


def test_identity_falls_back_to_the_headers() -> None:
    client = FakeCapturer()
    environ = environ_for(
        HTTP_X_EVPANDA_PLATFORM_ID="acme", HTTP_X_EVPANDA_PLATFORM_NAME="Acme Mobility"
    )

    run(WSGIMiddleware(echo_app(), client), environ)

    assert client.inbound[0][0] == PARTNER


def test_identity_falls_back_to_the_context_var() -> None:
    client = FakeCapturer()
    with use_identity(PARTNER):
        run(WSGIMiddleware(echo_app(), client), environ_for())

    assert client.inbound[0][0] == PARTNER


def test_the_environ_beats_the_headers() -> None:
    client = FakeCapturer()
    environ = environ_for(HTTP_X_EVPANDA_PLATFORM_ID="other", HTTP_X_EVPANDA_PLATFORM_NAME="O")
    set_identity(environ, PARTNER)

    run(WSGIMiddleware(echo_app(), client), environ)

    assert client.inbound[0][0] == PARTNER


def test_an_unidentified_request_is_served_but_not_captured() -> None:
    client = FakeCapturer()
    status, body = run(WSGIMiddleware(echo_app(), client), environ_for(b"{}"))

    assert (status, body) == ("201 Created", b'{"status_code":1000}')
    assert client.inbound == []


def test_a_half_set_tenant_pair_is_not_captured() -> None:
    client = FakeCapturer()
    environ = environ_for(
        HTTP_X_EVPANDA_PLATFORM_ID="acme",
        HTTP_X_EVPANDA_PLATFORM_NAME="Acme",
        HTTP_X_EVPANDA_TENANT_ID="t-1",
    )

    run(WSGIMiddleware(echo_app(), client), environ)

    assert client.inbound == []


def test_a_custom_resolver_replaces_the_default() -> None:
    client = FakeCapturer()

    def by_path(info: RequestInfo) -> evpanda.Platform | None:
        name = info.url.removeprefix("/partners/").split("/")[0]
        if not name or not info.url.startswith("/partners/"):
            return None
        return evpanda.Platform(id=name, name=name)

    app = WSGIMiddleware(echo_app(), client, resolver=by_path)
    run(app, environ_for(PATH_INFO="/partners/acme/cdrs", QUERY_STRING=""))
    run(app, environ_for(PATH_INFO="/health", QUERY_STRING=""))

    assert [identity.id for identity, _ in client.inbound] == ["acme"]


def test_a_broken_resolver_only_costs_the_capture() -> None:
    client = FakeCapturer()

    def explode(info: RequestInfo) -> evpanda.Platform:
        raise RuntimeError("boom")

    status, _ = run(WSGIMiddleware(echo_app(), client, resolver=explode), environ_for())

    assert status == "201 Created"
    assert client.inbound == []


def test_an_oversize_body_drops_the_exchange() -> None:
    client = FakeCapturer(max_capture_bytes=16)
    environ = environ_for(b"x" * 64)
    set_identity(environ, PARTNER)

    status, _ = run(WSGIMiddleware(echo_app(), client), environ)

    assert status == "201 Created"
    assert client.inbound == []


def test_a_body_the_app_never_reads_is_still_recorded() -> None:
    """The body is taken up front, so a handler that rejects a request
    without parsing it still records what the partner sent — which is the
    exchange you most want to see.
    """
    client = FakeCapturer()
    environ = environ_for(b'{"id":"cdr-1"}')
    set_identity(environ, PARTNER)

    def ignores_body(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        start_response("204 No Content", [])
        return []

    run(WSGIMiddleware(ignores_body, client), environ)

    _, data = client.inbound[0]
    assert data.request_body == b'{"id":"cdr-1"}'
    assert data.status_code == 204


def test_an_oversize_body_is_never_read_into_memory() -> None:
    """The cap is enforced from Content-Length, before anything is read."""
    client = FakeCapturer(max_capture_bytes=16)
    environ = environ_for(b"x" * 64)
    set_identity(environ, PARTNER)
    original = environ["wsgi.input"]

    def reads_body(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        assert environ["wsgi.input"] is original  # untouched: we never read it
        environ["wsgi.input"].read()
        start_response("413 Payload Too Large", [])
        return []

    status, _ = run(WSGIMiddleware(reads_body, client), environ)

    assert status == "413 Payload Too Large"
    assert client.inbound == []  # oversize drops the exchange, as everywhere


def test_a_chunked_request_is_captured_without_its_body() -> None:
    """No Content-Length means no bounded read, so the stream is left alone."""
    client = FakeCapturer()
    environ = environ_for(b'{"id":"cdr-1"}')
    del environ["CONTENT_LENGTH"]
    set_identity(environ, PARTNER)

    run(WSGIMiddleware(echo_app(), client), environ)

    _, data = client.inbound[0]
    assert data.request_body is None
    assert data.status_code == 201


def test_an_app_that_raises_is_still_captured() -> None:
    client = FakeCapturer()
    environ = environ_for()
    set_identity(environ, PARTNER)

    def explode(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        start_response("500 Internal Server Error", [])
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run(WSGIMiddleware(explode, client), environ)

    assert client.inbound[0][1].status_code == 500


def test_a_closed_client_reverts_to_a_pass_through() -> None:
    client = FakeCapturer(max_capture_bytes=None)
    environ = environ_for(b'{"id":"cdr-1"}')
    set_identity(environ, PARTNER)
    original_input = environ["wsgi.input"]

    status, _ = run(WSGIMiddleware(echo_app(), client), environ)

    assert status == "201 Created"
    assert environ["wsgi.input"] is original_input  # nothing was even wrapped
    assert client.inbound == []


def test_an_app_that_iterates_the_body_still_records_it() -> None:
    client = FakeCapturer()
    environ = environ_for(b'{"a":1}\n{"b":2}\n')
    set_identity(environ, PARTNER)

    def line_reader(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        lines = list(environ["wsgi.input"])
        start_response("200 OK", [])
        return [b"".join(lines)]

    run(WSGIMiddleware(line_reader, client), environ)

    assert client.inbound[0][1].request_body == b'{"a":1}\n{"b":2}\n'


def test_repeated_response_headers_are_joined() -> None:
    client = FakeCapturer()
    environ = environ_for()
    set_identity(environ, PARTNER)

    def two_cookies(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        start_response("200 OK", [("Link", "<a>"), ("Link", "<b>")])
        return [b""]

    run(WSGIMiddleware(two_cookies, client), environ)

    assert client.inbound[0][1].response_headers == {"link": "<a>, <b>"}
