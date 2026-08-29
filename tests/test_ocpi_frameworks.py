"""The inbound adapters against the frameworks people actually run.

The hand-rolled harnesses in the WSGI and ASGI suites drive the protocols
exactly as written; a real framework does not. Werkzeug reads request
bodies through ``readinto`` rather than ``read``, which an earlier version
of the tee delegated straight past — Flask worked perfectly and every body
arrived empty. These tests exist so that cannot happen again.
"""

from __future__ import annotations

from typing import Any

import pytest

import evpanda
from conftest import FakeCapturer
from evpanda.ocpi import set_identity
from evpanda.ocpi.asgi import ASGIMiddleware
from evpanda.ocpi.wsgi import WSGIMiddleware
from test_ocpi_asgi import run as run_asgi
from test_ocpi_asgi import scope_for

PARTNER = evpanda.RoamingIdentity(platform_id="acme", platform_name="Acme Mobility")


def test_flask_bodies_are_captured_in_both_directions() -> None:
    flask = pytest.importorskip("flask")

    client = FakeCapturer()
    app = flask.Flask(__name__)

    @app.post("/ocpi/2.2/cdrs")
    def cdrs() -> Any:
        payload = flask.request.get_json()
        set_identity(flask.request.environ, PARTNER)
        return flask.jsonify({"status_code": 1000, "data": payload}), 201

    app.wsgi_app = WSGIMiddleware(app.wsgi_app, client)

    response = app.test_client().post("/ocpi/2.2/cdrs", json={"id": "cdr-1"})

    # Reading the body is what a real WSGI server does, and what completes
    # the capture: the exchange ships when the response iterable is done.
    assert response.get_json() == {"status_code": 1000, "data": {"id": "cdr-1"}}
    assert response.status_code == 201
    identity, data = client.inbound[0]
    assert identity == PARTNER
    assert data.method == "POST"
    assert data.url == "/ocpi/2.2/cdrs"
    assert data.status_code == 201
    assert data.request_body is not None and b"cdr-1" in data.request_body
    assert data.response_body is not None and b"status_code" in data.response_body


def test_flask_serves_an_unidentified_request_untouched() -> None:
    flask = pytest.importorskip("flask")

    client = FakeCapturer()
    app = flask.Flask(__name__)

    @app.get("/health")
    def health() -> Any:
        return flask.jsonify(ok=True)

    app.wsgi_app = WSGIMiddleware(app.wsgi_app, client)

    response = app.test_client().get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True}

    assert client.inbound == []


def test_starlette_identity_stamped_by_a_middleware_layer() -> None:
    """The pattern the README documents, pinned so it cannot go stale again.

    Starlette dropped its ``@app.middleware("http")`` decorator, which the
    README used to show; this is the form that works, and it also proves an
    identity stamped by an inner middleware still reaches the outer capture
    middleware.
    """
    pytest.importorskip("starlette")
    from starlette.applications import Starlette
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    client = FakeCapturer()

    async def cdrs(request: Any) -> Any:
        await request.body()
        return JSONResponse({"status_code": 1000}, status_code=201)

    async def authenticate(request: Any, call_next: Any) -> Any:
        if request.headers.get("authorization") != "Token good":
            return JSONResponse({"status_code": 2001}, status_code=401)
        set_identity(request.scope, PARTNER)
        return await call_next(request)

    app = Starlette(routes=[Route("/ocpi/2.2/cdrs", cdrs, methods=["POST"])])
    app.add_middleware(BaseHTTPMiddleware, dispatch=authenticate)

    scope = scope_for(query_string=b"")
    scope["headers"] = [(b"content-type", b"application/json"), (b"authorization", b"Token good")]
    sent = run_asgi(ASGIMiddleware(app, client), scope, b'{"id":"cdr-3"}')

    assert sent[0]["status"] == 201
    identity, data = client.inbound[0]
    assert identity == PARTNER
    assert data.request_body == b'{"id":"cdr-3"}'

    # An unauthenticated request is answered, and not attributed to anyone.
    rejected = scope_for(query_string=b"")
    rejected["headers"] = [(b"content-type", b"application/json")]
    sent = run_asgi(ASGIMiddleware(app, client), rejected, b'{"id":"cdr-4"}')

    assert sent[0]["status"] == 401
    assert len(client.inbound) == 1


def test_starlette_bodies_are_captured_in_both_directions() -> None:
    pytest.importorskip("starlette")
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    client = FakeCapturer()

    async def cdrs(request: Any) -> Any:
        payload = await request.json()
        set_identity(request.scope, PARTNER)
        return JSONResponse({"status_code": 1000, "data": payload}, status_code=201)

    app = Starlette(routes=[Route("/ocpi/2.2/cdrs", cdrs, methods=["POST"])])

    sent = run_asgi(ASGIMiddleware(app, client), scope_for(query_string=b""), b'{"id":"cdr-2"}')

    assert sent[0]["status"] == 201
    identity, data = client.inbound[0]
    assert identity == PARTNER
    assert data.url == "/ocpi/2.2/cdrs"
    assert data.status_code == 201
    assert data.request_body == b'{"id":"cdr-2"}'
    assert data.response_body is not None and b"cdr-2" in data.response_body
