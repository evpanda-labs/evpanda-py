"""A Capturer that misbehaves must never reach the host.

The Go SDK's version of this covers a typed-nil client — one that is not
``== nil`` as an interface and panics on first use. Python has no typed
nil, but it has the same shape of fault: a client that is ``None``, one
whose ``capturing()`` raises, and one that raises at capture time. In every
case the request must be served exactly as it would have been.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import evpanda
from conftest import PartnerServer
from evpanda.ocpi import set_identity, use_identity
from evpanda.ocpi.asgi import ASGIMiddleware
from evpanda.ocpi.wsgi import WSGIMiddleware
from test_ocpi_asgi import echo_app as asgi_echo
from test_ocpi_asgi import run as run_asgi
from test_ocpi_asgi import scope_for
from test_ocpi_wsgi import echo_app as wsgi_echo
from test_ocpi_wsgi import environ_for
from test_ocpi_wsgi import run as run_wsgi

PARTNER = evpanda.Platform(id="acme", name="Acme Mobility")


class Exploding:
    """A Capturer that raises from every method it has."""

    def capturing(self) -> int | None:
        raise RuntimeError("boom")

    def capture_inbound_message(self, identity: Any, data: Any) -> None:
        raise RuntimeError("boom")

    def capture_outbound_message(self, identity: Any, data: Any) -> None:
        raise RuntimeError("boom")


class ExplodesOnCapture:
    """A Capturer that looks healthy until the exchange is shipped."""

    def capturing(self) -> int | None:
        return 65536

    def capture_inbound_message(self, identity: Any, data: Any) -> None:
        raise RuntimeError("boom")

    def capture_outbound_message(self, identity: Any, data: Any) -> None:
        raise RuntimeError("boom")


BROKEN: list[Any] = [None, Exploding(), ExplodesOnCapture()]
IDS = ["none", "capturing raises", "capture raises"]


@pytest.mark.parametrize("client", BROKEN, ids=IDS)
def test_the_wsgi_middleware_still_serves_the_request(client: Any) -> None:
    environ = environ_for(b'{"id":"cdr-1"}')
    set_identity(environ, PARTNER)

    status, body = run_wsgi(WSGIMiddleware(wsgi_echo(), client), environ)

    assert (status, body) == ("201 Created", b'{"status_code":1000}')


@pytest.mark.parametrize("client", BROKEN, ids=IDS)
def test_the_asgi_middleware_still_serves_the_request(client: Any) -> None:
    scope = scope_for()
    set_identity(scope, PARTNER)

    sent = run_asgi(ASGIMiddleware(asgi_echo(), client), scope, b'{"id":"cdr-1"}')

    assert sent[0]["status"] == 201
    assert sent[1]["body"] == b'{"status_code":1000}'


@pytest.mark.parametrize("client", BROKEN, ids=IDS)
def test_the_httpx_transport_still_makes_the_call(client: Any) -> None:
    httpx = pytest.importorskip("httpx")
    from evpanda.ocpi.httpx_transport import HTTPXTransport

    def partner(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"status_code": 1000})

    transport = HTTPXTransport(client, httpx.MockTransport(partner))
    with httpx.Client(transport=transport) as http, use_identity(PARTNER):
        response = http.post("https://partner.test/sessions", json={"id": "s-1"})

    assert response.status_code == 201
    assert response.json() == {"status_code": 1000}


@pytest.mark.parametrize("client", BROKEN, ids=IDS)
def test_the_async_httpx_transport_still_makes_the_call(client: Any) -> None:
    httpx = pytest.importorskip("httpx")
    from evpanda.ocpi.httpx_transport import AsyncHTTPXTransport

    def partner(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"status_code": 1000})

    async def call() -> httpx.Response:
        transport = AsyncHTTPXTransport(client, httpx.MockTransport(partner))
        with use_identity(PARTNER):
            async with httpx.AsyncClient(transport=transport) as http:
                return await http.post("https://partner.test/sessions", json={"id": "s-1"})

    assert asyncio.run(call()).status_code == 201


@pytest.mark.parametrize("client", BROKEN, ids=IDS)
def test_the_requests_adapter_still_makes_the_call(client: Any, partner: PartnerServer) -> None:
    pytest.importorskip("requests")
    import requests

    from evpanda.ocpi.requests_adapter import instrument_session

    session = instrument_session(requests.Session(), client)
    with use_identity(PARTNER):
        response = session.get(f"{partner.url}/ocpi/2.2/locations")

    assert response.status_code == 200
    assert response.json() == {"status_code": 1000, "data": []}
