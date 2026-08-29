"""The ASGI middleware."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

import evpanda
from conftest import FakeCapturer
from evpanda.ocpi import RequestInfo, set_identity, use_identity
from evpanda.ocpi.asgi import ASGIMiddleware

PARTNER = evpanda.RoamingIdentity(platform_id="acme", platform_name="Acme Mobility")


def scope_for(**overrides: Any) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "root_path": "",
        "path": "/ocpi/2.2/cdrs",
        "query_string": b"limit=10",
        "headers": [
            (b"content-type", b"application/json"),
            (b"authorization", b"Token secret"),
        ],
    }
    scope.update(overrides)
    return scope


def echo_app(
    status: int = 201, response: bytes = b'{"status_code":1000}', chunks: int = 1
) -> Callable[..., Any]:
    """An ASGI app that drains the request body and answers with ``response``."""

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if not message.get("more_body"):
                break
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        if chunks == 1:
            await send({"type": "http.response.body", "body": response})
        else:
            half = len(response) // 2
            await send({"type": "http.response.body", "body": response[:half], "more_body": True})
            await send({"type": "http.response.body", "body": response[half:]})

    return app


def run(app: Any, scope: dict[str, Any], body: bytes = b"") -> list[dict[str, Any]]:
    """Drive an ASGI app the way a server would, returning what it sent."""
    sent: list[dict[str, Any]] = []
    pending = [
        {"type": "http.request", "body": body, "more_body": False},
        {"type": "http.disconnect"},
    ]

    async def receive() -> dict[str, Any]:
        return pending.pop(0) if pending else {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    asyncio.run(app(scope, receive, send))
    return sent


def test_an_identified_exchange_is_captured() -> None:
    client = FakeCapturer()
    scope = scope_for()
    set_identity(scope, PARTNER)

    sent = run(ASGIMiddleware(echo_app(), client), scope, b'{"id":"cdr-1"}')

    assert sent[0]["status"] == 201
    assert sent[1]["body"] == b'{"status_code":1000}'
    identity, data = client.inbound[0]
    assert identity == PARTNER
    assert data.url == "/ocpi/2.2/cdrs?limit=10"
    assert data.status_code == 201
    assert data.request_body == b'{"id":"cdr-1"}'
    assert data.response_body == b'{"status_code":1000}'
    assert data.request_headers["content-type"] == "application/json"


def test_a_chunked_response_is_recorded_whole() -> None:
    client = FakeCapturer()
    scope = scope_for()
    set_identity(scope, PARTNER)

    run(ASGIMiddleware(echo_app(chunks=2), client), scope)

    assert client.inbound[0][1].response_body == b'{"status_code":1000}'


def test_identity_stamped_by_a_dependency_is_still_seen() -> None:
    client = FakeCapturer()

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        set_identity(scope, PARTNER)  # a dependency inside the middleware
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    run(ASGIMiddleware(app, client), scope_for())

    assert client.inbound[0][0] == PARTNER


def test_identity_falls_back_to_the_headers() -> None:
    client = FakeCapturer()
    scope = scope_for(
        headers=[
            (b"x-evpanda-platform-id", b"acme"),
            (b"x-evpanda-platform-name", b"Acme Mobility"),
        ]
    )

    run(ASGIMiddleware(echo_app(), client), scope)

    assert client.inbound[0][0] == PARTNER


def test_identity_falls_back_to_the_context_var() -> None:
    client = FakeCapturer()

    with use_identity(PARTNER):
        run(ASGIMiddleware(echo_app(), client), scope_for())

    assert client.inbound[0][0] == PARTNER


def test_an_unidentified_request_is_served_but_not_captured() -> None:
    client = FakeCapturer()
    sent = run(ASGIMiddleware(echo_app(), client), scope_for())

    assert sent[0]["status"] == 201
    assert client.inbound == []


def test_a_websocket_scope_passes_straight_through() -> None:
    client = FakeCapturer()
    seen: list[str] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        seen.append(scope["type"])

    run(ASGIMiddleware(app, client), {"type": "websocket", "path": "/ws"})
    run(ASGIMiddleware(app, client), {"type": "lifespan"})

    assert seen == ["websocket", "lifespan"]
    assert client.inbound == []


def test_a_custom_resolver_replaces_the_default() -> None:
    client = FakeCapturer()

    def by_header(info: RequestInfo) -> evpanda.RoamingIdentity | None:
        party = info.headers.get("ocpi-from-party-id")
        return None if party is None else evpanda.RoamingIdentity(party, party)

    scope = scope_for(headers=[(b"ocpi-from-party-id", b"ACM")])
    run(ASGIMiddleware(echo_app(), client, resolver=by_header), scope)

    assert client.inbound[0][0].platform_id == "ACM"


def test_an_oversize_body_drops_the_exchange() -> None:
    client = FakeCapturer(max_capture_bytes=8)
    scope = scope_for()
    set_identity(scope, PARTNER)

    sent = run(ASGIMiddleware(echo_app(), client), scope, b"x" * 64)

    assert sent[0]["status"] == 201
    assert client.inbound == []


def test_an_app_that_raises_is_still_captured() -> None:
    client = FakeCapturer()
    scope = scope_for()
    set_identity(scope, PARTNER)

    async def explode(scope: dict[str, Any], receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 500, "headers": []})
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run(ASGIMiddleware(explode, client), scope)

    assert client.inbound[0][1].status_code == 500


def test_a_closed_client_reverts_to_a_pass_through() -> None:
    client = FakeCapturer(max_capture_bytes=None)
    scope = scope_for()
    set_identity(scope, PARTNER)

    sent = run(ASGIMiddleware(echo_app(), client), scope, b'{"id":"cdr-1"}')

    assert sent[0]["status"] == 201
    assert client.inbound == []


def test_the_root_path_is_part_of_the_url() -> None:
    client = FakeCapturer()
    scope = scope_for(root_path="/api", query_string=b"")
    set_identity(scope, PARTNER)

    run(ASGIMiddleware(echo_app(), client), scope)

    assert client.inbound[0][1].url == "/api/ocpi/2.2/cdrs"
