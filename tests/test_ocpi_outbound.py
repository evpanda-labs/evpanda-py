"""The outbound adapters: httpx and requests."""

from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any

import pytest

# The two outbound adapters are optional extras, so this whole suite is
# skipped on an install that does not have them.
httpx = pytest.importorskip("httpx")
requests = pytest.importorskip("requests")

import evpanda  # noqa: E402
from conftest import FakeCapturer, PartnerServer  # noqa: E402
from evpanda.ocpi import IDENTITY_KEY, RequestInfo, use_identity  # noqa: E402
from evpanda.ocpi.httpx_transport import (  # noqa: E402
    AsyncHTTPXTransport,
    HTTPXTransport,
)
from evpanda.ocpi.requests_adapter import (  # noqa: E402
    RequestsAdapter,
    instrument_session,
)

PARTNER = evpanda.RoamingIdentity(platform_id="acme", platform_name="Acme Mobility")
URL = "https://partner.example/ocpi/2.2/sessions"


def mock_transport(
    seen: list[httpx.Request], status: int = 201, body: bytes = b'{"status_code":1000}'
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        request.read()
        seen.append(request)
        return httpx.Response(status, headers={"content-type": "application/json"}, content=body)

    return httpx.MockTransport(handler)


def test_a_missing_extra_names_the_install_command() -> None:
    """The import error has to say what to install, not just what is absent."""
    import builtins

    real_import = builtins.__import__

    def without_httpx(name: str, *args: Any) -> Any:
        if name == "httpx":
            raise ImportError("No module named 'httpx'")
        return real_import(name, *args)

    for module in ("evpanda.ocpi.httpx_transport", "evpanda.ocpi.requests_adapter"):
        sys.modules.pop(module, None)
    builtins.__import__ = without_httpx
    try:
        with pytest.raises(ImportError, match=r"pip install 'evpanda\[httpx\]'"):
            importlib.import_module("evpanda.ocpi.httpx_transport")
    finally:
        builtins.__import__ = real_import
        sys.modules.pop("evpanda.ocpi.httpx_transport", None)
        importlib.import_module("evpanda.ocpi.httpx_transport")


# ── httpx, synchronous ───────────────────────────────────────────────────


def test_an_identified_call_is_captured() -> None:
    client = FakeCapturer()
    seen: list[httpx.Request] = []
    transport = HTTPXTransport(client, mock_transport(seen))

    with httpx.Client(transport=transport) as http, use_identity(PARTNER):
        response = http.post(URL, json={"id": "session-1"})

    assert response.status_code == 201
    identity, data = client.outbound[0]
    assert identity == PARTNER
    assert data.method == "POST"
    assert data.url == URL
    assert data.status_code == 201
    assert data.request_body == b'{"id":"session-1"}'
    assert data.response_body == b'{"status_code":1000}'
    assert data.response_headers["content-type"] == "application/json"


def test_identity_can_ride_on_the_request_extensions() -> None:
    client = FakeCapturer()
    transport = HTTPXTransport(client, mock_transport([]))

    with httpx.Client(transport=transport) as http:
        http.get(URL, extensions={IDENTITY_KEY: PARTNER})

    assert client.outbound[0][0] == PARTNER


def test_the_identity_headers_never_reach_the_partner() -> None:
    client = FakeCapturer()
    seen: list[httpx.Request] = []
    transport = HTTPXTransport(client, mock_transport(seen))

    with httpx.Client(transport=transport) as http:
        http.get(
            URL,
            headers={
                "X-EVPanda-Platform-Id": "acme",
                "X-EVPanda-Platform-Name": "Acme Mobility",
                "Authorization": "Token partner-secret",
            },
        )

    assert client.outbound[0][0] == PARTNER  # the headers still resolved it
    sent = seen[0].headers
    assert "x-evpanda-platform-id" not in sent
    assert "x-evpanda-platform-name" not in sent
    assert sent["authorization"] == "Token partner-secret"  # theirs is untouched


def test_an_unidentified_call_is_made_but_not_captured() -> None:
    client = FakeCapturer()
    transport = HTTPXTransport(client, mock_transport([]))

    with httpx.Client(transport=transport) as http:
        assert http.get(URL).status_code == 201

    assert client.outbound == []


def test_a_streamed_response_is_captured_when_it_is_read() -> None:
    client = FakeCapturer()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=iter([b"chunk-", b"body"]))

    transport = HTTPXTransport(client, httpx.MockTransport(handler))
    with (
        httpx.Client(transport=transport) as http,
        use_identity(PARTNER),
        http.stream("GET", URL) as response,
    ):
        assert client.outbound == []  # nothing shipped until the body is done
        body = response.read()

    assert body == b"chunk-body"
    assert client.outbound[0][1].response_body == b"chunk-body"


def test_a_streamed_upload_is_teed_as_it_is_sent() -> None:
    client = FakeCapturer()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        # A real transport iterates the request stream to send it; do the
        # same, so the tee sees the chunks.
        seen.append(request)
        b"".join(request.stream)  # type: ignore[arg-type]
        return httpx.Response(201, content=b"{}")

    transport = HTTPXTransport(client, httpx.MockTransport(handler))
    with httpx.Client(transport=transport) as http, use_identity(PARTNER):
        http.post(URL, content=iter([b"chunk-one-", b"chunk-two"]))

    assert client.outbound[0][1].request_body == b"chunk-one-chunk-two"


def test_a_transport_error_captures_nothing() -> None:
    client = FakeCapturer()

    def explode(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    transport = HTTPXTransport(client, httpx.MockTransport(explode))
    with (
        httpx.Client(transport=transport) as http,
        use_identity(PARTNER),
        pytest.raises(httpx.ConnectError),
    ):
        http.get(URL)

    assert client.outbound == []


def test_an_oversize_body_drops_the_exchange() -> None:
    client = FakeCapturer(max_capture_bytes=8)
    transport = HTTPXTransport(client, mock_transport([]))

    with httpx.Client(transport=transport) as http, use_identity(PARTNER):
        assert http.post(URL, content=b"x" * 64).status_code == 201

    assert client.outbound == []


def test_a_closed_client_still_strips_the_headers() -> None:
    client = FakeCapturer(max_capture_bytes=None)
    seen: list[httpx.Request] = []
    transport = HTTPXTransport(client, mock_transport(seen))

    with httpx.Client(transport=transport) as http:
        http.get(URL, headers={"X-EVPanda-Platform-Id": "acme"})

    assert "x-evpanda-platform-id" not in seen[0].headers
    assert client.outbound == []


def test_a_custom_resolver_replaces_the_default() -> None:
    client = FakeCapturer()

    def by_host(info: RequestInfo) -> evpanda.RoamingIdentity | None:
        host = httpx.URL(info.url).host
        return evpanda.RoamingIdentity(platform_id=host, platform_name=host)

    transport = HTTPXTransport(client, mock_transport([]), resolver=by_host)
    with httpx.Client(transport=transport) as http:
        http.get(URL)

    assert client.outbound[0][0].platform_id == "partner.example"


# ── httpx, asynchronous ──────────────────────────────────────────────────


def test_the_async_transport_captures_too() -> None:
    client = FakeCapturer()
    seen: list[httpx.Request] = []
    transport = AsyncHTTPXTransport(client, mock_transport(seen))

    async def call() -> httpx.Response:
        with use_identity(PARTNER):
            async with httpx.AsyncClient(transport=transport) as http:
                return await http.post(URL, json={"id": "session-1"})

    response = asyncio.run(call())

    assert response.status_code == 201
    identity, data = client.outbound[0]
    assert identity == PARTNER
    assert data.request_body == b'{"id":"session-1"}'
    assert data.response_body == b'{"status_code":1000}'


def test_the_async_transport_leaves_unidentified_calls_alone() -> None:
    client = FakeCapturer()
    transport = AsyncHTTPXTransport(client, mock_transport([]))

    async def call() -> httpx.Response:
        async with httpx.AsyncClient(transport=transport) as http:
            return await http.get(URL)

    assert asyncio.run(call()).status_code == 201
    assert client.outbound == []


# ── requests ─────────────────────────────────────────────────────────────


def session_for(client: Any, **kwargs: Any) -> requests.Session:
    session = requests.Session()
    adapter = RequestsAdapter(client, **kwargs)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def test_requests_captures_an_identified_call(partner: PartnerServer) -> None:
    client = FakeCapturer()
    session = session_for(client)

    with use_identity(PARTNER):
        response = session.post(f"{partner.url}/ocpi/2.2/sessions", json={"id": "s-1"})

    assert response.status_code == 201
    assert response.json() == {"status_code": 1000, "data": []}
    identity, data = client.outbound[0]
    assert identity == PARTNER
    assert data.method == "POST"
    assert data.url.endswith("/ocpi/2.2/sessions")
    assert data.status_code == 201
    assert data.request_body == b'{"id": "s-1"}'
    assert data.response_body is not None
    assert b"status_code" in data.response_body


def test_requests_strips_the_identity_headers(partner: PartnerServer) -> None:
    client = FakeCapturer()
    session = session_for(client)

    session.get(
        f"{partner.url}/ocpi/2.2/locations",
        headers={
            "X-EVPanda-Platform-Id": "acme",
            "X-EVPanda-Platform-Name": "Acme Mobility",
        },
    )

    assert client.outbound[0][0] == PARTNER
    assert "x-evpanda-platform-id" not in partner.received[0].headers


def test_requests_leaves_unidentified_calls_alone(partner: PartnerServer) -> None:
    client = FakeCapturer()
    session = session_for(client)

    assert session.get(partner.url).status_code == 200
    assert client.outbound == []


def test_a_streamed_response_is_captured_without_its_body(partner: PartnerServer) -> None:
    client = FakeCapturer()
    session = session_for(client)

    with use_identity(PARTNER):
        response = session.get(f"{partner.url}/big", stream=True)
        body = response.content  # still the caller's to read

    assert b"status_code" in body
    data = client.outbound[0][1]
    assert data.status_code == 200
    assert data.response_body is None


def test_instrument_session_mounts_both_schemes(partner: PartnerServer) -> None:
    client = FakeCapturer()
    session = instrument_session(requests.Session(), client)

    with use_identity(PARTNER):
        session.get(partner.url)

    assert isinstance(session.adapters["https://"], RequestsAdapter)
    assert len(client.outbound) == 1


def test_a_closed_client_reverts_to_a_plain_adapter(partner: PartnerServer) -> None:
    client = FakeCapturer(max_capture_bytes=None)
    session = session_for(client)

    with use_identity(PARTNER):
        assert session.get(partner.url).status_code == 200

    assert client.outbound == []
