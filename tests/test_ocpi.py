"""OCPIClient + OCPI redaction: header allowlist, credentials-token mask,
direction stamping, inert-on-bad-config, and end-to-end delivery.
"""

from __future__ import annotations

import base64
import json

import pytest
from conftest import MockUpstream, wait_for

from evpanda.config import OCPIConfig
from evpanda.identity import RoamingIdentity
from evpanda.ocpi import TOKEN_PLACEHOLDER, OCPIClient, make_ocpi_redactor
from evpanda.types import HttpExchange, OCPIDirection, OCPIMessage, OCPIMessageInput

IDENTITY = RoamingIdentity(platform_id="acme", platform_name="Acme Mobility")


def message(**data: object) -> OCPIMessage:
    kwargs = {"method": "POST", "url": "/ocpi/2.2/cdrs", **data}
    return OCPIMessage(
        direction=OCPIDirection.IN,
        identity=IDENTITY,
        data=HttpExchange(**kwargs),  # type: ignore[arg-type]
    )


# ── redaction ────────────────────────────────────────────────────────────


def test_headers_outside_the_allowlist_are_dropped() -> None:
    redact = make_ocpi_redactor()
    out = redact(
        message(
            request_headers={
                "Authorization": "Token SECRET",
                "Cookie": "sid=1",
                "X-API-Key": "k",
                "Content-Type": "application/json",  # allowlisted, case-insensitive
                "OCPI-from-party-id": "ACM",
            },
            response_headers={"X-Total-Count": "3", "Set-Cookie": "sid=1"},
        )
    )
    assert out.data.request_headers == {
        "Content-Type": "application/json",
        "OCPI-from-party-id": "ACM",
    }
    assert out.data.response_headers == {"X-Total-Count": "3"}


def test_extra_allowed_headers_extend_the_defaults() -> None:
    # The list arrives already trimmed/lowercased from config resolution.
    redact = make_ocpi_redactor(["x-trace", "X-Extra"])
    out = redact(
        message(request_headers={"X-Trace": "t", "x-extra": "e", "Authorization": "secret"})
    )
    assert out.data.request_headers == {"X-Trace": "t", "x-extra": "e"}
    # Defaults still apply alongside the extras.
    assert redact(message(request_headers={"accept": "*/*"})).data.request_headers == {
        "accept": "*/*"
    }


def test_redaction_does_not_mutate_the_input() -> None:
    original = message(request_headers={"Authorization": "secret"})
    make_ocpi_redactor()(original)
    assert original.data.request_headers == {"Authorization": "secret"}


@pytest.mark.parametrize(
    "url",
    [
        "/ocpi/2.2/credentials",
        "/ocpi/2.2/credentials/",
        "/ocpi/2.2/credentials?x=1",
        "/CREDENTIALS",
    ],
)
def test_credentials_token_is_masked_at_the_root(url: str) -> None:
    body = json.dumps({"token": "SECRET", "url": "https://partner/versions"}).encode()
    out = make_ocpi_redactor()(message(url=url, request_body=body))
    assert out.data.request_body is not None
    parsed = json.loads(out.data.request_body)
    assert parsed["token"] == TOKEN_PLACEHOLDER
    assert parsed["url"] == "https://partner/versions"


def test_credentials_token_is_masked_under_the_data_envelope() -> None:
    body = json.dumps({"status_code": 1000, "data": {"token": "SECRET"}}).encode()
    out = make_ocpi_redactor()(message(url="/ocpi/2.2/credentials", response_body=body))
    assert out.data.response_body is not None
    assert json.loads(out.data.response_body)["data"]["token"] == TOKEN_PLACEHOLDER


@pytest.mark.parametrize(
    ("url", "body"),
    [
        ("/ocpi/2.2/cdrs", b'{"token":"SECRET"}'),  # not a credentials URL
        ("/ocpi/2.2/credentials/foo", b'{"token":"SECRET"}'),  # no such route
        ("/ocpi/2.2/credentials", b"not json at all"),  # unparseable
        ("/ocpi/2.2/credentials", b'["token"]'),  # not an object
        ("/ocpi/2.2/credentials", b'{"url":"https://x"}'),  # no token anywhere
        ("/ocpi/2.2/credentials", b'{"token":""}'),  # empty token
    ],
)
def test_bodies_without_a_maskable_token_are_returned_untouched(url: str, body: bytes) -> None:
    out = make_ocpi_redactor()(message(url=url, request_body=body))
    assert out.data.request_body == body


# ── client ───────────────────────────────────────────────────────────────


def test_capture_stamps_the_direction(mock: MockUpstream) -> None:
    client = OCPIClient.start(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    try:
        client.capture_inbound_message(
            OCPIMessageInput(
                identity=IDENTITY,
                data=HttpExchange(
                    method="POST",
                    url="/in",
                    status_code=200,
                    request_headers={"Authorization": "Token SECRET", "accept": "application/json"},
                    request_body=b"payload",
                ),
            )
        )
        client.capture_outbound_message(
            OCPIMessageInput(
                identity=IDENTITY,
                data=HttpExchange(method="GET", url="/out"),
            )
        )
        wait_for(lambda: len(mock.ocpi_records()) == 2, 3.0)

        by_url = {r["url"]: r for r in mock.ocpi_records()}
        assert by_url["/in"]["direction"] == "IN"
        assert by_url["/out"]["direction"] == "OUT"
        assert by_url["/in"]["platform_id"] == "acme"
        assert base64.standard_b64decode(by_url["/in"]["request_body"]) == b"payload"
        # The client wired the allowlist redactor into the chokepoint.
        assert by_url["/in"]["request_headers"] == {"accept": "application/json"}
    finally:
        client.close()


def test_allowed_headers_from_config_reach_the_redactor(mock: MockUpstream) -> None:
    client = OCPIClient.start(
        OCPIConfig(
            endpoint=mock.url,
            api_key="k",
            flush_interval=0.1,
            ocpi_allowed_headers=["X-Trace"],
        )
    )
    try:
        client.capture_inbound_message(
            OCPIMessageInput(
                identity=IDENTITY,
                data=HttpExchange(
                    method="GET", url="/x", request_headers={"X-Trace": "t", "Cookie": "sid=1"}
                ),
            )
        )
        wait_for(lambda: len(mock.ocpi_records()) == 1, 3.0)
        assert mock.ocpi_records()[0]["request_headers"] == {"X-Trace": "t"}
    finally:
        client.close()


def test_flush_and_close_deliver(mock: MockUpstream) -> None:
    client = OCPIClient.start(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    client.capture_inbound_message(
        OCPIMessageInput(identity=IDENTITY, data=HttpExchange(method="GET", url="/a"))
    )
    client.flush()
    wait_for(lambda: len(mock.ocpi_records()) == 1, 3.0)

    client.capture_inbound_message(
        OCPIMessageInput(identity=IDENTITY, data=HttpExchange(method="GET", url="/b"))
    )
    client.close()  # drains
    assert len(mock.ocpi_records()) == 2


def test_close_is_idempotent_and_goes_inert(mock: MockUpstream) -> None:
    client = OCPIClient.start(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    assert client._internal is not None
    assert client._internal.max_capture_bytes == 64 * 1024

    client.close()
    client.close()
    assert client._internal is None

    # Post-close capture is a silent no-op.
    client.capture_inbound_message(
        OCPIMessageInput(identity=IDENTITY, data=HttpExchange(method="GET", url="/after"))
    )
    client.flush()
    assert [r for r in mock.ocpi_records() if r["url"] == "/after"] == []


def test_bad_config_yields_an_inert_client(mock: MockUpstream) -> None:
    client = OCPIClient.start(OCPIConfig(endpoint="not-a-url", api_key="k"))
    assert client._internal is None  # adapters short-circuit on this

    client.capture_inbound_message(
        OCPIMessageInput(identity=IDENTITY, data=HttpExchange(method="GET", url="/x"))
    )
    client.flush()
    client.close()
    assert mock.ocpi_records() == []


def test_garbage_input_never_raises(mock: MockUpstream) -> None:
    client = OCPIClient.start(OCPIConfig(endpoint=mock.url, api_key="k"))
    try:
        client.capture_inbound_message("not a message")  # type: ignore[arg-type]
        client.capture_outbound_message(None)  # type: ignore[arg-type]
        client.capture_inbound_message(
            OCPIMessageInput(identity=IDENTITY, data=None)  # type: ignore[arg-type]
        )
        client.flush()
    finally:
        client.close()
