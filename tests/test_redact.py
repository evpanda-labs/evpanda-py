"""Redaction: the header allowlist and the credentials token mask."""

from __future__ import annotations

import json

import pytest

from evpanda._redact import (
    DEFAULT_OCPI_HEADER_ALLOWLIST,
    TOKEN_PLACEHOLDER,
    make_ocpi_redactor,
    mask_credentials_token,
)
from evpanda._types import HTTPExchange, OCPIDirection, OCPIMessage, Platform

PARTNER = Platform(id="acme", name="Acme")


def redact_headers(headers: dict[str, str], allowed: tuple[str, ...] = ()) -> dict[str, str]:
    message = OCPIMessage(
        direction=OCPIDirection.IN,
        identity=PARTNER,
        data=HTTPExchange(method="GET", url="/ocpi/2.2/cdrs", request_headers=headers),
    )
    return make_ocpi_redactor(allowed)(message).data.request_headers


def test_secrets_fall_off_the_end() -> None:
    kept = redact_headers(
        {
            "Authorization": "Token secret",
            "Cookie": "session=1",
            "X-API-Key": "key",
            "Content-Type": "application/json",
            "OCPI-from-party-id": "ACM",
        }
    )
    assert kept == {"Content-Type": "application/json", "OCPI-from-party-id": "ACM"}


def test_the_allowlist_only_ever_grows() -> None:
    kept = redact_headers({"X-Trace": "abc", "accept": "*/*"}, allowed=("x-trace",))
    assert kept == {"X-Trace": "abc", "accept": "*/*"}


def test_every_default_header_is_lowercase() -> None:
    assert all(h == h.lower() for h in DEFAULT_OCPI_HEADER_ALLOWLIST)


def test_response_headers_are_filtered_too() -> None:
    message = OCPIMessage(
        direction=OCPIDirection.OUT,
        identity=PARTNER,
        data=HTTPExchange(
            method="GET",
            url="/ocpi/2.2/locations",
            response_headers={"Set-Cookie": "a=1", "X-Total-Count": "12"},
        ),
    )
    kept = make_ocpi_redactor()(message).data.response_headers
    assert kept == {"X-Total-Count": "12"}


@pytest.mark.parametrize(
    "url",
    [
        "/ocpi/2.2/credentials",
        "/ocpi/2.2/credentials/",
        "/ocpi/2.2/credentials?x=1",
        "https://partner.example/ocpi/2.2/CREDENTIALS",
    ],
)
def test_a_credentials_token_is_masked(url: str) -> None:
    body = json.dumps({"token": "super-secret", "url": "https://acme.test"}).encode()
    masked = mask_credentials_token(body, url)
    assert masked is not None
    assert json.loads(masked) == {"token": TOKEN_PLACEHOLDER, "url": "https://acme.test"}


def test_a_token_under_the_response_envelope_is_masked() -> None:
    body = json.dumps(
        {"data": {"token": "super-secret", "party_id": "ACM"}, "status_code": 1000}
    ).encode()
    masked = mask_credentials_token(body, "/ocpi/2.2/credentials")
    assert masked is not None
    assert json.loads(masked)["data"] == {"token": TOKEN_PLACEHOLDER, "party_id": "ACM"}


@pytest.mark.parametrize(
    ("body", "url"),
    [
        (b'{"token":"secret"}', "/ocpi/2.2/cdrs"),  # not the credentials route
        (b'{"token":"secret"}', "/ocpi/2.2/credentials/foo"),  # no such route
        (b"not json at all", "/ocpi/2.2/credentials"),
        (b'["a","list"]', "/ocpi/2.2/credentials"),
        (b'{"other":"field"}', "/ocpi/2.2/credentials"),
        (b'{"token":""}', "/ocpi/2.2/credentials"),
        (b"\xff\xfe binary", "/ocpi/2.2/credentials"),
        (None, "/ocpi/2.2/credentials"),
    ],
)
def test_anything_it_cannot_rewrite_is_left_alone(body: bytes | None, url: str) -> None:
    assert mask_credentials_token(body, url) == body


def test_the_redactor_masks_both_bodies() -> None:
    body = json.dumps({"token": "secret"}).encode()
    message = OCPIMessage(
        direction=OCPIDirection.OUT,
        identity=PARTNER,
        data=HTTPExchange(
            method="POST",
            url="/ocpi/2.2/credentials",
            request_body=body,
            response_body=json.dumps({"data": {"token": "other"}}).encode(),
        ),
    )
    data = make_ocpi_redactor()(message).data
    assert data.request_body is not None
    assert data.response_body is not None
    assert b"secret" not in data.request_body
    assert b"other" not in data.response_body
