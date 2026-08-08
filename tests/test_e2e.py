"""End-to-end tests: drive the capture chokepoint against a mock upstream and
assert the wire shape, batching, redaction, routing, compression, drop-oldest,
graceful close, and that nothing raises.

The protocol clients (``evpanda.ocpi`` / ``evpanda.ocpp``) are not ported
yet, so these assemble the same core the clients will: resolve the config,
build the ring + transport, and drive :class:`evpanda.worker.Worker`.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Callable

import pytest
from conftest import MockUpstream, identity_redactor, make_ocpi, strip_authorization, wait_for

from evpanda import transport
from evpanda.config import OCPIConfig, OCPPConfig
from evpanda.identity import ChargerIdentity, RoamingIdentity
from evpanda.types import (
    HttpExchange,
    OCPIDirection,
    OCPIMessage,
    OCPPEventType,
    OCPPMessage,
)
from evpanda.worker import Worker

_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

Build = Callable[..., Worker]


def test_capture_batch_redact_route(mock: MockUpstream, workers: Build) -> None:
    worker = workers(
        OCPIConfig(endpoint=mock.url, api_key="test-key", flush_interval=0.1),
    )
    for i in range(3):
        worker.capture_ocpi(make_ocpi(i), strip_authorization)

    wait_for(lambda: len(mock.ocpi_records()) == 3, 3.0)

    recs = sorted(mock.ocpi_records(), key=lambda r: r["url"])
    assert len(recs) == 3
    assert mock.ocpi_posts()[0].headers.get("X-API-Key") == "test-key"

    for i, rec in enumerate(recs):
        assert _TS_RE.match(rec["captured_at"])
        assert rec["url"] == f"/ocpi/2.2/cdrs/{i}"
        assert rec["platform_id"] == "acme"
        assert rec["direction"] == "IN"
        assert rec["http_method"] == "POST"
        assert rec["response_status_code"] == 200
        # The redactor handed to the chokepoint was applied.
        req_headers = rec["request_headers"]
        assert "Authorization" not in req_headers
        assert "authorization" not in req_headers
        assert req_headers["X-Trace"] == str(i)
        assert base64.standard_b64decode(rec["request_body"]) == f"body-{i}".encode()


def test_gzip_and_chunking(mock: MockUpstream, workers: Build) -> None:
    worker = workers(
        OCPIConfig(
            endpoint=mock.url,
            api_key="k",
            compression="gzip",
            flush_interval=0.1,
            buffer_capacity=100_000,
        )
    )
    n = 2500
    for i in range(n):
        worker.capture_ocpi(make_ocpi(i), identity_redactor)

    wait_for(lambda: len(mock.ocpi_records()) == n, 8.0)

    posts = mock.ocpi_posts()
    assert len(posts) >= 3
    for p in posts:
        assert len(p.records) <= 1000
        assert p.headers.get("Content-Encoding") == "gzip"

    for i, rec in enumerate(mock.ocpi_records()):
        assert rec["url"] == f"/ocpi/2.2/cdrs/{i}"


def test_default_zstd_path(mock: MockUpstream, workers: Build) -> None:
    pytest.importorskip("zstandard")
    worker = workers(
        OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1, buffer_capacity=100_000)
    )
    # >= 1024 bytes of payload so compression actually kicks in.
    n = 200
    for i in range(n):
        worker.capture_ocpi(make_ocpi(i), identity_redactor)

    wait_for(lambda: len(mock.ocpi_records()) == n, 5.0)
    assert any(p.headers.get("Content-Encoding") == "zstd" for p in mock.ocpi_posts())


def test_gzip_fallback_when_zstd_is_absent(
    mock: MockUpstream, workers: Build, monkeypatch: pytest.MonkeyPatch
) -> None:
    # zstd is an optional extra; without it the transport degrades to gzip.
    monkeypatch.setattr(transport, "_load_zstd", lambda: None)
    worker = workers(
        OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1, buffer_capacity=100_000)
    )
    n = 200
    for i in range(n):
        worker.capture_ocpi(make_ocpi(i), identity_redactor)

    wait_for(lambda: len(mock.ocpi_records()) == n, 5.0)
    assert all(p.headers.get("Content-Encoding") == "gzip" for p in mock.ocpi_posts())


def test_small_payload_sent_identity(mock: MockUpstream, workers: Build) -> None:
    worker = workers(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    worker.capture_ocpi(
        OCPIMessage(
            direction=OCPIDirection.OUT,
            identity=RoamingIdentity(platform_id="acme", platform_name="Acme"),
            data=HttpExchange(method="GET", url="/x"),
        ),
        identity_redactor,
    )
    wait_for(lambda: len(mock.ocpi_records()) == 1, 3.0)
    assert mock.ocpi_posts()[0].headers.get("Content-Encoding") is None


def test_drop_oldest(mock: MockUpstream, workers: Build) -> None:
    worker = workers(
        OCPIConfig(
            endpoint=mock.url,
            api_key="k",
            buffer_capacity=5,
            flush_interval=60.0,  # no auto flush during the test
        )
    )
    for i in range(12):  # 0..11
        worker.capture_ocpi(make_ocpi(i), identity_redactor)
    worker.flush_once()  # force one drain

    wait_for(lambda: len(mock.ocpi_records()) == 5, 3.0)
    urls = sorted(r["url"] for r in mock.ocpi_records())
    assert urls == sorted(f"/ocpi/2.2/cdrs/{i}" for i in (7, 8, 9, 10, 11))


def test_flush_on_close(mock: MockUpstream, workers: Build) -> None:
    worker = workers(
        OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=60.0)  # never auto-flushes
    )
    for i in range(4):
        worker.capture_ocpi(make_ocpi(i), identity_redactor)
    assert len(mock.ocpi_records()) == 0

    worker.close()  # graceful drain
    assert len(mock.ocpi_records()) == 4

    worker.close()  # idempotent


def test_never_raises_when_upstream_fails(mock: MockUpstream, workers: Build) -> None:
    mock.set_status(400)  # permanent reject → dropped
    worker = workers(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    for i in range(3):
        worker.capture_ocpi(make_ocpi(i), identity_redactor)

    worker.flush_once()  # returns even though the upstream 400s

    assert len(mock.ocpi_posts()) > 0
    worker.capture_ocpi(make_ocpi(100), identity_redactor)
    worker.flush_once()


def test_invalid_identity_is_dropped(mock: MockUpstream, workers: Build) -> None:
    worker = workers(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    bad = [
        RoamingIdentity(platform_id="", platform_name="Acme"),  # no platform id
        RoamingIdentity(platform_id="acme", platform_name="   "),  # blank name
        RoamingIdentity(platform_id="acme", platform_name="Acme", tenant_id="t"),  # half tenant
    ]
    for identity in bad:
        worker.capture_ocpi(
            OCPIMessage(
                direction=OCPIDirection.IN,
                identity=identity,
                data=HttpExchange(method="GET", url="/x"),
            ),
            identity_redactor,
        )
    worker.flush_once()
    assert mock.ocpi_records() == []


def test_oversize_body_drops_the_message(mock: MockUpstream, workers: Build) -> None:
    worker = workers(
        OCPIConfig(endpoint=mock.url, api_key="k", max_capture_bytes=16, flush_interval=60.0)
    )
    ident = RoamingIdentity(platform_id="acme", platform_name="Acme")
    worker.capture_ocpi(
        OCPIMessage(
            direction=OCPIDirection.IN,
            identity=ident,
            data=HttpExchange(method="POST", url="/big", request_body=b"x" * 17),
        ),
        identity_redactor,
    )
    worker.capture_ocpi(
        OCPIMessage(
            direction=OCPIDirection.IN,
            identity=ident,
            data=HttpExchange(method="POST", url="/big-response", response_body=b"y" * 17),
        ),
        identity_redactor,
    )
    worker.capture_ocpi(
        OCPIMessage(
            direction=OCPIDirection.IN,
            identity=ident,
            data=HttpExchange(method="POST", url="/ok", request_body=b"z" * 16),
        ),
        identity_redactor,
    )
    worker.flush_once()

    wait_for(lambda: len(mock.ocpi_records()) == 1, 3.0)
    assert mock.ocpi_records()[0]["url"] == "/ok"


def test_nullable_fields_serialized_as_null(mock: MockUpstream, workers: Build) -> None:
    worker = workers(OCPIConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    # No tenant, no bodies, no status code, no headers.
    worker.capture_ocpi(
        OCPIMessage(
            direction=OCPIDirection.OUT,
            identity=RoamingIdentity(platform_id="acme", platform_name="Acme"),
            data=HttpExchange(method="GET", url="/x"),
        ),
        identity_redactor,
    )
    wait_for(lambda: len(mock.ocpi_records()) == 1, 3.0)

    rec = mock.ocpi_records()[0]
    # Keys are PRESENT and explicitly null when absent.
    for key in (
        "tenant_id",
        "tenant_name",
        "response_status_code",
        "request_headers",
        "request_body",
        "response_headers",
        "response_body",
    ):
        assert key in rec, f"{key} must be present"
        assert rec[key] is None, f"{key} must be JSON null"
    assert rec["platform_id"] == "acme"
    assert rec["direction"] == "OUT"
    assert rec["http_method"] == "GET"


def test_ocpp_event_type_never_null(mock: MockUpstream, workers: Build) -> None:
    worker = workers(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    # DISCONNECT == 0 must still serialize as 0, never null.
    worker.capture_ocpp(
        OCPPMessage(
            event_type=OCPPEventType.DISCONNECT,
            identity=ChargerIdentity(charger_id="CP-001"),
            connection_id="conn-1",
        ),
        identity_redactor,
    )
    wait_for(lambda: len(mock.records("/v1/ocpp")) == 1, 3.0)

    rec = mock.records("/v1/ocpp")[0]
    assert rec["event_type"] == 0
    assert rec["event_type"] is not None
    assert rec["direction"] is None  # absent ⇒ null
    assert rec["raw_frame"] is None
    assert rec["charger_id"] == "CP-001"
    assert rec["connection_id"] == "conn-1"
