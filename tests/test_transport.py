"""The wire records, the codec, and the bounded retry."""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest
import zstandard

import evpanda
from conftest import IngestServer
from evpanda._buffer import BufferedMessage
from evpanda._config import LogMode, resolve_ocpi_config, resolve_ocpp_config
from evpanda._stats import Counters
from evpanda._transport import (
    BACKOFF_BASE,
    BACKOFF_MAX,
    BACKOFF_MAX_ATTEMPTS,
    COMPRESS_MIN_BYTES,
    Transport,
    compress,
    next_delay,
    serialize,
)
from evpanda._types import (
    Charger,
    HTTPExchange,
    OCPIDirection,
    OCPIMessage,
    OCPPDirection,
    OCPPEventType,
    OCPPMessage,
    Platform,
    Protocol,
)

STAMP = "2026-05-18T12:34:56.789Z"


def ocpi_envelope(**overrides: Any) -> BufferedMessage:
    fields: dict[str, Any] = {
        "method": "POST",
        "url": "/ocpi/2.2/cdrs",
        "status_code": 201,
        "request_headers": {"content-type": "application/json"},
        "request_body": b'{"id":"cdr-1"}',
    }
    fields.update(overrides)
    data = HTTPExchange(**fields)
    return BufferedMessage(
        captured_at=STAMP,
        message=OCPIMessage(
            direction=OCPIDirection.IN,
            identity=Platform(
                id="acme",
                name="Acme",
                tenant_id="t-1",
                tenant_name="Tenant",
            ),
            data=data,
        ),
    )


def ocpp_envelope(**overrides: Any) -> BufferedMessage:
    fields: dict[str, Any] = {
        "event_type": OCPPEventType.MESSAGE,
        "identity": Charger(id="CP-001"),
        "connection_id": "conn-1",
        "direction": OCPPDirection.FROM_CP,
        "payload": b'[2,"1","Heartbeat",{}]',
    }
    fields.update(overrides)
    return BufferedMessage(captured_at=STAMP, message=OCPPMessage(**fields))


def test_the_ocpi_wire_shape() -> None:
    body = json.loads(serialize([ocpi_envelope()]))
    assert body == {
        "messages": [
            {
                "captured_at": STAMP,
                "platform_id": "acme",
                "platform_name": "Acme",
                "tenant_id": "t-1",
                "tenant_name": "Tenant",
                "direction": "IN",
                "http_method": "POST",
                "url": "/ocpi/2.2/cdrs",
                "response_status_code": 201,
                "request_headers": {"content-type": "application/json"},
                "request_body": base64.standard_b64encode(b'{"id":"cdr-1"}').decode(),
                "response_headers": None,
                "response_body": None,
            }
        ]
    }


def test_the_ocpp_wire_shape() -> None:
    body = json.loads(serialize([ocpp_envelope()]))
    assert body == {
        "messages": [
            {
                "charger_id": "CP-001",
                "connection_id": "conn-1",
                "tenant_id": None,
                "tenant_name": None,
                "captured_at": STAMP,
                "event_type": 2,
                "direction": "FROM_CP",
                "raw_frame": base64.standard_b64encode(b'[2,"1","Heartbeat",{}]').decode(),
            }
        ]
    }


def test_a_connect_event_carries_no_frame() -> None:
    envelope = ocpp_envelope(event_type=OCPPEventType.CONNECT, direction=None, payload=None)
    record = json.loads(serialize([envelope]))["messages"][0]
    assert record["event_type"] == 1
    assert record["direction"] is None
    assert record["raw_frame"] is None


def test_absent_values_serialize_as_null() -> None:
    envelope = ocpi_envelope(request_body=None, response_body=None, status_code=None)
    record = json.loads(serialize([envelope]))["messages"][0]
    assert record["request_body"] is None
    assert record["response_body"] is None
    assert record["response_status_code"] is None
    assert record["response_headers"] is None


def test_a_zero_status_is_sent_as_null() -> None:
    record = json.loads(serialize([ocpi_envelope(status_code=0)]))["messages"][0]
    assert record["response_status_code"] is None


def test_small_payloads_are_not_compressed() -> None:
    raw = b"x" * (COMPRESS_MIN_BYTES - 1)
    assert compress(raw) == (raw, "identity")


def test_large_payloads_are_zstd_compressed() -> None:
    raw = b'{"messages":[]}' * 500
    body, encoding = compress(raw)
    assert encoding == "zstd"
    assert len(body) < len(raw)
    assert zstandard.decompress(body) == raw


def test_backoff_stays_inside_its_bounds() -> None:
    for attempt in range(BACKOFF_MAX_ATTEMPTS):
        delay = next_delay(attempt)
        assert 0 <= delay <= min(BACKOFF_MAX, BACKOFF_BASE * 2**attempt)


def transport_for(ingest: IngestServer, **overrides: Any) -> tuple[Transport, Counters]:
    resolved = resolve_ocpp_config(
        evpanda.OCPPConfig(endpoint=ingest.url, api_key="test-key", **overrides)
    )
    counters = Counters()
    return Transport(resolved, counters), counters


def test_a_batch_is_posted_with_its_api_key(ingest: IngestServer) -> None:
    transport, _ = transport_for(ingest)
    transport.send(Protocol.OCPP, [ocpp_envelope()])

    assert len(ingest.received) == 1
    assert ingest.received[0].path == "/v1/ocpp"
    assert ingest.received[0].headers["x-api-key"] == "test-key"
    assert ingest.received[0].headers["content-type"] == "application/json"


def test_a_transient_failure_is_retried(ingest: IngestServer, fast_backoff: None) -> None:
    ingest.statuses = [500, 503]
    transport, counters = transport_for(ingest)

    transport.send(Protocol.OCPP, [ocpp_envelope()])

    assert len(ingest.received) == 1
    assert counters.snapshot().dropped_undeliverable == 0


@pytest.mark.parametrize("status", [400, 401, 413])
def test_a_permanent_rejection_is_never_retried(
    ingest: IngestServer, status: int, logs: pytest.LogCaptureFixture, fast_backoff: None
) -> None:
    ingest.statuses = [status, 200]
    transport, counters = transport_for(ingest, log_mode=LogMode.DEBUG)

    transport.send(Protocol.OCPP, [ocpp_envelope(), ocpp_envelope()])

    assert ingest.received == []  # it gave up on the first answer
    assert counters.snapshot().dropped_undeliverable == 2
    assert any("permanent rejection" in r.getMessage() for r in logs.records)


def test_retries_are_bounded_and_the_batch_is_dropped(
    ingest: IngestServer, fast_backoff: None
) -> None:
    ingest.statuses = [500] * (BACKOFF_MAX_ATTEMPTS + 2)
    transport, counters = transport_for(ingest)

    transport.send(Protocol.OCPP, [ocpp_envelope()], deadline=time.monotonic() + 2)

    assert ingest.received == []
    assert counters.snapshot().dropped_undeliverable == 1


def test_a_deadline_stops_the_retry_loop(ingest: IngestServer) -> None:
    ingest.statuses = [500] * 10
    transport, counters = transport_for(ingest)

    started = time.monotonic()
    transport.send(Protocol.OCPP, [ocpp_envelope()], deadline=started + 0.2)
    elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert counters.snapshot().dropped_undeliverable == 1


def test_an_unreachable_endpoint_is_never_raised(fast_backoff: None) -> None:
    resolved = resolve_ocpi_config(evpanda.OCPIConfig(endpoint="http://127.0.0.1:1", api_key="k"))
    counters = Counters()
    transport = Transport(resolved, counters)

    transport.send(Protocol.OCPI, [ocpi_envelope()], deadline=time.monotonic() + 5)

    assert counters.snapshot().dropped_undeliverable == 1


def test_an_empty_batch_does_nothing(ingest: IngestServer) -> None:
    transport, counters = transport_for(ingest)
    transport.send(Protocol.OCPP, [])
    assert ingest.received == []
    assert counters.snapshot().total_dropped == 0
