"""The capture chokepoint and the worker's own reporting."""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest

import evpanda
from evpanda._buffer import RingBuffer
from evpanda._config import LogMode, resolve_ocpi_config, resolve_ocpp_config
from evpanda._redact import make_ocpi_redactor
from evpanda._stats import Counters, DropReason, Stats
from evpanda._types import (
    Charger,
    HTTPExchange,
    OCPIDirection,
    OCPIMessage,
    OCPPDirection,
    OCPPEventType,
    OCPPMessage,
    Platform,
)
from evpanda._worker import BATCH_CAP, Worker, header_map, prepare_ocpi, prepare_ocpp

PARTNER = Platform(id="acme", name="Acme")
CHARGER = Charger(id="CP-001")
CAP = 1024


class StubTransport:
    """Records what the worker hands it instead of making a request."""

    def __init__(self) -> None:
        self.batches: list[list[Any]] = []

    def send(self, protocol: Any, batch: list[Any], deadline: float | None = None) -> None:
        self.batches.append(batch)


def build_worker(**overrides: Any) -> tuple[Worker, StubTransport, Counters]:
    resolved = resolve_ocpp_config(evpanda.OCPPConfig(api_key="k", **overrides))
    counters = Counters()
    transport = StubTransport()
    worker = Worker(
        RingBuffer(resolved.max_buffer_bytes, counters),
        transport,  # type: ignore[arg-type]
        resolved,
        counters,
    )
    return worker, transport, counters


def ocpi(**overrides: Any) -> OCPIMessage:
    data = HTTPExchange(method="POST", url="/ocpi/2.2/cdrs", **overrides)
    return OCPIMessage(direction=OCPIDirection.IN, identity=PARTNER, data=data)


# ── OCPI chokepoint ──────────────────────────────────────────────────────


def test_prepare_ocpi_accepts_a_good_message() -> None:
    envelope, reason, _ = prepare_ocpi(ocpi(), None, CAP)
    assert reason is DropReason.NONE
    assert envelope is not None
    assert envelope.captured_at.endswith("Z")


@pytest.mark.parametrize(
    "identity",
    [
        Platform(id="", name="Acme"),
        Platform(id="acme", name="   "),
        Platform(id="acme", name="Acme", tenant_id="t"),
        Platform(id="acme", name="Acme", tenant_name="T"),
    ],
)
def test_prepare_ocpi_drops_an_invalid_identity(identity: Platform) -> None:
    message = ocpi()
    message.identity = identity
    envelope, reason, _ = prepare_ocpi(message, None, CAP)
    assert envelope is None
    assert reason is DropReason.INVALID_IDENTITY


def test_prepare_ocpi_drops_an_oversize_body() -> None:
    for field in ("request_body", "response_body"):
        envelope, reason, _ = prepare_ocpi(ocpi(**{field: b"x" * (CAP + 1)}), None, CAP)
        assert envelope is None
        assert reason is DropReason.OVERSIZE


def test_an_absent_identity_is_invalid_not_a_fault() -> None:
    """Type hints are not enforced at runtime. A host that passes None has
    given us a message we cannot attribute — that is an invalid identity,
    not a bug in the SDK, and it must not land in the fault counter.
    """
    message = ocpi()
    message.identity = None  # type: ignore[assignment]
    assert prepare_ocpi(message, None, CAP) == (None, DropReason.INVALID_IDENTITY, 0)

    frame = ocpp()
    frame.identity = None  # type: ignore[assignment]
    assert prepare_ocpp(frame, None, CAP) == (None, DropReason.INVALID_IDENTITY, 0)


def test_a_tenant_pair_is_all_or_nothing() -> None:
    both = Platform(id="acme", name="Acme", tenant_id="t", tenant_name="T")
    assert both.valid()
    assert Charger(id="CP", tenant_id="t", tenant_name="T").valid()
    assert not Charger(id="CP", tenant_id="t").valid()
    assert not Charger(id=" ").valid()


def test_the_chokepoint_takes_ownership() -> None:
    """What the host mutates after the call cannot reach the buffer."""
    headers = {"content-type": "application/json"}
    body = bytearray(b'{"id":"cdr-1"}')
    message = ocpi(request_headers=headers, request_body=body)

    envelope, _, _ = prepare_ocpi(message, None, CAP)
    assert envelope is not None
    headers["content-type"] = "text/plain"
    headers["authorization"] = "Token secret"
    body.extend(b"tampered")

    captured = envelope.message.data
    assert captured.request_headers == {"content-type": "application/json"}
    assert captured.request_body == b'{"id":"cdr-1"}'


def test_a_string_body_is_encoded_as_utf8() -> None:
    envelope, _, _ = prepare_ocpi(ocpi(request_body="héllo"), None, CAP)
    assert envelope is not None
    assert envelope.message.data.request_body == "héllo".encode()


def test_prepare_runs_the_redactor() -> None:
    message = ocpi(request_headers={"authorization": "Token secret", "accept": "*/*"})
    envelope, _, _ = prepare_ocpi(message, make_ocpi_redactor(), CAP)
    assert envelope is not None
    assert envelope.message.data.request_headers == {"accept": "*/*"}


def test_prepare_skips_a_missing_redactor() -> None:
    message = ocpi(request_headers={"authorization": "Token secret"})
    envelope, _, _ = prepare_ocpi(message, None, CAP)
    assert envelope is not None
    assert envelope.message.data.request_headers == {"authorization": "Token secret"}


def test_a_redactor_may_rewrite_in_place() -> None:
    def redact(message: OCPIMessage) -> OCPIMessage:
        message.data.url = "/redacted"
        return message

    envelope, _, _ = prepare_ocpi(ocpi(), redact, CAP)
    assert envelope is not None
    assert envelope.message.data.url == "/redacted"


def test_header_map_stringifies() -> None:
    assert header_map(None) == {}
    assert header_map({"X-Limit": 10}) == {"X-Limit": "10"}


# ── OCPP chokepoint ──────────────────────────────────────────────────────


def ocpp(**overrides: Any) -> OCPPMessage:
    fields: dict[str, Any] = {
        "event_type": OCPPEventType.MESSAGE,
        "identity": CHARGER,
        "connection_id": "c-1",
        "direction": OCPPDirection.FROM_CP,
        "payload": b'[2,"1","Heartbeat",{}]',
    }
    fields.update(overrides)
    return OCPPMessage(**fields)


def test_an_ocpi_body_that_is_not_utf8_is_dropped_but_the_exchange_survives() -> None:
    """A body the wire contract cannot carry as text goes, and the exchange
    around it stays: method, URL, status and headers are still worth having.
    """
    message = ocpi(request_body=b"\xff\xfe\x00\x01", response_body=b'{"status_code":1000}')
    envelope, reason, bodies_dropped = prepare_ocpi(message, None, CAP)

    assert reason is DropReason.NONE
    assert bodies_dropped == 1
    assert envelope is not None
    assert envelope.message.data.request_body is None
    # The good half is untouched.
    assert envelope.message.data.response_body == b'{"status_code":1000}'
    assert envelope.message.data.url == "/ocpi/2.2/cdrs"


def test_an_ocpp_frame_that_is_not_utf8_takes_the_message_with_it() -> None:
    """``event_type`` 2 requires a frame, so there is no message left to
    ship. It is counted twice: as the body, and as the message.
    """
    envelope, reason, bodies_dropped = prepare_ocpp(ocpp(payload=b"\xff\xfe\x00\x01"), None, CAP)

    assert envelope is None
    assert reason is DropReason.OVERSIZE
    assert bodies_dropped == 1


def test_the_counters_see_a_dropped_body() -> None:
    """The whole point of the counter: an operator can tell that payloads
    are being omitted without reading the wire.
    """
    worker, _, counters = build_worker()
    worker.capture_ocpi(ocpi(request_body=b"\xff\xfe"), None)

    stats = counters.snapshot()
    assert stats.bodies_dropped == 1
    assert stats.captured == 1  # the exchange still shipped
    assert stats.total_dropped == 0  # no message was lost


def test_prepare_ocpp_accepts_a_good_frame() -> None:
    envelope, reason, _ = prepare_ocpp(ocpp(), None, CAP)
    assert reason is DropReason.NONE
    assert envelope is not None


def test_prepare_ocpp_drops_an_invalid_identity() -> None:
    envelope, reason, _ = prepare_ocpp(ocpp(identity=Charger(id="")), None, CAP)
    assert envelope is None
    assert reason is DropReason.INVALID_IDENTITY


def test_prepare_ocpp_drops_an_oversize_frame() -> None:
    envelope, reason, _ = prepare_ocpp(ocpp(payload=b"x" * (CAP + 1)), None, CAP)
    assert envelope is None
    assert reason is DropReason.OVERSIZE


@pytest.mark.parametrize("missing", [{"payload": None}, {"direction": None}])
def test_a_message_event_needs_a_frame_and_a_direction(missing: dict[str, Any]) -> None:
    envelope, reason, _ = prepare_ocpp(ocpp(**missing), None, CAP)
    assert envelope is None
    assert reason is DropReason.OVERSIZE


def test_connect_and_disconnect_carry_no_frame() -> None:
    for event in (OCPPEventType.CONNECT, OCPPEventType.DISCONNECT):
        envelope, reason, _ = prepare_ocpp(
            ocpp(event_type=event, payload=None, direction=None), None, CAP
        )
        assert reason is DropReason.NONE
        assert envelope is not None


def test_ocpp_frames_may_be_strings() -> None:
    envelope, _, _ = prepare_ocpp(ocpp(payload='[2,"1","Heartbeat",{}]'), None, CAP)
    assert envelope is not None
    assert envelope.message.payload == b'[2,"1","Heartbeat",{}]'


def test_the_ocpp_redactor_seam_is_optional() -> None:
    def redact(message: OCPPMessage) -> OCPPMessage:
        message.payload = b"masked"
        return message

    envelope, _, _ = prepare_ocpp(ocpp(), redact, CAP)
    assert envelope is not None
    assert envelope.message.payload == b"masked"


# ── Worker behaviour ─────────────────────────────────────────────────────


def test_a_full_batch_triggers_a_flush() -> None:
    worker, transport, _ = build_worker(flush_interval=3600.0)
    worker.start()
    try:
        for _ in range(BATCH_CAP):
            worker.capture_ocpp(ocpp())
        deadline = time.monotonic() + 5
        while not transport.batches and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        worker.close(timeout=5)

    assert sum(len(batch) for batch in transport.batches) == BATCH_CAP


def test_a_flush_chunks_at_the_batch_cap() -> None:
    worker, transport, _ = build_worker(flush_interval=3600.0)
    for _ in range(BATCH_CAP + 7):
        worker.capture_ocpp(ocpp())

    worker.flush_once()

    assert [len(batch) for batch in transport.batches] == [BATCH_CAP, 7]


def test_capture_counts_its_drops() -> None:
    worker, _, counters = build_worker()
    worker.capture_ocpp(ocpp(identity=Charger(id="")))
    worker.capture_ocpp(ocpp(payload=b"x" * (64 * 1024 + 1)))

    stats = counters.snapshot()
    assert stats.dropped_invalid == 1
    assert stats.dropped_oversize == 1


def test_close_is_idempotent_and_drains() -> None:
    worker, transport, _ = build_worker(flush_interval=3600.0)
    worker.start()
    worker.capture_ocpp(ocpp())

    assert worker.close(timeout=5) is True
    assert worker.close(timeout=5) is True
    assert sum(len(batch) for batch in transport.batches) == 1


def test_health_is_reported_once_per_window(logs: pytest.LogCaptureFixture) -> None:
    worker, _, counters = build_worker()

    worker._report_health()  # nothing dropped yet — silent
    assert logs.records == []

    counters.count_drop(DropReason.EVICTED, 4)
    worker._report_health()
    assert len(logs.records) == 1
    assert "captures dropped" in logs.records[0].getMessage()
    assert "evicted=4" in logs.records[0].getMessage()

    worker._report_health()  # the window's damage was already reported
    assert len(logs.records) == 1


def test_health_reporting_is_silent_in_silent_mode(logs: pytest.LogCaptureFixture) -> None:
    worker, _, counters = build_worker(log_mode=LogMode.SILENT)
    counters.count_drop(DropReason.EVICTED, 9)

    worker._report_health()
    worker._report_shutdown(drained=True)

    assert logs.records == []


def test_shutdown_reports_by_mode(logs: pytest.LogCaptureFixture) -> None:
    clean, _, _ = build_worker()
    clean._report_shutdown(drained=True)
    assert logs.records == []  # a clean run leaves no trace

    debug, _, _ = build_worker(log_mode=LogMode.DEBUG)
    debug._report_shutdown(drained=True)
    assert logs.records[-1].levelno == logging.INFO

    lossy, _, counters = build_worker()
    counters.count_drop(DropReason.UNDELIVERABLE, 2)
    lossy._report_shutdown(drained=False)
    assert logs.records[-1].levelno == logging.WARNING
    assert "drain=incomplete" in logs.records[-1].getMessage()


def test_an_exception_in_the_loop_is_counted_and_logged(
    logs: pytest.LogCaptureFixture,
) -> None:
    worker, _, counters = build_worker()

    def explode() -> None:
        raise RuntimeError("boom")

    worker._run = explode  # type: ignore[method-assign]
    worker._loop()

    assert counters.snapshot().dropped_fault == 1
    assert any(r.levelno == logging.ERROR for r in logs.records)


def test_snapshot_includes_the_buffer_gauges() -> None:
    worker, _, _ = build_worker()
    worker.capture_ocpp(ocpp())
    snapshot = worker.snapshot()
    assert snapshot.buffered_messages == 1
    assert snapshot.buffer_bytes > 0
    assert isinstance(snapshot, Stats)


def test_an_ocpi_worker_carries_its_allowlist() -> None:
    resolved = resolve_ocpi_config(
        evpanda.OCPIConfig(api_key="k", ocpi_allowed_headers=["x-trace"])
    )
    assert resolved.allowed_headers == ("x-trace",)
