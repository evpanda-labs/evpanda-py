"""Client lifecycle: inert clients, guards, sessions, close."""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any

import pytest

import evpanda
from conftest import CHARGER, PARTNER, IngestServer, exchange, ocpi_client, ocpp_client
from evpanda._client import _live
from evpanda._config import LogMode

# ── Inert clients ────────────────────────────────────────────────────────


def test_a_missing_api_key_yields_an_inert_client(logs: pytest.LogCaptureFixture) -> None:
    panda = evpanda.start_ocpi()

    assert isinstance(panda.error, evpanda.APIKeyError)
    assert panda.capturing() is None
    assert any("inert" in r.getMessage() for r in logs.records)


def test_a_malformed_endpoint_yields_an_inert_client() -> None:
    panda = evpanda.start_ocpp(evpanda.OCPPConfig(api_key="k", endpoint="nope"))
    assert isinstance(panda.error, evpanda.EndpointError)


def test_an_inert_client_captures_nothing_and_never_raises() -> None:
    panda = evpanda.start_ocpi()

    panda.capture_inbound_message(PARTNER, exchange())
    panda.capture_outbound_message(PARTNER, exchange())
    panda.flush()

    assert panda.stats() == evpanda.Stats()
    assert panda.close() is True


def test_an_inert_ocpp_client_still_hands_back_a_session() -> None:
    panda = evpanda.start_ocpp()
    session = panda.connection(CHARGER)
    session.message(b'[2,"1","Heartbeat",{}]', evpanda.OCPPDirection.FROM_CP)
    session.disconnect()
    assert panda.stats().captured == 0


def test_an_api_key_from_the_environment_starts_a_live_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(evpanda.API_KEY_ENV_VAR, "from-env")
    panda = evpanda.start_ocpp(evpanda.OCPPConfig(endpoint="http://127.0.0.1:1"))
    try:
        assert panda.error is None
        assert panda.capturing() == 64 * 1024
    finally:
        panda.close(timeout=5)


# ── Live clients ─────────────────────────────────────────────────────────


def test_capture_buffers_a_message(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(PARTNER, exchange())
        assert panda.stats().captured == 1
        assert panda.stats().buffered_messages == 1
    finally:
        panda.close(timeout=5)


def test_an_invalid_identity_is_counted_not_raised(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(evpanda.Platform(id="", name=""), exchange())
        assert panda.stats().dropped_invalid == 1
    finally:
        panda.close(timeout=5)


def test_close_stops_capture(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest)
    panda.capture_inbound_message(PARTNER, exchange())
    assert panda.close(timeout=5) is True

    panda.capture_inbound_message(PARTNER, exchange())

    assert panda.capturing() is None
    assert panda.stats().captured == 1  # the counters survive close
    assert panda.close() is True  # idempotent


def test_a_client_is_a_context_manager(ingest: IngestServer) -> None:
    with ocpi_client(ingest) as panda:
        panda.capture_inbound_message(PARTNER, exchange())
    assert panda.capturing() is None
    assert ingest.wait_for_messages(1)


def test_a_capture_fault_is_swallowed_and_counted(
    ingest: IngestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    panda = ocpi_client(ingest, log_mode=LogMode.DEBUG)
    try:

        def explode(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(panda._current, "capture_ocpi", explode)
        panda.capture_inbound_message(PARTNER, exchange())

        assert panda.stats().dropped_fault == 1
    finally:
        panda.close(timeout=5)


def test_a_capture_fault_is_quiet_outside_debug_mode(
    ingest: IngestServer, monkeypatch: pytest.MonkeyPatch, logs: pytest.LogCaptureFixture
) -> None:
    panda = ocpi_client(ingest)
    try:

        def explode(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(panda._current, "capture_ocpi", explode)
        panda.capture_inbound_message(PARTNER, exchange())

        assert panda.stats().dropped_fault == 1
        assert not [r for r in logs.records if "capture failed" in r.getMessage()]
    finally:
        panda.close(timeout=5)


def test_a_capture_fault_is_logged_in_debug_mode(
    ingest: IngestServer, monkeypatch: pytest.MonkeyPatch, logs: pytest.LogCaptureFixture
) -> None:
    panda = ocpi_client(ingest, log_mode=LogMode.DEBUG)
    try:

        def explode(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(panda._current, "capture_ocpi", explode)
        panda.capture_inbound_message(PARTNER, exchange())

        assert any("capture failed" in r.getMessage() for r in logs.records)
        assert any(r.levelno == logging.WARNING for r in logs.records)
    finally:
        panda.close(timeout=5)


# ── OCPP sessions ────────────────────────────────────────────────────────


def test_a_session_carries_the_connection_across_a_socket(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        with panda.connection(CHARGER) as session:
            session.message(b'[2,"1","Heartbeat",{}]', evpanda.OCPPDirection.FROM_CP)
            session.message(b'[3,"1",{}]', evpanda.OCPPDirection.TO_CP)
        panda.flush()
    finally:
        panda.close(timeout=5)

    messages = ingest.wait_for_messages(4)
    assert [m["event_type"] for m in messages] == [1, 2, 2, 0]
    assert len({m["connection_id"] for m in messages}) == 1
    uuid.UUID(messages[0]["connection_id"])  # a real UUID, not a counter


def test_each_connection_gets_a_fresh_id(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        first = panda.connection(CHARGER)
        second = panda.connection(CHARGER)
        assert first.connection_id != second.connection_id
    finally:
        panda.close(timeout=5)


def test_a_frame_without_a_direction_is_dropped(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_message(CHARGER, "c-1", b"frame", None)  # type: ignore[arg-type]
        panda.capture_message(CHARGER, "c-1", b"", evpanda.OCPPDirection.TO_CP)
        assert panda.stats().captured == 0
        assert panda.stats().dropped_oversize == 2
    finally:
        panda.close(timeout=5)


# ── Process lifecycle ────────────────────────────────────────────────────


def test_a_live_client_is_tracked_for_shutdown(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest)
    try:
        assert panda in set(_live)
    finally:
        panda.close(timeout=5)


def test_after_fork_restarts_the_worker(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(PARTNER, exchange())
        assert panda.stats().buffered_messages == 1

        panda._after_fork()

        # The parent's captures stay with the parent, and the child counts
        # from zero — but the client is live again.
        assert panda.stats() == evpanda.Stats()
        panda.capture_inbound_message(PARTNER, exchange())
        assert panda.stats().captured == 1
        names = {t.name for t in threading.enumerate()}
        assert "evpanda-worker" in names
    finally:
        panda.close(timeout=5)


def test_no_worker_thread_survives_close(ingest: IngestServer) -> None:
    before = sum(t.name == "evpanda-worker" for t in threading.enumerate())
    panda = ocpi_client(ingest)
    panda.capture_inbound_message(PARTNER, exchange())
    panda.close(timeout=5)

    after = sum(t.name == "evpanda-worker" for t in threading.enumerate())
    assert after == before
