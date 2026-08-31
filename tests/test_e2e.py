"""End to end: capture on one side, decoded records on the other."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from typing import Any

import pytest

import evpanda
from conftest import CHARGER, PARTNER, IngestServer, exchange, ocpi_client, ocpp_client
from evpanda._worker import BATCH_CAP


def test_an_inbound_ocpi_exchange_arrives_intact(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(
            evpanda.Platform(
                id="acme",
                name="Acme Mobility",
                tenant_id="t-1",
                tenant_name="Tenant One",
            ),
            exchange(),
        )
        panda.flush()
    finally:
        panda.close(timeout=5)

    record = ingest.wait_for_messages(1)[0]
    assert ingest.received[0].path == "/v1/ocpi"
    assert record["direction"] == "IN"
    assert record["platform_id"] == "acme"
    assert record["tenant_name"] == "Tenant One"
    assert record["http_method"] == "POST"
    assert record["url"] == "/ocpi/2.2/cdrs"
    assert record["response_status_code"] == 201
    assert record["request_body"] == '{"id":"cdr-1"}'
    assert record["request_body_encoding"] == "utf8"
    assert record["captured_at"].endswith("Z")


def test_the_method_stamps_the_direction(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(PARTNER, exchange())
        panda.capture_outbound_message(PARTNER, exchange())
        panda.flush()
    finally:
        panda.close(timeout=5)

    assert [m["direction"] for m in ingest.wait_for_messages(2)] == ["IN", "OUT"]


def test_secrets_never_reach_the_wire(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_outbound_message(
            PARTNER,
            exchange(
                url="/ocpi/2.2/credentials",
                request_headers={"Authorization": "Token super-secret", "accept": "*/*"},
                request_body=json.dumps({"token": "another-secret"}).encode(),
            ),
        )
        panda.flush()
    finally:
        panda.close(timeout=5)

    record = ingest.wait_for_messages(1)[0]
    assert record["request_headers"] == {"accept": "*/*"}
    body = json.loads(record["request_body"])
    assert body == {"token": "[redacted]"}


def test_an_ocpp_session_arrives_as_three_events(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        session = panda.connection(CHARGER)
        session.message('[2,"1","Heartbeat",{}]', evpanda.OCPPDirection.FROM_CP)
        session.disconnect()
        panda.flush()
    finally:
        panda.close(timeout=5)

    messages = ingest.wait_for_messages(3)
    assert [m["event_type"] for m in messages] == [1, 2, 0]
    assert messages[1]["direction"] == "FROM_CP"
    assert messages[1]["raw_frame"] == '[2,"1","Heartbeat",{}]'
    assert messages[1]["raw_frame_encoding"] == "utf8"
    assert messages[0]["raw_frame_encoding"] is None
    assert messages[0]["raw_frame"] is None
    assert messages[0]["charger_id"] == "CP-001"


def test_a_full_batch_flushes_without_waiting(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        session = panda.connection(CHARGER)
        for _ in range(BATCH_CAP):
            session.message(b'[2,"1","Heartbeat",{}]', evpanda.OCPPDirection.FROM_CP)
        # No flush() call: the size trigger alone must deliver it.
        assert len(ingest.wait_for_messages(BATCH_CAP)) >= BATCH_CAP
    finally:
        panda.close(timeout=5)


def test_a_large_backlog_is_chunked(ingest: IngestServer) -> None:
    panda = ocpp_client(ingest, flush_interval=3600.0)
    try:
        session = panda.connection(CHARGER)
        for _ in range(BATCH_CAP + 500):
            session.message(b'[2,"1","Heartbeat",{}]', evpanda.OCPPDirection.FROM_CP)
        panda.flush()
    finally:
        panda.close(timeout=5)

    ingest.wait_for_messages(BATCH_CAP + 501)
    assert all(len(request.messages) <= BATCH_CAP for request in ingest.received)


def test_the_interval_flushes_on_its_own(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=0.05)
    try:
        panda.capture_inbound_message(PARTNER, exchange())
        assert len(ingest.wait_for_messages(1)) == 1
    finally:
        panda.close(timeout=5)


def test_close_delivers_what_was_buffered(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    panda.capture_inbound_message(PARTNER, exchange())

    assert panda.close(timeout=5) is True

    assert len(ingest.messages) == 1


def test_the_buffer_evicts_rather_than_growing(ingest: IngestServer) -> None:
    panda = ocpi_client(
        ingest, flush_interval=3600.0, max_buffer_bytes=64 << 10, max_capture_bytes=1024
    )
    try:
        for _ in range(500):
            panda.capture_inbound_message(PARTNER, exchange(request_body=b"x" * 512))

        stats = panda.stats()
        assert stats.buffer_bytes <= 64 << 10
        assert stats.dropped_evicted > 0
    finally:
        panda.close(timeout=5)


def test_an_oversize_body_drops_only_that_message(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0, max_capture_bytes=64)
    try:
        panda.capture_inbound_message(PARTNER, exchange(request_body=b"x" * 65))
        panda.capture_inbound_message(PARTNER, exchange(request_body=b"x" * 8))
        panda.flush()
    finally:
        panda.close(timeout=5)

    assert len(ingest.wait_for_messages(1)) == 1
    assert panda.stats().dropped_oversize == 1


def test_a_dead_upstream_never_reaches_the_host(fast_backoff: None) -> None:
    panda = evpanda.start_ocpi(
        evpanda.OCPIConfig(
            endpoint="http://127.0.0.1:1", api_key="k", flush_interval=0.02, drain_timeout=5.0
        )
    )
    try:
        for _ in range(50):
            panda.capture_inbound_message(PARTNER, exchange())
        panda.flush()
    finally:
        assert panda.close(timeout=5) is True

    assert panda.stats().dropped_undeliverable > 0


def test_capture_flush_and_close_race_safely(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=0.01)
    errors: list[BaseException] = []

    def capture() -> None:
        try:
            for _ in range(200):
                panda.capture_inbound_message(PARTNER, exchange())
        except BaseException as exc:  # noqa: BLE001 - the point of the test
            errors.append(exc)

    def flush() -> None:
        try:
            for _ in range(20):
                panda.flush()
        except BaseException as exc:  # noqa: BLE001 - the point of the test
            errors.append(exc)

    threads = [threading.Thread(target=capture) for _ in range(4)]
    threads.append(threading.Thread(target=flush))
    for thread in threads:
        thread.start()
    time.sleep(0.02)
    panda.close(timeout=5)
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert panda.stats().captured == 800


def test_the_body_is_zstd_compressed_above_the_floor(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        for _ in range(50):
            panda.capture_inbound_message(PARTNER, exchange())
        panda.flush()
    finally:
        panda.close(timeout=5)

    assert ingest.received[0].headers.get("content-encoding") == "zstd"


def test_a_small_body_goes_out_uncompressed(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    try:
        panda.capture_inbound_message(PARTNER, exchange(request_body=None, response_body=None))
        panda.flush()
    finally:
        panda.close(timeout=5)

    assert "content-encoding" not in ingest.received[0].headers


@pytest.mark.parametrize("log_mode", ["silent", "errors", "debug"])
def test_every_log_mode_still_delivers(ingest: IngestServer, log_mode: str) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0, log_mode=log_mode)
    try:
        panda.capture_inbound_message(PARTNER, exchange())
        panda.flush()
    finally:
        panda.close(timeout=5)

    assert len(ingest.wait_for_messages(1)) == 1


def test_an_unclosed_client_still_drains_at_exit(ingest: IngestServer) -> None:
    """A daemon thread would otherwise take the last captures down with it."""
    script = f"""
import evpanda

panda = evpanda.start_ocpi(evpanda.OCPIConfig(
    endpoint={ingest.url!r}, api_key="k", flush_interval=3600.0,
))
panda.capture_inbound_message(
    evpanda.Platform(id="acme", name="Acme Mobility"),
    evpanda.HTTPExchange(method="GET", url="/ocpi/2.2/versions", status_code=200),
)
# and then the process simply ends, without close()
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, timeout=60, check=False
    )

    assert result.returncode == 0, result.stderr.decode()
    assert [m["url"] for m in ingest.wait_for_messages(1)] == ["/ocpi/2.2/versions"]


def test_stats_survive_the_client(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    panda.capture_inbound_message(PARTNER, exchange())
    panda.capture_inbound_message(evpanda.Platform(id="", name=""), exchange())
    panda.close(timeout=5)

    final: Any = panda.stats()
    assert final.captured == 1
    assert final.dropped_invalid == 1
    assert final.buffered_messages == 0
