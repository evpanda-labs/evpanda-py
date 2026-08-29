"""The byte-bounded, drop-oldest queue."""

from __future__ import annotations

import threading

import pytest

from evpanda._buffer import BufferedMessage, RingBuffer, now_iso
from evpanda._stats import Counters, DropReason
from evpanda._types import (
    ChargerIdentity,
    HTTPExchange,
    OCPIDirection,
    OCPIMessage,
    OCPPDirection,
    OCPPEventType,
    OCPPMessage,
    RoamingIdentity,
)

IDENTITY = ChargerIdentity(charger_id="CP-001")


def frame(payload: bytes, connection_id: str = "c-1") -> BufferedMessage:
    return BufferedMessage(
        captured_at=now_iso(),
        message=OCPPMessage(
            event_type=OCPPEventType.MESSAGE,
            identity=IDENTITY,
            connection_id=connection_id,
            direction=OCPPDirection.FROM_CP,
            payload=payload,
        ),
    )


def test_rejects_a_non_positive_budget() -> None:
    for bad in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="positive integer"):
            RingBuffer(bad, Counters())  # type: ignore[arg-type]


def test_drains_fifo() -> None:
    buffer = RingBuffer(1 << 20, Counters())
    for i in range(5):
        buffer.enqueue(frame(f"{i}".encode()))

    drained = buffer.drain()

    assert [e.message.payload for e in drained] == [b"0", b"1", b"2", b"3", b"4"]
    assert len(buffer) == 0
    assert buffer.byte_len == 0
    assert buffer.drain() == []


def test_evicts_by_bytes_not_count() -> None:
    """Ten small frames fit where two large ones do not."""
    counters = Counters()
    buffer = RingBuffer(4096, counters)

    for _ in range(10):
        buffer.enqueue(frame(b"x" * 8))
    assert len(buffer) == 10

    buffer.enqueue(frame(b"y" * 3000))
    # The big one forced older frames out rather than a fixed slot count.
    assert buffer.byte_len <= 4096
    assert counters.snapshot().dropped_evicted > 0


def test_drops_oldest_first() -> None:
    buffer = RingBuffer(1200, Counters())  # room for ~3 envelopes of 256 + payload
    for i in range(6):
        buffer.enqueue(frame(f"{i}".encode()))

    payloads = [e.message.payload for e in buffer.drain()]

    assert payloads[-1] == b"5"
    assert b"0" not in payloads


def test_never_exceeds_the_budget() -> None:
    buffer = RingBuffer(8192, Counters())
    for i in range(500):
        buffer.enqueue(frame(b"z" * (i % 64)))
        assert buffer.byte_len <= 8192


def test_drops_a_message_larger_than_the_whole_budget() -> None:
    counters = Counters()
    buffer = RingBuffer(64 << 10, counters)
    buffer.enqueue(frame(b"a" * 128))

    buffer.enqueue(frame(b"b" * (64 << 10)))

    # The oversize one is dropped outright — it never empties the buffer for
    # something that would not fit anyway.
    assert len(buffer) == 1
    assert counters.snapshot().dropped_oversize == 1


def test_counts_what_it_accepts() -> None:
    counters = Counters()
    buffer = RingBuffer(1 << 20, counters)
    for _ in range(3):
        buffer.enqueue(frame(b"x"))
    assert counters.snapshot().captured == 3


def test_byte_accounting_matches_the_message() -> None:
    buffer = RingBuffer(1 << 20, Counters())
    envelope = frame(b"x" * 100)
    buffer.enqueue(envelope)

    assert envelope.size == envelope.message.size()
    assert buffer.byte_len == envelope.size


def test_a_connect_event_is_cheaper_than_a_message() -> None:
    connect = OCPPMessage(event_type=OCPPEventType.CONNECT, identity=IDENTITY, connection_id="c-1")
    message = OCPPMessage(
        event_type=OCPPEventType.MESSAGE,
        identity=IDENTITY,
        connection_id="c-1",
        direction=OCPPDirection.FROM_CP,
        payload=b'[2,"1","Heartbeat",{}]',
    )
    assert connect.size() < message.size()


def test_ocpi_accounting_charges_bodies_and_headers() -> None:
    small = OCPIMessage(
        direction=OCPIDirection.IN,
        identity=RoamingIdentity(platform_id="a", platform_name="b"),
        data=HTTPExchange(method="GET", url="/x"),
    )
    large = OCPIMessage(
        direction=OCPIDirection.IN,
        identity=RoamingIdentity(platform_id="a", platform_name="b"),
        data=HTTPExchange(
            method="GET",
            url="/x",
            request_headers={"content-type": "application/json"},
            request_body=b"y" * 500,
        ),
    )
    assert large.size() > small.size() + 500


def test_concurrent_producers_and_drains() -> None:
    counters = Counters()
    buffer = RingBuffer(1 << 20, counters)
    stop = threading.Event()
    drained: list[int] = []

    def produce() -> None:
        for _ in range(500):
            buffer.enqueue(frame(b"payload"))

    def drain() -> None:
        while not stop.is_set():
            drained.append(len(buffer.drain()))

    producers = [threading.Thread(target=produce) for _ in range(4)]
    consumer = threading.Thread(target=drain)
    consumer.start()
    for thread in producers:
        thread.start()
    for thread in producers:
        thread.join()
    stop.set()
    consumer.join()
    drained.append(len(buffer.drain()))

    assert sum(drained) == 2000
    assert counters.snapshot().captured == 2000


def test_after_fork_starts_empty() -> None:
    counters = Counters()
    buffer = RingBuffer(1 << 20, counters)
    buffer.enqueue(frame(b"parent"))

    buffer.after_fork()

    assert len(buffer) == 0
    assert buffer.byte_len == 0
    assert buffer.drain() == []


def test_drain_releases_its_references() -> None:
    buffer = RingBuffer(1 << 20, Counters())
    buffer.enqueue(frame(b"x"))
    buffer.drain()
    # A second drain sees nothing, so the queue is not holding the batch.
    assert buffer.drain() == []
    assert len(buffer) == 0


def test_counters_count_each_drop_reason() -> None:
    counters = Counters()
    for reason in (
        DropReason.INVALID_IDENTITY,
        DropReason.OVERSIZE,
        DropReason.EVICTED,
        DropReason.UNDELIVERABLE,
        DropReason.FAULT,
    ):
        counters.count_drop(reason)
    counters.count_drop(DropReason.NONE)  # not a counter
    counters.count_drop(DropReason.OVERSIZE, 0)  # nothing to charge

    stats = counters.snapshot()
    assert stats.dropped_invalid == 1
    assert stats.dropped_oversize == 1
    assert stats.dropped_evicted == 1
    assert stats.dropped_undeliverable == 1
    assert stats.dropped_fault == 1
    assert stats.total_dropped == 5


def test_stats_deltas_and_log_line() -> None:
    counters = Counters()
    counters.count_captured()
    counters.count_drop(DropReason.EVICTED, 3)
    first = counters.snapshot()

    counters.count_drop(DropReason.EVICTED, 2)
    delta = counters.snapshot(buffered_messages=7, buffer_bytes=99).sub(first)

    assert delta.dropped_evicted == 2
    assert delta.captured == 0
    assert delta.buffered_messages == 7  # gauges carry across, not differenced
    assert delta.log_line() == "dropped_evicted=2 buffered=7 buffer_bytes=99"


def test_timestamps_are_rfc3339_millis() -> None:
    stamp = now_iso()
    assert stamp.endswith("Z")
    assert len(stamp) == len("2026-05-18T12:34:56.789Z")
