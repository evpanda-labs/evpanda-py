"""Fixed-size drop-oldest ring. No I/O: :meth:`RingBuffer.flush` copies out
and resets; the worker does the POST. Internal.

Unlike the Node SDK — where the event loop serializes access — producers
here can enqueue from any thread, so the ring is guarded by a lock.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from .types import AnyMessage


@dataclass
class BufferedMessage:
    """Internal envelope: SDK-stamped ``captured_at`` (receive time) + message."""

    captured_at: str
    message: AnyMessage


def now_iso() -> str:
    """Return the current time as a wire timestamp.

    RFC3339 with millisecond precision and a literal UTC ``Z``, e.g.
    ``2026-05-18T12:34:56.789Z`` — the same shape JavaScript's
    ``new Date().toISOString()`` produces.
    """
    now = datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


class RingBuffer:
    """A fixed-capacity, drop-oldest queue, safe for concurrent use."""

    def __init__(self, capacity: int) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1:
            raise ValueError("evpanda: RingBuffer capacity must be a positive integer")
        self._lock = threading.Lock()
        self._buf: list[BufferedMessage | None] = [None] * capacity
        self._head = 0
        self._count = 0
        self._capacity = capacity

    def enqueue(self, envelope: BufferedMessage) -> None:
        """Drop-oldest when full (advance head; old ref overwritten below)."""
        with self._lock:
            if self._count == self._capacity:
                self._head = (self._head + 1) % self._capacity
            else:
                self._count += 1
            idx = (self._head + self._count - 1) % self._capacity
            self._buf[idx] = envelope

    def flush(self) -> list[BufferedMessage]:
        """Return live slots oldest→newest; clear refs + reset."""
        with self._lock:
            out: list[BufferedMessage] = []
            for i in range(self._count):
                idx = (self._head + i) % self._capacity
                msg = self._buf[idx]
                # Live slots are always populated; skip rather than raise, so a
                # broken invariant can never take out a flush cycle.
                if msg is not None:
                    out.append(msg)
                self._buf[idx] = None  # release ref
            self._head = 0
            self._count = 0
            return out

    @property
    def count(self) -> int:
        with self._lock:
            return self._count
