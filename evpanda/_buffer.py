"""A byte-bounded, drop-oldest queue.

It holds no I/O: :meth:`RingBuffer.drain` takes the live messages out and
resets; the worker does the POST.

The bound is bytes rather than a slot count because captured messages vary
by two orders of magnitude — an OCPP heartbeat is a few hundred bytes, an
OCPI CDR batch can be tens of kilobytes. A slot count is a proxy for the
thing an operator actually has to provision, and a poor one: the same
10 000 slots is ~3 MiB of OCPP heartbeats or 625 MiB of capped bodies.
``max_buffer_bytes`` is that number directly.

A :class:`collections.deque` backs it, so eviction is O(1) at the front and
the queue costs only what it holds — a low-traffic host never allocates for
its ceiling.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime

from ._stats import Counters, DropReason
from ._types import Message


@dataclass(slots=True)
class BufferedMessage:
    """The internal envelope: the SDK-stamped receive time, the message, its footprint."""

    captured_at: str
    message: Message
    #: Filled in by :meth:`RingBuffer.enqueue`; producers never set it.
    size: int = 0


def now_iso() -> str:
    """The current time as a wire timestamp.

    RFC 3339 with millisecond precision and a literal UTC ``Z``, matching
    the ``captured_at`` example in the ingestion spec:
    ``2026-05-18T12:34:56.789Z``.
    """
    now = datetime.now(UTC)
    return f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond // 1000:03d}Z"


class RingBuffer:
    """A byte-bounded, drop-oldest queue, safe for concurrent use.

    Producers enqueue from any thread; the worker drains.
    """

    __slots__ = ("_bytes", "_counters", "_lock", "_max_bytes", "_queue")

    def __init__(self, max_bytes: int, counters: Counters) -> None:
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
            raise ValueError("evpanda: buffer byte budget must be a positive integer")
        self._lock = threading.Lock()
        self._queue: deque[BufferedMessage] = deque()
        self._bytes = 0
        self._max_bytes = max_bytes
        #: Counts evictions; eviction is otherwise invisible, and it is the
        #: one drop that means data is being lost right now.
        self._counters = counters

    def enqueue(self, envelope: BufferedMessage) -> int:
        """Append a message, evicting the oldest until it fits.

        Returns the resulting message count. It never blocks and never
        grows past the budget.

        A message larger than the whole budget is dropped outright rather
        than emptying the buffer for something that still would not fit.
        The chokepoint's ``max_capture_bytes`` cap makes that unreachable
        unless the two are misconfigured relative to each other, which
        config resolution warns about.
        """
        envelope.size = envelope.message.size()

        with self._lock:
            if envelope.size > self._max_bytes:
                self._counters.count_drop(DropReason.OVERSIZE)
                return len(self._queue)
            while self._bytes + envelope.size > self._max_bytes:
                self._evict_oldest()
            self._queue.append(envelope)
            self._bytes += envelope.size
            self._counters.count_captured()
            return len(self._queue)

    def _evict_oldest(self) -> None:
        """Drop the front message. The caller holds the lock, and the enqueue
        loop only calls it while the budget is still exceeded — which cannot
        be true of an empty queue.
        """
        self._bytes -= self._queue.popleft().size
        self._counters.count_drop(DropReason.EVICTED)

    def drain(self) -> list[BufferedMessage]:
        """Remove and return everything buffered, oldest first."""
        with self._lock:
            if not self._queue:
                return []
            out = list(self._queue)
            self._queue.clear()
            self._bytes = 0
            return out

    def after_fork(self) -> None:
        """Rebuild in a freshly forked child: fresh lock, empty queue.

        A lock another thread held at fork time stays locked forever in the
        child, so it is replaced rather than reused. The parent's
        undelivered captures go with it — they are the parent's to deliver,
        and shipping them from both processes would duplicate every one.
        """
        self._lock = threading.Lock()
        self._queue.clear()
        self._bytes = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)

    @property
    def byte_len(self) -> int:
        """The accounted footprint of everything buffered, always <= the budget."""
        with self._lock:
            return self._bytes
