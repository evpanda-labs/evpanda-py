"""Delivery counters.

The SDK discards data in five places by design, and four of them are
otherwise invisible: a customer whose identity resolution is misconfigured
sees no traffic and no explanation. These counters are what make the
best-effort trade auditable.

They are always on — there is no log mode that turns them off, because an
increment nobody reads costs nothing and the alternative is a support
conversation that cannot be answered.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, fields
from enum import Enum


class DropReason(Enum):
    """The single taxonomy of why a message was lost.

    The capture chokepoint returns one (and only ever uses the first two);
    the buffer, the transport and the fault guard charge theirs directly.
    """

    NONE = "none"
    INVALID_IDENTITY = "invalid_identity"
    OVERSIZE = "oversize"
    EVICTED = "evicted"
    UNDELIVERABLE = "undeliverable"
    FAULT = "fault"


@dataclass(frozen=True, slots=True)
class Stats:
    """A point-in-time snapshot of one client's delivery counters.

    The ``dropped_*`` fields are monotonic totals since the client started;
    the ``buffer*`` fields are instantaneous.

    Each counter maps to exactly one root cause, which is what makes them
    worth reading during an integration problem:

    ==========================  ==================================================
    ``captured`` 0              the capture path is not wired in
    ``dropped_invalid`` high    identity resolution is failing — for the adapters,
                                usually no identity on the request at all
    ``dropped_oversize`` high   bodies exceed ``max_capture_bytes``
    ``dropped_evicted`` high    upstream can't keep up, or the buffer is
                                undersized for the traffic
    ``dropped_undeliverable``   network, API key, or ingestion fault
    ``dropped_fault`` > 0       a bug in the SDK; please report it
    ==========================  ==================================================
    """

    #: Messages that passed the chokepoint and entered the buffer.
    captured: int = 0
    #: Messages whose identity failed validation.
    dropped_invalid: int = 0
    #: Messages whose body or frame exceeded ``max_capture_bytes``, or
    #: which lacked a field the wire contract requires.
    dropped_oversize: int = 0
    #: Messages evicted from the buffer under pressure, oldest first.
    dropped_evicted: int = 0
    #: Messages in batches the transport could not deliver — retries
    #: exhausted, or a permanent rejection.
    dropped_undeliverable: int = 0
    #: Captures lost to a swallowed exception inside the SDK. Any value
    #: above zero is a bug.
    dropped_fault: int = 0

    #: How many messages are awaiting delivery now.
    buffered_messages: int = 0
    #: Their accounted footprint, always at or below ``max_buffer_bytes``.
    buffer_bytes: int = 0

    @property
    def total_dropped(self) -> int:
        """The sum of every ``dropped_*`` counter."""
        return (
            self.dropped_invalid
            + self.dropped_oversize
            + self.dropped_evicted
            + self.dropped_undeliverable
            + self.dropped_fault
        )

    def sub(self, previous: Stats) -> Stats:
        """The counters accumulated between ``previous`` and this snapshot.

        Only the monotonic fields are differenced; the buffer gauges are
        carried across, since a delta of an instantaneous value is
        meaningless.
        """
        return Stats(
            captured=self.captured - previous.captured,
            dropped_invalid=self.dropped_invalid - previous.dropped_invalid,
            dropped_oversize=self.dropped_oversize - previous.dropped_oversize,
            dropped_evicted=self.dropped_evicted - previous.dropped_evicted,
            dropped_undeliverable=self.dropped_undeliverable - previous.dropped_undeliverable,
            dropped_fault=self.dropped_fault - previous.dropped_fault,
            buffered_messages=self.buffered_messages,
            buffer_bytes=self.buffer_bytes,
        )

    #: The key each counter is logged under. Deliberately not the field
    #: names: the health line is operator-facing, and an operator grepping a
    #: polyglot fleet should see one vocabulary, not three. All three SDKs
    #: emit exactly these keys.
    _LOG_KEYS = (
        ("captured", "captured"),
        ("invalid_identity", "dropped_invalid"),
        ("oversize", "dropped_oversize"),
        ("evicted", "dropped_evicted"),
        ("undeliverable", "dropped_undeliverable"),
        ("fault", "dropped_fault"),
    )

    def log_line(self) -> str:
        """Render the snapshot as ``key=value`` pairs, omitting zero counters.

        Keeping the line to what actually happened is what makes it
        readable at a glance in a production log.
        """
        pairs = [(key, getattr(self, name)) for key, name in self._LOG_KEYS if getattr(self, name)]
        pairs += [("buffered", self.buffered_messages), ("buffer_bytes", self.buffer_bytes)]
        return " ".join(f"{k}={v}" for k, v in pairs)


#: The counter each drop reason charges. The single mapping from a reason
#: to its field, so adding a reason is one line here and one on Stats.
_FIELD_FOR_REASON = {
    DropReason.INVALID_IDENTITY: "dropped_invalid",
    DropReason.OVERSIZE: "dropped_oversize",
    DropReason.EVICTED: "dropped_evicted",
    DropReason.UNDELIVERABLE: "dropped_undeliverable",
    DropReason.FAULT: "dropped_fault",
}

_COUNTER_FIELDS = tuple(
    f.name for f in fields(Stats) if f.name not in ("buffered_messages", "buffer_bytes")
)


class Counters:
    """The live counter set, shared by the chokepoint, the buffer and the transport.

    Safe for concurrent use. A single lock rather than per-counter atomics:
    the capture path already takes the buffer lock, so one more uncontended
    acquisition is not what will cost the host anything.
    """

    __slots__ = ("_lock", "_values")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, int] = dict.fromkeys(_COUNTER_FIELDS, 0)

    def count_captured(self) -> None:
        with self._lock:
            self._values["captured"] += 1

    def count_drop(self, reason: DropReason, n: int = 1) -> None:
        """Charge ``n`` messages to the counter for ``reason``.

        It takes a count because the transport loses a whole batch at once.
        """
        field = _FIELD_FOR_REASON.get(reason)
        if field is None or n <= 0:
            return
        with self._lock:
            self._values[field] += n

    def snapshot(self, buffered_messages: int = 0, buffer_bytes: int = 0) -> Stats:
        """Read the counters, optionally alongside the live buffer gauges."""
        with self._lock:
            values = dict(self._values)
        return Stats(**values, buffered_messages=buffered_messages, buffer_bytes=buffer_bytes)

    def after_fork(self) -> None:
        """Rebuild in a freshly forked child: fresh lock, zeroed counters.

        The child is a new client as far as delivery goes, so it starts its
        tally from zero rather than inheriting the parent's.
        """
        self._lock = threading.Lock()
        self._values = dict.fromkeys(_COUNTER_FIELDS, 0)
