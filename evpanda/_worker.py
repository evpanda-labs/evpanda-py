"""The SDK's only background thread.

One daemon thread owns every flush: it waits on the flush interval and on
a wake event a producer raises when a full batch is ready, so flushes are
serialized by construction and an idle SDK costs one sleeping thread.

The worker is also the producer chokepoint. :meth:`Worker.capture_ocpi` and
:meth:`Worker.capture_ocpp` run the pure ``prepare_*`` helpers at the
bottom of the file, which are the single place a message is validated,
capped, owned and redacted before it reaches the queue.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from typing import Any

from ._buffer import BufferedMessage, RingBuffer, now_iso
from ._config import LogMode, ResolvedConfig
from ._redact import OCPIRedactor, OCPPRedactor
from ._stats import Counters, DropReason, Stats
from ._transport import Transport
from ._types import (
    HTTPExchange,
    OCPIMessage,
    OCPPEventType,
    OCPPMessage,
    coerce_body,
    valid_charger,
    valid_platform,
)

#: The ingestion API's per-request maximum and, equally, the size-based
#: flush trigger.
BATCH_CAP = 1000

#: Bounds how often the health line can appear. It is a ceiling on log
#: volume, not a sampling rate: the line reports everything that happened
#: in the window, so nothing is hidden by the delay.
#:
#: This is why drops are summarized rather than logged per event. The
#: common integration fault — an adapter that resolves no identity — drops
#: on every single request, so per-event logging would emit at request rate
#: and cost the host real money in log ingestion.
REPORT_INTERVAL = 60.0


class Worker:
    """Owns the buffer, the delivery thread, and the capture chokepoint."""

    def __init__(
        self,
        buffer: RingBuffer,
        transport: Transport,
        config: ResolvedConfig,
        counters: Counters,
    ) -> None:
        self._buffer = buffer
        self._transport = transport
        self._config = config
        self._counters = counters

        #: Raised by a producer once a full batch is waiting, and by close.
        #: A burst collapses into a single flush.
        self._wake = threading.Event()
        self._stopped = False
        self._thread: threading.Thread | None = None

        #: Serializes flushes: an explicit flush() queues behind the one the
        #: worker has in flight rather than starting a second.
        self._flush_lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closed = False
        self._drained = True

        #: The snapshot the previous health line reported. Only the loop
        #: thread touches it.
        self._last_report = Stats()

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        """Launch the delivery thread."""
        if self._stopped:
            return
        thread = threading.Thread(target=self._loop, name="evpanda-worker", daemon=True)
        self._thread = thread
        thread.start()

    def _loop(self) -> None:
        # The host does not own this thread: an exception escaping here
        # would stop delivery silently. It is caught, counted, and is the
        # one thing logged at error level — a client whose worker died is
        # broken and should be noisy about it.
        try:
            self._run()
        except Exception as exc:  # noqa: BLE001 - see above
            self._counters.count_drop(DropReason.FAULT)
            if self._config.logger is not None:
                self._config.logger.error(
                    "evpanda: delivery stopped by an exception — please report this: %r", exc
                )

    def _run(self) -> None:
        interval = self._config.flush_interval
        next_flush = time.monotonic() + interval
        next_report = time.monotonic() + REPORT_INTERVAL

        while True:
            timeout = max(0.0, min(next_flush, next_report) - time.monotonic())
            woke = self._wake.wait(timeout)
            if self._stopped:
                return
            if woke:
                self._wake.clear()

            now = time.monotonic()
            # Every flush restarts the interval: whatever the reason, the
            # buffer is empty afterwards, so a tick already pending would be
            # a no-op.
            if woke or now >= next_flush:
                self.flush_once()
                next_flush = time.monotonic() + interval
            if now >= next_report:
                self._report_health()
                next_report = now + REPORT_INTERVAL

    def close(self, timeout: float | None = None) -> bool:
        """Stop the loop and drain what is left, bounded by ``timeout`` seconds.

        Returns whether the drain completed. Idempotent: later calls return
        the first call's result.
        """
        with self._close_lock:
            if self._closed:
                return self._drained
            self._closed = True
            self._drained = self._shutdown(timeout)
            return self._drained

    def _shutdown(self, timeout: float | None) -> bool:
        seconds = self._config.drain_timeout if timeout is None else max(0.0, timeout)
        end = time.monotonic() + seconds

        self._stopped = True
        self._wake.set()

        # Joining the loop also waits out a flush it has in flight.
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.0, end - time.monotonic()))
            if thread.is_alive():
                # The thread is a daemon, so it cannot outlive the process;
                # the in-flight POST it is stuck on has its own timeout.
                self._report_shutdown(drained=False)
                return False

        # The loop is gone, so the final drain runs on the caller's thread.
        while len(self._buffer) > 0:
            if time.monotonic() >= end:
                self._report_shutdown(drained=False)
                return False
            self.flush_once(deadline=end)

        self._report_shutdown(drained=True)
        return True

    # ── Capture ──────────────────────────────────────────────────────────

    def capture_ocpi(self, message: OCPIMessage, redact: OCPIRedactor | None) -> None:
        """The producer entry point for OCPI; see :func:`prepare_ocpi`."""
        envelope, reason, bodies_dropped = prepare_ocpi(
            message, redact, self._config.max_capture_bytes
        )
        self._counters.count_bodies_dropped(bodies_dropped)
        if envelope is None:
            self._counters.count_drop(reason)
            return
        self._enqueue(envelope)

    def capture_ocpp(self, message: OCPPMessage, redact: OCPPRedactor | None = None) -> None:
        """The producer entry point for OCPP; see :func:`prepare_ocpp`."""
        envelope, reason, bodies_dropped = prepare_ocpp(
            message, redact, self._config.max_capture_bytes
        )
        self._counters.count_bodies_dropped(bodies_dropped)
        if envelope is None:
            self._counters.count_drop(reason)
            return
        self._enqueue(envelope)

    def _enqueue(self, envelope: BufferedMessage) -> None:
        """Buffer the envelope and raise the size trigger once a batch is full."""
        if self._buffer.enqueue(envelope) >= BATCH_CAP:
            self._wake.set()

    # ── Delivery ─────────────────────────────────────────────────────────

    def flush_once(self, deadline: float | None = None) -> None:
        """Deliver everything buffered, serialized against any other flush."""
        with self._flush_lock:
            batch = self._buffer.drain()
            if not batch:
                return
            # A client serves one protocol, so the whole batch goes to one
            # route. The transport owns retry; the worker sends and moves on.
            for start in range(0, len(batch), BATCH_CAP):
                self._transport.send(
                    self._config.protocol, batch[start : start + BATCH_CAP], deadline
                )

    # ── Reporting ────────────────────────────────────────────────────────

    def snapshot(self) -> Stats:
        """The counters plus the live buffer gauges."""
        return self._counters.snapshot(len(self._buffer), self._buffer.byte_len)

    def _report_health(self) -> None:
        """Emit at most one line per :data:`REPORT_INTERVAL`, and only when
        something was dropped in that window. A healthy client is silent.
        """
        if self._config.logger is None:
            return
        current = self.snapshot()
        delta = current.sub(self._last_report)
        self._last_report = current
        if delta.total_dropped == 0:
            return
        self._config.logger.warning(
            "evpanda: captures dropped window=%.0fs %s", REPORT_INTERVAL, delta.log_line()
        )

    def _report_shutdown(self, drained: bool) -> None:
        """Log the client's lifetime totals as it closes.

        In DEBUG it always logs; otherwise only when something was dropped
        or the drain fell short, so a clean run leaves no trace.
        """
        logger = self._config.logger
        if logger is None:
            return
        total = self.snapshot()
        if total.total_dropped == 0 and drained and self._config.log_mode is not LogMode.DEBUG:
            return
        line = total.log_line()
        if not drained:
            line += " drain=incomplete"
        if total.total_dropped == 0 and drained:
            logger.info("evpanda: client closed %s", line)
        else:
            logger.warning("evpanda: client closed %s", line)

    # ── Fork ─────────────────────────────────────────────────────────────

    def after_fork(self) -> None:
        """Rebuild this worker in a freshly forked child process.

        Only the forking thread survives a fork, so the delivery thread is
        gone and any lock another thread held is stuck locked forever. Every
        lock is therefore replaced, the parent's undelivered captures are
        dropped (they are the parent's to deliver, and shipping them from
        both processes would duplicate every one), the counters start from
        zero, and the thread is started again.
        """
        self._wake = threading.Event()
        self._flush_lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._buffer.after_fork()
        self._counters.after_fork()
        self._last_report = Stats()
        self._thread = None
        if not self._stopped and not self._closed:
            self.start()

    @property
    def config(self) -> ResolvedConfig:
        """The resolved config this worker runs on."""
        return self._config


# ── Producer chokepoints ─────────────────────────────────────────────────
#
# The one place messages are validated, capped, owned and redacted before
# the queue. Pure: they return the envelope to enqueue, or None plus the
# reason the drop belongs to. Callers go through Worker.capture_*.


def prepare_ocpi(
    message: OCPIMessage, redact: OCPIRedactor | None, max_capture_bytes: int
) -> tuple[BufferedMessage | None, DropReason, int]:
    """Validate the identity, enforce the body cap, take ownership, redact.

    An oversize body on either side drops the whole message — half a body
    is broken JSON, and it would defeat the credentials redactor.

    The third element is how many bodies were omitted for not being valid
    UTF-8.
    """
    if not valid_platform(message.identity):
        return None, DropReason.INVALID_IDENTITY, 0

    source = message.data
    request_body = coerce_body(source.request_body)
    response_body = coerce_body(source.response_body)
    if len(request_body or b"") > max_capture_bytes:
        return None, DropReason.OVERSIZE, 0
    if len(response_body or b"") > max_capture_bytes:
        return None, DropReason.OVERSIZE, 0

    # A body that is not valid UTF-8 cannot travel: the wire contract
    # carries it as text. Drop the body, keep the exchange — method, URL,
    # status and headers are still worth having, and the counter says the
    # body went missing on purpose.
    bodies_dropped = 0
    if not is_utf8(request_body):
        request_body = None
        bodies_dropped += 1
    if not is_utf8(response_body):
        response_body = None
        bodies_dropped += 1

    # Take ownership before redacting. From here the exchange is the SDK's,
    # so the redactor may rewrite it in place, and the host may reuse or
    # mutate what it passed the moment the capture call returns.
    message.data = HTTPExchange(
        method=source.method,
        url=source.url,
        status_code=source.status_code,
        request_headers=header_map(source.request_headers),
        response_headers=header_map(source.response_headers),
        request_body=request_body,
        response_body=response_body,
    )
    if redact is not None:
        message = redact(message)
    return BufferedMessage(captured_at=now_iso(), message=message), DropReason.NONE, bodies_dropped


def prepare_ocpp(
    message: OCPPMessage, redact: OCPPRedactor | None, max_capture_bytes: int
) -> tuple[BufferedMessage | None, DropReason, int]:
    """Validate the identity and enforce the frame cap.

    A message event with no frame or no direction is dropped: the ingestion
    contract requires both on ``event_type`` 2.
    """
    if not valid_charger(message.identity):
        return None, DropReason.INVALID_IDENTITY, 0

    payload = coerce_body(message.payload)
    if len(payload or b"") > max_capture_bytes:
        return None, DropReason.OVERSIZE, 0
    if message.event_type is OCPPEventType.MESSAGE and (not payload or not message.direction):
        return None, DropReason.OVERSIZE, 0
    # Unlike an OCPI body, a frame is the whole message: ``event_type`` 2
    # requires one, so a frame that is not valid UTF-8 takes the message
    # with it. It is counted twice on purpose, once as the body that went
    # missing and once as the message that did.
    if not is_utf8(payload):
        return None, DropReason.OVERSIZE, 1

    message.payload = payload
    # None is the normal case for OCPP: there is nothing to redact, so
    # there is no redactor rather than one that does nothing.
    if redact is not None:
        message = redact(message)
    return BufferedMessage(captured_at=now_iso(), message=message), DropReason.NONE, 0


def is_utf8(body: bytes | None) -> bool:
    """Whether ``body`` is valid UTF-8, which the wire contract requires.

    ``bytes.decode`` walks the buffer once in C and raises on the first
    invalid sequence, which is cheaper than any check written in Python.
    """
    if not body:
        return True
    try:
        body.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def header_map(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """A private copy of a header mapping, with string keys and values.

    Copying is what makes the capture safe to hold: the buffered message
    outlives the call, so a mapping the host goes on to mutate would
    otherwise be serialized in whatever state it ended up in.
    """
    if not headers:
        return {}
    return {str(k): str(v) for k, v in headers.items()}
