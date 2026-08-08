"""Single non-reentrant worker: a daemon thread flushes the buffer on
count ≥ BATCH_CAP or ``flush_interval``, drains, and POSTs via Transport
(which owns retry). Also owns the bounded shutdown drain. Never raises.

Worker is also the producer chokepoint — :meth:`Worker.capture_ocpi` /
:meth:`Worker.capture_ocpp` delegate to the pure ``_prepare_ocpi`` /
``_prepare_ocpp`` helpers at the bottom of the file, keeping the
validate/cap/redact logic out of the class body.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from .buffer import BufferedMessage, RingBuffer, now_iso
from .config import ResolvedConfig
from .identity import validate_charger_identity, validate_roaming_identity
from .transport import Transport
from .types import OCPIMessage, OCPPMessage

#: Pure transform applied to a message right before enqueue. The concrete
#: redactors ship with the protocol packages (``evpanda.ocpi.redact`` /
#: ``evpanda.ocpp.redact``); the worker only needs the contract.
type OCPIRedactor = Callable[[OCPIMessage], OCPIMessage]
type OCPPRedactor = Callable[[OCPPMessage], OCPPMessage]

#: Server batch cap — also the size-based flush trigger.
BATCH_CAP = 1000

#: Poll granularity for the size trigger (producers don't push).
_POLL_INTERVAL = 0.2


class Worker:
    """Polls the buffer and flushes on size or interval."""

    def __init__(self, buffer: RingBuffer, transport: Transport, config: ResolvedConfig) -> None:
        self._buffer = buffer
        self._transport = transport
        self._config = config

        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None

        #: Single-flight: a concurrent caller waits out the in-flight flush.
        self._flush_lock = threading.Lock()
        self._last_lock = threading.Lock()
        self._last_flush_at = 0.0

    def start(self) -> None:
        """Arm the polling daemon thread."""
        if self._stopped.is_set():
            return
        self._set_last_flush(time.monotonic())
        self._thread = threading.Thread(target=self._loop, name="evpanda-worker", daemon=True)
        self._thread.start()

    def capture_ocpi(self, msg: OCPIMessage, redact: OCPIRedactor) -> None:
        """Producer entry point for OCPI; see ``_prepare_ocpi``."""
        env = _prepare_ocpi(msg, redact, self._config.max_capture_bytes)
        if env is not None:
            self._buffer.enqueue(env)

    def capture_ocpp(self, msg: OCPPMessage, redact: OCPPRedactor) -> None:
        """Producer entry point for OCPP; see ``_prepare_ocpp``."""
        env = _prepare_ocpp(msg, redact, self._config.max_capture_bytes)
        if env is not None:
            self._buffer.enqueue(env)

    def flush_once(self) -> None:
        """Run one flush, serialized so it never overlaps another."""
        with self._flush_lock:
            self._run_flush()

    def close(self, deadline: float | None = None) -> None:
        """One-shot, idempotent: stop the loop, bounded final drain.

        ``deadline`` is in seconds; None uses the configured drain timeout.
        Where the Go SDK returns ``ErrDrainIncomplete``, this logs a warning
        (when a logger is configured) and returns.
        """
        if self._stopped.is_set():
            return
        self._stopped.set()

        seconds = self._config.drain_timeout if deadline is None else deadline
        end = time.monotonic() + seconds

        # Joining the loop thread also waits out a flush it has in flight.
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, end - time.monotonic()))
            if thread.is_alive():
                self._log_drain_incomplete()
                return

        while self._buffer.count > 0:
            if time.monotonic() >= end:
                self._log_drain_incomplete()
                return
            self.flush_once()

    # ── internal ──────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stopped.is_set():
            # Wait on the stop flag with the poll timeout; returns early on
            # stop. Re-checked after the flush settles (non-reentrant).
            self._stopped.wait(_POLL_INTERVAL)
            if self._stopped.is_set():
                return
            if self._should_flush():
                self.flush_once()

    def _set_last_flush(self, t: float) -> None:
        with self._last_lock:
            self._last_flush_at = t

    def _should_flush(self) -> bool:
        n = self._buffer.count
        if n == 0:
            return False
        with self._last_lock:
            since = time.monotonic() - self._last_flush_at
        return n >= BATCH_CAP or since >= self._config.flush_interval

    def _run_flush(self) -> None:
        try:
            self._set_last_flush(time.monotonic())
            batch = self._buffer.flush()

            # A client serves one protocol, so the whole batch goes to one
            # endpoint, chunked at BATCH_CAP.
            protocol = self._config.protocol
            for i in range(0, len(batch), BATCH_CAP):
                # Transport owns retry; the worker calls send once and moves on.
                self._transport.send(protocol, batch[i : i + BATCH_CAP])
        except Exception:  # noqa: BLE001
            pass  # a failed cycle is swallowed — never raises into the host

    def _log_drain_incomplete(self) -> None:
        logger = self._config.logger
        if logger is None:
            return
        logger.warning("evpanda: close drain deadline exceeded with messages still buffered")


# ── Producer chokepoints (module-local, not exported) ────────────────────
#
# The one place messages are validated, capped, and redacted before the
# queue. Pure: they return the envelope to enqueue, or None to drop.
# Callers go through `Worker.capture_ocpi` / `capture_ocpp`.


def _prepare_ocpi(
    msg: OCPIMessage, redact: OCPIRedactor, max_capture_bytes: int
) -> BufferedMessage | None:
    """Validate, enforce the body cap, redact. An oversize body on either
    side drops the whole message — a half-body is broken JSON and would
    defeat the credentials redactor. Invalid identity ⇒ dropped.
    """
    if not validate_roaming_identity(msg.identity):
        return None
    if len(msg.data.request_body or b"") > max_capture_bytes:
        return None
    if len(msg.data.response_body or b"") > max_capture_bytes:
        return None
    return BufferedMessage(captured_at=now_iso(), message=redact(msg))


def _prepare_ocpp(
    msg: OCPPMessage, redact: OCPPRedactor, max_capture_bytes: int
) -> BufferedMessage | None:
    """Validate, enforce the payload cap, redact."""
    if not validate_charger_identity(msg.identity):
        return None
    if len(msg.payload or b"") > max_capture_bytes:
        return None
    return BufferedMessage(captured_at=now_iso(), message=redact(msg))
