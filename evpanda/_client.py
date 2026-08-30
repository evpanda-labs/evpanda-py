"""The two clients, and the lifecycle core they share.

The protocol is the client — there is no network-type switch.
:func:`start_ocpi` returns an :class:`OCPIClient`, :func:`start_ocpp` an
:class:`OCPPClient`, and a client instance serves exactly one protocol for
its whole life. Both build on the private ``_Client`` below, which owns the
worker and the counters and supplies ``stats`` / ``capturing`` / ``flush``
/ ``close``.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import os
import threading
import uuid
import weakref
from collections.abc import Callable
from types import TracebackType
from typing import Self

from ._buffer import RingBuffer
from ._config import (
    ConfigError,
    LogMode,
    OCPIConfig,
    OCPPConfig,
    ResolvedConfig,
    logger_for,
    resolve_ocpi_config,
    resolve_ocpp_config,
)
from ._redact import OCPIRedactor, OCPPRedactor, make_ocpi_redactor
from ._stats import Counters, DropReason, Stats
from ._transport import Transport
from ._types import (
    BodyInput,
    Charger,
    HTTPExchange,
    OCPIDirection,
    OCPIMessage,
    OCPPDirection,
    OCPPEventType,
    OCPPMessage,
    Platform,
)
from ._worker import Worker

# ══ Shared lifecycle core ═══════════════════════════════════════════════


class _Client:
    """Holds the running worker, drops it on close, and reports on itself.

    ``start_ocpi`` and ``start_ocpp`` never return None and never raise, so
    every method here has a live receiver — including on an inert client
    (one built from a bad config), whose worker is None but whose object is
    not.
    """

    __slots__ = ("__weakref__", "_counters", "_error", "_lock", "_worker")

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._worker: Worker | None = None
        #: Held here rather than on the worker so the counters survive
        #: close — the final tally is often the thing worth reading.
        self._counters = Counters()
        self._error: ConfigError | None = None

    # ── Construction ─────────────────────────────────────────────────────

    def _begin(self, resolved: ResolvedConfig) -> None:
        """Build and launch the pipeline, leaving the client live."""
        buffer = RingBuffer(resolved.max_buffer_bytes, self._counters)
        worker = Worker(buffer, Transport(resolved, self._counters), resolved, self._counters)
        worker.start()
        self._worker = worker
        _register(self)

    def _fail(self, error: ConfigError, logger: logging.Logger | None) -> None:
        """Leave the client inert, carrying the fault that got it there.

        The error is logged as well as stored: a host that never looks at
        :attr:`error` still has to be told that its SDK is not running.
        """
        self._error = error
        if logger is not None:
            logger.error("%s — the client is inert and will capture nothing", error)

    # ── State ────────────────────────────────────────────────────────────

    @property
    def error(self) -> ConfigError | None:
        """The configuration fault that made this client inert, if any.

        None on a healthy client. Match :class:`~evpanda.APIKeyError` to
        tell a deployment problem (the key never reached the process) from
        a code one::

            panda = evpanda.start_ocpi()
            if isinstance(panda.error, evpanda.APIKeyError):
                raise SystemExit("EVPANDA_API_KEY is not set in this environment")
            if panda.error:
                log.warning("%s (running inert)", panda.error)
        """
        return self._error

    @property
    def _current(self) -> Worker | None:
        """The running worker, or None when the client is inert or closed."""
        with self._lock:
            return self._worker

    def _detach(self) -> Worker | None:
        """Take the worker and leave the client inert.

        Returns None if the client was already inert, which is what makes
        close idempotent.
        """
        with self._lock:
            worker, self._worker = self._worker, None
            return worker

    def stats(self) -> Stats:
        """A snapshot of this client's delivery counters.

        Always available: there is no log mode that turns the counters off,
        and it is safe to call on an inert or closed client (a closed one
        reports its final totals with an empty buffer).

        Use it to answer "why am I seeing no data?" without a redeploy —
        each counter maps to one root cause, documented on
        :class:`~evpanda.Stats` — or to feed your own metrics system::

            gauge.set(panda.stats().dropped_evicted)
        """
        worker = self._current
        if worker is not None:
            return worker.snapshot()
        return self._counters.snapshot()  # inert or closed: counters only, no buffer

    def capturing(self) -> int | None:
        """The per-body byte cap while this client is capturing, else None.

        The cap and the fact of capturing come back together deliberately:
        asked separately they could straddle a close and disagree::

            max_bytes = panda.capturing()
            if max_bytes is not None:
                ...  # safe to instrument, and bound what you buffer at max_bytes

        The shipped adapters use it to bound what they accumulate from a
        streaming body and to skip instrumentation entirely when there is
        nothing to capture into. It is public so an adapter for a framework
        the SDK does not ship has the same two facts.
        """
        worker = self._current
        return None if worker is None else worker.config.max_capture_bytes

    # ── Lifecycle ────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Deliver everything currently buffered and wait for that delivery.

        It blocks for as long as the transport's bounded retry takes, so it
        is a diagnostic and shutdown tool rather than something to call on
        a hot path — capture is already asynchronous. Never raises.
        """
        worker = self._current
        if worker is None:
            return
        try:
            worker.flush_once()
        except Exception as exc:  # noqa: BLE001 - the SDK never raises into the host
            self._log_fault("flush", exc)

    def close(self, timeout: float | None = None) -> bool:
        """Stop capture and drain what is buffered, then report whether it drained.

        ``timeout`` is in seconds and defaults to the configured
        ``drain_timeout``. Returns False if the deadline passed with
        messages still buffered, meaning some captured data was dropped on
        shutdown.

        Idempotent, and never raises. Captures made after it are safe
        no-ops.
        """
        worker = self._detach()
        if worker is None:
            return True
        try:
            return worker.close(timeout)
        except Exception as exc:  # noqa: BLE001 - the SDK never raises into the host
            self._log_fault("close", exc)
            return False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ── Fault handling ───────────────────────────────────────────────────

    def _guard(self, op: str, fn: Callable[[], None]) -> None:
        """Run a capture path, swallowing and counting any exception.

        The SDK never raises into the host, and a capture has no error to
        return.
        """
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - see above
            # Reporting the fault must not raise into the host either.
            with contextlib.suppress(Exception):
                self._log_fault(op, exc)

    def _log_fault(self, op: str, exc: BaseException) -> None:
        """Count a swallowed capture fault and, in DEBUG, log it.

        In the default mode the worker's health line reports it instead: a
        fault that repeats per message would otherwise log at message rate.
        """
        self._counters.count_drop(DropReason.FAULT)
        worker = self._current
        if worker is None or worker.config.logger is None:
            return
        if worker.config.log_mode is not LogMode.DEBUG:
            return
        worker.config.logger.warning("evpanda: capture failed op=%s error=%r", op, exc)

    def _after_fork(self) -> None:
        """Rebuild this client's worker in a freshly forked child."""
        self._lock = threading.RLock()
        worker = self._worker
        if worker is not None:
            worker.after_fork()


# ══ OCPI ════════════════════════════════════════════════════════════════


class OCPIClient(_Client):
    """Captures and ships OCPI roaming traffic.

    Build it with :func:`start_ocpi`; it is safe for concurrent use.

    OCPI traffic flows both ways between roaming partners and the SDK
    records each direction separately. There is no direction argument — the
    method you call stamps it:

    * :meth:`capture_inbound_message` — a partner called your OCPI server.
      You are the server, so you capture the request they sent and the
      response you returned.
    * :meth:`capture_outbound_message` — you called a partner's OCPI
      server. You are the client, so you capture the request you sent and
      the response they returned.

    In both cases ``identity`` is the partner on the other side of the
    exchange, never your own platform.
    """

    __slots__ = ("_redact",)

    def __init__(self) -> None:
        super().__init__()
        #: The header allowlist and credentials mask. start_ocpi always
        #: sets it on a live client — the chokepoint reads None as "nothing
        #: to redact".
        self._redact: OCPIRedactor | None = None

    def capture_inbound_message(self, identity: Platform, data: HTTPExchange) -> None:
        """Buffer an inbound OCPI message (partner → host) for delivery.

        Non-blocking and never raises; a message with an invalid identity
        or an oversize body is silently dropped.
        """
        self._guard(
            "capture_inbound_message",
            lambda: self._capture_ocpi(identity, data, OCPIDirection.IN),
        )

    def capture_outbound_message(self, identity: Platform, data: HTTPExchange) -> None:
        """Buffer an outbound OCPI message (host → partner) for delivery.

        Non-blocking and never raises; a message with an invalid identity
        or an oversize body is silently dropped.
        """
        self._guard(
            "capture_outbound_message",
            lambda: self._capture_ocpi(identity, data, OCPIDirection.OUT),
        )

    def _capture_ocpi(
        self, identity: Platform, data: HTTPExchange, direction: OCPIDirection
    ) -> None:
        """Stamp the direction and hand the message to the worker, which runs
        the validate → cap → own → redact chokepoint.
        """
        worker = self._current
        if worker is None:
            return
        worker.capture_ocpi(
            OCPIMessage(direction=direction, identity=identity, data=data), self._redact
        )


def start_ocpi(config: OCPIConfig | None = None) -> OCPIClient:
    """Validate the config, build the client, and launch its background worker.

    It always returns a usable :class:`OCPIClient` and never raises.
    ``api_key`` is hard-required and ``endpoint`` must parse: if either
    fails, the returned client is an inert no-op carrying the fault on
    :attr:`~OCPIClient.error`, so a config typo can never stop the host
    booting. Every other field is tunable — a bad value falls back to its
    default and is reported through ``log_mode``.

    ::

        # endpoint defaults to production; api_key comes from EVPANDA_API_KEY
        panda = evpanda.start_ocpi()
        if panda.error:
            log.warning("%s (running inert)", panda.error)
    """
    client = OCPIClient()
    config = config if config is not None else OCPIConfig()
    try:
        resolved = resolve_ocpi_config(config)
    except ConfigError as exc:
        client._fail(exc, logger_for(config))
        return client
    client._redact = make_ocpi_redactor(resolved.allowed_headers)
    client._begin(resolved)
    return client


# ══ OCPP ════════════════════════════════════════════════════════════════


class OCPPClient(_Client):
    """Captures and ships OCPP CSMS traffic.

    Build it with :func:`start_ocpp`; it is safe for concurrent use.

    There are two ways to capture:

    * :meth:`connection` — the recommended path: a session handle that owns
      the connection ID and carries the identity. Attach it to your
      WebSocket and call ``message`` per frame, ``disconnect`` on close.
    * :meth:`capture_connect` / :meth:`capture_message` /
      :meth:`capture_disconnect` — the flat primitives the session is built
      on, for one-off capture.

    ``identity`` is a :class:`~evpanda.Charger` value, not a
    resolver: OCPP identity is known at connect time. An invalid one drops
    the message.
    """

    __slots__ = ("_redact",)

    def __init__(self) -> None:
        super().__init__()
        #: None today: OCPP frames are captured verbatim, and the
        #: chokepoint reads None as "nothing to redact". See _redact.py.
        self._redact: OCPPRedactor | None = None

    def connection(self, identity: Charger) -> OCPPSession:
        """Open a capture session for one OCPP connection.

        It mints a connection ID, records the connect, and hands back the
        session. Use one per socket: its connection ID ties the connect,
        every frame and the disconnect into a single session, and a
        reconnect gets a fresh one.
        """
        connection_id = str(uuid.uuid4())
        self.capture_connect(identity, connection_id)
        return OCPPSession(self, identity, connection_id)

    def capture_connect(self, identity: Charger, connection_id: str) -> None:
        """Record a new OCPP connection. Non-blocking and never raises."""
        self._guard(
            "capture_connect",
            lambda: self._capture_ocpp(
                OCPPMessage(
                    event_type=OCPPEventType.CONNECT,
                    identity=identity,
                    connection_id=connection_id,
                )
            ),
        )

    def capture_message(
        self,
        identity: Charger,
        connection_id: str,
        data: BodyInput,
        direction: OCPPDirection,
    ) -> None:
        """Record one OCPP frame.

        ``data`` and ``direction`` are both required — the ingestion
        contract requires them on a message event — and the message is
        dropped if either is missing. Oversize frames are dropped too.
        Non-blocking and never raises.
        """
        self._guard(
            "capture_message",
            lambda: self._capture_ocpp(
                OCPPMessage(
                    event_type=OCPPEventType.MESSAGE,
                    identity=identity,
                    connection_id=connection_id,
                    direction=direction,
                    payload=data,
                )
            ),
        )

    def capture_disconnect(self, identity: Charger, connection_id: str) -> None:
        """Record the connection closing. Non-blocking and never raises."""
        self._guard(
            "capture_disconnect",
            lambda: self._capture_ocpp(
                OCPPMessage(
                    event_type=OCPPEventType.DISCONNECT,
                    identity=identity,
                    connection_id=connection_id,
                )
            ),
        )

    def _capture_ocpp(self, message: OCPPMessage) -> None:
        """Hand the message to the worker, which runs the chokepoint."""
        worker = self._current
        if worker is None:
            return
        worker.capture_ocpp(message, self._redact)


class OCPPSession:
    """A live capture handle for one OCPP WebSocket connection.

    Returned by :meth:`OCPPClient.connection`, it owns the connection ID
    and the identity so per-frame calls carry neither. Attach it to your
    connection object and call :meth:`message` per frame,
    :meth:`disconnect` when the socket closes.

    It is also a context manager, so leaving the block records the
    disconnect::

        with panda.connection(identity) as session:
            async for frame in socket:
                session.message(frame, evpanda.OCPPDirection.FROM_CP)
    """

    __slots__ = ("_client", "_identity", "connection_id")

    def __init__(self, client: OCPPClient, identity: Charger, connection_id: str) -> None:
        #: The SDK-minted ID for this connection — fresh per
        #: :meth:`OCPPClient.connection` call, which is how the ingestion
        #: side separates one charger's sessions across reconnects.
        self.connection_id = connection_id
        self._client = client
        self._identity = identity

    def message(self, data: BodyInput, direction: OCPPDirection) -> None:
        """Capture one OCPP frame. Oversize frames are dropped."""
        self._client.capture_message(self._identity, self.connection_id, data, direction)

    def disconnect(self) -> None:
        """Capture the connection closing."""
        self._client.capture_disconnect(self._identity, self.connection_id)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.disconnect()


def start_ocpp(config: OCPPConfig | None = None) -> OCPPClient:
    """Validate the config, build the client, and launch its background worker.

    It always returns a usable :class:`OCPPClient` and never raises; see
    :func:`start_ocpi` for what can fail and what an inert client does.

    ::

        panda = evpanda.start_ocpp()
        if panda.error:
            log.warning("%s (running inert)", panda.error)
    """
    client = OCPPClient()
    config = config if config is not None else OCPPConfig()
    try:
        resolved = resolve_ocpp_config(config)
    except ConfigError as exc:
        client._fail(exc, logger_for(config))
        return client
    # _redact stays None: OCPP frames are captured verbatim today.
    client._begin(resolved)
    return client


# ══ Process lifecycle ═══════════════════════════════════════════════════
#
# Two things a Go SDK never has to think about, and a Python one does: the
# interpreter exits without joining daemon threads, and a preforking server
# (gunicorn, uWSGI) forks workers out of a parent that already built the
# client. Both are handled once, here, for every live client.

_live: weakref.WeakSet[_Client] = weakref.WeakSet()
_registry_lock = threading.Lock()
_fork_hook_installed = False


def _register(client: _Client) -> None:
    """Track a live client for the at-exit drain and the after-fork restart."""
    global _fork_hook_installed
    with _registry_lock:
        _live.add(client)
        if not _fork_hook_installed and hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=_after_fork_in_child)
            _fork_hook_installed = True


def _after_fork_in_child() -> None:
    """Restart every client's worker in the child.

    Only the forking thread survives a fork, so without this a preforked
    worker process would capture into a buffer nothing ever drains. It runs
    single-threaded in the child and takes no lock of its own: a lock held
    by another thread at fork time is still held here.
    """
    global _registry_lock
    _registry_lock = threading.Lock()
    for client in list(_live):
        # A fork must not fail on our account.
        with contextlib.suppress(Exception):
            client._after_fork()


@atexit.register
def _drain_at_exit() -> None:
    """Drain every client that the host did not close itself.

    The delivery thread is a daemon, so an interpreter exit would otherwise
    discard whatever was captured since the last flush. Closing is
    idempotent, so a host that closes its own client pays nothing here.
    """
    for client in list(_live):
        # Never raise during interpreter shutdown.
        with contextlib.suppress(Exception):
            client.close()
