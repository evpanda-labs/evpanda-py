"""OCPPClient — passive OCPP CSMS traffic capture, plus the (currently
pass-through) OCPP redaction seam.

Two ways to capture:
  - ``connection(identity)`` — the recommended path: a session handle that
    owns the ``connection_id`` and carries the identity. Attach it to your
    WebSocket and call ``message`` / ``disconnect``.
  - ``capture_connect`` / ``capture_message`` / ``capture_disconnect`` — the
    flat primitives the session is built on, for one-off capture.

Identity is a :class:`~evpanda.identity.ChargerIdentity`; an invalid one is
dropped.
"""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol

from .buffer import RingBuffer
from .client import BaseClient
from .config import OCPPConfig, resolve_ocpp_config
from .identity import ChargerIdentity, validate_charger_identity
from .transport import Transport
from .types import OCPPDirection, OCPPEventType, OCPPMessage
from .worker import OCPPRedactor, Worker


def make_ocpp_redactor() -> OCPPRedactor:
    """Build the OCPP redactor. Today the identity transform — frames are
    captured verbatim. The seam exists so masking (e.g. ``idTag`` in
    ``Authorize``) can be added later without touching ``worker.py`` or
    :class:`OCPPClient`.
    """

    def redact(msg: OCPPMessage) -> OCPPMessage:
        return msg

    return redact


@dataclass
class OCPPMessageInput:
    """Input shape for all three OCPP capture methods. ``data`` / ``direction``
    are optional on the type because connect/disconnect don't carry a frame;
    :meth:`OCPPClient.capture_message` requires both and drops the message if
    either is missing.
    """

    #: The charge point this event belongs to. Invalid ⇒ message dropped.
    identity: ChargerIdentity
    #: Stable for the lifetime of this connection; minted by the caller.
    connection_id: str
    #: Frame bytes (str → UTF-8). Required by ``capture_message``.
    data: bytes | str | None = None
    #: Frame direction. Required by ``capture_message``.
    direction: OCPPDirection | None = None


class OCPPSession:
    """A live capture handle for one OCPP WebSocket connection. Returned by
    :meth:`OCPPClient.connection`; it owns the ``connection_id`` and the
    identity so per-frame calls carry neither. Attach it to your connection
    object and call ``message`` per frame, ``disconnect`` when the socket
    closes.

    Usable as a context manager — leaving the block records the disconnect.
    """

    def __init__(self, client: OCPPClient, identity: ChargerIdentity, connection_id: str) -> None:
        self._client = client
        self._identity = identity
        #: SDK-minted id for this connection — fresh per ``connection()`` call.
        self.connection_id = connection_id

    def message(self, data: bytes | str, direction: OCPPDirection) -> None:
        """Capture one OCPP frame. Oversize frames are dropped."""
        self._client.capture_message(
            OCPPMessageInput(
                identity=self._identity,
                connection_id=self.connection_id,
                data=data,
                direction=direction,
            )
        )

    def disconnect(self) -> None:
        """Capture the connection closing."""
        self._client.capture_disconnect(
            OCPPMessageInput(identity=self._identity, connection_id=self.connection_id)
        )

    def __enter__(self) -> OCPPSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.disconnect()


class Engine(Protocol):
    def enqueue(self, msg: OCPPMessage) -> None: ...

    @property
    def max_capture_bytes(self) -> float: ...

    #: Effective logger (set only when ``debug=True``); None ⇒ silent.
    @property
    def logger(self) -> logging.Logger | None: ...

    def flush(self) -> None: ...

    def close(self, deadline: float | None = None) -> None: ...


class _ActiveEngine:
    def __init__(self, config: OCPPConfig) -> None:
        resolved = resolve_ocpp_config(config)
        buffer = RingBuffer(resolved.buffer_capacity)
        self._worker = Worker(buffer, Transport(resolved), resolved)
        self._max_capture_bytes = resolved.max_capture_bytes
        self._logger = resolved.logger
        self._redact = make_ocpp_redactor()

    def arm(self) -> None:
        self._worker.start()

    def enqueue(self, msg: OCPPMessage) -> None:
        self._worker.capture_ocpp(msg, self._redact)

    @property
    def max_capture_bytes(self) -> float:
        return self._max_capture_bytes

    @property
    def logger(self) -> logging.Logger | None:
        return self._logger

    def flush(self) -> None:
        self._worker.flush_once()

    def close(self, deadline: float | None = None) -> None:
        self._worker.close(deadline)


class _NoopEngine:
    def enqueue(self, msg: OCPPMessage) -> None:
        return None

    @property
    def max_capture_bytes(self) -> float:
        # Generous so the message-frame oversize check never short-circuits
        # here; the noop engine would drop the message anyway.
        return math.inf

    @property
    def logger(self) -> logging.Logger | None:
        return None

    def flush(self) -> None:
        return None

    def close(self, deadline: float | None = None) -> None:
        return None


class OCPPClient(BaseClient[Engine]):
    """Captures and ships OCPP CSMS traffic. Build with
    :meth:`OCPPClient.start` — a bad config never raises; it yields an inert
    no-op client.
    """

    def __init__(self, engine: Engine) -> None:
        """Internal — use :meth:`start`."""
        super().__init__(engine, _NoopEngine)

    @classmethod
    def start(cls, config: OCPPConfig) -> OCPPClient:
        """Build and start. Any fault yields an inert client; never raises to
        the host.
        """
        try:
            engine = _ActiveEngine(config)
            engine.arm()
        except Exception:  # noqa: BLE001
            return cls(_NoopEngine())
        return cls(engine)

    def connection(self, identity: ChargerIdentity) -> OCPPSession:
        """Open a capture session for one OCPP connection: mints a
        ``connection_id``, records the connect, and returns an
        :class:`OCPPSession`. Attach the handle to your WebSocket.
        """
        connection_id = str(uuid.uuid4())
        self.capture_connect(OCPPMessageInput(identity=identity, connection_id=connection_id))
        return OCPPSession(self, identity, connection_id)

    def capture_connect(self, msg: OCPPMessageInput) -> None:
        """Record a new OCPP connection. Uses ``identity`` + ``connection_id``
        only.
        """
        try:
            if not validate_charger_identity(msg.identity):
                return
            self._engine.enqueue(
                OCPPMessage(
                    event_type=OCPPEventType.CONNECT,
                    identity=msg.identity,
                    connection_id=msg.connection_id,
                )
            )
        except Exception as err:  # noqa: BLE001
            self._log_fault("capture_connect", err)

    def capture_message(self, msg: OCPPMessageInput) -> None:
        """Record one OCPP frame. Requires ``data`` + ``direction``; oversize
        ⇒ dropped.
        """
        try:
            if msg.data is None or msg.direction is None:
                return
            if not validate_charger_identity(msg.identity):
                return
            payload, overflowed = _encode_frame(msg.data, self._engine.max_capture_bytes)
            if overflowed:
                return
            self._engine.enqueue(
                OCPPMessage(
                    event_type=OCPPEventType.MESSAGE,
                    identity=msg.identity,
                    connection_id=msg.connection_id,
                    direction=msg.direction,
                    payload=payload,
                )
            )
        except Exception as err:  # noqa: BLE001
            self._log_fault("capture_message", err)

    def capture_disconnect(self, msg: OCPPMessageInput) -> None:
        """Record the connection closing. Uses ``identity`` + ``connection_id``
        only.
        """
        try:
            if not validate_charger_identity(msg.identity):
                return
            self._engine.enqueue(
                OCPPMessage(
                    event_type=OCPPEventType.DISCONNECT,
                    identity=msg.identity,
                    connection_id=msg.connection_id,
                )
            )
        except Exception as err:  # noqa: BLE001
            self._log_fault("capture_disconnect", err)

    def _log_fault(self, op: str, err: Exception) -> None:
        """Surface a swallowed capture fault when a debug logger is configured."""
        logger = self._engine.logger
        if logger is None:
            return
        logger.warning("evpanda: OCPP %s failed: %s", op, err)


def _encode_frame(data: bytes | str, maximum: float) -> tuple[bytes, bool]:
    """Encode a frame to bytes and signal overflow against the configured cap.

    Anything that isn't bytes-like or a string raises — the caller logs and
    drops it. (Notably `bytes(5)` would silently mint a 5-byte zero frame.)
    """
    if isinstance(data, str):
        buf = data.encode("utf-8")
    elif isinstance(data, bytes | bytearray | memoryview):
        buf = bytes(data)
    else:
        raise TypeError(f"OCPP frame must be bytes or str, got {type(data).__name__}")
    if len(buf) > maximum:
        return b"", True
    return buf, False
