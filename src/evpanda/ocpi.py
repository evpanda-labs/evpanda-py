"""OCPIClient — passive OCPI traffic capture, plus the redaction applied at
the capture chokepoint. Public surface: :meth:`OCPIClient.start`,
``capture_inbound_message``, ``capture_outbound_message``, ``flush``,
``close``.

Redaction has two rules:

1. Header allowlist — only listed headers are kept; Authorization, Cookie,
   X-API-Key, etc. fall off the end. ``ocpi_allowed_headers`` extends the
   list, never shrinks it.
2. Credentials-endpoint token mask — on a ``/credentials`` URL the ``token``
   field (root for requests, under ``data`` for the response envelope) is
   replaced with ``[redacted]``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from .buffer import RingBuffer
from .client import BaseClient
from .config import OCPIConfig, resolve_ocpi_config
from .transport import Transport
from .types import HttpExchange, OCPIDirection, OCPIMessage, OCPIMessageInput
from .worker import OCPIRedactor, Worker

#: Stock OCPI headers safe to capture — none of these can carry a secret.
DEFAULT_OCPI_HEADER_ALLOWLIST: tuple[str, ...] = (
    # OCPI routing
    "ocpi-from-country-code",
    "ocpi-from-party-id",
    "ocpi-to-country-code",
    "ocpi-to-party-id",
    # Content negotiation + standard HTTP
    "content-type",
    "accept",
    "user-agent",
    # Tracing
    "x-correlation-id",
    "x-request-id",
    # Pagination
    "x-total-count",
    "x-limit",
    "link",
)

#: Placeholder string written in place of redacted token values.
TOKEN_PLACEHOLDER = "[redacted]"

#: URL ends with ``/credentials``, ``/credentials/``, or ``/credentials?…``.
#: Sub-paths like ``/credentials/foo`` don't match — no such OCPI route.
_CREDENTIALS_URL = re.compile(r"/credentials/?(?:\?|$)", re.IGNORECASE)


def make_ocpi_redactor(extra_allowed_headers: Iterable[str] = ()) -> OCPIRedactor:
    """Build a redactor closure from the resolved config. Called once at
    client construction; the allowlist set is amortized across every message.
    """
    allow = {h.lower() for h in (*DEFAULT_OCPI_HEADER_ALLOWLIST, *extra_allowed_headers)}

    def redact(msg: OCPIMessage) -> OCPIMessage:
        return OCPIMessage(
            direction=msg.direction,
            identity=msg.identity,
            data=_redact_http(msg.data, allow),
        )

    return redact


def _redact_http(data: HttpExchange, allow: set[str]) -> HttpExchange:
    """Apply the allowlist + credentials-token mask to a captured HTTP envelope."""
    return HttpExchange(
        method=data.method,
        url=data.url,
        status_code=data.status_code,
        request_headers=_filter_headers(data.request_headers, allow),
        response_headers=_filter_headers(data.response_headers, allow),
        request_body=_mask_credentials_token(data.request_body, data.url),
        response_body=_mask_credentials_token(data.response_body, data.url),
    )


def _filter_headers(headers: dict[str, str], allow: set[str]) -> dict[str, str]:
    """Keep only allowlisted headers; case-insensitive on the key."""
    return {k: v for k, v in headers.items() if k.lower() in allow}


def _mask_credentials_token(body: bytes | None, url: str) -> bytes | None:
    """Mask ``token`` in an OCPI credentials body. Returns the original bytes
    on any miss (non-credentials URL, non-JSON, no token at either known path,
    re-encode error) — redaction never silently drops data it couldn't safely
    rewrite.
    """
    if not body or not _CREDENTIALS_URL.search(url):
        return body

    try:
        parsed = json.loads(body.decode("utf-8", errors="replace"))
    except ValueError:
        return body
    if not isinstance(parsed, dict):
        return body

    # Token lives at the root (request) or under `data` (response envelope).
    # We own `parsed` — no external alias — so we mutate in place.
    token = parsed.get("token")
    if isinstance(token, str) and token:
        parsed["token"] = TOKEN_PLACEHOLDER
    elif _is_credentials_data(parsed.get("data")):
        parsed["data"]["token"] = TOKEN_PLACEHOLDER
    else:
        return body

    try:
        return json.dumps(parsed, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError):
        return body


def _is_credentials_data(data: Any) -> bool:
    """Guard: ``data`` is an object with a non-empty string ``token``."""
    if not isinstance(data, dict):
        return False
    token = data.get("token")
    return isinstance(token, str) and token != ""


@dataclass(frozen=True)
class SdkInternal:
    """Package-private channel from a client to its adapters, carried on the
    client's ``_internal`` field. Not part of the public API.
    """

    #: Resolved per-body cap; adapters use it to bound streaming accumulation.
    max_capture_bytes: int
    #: Effective logger (set only when ``debug=True``); adapters log faults here.
    logger: logging.Logger | None = None


class Engine(Protocol):
    def capture_message(self, msg: OCPIMessage) -> None: ...

    def flush(self) -> None: ...

    def close(self, deadline: float | None = None) -> None: ...


class _ActiveEngine:
    """Live engine. Building it has no side effects; ``arm`` starts the worker."""

    def __init__(self, config: OCPIConfig) -> None:
        resolved = resolve_ocpi_config(config)
        self._worker = Worker(
            RingBuffer(resolved.buffer_capacity),
            Transport(resolved),
            resolved,
        )
        self._redact = make_ocpi_redactor(resolved.ocpi_allowed_headers)
        #: Snapshot of resolved fields adapters need; exposed via the bridge.
        self.bridge = SdkInternal(
            max_capture_bytes=resolved.max_capture_bytes,
            logger=resolved.logger,
        )

    def arm(self) -> None:
        self._worker.start()

    def capture_message(self, msg: OCPIMessage) -> None:
        self._worker.capture_ocpi(msg, self._redact)

    def flush(self) -> None:
        self._worker.flush_once()

    def close(self, deadline: float | None = None) -> None:
        self._worker.close(deadline)


class _NoopEngine:
    """Inert twin used when construction failed or after ``close``."""

    def capture_message(self, msg: OCPIMessage) -> None:
        return None

    def flush(self) -> None:
        return None

    def close(self, deadline: float | None = None) -> None:
        return None


class OCPIClient(BaseClient[Engine]):
    """Captures and ships OCPI roaming traffic. Build with
    :meth:`OCPIClient.start` — a bad config never raises; it yields an inert
    no-op client.
    """

    def __init__(self, engine: Engine, internal: SdkInternal | None = None) -> None:
        """Internal — use :meth:`start`."""
        super().__init__(engine, _NoopEngine)
        #: Adapter-only snapshot; None on an inert client — which is how
        #: adapters short-circuit to a pass-through — and cleared by `close`
        #: so a closed client stops doing capture work. Not public API.
        self._internal = internal

    @classmethod
    def start(cls, config: OCPIConfig) -> OCPIClient:
        """Build and start. Any fault yields an inert client; never raises to
        the host.
        """
        try:
            engine = _ActiveEngine(config)
            engine.arm()
        except Exception:  # noqa: BLE001
            return cls(_NoopEngine())
        return cls(engine, engine.bridge)

    def close(self, deadline: float | None = None) -> None:
        """Go inert, then drain. Dropping the channel first means adapters
        wrapped around this client fall back to their zero-overhead
        pass-through instead of resolving identities and buffering bodies into
        a no-op engine. Idempotent; never raises.
        """
        self._internal = None
        super().close(deadline)

    def capture_inbound_message(self, msg: OCPIMessageInput) -> None:
        """Buffer an inbound OCPI message (partner → host). Non-blocking;
        never raises.
        """
        self._capture(msg, OCPIDirection.IN)

    def capture_outbound_message(self, msg: OCPIMessageInput) -> None:
        """Buffer an outbound OCPI message (host → partner). Non-blocking;
        never raises.
        """
        self._capture(msg, OCPIDirection.OUT)

    def _capture(self, msg: OCPIMessageInput, direction: OCPIDirection) -> None:
        """Stamp the direction and hand the full message to the engine."""
        with contextlib.suppress(Exception):
            self._engine.capture_message(
                OCPIMessage(direction=direction, identity=msg.identity, data=msg.data)
            )
