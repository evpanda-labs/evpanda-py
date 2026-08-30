"""Hand-rolled transport over :mod:`urllib.request`.

Body: JSON, zstd-compressed above a size floor. It owns the bounded retry
— 200 or 400/401/413 is terminal, 5xx and network errors back off; the
caller never retries. It never raises.

The ``POST /v1/{protocol}`` call lives in :meth:`Transport._post`. No
generated client, which would pull heavy transitive dependencies into
customer production for two endpoints, and no third-party HTTP library:
:mod:`urllib.request` is stdlib and picks up ``HTTPS_PROXY`` and friends
the way an enterprise deployment expects.
"""

from __future__ import annotations

import base64
import json
import logging
import random
import time
import urllib.error
import urllib.request
from typing import Literal, TypedDict

import zstandard

from ._buffer import BufferedMessage
from ._config import LogMode, ResolvedConfig
from ._stats import Counters, DropReason
from ._types import Message, OCPIMessage, Protocol, coerce_body

# ── Compression ──────────────────────────────────────────────────────────
#
# zstd is the codec, and the SDK's one runtime dependency. Every EVPanda
# SDK compresses the same way, so a batch on the wire looks the same
# whichever language sent it, and the ingestion API's capacity planning
# holds across all of them. (It also accepts gzip and uncompressed bodies;
# neither is used.)

type ContentEncoding = Literal["identity", "zstd"]

#: Below this raw size compression is not worth the CPU; the payload is
#: sent as-is.
COMPRESS_MIN_BYTES = 1024


def compress(raw: bytes) -> tuple[bytes, ContentEncoding]:
    """Encode the body with zstd, above the size floor.

    An uncompressed body is always a safe answer — the ingestion API
    accepts one — so a codec fault degrades to identity rather than
    costing us the batch.
    """
    if len(raw) < COMPRESS_MIN_BYTES:
        return raw, "identity"
    try:
        return zstandard.compress(raw), "zstd"
    except Exception:  # noqa: BLE001 - see above
        return raw, "identity"


# ── Retry ────────────────────────────────────────────────────────────────
#
# Backoff bounds are fixed by design — not configurable.

BACKOFF_BASE = 0.5
BACKOFF_MAX = 30.0
BACKOFF_MAX_ATTEMPTS = 5

#: Caps a single POST attempt, so a hung connection still feeds the backoff
#: instead of stalling the worker.
REQUEST_TIMEOUT = 30.0


def next_delay(attempt: int) -> float:
    """The capped-exponential, fully-jittered backoff before a retry attempt."""
    capped = min(BACKOFF_MAX, BACKOFF_BASE * 2**attempt)
    return random.uniform(0, capped)


_HEADER_CONTENT_TYPE = "Content-Type"
_HEADER_CONTENT_ENCODING = "Content-Encoding"
_HEADER_API_KEY = "X-API-Key"
_CONTENT_TYPE_JSON = "application/json"

#: Statuses the ingestion contract defines as permanent. Everything else
#: that is not a 200 is retried.
PERMANENT_STATUSES = frozenset({400, 401, 413})


# ── Ingestion wire records ───────────────────────────────────────────────
#
# The exact request payload shapes the ingestion service accepts — keep
# these in lock-step with apispec/ingestion-api.yaml and the Go/Node SDKs.
# Optional fields are `T | None`: an absent value serializes as JSON null,
# never as a zero value or a missing key.


class OCPIIngest(TypedDict):
    captured_at: str
    platform_id: str
    platform_name: str
    tenant_id: str | None
    tenant_name: str | None
    direction: str
    http_method: str
    url: str
    response_status_code: int | None
    request_headers: dict[str, str] | None
    request_body: str | None
    response_headers: dict[str, str] | None
    response_body: str | None


class OCPPIngest(TypedDict):
    charger_id: str
    connection_id: str
    tenant_id: str | None
    tenant_name: str | None
    captured_at: str
    event_type: int
    direction: str | None
    raw_frame: str | None


class IngestBody(TypedDict):
    messages: list[OCPIIngest | OCPPIngest]


def _body_b64(body: bytes | None) -> str | None:
    """base64-encode a body or frame, or None to send null when empty.

    Every byte payload the SDK ships goes through this — OCPI HTTP bodies
    and OCPP wire frames alike. The ingest server decodes before
    persistence, so consumers see plain UTF-8; encoding keeps the wire
    contract uniform across protocols and binary-safe.
    """
    if not body:
        return None
    return base64.standard_b64encode(body).decode("ascii")


def _opt_str(value: str | None) -> str | None:
    """The value, or None for an empty string, so the field serializes as null."""
    return value or None


def _opt_int(value: int | None) -> int | None:
    """The value, or None for 0 (treated as absent), matching the Go SDK."""
    return value or None


def record(envelope: BufferedMessage) -> OCPIIngest | OCPPIngest:
    """Map one buffered capture onto its flat ingestion record."""
    message: Message = envelope.message
    if isinstance(message, OCPIMessage):
        data = message.data
        return OCPIIngest(
            captured_at=envelope.captured_at,
            platform_id=message.identity.id,
            platform_name=message.identity.name,
            tenant_id=_opt_str(message.identity.tenant_id),
            tenant_name=_opt_str(message.identity.tenant_name),
            direction=str(message.direction),
            http_method=data.method,
            url=data.url,
            response_status_code=_opt_int(data.status_code),
            request_headers=dict(data.request_headers) or None,
            request_body=_body_b64(coerce_body(data.request_body)),
            response_headers=dict(data.response_headers) or None,
            response_body=_body_b64(coerce_body(data.response_body)),
        )
    return OCPPIngest(
        charger_id=message.identity.id,
        connection_id=message.connection_id,
        tenant_id=_opt_str(message.identity.tenant_id),
        tenant_name=_opt_str(message.identity.tenant_name),
        captured_at=envelope.captured_at,
        event_type=int(message.event_type),
        direction=_opt_str(message.direction),
        raw_frame=_body_b64(coerce_body(message.payload)),
    )


def serialize(batch: list[BufferedMessage]) -> bytes:
    """Map the batch onto the wire records and encode the request envelope."""
    body = IngestBody(messages=[record(envelope) for envelope in batch])
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


# ── HTTP ─────────────────────────────────────────────────────────────────


class Transport:
    """Serialize, compress and POST a batch with bounded retry. Never raises."""

    __slots__ = ("_api_key", "_counters", "_endpoint", "_log_mode", "_logger", "_opener")

    def __init__(self, config: ResolvedConfig, counters: Counters) -> None:
        self._endpoint = config.endpoint
        self._api_key = config.api_key
        #: Records dropped batches; None means silent.
        self._logger: logging.Logger | None = config.logger
        self._log_mode = config.log_mode
        self._counters = counters
        #: The client's own opener rather than the module-level default,
        #: so a host that called `urllib.request.install_opener` does not
        #: end up routing the SDK's batches through it.
        self._opener = urllib.request.build_opener()

    def send(
        self,
        protocol: Protocol,
        batch: list[BufferedMessage],
        deadline: float | None = None,
    ) -> None:
        """Serialize, compress and POST the batch with bounded retry.

        200 or 400/401/413 is terminal; 5xx and network errors back off and
        retry. A batch that cannot be delivered is dropped — loss is
        acceptable by design, and the alternative is unbounded memory in
        the host.

        ``deadline`` is a :func:`time.monotonic` value past which no
        further attempt is started, so a shutdown drain cannot outlive the
        timeout the caller gave it.
        """
        if not batch:
            return
        size = len(batch)
        try:
            body, encoding = compress(serialize(batch))
        except Exception:  # noqa: BLE001 - an unserializable batch is dropped, not raised
            self._log_drop(protocol, size, "batch could not be serialized")
            return

        last_status = 0
        for attempt in range(BACKOFF_MAX_ATTEMPTS):
            if attempt > 0 and not self._sleep_before_retry(attempt, protocol, size, deadline):
                return

            timeout = self._attempt_timeout(deadline)
            if timeout <= 0:
                self._log_drop(protocol, size, "deadline passed before delivery")
                return
            try:
                status = self._post(protocol, body, encoding, timeout)
            except Exception:  # noqa: BLE001 - network error or timeout: retryable
                last_status = 0
                continue
            last_status = status

            if status == 200:
                return
            if status in PERMANENT_STATUSES:
                self._log_drop(protocol, size, f"permanent rejection: HTTP {status}")
                return

        reason = (
            f"retries exhausted (last HTTP {last_status})"
            if last_status
            else "retries exhausted (network error / timeout)"
        )
        self._log_drop(protocol, size, reason)

    def _sleep_before_retry(
        self, attempt: int, protocol: Protocol, size: int, deadline: float | None
    ) -> bool:
        """Wait out the backoff, or report False when the deadline forbids it."""
        delay = next_delay(attempt)
        if deadline is not None and time.monotonic() + delay >= deadline:
            self._log_drop(protocol, size, "deadline passed before retry")
            return False
        time.sleep(delay)
        return True

    @staticmethod
    def _attempt_timeout(deadline: float | None) -> float:
        """The per-attempt socket timeout, never past the caller's deadline."""
        if deadline is None:
            return REQUEST_TIMEOUT
        return min(REQUEST_TIMEOUT, deadline - time.monotonic())

    def _post(
        self, protocol: Protocol, body: bytes, encoding: ContentEncoding, timeout: float
    ) -> int:
        """Issue one POST, drain the response, and return the status code."""
        request = urllib.request.Request(
            f"{self._endpoint}/v1/{protocol}", data=body, method="POST"
        )
        request.add_header(_HEADER_CONTENT_TYPE, _CONTENT_TYPE_JSON)
        request.add_header(_HEADER_API_KEY, self._api_key)
        if encoding != "identity":
            request.add_header(_HEADER_CONTENT_ENCODING, encoding)
        try:
            with self._opener.open(request, timeout=timeout) as response:
                response.read()  # drain so the socket is released; the body is unused
                return int(response.status)
        except urllib.error.HTTPError as exc:
            # A non-2xx response is a status, not a transport failure.
            try:
                exc.read()
            finally:
                exc.close()
            return int(exc.code)

    def _log_drop(self, protocol: Protocol, count: int, reason: str) -> None:
        """Count a dropped batch, and log it per-occurrence only in DEBUG.

        In the default mode the worker's once-a-minute health line reports
        the same loss with bounded volume — an outage would otherwise emit
        a line every flush interval, for as long as it lasts, across every
        client at once.
        """
        self._counters.count_drop(DropReason.UNDELIVERABLE, count)
        if self._logger is None or self._log_mode is not LogMode.DEBUG:
            return
        self._logger.warning(
            "evpanda: dropped batch (delivery failed) protocol=%s messages=%d reason=%s",
            protocol,
            count,
            reason,
        )
