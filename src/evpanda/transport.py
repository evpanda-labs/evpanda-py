"""Hand-rolled transport over :mod:`urllib.request`. Body: JSON; zstd by
default, gzip when configured, identity for tiny payloads. Owns the bounded
retry: 200 or 400/401/413 → done; 5xx/network → backoff; the caller never
retries. Never raises.

The actual ``POST /v1/{protocol}`` lives in ``Transport._post`` — no
generated client (it would pull heavy transitive deps into customer
production for two endpoints) and no separate wrapper class.
"""

from __future__ import annotations

import base64
import contextlib
import functools
import gzip
import json
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Literal, TypedDict

from .buffer import BufferedMessage
from .config import Compression, ResolvedConfig
from .types import OCPIMessage, OCPPMessage, Protocol

# ── zstd — optional ──────────────────────────────────────────────────────
#
# `zstandard` is an optional extra (`pip install evpanda[zstd]`), imported
# lazily; absent ⇒ gzip fallback. So the SDK has no hard runtime dependency.


@functools.cache
def _load_zstd() -> Callable[[bytes], bytes] | None:
    """Resolve zstd's one-shot compressor once; None when the extra is absent."""
    try:
        from zstandard import compress
    except ImportError:
        return None  # optional extra not installed — gzip is used instead
    return compress


# ── Backoff (module-private, fixed by design — not configurable) ─────────

_BACKOFF_BASE = 0.5
_BACKOFF_MAX = 30.0
_BACKOFF_MAX_ATTEMPTS = 5


def _next_delay(attempt: int) -> float:
    """Delay (seconds) before a retry attempt. Capped exponential with full
    jitter. The retry count is bounded by the ``send`` loop, not here.
    """
    capped = min(_BACKOFF_MAX, _BACKOFF_BASE * 2**attempt)
    return random.uniform(0, capped)


#: Per-attempt request cap so a hung connection still feeds the backoff.
_REQUEST_TIMEOUT = 30.0

type ContentEncoding = Literal["identity", "gzip", "zstd"]

#: Below this raw size, compression isn't worth the CPU; send identity.
_COMPRESS_MIN_BYTES = 1024

_HEADER_CONTENT_TYPE = "Content-Type"
_HEADER_CONTENT_ENCODING = "Content-Encoding"
_HEADER_API_KEY = "X-API-Key"
_CONTENT_TYPE_JSON = "application/json"


# ── Ingestion wire records ───────────────────────────────────────────────
#
# The exact request payload shapes the ingestion service accepts — keep in
# lock-step with that service and the Go/Node SDKs. Optional fields are
# `T | None`: an absent value serializes as JSON null, never a zero or
# omitted key.


class OcpiIngest(TypedDict):
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


class OcppIngest(TypedDict):
    charger_id: str
    connection_id: str
    tenant_id: str | None
    tenant_name: str | None
    captured_at: str
    event_type: int
    direction: str | None
    raw_frame: str | None


class IngestBody(TypedDict):
    messages: list[OcpiIngest | OcppIngest]


def _headers_json(h: dict[str, str] | None) -> dict[str, str] | None:
    """Header map for the wire, or None when empty."""
    if not h:
        return None
    return h


def _body_b64(b: bytes | None) -> str | None:
    """base64-encode a body/frame, or None when empty. Used for every byte
    payload the SDK ships — OCPI HTTP bodies AND OCPP wire frames. The
    ingest server decodes before persistence (so DB / consumers see plain
    UTF-8 for OCPP, raw bytes for OCPI). Rationale: keeps the wire contract
    uniform across protocols and binary-safe for any future payload.
    """
    if not b:
        return None
    return base64.standard_b64encode(b).decode("ascii")


def _opt_str(s: str | None) -> str | None:
    """Non-empty string, or None."""
    return None if s is None or s == "" else str(s)


def _opt_int(n: int | None) -> int | None:
    """Non-zero number, or None (0 is treated as absent, matching Go)."""
    return None if n is None or n == 0 else int(n)


def _ocpi_record(e: BufferedMessage, m: OCPIMessage) -> OcpiIngest:
    return OcpiIngest(
        captured_at=e.captured_at,
        platform_id=m.identity.platform_id,
        platform_name=m.identity.platform_name,
        tenant_id=_opt_str(m.identity.tenant_id),
        tenant_name=_opt_str(m.identity.tenant_name),
        direction=str(m.direction),
        http_method=m.data.method,
        url=m.data.url,
        response_status_code=_opt_int(m.data.status_code),
        request_headers=_headers_json(m.data.request_headers),
        request_body=_body_b64(m.data.request_body),
        response_headers=_headers_json(m.data.response_headers),
        response_body=_body_b64(m.data.response_body),
    )


def _ocpp_record(e: BufferedMessage, m: OCPPMessage) -> OcppIngest:
    return OcppIngest(
        charger_id=m.identity.charger_id,
        connection_id=m.connection_id,
        tenant_id=_opt_str(m.identity.tenant_id),
        tenant_name=_opt_str(m.identity.tenant_name),
        captured_at=e.captured_at,
        event_type=int(m.event_type),
        direction=_opt_str(m.direction),
        raw_frame=_body_b64(m.payload),
    )


def serialize(batch: list[BufferedMessage]) -> bytes:
    """Envelope list → JSON request body ``{"messages":[<record>,...]}``.

    Each message is mapped to the flat snake_case ingestion record by kind;
    bodies are base64 of the raw bytes. Wire shape must match the ingestion
    service.
    """
    messages: list[OcpiIngest | OcppIngest] = [
        _ocpi_record(e, e.message)
        if isinstance(e.message, OCPIMessage)
        else _ocpp_record(e, e.message)
        for e in batch
    ]
    body = IngestBody(messages=messages)
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


class Transport:
    """Serialize → compress → POST with bounded retry. Never raises."""

    def __init__(self, config: ResolvedConfig) -> None:
        self._endpoint = config.endpoint
        self._api_key = config.api_key
        self._compression: Compression = config.compression
        #: Records dropped batches; None means silent.
        self._logger = config.logger

    def _log_drop(self, protocol: Protocol, n: int, reason: str) -> None:
        """Records a dropped batch when the debug logger is configured."""
        if self._logger is None:
            return
        self._logger.warning(
            "evpanda: dropped batch (delivery failed): protocol=%s messages=%d reason=%s",
            protocol,
            n,
            reason,
        )

    def _post(self, protocol: Protocol, body: bytes, encoding: ContentEncoding) -> int:
        """Single POST /v1/{protocol}; drains the body, returns the status."""
        req = urllib.request.Request(f"{self._endpoint}/v1/{protocol}", data=body, method="POST")
        req.add_header(_HEADER_CONTENT_TYPE, _CONTENT_TYPE_JSON)
        req.add_header(_HEADER_API_KEY, self._api_key)
        if encoding != "identity":
            req.add_header(_HEADER_CONTENT_ENCODING, encoding)
        try:
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as res:
                res.read()  # drain so the socket can be released; body unused
                return int(res.status)
        except urllib.error.HTTPError as exc:
            # A non-2xx response is a status, not a transport failure.
            with contextlib.suppress(Exception):
                exc.read()
            return int(exc.code)

    def _compress(self, raw: bytes) -> tuple[bytes, ContentEncoding]:
        """Encode with the configured codec — identity for tiny payloads, gzip
        if zstd is requested but its optional extra is absent, identity on
        failure.
        """
        if len(raw) < _COMPRESS_MIN_BYTES:
            return raw, "identity"
        try:
            if self._compression == "zstd":
                zstd = _load_zstd()
                if zstd is not None:
                    return zstd(raw), "zstd"
                # zstd requested but the optional extra is absent — fall
                # through to gzip.
            return gzip.compress(raw), "gzip"
        except Exception:  # noqa: BLE001
            return raw, "identity"

    def send(self, protocol: Protocol, batch: list[BufferedMessage]) -> None:
        """Serialize → compress → POST with internal bounded retry. 200 is
        success; 400/401/413 is a permanent drop; 5xx/network errors back off
        and retry; a batch that can't be delivered is dropped. Never raises.
        """
        if not batch:
            return

        try:
            body, encoding = self._compress(serialize(batch))
        except Exception:  # noqa: BLE001
            return  # unserializable batch is dropped

        last_status = 0
        for attempt in range(_BACKOFF_MAX_ATTEMPTS):
            if attempt > 0:
                time.sleep(_next_delay(attempt))

            try:
                status = self._post(protocol, body, encoding)
            except Exception:  # noqa: BLE001
                last_status = 0
                continue  # network error / timeout → retryable
            last_status = status

            # 200 accepted; 400/401/413 permanent (drop, never retry — only
            # these three per the ingestion contract); any other non-2xx →
            # retryable.
            if status == 200:
                return
            if status in (400, 401, 413):
                self._log_drop(protocol, len(batch), f"permanent rejection: HTTP {status}")
                return

        # retries exhausted → batch dropped (loss acceptable by design)
        self._log_drop(
            protocol,
            len(batch),
            f"retries exhausted (last HTTP {last_status})"
            if last_status != 0
            else "retries exhausted (network error / timeout)",
        )
