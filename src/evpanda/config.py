"""Customer-facing configuration. The protocol is the class — there is no
``network_type`` field. Common fields live on :class:`BaseConfig`;
per-protocol extensions add fields only that protocol's client cares about.

Intervals are float **seconds** (Python's unit for :func:`time.sleep`,
``Event.wait`` and socket timeouts), not the milliseconds the Node SDK uses.
"""

from __future__ import annotations

import contextlib
import logging
import math
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Literal, TypedDict
from urllib.parse import urlparse

from .types import Protocol

type Compression = Literal["gzip", "zstd"]


@dataclass
class BaseConfig:
    """Fields shared by every protocol's client."""

    #: Ingestion API base, e.g. https://ingest.evpanda.io
    endpoint: str
    #: Sent as X-API-Key. If empty, falls back to the EVPANDA_API_KEY env
    #: var; one of the two must be set.
    api_key: str | None = None

    #: Ring buffer slots. Worst-case mem = buffer_capacity × max_capture_bytes.
    buffer_capacity: int | None = None
    #: Per-body / per-frame capture cap in bytes.
    max_capture_bytes: int | None = None
    #: Worker flush cadence in seconds; ~5–10s.
    flush_interval: float | None = None
    #: close() drain deadline in seconds. Default 10.0; explicit value must be ≥ 5.0.
    drain_timeout: float | None = None
    #: Default "zstd". "zstd" needs the optional extra (else gzip fallback).
    compression: Compression | None = None

    #: Master log switch; default False (totally silent).
    debug: bool = False
    logger: logging.Logger | None = None


@dataclass
class OCPIConfig(BaseConfig):
    """Configuration for an OCPI roaming gateway client."""

    #: Extra headers to capture on top of the default allowlist; can't
    #: disable defaults.
    ocpi_allowed_headers: Iterable[str] | None = None


@dataclass
class OCPPConfig(BaseConfig):
    """Configuration for an OCPP CSMS client. No protocol-specific fields today."""


class ConfigError(Exception):
    """Raised by the ``resolve_*_config`` functions on a hard-invalid config.

    Only ``endpoint`` and ``api_key`` are hard-required; a client catches
    this and runs inert. Tunable fields never raise — they warn and fall
    back to their default.
    """


# ── Resolved shapes — internal; clients build these from the user config ──


@dataclass(frozen=True)
class ResolvedBaseConfig:
    endpoint: str
    api_key: str
    protocol: Protocol
    buffer_capacity: int
    max_capture_bytes: int
    flush_interval: float
    drain_timeout: float
    compression: Compression
    debug: bool
    #: Non-None only when debug is True.
    logger: logging.Logger | None = None


@dataclass(frozen=True)
class ResolvedOCPIConfig(ResolvedBaseConfig):
    ocpi_allowed_headers: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResolvedOCPPConfig(ResolvedBaseConfig):
    pass


#: The union the worker / transport accept — they only read the base fields.
type ResolvedConfig = ResolvedOCPIConfig | ResolvedOCPPConfig


#: ≤ 1000 server batch cap is the flush trigger; capacity is larger.
_DEFAULT_BUFFER_CAPACITY = 10_000
_DEFAULT_MAX_CAPTURE_BYTES = 64 * 1024
_DEFAULT_FLUSH_INTERVAL = 5.0
_DEFAULT_DRAIN_TIMEOUT = 10.0
_DEFAULT_COMPRESSION: Compression = "zstd"

#: Lower bounds for the tunable fields. Anything below warns and defaults.
_MIN_FLUSH_INTERVAL = 0.001
_MIN_DRAIN_TIMEOUT = 5.0

#: Fallback source for api_key when config.api_key is empty.
_API_KEY_ENV_VAR = "EVPANDA_API_KEY"

_ERR = "evpanda config"

#: Warn sink for the tunable-field resolvers; logs only when ``debug=True``.
type _Warn = Callable[[str], None]


def _make_warn(logger: logging.Logger | None) -> _Warn:
    """Build the warn sink over an already-resolved logger — silent when that
    is None, which is the case unless ``debug`` is on. Taking the logger (not
    the raw config) keeps the "logger only when debug" rule in one place.
    """

    def warn(msg: str) -> None:
        if logger is None:
            return
        # A malformed customer logger must not fail config resolution.
        with contextlib.suppress(Exception):
            logger.warning("%s: %s", _ERR, msg)

    return warn


def _resolve_int(value: Any, fallback: int, field_name: str, minimum: int, warn: _Warn) -> int:
    """None or invalid (non-integer / below min) ⇒ fallback (+ warn)."""
    if value is None:
        return fallback
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        warn(f"`{field_name}` must be an integer >= {minimum}; using default {fallback}")
        return fallback
    return value


def _resolve_seconds(
    value: Any, fallback: float, field_name: str, minimum: float, warn: _Warn
) -> float:
    """None or invalid (non-number / NaN / below min) ⇒ fallback (+ warn)."""
    if value is None:
        return fallback
    if isinstance(value, bool) or not isinstance(value, int | float) or math.isnan(value):
        warn(f"`{field_name}` must be a number >= {minimum}; using default {fallback}")
        return fallback
    if value < minimum:
        warn(f"`{field_name}` must be a number of seconds >= {minimum}; using default {fallback}")
        return fallback
    return float(value)


def _resolve_endpoint(raw: Any) -> str:
    if not isinstance(raw, str) or raw.strip() == "":
        raise ConfigError(f"{_ERR}: `endpoint` is required and must be a non-empty string")
    s = raw.strip()
    parsed = urlparse(s)
    if not parsed.netloc:
        raise ConfigError(f"{_ERR}: `endpoint` must be a valid URL")
    if parsed.scheme not in ("http", "https"):
        raise ConfigError(f"{_ERR}: `endpoint` must use http or https")
    return s.rstrip("/")  # transport appends /v1/{protocol}


def _resolve_api_key(value: Any) -> str:
    """config.api_key, or the EVPANDA_API_KEY env var, or raises if neither
    is set."""
    if isinstance(value, str) and value.strip() != "":
        return value.strip()
    env = os.environ.get(_API_KEY_ENV_VAR)
    if env is not None and env.strip() != "":
        return env.strip()
    raise ConfigError(
        f"{_ERR}: `api_key` is required — set `api_key` or the {_API_KEY_ENV_VAR} env var"
    )


def _resolve_compression(value: Any, warn: _Warn) -> Compression:
    """None or invalid ⇒ "zstd" default (+ warn); else the given codec."""
    if value is None:
        return _DEFAULT_COMPRESSION
    if value in ("gzip", "zstd"):
        return value  # type: ignore[no-any-return]
    warn(f'`compression` must be "gzip" or "zstd"; using default {_DEFAULT_COMPRESSION}')
    return _DEFAULT_COMPRESSION


def _resolve_ocpi_allowed_headers(value: Any, warn: _Warn) -> tuple[str, ...]:
    """Coerce to a trimmed, lowercased, deduplicated, immutable tuple. A
    non-iterable (or bare string) value falls back to ``()`` (+ warn); a
    non-string entry is skipped (+ warn) so the good entries still apply.
    """
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Iterable):
        warn("`ocpi_allowed_headers` must be an iterable of strings; ignoring it")
        return ()
    out: dict[str, None] = {}  # insertion-ordered set
    for v in value:
        if not isinstance(v, str):
            warn("`ocpi_allowed_headers` entries must be strings; skipping one")
            continue
        trimmed = v.strip().lower()
        if trimmed:
            out[trimmed] = None
    return tuple(out)


class _BaseFields(TypedDict):
    """The resolved base fields, unpacked into a per-protocol resolved config."""

    endpoint: str
    api_key: str
    protocol: Protocol
    buffer_capacity: int
    max_capture_bytes: int
    flush_interval: float
    drain_timeout: float
    compression: Compression
    debug: bool
    logger: logging.Logger | None


def _resolve_base(config: BaseConfig, protocol: Protocol) -> _BaseFields:
    """Resolve the shared fields. ``endpoint`` / ``api_key`` are
    hard-required — a bad value raises (⇒ inert client); tunable fields fall
    back to their default.
    """
    if not isinstance(config, BaseConfig):
        raise ConfigError(f"{_ERR}: a config object is required")
    debug = config.debug is True
    # debug without an explicit logger uses the package logger (Python's
    # equivalent of the Node SDK falling back to `console`).
    logger = (config.logger or logging.getLogger("evpanda")) if debug else None
    warn = _make_warn(logger)
    return {
        "endpoint": _resolve_endpoint(config.endpoint),
        "api_key": _resolve_api_key(config.api_key),
        "protocol": protocol,
        "buffer_capacity": _resolve_int(
            config.buffer_capacity, _DEFAULT_BUFFER_CAPACITY, "buffer_capacity", 1, warn
        ),
        "max_capture_bytes": _resolve_int(
            config.max_capture_bytes, _DEFAULT_MAX_CAPTURE_BYTES, "max_capture_bytes", 1, warn
        ),
        "flush_interval": _resolve_seconds(
            config.flush_interval,
            _DEFAULT_FLUSH_INTERVAL,
            "flush_interval",
            _MIN_FLUSH_INTERVAL,
            warn,
        ),
        "drain_timeout": _resolve_seconds(
            config.drain_timeout,
            _DEFAULT_DRAIN_TIMEOUT,
            "drain_timeout",
            _MIN_DRAIN_TIMEOUT,
            warn,
        ),
        "compression": _resolve_compression(config.compression, warn),
        "debug": debug,
        "logger": logger,
    }


def resolve_ocpi_config(config: OCPIConfig) -> ResolvedOCPIConfig:
    base = _resolve_base(config, Protocol.OCPI)
    return ResolvedOCPIConfig(
        **base,
        ocpi_allowed_headers=_resolve_ocpi_allowed_headers(
            getattr(config, "ocpi_allowed_headers", None), _make_warn(base["logger"])
        ),
    )


def resolve_ocpp_config(config: OCPPConfig) -> ResolvedOCPPConfig:
    return ResolvedOCPPConfig(**_resolve_base(config, Protocol.OCPP))
