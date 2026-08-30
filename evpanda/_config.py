"""Customer-facing configuration.

The protocol is the client — there is no network-type field. Common fields
live on :class:`BaseConfig`; per-protocol configs add only what that
protocol's client cares about.

``api_key`` is the one field with no usable default, so a missing key fails
:func:`evpanda.start_ocpi` / :func:`evpanda.start_ocpp` (which hand back an
inert client carrying the error). A malformed ``endpoint`` fails the same
way — but an empty one is not malformed, it just means production. Every
other field is tunable: a bad value falls back to its default and says so
in the host's logs, so a typo can never silence the SDK entirely.

Durations are float **seconds**, Python's unit for :func:`time.sleep`,
``Event.wait`` and socket timeouts.
"""

from __future__ import annotations

import contextlib
import logging
import math
import os
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

from ._types import Protocol


class LogMode(StrEnum):
    """How much the SDK says for itself.

    Unset means the ``EVPANDA_LOG`` environment variable decides, and
    failing that :attr:`ERRORS`.

    The default is deliberately not silence. An SDK that captures nothing
    because its identity resolution is misconfigured looks exactly like an
    SDK on an idle system, and a customer should not have to redeploy with
    a debug flag to tell those apart. What it will not do is log per
    event: problems are summarized once a minute, so a fault that occurs
    on every request still costs one line, and a healthy client says
    nothing at all.
    """

    #: Disable SDK logging completely. :meth:`~evpanda.OCPIClient.stats` keeps working.
    SILENT = "silent"
    #: The default: config problems at startup, plus a once-a-minute
    #: summary whenever captures are being dropped.
    ERRORS = "errors"
    #: Adds per-batch delivery failures, recovered capture faults, and a
    #: summary line on close even when nothing went wrong.
    DEBUG = "debug"


#: Lets an operator change the setting without a code change — including
#: turning the SDK silent during an incident, which is the case that most
#: needs a restart-only escape hatch.
LOG_MODE_ENV_VAR = "EVPANDA_LOG"

#: The fallback source for ``api_key`` when the config field is empty.
API_KEY_ENV_VAR = "EVPANDA_API_KEY"


# ── Errors ───────────────────────────────────────────────────────────────
#
# start_ocpi / start_ocpp never raise: the client they return carries the
# failure on `.error`. The hierarchy is what `errors.Is` gives the Go SDK —
# match ConfigError for any configuration fault, or one of the two
# subclasses to tell a deployment problem (no key) from a code one (bad
# endpoint).


class EVPandaError(Exception):
    """Base class for every error this SDK produces."""


class ConfigError(EVPandaError):
    """A configuration fault. Both specific config errors subclass it."""


class APIKeyError(ConfigError):
    """No API key was found in the config or ``EVPANDA_API_KEY``."""


class EndpointError(ConfigError):
    """``endpoint`` is not a valid http(s) URL. An empty one is not an error."""


# ── Configuration ────────────────────────────────────────────────────────


@dataclass
class BaseConfig:
    """Fields shared by :class:`OCPIConfig` and :class:`OCPPConfig`.

    Every field except ``endpoint`` and ``api_key`` falls back to a
    default when left at ``None``.
    """

    #: Ingestion API base. Empty uses the production default,
    #: ``https://ingest.evpanda.io``; set it to reach a different
    #: environment. A non-empty value must be a valid http(s) URL.
    endpoint: str | None = None
    #: Sent as the ``X-API-Key`` header. If empty it falls back to the
    #: ``EVPANDA_API_KEY`` environment variable; one of the two must be set.
    api_key: str | None = None

    #: The ceiling on everything held in memory awaiting delivery. Past it
    #: the oldest captures are evicted, so this is the SDK's memory
    #: footprint, not an estimate of it. None uses the default (32 MiB);
    #: the buffer grows on demand and idles far below it.
    max_buffer_bytes: int | None = None
    #: The per-body / per-frame capture cap, enforced at capture: an
    #: oversize body or frame drops the whole message. None uses the
    #: default (65536).
    max_capture_bytes: int | None = None
    #: Maximum seconds between flushes. None uses the default (5.0).
    flush_interval: float | None = None
    #: How many seconds ``close()`` waits to drain buffered messages. None
    #: uses the default (10.0); an explicit value must be >= 5.0.
    drain_timeout: float | None = None
    #: How much the SDK logs. None consults ``EVPANDA_LOG``, then falls
    #: back to :attr:`LogMode.ERRORS`.
    log_mode: LogMode | str | None = None
    #: Receives the SDK's own logs. None uses ``logging.getLogger("evpanda")``.
    logger: logging.Logger | None = None


@dataclass
class OCPIConfig(BaseConfig):
    """Configuration for :func:`evpanda.start_ocpi`."""

    #: Extends the default capture allowlist with additional header names
    #: (matched case-insensitively). It can only extend the list, never
    #: shrink it.
    ocpi_allowed_headers: Iterable[str] = field(default_factory=tuple)


@dataclass
class OCPPConfig(BaseConfig):
    """Configuration for :func:`evpanda.start_ocpp`. No protocol-specific fields today."""


@dataclass(frozen=True, slots=True)
class ResolvedConfig:
    """A config with defaults applied and validation passed.

    Internal: it is what the worker and the transport read.
    """

    endpoint: str
    api_key: str
    protocol: Protocol
    max_buffer_bytes: int
    max_capture_bytes: int
    flush_interval: float
    drain_timeout: float
    log_mode: LogMode
    #: The effective logger: None exactly when ``log_mode`` is SILENT, so a
    #: None check is the only silence test callers need.
    logger: logging.Logger | None
    #: The lowercased extra allowlist (OCPI only).
    allowed_headers: tuple[str, ...] = ()


# ── Defaults and bounds ──────────────────────────────────────────────────

#: The production ingestion API. A host that never sets ``endpoint``
#: reaches it, which is what almost every host wants; staging deployments
#: set the field.
DEFAULT_ENDPOINT = "https://ingest.evpanda.io"

#: Covers roughly one full retry window of a 10 000-charger CSMS (~400
#: msg/s at ~500 B) — enough to ride out a blip, small enough to sit inside
#: an ordinary container limit.
DEFAULT_MAX_BUFFER_BYTES = 32 << 20  # 32 MiB
DEFAULT_MAX_CAPTURE_BYTES = 64 * 1024
DEFAULT_FLUSH_INTERVAL = 5.0
DEFAULT_DRAIN_TIMEOUT = 10.0

#: One default-sized capture; below it the buffer could not hold even a
#: single message.
MIN_MAX_BUFFER_BYTES = 64 << 10  # 64 KiB
MIN_FLUSH_INTERVAL = 0.001
MIN_DRAIN_TIMEOUT = 5.0

_ERR_PREFIX = "evpanda: config"


# ── Resolution ───────────────────────────────────────────────────────────


class _Warn:
    """The sink the tunable-field resolvers report to.

    A no-op when the resolved logger is None (SILENT), which keeps the
    silence rule in one place. A host logger that raises must not fail
    config resolution either.
    """

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger | None) -> None:
        self._logger = logger

    def __call__(self, message: str) -> None:
        if self._logger is None:
            return
        # A host logger that raises must not fail config resolution.
        with contextlib.suppress(Exception):
            self._logger.warning("%s: %s", _ERR_PREFIX, message)


def _resolve_log_mode(value: LogMode | str | None) -> tuple[LogMode, str | None]:
    """Apply the config field, then the environment, then the default.

    An unrecognised value in either falls back to ERRORS — a typo must not
    silence the SDK, which is the whole point of the default.

    Config wins over the environment, matching how ``api_key`` resolves.
    Since most hosts never set the field, ``EVPANDA_LOG`` still reaches
    almost every deployment, which is what makes it usable as an incident
    escape hatch.
    """
    if value is not None:
        try:
            return LogMode(str(value).strip().lower()), None
        except ValueError:
            return LogMode.ERRORS, (
                f"`log_mode` must be one of {_log_mode_values()}; using {LogMode.ERRORS}"
            )

    raw = os.environ.get(LOG_MODE_ENV_VAR, "").strip().lower()
    if raw == "":
        return LogMode.ERRORS, None
    try:
        return LogMode(raw), None
    except ValueError:
        return LogMode.ERRORS, (
            f"{LOG_MODE_ENV_VAR} must be one of {_log_mode_values()}; using {LogMode.ERRORS}"
        )


def _log_mode_values() -> str:
    return ", ".join(f'"{m}"' for m in LogMode)


def _effective_logger(logger: logging.Logger | None, mode: LogMode) -> logging.Logger | None:
    """The logger to use, or None when the mode is silent.

    A None logger is the single signal for "say nothing".
    """
    if mode is LogMode.SILENT:
        return None
    return logger if isinstance(logger, logging.Logger) else logging.getLogger("evpanda")


def _resolve_api_key(value: Any) -> str:
    """The configured key, or ``EVPANDA_API_KEY``, or raise."""
    if isinstance(value, str) and value.strip() != "":
        return value.strip()
    env = os.environ.get(API_KEY_ENV_VAR, "")
    if env.strip() != "":
        return env.strip()
    raise APIKeyError(
        f"{_ERR_PREFIX}: no API key — set `api_key` or the {API_KEY_ENV_VAR} environment variable"
    )


def _resolve_endpoint(raw: Any) -> str:
    """Default an empty value to production, else require a valid http(s) URL.

    Trailing slashes are trimmed; the transport appends ``/v1/{protocol}``.
    """
    if raw is None:
        return DEFAULT_ENDPOINT
    if not isinstance(raw, str):
        raise EndpointError(f"{_ERR_PREFIX}: `endpoint` must be a string")
    value = raw.strip()
    if value == "":
        return DEFAULT_ENDPOINT
    parsed = urlparse(value)
    if not parsed.netloc:
        raise EndpointError(f"{_ERR_PREFIX}: `endpoint` {value!r} is not a valid URL")
    if parsed.scheme not in ("http", "https"):
        raise EndpointError(f"{_ERR_PREFIX}: `endpoint` {value!r} must use http or https")
    return value.rstrip("/")


def _resolve_int(value: Any, fallback: int, name: str, minimum: int, warn: _Warn) -> int:
    """None, the wrong type, or below the minimum ⇒ the default (warning on the last two)."""
    if value is None:
        return fallback
    if isinstance(value, bool) or not isinstance(value, int):
        warn(f"`{name}` must be an integer; using default {fallback}")
        return fallback
    if value < minimum:
        warn(f"`{name}` must be >= {minimum}; using default {fallback}")
        return fallback
    return value


def _resolve_seconds(value: Any, fallback: float, name: str, minimum: float, warn: _Warn) -> float:
    """None, the wrong type, NaN, or below the minimum ⇒ the default."""
    if value is None:
        return fallback
    if isinstance(value, bool) or not isinstance(value, int | float) or math.isnan(value):
        warn(f"`{name}` must be a number of seconds; using default {fallback}")
        return fallback
    if value < minimum:
        warn(f"`{name}` must be >= {minimum}; using default {fallback}")
        return fallback
    return float(value)


def _resolve_allowed_headers(value: Any, warn: _Warn) -> tuple[str, ...]:
    """Trim, lowercase and deduplicate (preserving order), skipping empties.

    A bare string or a non-iterable is ignored with a warning, so one
    mistyped field cannot take the allowlist down with it.
    """
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Iterable):
        warn("`ocpi_allowed_headers` must be an iterable of strings; ignoring it")
        return ()
    out: dict[str, None] = {}  # an insertion-ordered set
    for item in value:
        if not isinstance(item, str):
            warn("`ocpi_allowed_headers` entries must be strings; skipping one")
            continue
        name = item.strip().lower()
        if name:
            out[name] = None
    return tuple(out)


def logger_for(config: BaseConfig) -> logging.Logger | None:
    """The logger a config would use, without validating anything else.

    ``start_ocpi`` / ``start_ocpp`` need it on the one path where there is
    no resolved config to read it from: reporting the fault that stopped
    the config from resolving at all.
    """
    mode, _ = _resolve_log_mode(config.log_mode)
    return _effective_logger(config.logger, mode)


def _resolve_base(config: BaseConfig, protocol: Protocol) -> ResolvedConfig:
    """Apply defaults and validate the shared fields.

    Only ``endpoint`` and ``api_key`` can fail.
    """
    log_mode, mode_warning = _resolve_log_mode(config.log_mode)
    logger = _effective_logger(config.logger, log_mode)
    warn = _Warn(logger)
    if mode_warning:
        warn(mode_warning)

    resolved = ResolvedConfig(
        endpoint=_resolve_endpoint(config.endpoint),
        api_key=_resolve_api_key(config.api_key),
        protocol=protocol,
        max_buffer_bytes=_resolve_int(
            config.max_buffer_bytes,
            DEFAULT_MAX_BUFFER_BYTES,
            "max_buffer_bytes",
            MIN_MAX_BUFFER_BYTES,
            warn,
        ),
        max_capture_bytes=_resolve_int(
            config.max_capture_bytes,
            DEFAULT_MAX_CAPTURE_BYTES,
            "max_capture_bytes",
            1,
            warn,
        ),
        flush_interval=_resolve_seconds(
            config.flush_interval,
            DEFAULT_FLUSH_INTERVAL,
            "flush_interval",
            MIN_FLUSH_INTERVAL,
            warn,
        ),
        drain_timeout=_resolve_seconds(
            config.drain_timeout,
            DEFAULT_DRAIN_TIMEOUT,
            "drain_timeout",
            MIN_DRAIN_TIMEOUT,
            warn,
        ),
        log_mode=log_mode,
        logger=logger,
    )

    # Both values are individually legal but nonsensical together: a
    # capture at the per-message cap would never fit in the buffer, so
    # every large message would be dropped after being redacted.
    if resolved.max_buffer_bytes < resolved.max_capture_bytes:
        warn(
            f"`max_buffer_bytes` ({resolved.max_buffer_bytes}) is below `max_capture_bytes` "
            f"({resolved.max_capture_bytes}); a full-size capture can never be buffered"
        )
    return resolved


def resolve_ocpi_config(config: OCPIConfig) -> ResolvedConfig:
    """Resolve an :class:`OCPIConfig`. Raises :class:`ConfigError` on a hard fault."""
    resolved = _resolve_base(config, Protocol.OCPI)
    headers = _resolve_allowed_headers(config.ocpi_allowed_headers, _Warn(resolved.logger))
    return replace(resolved, allowed_headers=headers)


def resolve_ocpp_config(config: OCPPConfig) -> ResolvedConfig:
    """Resolve an :class:`OCPPConfig`. Raises :class:`ConfigError` on a hard fault."""
    return _resolve_base(config, Protocol.OCPP)
