"""Config resolution: what is hard-required, what falls back, what warns."""

from __future__ import annotations

import logging

import pytest

import evpanda
from evpanda._config import (
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_ENDPOINT,
    DEFAULT_FLUSH_INTERVAL,
    DEFAULT_MAX_BUFFER_BYTES,
    DEFAULT_MAX_CAPTURE_BYTES,
    LogMode,
    _effective_logger,
    _resolve_allowed_headers,
    _resolve_endpoint,
    _resolve_log_mode,
    _Warn,
    resolve_ocpi_config,
    resolve_ocpp_config,
)
from evpanda._types import Protocol


def test_defaults_are_applied() -> None:
    resolved = resolve_ocpp_config(evpanda.OCPPConfig(api_key="k"))

    assert resolved.endpoint == DEFAULT_ENDPOINT
    assert resolved.api_key == "k"
    assert resolved.protocol is Protocol.OCPP
    assert resolved.max_buffer_bytes == DEFAULT_MAX_BUFFER_BYTES
    assert resolved.max_capture_bytes == DEFAULT_MAX_CAPTURE_BYTES
    assert resolved.flush_interval == DEFAULT_FLUSH_INTERVAL
    assert resolved.drain_timeout == DEFAULT_DRAIN_TIMEOUT
    assert resolved.log_mode is LogMode.ERRORS
    assert resolved.logger is not None


def test_endpoint_defaults_to_production() -> None:
    assert _resolve_endpoint(None) == DEFAULT_ENDPOINT
    assert _resolve_endpoint("  ") == DEFAULT_ENDPOINT


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://ingest.example.com", "https://ingest.example.com"),
        ("http://localhost:8080/", "http://localhost:8080"),
        ("https://example.com/base///", "https://example.com/base"),
    ],
)
def test_endpoint_is_trimmed(raw: str, expected: str) -> None:
    assert _resolve_endpoint(raw) == expected


@pytest.mark.parametrize("raw", ["not-a-url", "ftp://example.com", "://x", 42])
def test_endpoint_rejects_malformed_values(raw: object) -> None:
    with pytest.raises(evpanda.EndpointError):
        _resolve_endpoint(raw)


def test_api_key_falls_back_to_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(evpanda.API_KEY_ENV_VAR, "from-env")
    assert resolve_ocpp_config(evpanda.OCPPConfig()).api_key == "from-env"


def test_api_key_prefers_the_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(evpanda.API_KEY_ENV_VAR, "from-env")
    assert resolve_ocpp_config(evpanda.OCPPConfig(api_key="explicit")).api_key == "explicit"


def test_config_errors_are_matchable() -> None:
    """Each fault is matchable at whichever level the caller cares about."""
    with pytest.raises(evpanda.APIKeyError) as missing_key:
        resolve_ocpp_config(evpanda.OCPPConfig())
    assert isinstance(missing_key.value, evpanda.ConfigError)
    assert isinstance(missing_key.value, evpanda.EVPandaError)

    with pytest.raises(evpanda.EndpointError) as bad_endpoint:
        resolve_ocpp_config(evpanda.OCPPConfig(api_key="k", endpoint="nope"))
    assert isinstance(bad_endpoint.value, evpanda.ConfigError)


def test_out_of_range_tunables_fall_back_and_warn(logs: pytest.LogCaptureFixture) -> None:
    resolved = resolve_ocpp_config(
        evpanda.OCPPConfig(
            api_key="k",
            max_buffer_bytes=10,
            max_capture_bytes=0,
            flush_interval=-1.0,
            drain_timeout=1.0,
        )
    )

    assert resolved.max_buffer_bytes == DEFAULT_MAX_BUFFER_BYTES
    assert resolved.max_capture_bytes == DEFAULT_MAX_CAPTURE_BYTES
    assert resolved.flush_interval == DEFAULT_FLUSH_INTERVAL
    assert resolved.drain_timeout == DEFAULT_DRAIN_TIMEOUT
    warnings = [r.getMessage() for r in logs.records]
    assert sum("using default" in w for w in warnings) == 4


def test_wrong_types_fall_back(logs: pytest.LogCaptureFixture) -> None:
    resolved = resolve_ocpp_config(
        evpanda.OCPPConfig(api_key="k", max_buffer_bytes=True, flush_interval="soon")  # type: ignore[arg-type]
    )
    assert resolved.max_buffer_bytes == DEFAULT_MAX_BUFFER_BYTES
    assert resolved.flush_interval == DEFAULT_FLUSH_INTERVAL


def test_warns_when_the_buffer_is_smaller_than_a_capture(
    logs: pytest.LogCaptureFixture,
) -> None:
    resolve_ocpp_config(
        evpanda.OCPPConfig(api_key="k", max_buffer_bytes=64 << 10, max_capture_bytes=1 << 20)
    )
    assert any("can never be buffered" in r.getMessage() for r in logs.records)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, LogMode.ERRORS),
        ("silent", LogMode.SILENT),
        ("DEBUG", LogMode.DEBUG),
        (LogMode.SILENT, LogMode.SILENT),
        ("nonsense", LogMode.ERRORS),
    ],
)
def test_log_mode_from_the_config(value: object, expected: LogMode) -> None:
    mode, _ = _resolve_log_mode(value)  # type: ignore[arg-type]
    assert mode is expected


def test_log_mode_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(evpanda.LOG_MODE_ENV_VAR, "debug")
    assert _resolve_log_mode(None)[0] is LogMode.DEBUG

    monkeypatch.setenv(evpanda.LOG_MODE_ENV_VAR, "shout")
    mode, warning = _resolve_log_mode(None)
    assert mode is LogMode.ERRORS
    assert warning is not None

    # The config still wins over the environment.
    monkeypatch.setenv(evpanda.LOG_MODE_ENV_VAR, "debug")
    assert _resolve_log_mode(LogMode.SILENT)[0] is LogMode.SILENT


def test_silent_mode_has_no_logger() -> None:
    assert _effective_logger(None, LogMode.SILENT) is None
    assert _effective_logger(logging.getLogger("host"), LogMode.SILENT) is None
    assert _effective_logger(logging.getLogger("host"), LogMode.ERRORS).name == "host"  # type: ignore[union-attr]
    assert _effective_logger(None, LogMode.ERRORS).name == "evpanda"  # type: ignore[union-attr]


def test_a_silent_warn_sink_says_nothing(logs: pytest.LogCaptureFixture) -> None:
    _Warn(None)("nothing to see")
    assert logs.records == []


def test_a_broken_host_logger_cannot_fail_resolution() -> None:
    class Exploding(logging.Logger):
        def warning(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

    resolved = resolve_ocpp_config(
        evpanda.OCPPConfig(api_key="k", logger=Exploding("boom"), flush_interval=-1)
    )
    assert resolved.flush_interval == DEFAULT_FLUSH_INTERVAL


def test_allowed_headers_are_normalized(logs: pytest.LogCaptureFixture) -> None:
    warn = _Warn(logging.getLogger("evpanda"))
    assert _resolve_allowed_headers([" X-Trace ", "x-trace", "", "X-Other"], warn) == (
        "x-trace",
        "x-other",
    )
    assert _resolve_allowed_headers(None, warn) == ()
    assert _resolve_allowed_headers("x-trace", warn) == ()  # a bare string is not a list
    assert _resolve_allowed_headers(["ok", 7], warn) == ("ok",)
    assert any("must be an iterable" in r.getMessage() for r in logs.records)


def test_ocpi_config_carries_the_allowlist() -> None:
    resolved = resolve_ocpi_config(
        evpanda.OCPIConfig(api_key="k", ocpi_allowed_headers=["X-Trace"])
    )
    assert resolved.protocol is Protocol.OCPI
    assert resolved.allowed_headers == ("x-trace",)
