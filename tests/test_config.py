"""Config resolution: endpoint/api_key are hard-required (raise ⇒ the client
runs inert); every tunable field warns and falls back to its default.
"""

from __future__ import annotations

import logging

import pytest

from evpanda.config import (
    ConfigError,
    OCPIConfig,
    OCPPConfig,
    resolve_ocpi_config,
    resolve_ocpp_config,
)
from evpanda.types import Protocol


def test_defaults_and_protocol_come_from_the_resolver() -> None:
    ocpi = resolve_ocpi_config(OCPIConfig(endpoint="https://ingest.example.com/", api_key="k"))
    assert ocpi.protocol is Protocol.OCPI
    assert ocpi.endpoint == "https://ingest.example.com"  # trailing slash stripped
    assert ocpi.buffer_capacity == 10_000
    assert ocpi.max_capture_bytes == 64 * 1024
    assert ocpi.flush_interval == 5.0
    assert ocpi.drain_timeout == 10.0
    assert ocpi.compression == "zstd"
    assert ocpi.debug is False
    assert ocpi.logger is None
    assert ocpi.ocpi_allowed_headers == ()

    ocpp = resolve_ocpp_config(OCPPConfig(endpoint="http://localhost:8080", api_key="k"))
    assert ocpp.protocol is Protocol.OCPP


@pytest.mark.parametrize(
    "endpoint",
    ["", "   ", "not-a-url", "ftp://ingest.example.com", "/relative/path"],
)
def test_bad_endpoint_raises(endpoint: str) -> None:
    with pytest.raises(ConfigError):
        resolve_ocpi_config(OCPIConfig(endpoint=endpoint, api_key="k"))


def test_api_key_falls_back_to_the_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EVPANDA_API_KEY", raising=False)
    with pytest.raises(ConfigError):
        resolve_ocpi_config(OCPIConfig(endpoint="https://x.example.com"))

    monkeypatch.setenv("EVPANDA_API_KEY", "  env-key  ")
    assert resolve_ocpi_config(OCPIConfig(endpoint="https://x.example.com")).api_key == "env-key"


def test_tunables_warn_and_default(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="evpanda")
    resolved = resolve_ocpi_config(
        OCPIConfig(
            endpoint="https://x.example.com",
            api_key="k",
            buffer_capacity=0,  # below the minimum
            max_capture_bytes=-1,  # below the minimum
            flush_interval=0.0,  # below the minimum
            drain_timeout=2.0,  # below the 5s minimum
            compression="brotli",  # type: ignore[arg-type]
            debug=True,
        )
    )
    assert resolved.buffer_capacity == 10_000
    assert resolved.max_capture_bytes == 64 * 1024
    assert resolved.flush_interval == 5.0
    assert resolved.drain_timeout == 10.0
    assert resolved.compression == "zstd"

    warned = "\n".join(r.getMessage() for r in caplog.records)
    for field_name in (
        "buffer_capacity",
        "max_capture_bytes",
        "flush_interval",
        "drain_timeout",
        "compression",
    ):
        assert field_name in warned


def test_tunables_are_silent_without_debug(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="evpanda")
    resolved = resolve_ocpi_config(
        OCPIConfig(endpoint="https://x.example.com", api_key="k", drain_timeout=1.0)
    )
    assert resolved.drain_timeout == 10.0
    assert resolved.logger is None
    assert caplog.records == []


def test_explicit_minimums_are_accepted() -> None:
    resolved = resolve_ocpi_config(
        OCPIConfig(
            endpoint="https://x.example.com",
            api_key="k",
            buffer_capacity=1,
            max_capture_bytes=1,
            flush_interval=0.001,
            drain_timeout=5.0,
        )
    )
    assert (resolved.buffer_capacity, resolved.max_capture_bytes) == (1, 1)
    assert (resolved.flush_interval, resolved.drain_timeout) == (0.001, 5.0)


def test_debug_uses_the_package_logger_or_the_injected_one() -> None:
    default = resolve_ocpi_config(
        OCPIConfig(endpoint="https://x.example.com", api_key="k", debug=True)
    )
    assert default.logger is logging.getLogger("evpanda")

    mine = logging.getLogger("host.app")
    injected = resolve_ocpi_config(
        OCPIConfig(endpoint="https://x.example.com", api_key="k", debug=True, logger=mine)
    )
    assert injected.logger is mine

    # A logger without debug stays silent.
    off = resolve_ocpi_config(
        OCPIConfig(endpoint="https://x.example.com", api_key="k", logger=mine)
    )
    assert off.logger is None


def test_ocpi_allowed_headers_are_normalized() -> None:
    resolved = resolve_ocpi_config(
        OCPIConfig(
            endpoint="https://x.example.com",
            api_key="k",
            ocpi_allowed_headers=["  X-Trace ", "x-trace", "X-Request-Id", "   "],
        )
    )
    assert resolved.ocpi_allowed_headers == ("x-trace", "x-request-id")


def test_ocpi_allowed_headers_reject_garbage(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="evpanda")
    # A bare string is not a list of headers.
    assert (
        resolve_ocpi_config(
            OCPIConfig(
                endpoint="https://x.example.com",
                api_key="k",
                ocpi_allowed_headers="x-trace",
                debug=True,
            )
        ).ocpi_allowed_headers
        == ()
    )
    # Non-string entries are skipped; the good ones still apply.
    assert resolve_ocpi_config(
        OCPIConfig(
            endpoint="https://x.example.com",
            api_key="k",
            ocpi_allowed_headers=["X-Trace", 42, None],  # type: ignore[list-item]
            debug=True,
        )
    ).ocpi_allowed_headers == ("x-trace",)
    assert any("ocpi_allowed_headers" in r.getMessage() for r in caplog.records)


def test_a_malformed_logger_cannot_break_resolution() -> None:
    class Exploding(logging.Logger):
        def warning(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

    resolved = resolve_ocpi_config(
        OCPIConfig(
            endpoint="https://x.example.com",
            api_key="k",
            drain_timeout=1.0,  # triggers a warn
            debug=True,
            logger=Exploding("exploding"),
        )
    )
    assert resolved.drain_timeout == 10.0
