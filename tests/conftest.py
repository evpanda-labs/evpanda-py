"""Shared fixtures: a mock ingestion upstream plus helpers that assemble the
core the way the protocol clients will (resolve → buffer → transport →
worker).
"""

from __future__ import annotations

import gzip
import json
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from evpanda.buffer import RingBuffer
from evpanda.config import OCPIConfig, OCPPConfig, resolve_ocpi_config, resolve_ocpp_config
from evpanda.transport import Transport
from evpanda.types import HttpExchange, OCPIDirection, OCPIMessage, RoamingIdentity
from evpanda.worker import Worker


class _CIHeaders:
    """Case-insensitive header view (HTTP headers are case-insensitive)."""

    def __init__(self, headers: dict[str, str]) -> None:
        self._h = {k.lower(): v for k, v in headers.items()}

    def get(self, key: str) -> str | None:
        return self._h.get(key.lower())


class Received:
    def __init__(self, path: str, headers: dict[str, str], records: list[dict[str, Any]]):
        self.path = path
        self.headers = _CIHeaders(headers)
        self.records = records


class MockUpstream:
    """Stands in for the ingestion API; decodes the body and keeps every POST."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.received: list[Received] = []
        self.status = 200
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # silence
                pass

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                enc = (self.headers.get("content-encoding") or "").lower()
                if enc == "gzip":
                    raw = gzip.decompress(raw)
                elif enc == "zstd":
                    import zstandard

                    raw = zstandard.ZstdDecompressor().decompress(raw)
                try:
                    records = json.loads(raw).get("messages") or []
                except Exception:
                    records = []
                with mock._lock:
                    mock.received.append(Received(self.path, dict(self.headers), records))
                    status = mock.status
                self.send_response(status)
                self.end_headers()
                self.wfile.write(b'{"captured":0,"failed":0}')

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        assert self._server is not None
        port = self._server.server_address[1]
        return f"http://127.0.0.1:{port}"

    def set_status(self, s: int) -> None:
        with self._lock:
            self.status = s

    def posts(self, path: str) -> list[Received]:
        with self._lock:
            return [r for r in self.received if r.path == path]

    def records(self, path: str) -> list[dict[str, Any]]:
        return [rec for r in self.posts(path) for rec in r.records]

    def ocpi_posts(self) -> list[Received]:
        return self.posts("/v1/ocpi")

    def ocpi_records(self) -> list[dict[str, Any]]:
        return self.records("/v1/ocpi")

    def close(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()


@pytest.fixture
def mock() -> Iterator[MockUpstream]:
    m = MockUpstream()
    m.start()
    yield m
    m.close()


@pytest.fixture
def workers() -> Iterator[Callable[..., Worker]]:
    """Build armed workers and close them all at the end of the test."""
    built: list[Worker] = []

    def build(config: OCPIConfig | OCPPConfig) -> Worker:
        resolved = (
            resolve_ocpi_config(config)
            if isinstance(config, OCPIConfig)
            else resolve_ocpp_config(config)
        )
        worker = Worker(RingBuffer(resolved.buffer_capacity), Transport(resolved), resolved)
        worker.start()
        built.append(worker)
        return worker

    yield build
    for w in built:
        w.close()


def wait_for(predicate: Callable[[], bool], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("wait_for: timed out")
        time.sleep(0.02)


#: Stand-in for `evpanda.ocpi.redact`: drops the Authorization header so the
#: tests can prove the chokepoint applies the redactor it is handed.
def strip_authorization(msg: OCPIMessage) -> OCPIMessage:
    data = msg.data
    return OCPIMessage(
        direction=msg.direction,
        identity=msg.identity,
        data=HttpExchange(
            method=data.method,
            url=data.url,
            status_code=data.status_code,
            request_headers={
                k: v for k, v in data.request_headers.items() if k.lower() != "authorization"
            },
            response_headers=dict(data.response_headers),
            request_body=data.request_body,
            response_body=data.response_body,
        ),
    )


def identity_redactor(msg: Any) -> Any:
    """Pass-through redactor, matching today's OCPP one."""
    return msg


def make_ocpi(i: int) -> OCPIMessage:
    return OCPIMessage(
        direction=OCPIDirection.IN,
        identity=RoamingIdentity(
            platform_id="acme",
            platform_name="Acme Mobility",
            tenant_id="t1",
            tenant_name="Tenant One",
        ),
        data=HttpExchange(
            method="POST",
            url=f"/ocpi/2.2/cdrs/{i}",
            status_code=200,
            request_headers={"Authorization": "Bearer SECRET", "X-Trace": str(i)},
            response_headers={"content-type": "application/json"},
            request_body=f"body-{i}".encode(),
        ),
    )
