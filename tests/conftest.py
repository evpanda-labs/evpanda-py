"""Shared fixtures: a stub ingestion server, and clients wired to it."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
import zstandard

import evpanda


@dataclass
class Received:
    """One request the stub server accepted."""

    path: str
    headers: dict[str, str]
    body: dict[str, Any]

    @property
    def messages(self) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = self.body["messages"]
        return messages


@dataclass
class IngestServer:
    """A stub of the ingestion API, recording what the SDK sends it."""

    url: str
    received: list[Received] = field(default_factory=list)
    #: Statuses to serve before falling back to 200, one per request.
    statuses: list[int] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def next_status(self) -> int:
        with self._lock:
            return self.statuses.pop(0) if self.statuses else 200

    def record(self, item: Received) -> None:
        with self._lock:
            self.received.append(item)

    @property
    def messages(self) -> list[dict[str, Any]]:
        """Every message from every request, in order."""
        with self._lock:
            return [m for request in self.received for m in request.messages]

    def wait_for_messages(self, count: int, timeout: float = 5.0) -> list[dict[str, Any]]:
        """Block until at least ``count`` messages have arrived."""
        deadline = threading.Event()
        waited = 0.0
        while waited < timeout:
            messages = self.messages
            if len(messages) >= count:
                return messages
            deadline.wait(0.02)
            waited += 0.02
        raise AssertionError(f"expected {count} messages, saw {len(self.messages)}")


def _decompress(raw: bytes, encoding: str | None) -> bytes:
    if encoding == "zstd":
        return bytes(zstandard.decompress(raw))
    return raw


@pytest.fixture
def ingest() -> Iterator[IngestServer]:
    """A stub ingestion API on localhost, torn down after the test."""
    state: IngestServer | None = None

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:  # BaseHTTPRequestHandler's spelling
            assert state is not None
            length = int(self.headers.get("Content-Length", "0"))
            raw = _decompress(self.rfile.read(length), self.headers.get("Content-Encoding"))
            status = state.next_status()
            if status == 200:
                state.record(
                    Received(
                        path=self.path,
                        headers={k.lower(): v for k, v in self.headers.items()},
                        body=json.loads(raw),
                    )
                )
                payload = json.dumps({"captured": len(json.loads(raw)["messages"]), "failed": 0})
            else:
                payload = json.dumps({"error": "stub"})
            body = payload.encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            """Keep the test output clean."""

    class Server(ThreadingHTTPServer):
        #: Keep-alive connections must not hold the teardown open.
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    host, port = server.server_address[:2]
    state = IngestServer(url=f"http://{host}:{port}")
    thread = threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the real retry ladder out of the test suite's wall clock."""
    monkeypatch.setattr("evpanda._transport.next_delay", lambda attempt: 0.001)


@pytest.fixture
def logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Capture the SDK's own log output."""
    caplog.set_level(logging.DEBUG, logger="evpanda")
    return caplog


@pytest.fixture(autouse=True)
def _quiet_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the ambient environment out of every test's config resolution."""
    monkeypatch.delenv(evpanda.API_KEY_ENV_VAR, raising=False)
    monkeypatch.delenv(evpanda.LOG_MODE_ENV_VAR, raising=False)


def ocpi_client(ingest: IngestServer, **overrides: Any) -> evpanda.OCPIClient:
    """An OCPI client pointed at the stub server."""
    config = evpanda.OCPIConfig(endpoint=ingest.url, api_key="test-key", **overrides)
    client = evpanda.start_ocpi(config)
    assert client.error is None
    return client


def ocpp_client(ingest: IngestServer, **overrides: Any) -> evpanda.OCPPClient:
    """An OCPP client pointed at the stub server."""
    config = evpanda.OCPPConfig(endpoint=ingest.url, api_key="test-key", **overrides)
    client = evpanda.start_ocpp(config)
    assert client.error is None
    return client


@dataclass
class FakeCapturer:
    """A Capturer that records instead of buffering.

    It is what the adapter tests drive, so they exercise the adapter rather
    than the pipeline behind it.
    """

    max_capture_bytes: int | None = 65536
    inbound: list[tuple[Any, evpanda.HTTPExchange]] = field(default_factory=list)
    outbound: list[tuple[Any, evpanda.HTTPExchange]] = field(default_factory=list)

    def capture_inbound_message(self, identity: Any, data: evpanda.HTTPExchange) -> None:
        self.inbound.append((identity, data))

    def capture_outbound_message(self, identity: Any, data: evpanda.HTTPExchange) -> None:
        self.outbound.append((identity, data))

    def capturing(self) -> int | None:
        return self.max_capture_bytes


@dataclass
class PartnerServer:
    """A stand-in for a roaming partner's OCPI server."""

    url: str
    received: list[Received] = field(default_factory=list)


@pytest.fixture
def partner() -> Iterator[PartnerServer]:
    """An HTTP server that answers with JSON and records what it was sent."""
    state: PartnerServer | None = None

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self) -> None:
            assert state is not None
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            state.received.append(
                Received(
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    body={"raw": raw.decode("utf-8", "replace")},
                )
            )
            body = json.dumps({"status_code": 1000, "data": []}).encode()
            self.send_response(201 if self.command == "POST" else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = _handle
        do_POST = _handle

        def log_message(self, *args: Any) -> None:
            """Keep the test output clean."""

    class Server(ThreadingHTTPServer):
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    host, port = server.server_address[:2]
    state = PartnerServer(url=f"http://{host}:{port}")
    thread = threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


PARTNER = evpanda.Platform(id="acme", name="Acme Mobility")
CHARGER = evpanda.Charger(id="CP-001")


def exchange(**overrides: Any) -> evpanda.HTTPExchange:
    """A plausible OCPI exchange, with fields overridable per test."""
    fields: dict[str, Any] = {
        "method": "POST",
        "url": "/ocpi/2.2/cdrs",
        "status_code": 201,
        "request_headers": {"content-type": "application/json"},
        "response_headers": {"content-type": "application/json"},
        "request_body": b'{"id":"cdr-1"}',
        "response_body": b'{"status_code":1000}',
    }
    fields.update(overrides)
    return evpanda.HTTPExchange(**fields)
