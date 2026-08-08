# evpanda

[![CI](https://github.com/evpanda-labs/evpanda-py/actions/workflows/ci.yml/badge.svg)](https://github.com/evpanda-labs/evpanda-py/actions/workflows/ci.yml)

Passive OCPI / OCPP traffic capture for Python — a functional port of the
[evpanda Node SDK](https://github.com/evpanda-labs/evpanda-node). Embed it in
your OCPI server or OCPP CSMS; it records protocol messages, buffers them
in-process, and ships them in batches to the EVPanda ingestion API.

> **It never gets in your way.** The SDK will not block your request path,
> raise into your handlers, crash your process, or grow memory unbounded.
> If it's under stress or the network is down it drops data — it never
> degrades your application.

- **Zero required dependencies** — HTTP, gzip, JSON, threading and logging
  are all stdlib. [`zstandard`](https://pypi.org/project/zstandard/) is an
  optional extra; without it the transport falls back to gzip.
- **Python ≥ 3.12.**

> **Status.** The OCPI and OCPP clients, redaction, buffering and transport
> are complete and tested. The only unported piece is the OCPI framework
> adapters (Flask/FastAPI/requests-style wrappers) — until they land,
> capture through the client methods below.

## Install

```sh
pip install evpanda          # gzip
pip install "evpanda[zstd]"  # + zstd (recommended)
```

## Quick start

**The protocol is the class.** There is no `network_type` switch:
`OCPIClient` takes an `OCPIConfig`, `OCPPClient` an `OCPPConfig`. Common
fields live on `BaseConfig`; per-protocol extensions add only what that
protocol's client cares about.

### OCPI

`OCPIClient.start()` never raises — a bad config yields an inert no-op
client, so it can't crash your boot.

```python
from evpanda import HttpExchange, OCPIClient, OCPIConfig, OCPIMessageInput
from evpanda import RoamingIdentity

panda = OCPIClient.start(
    OCPIConfig(
        endpoint="https://ingest.evpanda.io",
        # api_key omitted ⇒ read from EVPANDA_API_KEY
        ocpi_allowed_headers=["x-correlation-id"],  # extends the allowlist
    )
)
try:
    panda.capture_inbound_message(
        OCPIMessageInput(  # partner → host
            identity=RoamingIdentity(
                platform_id="acme",
                platform_name="Acme Mobility",
                tenant_id="cpo-42",  # tenant is all-or-nothing
                tenant_name="CPO 42",
            ),
            data=HttpExchange(
                method="POST",
                url="/ocpi/2.2/cdrs",
                status_code=200,
                request_headers={"content-type": "application/json"},
                request_body=b'{"id":"..."}',
            ),
        )
    )
finally:
    panda.close()  # flushes what's buffered, within drain_timeout
```

`capture_outbound_message` (host → partner) is the same call with the other
direction; the method stamps `IN` / `OUT` for you.

### OCPP

The recommended path is a **session handle** — it mints the `connection_id`
and carries the identity, so per-frame calls carry neither:

```python
from evpanda import ChargerIdentity, OCPPClient, OCPPConfig, OCPPDirection

panda = OCPPClient.start(OCPPConfig(endpoint="https://ingest.evpanda.io"))

# On WebSocket connect — records the connect and returns the handle.
session = panda.connection(ChargerIdentity(charger_id="CP-001"))
session.message('[2,"id","BootNotification",{}]', OCPPDirection.FROM_CP)
session.disconnect()  # on socket close

# Or scope it to a block — leaving the block records the disconnect:
with panda.connection(ChargerIdentity(charger_id="CP-002")) as session:
    session.message(frame, OCPPDirection.TO_CP)
```

`capture_connect` / `capture_message` / `capture_disconnect` are the flat
primitives underneath, taking an `OCPPMessageInput`, for one-off capture.
Frames may be `bytes` or `str` (encoded UTF-8).

Capture is **non-blocking and never raises** — messages are buffered and
delivered on a background daemon thread. One client serves one protocol.

## Identity

Every message carries its own identity; the SDK validates it and silently
drops what it can't attribute (it never raises back at you).

- **OCPI →** `RoamingIdentity`: `platform_id` + `platform_name` required.
- **OCPP →** `ChargerIdentity`: `charger_id` required.
- `tenant_id` + `tenant_name` are optional but **all-or-nothing** — supply
  both or neither.

Identity is per message, not global config — one OCPI process can serve
many platforms and tenants. OCPI identity lives in the partner's request
headers, so you supply it per capture call; OCPP identity is known at
connect time, so the session handle carries it for you.

(The adapters' `OCPIResolver` contract — request context → identity — lands
with the adapters themselves.)

## Configuration

`endpoint` and `api_key` are **hard-required**: a bad value means the client
runs inert rather than crashing your boot. Every other field is a tunable —
an invalid value warns (only when `debug=True`) and falls back to its
default, never raises.

| Field                  | Default            | Description                                                                            |
|------------------------|--------------------|----------------------------------------------------------------------------------------|
| `endpoint`             | —                  | Ingestion API base URL (`http(s)://…`).                                                |
| `api_key`              | `$EVPANDA_API_KEY` | Sent as `X-API-Key`. Falls back to the env var when empty; one of the two must be set. |
| `buffer_capacity`      | `10000`            | Ring buffer slots. Worst-case mem = `buffer_capacity × max_capture_bytes`.             |
| `max_capture_bytes`    | `65536`            | Per-body / per-frame cap. An oversize body drops the whole message.                    |
| `flush_interval`       | `5.0`              | Worker flush cadence, in **seconds**.                                                  |
| `drain_timeout`        | `10.0`             | `close()` drain deadline, in seconds. An explicit value must be ≥ `5.0`.               |
| `compression`          | `"zstd"`           | `"zstd"` (needs the optional extra, else gzip) or `"gzip"`.                            |
| `debug`                | `False`            | Master log switch; default totally silent.                                             |
| `logger`               | `None`             | `logging.Logger` used when `debug=True`; if `None`, `logging.getLogger("evpanda")`.    |
| `ocpi_allowed_headers` | `[]`               | *(OCPIConfig only)* Extra headers to capture on top of the default allowlist.          |

Intervals are float **seconds** — Python's unit for `time.sleep`,
`Event.wait` and socket timeouts — where the Node SDK uses milliseconds.

## Behavior

- **Batched delivery.** Messages flush when the buffer reaches 1000 or on
  `flush_interval`, whichever comes first; each POST carries at most 1000
  records.
- **Backpressure = drop-oldest.** If the upstream is slow or down, the ring
  caps at `buffer_capacity` and discards the oldest. Your app never blocks.
- **Redaction at the chokepoint.** Every capture path — adapter or
  primitive — goes through one validate → cap → redact step before the
  queue. OCPI keeps a **header allowlist** (`ocpi_allowed_headers` extends
  it, never shrinks it — `Authorization`, `Cookie`, `X-API-Key` and anything
  else unlisted fall off) and masks the `token` field on `/credentials`
  bodies; OCPP frames are captured verbatim today, behind a seam ready for
  masking.
- **Resilient transport.** Bounded retry (max 5 attempts) with capped
  exponential backoff + full jitter on other status / network errors;
  permanent rejections (400/401/413) are dropped without retry storms.
  Payloads under 1 KiB are sent uncompressed.
- **Graceful shutdown.** `close()` flushes what's buffered within
  `drain_timeout`, then stops. Idempotent, and it swaps in an inert engine
  first so post-close calls are safe no-ops. If the deadline elapses with
  messages still buffered it logs a warning (when `debug=True`).

## Development

```sh
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"

.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy src
.venv/bin/pytest -q
```

Porting decisions and the wire-contract parity notes against the Node SDK
live in [PORTING_NOTES.md](PORTING_NOTES.md).
