# evpanda-py

[![CI](https://github.com/evpanda-labs/evpanda-py/actions/workflows/build.yml/badge.svg)](https://github.com/evpanda-labs/evpanda-py/actions/workflows/build.yml)
[![PyPI](https://img.shields.io/pypi/v/evpanda.svg)](https://pypi.org/project/evpanda/)

Passive OCPI / OCPP traffic capture for Python. Embed it in your OCPI server or
OCPP CSMS and it records protocol messages, buffers them in memory, and ships
them in batches to the EVPanda ingestion API.

- **Non-blocking.** Capture calls never wait on the network and never raise
  into your process.
- **Bounded.** Undelivered captures are capped by a byte budget you set; under
  pressure the SDK drops its own data rather than yours.
- **Safe by default.** Secrets are stripped before anything is buffered.
- **Small.** One runtime dependency, one background thread per client.

## Requirements

Python 3.12 or later.

## Installation

```sh
pip install evpanda
```

That brings `zstandard`, the codec every EVPanda SDK compresses batches
with, and nothing else. Two optional extras add the outbound adapters:

```sh
pip install 'evpanda[httpx]'     # evpanda.ocpi.httpx_transport
pip install 'evpanda[requests]'  # evpanda.ocpi.requests_adapter
```

## Quick start

Pick the client for the protocol your service speaks. `start_ocpi` and
`start_ocpp` always return a usable client — if the config is bad you get an
inert one carrying the reason on `.error`, so a typo can't stop your service
from booting.

**The only thing you must supply is an API key.** Set `EVPANDA_API_KEY` in the
environment, or pass `api_key` in the config. `endpoint` defaults to the
production ingestion API, so leave it unset unless you're pointing at another
environment.

### OCPP

`connection()` returns a session handle that mints the connection ID and
carries the charger identity, so per-frame calls need neither. It is also a
context manager, so leaving the block records the disconnect.

```python
import evpanda

# endpoint defaults to production; api_key comes from EVPANDA_API_KEY
panda = evpanda.start_ocpp()
if panda.error:
    log.warning("%s (running inert)", panda.error)

async def handle_charger(websocket):
    # However your CSMS identifies a charge point at handshake time.
    identity = resolve_charger_identity(websocket)  # -> evpanda.Charger(id="CP-001")
    if identity is None:
        await websocket.close(code=1008)
        return

    with panda.connection(identity) as session:   # records the connect…
        async for frame in websocket:             # …and the close on the way out
            session.message(frame, evpanda.OCPPDirection.FROM_CP)

            reply = handle_frame(frame)           # your CSMS logic
            await websocket.send(reply)
            session.message(reply, evpanda.OCPPDirection.TO_CP)
```

`direction` is from the charge point's perspective: `FROM_CP` for frames it
sent you, `TO_CP` for frames you send it. Use one session per socket — its
connection ID ties the connect, every frame and the disconnect into a single
session, and a reconnect gets a fresh one.

`capture_connect` / `capture_message` / `capture_disconnect` are the flat
primitives underneath, for cases a session handle doesn't fit.

### OCPI

Two methods, one per direction. The method name sets the direction — there's no
argument to get backwards.

| Method | You are the… | Typical case |
|---|---|---|
| `capture_inbound_message` | server | A partner pushes a CDR to your endpoint |
| `capture_outbound_message` | client | You pull a partner's locations |

`identity` is always the **partner on the other side** — never your own
platform.

```python
import evpanda

panda = evpanda.start_ocpi()
if panda.error:
    log.warning("%s (running inert)", panda.error)

panda.capture_inbound_message(
    identity=evpanda.Platform(id="acme", name="Acme Mobility"),
    data=evpanda.HTTPExchange(
        method="POST",
        url="/ocpi/2.2/cdrs",
        status_code=201,
        request_headers={"content-type": "application/json"},
        response_headers={"content-type": "application/json"},
        request_body=request_bytes,     # bytes or str
        response_body=response_bytes,
    ),
)
```

`status_code` and both bodies are optional; the header mappings may be left
empty. Bodies are `bytes` (a `str` is encoded as UTF-8), and since `bytes` is
immutable the SDK never has to copy what you hand it — a `bytearray` or
`memoryview` is copied, so you can reuse your own buffer the moment the call
returns.

## HTTP adapters

`evpanda.ocpi` wraps the HTTP layers your service already speaks, so you don't
have to assemble exchanges yourself. Four adapters, one per layer, each in the
module named after what it wraps:

| Adapter | Direction | For |
|---|---|---|
| `evpanda.ocpi.wsgi.WSGIMiddleware` | inbound | Flask, Django, Pyramid, any WSGI app |
| `evpanda.ocpi.asgi.ASGIMiddleware` | inbound | FastAPI, Starlette, Litestar, any ASGI app |
| `evpanda.ocpi.httpx_transport.HTTPXTransport` | outbound | `httpx` (`AsyncHTTPXTransport` for async) |
| `evpanda.ocpi.requests_adapter.RequestsAdapter` | outbound | `requests` |

```python
from evpanda.ocpi.asgi import ASGIMiddleware
from evpanda.ocpi.wsgi import WSGIMiddleware

app = ASGIMiddleware(app, panda)                     # FastAPI / Starlette
app.wsgi_app = WSGIMiddleware(app.wsgi_app, panda)   # Flask
```

```python
import httpx
from evpanda.ocpi.httpx_transport import HTTPXTransport

client = httpx.Client(transport=HTTPXTransport(panda))
```

> **If your client sets `verify`, `cert`, `limits`, `proxy` or `http2`**, pass a
> base transport carrying them. httpx applies those arguments only when it
> builds its own transport, so supplying `transport=` silently drops them —
> including the TLS ones:
>
> ```python
> base = httpx.HTTPTransport(verify=ctx, limits=httpx.Limits(max_connections=50))
> client = httpx.Client(transport=HTTPXTransport(panda, base))
> ```
>
> Client-level settings that are not transport arguments — `timeout`,
> `follow_redirects`, `headers`, `auth` — are unaffected.

A request with no resolvable identity is served exactly as it would have
been — it just isn't captured.

### Telling the adapters who the partner is

Stamp the identity wherever you already look the partner up. Inbound, that is
the request's own mapping — the WSGI environ or the ASGI scope:

```python
from evpanda.ocpi import set_identity
from starlette.middleware.base import BaseHTTPMiddleware

async def authenticate(request, call_next):
    partner = lookup_partner(request.headers.get("authorization"))
    if partner is None:
        return JSONResponse({"status_code": 2001}, status_code=401)
    set_identity(request.scope, evpanda.Platform(
        id=partner.id, name=partner.name,
    ))
    return await call_next(request)

app.add_middleware(BaseHTTPMiddleware, dispatch=authenticate)
# FastAPI keeps the decorator form: @app.middleware("http")
```

The scope is read when the response finishes, so **it does not matter where you
stamp it**. Your auth layer can sit inside or outside the capture middleware,
and a service that authenticates in the route handler itself can stamp it
there:

```python
async def cdrs(request):
    partner = authenticate(request)          # however your service does it
    set_identity(request.scope, evpanda.Platform(
        id=partner.id, name=partner.name,
    ))
    ...
```

Outbound, use the context manager — you have already looked the partner up to
get their token:

```python
from evpanda.ocpi import use_identity

with use_identity(evpanda.Platform(id=partner.id, name=partner.name)):
    response = client.post(f"{partner.url}/ocpi/2.2/sessions", json=payload,
                           headers={"Authorization": f"Token {partner.token_b}"})
```

It is a `ContextVar`, so it is task-local in async code and thread-local in
sync code. httpx also takes it per request:
`client.get(url, extensions={"evpanda.identity": identity})`.

Failing both, all four adapters read the `X-EVPanda-Platform-Id` /
`X-EVPanda-Platform-Name` headers (plus optional `-Tenant-Id` / `-Tenant-Name`).
The outbound adapters strip them before dispatch, so partners never see them.

If identity lives somewhere else entirely — a client certificate, a path prefix
— pass your own resolver:

```python
from evpanda.ocpi import RequestInfo

def by_path(info: RequestInfo) -> evpanda.Platform | None:
    if not info.url.startswith("/partners/"):
        return None                      # not captured
    name = info.url.removeprefix("/partners/").split("/")[0]
    return evpanda.Platform(id=name, name=name)

ASGIMiddleware(app, panda, resolver=by_path)
```

## Identity

Every message carries its own identity; messages the SDK can't attribute are
dropped rather than shipped as orphans.

| Protocol | Type | Required fields |
|---|---|---|
| OCPI | `Platform` | `id`, `name` |
| OCPP | `Charger` | `id` |

`tenant_id` and `tenant_name` are optional but **all-or-nothing** — set both or
neither. They keep their prefix because they describe a different subject:
which of *your* tenants an exchange belongs to, not a property of the partner
or the charger. Call `identity.valid()` to check one yourself.

## Configuration

`api_key` is the only required field; it falls back to `$EVPANDA_API_KEY`.
Everything else takes its default when left at `None`, and an out-of-range
value falls back to that default with a warning rather than failing.

A missing key and a malformed `endpoint` are the only things `start_*` reports,
and both are matchable — useful because a missing key is usually a deployment
problem while a bad endpoint is a code one:

```python
panda = evpanda.start_ocpi(config)
if isinstance(panda.error, evpanda.APIKeyError):
    raise SystemExit("EVPANDA_API_KEY is not set in this environment")
if panda.error:
    log.warning("%s (running inert)", panda.error)
```

| Field | Default | Description |
|---|---|---|
| `endpoint` | `https://ingest.evpanda.io` | Ingestion API base URL. Set only to reach another environment |
| `api_key` | `$EVPANDA_API_KEY` | Sent as `X-API-Key`. **Required** |
| `max_buffer_bytes` | `32 MiB` | Memory ceiling for undelivered captures; oldest are evicted past it |
| `max_capture_bytes` | `64 KiB` | Per body / per frame cap; an oversize body drops the whole message. Also bounds what the HTTP adapters hold per in-flight request |
| `flush_interval` | `5.0` | Maximum seconds between deliveries |
| `drain_timeout` | `10.0` | How long `close()` waits to drain (minimum `5.0`) |
| `log_mode` | `"errors"` | `"silent"`, `"errors"`, `"debug"` |
| `logger` | `logging.getLogger("evpanda")` | Where the SDK's own logs go |
| `ocpi_allowed_headers` | `()` | *(OCPI only)* Extra headers to capture, on top of the defaults |

## Memory

`max_buffer_bytes` caps everything waiting to be delivered — that is the number
to provision against, and the SDK evicts rather than exceed it.

The HTTP adapters add a second, smaller cost: while a request is in flight they
hold a copy of its bodies, bounded per request by `max_capture_bytes` and
released as soon as the exchange is captured. That cost scales with concurrency
rather than with the buffer, and with the bodies that actually pass rather than
with the cap — ordinary OCPI traffic barely registers. The WSGI adapter is the
one that pays it even for a handler that never reads the body, since it takes
the body up front; a server taking large CDR pushes at high concurrency should
count on roughly one extra copy of each in-flight body. Calling
`capture_inbound_message` yourself instead of using an adapter avoids it
entirely, since you already hold the bytes.

It is deliberately not bounded in aggregate. Doing that would mean making a
request wait on a capture budget, and capture never blocks the host.

## Logging

The SDK reports problems to your logger by default, at a bounded rate: at most
one summary line per minute, and nothing at all while it's healthy.

```
WARNING evpanda: captures dropped window=60s captured=12 dropped_invalid=148302 buffered=0 buffer_bytes=0
```

Set `log_mode` to change that, or `EVPANDA_LOG=silent|errors|debug` to change it
without touching code:

| Mode | Output |
|---|---|
| `"silent"` | Nothing. Counters still work. |
| `"errors"` | Default. Config problems at startup, plus the per-minute summary. |
| `"debug"` | Adds per-batch delivery failures and a summary on close. |

## Is it working?

`stats()` is a snapshot of the client's delivery counters, always available and
safe on an inert or closed client. Each counter maps to one root cause:

```python
stats = panda.stats()
# Stats(captured=40120, dropped_invalid=0, dropped_oversize=0, dropped_evicted=9402,
#       dropped_undeliverable=0, dropped_fault=0, buffered_messages=2, buffer_bytes=528)
```

| Counter | What a high value means |
|---|---|
| `captured` is 0 | The capture path is not wired in |
| `dropped_invalid` | Identity resolution is failing |
| `dropped_oversize` | Bodies exceed `max_capture_bytes` |
| `dropped_evicted` | Upstream can't keep up, or the buffer is undersized |
| `dropped_undeliverable` | Network, API key, or ingestion fault |
| `dropped_fault` | A bug in the SDK — please report it |

It is a pull-based snapshot, so it feeds Prometheus, OpenTelemetry or a log
line without the SDK depending on any of them.

## Shutdown

```python
server.shutdown()              # stop accepting first…
if not panda.close():          # …then drain what was captured
    log.warning("evpanda: shut down with messages still buffered")
```

`close(timeout=None)` drains within `drain_timeout` (or the seconds you pass)
and returns whether it managed to. It is idempotent, never raises, and captures
after it are safe no-ops. Clients are context managers too, so
`with evpanda.start_ocpi() as panda:` closes on the way out.

A client the host never closes is drained at interpreter exit, and a client
inherited across `fork()` — gunicorn, uWSGI and other preforking servers —
restarts its delivery thread in the child.

`flush()` forces an immediate delivery and waits for it. It blocks for as long
as the transport's retries take, so use it at shutdown or while debugging — not
on a request path.

## Documentation

- [Architecture and design notes](https://claude.ai/code/artifact/d6759cc2-ff5f-4279-ad63-c2738222f8f8)
  — how it works, and why. The source lives at [`docs/design.html`](docs/design.html)
- [evpanda-go](https://github.com/evpanda-labs/evpanda-go) — the reference
  implementation this SDK tracks
