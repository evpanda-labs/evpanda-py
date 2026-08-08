# Porting notes: evpanda-node `src/` → evpanda (Python)

This is a functional port of the Node reference SDK's shared engine.
Behavior is identical where it matters; the divergences below are
deliberate idiomatic-Python choices that do not change observable
semantics. **The wire contract is byte-parity with Node and Go** (see the
dedicated section at the end).

## Module mapping

| Node file        | Python module          |
|------------------|------------------------|
| `types.ts`       | `evpanda/types.py`     |
| `identity.ts`    | `evpanda/identity.py`  |
| `config.ts`      | `evpanda/config.py`    |
| `buffer.ts`      | `evpanda/buffer.py`    |
| `transport.ts`   | `evpanda/transport.py` |
| `worker.ts`      | `evpanda/worker.py`    |
| `client.ts`      | `evpanda/client.py`    |
| `index.ts`       | `evpanda/__init__.py`  |
| `ocpi/client.ts` + `ocpi/redact.ts` | `evpanda/ocpi.py` |
| `ocpp/client.ts` + `ocpp/redact.ts` | `evpanda/ocpp.py` |

Each protocol is one flat module rather than a package: the client and its
redactor are ~200 lines together and always change in lock-step.

`src/ocpi/adapters/` (express / fetch / axios) is **not ported**. Its seams
are in place: `OCPIClient._internal` carries the `SdkInternal` bridge
(`max_capture_bytes` + logger), cleared on `close` so adapters fall back to
a pass-through. The `OCPIResolver` / `OCPIResolverCtx` contract is *not*
ported either — the adapters are its only consumer, so it lands with them
rather than sitting as unused public surface.

## Idiomatic-Python divergences (intentional)

1. **`threading` instead of the event loop.** Node's SDK is async
   throughout (`Promise`, a self-rescheduling `setTimeout`, `unref`).
   Python's equivalent for an SDK that must work inside sync hosts
   (Flask/Django/plain WSGI) is a daemon `threading.Thread`. The poll timer
   becomes `Event.wait(0.2)` — the same 200 ms poll — and every public
   method is synchronous: `flush()`, `close()` and the `capture_*` calls
   return `None`, not an awaitable. `close()` bounds the drain with
   `Thread.join(timeout=...)` plus a deadline loop instead of
   `Promise.race`.

2. **Single-flight flush is a `Lock`, not a shared promise.** Node returns
   the in-flight promise to concurrent callers, so a second flush never
   runs. Python serializes on a `threading.Lock`: a concurrent caller
   blocks and then drains whatever accumulated. Same guarantee that
   matters — flushes never overlap.

3. **The ring buffer takes a lock.** Node relies on the event loop to
   serialize `enqueue`/`flush`. Python producers can be on any thread, so
   `RingBuffer` guards its state with a `threading.Lock`. `count` is a
   property rather than a getter method.

4. **Float seconds instead of milliseconds.** `flush_interval`,
   `drain_timeout` and every backoff/timeout constant are float seconds —
   Python's unit for `time.sleep`, `Event.wait` and socket timeouts.
   Defaults are unchanged in real terms: `flush_interval=5.0`,
   `drain_timeout=10.0` (explicit minimum `5.0`), `flush_interval` minimum
   `0.001` (Node's 1 ms), backoff base `0.5`, cap `30.0`, request timeout
   `30.0`.

5. **`dataclass` value objects instead of TS interfaces.** `BaseConfig`,
   `OCPIConfig`, `OCPPConfig`, the resolved configs, `HttpExchange`,
   `OCPIMessage`, `OCPIMessageInput`, `OCPPMessage`, `RoamingIdentity`,
   `ChargerIdentity`, `OCPIResolverCtx` and `BufferedMessage` are all
   dataclasses (the resolved ones frozen). Absent optional values are
   `None`, matching Node's `undefined`; the resolvers still accept and
   reject garbage at runtime exactly as Node's do, since a type annotation
   enforces nothing.
   `OCPPConfig` is a real (empty) subclass of `BaseConfig` rather than
   Node's `type OCPPConfig = BaseConfig`, because Python users need
   something constructible.

6. **`OCPPConfig`/`OCPIConfig` are constructed, not object literals.**
   Consequently `endpoint` is a required positional/keyword field and the
   "a config object is required" guard is an `isinstance(config,
   BaseConfig)` check.

7. **`logging` instead of the `Logger` interface.** Node accepts any
   `{debug,info,warn,error}` object and defaults to `console` when
   `debug: true`. Python takes a `logging.Logger` and defaults to
   `logging.getLogger("evpanda")`. Node's `(msg, meta)` pairs are folded
   into one `%`-formatted message — e.g. `logger.warning("evpanda: dropped
   batch (delivery failed): protocol=%s messages=%d reason=%s", ...)`. The
   "logger only when `debug`" rule and the try/except around customer
   logger calls are preserved.

8. **`ConfigError` instead of bare `Error`.** `resolve_ocpi_config` /
   `resolve_ocpp_config` raise `ConfigError` for the two hard-required
   fields; tunables warn and fall back, never raise — same split as Node.

9. **`zstandard` is an optional extra, not an optional peer dep.**
   `pip install evpanda[zstd]`. It is imported lazily behind
   `functools.cache` (Node's `undefined | null | fn` module-level cache)
   and its absence falls back to gzip. The SDK has no required runtime
   dependency.

10. **PEP 695 generics and type aliases.** `BaseClient[E: ClientEngine]`
    with a `typing.Protocol` bound replaces `BaseClient<E extends
    ClientEngine>`; `type X = ...` replaces TS type aliases. `BaseClient`
    is a plain class (documented as internal) rather than `abstract`, since
    it declares no abstract members. Node's `#engine` private field becomes
    a name-mangled `__engine` guarded by a lock, exposed to subclasses via
    the `_engine` property.

11. **src layout + hatchling.** `src/evpanda/` package, `pyproject.toml`
    with the hatchling build backend, `ruff` + `mypy --strict` + `pytest`.

12. **`OCPPSession` is a class and a context manager.** Node returns an
    object literal with `connectionId` / `message` / `disconnect`. The
    Python handle is a small class with the same three members, plus
    `__enter__`/`__exit__` so `with client.connection(identity) as session:`
    records the disconnect on the way out — the natural Python idiom for a
    connection-scoped handle. Nothing else changed: `connection()` still
    mints a `uuid4` and records the connect eagerly.

13. **The redactor type aliases live in `worker.py`, not the protocol
    modules.** Node's `worker.ts` imports `OCPIRedactor` / `OCPPRedactor`
    from `ocpi/redact.ts` / `ocpp/redact.ts`; that is a *type-only* import,
    erased at compile time. In Python it would be a real import cycle
    (`ocpi.py` imports `Worker`), so the two aliases sit next to their
    consumer in `worker.py` and `ocpi.py` / `ocpp.py` import them from
    there.

14. **`_encode_frame` rejects non-bytes-like input instead of coercing.**
    Node's `Buffer.from(x, "utf8")` throws on a number; Python's
    `bytes(12345)` would silently mint a 12 KB zero-filled frame, so the
    encoder raises `TypeError` for anything that isn't `str` /
    `bytes` / `bytearray` / `memoryview`. `capture_message` catches it,
    logs the fault (when `debug=True`) and drops the message — the same
    observable behavior as Node.

15. **Capture methods take `msg`, not `input`.** `input` is a Python
    builtin; the parameter is renamed rather than shadowing it.

16. **Identity is per message only.** The earlier Go-derived Python port
    propagated identity through `contextvars`; the Node SDK made identity
    explicit on every message with an `OCPIResolver` for the adapters, so
    the `ContextVar` surface (`set_roaming_identity`,
    `roaming_identity_from_context`, the context managers and the
    `charger_*` trio) is **gone**. Same for `evpanda.start()` /
    `evpanda.Client` / `Config(network_type=...)` — the protocol is the
    class now.

## Behavioral parity (verified, not divergent)

- Drop-oldest ring: same head/count modular arithmetic as `buffer.ts`; same
  FIFO drain order; live slots skipped rather than raising.
- Capture chokepoint: validate identity → enforce `max_capture_bytes` on
  each body/frame (oversize drops the **whole** message) → redact →
  enqueue. Invalid identity is silently dropped, never raised.
- OCPI redaction: default header allowlist (OCPI routing, content
  negotiation, tracing, pagination) matched case-insensitively, extended —
  never shrunk — by `ocpi_allowed_headers`; `token` masked with
  `[redacted]` at the body root (request) or under `data` (response
  envelope) on a `/credentials`, `/credentials/` or `/credentials?…` URL
  only, and the original bytes returned on any miss (non-credentials URL,
  non-JSON, non-object, no token, re-encode error). OCPP redaction is the
  identity transform.
- Client lifecycle: `start()` never raises — any fault yields an inert
  client; `close()` swaps the inert engine in first (and `OCPIClient.close`
  drops `_internal` before that), then drains; both are idempotent.
- OCPP: `connection()` mints a fresh id per call and records `CONNECT`
  eagerly; `capture_message` drops the message when `data` or `direction`
  is missing, when the identity is invalid, or when the frame exceeds
  `max_capture_bytes`; the inert engine reports an infinite cap so the
  oversize check never short-circuits there.
- Validation: non-empty after `strip()`; tenant id/name all-or-nothing.
- Config: `endpoint` trimmed, `http`/`https` only, trailing slashes
  stripped; `api_key` trimmed with `EVPANDA_API_KEY` fallback;
  `ocpi_allowed_headers` trimmed, lowercased, deduplicated (insertion
  order) and frozen, with non-string entries skipped.
- Retry: base `0.5s`, max `30s`, max `5` attempts, capped exponential with
  full jitter. `200` ⇒ done (no log); `400/401/413` ⇒ permanent drop
  (log + done); any other status / network error / timeout ⇒ retry;
  exhaustion ⇒ log. Per-attempt 30 s timeout.
- Flush triggers: buffer count ≥ 1000 **or** elapsed-since-last-flush ≥
  `flush_interval`; 200 ms poll; batch chunked at 1000 per POST; one
  protocol per client.
- Compression: `<1024` bytes ⇒ identity; zstd requested but absent ⇒ gzip;
  any failure ⇒ identity.
- Timestamp: UTC RFC3339 with millisecond precision and a literal `Z`,
  e.g. `2026-05-18T12:34:56.789Z` — the same shape as
  `new Date().toISOString()`.

## Wire-contract byte-parity (explicit confirmation)

The serialized request is identical to `transport.ts`'s output:

- **Envelope:** top-level `{"messages": [ <record>, ... ]}`.
- **Flat snake_case records:** no nested `data`/`identity` objects; OCPI
  fields are `captured_at, platform_id, platform_name, tenant_id,
  tenant_name, direction, http_method, url, response_status_code,
  request_headers, request_body, response_headers, response_body`; OCPP
  fields are `charger_id, connection_id, tenant_id, tenant_name,
  captured_at, event_type, direction, raw_frame`.
- **Explicit nulls:** every optional field is *always present* with JSON
  `null` when absent — never omitted, never a zero value.
  `tenant_id`/`tenant_name` are `null` when empty, `response_status_code`
  is `null` when `0`/absent, `request_headers`/`response_headers` are
  `null` when empty, `request_body`/`response_body`/`raw_frame` are `null`
  when empty, OCPP `direction` is `null` when absent.
- **Direction values:** OCPI `"IN"`/`"OUT"`, OCPP `"TO_CP"`/`"FROM_CP"` —
  the `StrEnum` values serialize as those exact strings.
- **base64 bodies:** standard base64 (`base64.standard_b64encode`).
- **No `protocol` / `truncated` keys** anywhere.
- **OCPP `event_type` is an int that is never null** — `0` (`DISCONNECT`)
  serializes as `0`. Asserted in
  `tests/test_e2e.py::test_ocpp_event_type_never_null`.

JSON is emitted with compact separators. The test suite asserts the
envelope, flat shape, redaction at the chokepoint, base64 round-trip,
explicit nulls, chunking, drop-oldest, the compression paths and OCPP
`event_type` never null.
