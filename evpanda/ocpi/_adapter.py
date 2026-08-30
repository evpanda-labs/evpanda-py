"""The pieces every OCPI adapter shares: the client seam, identity
resolution, bounded body accumulation, and the fault guard.

Adapters live apart from the core so the two can move independently:
adapters accumulate framework surface over time, the capture pipeline does
not.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .._types import HTTPExchange, Platform

# ── The client seam ──────────────────────────────────────────────────────


@runtime_checkable
class Capturer(Protocol):
    """What the adapters need from a live :class:`evpanda.OCPIClient`.

    Taking the protocol rather than the concrete class keeps the seam
    explicit and lets a test drive an adapter without a running pipeline.
    """

    def capture_inbound_message(self, identity: Platform, data: HTTPExchange) -> None: ...

    def capture_outbound_message(self, identity: Platform, data: HTTPExchange) -> None: ...

    def capturing(self) -> int | None: ...


def capturing(client: Capturer | None) -> int | None:
    """Ask the client what it can capture, tolerating one that misbehaves.

    A None client or a third-party implementation with a fault of its own
    both mean "not capturing", which leaves the adapter a pass-through
    rather than a broken host.
    """
    if client is None:
        return None
    try:
        return client.capturing()
    except Exception:  # noqa: BLE001 - a broken Capturer is not the host's problem
        return None


def guard(fn: Callable[[], None]) -> None:
    """Run ``fn``, swallowing anything it raises.

    A fault while assembling a capture can never reach the host. The
    capture calls themselves are already guarded inside the SDK — that is
    where a fault gets counted — so this covers only the adapter's own
    bookkeeping.
    """
    with contextlib.suppress(Exception):  # see above
        fn()


# ── Identity ─────────────────────────────────────────────────────────────
#
# Three carriers, tried in order, because Python's HTTP layers hand you
# three different places to put a value:
#
#   1. the per-request mapping — a WSGI environ, an ASGI scope, an httpx
#      request's `extensions`. Both server mappings are mutable and shared
#      with everything inside them, so an auth layer *inside* this
#      middleware can still stamp the identity and be seen: unlike the Go
#      SDK's context, mount order does not matter here.
#   2. a ContextVar, for code that has no request mapping to hand — an
#      outgoing call through `requests`, or a framework that hides the
#      scope from you. It is async-safe and task-local.
#   3. the X-EVPanda-* headers, which the outgoing adapters strip before
#      dispatch so a partner never receives them.

#: The key both server adapters read on the request mapping.
IDENTITY_KEY = "evpanda.identity"

#: Request headers the shipped resolver reads as a last fallback. Matching
#: is case-insensitive; the adapters lowercase what they collect. The wire
#: names are unchanged by the Platform rename — they carry Platform.id,
#: Platform.name, Platform.tenant_id and Platform.tenant_name.
HEADER_PLATFORM_ID = "x-evpanda-platform-id"
HEADER_PLATFORM_NAME = "x-evpanda-platform-name"
HEADER_TENANT_ID = "x-evpanda-tenant-id"
HEADER_TENANT_NAME = "x-evpanda-tenant-name"

#: The same set as a tuple. The outgoing adapters strip these before
#: dispatch — tenant ID and name in particular describe your own tenant,
#: not the partner's.
IDENTITY_HEADERS = (
    HEADER_PLATFORM_ID,
    HEADER_PLATFORM_NAME,
    HEADER_TENANT_ID,
    HEADER_TENANT_NAME,
)

_identity_var: ContextVar[Platform | None] = ContextVar("evpanda_ocpi_identity", default=None)


def set_identity(carrier: MutableMapping[str, Any], identity: Platform) -> None:
    """Stamp the partner's identity on a WSGI environ or an ASGI scope.

    Do it wherever you already look the partner up::

        def authenticate(app):
            def middleware(environ, start_response):
                partner = lookup(environ.get("HTTP_AUTHORIZATION"))
                if partner:
                    ocpi.set_identity(environ, evpanda.Platform(
                        id=partner.id, name=partner.name,
                    ))
                return app(environ, start_response)
            return middleware

    The mapping is read when the response completes, so this works whether
    your auth layer runs outside the capture middleware or inside it.
    """
    carrier[IDENTITY_KEY] = identity


def identity_from(carrier: Mapping[str, Any] | None) -> Platform | None:
    """The identity :func:`set_identity` stamped on a mapping, if any."""
    if not isinstance(carrier, Mapping):
        return None
    value = carrier.get(IDENTITY_KEY)
    return value if isinstance(value, Platform) else None


@contextmanager
def use_identity(identity: Platform) -> Iterator[None]:
    """Attribute every OCPI call made inside the block to ``identity``.

    The natural way to attribute an outgoing call, where there is no
    request mapping to stamp yet::

        with ocpi.use_identity(partner_identity):
            response = client.post(f"{partner.url}/sessions", json=payload)

    It is a :class:`~contextvars.ContextVar` underneath, so it is
    task-local in async code and thread-local in sync code.
    """
    token = _identity_var.set(identity)
    try:
        yield
    finally:
        _identity_var.reset(token)


def current_identity() -> Platform | None:
    """The identity :func:`use_identity` is currently in scope for, if any."""
    return _identity_var.get()


def identity_from_headers(headers: Mapping[str, str]) -> Platform | None:
    """The identity carried by the ``X-EVPanda-*`` headers, if any.

    Tenant stays all-or-nothing: set both tenant values or neither, since a
    half-set pair fails validation and drops the message.
    """
    platform_id = headers.get(HEADER_PLATFORM_ID, "").strip()
    platform_name = headers.get(HEADER_PLATFORM_NAME, "").strip()
    if not platform_id and not platform_name:
        return None
    return Platform(
        id=platform_id,
        name=platform_name,
        tenant_id=headers.get(HEADER_TENANT_ID, "").strip() or None,
        tenant_name=headers.get(HEADER_TENANT_NAME, "").strip() or None,
    )


@dataclass(frozen=True, slots=True)
class RequestInfo:
    """One request, as much of it as a resolver could want.

    Adapters build it once per request and hand it to the resolver.
    """

    #: The HTTP method, uppercased.
    method: str
    #: The request URL as the host saw it — a path for inbound requests,
    #: an absolute URL for outgoing ones.
    url: str
    #: Request headers, keys lowercased, repeats comma-joined.
    headers: Mapping[str, str] = field(default_factory=dict)
    #: Whatever the adapter's own carrier held: the value
    #: :func:`set_identity` put on the WSGI environ or ASGI scope, or an
    #: httpx request extension.
    identity: Platform | None = None
    #: The framework-native object, for a resolver that needs more than the
    #: fields above: the WSGI environ, the ASGI scope, the ``httpx.Request``
    #: or the ``requests.PreparedRequest``.
    context: Any = None


#: Derives the roaming partner's identity for one request. Returning None —
#: or an identity that fails validation — means the exchange is not
#: captured; the request itself is never blocked or altered because of it.
type Resolver = Callable[[RequestInfo], Platform | None]


def default_resolver(info: RequestInfo) -> Platform | None:
    """What every adapter uses when no resolver is configured.

    The request mapping first, then the :func:`use_identity` ContextVar,
    then the ``X-EVPanda-*`` headers. A request carrying none of the three
    is simply not captured — no error, no partial record.
    """
    if info.identity is not None:
        return info.identity
    scoped = current_identity()
    if scoped is not None:
        return scoped
    return identity_from_headers(info.headers)


def resolve(resolver: Resolver | None, info: RequestInfo) -> Platform | None:
    """Run a resolver under a guard and validate what it returns.

    A resolver that raises, returns None, or returns an invalid identity
    all mean the same thing: skip capture for this request.
    """
    try:
        identity = (resolver or default_resolver)(info)
    except Exception:  # noqa: BLE001 - a broken resolver must not fail the request
        return None
    if identity is None or not isinstance(identity, Platform):
        return None
    return identity if identity.valid() else None


# ── Bounded body accumulation ────────────────────────────────────────────


class CappedBody:
    """Accumulates at most ``limit`` bytes.

    Crossing the cap sets ``overflowed`` and releases what was held: the
    SDK drops an oversize exchange rather than storing half a body, so
    there is nothing to keep.

    It is lock-guarded because a request body can be written by a
    transport's own thread while the capture is assembled on the caller's.
    """

    __slots__ = ("_chunks", "_limit", "_lock", "_overflowed", "_size")

    def __init__(self, limit: int) -> None:
        self._lock = threading.Lock()
        self._chunks: list[bytes] = []
        self._size = 0
        self._limit = limit
        self._overflowed = False

    def push(self, chunk: bytes) -> None:
        """Record a chunk, or mark the body overflowed if it no longer fits."""
        if not chunk:
            return
        with self._lock:
            if self._overflowed:
                return
            if self._size + len(chunk) > self._limit:
                self._overflowed = True
                self._chunks = []
                self._size = 0
                return
            self._chunks.append(bytes(chunk))
            self._size += len(chunk)

    def overflow(self) -> None:
        """Mark the body too large to keep, without having to read it."""
        with self._lock:
            self._overflowed = True
            self._chunks = []
            self._size = 0

    def result(self) -> tuple[bytes | None, bool]:
        """The accumulated bytes and whether the cap was exceeded."""
        with self._lock:
            if self._overflowed:
                return None, True
            if not self._chunks:
                return None, False
            return b"".join(self._chunks), False


# ── Header normalization ─────────────────────────────────────────────────


def header_map(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Flatten header pairs into the SDK's mapping.

    Keys are lowercased and repeated values comma-joined — the same
    normalization the Go and Node SDKs apply, so every SDK ships identical
    records. The OCPI redactor filters this against the allowlist
    afterwards.
    """
    out: dict[str, str] = {}
    for key, value in pairs:
        name = key.lower()
        existing = out.get(name)
        out[name] = value if existing is None else f"{existing}, {value}"
    return out


# ── Shipping ─────────────────────────────────────────────────────────────


@dataclass(slots=True)
class Exchange:
    """An assembled-but-not-yet-shipped capture.

    The bodies fill in as the host reads the request and writes the
    response, so this is built early and read at the end.
    """

    method: str
    url: str
    request_headers: dict[str, str]
    request_body: CappedBody
    response_body: CappedBody
    status_code: int | None = None
    response_headers: dict[str, str] = field(default_factory=dict)


def ship(
    client: Capturer,
    identity: Platform,
    exchange: Exchange,
    *,
    inbound: bool,
) -> None:
    """Turn a recorded exchange into a capture.

    It is dropped if either body outgrew the cap — the SDK would drop it at
    the chokepoint anyway, and this saves assembling the message first.
    """
    request_body, request_over = exchange.request_body.result()
    response_body, response_over = exchange.response_body.result()
    if request_over or response_over:
        return
    data = HTTPExchange(
        method=exchange.method,
        url=exchange.url,
        status_code=exchange.status_code,
        request_headers=exchange.request_headers,
        response_headers=exchange.response_headers,
        request_body=request_body,
        response_body=response_body,
    )
    if inbound:
        client.capture_inbound_message(identity, data)
    else:
        client.capture_outbound_message(identity, data)
