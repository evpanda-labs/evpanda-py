"""Message types and per-message identity.

The wire-facing shapes here must match ``apispec/ingestion-api.yaml``.
``protocol`` and the capture timestamp are SDK-owned — they live on the
internal envelope (see :mod:`evpanda._buffer`) and deliberately not on
these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum

# ── Protocol and direction ───────────────────────────────────────────────


class Protocol(StrEnum):
    """Routes a batch to ``POST /v1/{protocol}``. One client, one protocol."""

    OCPI = "ocpi"
    OCPP = "ocpp"


class OCPIDirection(StrEnum):
    """An OCPI message's direction relative to the host.

    Internal: the capture method you call stamps it, so there is no field
    to get backwards. The values are the exact wire strings the ingestion
    API validates.
    """

    IN = "IN"
    OUT = "OUT"


class OCPPDirection(StrEnum):
    """An OCPP frame's direction relative to the charge point.

    ``FROM_CP`` is a frame the charger sent you, ``TO_CP`` one you send it.
    The values are the exact wire strings the ingestion API validates.
    """

    TO_CP = "TO_CP"
    FROM_CP = "FROM_CP"


class OCPPEventType(IntEnum):
    """An OCPP WebSocket lifecycle event, mapped onto ``event_type``."""

    DISCONNECT = 0
    CONNECT = 1
    MESSAGE = 2


# ── Identity ─────────────────────────────────────────────────────────────
#
# Per-message identity: the two protocol shapes and their validation
# rules. ``valid()`` is the single rule source — every capture path goes
# through it, and the adapters use it to decide whether instrumenting a
# request is worth the work. Nothing here raises; an invalid identity
# means the caller drops the message.


def _is_non_empty(v: object) -> bool:
    """A usable string value: present, a string, not blank."""
    return isinstance(v, str) and v.strip() != ""


def _is_tenant_pair_valid(tenant_id: object, tenant_name: object) -> bool:
    """Tenant is all-or-nothing: both ``tenant_id`` and ``tenant_name``, or neither."""
    return _is_non_empty(tenant_id) == _is_non_empty(tenant_name)


@dataclass(frozen=True, slots=True)
class Platform:
    """The roaming partner an OCPI message was exchanged with.

    It is always the partner on the other side — never your own platform.

    ``id`` and ``name`` are required. ``tenant_id`` and ``tenant_name``
    describe a different subject: which of *your* tenants the exchange
    belongs to, which is why they keep the prefix the platform's own
    fields do not need. They are optional but all-or-nothing — supply both
    or neither.
    """

    id: str
    name: str
    tenant_id: str | None = None
    tenant_name: str | None = None

    def valid(self) -> bool:
        """Whether this platform can attribute a message.

        ``id`` and ``name`` present, and the tenant pair all-or-nothing.
        The SDK silently drops messages that fail it.
        """
        return (
            _is_non_empty(self.id)
            and _is_non_empty(self.name)
            and _is_tenant_pair_valid(self.tenant_id, self.tenant_name)
        )


@dataclass(frozen=True, slots=True)
class Charger:
    """The charge point an OCPP event belongs to.

    ``id`` is required. ``tenant_id`` and ``tenant_name`` say which of
    your tenants the charger belongs to; they are optional but
    all-or-nothing.
    """

    id: str
    tenant_id: str | None = None
    tenant_name: str | None = None

    def valid(self) -> bool:
        """Whether this charger can attribute a message."""
        return _is_non_empty(self.id) and _is_tenant_pair_valid(self.tenant_id, self.tenant_name)


# ── Captured data ────────────────────────────────────────────────────────

#: What a caller may hand us as a body or frame. ``str`` is encoded as
#: UTF-8; ``bytearray`` and ``memoryview`` are copied into ``bytes`` at the
#: capture chokepoint, so the SDK never aliases a buffer the host reuses.
type BodyInput = bytes | bytearray | memoryview | str


def coerce_body(value: BodyInput | None) -> bytes | None:
    """Normalize a caller's body to immutable ``bytes``, or None when empty.

    ``str`` is encoded as UTF-8; ``bytearray`` and ``memoryview`` are
    copied, so the SDK owns what it buffers and the host is free to reuse
    its own buffer the moment the capture call returns. ``bytes`` is
    already immutable and is handed straight through — which is why this
    SDK needs no equivalent of the Go SDK's body-copying setters.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return value or None
    if isinstance(value, str):
        return value.encode("utf-8") or None
    return bytes(value) or None


@dataclass(slots=True)
class HTTPExchange:
    """A captured HTTP request/response pair.

    Bodies are raw bytes (a ``str`` is encoded as UTF-8). A body larger
    than ``max_capture_bytes`` drops the whole message at capture rather
    than storing a truncated one.

    ``status_code`` and both bodies are optional; either header mapping may
    be left empty.
    """

    method: str
    url: str
    status_code: int | None = None
    request_headers: dict[str, str] = field(default_factory=dict)
    response_headers: dict[str, str] = field(default_factory=dict)
    request_body: BodyInput | None = None
    response_body: BodyInput | None = None


# ── Internal buffered forms ──────────────────────────────────────────────
#
# What the ring buffer actually holds: the caller's input plus whatever the
# capture method stamped. ``size`` is buffer bookkeeping and lives with the
# accounting constants below; the wire mapping lives in _transport.py.

#: Per-message accounting overhead, deliberately generous: the envelope,
#: the object header, attribute slots, and the list slot itself.
#: Over-counting keeps the configured budget a true ceiling — under-counting
#: would quietly break it. The numbers match the Go SDK so the two report
#: comparable buffer footprints.
_ENVELOPE_OVERHEAD = 256
_HEADER_ENTRY_OVERHEAD = 48


def _header_size(headers: dict[str, str] | None) -> int:
    """Charge each header entry its key and value plus a share of dict overhead."""
    if not headers:
        return 0
    return sum(len(k) + len(v) + _HEADER_ENTRY_OVERHEAD for k, v in headers.items())


@dataclass(slots=True)
class OCPIMessage:
    """The internal buffered form of an OCPI capture."""

    direction: OCPIDirection
    identity: Platform
    data: HTTPExchange

    def size(self) -> int:
        """The accounted footprint of this capture, in bytes."""
        d = self.data
        return (
            _ENVELOPE_OVERHEAD
            + len(self.direction)
            + len(self.identity.id)
            + len(self.identity.name)
            + len(self.identity.tenant_id or "")
            + len(self.identity.tenant_name or "")
            + len(d.method)
            + len(d.url)
            + len(d.request_body or b"")
            + len(d.response_body or b"")
            + _header_size(d.request_headers)
            + _header_size(d.response_headers)
        )


@dataclass(slots=True)
class OCPPMessage:
    """The internal buffered form of an OCPP capture.

    ``direction`` and ``payload`` are None for connect and disconnect.
    """

    event_type: OCPPEventType
    identity: Charger
    connection_id: str
    direction: OCPPDirection | None = None
    payload: BodyInput | None = None

    def size(self) -> int:
        """The accounted footprint of this capture, in bytes."""
        return (
            _ENVELOPE_OVERHEAD
            + len(self.identity.id)
            + len(self.identity.tenant_id or "")
            + len(self.identity.tenant_name or "")
            + len(self.connection_id)
            + len(self.direction or "")
            + len(self.payload or b"")
        )


#: What the ring buffer carries, without a tag to switch on.
type Message = OCPIMessage | OCPPMessage
