"""Hand-maintained message types; must match apispec/ingestion-api.yaml
(fixture pack). ``protocol``/``captured_at`` are SDK-owned (internal
envelope, see :mod:`evpanda.buffer`) — deliberately not on these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum

from .identity import ChargerIdentity, RoamingIdentity


class Protocol(StrEnum):
    """The wire protocol a client serves."""

    OCPI = "ocpi"
    OCPP = "ocpp"


class OCPIDirection(StrEnum):
    """Direction of an OCPI message relative to the host."""

    IN = "IN"
    OUT = "OUT"


class OCPPDirection(StrEnum):
    """Direction of an OCPP frame relative to the charge point."""

    TO_CP = "TO_CP"
    FROM_CP = "FROM_CP"


class OCPPEventType(IntEnum):
    """OCPP WS lifecycle → ingestion event_type."""

    DISCONNECT = 0
    CONNECT = 1
    MESSAGE = 2


@dataclass
class HttpExchange:
    """A captured HTTP request/response pair."""

    method: str
    url: str
    status_code: int | None = None
    request_headers: dict[str, str] = field(default_factory=dict)
    response_headers: dict[str, str] = field(default_factory=dict)
    #: Capped at ``max_capture_bytes``; an oversize body drops the whole message.
    request_body: bytes | None = None
    response_body: bytes | None = None


@dataclass
class OCPIMessage:
    """A captured OCPI HTTP message."""

    direction: OCPIDirection
    identity: RoamingIdentity
    data: HttpExchange


@dataclass
class OCPIMessageInput:
    """OCPI message as supplied to ``OCPIClient.capture_inbound_message`` /
    ``capture_outbound_message``. ``direction`` is omitted — the chosen
    method sets it.
    """

    identity: RoamingIdentity
    data: HttpExchange


@dataclass
class OCPPMessage:
    """A captured OCPP WebSocket event."""

    event_type: OCPPEventType
    identity: ChargerIdentity
    #: SDK-owned UUID, stable per connection, regenerated on reconnect.
    connection_id: str
    #: Optional for OCPP.
    direction: OCPPDirection | None = None
    payload: bytes | None = None


AnyMessage = OCPIMessage | OCPPMessage
