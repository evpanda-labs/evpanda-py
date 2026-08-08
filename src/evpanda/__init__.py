"""evpanda — passive OCPI/OCPP traffic capture.

Embed it in your OCPI server or OCPP CSMS; it records protocol messages,
buffers them in-process, and ships them in batches to the EVPanda ingestion
API.

The SDK stays out of the host's way: capture calls are non-blocking and
never raise, memory is bounded, and under stress or network failure it
drops data rather than degrading the application.

The protocol is the class: :class:`~evpanda.ocpi.OCPIClient` takes an
``OCPIConfig``, :class:`~evpanda.ocpp.OCPPClient` an ``OCPPConfig`` — there
is no ``network_type`` switch::

    import evpanda

    panda = evpanda.OCPIClient.start(evpanda.OCPIConfig(
        endpoint="https://ingest.evpanda.io",   # api_key ⇒ EVPANDA_API_KEY
    ))
    try:
        panda.capture_inbound_message(evpanda.OCPIMessageInput(
            identity=evpanda.RoamingIdentity(
                platform_id="acme", platform_name="Acme Mobility",
            ),
            data=evpanda.HttpExchange(method="POST", url="/ocpi/2.2/cdrs",
                                      status_code=200),
        ))
    finally:
        panda.close()

The OCPI framework adapters are not ported yet.
"""

from __future__ import annotations

from .config import BaseConfig, Compression, ConfigError, OCPIConfig, OCPPConfig
from .identity import ChargerIdentity, RoamingIdentity
from .ocpi import OCPIClient
from .ocpp import OCPPClient, OCPPMessageInput, OCPPSession
from .types import (
    HttpExchange,
    OCPIDirection,
    OCPIMessage,
    OCPIMessageInput,
    OCPPDirection,
    OCPPEventType,
    OCPPMessage,
    Protocol,
)

__version__ = "0.1.0"

__all__ = [
    "BaseConfig",
    "ChargerIdentity",
    "Compression",
    "ConfigError",
    "HttpExchange",
    "OCPIClient",
    "OCPIConfig",
    "OCPIDirection",
    "OCPIMessage",
    "OCPIMessageInput",
    "OCPPClient",
    "OCPPConfig",
    "OCPPDirection",
    "OCPPEventType",
    "OCPPMessage",
    "OCPPMessageInput",
    "OCPPSession",
    "Protocol",
    "RoamingIdentity",
    "__version__",
]
