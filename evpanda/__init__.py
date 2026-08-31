"""Passive OCPI/OCPP traffic capture for Python.

Embed it in your OCPI server or OCPP CSMS and it records protocol
messages, buffers them in-process, and ships them in batches to the EVPanda
ingestion API.

The SDK stays out of the host's way: capture calls are non-blocking and
never raise, memory is bounded, and under stress or network failure it
drops data rather than degrading the application.

The protocol is the client
--------------------------

:func:`start_ocpi` returns an :class:`OCPIClient`, :func:`start_ocpp` an
:class:`OCPPClient` — pick the one your service speaks. Both always hand
back a usable client: on a bad endpoint or API key the client is an inert
no-op carrying the fault on ``.error``, so a config typo can never crash
the host's boot::

    import evpanda

    # api_key omitted ⇒ read from the EVPANDA_API_KEY env var, and
    # endpoint defaults to the production ingestion API.
    panda = evpanda.start_ocpi()
    if panda.error:
        log.warning("%s (running inert)", panda.error)

    panda.capture_inbound_message(identity=..., data=...)

For OCPP, prefer the session handle returned by
:meth:`OCPPClient.connection`: it mints the connection ID and carries the
identity, so per-frame calls carry neither::

    with panda.connection(evpanda.Charger(id="CP-001")) as session:
        session.message(frame, evpanda.OCPPDirection.FROM_CP)

OCPI adapters
-------------

:mod:`evpanda.ocpi` wraps the HTTP layers a Python service already speaks
so a host needs no capture code of its own — WSGI and ASGI middleware for
the requests partners make to you, an httpx transport and a requests
adapter for the ones you make to them. A request with no resolvable
identity is served exactly as it would have been, and simply is not
captured.

Knowing whether it is working
-----------------------------

Every client exposes :meth:`~OCPIClient.stats`, a snapshot of its delivery
counters whose fields each map to one root cause — see :class:`Stats`.
Problems are also reported to your logger by default, summarized once a
minute so a fault that recurs on every request still costs one line;
:class:`LogMode` and the ``EVPANDA_LOG`` environment variable control that.

Delivery
--------

Capture hands the message to a byte-bounded buffer and returns. One
background thread owns delivery: it flushes when a full batch (1000) is
waiting or when ``flush_interval`` elapses, whichever comes first, and the
transport retries with capped exponential backoff. If the upstream is slow
or down the buffer evicts its oldest entries once ``max_buffer_bytes`` is
reached — the host never blocks and memory never grows past that ceiling.

Both clients carry the same lifecycle methods: ``flush`` forces a delivery
and waits for it, and ``close`` drains what is buffered within
``drain_timeout`` (or a timeout you pass) and reports whether it managed
to. Both are idempotent, both are context managers, and captures made
after close are safe no-ops. A client the host never closes is drained at
interpreter exit.
"""

from __future__ import annotations

from ._client import (
    OCPIClient,
    OCPPClient,
    OCPPSession,
    start_ocpi,
    start_ocpp,
)
from ._config import (
    API_KEY_ENV_VAR,
    DEFAULT_ENDPOINT,
    LOG_MODE_ENV_VAR,
    APIKeyError,
    BaseConfig,
    ConfigError,
    EndpointError,
    EVPandaError,
    LogMode,
    OCPIConfig,
    OCPPConfig,
)
from ._stats import Stats
from ._types import (
    Charger,
    HTTPExchange,
    OCPPDirection,
    Platform,
)

__version__ = "0.3.0"

__all__ = [
    "API_KEY_ENV_VAR",
    "DEFAULT_ENDPOINT",
    "LOG_MODE_ENV_VAR",
    "APIKeyError",
    "BaseConfig",
    "Charger",
    "ConfigError",
    "EVPandaError",
    "EndpointError",
    "HTTPExchange",
    "LogMode",
    "OCPIClient",
    "OCPIConfig",
    "OCPPClient",
    "OCPPConfig",
    "OCPPDirection",
    "OCPPSession",
    "Platform",
    "Stats",
    "__version__",
    "start_ocpi",
    "start_ocpp",
]
