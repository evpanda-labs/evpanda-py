"""Drop-in capture adapters for OCPI traffic.

Four adapters, one per HTTP layer a Python service is likely to speak:

===============================  ==========================================
:class:`~evpanda.ocpi.wsgi.WSGIMiddleware`        inbound, Flask / Django / any WSGI app
:class:`~evpanda.ocpi.asgi.ASGIMiddleware`        inbound, FastAPI / Starlette / any ASGI app
``HTTPXTransport``               outbound, ``httpx`` (``AsyncHTTPXTransport`` for async)
``RequestsAdapter``              outbound, ``requests``
===============================  ==========================================

They assemble the HTTP exchange for you — collect headers and bodies,
resolve the partner's identity, call the right capture method — so a host
needs no capture code of its own::

    import evpanda
    from evpanda.ocpi.asgi import ASGIMiddleware

    panda = evpanda.start_ocpi()
    app = ASGIMiddleware(app, panda)

Identity comes from :func:`default_resolver` unless a ``resolver=`` of your
own replaces it: the request's own mapping first (:func:`set_identity` on a
WSGI environ or an ASGI scope, an ``evpanda.identity`` httpx extension),
then the :func:`use_identity` ContextVar, then the ``X-EVPanda-*`` headers.
A request with no resolvable identity is served exactly as it would have
been and simply is not captured — the adapters never block, alter, or fail
a request on the SDK's account.

The two outbound adapters are imported lazily, so ``httpx`` and
``requests`` are only needed if you actually use them:
``pip install 'evpanda[httpx]'`` or ``pip install 'evpanda[requests]'``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._adapter import (
    HEADER_PLATFORM_ID,
    HEADER_PLATFORM_NAME,
    HEADER_TENANT_ID,
    HEADER_TENANT_NAME,
    IDENTITY_HEADERS,
    IDENTITY_KEY,
    Capturer,
    RequestInfo,
    Resolver,
    current_identity,
    default_resolver,
    identity_from,
    identity_from_headers,
    set_identity,
    use_identity,
)
from .asgi import ASGIMiddleware
from .wsgi import WSGIMiddleware

if TYPE_CHECKING:  # the outbound adapters, for type checkers only
    from .httpx_transport import AsyncHTTPXTransport, HTTPXTransport
    from .requests_adapter import RequestsAdapter, instrument_session

#: Which module each lazily-imported name lives in, and what to install if
#: it is missing.
_LAZY = {
    "HTTPXTransport": ("httpx_transport", "httpx"),
    "AsyncHTTPXTransport": ("httpx_transport", "httpx"),
    "RequestsAdapter": ("requests_adapter", "requests"),
    "instrument_session": ("requests_adapter", "requests"),
}


def __getattr__(name: str) -> Any:
    """Import an outbound adapter on first use.

    Keeps ``import evpanda.ocpi`` free of third-party imports, so a host
    that only serves OCPI never installs an HTTP client it does not use.
    """
    entry = _LAZY.get(name)
    if entry is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, extra = entry
    try:
        module = __import__(f"{__name__}.{module_name}", fromlist=[name])
    except ImportError as exc:
        raise ImportError(
            f"{__name__}.{name} needs `{extra}` — pip install 'evpanda[{extra}]'"
        ) from exc
    return getattr(module, name)


def __dir__() -> list[str]:
    return sorted(__all__)


__all__ = [
    "HEADER_PLATFORM_ID",
    "HEADER_PLATFORM_NAME",
    "HEADER_TENANT_ID",
    "HEADER_TENANT_NAME",
    "IDENTITY_HEADERS",
    "IDENTITY_KEY",
    "ASGIMiddleware",
    "AsyncHTTPXTransport",
    "Capturer",
    "HTTPXTransport",
    "RequestInfo",
    "RequestsAdapter",
    "Resolver",
    "WSGIMiddleware",
    "current_identity",
    "default_resolver",
    "identity_from",
    "identity_from_headers",
    "instrument_session",
    "set_identity",
    "use_identity",
]
