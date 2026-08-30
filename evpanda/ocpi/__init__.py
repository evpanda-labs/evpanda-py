"""Drop-in capture adapters for OCPI traffic.

One adapter per HTTP layer a Python service is likely to speak, each in the
module named after what it wraps:

=========================================  ==========================================
``evpanda.ocpi.wsgi.WSGIMiddleware``       inbound, Flask / Django / any WSGI app
``evpanda.ocpi.asgi.ASGIMiddleware``       inbound, FastAPI / Starlette / any ASGI app
``evpanda.ocpi.httpx_transport``           outbound, ``httpx`` — sync and async
``evpanda.ocpi.requests_adapter``          outbound, ``requests``
=========================================  ==========================================

They assemble the HTTP exchange for you — collect headers and bodies,
resolve the partner's identity, call the right capture method — so a host
needs no capture code of its own::

    import evpanda
    from evpanda.ocpi.asgi import ASGIMiddleware

    panda = evpanda.start_ocpi()
    app = ASGIMiddleware(app, panda)

Importing this package pulls in nothing but the standard library. The two
outbound adapters need ``httpx`` or ``requests``, and say so when their
module is imported without one: ``pip install 'evpanda[httpx]'``.

This package itself holds what the four adapters share — the client seam,
the identity carriers, and the resolver contract. Identity comes from
:func:`default_resolver` unless a ``resolver=`` of your own replaces it:
the request's own mapping first (:func:`set_identity` on a WSGI environ or
an ASGI scope, an ``evpanda.identity`` httpx extension), then the
:func:`use_identity` ContextVar, then the ``X-EVPanda-*`` headers. A
request with no resolvable identity is served exactly as it would have been
and simply is not captured — the adapters never block, alter, or fail a
request on the SDK's account.
"""

from __future__ import annotations

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

__all__ = [
    "HEADER_PLATFORM_ID",
    "HEADER_PLATFORM_NAME",
    "HEADER_TENANT_ID",
    "HEADER_TENANT_NAME",
    "IDENTITY_HEADERS",
    "IDENTITY_KEY",
    "Capturer",
    "RequestInfo",
    "Resolver",
    "current_identity",
    "default_resolver",
    "identity_from",
    "identity_from_headers",
    "set_identity",
    "use_identity",
]
