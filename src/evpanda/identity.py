"""Per-message identity: the two protocol shapes and the validation rules.
``validate_*`` is the single rule source — every capture path (adapter and
primitive) goes through it. Nothing here raises; an absent/invalid identity
⇒ the caller drops the message.

The OCPI resolver contract (``OCPIResolver`` / ``OCPIResolverCtx``) lands
with the adapters — they are the only consumers of it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RoamingIdentity:
    """OCPI roaming context. platform required; tenant all-or-nothing."""

    platform_id: str
    platform_name: str
    tenant_id: str | None = None
    tenant_name: str | None = None


@dataclass
class ChargerIdentity:
    """OCPP charger context. ``charger_id`` required; tenant all-or-nothing."""

    charger_id: str
    tenant_id: str | None = None
    tenant_name: str | None = None


# ── Validators — the single rule source ──────────────────────────────────


def _is_non_empty(v: Any) -> bool:
    """A usable string value: present, a string, not blank."""
    return isinstance(v, str) and v.strip() != ""


def _is_tenant_pair_valid(tenant_id: Any, tenant_name: Any) -> bool:
    """Tenant is all-or-nothing: both tenant_id & tenant_name, or neither."""
    return _is_non_empty(tenant_id) == _is_non_empty(tenant_name)


def validate_roaming_identity(identity: RoamingIdentity | None) -> bool:
    """True iff platform_id + platform_name non-empty and tenant all-or-nothing."""
    return (
        isinstance(identity, RoamingIdentity)
        and _is_non_empty(identity.platform_id)
        and _is_non_empty(identity.platform_name)
        and _is_tenant_pair_valid(identity.tenant_id, identity.tenant_name)
    )


def validate_charger_identity(identity: ChargerIdentity | None) -> bool:
    """True iff charger_id non-empty and tenant all-or-nothing."""
    return (
        isinstance(identity, ChargerIdentity)
        and _is_non_empty(identity.charger_id)
        and _is_tenant_pair_valid(identity.tenant_id, identity.tenant_name)
    )
