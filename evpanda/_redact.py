"""Redaction, applied at the capture chokepoint.

The chokepoint is the last point at which a secret can be removed, since
the next step is memory that outlives the call.

OCPI has two rules:

1. **Header allowlist** — only listed headers are kept; ``Authorization``,
   ``Cookie``, ``X-API-Key`` and anything else unlisted fall off the end.
   ``OCPIConfig.ocpi_allowed_headers`` extends the list, never shrinks it.
2. **Credentials-endpoint token mask** — on a ``/credentials`` URL the
   ``token`` field (at the root for requests, under ``data`` for the
   response envelope) is replaced with ``[redacted]``.

There is no OCPP redactor. Frames are captured verbatim, so the OCPP
client's redactor stays None and the chokepoint skips the step entirely
rather than paying a call per frame to run an identity transform. The seam
is the None-able argument itself: masking ``idTag`` in ``Authorize``, say,
arrives as a ``make_ocpp_redactor`` here plus one line in
``start_ocpp``, with nothing in the worker or the client to change.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable
from typing import Any

from ._types import OCPIMessage, OCPPMessage, coerce_body

#: The set of stock OCPI headers safe to capture — none of these can carry
#: a secret.
DEFAULT_OCPI_HEADER_ALLOWLIST: tuple[str, ...] = (
    # OCPI routing
    "ocpi-from-country-code",
    "ocpi-from-party-id",
    "ocpi-to-country-code",
    "ocpi-to-party-id",
    # Content negotiation and standard HTTP
    "content-type",
    "accept",
    "user-agent",
    # Tracing
    "x-correlation-id",
    "x-request-id",
    # Pagination
    "x-total-count",
    "x-limit",
    "link",
)

#: Written in place of redacted token values.
TOKEN_PLACEHOLDER = "[redacted]"

#: Matches a URL ending with ``/credentials``, ``/credentials/`` or
#: ``/credentials?…``. Sub-paths like ``/credentials/foo`` do not match —
#: there is no such OCPI route.
_CREDENTIALS_URL = re.compile(r"/credentials/?(?:\?|$)", re.IGNORECASE)

#: The transforms applied to a message right before it is enqueued.
type OCPIRedactor = Callable[[OCPIMessage], OCPIMessage]
type OCPPRedactor = Callable[[OCPPMessage], OCPPMessage]


def make_ocpi_redactor(extra_allowed_headers: Iterable[str] = ()) -> OCPIRedactor:
    """Build the redactor from the resolved config.

    Called once per client, so the allowlist set is amortized across every
    message. The chokepoint has already taken ownership of the message, so
    the redactor is free to rewrite it in place.
    """
    allow = {h.lower() for h in (*DEFAULT_OCPI_HEADER_ALLOWLIST, *extra_allowed_headers)}

    def redact(message: OCPIMessage) -> OCPIMessage:
        data = message.data
        data.request_headers = filter_headers(data.request_headers, allow)
        data.response_headers = filter_headers(data.response_headers, allow)
        data.request_body = mask_credentials_token(coerce_body(data.request_body), data.url)
        data.response_body = mask_credentials_token(coerce_body(data.response_body), data.url)
        return message

    return redact


def filter_headers(headers: dict[str, str], allow: set[str]) -> dict[str, str]:
    """Keep only allowlisted headers, matching the key case-insensitively."""
    if not headers:
        return {}
    return {k: v for k, v in headers.items() if k.lower() in allow}


def mask_credentials_token(body: bytes | None, url: str) -> bytes | None:
    """Mask the ``token`` field in an OCPI credentials body.

    Returns the original bytes on any miss — a non-credentials URL,
    non-JSON, no token at either known path, a re-encode error. Redaction
    never silently drops data it could not safely rewrite.
    """
    if not body or not _CREDENTIALS_URL.search(url):
        return body
    try:
        parsed = json.loads(body)
    except (ValueError, TypeError, UnicodeDecodeError):
        return body
    if not isinstance(parsed, dict):
        return body

    # The token lives at the root (request) or under `data` (the response
    # envelope). We own `parsed`, so it is rewritten in place.
    if _has_token(parsed):
        parsed["token"] = TOKEN_PLACEHOLDER
    elif _has_token(parsed.get("data")):
        parsed["data"]["token"] = TOKEN_PLACEHOLDER
    else:
        return body

    try:
        return json.dumps(parsed, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError):
        return body


def _has_token(obj: Any) -> bool:
    """Whether ``obj`` is a mapping carrying a non-empty string ``token``."""
    if not isinstance(obj, dict):
        return False
    token = obj.get("token")
    return isinstance(token, str) and token != ""
