"""Unambiguous byte encodings for authenticated context.

Both the DEK wrap and the data encryption authenticate a set of named fields (`org_id`,
`purpose`, `dek_id`, caller-supplied row context). Concatenating them naively is ambiguous -
`("ab", "c")` and `("a", "bc")` would produce the same bytes - which turns "bound to this
tenant" into "bound to any tenant whose fields happen to concatenate the same way". Every
field here is length-prefixed, the keys are sorted, and the whole thing carries a domain
label so an encoding built for one use can never authenticate as another.
"""

from __future__ import annotations

import re
import struct
import uuid
from collections.abc import Mapping

# Lowercase, dot/underscore/colon/hyphen separated, short. A purpose is a stable code-level
# constant such as `oauth_tokens.access_token`; it is not user input, and constraining its
# shape keeps a typo from silently becoming a new, unrelated key domain.
_PURPOSE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_CONTEXT_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

# Reserved: the library sets these itself, so caller context may not shadow them.
RESERVED_CONTEXT_KEYS = frozenset({"org_id", "purpose", "dek_id", "kek_id"})


def validate_purpose(purpose: str) -> str:
    if not isinstance(purpose, str) or not _PURPOSE_RE.match(purpose):
        raise ValueError(
            "purpose must match ^[a-z][a-z0-9_.:-]{0,63}$ (a code constant, e.g. "
            "'oauth_tokens.access_token')"
        )
    return purpose


def validate_org_id(org_id: uuid.UUID) -> uuid.UUID:
    # A str that merely looks like a UUID is refused: the tenant key is typed everywhere else
    # in this codebase, and accepting strings here is how "org_id" ends up being a Clerk id.
    if not isinstance(org_id, uuid.UUID):
        raise TypeError("org_id must be a uuid.UUID")
    return org_id


def validate_context(context: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in (context or {}).items():
        if not isinstance(key, str) or not _CONTEXT_KEY_RE.match(key):
            raise ValueError(f"context key {key!r} must match ^[a-z][a-z0-9_]{{0,63}}$")
        if key in RESERVED_CONTEXT_KEYS:
            raise ValueError(f"context key {key!r} is reserved by the envelope library")
        if not isinstance(value, str):
            raise TypeError(f"context value for {key!r} must be str")
        out[key] = value
    return out


def encode_fields(label: bytes, fields: Mapping[str, str]) -> bytes:
    """`label || count || (len(k) k len(v) v)*` with keys sorted. Injective by construction."""
    parts = [struct.pack(">H", len(label)), label, struct.pack(">I", len(fields))]
    for key in sorted(fields):
        k = key.encode("utf-8")
        v = fields[key].encode("utf-8")
        parts += [struct.pack(">I", len(k)), k, struct.pack(">I", len(v)), v]
    return b"".join(parts)
