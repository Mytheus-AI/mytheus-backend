"""Keyed blind indexes: an equality-searchable column that is not an oracle for the value.

`pii_tokens.value_hmac` must let the vault find "have we seen this email before in this
tenant?" without storing anything from which the email can be recovered. A plain digest fails
that: the space of plausible emails or phone numbers is small enough to enumerate, so
`sha256(email)` is the email. An HMAC under a key the database never sees is not.

The key is per tenant AND per purpose, derived by the `KeyProvider` (HKDF locally, a KMS MAC
in production), so the same value in two tenants indexes to unrelated bytes and a stolen
index column from one tenant says nothing about another.

The derived key is deterministic and NOT rotated with the KEK. Rotating it would change every
stored index and break dedupe, so it is a deliberate, separate operation (a re-index job) and
is recorded as such in ADR-0024.
"""

from __future__ import annotations

import hmac
import threading
import uuid
from hashlib import sha256

from libs.shared.crypto._encoding import validate_org_id, validate_purpose
from libs.shared.crypto.providers import KeyProvider

_CACHE_MAX = 4096


class BlindIndex:
    def __init__(self, provider: KeyProvider) -> None:
        self._provider = provider
        self._lock = threading.Lock()
        self._keys: dict[tuple[uuid.UUID, str], bytes] = {}

    def __repr__(self) -> str:
        return f"BlindIndex(provider={self._provider!r})"

    def _key(self, org_id: uuid.UUID, purpose: str) -> bytes:
        slot = (org_id, purpose)
        with self._lock:
            hit = self._keys.get(slot)
        if hit is not None:
            return hit
        key = self._provider.derive_tenant_key(org_id, purpose)
        with self._lock:
            if len(self._keys) >= _CACHE_MAX:
                self._keys.clear()
            self._keys[slot] = key
        return key

    def compute(self, org_id: uuid.UUID, purpose: str, message: bytes) -> bytes:
        """HMAC-SHA256 of `message` under the (tenant, purpose) key. 32 bytes."""
        org_id = validate_org_id(org_id)
        purpose = validate_purpose(purpose)
        if not isinstance(message, bytes):
            raise TypeError("message must be bytes")
        return hmac.new(self._key(org_id, purpose), message, sha256).digest()
