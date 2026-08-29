"""Per-tenant envelope encryption: the one implementation of the dangerous thing.

[ADR-0024](../../../mytheus-docs/adr/0024-key-custody-envelope-encryption.md) is the decision
record. This module is its D1/D2/D4 in code, and it exists as `libs/shared` for the same reason
`libs/shared/audit.py` does: two tables need it (`pii_tokens` now, `oauth_tokens` when it lands)
and two services will call it (`identity-tenant-service` owns both tables' key material,
`integration-service` reads it), so there must not be two dialects of key handling in one
program. There is no second place in this repo that may call AES, HMAC or HKDF.

WHAT THIS BUYS AND WHAT IT DOES NOT

The only `KeyProvider` implemented today is `LocalKeyProvider`, whose root key comes from
`MYTHEUS_ROOT_KEK` in the application's own environment. On the single-node substrate
(ADR-0016) that is the same host as the database, so it defends against a stolen dump or a
backup and not at all against a compromised host. ADR-0020's objection - that a vault with
ad-hoc key management is *strictly worse* than the irreversible redaction shipping today -
is the reason `is_production_grade` exists below and the reason the mint route refuses to run
without an explicit acknowledgement. Do not add a provider that reports `True` unless the root
key genuinely never enters this process.

THREE THINGS HERE ARE EASY TO GET SUBTLY WRONG, SO THEY ARE CENTRALIZED

1. **`org_id` is bound in as AEAD additional authenticated data, everywhere.** A ciphertext
   copied from one tenant's row to another's fails to open rather than opening into the wrong
   tenant. Without this, RLS is the only thing standing between a misrouted row and a
   cross-tenant disclosure, and RLS protects the query, not the bytes.
2. **The encryption key and the blind-index key are never the same bytes**, even when derived
   from key material that was generated the same way: HKDF with distinct info labels separates
   them before either primitive sees a key. Reusing one key across AES-GCM and HMAC is the
   textbook error, and here it would also let anyone who can compute the index compute nothing
   useful about the ciphertext, which is the property the separation preserves.
3. **The blind index is a keyed HMAC, never a plain digest.** Direct identifiers have
   catastrophically low entropy: the set of plausible email addresses is enumerable, so an
   unkeyed SHA-256 of an email is equivalent to storing the email in a column you then index.

ROTATION IS ASYMMETRIC ON PURPOSE (ADR-0024 D4)

`Purpose.ENC` rotates freely, because every `pii_tokens` row records the `dek_id` that
encrypted it and old rows stay readable under a retired key. `Purpose.INDEX` is minted once per
tenant and never retired: `value_hmac` carries a UNIQUE constraint that is the entire coherence
mechanism, so rotating its key would make every existing row unreachable, mint a second token
for a value that already had one, and fail *silently*. `assert_rotatable` refuses it rather
than trusting the caller to remember.
"""

from __future__ import annotations

import base64
import hmac
import os
from enum import StrEnum
from functools import lru_cache
from hashlib import sha256
from typing import Protocol
from uuid import UUID

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

# AES-256 and HMAC-SHA256 both want 32 bytes; GCM's standard nonce is 96 bits. These are not
# tunable knobs - a "configurable" cipher parameter is how a deployment ends up with a weak one.
KEY_BYTES = 32
_NONCE_BYTES = 12

# HKDF info labels. Versioned so a future scheme change is a new label rather than a silent
# reinterpretation of key material that is already in the database.
_INFO_ENC = b"mytheus/envelope/enc/v1"
_INFO_INDEX = b"mytheus/envelope/index/v1"
_INFO_TOKEN = b"mytheus/envelope/token/v1"

# The opaque placeholder body, e.g. the `7f3a` in `<PERSON:7f3a>`. 64 bits, keyed, so it leaks
# nothing about the value and is stable for the same value in the same tenant.
TOKEN_HEX_LEN = 16


class Purpose(StrEnum):
    """`tenant_keys.purpose`. See the module docstring on why these rotate differently."""

    ENC = "enc"
    INDEX = "index"


class KeyProvider(Protocol):
    """Custody of the root key, in exactly the two operations a cloud KMS already offers.

    Nothing above this interface knows whether the root key is a local secret, AWS KMS, GCP
    KMS or Vault transit. That is the whole point: adopting a real KMS is a deployment change,
    not a migration.
    """

    @property
    def key_id(self) -> str:
        """Recorded as `tenant_keys.wrap_key_id`, so the ROOT key can rotate too: a row says
        which root key wrapped it, and a re-wrap job can find the ones that are behind."""
        ...

    @property
    def is_production_grade(self) -> bool:
        """False when the root key is reachable from this process. See the module docstring."""
        ...

    def wrap(self, org_id: UUID, plaintext_key: bytes) -> bytes: ...

    def unwrap(self, org_id: UUID, wrapped_key: bytes) -> bytes: ...


class RootKeyUnavailable(RuntimeError):
    """Raised when no root key is configured. Deliberately not a soft failure.

    There is no generate-one-on-boot fallback anywhere in this module. A key that appears by
    itself is a key nobody decided to back up, and losing it destroys every value it protects
    (ADR-0024 § Consequences) - so the failure has to happen at configuration time, loudly,
    rather than at the first restore.
    """


def _aad(org_id: UUID, label: bytes) -> bytes:
    """The AEAD binding. `label` separates a wrapped key from an encrypted value so a wrapped
    DEK can never be opened by the value path or vice versa, even within one tenant."""
    return label + b"|" + str(org_id).encode("ascii")


def _seal(key: bytes, aad: bytes, plaintext: bytes) -> bytes:
    """`nonce || ciphertext`. A fresh random nonce per call: GCM nonce reuse under one key is
    catastrophic (it leaks the authentication subkey), and 96 random bits is the standard
    construction for a key that will not see 2^32 messages."""
    nonce = os.urandom(_NONCE_BYTES)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, aad)


def _open(key: bytes, aad: bytes, sealed: bytes) -> bytes:
    if len(sealed) <= _NONCE_BYTES:
        raise ValueError("ciphertext is too short to contain a nonce")
    return AESGCM(key).decrypt(sealed[:_NONCE_BYTES], sealed[_NONCE_BYTES:], aad)


class LocalKeyProvider:
    """Root key from `MYTHEUS_ROOT_KEK` (base64, 32 bytes). DEVELOPMENT ONLY.

    `is_production_grade` is `False` and must stay that way. This class is not a stepping stone
    to be hardened in place; the production shape is a separate provider whose `wrap`/`unwrap`
    are network calls to something that never releases the root key. See ADR-0024 D2.
    """

    ENV_VAR = "MYTHEUS_ROOT_KEK"

    def __init__(self, root_key: bytes, key_id: str = "local") -> None:
        if len(root_key) != KEY_BYTES:
            raise RootKeyUnavailable(
                f"{self.ENV_VAR} must decode to exactly {KEY_BYTES} bytes, " f"got {len(root_key)}"
            )
        self._root = root_key
        self._key_id = key_id

    @classmethod
    def from_env(cls) -> LocalKeyProvider:
        raw = os.environ.get(cls.ENV_VAR, "").strip()
        if not raw:
            raise RootKeyUnavailable(
                f"{cls.ENV_VAR} is unset. Generate one with "
                f'`python -c "import os,base64;'
                f'print(base64.b64encode(os.urandom(32)).decode())"` and note that LOSING IT '
                f"DESTROYS EVERY VALUE IT PROTECTS: a PostgreSQL backup is not a backup of "
                f"the vault (ADR-0024)."
            )
        try:
            root = base64.b64decode(raw, validate=True)
        except Exception as exc:  # noqa: BLE001 - any decode failure is the same misconfiguration
            raise RootKeyUnavailable(f"{cls.ENV_VAR} is not valid base64") from exc
        return cls(root)

    @property
    def key_id(self) -> str:
        return self._key_id

    @property
    def is_production_grade(self) -> bool:
        return False

    def wrap(self, org_id: UUID, plaintext_key: bytes) -> bytes:
        return _seal(self._root, _aad(org_id, b"wrap"), plaintext_key)

    def unwrap(self, org_id: UUID, wrapped_key: bytes) -> bytes:
        return _open(self._root, _aad(org_id, b"wrap"), wrapped_key)


@lru_cache(maxsize=1)
def get_key_provider() -> KeyProvider:
    """The process-wide provider, built lazily so importing this module never requires a key.

    Lazy for the same reason `libs.shared.db.get_engine` is: collection-time imports must not
    depend on deployment configuration. When `ExternalKmsKeyProvider` exists this is the one
    function that learns about it.
    """
    return LocalKeyProvider.from_env()


def new_key_material() -> bytes:
    """A fresh data key. `os.urandom` is the CSPRNG; nothing here derives a key from a
    passphrase, a tenant id, or anything else guessable."""
    return os.urandom(KEY_BYTES)


def _derive(key_material: bytes, info: bytes) -> bytes:
    """HKDF-SHA256, no salt.

    Salt is genuinely unnecessary here and omitting it is deliberate rather than lazy: HKDF's
    salt exists to extract entropy from non-uniform input keying material, and `key_material`
    is already 32 uniform bytes from `os.urandom`. The info label is what does the work, by
    separating the derived keys per purpose.
    """
    return HKDF(algorithm=SHA256(), length=KEY_BYTES, salt=None, info=info).derive(key_material)


def assert_rotatable(purpose: Purpose) -> None:
    """Refuse to rotate the blind-index key. ADR-0024 D4.

    This is a guard rather than a comment because the failure it prevents is silent: rotating
    the index key does not raise anywhere, it just makes every existing `value_hmac` unreachable
    so the next mint of an already-vaulted value creates a SECOND token for it. Cross-boundary
    coherence - the only reason the vault exists - would degrade with nothing in any log.

    Rotating it for real means decrypting and re-HMACing every row for that tenant in one
    transaction. That is a deliberate migration, not something the ordinary rotation path
    should be able to reach.
    """
    if purpose is Purpose.INDEX:
        raise ValueError(
            "the blind-index key is minted once per tenant and never rotated (ADR-0024 D4); "
            "re-keying it requires re-HMACing every pii_tokens row for the tenant in one "
            "transaction, which is a migration, not a rotation"
        )


def encrypt_value(dek: bytes, org_id: UUID, plaintext: str) -> bytes:
    """`value_ciphertext`. The DEK is the `Purpose.ENC` key material, already unwrapped."""
    return _seal(_derive(dek, _INFO_ENC), _aad(org_id, b"value"), plaintext.encode("utf-8"))


def decrypt_value(dek: bytes, org_id: UUID, ciphertext: bytes) -> str:
    return _open(_derive(dek, _INFO_ENC), _aad(org_id, b"value"), ciphertext).decode("utf-8")


def normalize_value(entity_type: str, raw: str) -> str:
    """The normalization the blind index is computed over, and therefore the definition of
    "the same value" for coherence purposes.

    It has to be conservative in one specific direction: two spellings that normalize together
    share a token, so over-normalizing merges two distinct people. Case and surrounding
    whitespace are safe to fold for every type. Digit-bearing identifiers additionally drop
    formatting separators, because `+1 (555) 010-9999` and `+15550109999` are the same phone
    number and a transcript will contain both spellings across a meeting.

    Deliberately NOT normalized: `PERSON`/`ORG`/`LOC`. "Dave" and "David Chen" are not
    reliably the same person, and a vault that guesses they are produces a confidently wrong
    coherence claim in a confirmed decision - the exact failure ADR-0020's "stays coherent"
    requirement is trying to avoid. Entity resolution is a different problem and does not
    belong behind a hash function.
    """
    folded = " ".join(raw.split()).casefold()
    if entity_type in ("PHONE", "IBAN", "CARD", "GOVID"):
        return "".join(ch for ch in folded if ch.isalnum())
    return folded


def blind_index(index_key: bytes, org_id: UUID, entity_type: str, raw: str) -> bytes:
    """`value_hmac`. Keyed, tenant-bound, and type-bound.

    `entity_type` is inside the MAC as well as in the UNIQUE constraint so a value cannot be
    confused across types (the string "12345678" as a `GOVID` and as a `CARD` are different
    facts). The separators make the input unambiguous: without them, ("EMAIL", "ab") and
    ("EMAILA", "b") would MAC identically.
    """
    msg = b"\x00".join(
        (
            str(org_id).encode("ascii"),
            entity_type.encode("ascii"),
            normalize_value(entity_type, raw).encode("utf-8"),
        )
    )
    return hmac.new(_derive(index_key, _INFO_INDEX), msg, sha256).digest()


def derive_token(index_key: bytes, org_id: UUID, value_hmac: bytes, attempt: int = 0) -> str:
    """The placeholder body, derived rather than random.

    Derived so that minting is naturally idempotent: the same value in the same tenant produces
    the same token without a round trip, which matters because a redaction pass mints in
    batches and a retry must not create a second token for a value that already has one.

    Keyed, and computed from `value_hmac` rather than the raw value, so the token itself is a
    pure opaque label - it is safe to write into `transcript_chunks`, ship over the bus, and
    show in a UI, which is the whole point of a placeholder.

    `attempt` is the collision escape hatch. Truncating to 64 bits makes a collision between
    two DIFFERENT values in one tenant vanishingly unlikely but not impossible, and the
    consequence would be one token resolving to the wrong person's data. `UNIQUE (org_id,
    token)` turns that into an insert failure rather than a silent corruption, and the caller
    re-derives at the next attempt instead of giving up or, worse, reusing the row.
    """
    msg = value_hmac + b"\x00" + str(attempt).encode("ascii")
    return hmac.new(_derive(index_key, _INFO_TOKEN), msg, sha256).hexdigest()[:TOKEN_HEX_LEN]
