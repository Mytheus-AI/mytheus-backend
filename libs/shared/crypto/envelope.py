"""Envelope encryption: AES-256-GCM under per-tenant data keys, wrapped by a KEK.

The scheme, in one paragraph. Each (`org_id`, `purpose`) pair gets its own data key (DEK),
generated locally from `os.urandom`, wrapped once by the `KeyProvider`'s active KEK, and cached
unwrapped in process memory for a bounded time and use count. Each value is encrypted under
that DEK with a fresh 96-bit nonce, and the resulting blob carries everything needed to decrypt
it except the KEK: the KEK id, the DEK id, the wrapped DEK, the nonce and the ciphertext. Two
tenants never share a DEK, and every encryption authenticates `org_id` + `purpose` (+ any
caller row context) as AAD, so a blob copied from tenant A's row into tenant B's - or from an
access-token column into a refresh-token column - fails authentication instead of decrypting.

Why the wrapped DEK rides inside every blob rather than in a key table. A key table would be a
store with no single owning service: `oauth_tokens` (integration-service) and `pii_tokens`
(identity-tenant-service) both need keys, and a shared key table read and written by both is
the cross-service store the one-writer rule forbids. A self-describing blob needs no table at
all, any process holding the provider can decrypt it, and this is the shape the AWS Encryption
SDK and Tink both use. The cost is ~80 bytes per value, which on a token table is noise.

Rotation, both kinds:

- DEK rotation is automatic. A cached DEK is retired for new writes after `dek_max_age` or
  `dek_max_uses` (well under the 2**32 random-nonce bound for GCM) or when the active KEK
  changes. Old blobs keep decrypting: their DEK is in the blob.
- KEK rotation is `rewrap(...)`: unwrap the DEK under the retired KEK, wrap it under the active
  one, keep the data ciphertext byte-for-byte. The DEK id is unchanged, so an indexed `dek_id`
  column stays correct. `needs_rewrap(blob)` finds the rows a rotation job still has to visit.
  `reencrypt(...)` is the heavier option that also replaces the DEK (e.g. after a suspected
  DEK exposure).

The blob layout (v1), all integers big-endian:

    b"MYE" | 0x01 | kek_id_len:u8 | kek_id | dek_id:16 | wrapped_len:u16 | wrapped | nonce:12
          | ciphertext||tag

The header is not itself in the data AAD apart from `dek_id`, deliberately: that is what makes
a rewrap possible without touching the data ciphertext. Tampering with `kek_id` or the wrapped
DEK either fails the wrap's own authentication or yields a different key, which then fails the
data tag. Tampering with `dek_id` fails the wrap AAD and the data AAD both.

Nothing here logs. `repr` of every object holding key material is redacted.
"""

from __future__ import annotations

import os
import struct
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from libs.shared.crypto._encoding import (
    encode_fields,
    validate_context,
    validate_org_id,
    validate_purpose,
)
from libs.shared.crypto.errors import DecryptionError, MalformedEnvelopeError
from libs.shared.crypto.providers import KEY_BYTES, KeyProvider, WrappedKey

MAGIC = b"MYE"
FORMAT_V1 = 1
_NONCE_BYTES = 12
_TAG_BYTES = 16
_DATA_LABEL = b"mytheus.envelope.data.v1"

DEFAULT_DEK_MAX_AGE_SECONDS = 24 * 3600.0
DEFAULT_DEK_MAX_USES = 1_000_000
_UNWRAP_CACHE_MAX = 4096


@dataclass(frozen=True)
class EnvelopeHeader:
    version: int
    kek_id: str
    dek_id: uuid.UUID
    wrapped_dek: bytes = field(repr=False)


@dataclass(frozen=True)
class _Parsed:
    header: EnvelopeHeader
    nonce: bytes = field(repr=False)
    ciphertext: bytes = field(repr=False)


@dataclass
class _ActiveDek:
    dek_id: uuid.UUID
    key: bytes = field(repr=False)
    wrapped: WrappedKey = field(repr=False)
    created: float
    uses: int = 0


def _build(header: EnvelopeHeader, nonce: bytes, ciphertext: bytes) -> bytes:
    kid = header.kek_id.encode("utf-8")
    return b"".join(
        [
            MAGIC,
            bytes([header.version]),
            struct.pack(">B", len(kid)),
            kid,
            header.dek_id.bytes,
            struct.pack(">H", len(header.wrapped_dek)),
            header.wrapped_dek,
            nonce,
            ciphertext,
        ]
    )


def _parse(blob: bytes) -> _Parsed:
    if not isinstance(blob, bytes | bytearray | memoryview):
        raise MalformedEnvelopeError("envelope must be bytes")
    b = bytes(blob)
    try:
        if b[:3] != MAGIC:
            raise MalformedEnvelopeError("not a Mytheus envelope")
        version = b[3]
        if version != FORMAT_V1:
            raise MalformedEnvelopeError("unsupported envelope version")
        pos = 4
        (kid_len,) = struct.unpack_from(">B", b, pos)
        pos += 1
        kek_id = b[pos : pos + kid_len].decode("utf-8")
        if len(kek_id.encode("utf-8")) != kid_len or not kek_id:
            raise MalformedEnvelopeError("truncated envelope")
        pos += kid_len
        dek_raw = b[pos : pos + 16]
        if len(dek_raw) != 16:
            raise MalformedEnvelopeError("truncated envelope")
        dek_id = uuid.UUID(bytes=dek_raw)
        pos += 16
        (wlen,) = struct.unpack_from(">H", b, pos)
        pos += 2
        wrapped = b[pos : pos + wlen]
        if len(wrapped) != wlen:
            raise MalformedEnvelopeError("truncated envelope")
        pos += wlen
        nonce = b[pos : pos + _NONCE_BYTES]
        pos += _NONCE_BYTES
        ciphertext = b[pos:]
        if len(nonce) != _NONCE_BYTES or len(ciphertext) < _TAG_BYTES:
            raise MalformedEnvelopeError("truncated envelope")
    except (struct.error, IndexError, UnicodeDecodeError) as exc:
        raise MalformedEnvelopeError("truncated envelope") from exc
    return _Parsed(EnvelopeHeader(version, kek_id, dek_id, wrapped), nonce, ciphertext)


def parse_envelope(blob: bytes) -> EnvelopeHeader:
    """Read the header (KEK id, DEK id) without decrypting. For rotation jobs and indexes."""
    return _parse(blob).header


class EnvelopeCipher:
    """Thread-safe. One instance per process, built from the configured `KeyProvider`."""

    def __init__(
        self,
        provider: KeyProvider,
        *,
        dek_max_age_seconds: float = DEFAULT_DEK_MAX_AGE_SECONDS,
        dek_max_uses: int = DEFAULT_DEK_MAX_USES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if dek_max_uses < 1 or dek_max_age_seconds <= 0:
            raise ValueError("DEK rotation limits must be positive")
        self._provider = provider
        self._max_age = dek_max_age_seconds
        self._max_uses = dek_max_uses
        self._clock = clock
        self._lock = threading.Lock()
        self._active: dict[tuple[uuid.UUID, str], _ActiveDek] = {}
        self._unwrapped: OrderedDict[tuple[uuid.UUID, str, str, uuid.UUID, bytes], bytes] = (
            OrderedDict()
        )

    def __repr__(self) -> str:
        return f"EnvelopeCipher(provider={self._provider!r})"

    # --- keys -------------------------------------------------------------------------

    @staticmethod
    def _wrap_context(org_id: uuid.UUID, purpose: str, dek_id: uuid.UUID) -> dict[str, str]:
        return {"org_id": str(org_id), "purpose": purpose, "dek_id": str(dek_id)}

    def _active_dek(self, org_id: uuid.UUID, purpose: str) -> _ActiveDek:
        slot = (org_id, purpose)
        now = self._clock()
        with self._lock:
            cur = self._active.get(slot)
            if (
                cur is not None
                and cur.uses < self._max_uses
                and now - cur.created < self._max_age
                and cur.wrapped.kek_id == self._provider.active_kek_id
            ):
                cur.uses += 1
                return cur
        # Generate and wrap outside the lock: with a KMS provider the wrap is a network call.
        dek_id = uuid.uuid4()
        key = os.urandom(KEY_BYTES)
        wrapped = self._provider.wrap_dek(key, self._wrap_context(org_id, purpose, dek_id))
        fresh = _ActiveDek(dek_id=dek_id, key=key, wrapped=wrapped, created=now, uses=1)
        with self._lock:
            self._active[slot] = fresh
            self._remember(org_id, purpose, wrapped, dek_id, key)
        return fresh

    def _remember(
        self, org_id: uuid.UUID, purpose: str, wrapped: WrappedKey, dek_id: uuid.UUID, key: bytes
    ) -> None:
        # Caller holds the lock. The cache key includes the tenant, purpose and the wrapped
        # bytes themselves, so a cache hit can never hand tenant A's DEK to a blob that merely
        # claims tenant A's dek_id while being decrypted under tenant B's context.
        ck = (org_id, purpose, wrapped.kek_id, dek_id, wrapped.ciphertext)
        self._unwrapped[ck] = key
        self._unwrapped.move_to_end(ck)
        while len(self._unwrapped) > _UNWRAP_CACHE_MAX:
            self._unwrapped.popitem(last=False)

    def _dek_for(self, org_id: uuid.UUID, purpose: str, header: EnvelopeHeader) -> bytes:
        wrapped = WrappedKey(kek_id=header.kek_id, ciphertext=header.wrapped_dek)
        ck = (org_id, purpose, header.kek_id, header.dek_id, header.wrapped_dek)
        with self._lock:
            hit = self._unwrapped.get(ck)
            if hit is not None:
                self._unwrapped.move_to_end(ck)
                return hit
        key = self._provider.unwrap_dek(wrapped, self._wrap_context(org_id, purpose, header.dek_id))
        with self._lock:
            self._remember(org_id, purpose, wrapped, header.dek_id, key)
        return key

    @staticmethod
    def _data_aad(
        org_id: uuid.UUID, purpose: str, dek_id: uuid.UUID, context: Mapping[str, str]
    ) -> bytes:
        return encode_fields(
            _DATA_LABEL,
            {**context, "org_id": str(org_id), "purpose": purpose, "dek_id": str(dek_id)},
        )

    # --- public API -------------------------------------------------------------------

    def encrypt(
        self,
        org_id: uuid.UUID,
        purpose: str,
        plaintext: bytes,
        context: Mapping[str, str] | None = None,
    ) -> bytes:
        """Encrypt `plaintext` for one tenant and purpose. `context` is extra row binding
        (e.g. `{"connection_id": ...}`) that must be supplied identically to decrypt."""
        org_id = validate_org_id(org_id)
        purpose = validate_purpose(purpose)
        ctx = validate_context(context)
        if not isinstance(plaintext, bytes):
            raise TypeError("plaintext must be bytes")
        dek = self._active_dek(org_id, purpose)
        nonce = os.urandom(_NONCE_BYTES)
        ct = AESGCM(dek.key).encrypt(
            nonce, plaintext, self._data_aad(org_id, purpose, dek.dek_id, ctx)
        )
        header = EnvelopeHeader(FORMAT_V1, dek.wrapped.kek_id, dek.dek_id, dek.wrapped.ciphertext)
        return _build(header, nonce, ct)

    def decrypt(
        self,
        org_id: uuid.UUID,
        purpose: str,
        blob: bytes,
        context: Mapping[str, str] | None = None,
    ) -> bytes:
        """Decrypt, or raise `DecryptionError` / `KeyUnavailableError`. Never partial."""
        org_id = validate_org_id(org_id)
        purpose = validate_purpose(purpose)
        ctx = validate_context(context)
        parsed = _parse(blob)
        h = parsed.header
        key = self._dek_for(org_id, purpose, h)
        try:
            return AESGCM(key).decrypt(
                parsed.nonce, parsed.ciphertext, self._data_aad(org_id, purpose, h.dek_id, ctx)
            )
        except InvalidTag as exc:
            raise DecryptionError("envelope failed authentication") from exc

    def encrypt_str(
        self, org_id: uuid.UUID, purpose: str, value: str, context: Mapping[str, str] | None = None
    ) -> bytes:
        return self.encrypt(org_id, purpose, value.encode("utf-8"), context)

    def decrypt_str(
        self, org_id: uuid.UUID, purpose: str, blob: bytes, context: Mapping[str, str] | None = None
    ) -> str:
        return self.decrypt(org_id, purpose, blob, context).decode("utf-8")

    def needs_rewrap(self, blob: bytes) -> bool:
        """True when the blob's DEK is wrapped under a KEK other than the active one."""
        return _parse(blob).header.kek_id != self._provider.active_kek_id

    def rewrap(self, org_id: uuid.UUID, purpose: str, blob: bytes) -> bytes:
        """KEK rotation: re-wrap the DEK under the active KEK; data ciphertext unchanged.

        Authenticates the tenant binding on the way (the unwrap checks `org_id`/`purpose`),
        but does not verify the data tag - that needs the caller's row context. A rotation job
        that wants proof should `decrypt` the result once with the row context.
        """
        org_id = validate_org_id(org_id)
        purpose = validate_purpose(purpose)
        parsed = _parse(blob)
        h = parsed.header
        if h.kek_id == self._provider.active_kek_id:
            return bytes(blob)
        key = self._dek_for(org_id, purpose, h)
        wrapped = self._provider.wrap_dek(key, self._wrap_context(org_id, purpose, h.dek_id))
        new_header = EnvelopeHeader(FORMAT_V1, wrapped.kek_id, h.dek_id, wrapped.ciphertext)
        return _build(new_header, parsed.nonce, parsed.ciphertext)

    def reencrypt(
        self,
        org_id: uuid.UUID,
        purpose: str,
        blob: bytes,
        context: Mapping[str, str] | None = None,
    ) -> bytes:
        """Full rotation: decrypt and encrypt again under the current active DEK."""
        return self.encrypt(org_id, purpose, self.decrypt(org_id, purpose, blob, context), context)
