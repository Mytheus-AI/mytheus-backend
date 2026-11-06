"""Key-encryption-key custody, behind one interface.

A `KeyProvider` holds the key-encryption keys (KEKs) and never hands them out. It does
exactly three things:

- `wrap_dek`   encrypt a freshly generated data key (DEK) under the ACTIVE KEK, bound to a
               context (`org_id`, `purpose`, `dek_id`) so a wrapped key lifted from one
               tenant's row cannot be unwrapped as another tenant's.
- `unwrap_dek` the inverse, under whichever KEK the envelope names (active or retired).
- `derive_tenant_key` a deterministic per-tenant, per-purpose 32-byte key, for keyed blind
               indexes (`pii_tokens.value_hmac`). Deterministic is the point: the same value
               must index to the same bytes on every pod, forever, or dedupe breaks.

Implementations (selected by `key_provider_from_env`, `MYTHEUS_KEY_CUSTODY`):

- `FileKeyProvider` - PRODUCTION on the VPS (ADR-0026). KEK from a 0400/0600 file owned by
  the service uid; never from an environment variable value.
- `LocalKeyProvider` - dev and test ONLY. KEK material from `MYTHEUS_KEK_BASE64`. Refuses to
  construct outside a development environment, and refuses to construct with no key in ANY
  environment. Neither local provider ever generates a key: a generated KEK encrypts data that
  is unreadable after the next restart, a data-loss bug disguised as a convenience.
- `KmsKeyProvider` - the documented upgrade path, NOT IMPLEMENTED.
- `RotatingKeyProvider` - routes unwrap by KEK id across custodians, so a file KEK can be
  rewrapped under a new file KEK or under KMS with no data migration.

Environment (all base64 values are 32 raw bytes, standard alphabet):

    ENVIRONMENT                       development | dev | local | test  (anything else, or
                                      unset, is treated as production and the local
                                      provider refuses to start)
    MYTHEUS_KEY_CUSTODY               local-file | kms | local-env (see key_provider_from_env)
    MYTHEUS_KEK_ID                    id of the active KEK ("local-1" / "file-1" defaults)
    MYTHEUS_KEK_BASE64                the active KEK (local-env, dev only)
    MYTHEUS_KEK_RETIRED               optional "id=base64,id=base64" - decrypt-only KEKs kept
                                      during a rotation until every envelope is rewrapped
    MYTHEUS_BLIND_INDEX_KEY_BASE64    root for `derive_tenant_key`. Separate from the KEK on
                                      purpose: KEK rotation must not change any blind index.
    MYTHEUS_KEK_FILE etc.             (local-file) see FileKeyProvider
    MYTHEUS_KMS_KEY_URI               (kms only) e.g. aws-kms://arn:... or gcp-kms://projects/...

No key, wrapped key, or plaintext is ever logged, put in an exception message, or rendered by
`repr`. This module imports no logger at all.
"""

from __future__ import annotations

import base64
import binascii
import os
import stat
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from libs.shared.crypto._encoding import encode_fields, validate_org_id, validate_purpose
from libs.shared.crypto.errors import (
    DecryptionError,
    KeyConfigurationError,
    KeyUnavailableError,
)

KEY_BYTES = 32  # AES-256 and HMAC-SHA256 key size
_NONCE_BYTES = 12

DEV_ENVIRONMENTS = frozenset({"development", "dev", "local", "test"})

_WRAP_LABEL = b"mytheus.kek.wrap.v1"
_DERIVE_SALT = b"mytheus.blind-index.v1"
_DERIVE_LABEL = b"mytheus.blind-index.tenant-key.v1"


@dataclass(frozen=True)
class WrappedKey:
    """A DEK encrypted under a KEK. Safe to store; useless without the provider."""

    kek_id: str
    ciphertext: bytes = field(repr=False)


class KeyProvider(ABC):
    """The custody seam. See the module docstring for the contract of each method."""

    @property
    @abstractmethod
    def active_kek_id(self) -> str:
        """The KEK id new DEKs are wrapped under."""

    @property
    def kek_ids(self) -> frozenset[str]:
        """Every KEK id this provider can unwrap. Used to route by id across custodians."""
        return frozenset({self.active_kek_id})

    @abstractmethod
    def wrap_dek(self, dek: bytes, context: Mapping[str, str]) -> WrappedKey:
        """Encrypt `dek` under the active KEK, authenticating `context`."""

    @abstractmethod
    def unwrap_dek(self, wrapped: WrappedKey, context: Mapping[str, str]) -> bytes:
        """Decrypt a wrapped DEK. Raises `KeyUnavailableError` for an unknown KEK id and
        `DecryptionError` if the ciphertext or `context` does not authenticate."""

    @abstractmethod
    def derive_tenant_key(self, org_id: uuid.UUID, purpose: str) -> bytes:
        """A deterministic 32-byte key unique to (`org_id`, `purpose`)."""


def _decode_key(name: str, value: str) -> bytes:
    try:
        raw = base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise KeyConfigurationError(f"{name} is not valid base64") from exc
    if len(raw) != KEY_BYTES:
        raise KeyConfigurationError(f"{name} must decode to exactly {KEY_BYTES} bytes")
    return raw


def _is_dev(environment: str | None) -> bool:
    return (environment or "").strip().lower() in DEV_ENVIRONMENTS


class _InMemoryKeyProvider(KeyProvider):
    """KEKs and the blind-index root held in process memory. Where they came from is the
    subclass's business; the cryptography is identical for every local custodian, which is what
    lets a dev-env key, a file key and (via `RotatingKeyProvider`) a KMS key interoperate."""

    _label = "InMemoryKeyProvider"

    def __init__(
        self, *, active_kek_id: str, keks: Mapping[str, bytes], blind_index_root: bytes
    ) -> None:
        if active_kek_id not in keks:
            raise KeyConfigurationError("the active KEK id has no key material")
        for kid, key in keks.items():
            if not kid or len(kid.encode("utf-8")) > 255:
                raise KeyConfigurationError("KEK ids must be 1-255 bytes")
            if len(key) != KEY_BYTES:
                raise KeyConfigurationError(f"KEK {kid!r} must be {KEY_BYTES} bytes")
        if len(blind_index_root) != KEY_BYTES:
            raise KeyConfigurationError(f"blind-index root must be {KEY_BYTES} bytes")
        self._active = active_kek_id
        self._keks = {kid: AESGCM(key) for kid, key in keks.items()}
        self._index_root = bytes(blind_index_root)

    def __repr__(self) -> str:
        return f"{self._label}(active={self._active!r}, kek_ids={sorted(self._keks)!r})"

    @property
    def active_kek_id(self) -> str:
        return self._active

    @property
    def kek_ids(self) -> frozenset[str]:
        return frozenset(self._keks)

    def _aad(self, kek_id: str, context: Mapping[str, str]) -> bytes:
        return encode_fields(_WRAP_LABEL, {**context, "kek_id": kek_id})

    def wrap_dek(self, dek: bytes, context: Mapping[str, str]) -> WrappedKey:
        if len(dek) != KEY_BYTES:
            raise ValueError(f"DEK must be {KEY_BYTES} bytes")
        nonce = os.urandom(_NONCE_BYTES)
        ct = self._keks[self._active].encrypt(nonce, dek, self._aad(self._active, context))
        return WrappedKey(kek_id=self._active, ciphertext=nonce + ct)

    def unwrap_dek(self, wrapped: WrappedKey, context: Mapping[str, str]) -> bytes:
        aead = self._keks.get(wrapped.kek_id)
        if aead is None:
            raise KeyUnavailableError(f"no KEK with id {wrapped.kek_id!r}")
        blob = wrapped.ciphertext
        if len(blob) != _NONCE_BYTES + KEY_BYTES + 16:
            raise DecryptionError("wrapped DEK has the wrong length")
        try:
            return aead.decrypt(
                blob[:_NONCE_BYTES], blob[_NONCE_BYTES:], self._aad(wrapped.kek_id, context)
            )
        except InvalidTag as exc:
            raise DecryptionError("wrapped DEK failed authentication") from exc

    def derive_tenant_key(self, org_id: uuid.UUID, purpose: str) -> bytes:
        info = encode_fields(
            _DERIVE_LABEL,
            {"org_id": str(validate_org_id(org_id)), "purpose": validate_purpose(purpose)},
        )
        return HKDF(algorithm=SHA256(), length=KEY_BYTES, salt=_DERIVE_SALT, info=info).derive(
            self._index_root
        )


def _parse_pairs(raw: str, name: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        kid, sep, value = entry.partition("=")
        kid, value = kid.strip(), value.strip()
        if not sep or not kid or not value:
            raise KeyConfigurationError(f"{name} entries must be id=value")
        out.append((kid, value))
    return out


class LocalKeyProvider(_InMemoryKeyProvider):
    """KEKs from environment-variable VALUES. Development and test only."""

    _label = "LocalKeyProvider"

    def __init__(
        self,
        *,
        active_kek_id: str,
        keks: Mapping[str, bytes],
        blind_index_root: bytes,
        environment: str | None,
    ) -> None:
        if not _is_dev(environment):
            raise KeyConfigurationError(
                "LocalKeyProvider is for development/test only; ENVIRONMENT="
                f"{(environment or '<unset>')!r} is treated as production. Production uses "
                "MYTHEUS_KEY_CUSTODY=local-file (ADR-0026)."
            )
        super().__init__(active_kek_id=active_kek_id, keks=keks, blind_index_root=blind_index_root)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LocalKeyProvider:
        env = os.environ if env is None else env
        environment = env.get("ENVIRONMENT")
        if not _is_dev(environment):
            # Checked before reading any key so a production misconfiguration fails on the
            # provider choice, not on whichever variable happens to be missing.
            raise KeyConfigurationError(
                "LocalKeyProvider refused: ENVIRONMENT is not a development environment"
            )
        kek_b64 = env.get("MYTHEUS_KEK_BASE64", "")
        if not kek_b64.strip():
            raise KeyConfigurationError(
                "MYTHEUS_KEK_BASE64 is unset. Refusing to start rather than generate a key: "
                "data encrypted under a generated key is lost on restart."
            )
        index_b64 = env.get("MYTHEUS_BLIND_INDEX_KEY_BASE64", "")
        if not index_b64.strip():
            raise KeyConfigurationError("MYTHEUS_BLIND_INDEX_KEY_BASE64 is unset")
        active = env.get("MYTHEUS_KEK_ID", "").strip() or "local-1"
        keks = {active: _decode_key("MYTHEUS_KEK_BASE64", kek_b64)}
        for kid, value in _parse_pairs(env.get("MYTHEUS_KEK_RETIRED", ""), "MYTHEUS_KEK_RETIRED"):
            if kid in keks:
                raise KeyConfigurationError(f"KEK id {kid!r} is listed twice")
            keks[kid] = _decode_key(f"MYTHEUS_KEK_RETIRED[{kid}]", value)
        return cls(
            active_kek_id=active,
            keks=keks,
            blind_index_root=_decode_key("MYTHEUS_BLIND_INDEX_KEY_BASE64", index_b64),
            environment=environment,
        )


DEFAULT_KEK_FILE = "/run/secrets/mytheus_kek"
DEFAULT_BLIND_INDEX_KEY_FILE = "/run/secrets/mytheus_blind_index_key"
_ALLOWED_FILE_MODES = frozenset({0o400, 0o600})
_ENV_KEY_VARS = ("MYTHEUS_KEK_BASE64", "MYTHEUS_BLIND_INDEX_KEY_BASE64", "MYTHEUS_KEK_RETIRED")


def read_key_file(path: str, *, name: str) -> bytes:
    """Read a base64 32-byte key file, refusing anything but a private, regular file.

    Refused: missing, not a regular file, group- or world-accessible (only 0400 or 0600), not
    owned by the effective uid, empty, not base64, or not exactly 32 bytes. The messages name
    the path and the rule broken, never the content.
    """
    try:
        st = os.stat(path)
    except FileNotFoundError as exc:
        raise KeyConfigurationError(f"{name}: key file {path!r} does not exist") from exc
    except OSError as exc:
        raise KeyConfigurationError(f"{name}: key file {path!r} cannot be stat'ed") from exc
    if not stat.S_ISREG(st.st_mode):
        raise KeyConfigurationError(f"{name}: {path!r} is not a regular file")
    mode = stat.S_IMODE(st.st_mode)
    if mode not in _ALLOWED_FILE_MODES:
        raise KeyConfigurationError(
            f"{name}: {path!r} has mode {mode:04o}; it must be 0400 or 0600 (no group/world)"
        )
    if st.st_uid != os.geteuid():
        raise KeyConfigurationError(
            f"{name}: {path!r} is owned by uid {st.st_uid}, not this service's uid "
            f"{os.geteuid()}"
        )
    try:
        with open(path, encoding="ascii") as fh:
            content = fh.read()
    except (OSError, UnicodeDecodeError) as exc:
        raise KeyConfigurationError(f"{name}: {path!r} cannot be read as ASCII") from exc
    if not content.strip():
        raise KeyConfigurationError(f"{name}: {path!r} is empty")
    return _decode_key(f"{name} ({path})", content)


class FileKeyProvider(_InMemoryKeyProvider):
    """Production custody on the VPS (ADR-0026): the KEK lives in a private file.

    Accepted with its residual risk stated plainly in ADR-0026: the KEK shares a host with
    Postgres, so a host-level compromise exposes both. Only the file is accepted; a key VALUE
    in the environment is refused outright, because environment variables leak into `docker
    inspect`, `/proc/*/environ`, crash reports and child processes in a way a 0400 file does
    not.

    Environment (paths and ids only, never key material):

        MYTHEUS_KEY_CUSTODY                 local-file
        MYTHEUS_KEK_FILE                    default /run/secrets/mytheus_kek
        MYTHEUS_KEK_ID                      id of the active file KEK (default "file-1"). Must
                                            be unique across custodians for all time: it is
                                            stamped into every envelope.
        MYTHEUS_KEK_RETIRED_FILES           optional "id=/path,id=/path" decrypt-only KEKs kept
                                            during a rotation until the rewrap job is done
        MYTHEUS_BLIND_INDEX_KEY_FILE        default /run/secrets/mytheus_blind_index_key
    """

    _label = "FileKeyProvider"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> FileKeyProvider:
        env = os.environ if env is None else env
        present = [v for v in _ENV_KEY_VARS if env.get(v, "").strip()]
        if present:
            raise KeyConfigurationError(
                "MYTHEUS_KEY_CUSTODY=local-file refuses key material in environment variables; "
                f"unset {', '.join(present)} and use the key files"
            )
        active = env.get("MYTHEUS_KEK_ID", "").strip() or "file-1"
        kek_path = env.get("MYTHEUS_KEK_FILE", "").strip() or DEFAULT_KEK_FILE
        keks = {active: read_key_file(kek_path, name="MYTHEUS_KEK_FILE")}
        for kid, path in _parse_pairs(
            env.get("MYTHEUS_KEK_RETIRED_FILES", ""), "MYTHEUS_KEK_RETIRED_FILES"
        ):
            if kid in keks:
                raise KeyConfigurationError(f"KEK id {kid!r} is listed twice")
            keks[kid] = read_key_file(path, name=f"MYTHEUS_KEK_RETIRED_FILES[{kid}]")
        index_path = (
            env.get("MYTHEUS_BLIND_INDEX_KEY_FILE", "").strip() or DEFAULT_BLIND_INDEX_KEY_FILE
        )
        return cls(
            active_kek_id=active,
            keks=keks,
            blind_index_root=read_key_file(index_path, name="MYTHEUS_BLIND_INDEX_KEY_FILE"),
        )


class RotatingKeyProvider(KeyProvider):
    """One active custodian plus decrypt-only predecessors, routed by KEK id.

    This is the custodian-swap path with no data migration: make KMS (or a new file KEK)
    `active`, keep the old provider in `decrypt_only`, run `EnvelopeCipher.rewrap` over every
    row, then drop the old provider. The blind-index key stays with `index_provider` (default:
    the active one) because a different custodian cannot reproduce another's derivation and
    every `value_hmac` would change; moving it is a separate re-index job.
    """

    def __init__(
        self,
        active: KeyProvider,
        decrypt_only: Sequence[KeyProvider] = (),
        *,
        index_provider: KeyProvider | None = None,
    ) -> None:
        self._active = active
        self._route: dict[str, KeyProvider] = {}
        for p in (active, *decrypt_only):
            for kid in p.kek_ids:
                if kid in self._route:
                    raise KeyConfigurationError(f"KEK id {kid!r} is held by two custodians")
                self._route[kid] = p
        self._index = index_provider or active

    def __repr__(self) -> str:
        return f"RotatingKeyProvider(active={self._active!r}, kek_ids={sorted(self._route)!r})"

    @property
    def active_kek_id(self) -> str:
        return self._active.active_kek_id

    @property
    def kek_ids(self) -> frozenset[str]:
        return frozenset(self._route)

    def wrap_dek(self, dek: bytes, context: Mapping[str, str]) -> WrappedKey:
        return self._active.wrap_dek(dek, context)

    def unwrap_dek(self, wrapped: WrappedKey, context: Mapping[str, str]) -> bytes:
        provider = self._route.get(wrapped.kek_id)
        if provider is None:
            raise KeyUnavailableError(f"no KEK with id {wrapped.kek_id!r}")
        return provider.unwrap_dek(wrapped, context)

    def derive_tenant_key(self, org_id: uuid.UUID, purpose: str) -> bytes:
        return self._index.derive_tenant_key(org_id, purpose)


class KmsKeyProvider(KeyProvider):
    """The KMS upgrade path. NOT IMPLEMENTED.

    ADR-0026 accepted `FileKeyProvider` for the VPS and names KMS as the explicit upgrade
    path (trigger: before the first enterprise tenant or real PII at scale). This class fixes
    the interface so that upgrade is a fill-in, and `RotatingKeyProvider` lets it happen with
    no data migration: KMS becomes active, the file KEK stays decrypt-only, a rewrap job moves
    every envelope across. The intended mapping, for whoever implements it:

    `wrap_dek(dek, context)`
        AWS: `kms.Encrypt(KeyId=<key>, Plaintext=dek, EncryptionContext=context)`.
        GCP: `encrypt(name=<key>, plaintext=dek, additional_authenticated_data=
             encode_fields(label, context))`.
        Return `WrappedKey(kek_id=<key version / ARN actually used>, ciphertext=<blob>)`.
        (`kms.GenerateDataKey` is equivalent and saves the local `os.urandom`.)

    `unwrap_dek(wrapped, context)`
        AWS `Decrypt` with the SAME EncryptionContext; GCP `decrypt` with the same AAD. Map the
        provider's auth/integrity failure to `DecryptionError`, and an unknown or disabled key
        to `KeyUnavailableError`. Never fall back to another key.

    `derive_tenant_key(org_id, purpose)`
        AWS: an HMAC KMS key, `GenerateMac(KeyId=<hmac key>, MacAlgorithm=HMAC_SHA_256,
        Message=encode_fields(label, {org_id, purpose}))`. GCP: `mac_sign` on an HMAC key.
        The output IS the tenant key; the caller caches it. Must be deterministic.

    Network calls belong off the event loop (`asyncio.to_thread`) and behind the envelope
    layer's DEK cache, which already amortizes them to one wrap per (tenant, purpose) per
    rotation window and one unwrap per DEK per process.
    """

    def __init__(self, key_uri: str) -> None:
        if not key_uri:
            raise KeyConfigurationError("MYTHEUS_KMS_KEY_URI is unset")
        self._key_uri = key_uri

    def __repr__(self) -> str:
        return f"KmsKeyProvider(key_uri={self._key_uri!r})"

    def _not_implemented(self) -> NotImplementedError:
        return NotImplementedError(
            "KmsKeyProvider is a documented upgrade boundary only (ADR-0026); no KMS call is "
            "implemented."
        )

    @property
    def active_kek_id(self) -> str:
        raise self._not_implemented()

    def wrap_dek(self, dek: bytes, context: Mapping[str, str]) -> WrappedKey:
        raise self._not_implemented()

    def unwrap_dek(self, wrapped: WrappedKey, context: Mapping[str, str]) -> bytes:
        raise self._not_implemented()

    def derive_tenant_key(self, org_id: uuid.UUID, purpose: str) -> bytes:
        raise self._not_implemented()


def key_provider_from_env(env: Mapping[str, str] | None = None) -> KeyProvider:
    """Build the configured custodian, or raise `KeyConfigurationError`. Call at startup.

    `MYTHEUS_KEY_CUSTODY`:
        local-file   `FileKeyProvider`; the production choice on the VPS (ADR-0026).
        kms          `KmsKeyProvider` (not implemented; the documented upgrade path).
        local-env    `LocalKeyProvider`; development/test only.
        unset        `local-env` in a development ENVIRONMENT; anywhere else, refuse. Production
                     never picks a custodian implicitly.
    """
    env = os.environ if env is None else env
    custody = env.get("MYTHEUS_KEY_CUSTODY", "").strip().lower()
    if custody == "local-file":
        return FileKeyProvider.from_env(env)
    if custody == "kms":
        return KmsKeyProvider(env.get("MYTHEUS_KMS_KEY_URI", "").strip())
    if custody == "local-env" or (not custody and _is_dev(env.get("ENVIRONMENT"))):
        return LocalKeyProvider.from_env(env)
    if not custody:
        raise KeyConfigurationError(
            "MYTHEUS_KEY_CUSTODY is unset outside a development ENVIRONMENT; set it explicitly "
            "(production on the VPS: local-file, ADR-0026)"
        )
    raise KeyConfigurationError(f"unknown MYTHEUS_KEY_CUSTODY {custody!r}")
