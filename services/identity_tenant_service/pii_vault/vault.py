"""The vault: mint (tokenize) and resolve (detokenize) against `pii_tokens`.

ADR-0020 D1-D3. identity-tenant-service is the only writer; the three redaction boundaries
call `/pii-tokens/mint` over the mesh and never touch the table.

Mint is deterministic per tenant. `value_hmac` = HMAC(tenant key, entity_type || NUL ||
normalized value), and the token is a public function of `value_hmac`, so the same value in
the same tenant mints the same token on every pod without coordination, while the same value
in another tenant (different derived key) mints an unrelated one. The row's UNIQUE
`(org_id, entity_type, value_hmac)` makes concurrent mints converge on one row.

Resolve is a disclosure of PII and is audited (`libs/shared/audit.py`, fail-closed, in the
caller's transaction): if the audit row cannot be written, no value is returned.

Every method takes the caller's `AsyncSession`, which MUST already have `app.current_org` set
for `org_id`. That is re-checked here rather than trusted, because a vault read under the
wrong tenant setting is the one bug this module cannot afford.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from hashlib import sha256

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.crypto import BlindIndex, EnvelopeCipher, parse_envelope
from services.identity_tenant_service.pii_vault.normalize import EntityType, normalize

PURPOSE_VALUE = "pii_tokens.value"
PURPOSE_INDEX = "pii_tokens.value_hmac"
TOKEN_HEX_LEN = 16  # 64 bits: per-tenant birthday bound ~4e9 distinct values
_TOKEN_LABEL = b"mytheus.pii.token.v1"

ACTION_PII_TOKEN_RESOLVED = "pii.token_resolved"
ACTION_PII_TOKEN_RESOLVE_MISS = "pii.token_resolve_miss"

_SELECT_BY_HMAC = text(
    "SELECT token FROM pii_tokens " "WHERE org_id = :org AND entity_type = :et AND value_hmac = :h"
)
_INSERT = text(
    "INSERT INTO pii_tokens (org_id, token, entity_type, value_ciphertext, value_hmac, dek_id) "
    "VALUES (:org, :token, :et, :ct, :h, :dek) "
    "ON CONFLICT (org_id, entity_type, value_hmac) DO NOTHING "
    "RETURNING token"
)
_SELECT_BY_TOKENS = text(
    "SELECT token, entity_type, value_ciphertext, value_hmac FROM pii_tokens "
    "WHERE org_id = :org AND token = ANY(:tokens)"
)
_CURRENT_ORG = text("SELECT NULLIF(current_setting('app.current_org', true), '')")


class VaultError(Exception):
    """A vault invariant failed. Never carries a raw value."""


def format_placeholder(entity_type: EntityType, token: str) -> str:
    """The typed, reversible placeholder of Compliance section 5.2, e.g. `‹PERSON:7f3a...›`."""
    return f"‹{entity_type.value}:{token}›"


@dataclass(frozen=True)
class MintedToken:
    entity_type: EntityType
    token: str

    @property
    def placeholder(self) -> str:
        return format_placeholder(self.entity_type, self.token)


@dataclass(frozen=True)
class ResolvedToken:
    token: str
    entity_type: EntityType
    value: str = field(repr=False)


def token_for(value_hmac: bytes) -> str:
    """Public function of the keyed index; reveals nothing the index does not."""
    return sha256(_TOKEN_LABEL + value_hmac).hexdigest()[:TOKEN_HEX_LEN]


class PiiVault:
    def __init__(self, cipher: EnvelopeCipher, index: BlindIndex) -> None:
        self._cipher = cipher
        self._index = index

    def __repr__(self) -> str:
        return "PiiVault()"

    @staticmethod
    async def _assert_rls(session: AsyncSession, org_id: uuid.UUID) -> None:
        current = (await session.execute(_CURRENT_ORG)).scalar_one_or_none()
        if current is None or str(current) != str(org_id):
            raise VaultError("session app.current_org does not match org_id")

    def value_hmac(self, org_id: uuid.UUID, entity_type: EntityType, value: str) -> bytes:
        message = entity_type.value.encode() + b"\x00" + normalize(entity_type, value).encode()
        return self._index.compute(org_id, PURPOSE_INDEX, message)

    @staticmethod
    def _row_context(entity_type: EntityType, value_hmac: bytes) -> dict[str, str]:
        # Binds the ciphertext to its row: moved onto another token's row it fails to decrypt.
        return {"entity_type": entity_type.value, "value_hmac": value_hmac.hex()}

    async def tokenize(
        self, session: AsyncSession, org_id: uuid.UUID, value: str, entity_type: EntityType
    ) -> MintedToken:
        return (await self.tokenize_many(session, org_id, [(entity_type, value)]))[0]

    async def tokenize_many(
        self,
        session: AsyncSession,
        org_id: uuid.UUID,
        items: Sequence[tuple[EntityType, str]],
    ) -> list[MintedToken]:
        await self._assert_rls(session, org_id)
        out: list[MintedToken] = []
        for entity_type, value in items:
            h = self.value_hmac(org_id, entity_type, value)
            params = {"org": org_id, "et": entity_type.value, "h": h}
            existing = (await session.execute(_SELECT_BY_HMAC, params)).scalar_one_or_none()
            if existing is None:
                token = token_for(h)
                blob = self._cipher.encrypt_str(
                    org_id, PURPOSE_VALUE, value, self._row_context(entity_type, h)
                )
                inserted = (
                    await session.execute(
                        _INSERT,
                        {
                            **params,
                            "token": token,
                            "ct": blob,
                            "dek": parse_envelope(blob).dek_id,
                        },
                    )
                ).scalar_one_or_none()
                # None: a concurrent mint won the race on the UNIQUE; read its row.
                existing = (
                    inserted
                    or (await session.execute(_SELECT_BY_HMAC, params)).scalar_one_or_none()
                )
                if existing is None:
                    raise VaultError("mint neither inserted nor found a row")
            out.append(MintedToken(entity_type, str(existing)))
        return out

    async def detokenize(
        self,
        session: AsyncSession,
        org_id: uuid.UUID,
        tokens: Sequence[str],
        *,
        actor_id: uuid.UUID | None,
        actor_type: str = "user",
        ip: str | None = None,
        reason: str | None = None,
    ) -> tuple[list[ResolvedToken], list[str]]:
        """Resolve tokens to raw values, auditing each one. Returns (resolved, unresolved).

        Unknown tokens - including another tenant's, which RLS makes invisible - are reported
        as unresolved, indistinguishably. Each request writes one audit row per token (hit or
        miss) before any value is returned; the audit rows carry the token and type, never the
        value.
        """
        await self._assert_rls(session, org_id)
        wanted = list(dict.fromkeys(tokens))
        rows = (
            (await session.execute(_SELECT_BY_TOKENS, {"org": org_id, "tokens": wanted}))
            .mappings()
            .all()
        )
        by_token = {r["token"]: r for r in rows}
        resolved: list[ResolvedToken] = []
        unresolved: list[str] = []
        for tok in wanted:
            row = by_token.get(tok)
            if row is None:
                unresolved.append(tok)
                await audit(
                    session,
                    org_id=org_id,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    action=ACTION_PII_TOKEN_RESOLVE_MISS,
                    target_type="pii_token",
                    target_id=tok,
                    metadata={"reason": reason},
                    ip=ip,
                )
                continue
            et = EntityType(row["entity_type"])
            h = bytes(row["value_hmac"])
            value = self._cipher.decrypt_str(
                org_id, PURPOSE_VALUE, bytes(row["value_ciphertext"]), self._row_context(et, h)
            )
            await audit(
                session,
                org_id=org_id,
                actor_id=actor_id,
                actor_type=actor_type,
                action=ACTION_PII_TOKEN_RESOLVED,
                target_type="pii_token",
                target_id=tok,
                metadata={"entity_type": et.value, "reason": reason},
                ip=ip,
            )
            resolved.append(ResolvedToken(tok, et, value))
        return resolved, unresolved
