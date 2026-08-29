"""The PII token vault: the only code in the program that writes `pii_tokens`.

ADR-0020 D1/D2 put this here, and the reasoning is worth keeping in front of anyone editing it.
`identity-tenant-service` already owns `tenant_settings` (whose `pii_redaction_enabled` decides
whether redaction runs at all) and `data_residency` (whose `region` decides where a raw value
may lawfully live). Reversal is defined as happening in-region, in-tenant, for authorized
members, which is exactly the state this service already guards. The three redaction boundaries
- `transcription-service`, `ingestion-service`, `agent-runtime-service`/`rag-service` - call
the routes over this module and never touch the table, which is what lets one shared token
space per tenant exist without becoming three writers of one store.

MINTING IS IDEMPOTENT, AND THAT IS THE FEATURE

`mint` is not "insert a token". It is "return the token this tenant already uses for this
value, creating it only if there isn't one". `UNIQUE (org_id, entity_type, value_hmac)` is what
guarantees it at the database level, and it is the entire coherence property: the same customer
named in a meeting transcript and in an uploaded document resolves to the same placeholder, so
the knowledge graph links them instead of producing two anonymous strangers.

Which means a redaction boundary can retry a batch freely. That matters more than it looks:
transcript finals arrive at-least-once, so a mint WILL be replayed, and a non-idempotent vault
would grow a second token for a value on every redelivery.

PLAINTEXT KEYS ARE NEVER CACHED

Unwrapped key material lives in a local variable for the duration of one call and is not
memoized anywhere. That costs an unwrap per request, which is free under `LocalKeyProvider` and
a real network call under a KMS - and it is still the right trade, because a process-wide cache
of plaintext DEKs turns any memory disclosure in this service into a disclosure of every
tenant's key at once. If unwrap latency ever becomes the bottleneck, the fix is a bounded
short-TTL cache with an explicit decision recorded, not an `lru_cache` added in passing.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.crypto import (
    KeyProvider,
    Purpose,
    blind_index,
    decrypt_value,
    derive_token,
    encrypt_value,
    new_key_material,
)

# How many times a token derivation is retried when `UNIQUE (org_id, token)` rejects it. A
# collision needs two different values in one tenant whose 64-bit derived tokens match, so the
# first retry is already improbable to the point of being untestable in production; the loop
# exists because the alternative to retrying is a wrong answer, not because it is expected to
# run. See `crypto.derive_token`.
_MAX_TOKEN_ATTEMPTS = 4


@dataclass(frozen=True)
class MintRequest:
    entity_type: str
    value: str


@dataclass(frozen=True)
class MintedToken:
    entity_type: str
    token: str


class VaultKeysUnavailable(RuntimeError):
    """The tenant's keys could not be established. Callers map this to a 503, never a 500.

    It is a dependency failure, not a bug: the root key may be unconfigured or a KMS may be
    unreachable. The redaction boundaries treat it exactly as they treat an unreachable vault
    and fall back to the irreversible placeholders, so the degraded path is the status quo.
    """


async def _key_row(
    session: AsyncSession, org_id: uuid.UUID, purpose: Purpose
) -> tuple[uuid.UUID, bytes] | None:
    """The live key row for a purpose, or None.

    `status = 'active'` is applied only to `enc`. An `index` key is never retired (ADR-0024 D4)
    and filtering on status here would silently mint a second one if a row were ever marked
    retired by hand - the partial unique index would then reject the insert, which is the
    correct outcome but a confusing one to debug. Asking the right question is cheaper.
    """
    clause = "AND status = 'active'" if purpose is Purpose.ENC else ""
    row = (
        await session.execute(
            text(
                "SELECT id, wrapped_key FROM tenant_keys "
                f"WHERE org_id = :org_id AND purpose = :purpose {clause}"
            ),
            {"org_id": org_id, "purpose": purpose.value},
        )
    ).first()
    return (row[0], bytes(row[1])) if row else None


async def ensure_key(
    session: AsyncSession, org_id: uuid.UUID, purpose: Purpose, provider: KeyProvider
) -> tuple[uuid.UUID, bytes]:
    """Get-or-create the tenant's key for `purpose`, returning `(dek_id, plaintext_key)`.

    Created lazily on first use rather than at org provisioning. Provisioning predates this
    table by several bands and every existing organization would need a backfill; more to the
    point, a tenant that never turns redaction on never needs a key, and a key that exists
    without ever being used is a liability with no benefit.

    The `ON CONFLICT DO NOTHING` + re-read is not defensive padding. Two concurrent finals for
    the same tenant can both find no key and both try to create one; the partial unique index
    lets exactly one win, and the loser must adopt the winner's key rather than fail, because
    both are encrypting values that have to end up under the same key to stay coherent.
    """
    existing = await _key_row(session, org_id, purpose)
    if existing is not None:
        return existing[0], provider.unwrap(org_id, existing[1])

    material = new_key_material()
    await session.execute(
        text(
            "INSERT INTO tenant_keys (org_id, purpose, wrapped_key, wrap_key_id, status) "
            "VALUES (:org_id, :purpose, :wrapped, :wrap_key_id, 'active') "
            "ON CONFLICT DO NOTHING"
        ),
        {
            "org_id": org_id,
            "purpose": purpose.value,
            "wrapped": provider.wrap(org_id, material),
            "wrap_key_id": provider.key_id,
        },
    )
    row = await _key_row(session, org_id, purpose)
    if row is None:
        # Unreachable through the partial unique indexes: either this INSERT landed or another
        # transaction's did. Raising beats returning `material`, which would encrypt values
        # under a key no row records - unreadable forever.
        raise VaultKeysUnavailable(f"could not establish a {purpose.value} key for {org_id}")
    return row[0], provider.unwrap(org_id, row[1])


async def mint(
    session: AsyncSession,
    org_id: uuid.UUID,
    items: list[MintRequest],
    provider: KeyProvider,
) -> list[MintedToken]:
    """Return one token per request item, in the same order, creating rows only as needed.

    Order is part of the contract: the caller is splicing these back into the exact character
    spans it detected, so a reordered response silently swaps two people's placeholders.
    """
    if not items:
        return []

    enc_id, enc_key = await ensure_key(session, org_id, Purpose.ENC, provider)
    _, index_key = await ensure_key(session, org_id, Purpose.INDEX, provider)

    minted: list[MintedToken] = []
    for item in items:
        digest = blind_index(index_key, org_id, item.entity_type, item.value)
        token = await _mint_one(session, org_id, item, digest, enc_id, enc_key, index_key)
        minted.append(MintedToken(entity_type=item.entity_type, token=token))
    return minted


async def _lookup(
    session: AsyncSession, org_id: uuid.UUID, entity_type: str, digest: bytes
) -> str | None:
    row = (
        await session.execute(
            text(
                "SELECT token FROM pii_tokens "
                "WHERE org_id = :org_id AND entity_type = :entity_type "
                "AND value_hmac = :digest"
            ),
            {"org_id": org_id, "entity_type": entity_type, "digest": digest},
        )
    ).first()
    return str(row[0]) if row else None


async def _mint_one(
    session: AsyncSession,
    org_id: uuid.UUID,
    item: MintRequest,
    digest: bytes,
    enc_id: uuid.UUID,
    enc_key: bytes,
    index_key: bytes,
) -> str:
    existing = await _lookup(session, org_id, item.entity_type, digest)
    if existing is not None:
        return existing

    ciphertext = encrypt_value(enc_key, org_id, item.value)
    for attempt in range(_MAX_TOKEN_ATTEMPTS):
        token = derive_token(index_key, org_id, digest, attempt)
        try:
            # SAVEPOINT, so that a rejected insert does not poison the caller's transaction.
            # This runs inside the request transaction that also carries the audit row, and in
            # PostgreSQL any failed statement aborts the whole transaction unless it is wrapped
            # - without this, one token collision would roll back an entire batch.
            async with session.begin_nested():
                inserted = (
                    await session.execute(
                        text(
                            "INSERT INTO pii_tokens "
                            "(org_id, token, entity_type, value_ciphertext, value_hmac, dek_id) "
                            "VALUES (:org_id, :token, :entity_type, :ct, :digest, :dek_id) "
                            "ON CONFLICT ON CONSTRAINT "
                            "uq_pii_tokens_org_id_entity_type_value_hmac DO NOTHING "
                            "RETURNING token"
                        ),
                        {
                            "org_id": org_id,
                            "token": token,
                            "entity_type": item.entity_type,
                            "ct": ciphertext,
                            "digest": digest,
                            "dek_id": enc_id,
                        },
                    )
                ).first()
        except IntegrityError:
            # The only other unique constraint on this table is `(org_id, token)`, so this is
            # the derived-token collision `derive_token`'s `attempt` parameter exists for: two
            # DIFFERENT values in this tenant produced the same 64-bit token. Re-derive rather
            # than reuse the row, which would resolve one value's placeholder to the other's.
            continue

        if inserted is not None:
            return token

        # DO NOTHING fired: a concurrent transaction minted this same value between our lookup
        # and our insert. Its token is the tenant's token now, and adopting it is required, not
        # merely polite - two tokens for one value is the coherence break the vault exists to
        # prevent.
        concurrent = await _lookup(session, org_id, item.entity_type, digest)
        if concurrent is not None:
            return concurrent
        raise VaultKeysUnavailable("mint conflicted but no row is visible; retry the batch")

    raise VaultKeysUnavailable(
        f"could not derive a non-colliding token after {_MAX_TOKEN_ATTEMPTS} attempts"
    )


async def resolve(
    session: AsyncSession,
    org_id: uuid.UUID,
    tokens: list[str],
    provider: KeyProvider,
) -> dict[str, str]:
    """Reverse tokens to raw values. The caller MUST have written an audit row first.

    Unknown tokens are omitted from the result rather than raising. A caller resolving a
    transcript's placeholders should not have the whole render fail because one token was
    minted in a tenant it is no longer looking at - and, less charitably, a per-token error
    would make this endpoint an oracle for which tokens exist in a tenant.

    Rows are grouped by `dek_id` because they need not share one: after an encryption-key
    rotation a tenant legitimately has rows under both the active key and a retired one, and
    reading them is exactly what `dek_id` was added for.
    """
    if not tokens:
        return {}

    rows = (
        await session.execute(
            text(
                "SELECT token, value_ciphertext, dek_id FROM pii_tokens "
                "WHERE org_id = :org_id AND token = ANY(:tokens)"
            ),
            {"org_id": org_id, "tokens": list(tokens)},
        )
    ).all()
    if not rows:
        return {}

    keys: dict[uuid.UUID, bytes] = {}
    resolved: dict[str, str] = {}
    for token, ciphertext, dek_id in rows:
        if dek_id not in keys:
            wrapped = (
                await session.execute(
                    text("SELECT wrapped_key FROM tenant_keys WHERE id = :id"),
                    {"id": dek_id},
                )
            ).first()
            if wrapped is None:
                # The FK is RESTRICT, so this means the key was removed by something that
                # bypassed it. The rows are unreadable and saying so is the only honest
                # answer; a crypto-shredded tenant reaches this state by design.
                continue
            keys[dek_id] = provider.unwrap(org_id, bytes(wrapped[0]))
        resolved[str(token)] = decrypt_value(keys[dek_id], org_id, bytes(ciphertext))
    return resolved
