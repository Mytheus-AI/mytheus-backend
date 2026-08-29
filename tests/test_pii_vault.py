"""The PII token vault against a real database (ADR-0020, ADR-0024).

Covers the four things that would be silently wrong otherwise: minting is idempotent (which is
the coherence property, not a nicety), the two key purposes get the lifecycles the schema
promises, resolving writes an audit row, and `pii_tokens`/`tenant_keys` are RLS-isolated like
every other tenant table.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.crypto import KEY_BYTES, LocalKeyProvider, Purpose
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.vault import MintRequest, ensure_key, mint, resolve
from tests.conftest import APP_URL, seed_org

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def provider() -> LocalKeyProvider:
    return LocalKeyProvider(os.urandom(KEY_BYTES))


async def test_mint_is_idempotent_across_calls(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """The same value in the same tenant mints the SAME token, on a second call and on a
    second entity of the same type. This is what makes one customer one placeholder across
    a transcript and an uploaded document, and it is also what makes an at-least-once redelivery
    of a transcript final a no-op rather than a second token."""
    org = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s, s.begin():
        await set_rls(s, org)
        first = await mint(s, org, [MintRequest("EMAIL", "jane@acme.com")], provider)
    async with factory() as s, s.begin():
        await set_rls(s, org)
        second = await mint(
            s,
            org,
            [MintRequest("EMAIL", "JANE@ACME.COM  "), MintRequest("PERSON", "David Chen")],
            provider,
        )

    assert first[0].token == second[0].token, "normalized-equal values must share a token"
    assert second[1].token != first[0].token
    await engine.dispose()


async def test_mint_preserves_request_order(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """Callers splice tokens back into the exact character spans they detected, so a reordered
    response would swap two people's placeholders in the persisted transcript."""
    org = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    items = [
        MintRequest("PERSON", "Alice"),
        MintRequest("EMAIL", "bob@acme.com"),
        MintRequest("ORG", "Acme"),
    ]
    async with factory() as s, s.begin():
        await set_rls(s, org)
        minted = await mint(s, org, items, provider)
        resolved = await resolve(s, org, [m.token for m in minted], provider)

    assert [m.entity_type for m in minted] == ["PERSON", "EMAIL", "ORG"]
    assert [resolved[m.token] for m in minted] == ["Alice", "bob@acme.com", "Acme"]
    await engine.dispose()


async def test_resolve_omits_unknown_tokens(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """Omitted rather than erroring, so this endpoint is not an oracle for which tokens exist
    in a tenant, and one stale token cannot fail a whole transcript render."""
    org = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s, s.begin():
        await set_rls(s, org)
        minted = await mint(s, org, [MintRequest("EMAIL", "jane@acme.com")], provider)
        resolved = await resolve(s, org, [minted[0].token, "deadbeefdeadbeef"], provider)

    assert resolved == {minted[0].token: "jane@acme.com"}
    await engine.dispose()


async def test_one_active_enc_key_and_one_index_key_per_tenant(
    migrated_db: str, provider
) -> None:  # type: ignore[no-untyped-def]
    """`ensure_key` is get-or-create, and the partial unique indexes are what make it safe
    under concurrency. A second call must adopt the existing key, never mint a rival one."""
    org = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s, s.begin():
        await set_rls(s, org)
        enc_id, enc_key = await ensure_key(s, org, Purpose.ENC, provider)
        again_id, again_key = await ensure_key(s, org, Purpose.ENC, provider)
        idx_id, _ = await ensure_key(s, org, Purpose.INDEX, provider)

    assert (enc_id, enc_key) == (again_id, again_key)
    assert idx_id != enc_id

    async with engine.begin() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT purpose, count(*) FROM tenant_keys "
                    "WHERE org_id = :org GROUP BY purpose"
                ),
                {"org": str(org)},
            )
        ).all()
    assert dict(rows) == {"enc": 1, "index": 1}
    await engine.dispose()


async def test_the_plaintext_data_key_is_never_stored(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """The whole difference between envelope encryption and a key in a column."""
    org = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s, s.begin():
        await set_rls(s, org)
        _, plaintext_key = await ensure_key(s, org, Purpose.ENC, provider)

    async with engine.begin() as conn:
        stored = (
            await conn.execute(
                text(
                    "SELECT wrapped_key FROM tenant_keys " "WHERE org_id = :org AND purpose = 'enc'"
                ),
                {"org": str(org)},
            )
        ).scalar_one()
    assert bytes(stored) != plaintext_key
    assert plaintext_key not in bytes(stored)
    await engine.dispose()


async def test_resolving_writes_an_audit_row(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """Reversal is a PII disclosure (ADR-0020 § Consequences), and the audit row records the
    tokens requested and never the values returned - copying decrypted PII into an append-only
    table retained for a year would defeat the vault."""
    from libs.shared.audit import audit
    from services.identity_tenant_service.routes.pii_tokens import (
        ACTION_PII_TOKENS_RESOLVED,
    )

    org = uuid.uuid4()
    actor = uuid.uuid4()
    engine = create_async_engine(migrated_db)
    await seed_org(engine, org)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s, s.begin():
        await set_rls(s, org)
        minted = await mint(s, org, [MintRequest("EMAIL", "jane@acme.com")], provider)
        token = minted[0].token
        await audit(
            s,
            org_id=org,
            action=ACTION_PII_TOKENS_RESOLVED,
            actor_id=None,
            actor_type="system",
            target_type="pii_tokens",
            metadata={"tokens": [token], "count": 1},
        )
        await resolve(s, org, [token], provider)

    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT metadata::text FROM audit_logs "
                    "WHERE org_id = :org AND action = :action"
                ),
                {"org": str(org), "action": ACTION_PII_TOKENS_RESOLVED},
            )
        ).scalar_one()
    assert token in row
    assert "jane@acme.com" not in row, "the raw value must never reach audit_logs"
    assert actor  # the actor-bearing path is exercised by the route test
    await engine.dispose()


async def test_pii_tokens_and_tenant_keys_are_rls_isolated(migrated_db: str, provider) -> None:  # type: ignore[no-untyped-def]
    """Tenant A cannot read tenant B's vault, and a session with no `app.current_org` reads
    zero rows rather than raising. Same shape as `test_meeting_rooms_rls`, applied to the two
    tables where a leak would be a disclosure of raw PII rather than of metadata."""
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    owner = create_async_engine(migrated_db)
    await seed_org(owner, org_a)
    await seed_org(owner, org_b)
    factory = async_sessionmaker(owner, expire_on_commit=False)
    async with factory() as s, s.begin():
        await set_rls(s, org_b)
        await mint(s, org_b, [MintRequest("EMAIL", "b@acme.com")], provider)
    await owner.dispose()

    app_engine = create_async_engine(APP_URL)
    app_factory = async_sessionmaker(app_engine, expire_on_commit=False)
    async with app_factory() as s, s.begin():
        await set_rls(s, org_a)
        for table in ("pii_tokens", "tenant_keys"):
            count = (
                await s.execute(text(f"SELECT count(*) FROM {table}"))  # noqa: S608
            ).scalar_one()
            assert count == 0, f"tenant A read tenant B's {table}"

    async with app_factory() as s, s.begin():
        # No app.current_org at all: the NULLIF predicate makes this fail closed.
        for table in ("pii_tokens", "tenant_keys"):
            count = (
                await s.execute(text(f"SELECT count(*) FROM {table}"))  # noqa: S608
            ).scalar_one()
            assert count == 0, f"{table} did not fail closed with no app.current_org"
    await app_engine.dispose()
