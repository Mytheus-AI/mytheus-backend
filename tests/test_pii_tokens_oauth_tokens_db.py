"""Real-Postgres checks for migrations `24e134664d84` (oauth_tokens) and `a2afd3bef634`
(pii_tokens): RLS FORCE + `tenant_isolation`, explicit ADR-0021 grants, the envelope CHECKs,
the canon UNIQUEs, and the vault end-to-end as `mytheus_app`.

The program-wide invariants ("every org_id table has tenant_isolation + FORCE", "every org_id
table grants SELECT to mytheus_app", "no BYPASSRLS role reaches a tenant table") are already
asserted exceptionlessly in `tests/test_roles_and_policies.py` and cover both new tables
automatically; the first test here pins them to these two tables by name so a regression
names the table that caused it.
"""

from __future__ import annotations

import base64
import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from libs.shared.crypto import BlindIndex, EnvelopeCipher, LocalKeyProvider
from services.identity_tenant_service.pii_vault.normalize import EntityType
from services.identity_tenant_service.pii_vault.vault import PiiVault, token_for
from tests.conftest import APP_URL, seed_org

TABLES = ("oauth_tokens", "pii_tokens")


def _vault() -> PiiVault:
    provider = LocalKeyProvider.from_env(
        {
            "ENVIRONMENT": "test",
            "MYTHEUS_KEK_BASE64": base64.b64encode(os.urandom(32)).decode(),
            "MYTHEUS_BLIND_INDEX_KEY_BASE64": base64.b64encode(os.urandom(32)).decode(),
        }
    )
    return PiiVault(EnvelopeCipher(provider), BlindIndex(provider))


async def _orgs(migrated_db: str, n: int = 2) -> list[uuid.UUID]:
    su = create_async_engine(migrated_db)
    ids = [uuid.uuid4() for _ in range(n)]
    for i in ids:
        await seed_org(su, i, slug_prefix="vault")
    await su.dispose()
    return ids


async def test_both_tables_are_policied_forced_and_granted(migrated_db):
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            for t in TABLES:
                row = (
                    (
                        await conn.execute(
                            text(
                                "SELECT c.relrowsecurity, c.relforcerowsecurity, "
                                "  EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid "
                                "          AND p.polname = 'tenant_isolation') AS policied, "
                                "  has_table_privilege('mytheus_app', c.oid, 'SELECT') AS sel, "
                                "  has_table_privilege('mytheus_app', c.oid, 'INSERT') AS ins, "
                                "  has_table_privilege('mytheus_app', c.oid, 'UPDATE') AS upd, "
                                "  has_table_privilege('mytheus_app', c.oid, 'DELETE') AS del, "
                                "  has_table_privilege('mytheus_relay', c.oid, 'SELECT') AS relay "
                                "FROM pg_class c WHERE c.oid = CAST(:t AS regclass)"
                            ),
                            {"t": t},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert row["relrowsecurity"] and row["relforcerowsecurity"], t
                assert row["policied"], t
                assert row["sel"] and row["ins"], t
                assert not row["relay"], f"{t}: mytheus_relay must hold nothing"
                if t == "pii_tokens":
                    assert not row["upd"] and not row["del"], "vault mappings are immutable"
                else:
                    assert row["upd"] and row["del"], "integration-service refreshes/disconnects"
    finally:
        await engine.dispose()


async def test_app_role_cannot_update_or_delete_pii_tokens(migrated_db):
    (org,) = await _orgs(migrated_db, 1)
    app = create_async_engine(APP_URL)
    try:
        for stmt in ("UPDATE pii_tokens SET token = token", "DELETE FROM pii_tokens"):
            async with app.connect() as conn:
                with pytest.raises(DBAPIError, match="permission denied"):
                    async with conn.begin():
                        await conn.execute(
                            text("SELECT set_config('app.current_org', :o, true)"),
                            {"o": str(org)},
                        )
                        await conn.execute(text(stmt))
    finally:
        await app.dispose()


async def test_vault_end_to_end_as_app_role_with_isolation(migrated_db):
    org_a, org_b = await _orgs(migrated_db)
    vault = _vault()
    app = create_async_engine(APP_URL)
    sm = async_sessionmaker(app, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sm() as s, s.begin():
            await s.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            t1 = await vault.tokenize(s, org_a, "Alice@Example.com", EntityType.EMAIL)
            t2 = await vault.tokenize(s, org_a, "alice@example.com", EntityType.EMAIL)
            assert t1 == t2
        async with sm() as s, s.begin():
            await s.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            resolved, unresolved = await vault.detokenize(s, org_a, [t1.token], actor_id=None)
            assert [r.value for r in resolved] == ["Alice@Example.com"] and unresolved == []
            audited = (
                await s.execute(
                    text(
                        "SELECT count(*) FROM audit_logs WHERE action = 'pii.token_resolved' "
                        "AND target_id = :t"
                    ),
                    {"t": t1.token},
                )
            ).scalar_one()
            assert audited == 1
        async with sm() as s, s.begin():
            await s.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            assert (await s.execute(text("SELECT count(*) FROM pii_tokens"))).scalar_one() == 0
            resolved, unresolved = await vault.detokenize(s, org_b, [t1.token], actor_id=None)
            assert resolved == [] and unresolved == [t1.token]
        async with app.connect() as conn, conn.begin():
            # No app.current_org at all: fail closed, zero rows, no error.
            for t in TABLES:
                assert (await conn.execute(text(f"SELECT count(*) FROM {t}"))).scalar_one() == 0
    finally:
        await app.dispose()


async def test_pii_tokens_constraints(migrated_db):
    (org,) = await _orgs(migrated_db, 1)
    vault = _vault()
    cipher_blob = vault._cipher.encrypt_str(org, "pii_tokens.value", "x")
    h = os.urandom(32)
    su = create_async_engine(migrated_db)
    insert = text(
        "INSERT INTO pii_tokens (org_id, token, entity_type, value_ciphertext, value_hmac, dek_id) "
        "VALUES (:o, :t, :et, :ct, :h, :d)"
    )
    base = {
        "o": org,
        "t": token_for(h),
        "et": "PERSON",
        "ct": cipher_blob,
        "h": h,
        "d": uuid.uuid4(),
    }
    try:
        async with su.begin() as conn:
            await conn.execute(insert, base)
        bad_rows = [
            {**base, "t": token_for(os.urandom(32)), "ct": b"plaintext value"},  # not envelope
            {**base, "t": token_for(os.urandom(32)), "h": os.urandom(32), "et": "SSN"},
            {**base, "t": "NOT-HEX", "h": os.urandom(32)},
            {**base, "t": token_for(os.urandom(32)), "h": os.urandom(16)},
            {**base, "t": token_for(os.urandom(32))},  # duplicate (org, entity_type, hmac)
            {**base, "h": os.urandom(32)},  # duplicate (org, token)
        ]
        for row in bad_rows:
            with pytest.raises(IntegrityError):
                async with su.begin() as conn:
                    await conn.execute(insert, row)
    finally:
        await su.dispose()


async def test_oauth_tokens_isolation_and_envelope_check(migrated_db):
    org_a, org_b = await _orgs(migrated_db)
    cipher = _vault()._cipher
    conn_id = uuid.uuid4()
    # `connection_id` references `integration_connections` since migration 7c3e9a1f5b20.
    su = create_async_engine(migrated_db)
    try:
        async with su.begin() as conn:
            user_id = uuid.uuid4()
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email) "
                    "VALUES (:u, :o, :c, 't@x.io')"
                ),
                {"u": user_id, "o": org_a, "c": f"user_{user_id.hex[:12]}"},
            )
            await conn.execute(
                text(
                    "INSERT INTO integration_connections (id, org_id, provider, installed_by) "
                    "VALUES (:c, :o, 'slack', :u)"
                ),
                {"c": conn_id, "o": org_a, "u": user_id},
            )
    finally:
        await su.dispose()
    blob = cipher.encrypt_str(
        org_a, "oauth_tokens.access_token", "ya29.x", {"connection_id": str(conn_id)}
    )
    app = create_async_engine(APP_URL)
    ins = text(
        "INSERT INTO oauth_tokens (org_id, connection_id, access_token_enc) VALUES (:o, :c, :a)"
    )
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            await conn.execute(ins, {"o": org_a, "c": conn_id, "a": blob})
        with pytest.raises(IntegrityError):
            async with app.connect() as conn, conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
                )
                await conn.execute(ins, {"o": org_a, "c": conn_id, "a": b"ya29.plaintext"})
        with pytest.raises(DBAPIError, match="row-level security"):
            async with app.connect() as conn, conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
                )
                await conn.execute(ins, {"o": org_a, "c": uuid.uuid4(), "a": blob})
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            assert (await conn.execute(text("SELECT count(*) FROM oauth_tokens"))).scalar_one() == 0
    finally:
        await app.dispose()
