"""`rbac_grants`: the authorization store, and its separation from `users.role`.

Plan section 8 decision 2 resolved these into separate stores. That decision is only real if
the schema enforces it, which is what the first test asserts: `tier` and `role` must be
different columns in different tables, so that editing a profile can never change what
someone is permitted to do.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import APP_URL, seed_org


async def _seed_user(engine, org_id: uuid.UUID) -> uuid.UUID:
    user_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
                "VALUES (:uid, :org, :cuid, 'a@b.co', 'Staff Engineer')"
            ),
            {"uid": str(user_id), "org": str(org_id), "cuid": f"user_{uuid.uuid4()}"},
        )
    return user_id


async def test_tier_and_domain_role_are_separate_stores(migrated_db):
    """The schema-level proof of the resolved decision."""
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            cols = (
                (
                    await conn.execute(
                        text(
                            "SELECT table_name || '.' || column_name "
                            "FROM information_schema.columns "
                            "WHERE table_schema='public' "
                            "AND (table_name='rbac_grants' AND column_name='tier') "
                            " OR (table_name='users' AND column_name='role')"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert set(cols) == {"rbac_grants.tier", "users.role"}

        # And `users` must not have grown a permission column of its own.
        async with engine.connect() as conn:
            user_cols = (
                (
                    await conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema='public' AND table_name='users'"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert "tier" not in user_cols
    finally:
        await engine.dispose()


async def test_one_tier_per_user_per_tenant(migrated_db):
    """Two grants for one user makes "what may they do" depend on row order."""
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="rbac")
    user_id = await _seed_user(su, org)

    async with su.begin() as conn:
        await conn.execute(
            text("INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (:o, :u, 'admin')"),
            {"o": str(org), "u": str(user_id)},
        )
    with pytest.raises(IntegrityError):
        async with su.begin() as conn:
            await conn.execute(
                text("INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (:o, :u, 'member')"),
                {"o": str(org), "u": str(user_id)},
            )
    await su.dispose()


async def test_tier_is_constrained_to_the_three_values(migrated_db):
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="rbactier")
    user_id = await _seed_user(su, org)

    with pytest.raises(IntegrityError):
        async with su.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO rbac_grants (org_id, user_id, tier) "
                    "VALUES (:o, :u, 'superuser')"
                ),
                {"o": str(org), "u": str(user_id)},
            )
    await su.dispose()


async def test_rbac_grants_are_tenant_isolated(migrated_db):
    """A grant is the most sensitive row in the tenant: it must not be readable across one."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="rbac-a")
    await seed_org(su, org_b, slug_prefix="rbac-b")
    user_a = await _seed_user(su, org_a)
    async with su.begin() as conn:
        await conn.execute(
            text("INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (:o, :u, 'owner')"),
            {"o": str(org_a), "u": str(user_a)},
        )
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            leaked = (await conn.execute(text("SELECT count(*) FROM rbac_grants"))).scalar_one()
        assert leaked == 0, "tenant B must not see tenant A's grants"

        async with app.connect() as conn, conn.begin():
            n = (await conn.execute(text("SELECT count(*) FROM rbac_grants"))).scalar_one()
        assert n == 0, "unset app.current_org must return zero grants (fail closed)"
    finally:
        await app.dispose()


async def test_same_clerk_user_can_belong_to_two_organizations(migrated_db):
    """Gap 13. The global unique on `clerk_user_id` made this impossible.

    Clerk lets one person belong to several organizations, so the second membership arrived
    at the webhook handler as a constraint violation rather than as a row.
    """
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="dual-a")
    await seed_org(su, org_b, slug_prefix="dual-b")
    clerk_user = f"user_{uuid.uuid4()}"

    async with su.begin() as conn:
        for org in (org_a, org_b):
            await conn.execute(
                text(
                    "INSERT INTO users (org_id, clerk_user_id, email) "
                    "VALUES (:org, :cuid, 'dual@b.co')"
                ),
                {"org": str(org), "cuid": clerk_user},
            )

    async with su.connect() as conn:
        n = (
            await conn.execute(
                text("SELECT count(*) FROM users WHERE clerk_user_id = :c"), {"c": clerk_user}
            )
        ).scalar_one()
    assert n == 2
    await su.dispose()
