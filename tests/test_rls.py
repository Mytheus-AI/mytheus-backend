"""RLS tenant isolation + fail-closed, verified as a non-superuser role (app_rls)."""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import APP_RLS_URL


async def _seed_org_and_user(engine, org_id: uuid.UUID, slug: str) -> None:
    async with engine.begin() as conn:  # superuser: bypasses RLS/WITH CHECK
        await conn.execute(
            text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
            {"id": str(org_id), "slug": slug},
        )
        await conn.execute(
            text(
                "INSERT INTO users (id, org_id, clerk_user_id, email) "
                "VALUES (:uid, :org, :cuid, 'a@b.co')"
            ),
            {"uid": str(uuid.uuid4()), "org": str(org_id), "cuid": f"user_{uuid.uuid4()}"},
        )


async def test_rls_isolation_and_fail_closed(migrated_db):
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await _seed_org_and_user(su, org_a, f"a-{org_a}")
    await _seed_org_and_user(su, org_b, f"b-{org_b}")
    await su.dispose()

    app = create_async_engine(APP_RLS_URL)
    try:
        # Tenant A, with app.current_org = A, sees only A's rows.
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            rows = (await conn.execute(text("SELECT org_id FROM users"))).scalars().all()
        assert rows, "tenant A should see its own users"
        assert all(str(r) == str(org_a) for r in rows), "tenant A must not see tenant B"

        # Unset GUC -> zero rows (fail-closed, not an error).
        async with app.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT org_id FROM users"))).scalars().all()
        assert rows == [], "unset app.current_org must return zero rows"
    finally:
        await app.dispose()
