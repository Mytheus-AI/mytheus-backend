"""RLS on the infrastructure tables, as the role the application actually connects as.

These are the literal acceptance checks from plan section 3.3. They target `outbox` rather
than `users` because the outbox is the table with two readers holding different reach: the
request path, which must see one tenant, and the relay, which must see all of them. Getting
that pair wrong is either an event-loss bug (defect D4) or a cross-tenant leak, and nothing
in between.

Tenant isolation for `users` and the role attributes themselves live in
`test_roles_and_policies.py`.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import APP_URL, seed_org


async def _seed_outbox_row(engine, org_id: uuid.UUID) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO outbox (org_id, topic, partition_key, idempotency_key, "
                "schema_subject, payload) VALUES (:o, 'meeting-events', :pk, :ik, "
                "'meeting-events-value', '{}'::jsonb)"
            ),
            {"o": str(org_id), "pk": str(org_id), "ik": str(uuid.uuid4())},
        )


async def test_outbox_is_isolated_between_tenants(migrated_db):
    """Plan 3.3 acceptance: insert as A, read as B, get zero rows."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="rls-a")
    await seed_org(su, org_b, slug_prefix="rls-b")
    await _seed_outbox_row(su, org_a)
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            mine = (
                await conn.execute(
                    text("SELECT count(*) FROM outbox WHERE org_id = :o"), {"o": org_a}
                )
            ).scalar_one()
        assert mine >= 1, "tenant A must see its own outbox rows"

        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            leaked = (await conn.execute(text("SELECT count(*) FROM outbox"))).scalar_one()
        assert leaked == 0, "tenant B must not see tenant A's outbox rows"
    finally:
        await app.dispose()


async def test_outbox_fails_closed_with_no_guc(migrated_db):
    """Plan 3.3 acceptance: unset `app.current_org` returns zero rows, and does not raise.

    Fail-closed has to be silent-empty rather than an error. `NULLIF(current_setting(...,
    true), '')` is what buys that: `current_setting` with `missing_ok` returns NULL instead of
    raising, the comparison is NULL, and the row is filtered. An exception would be a worse
    outcome, because the natural fix for a crashing query is to make the query work.
    """
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="rls-fc")
    await _seed_outbox_row(su, org)
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT id FROM outbox"))).scalars().all()
        assert rows == [], "unset app.current_org must return zero rows"

        # Explicitly reset to empty: the other half of the NULLIF, and the shape a pooled
        # connection lands in between transactions.
        async with app.connect() as conn, conn.begin():
            await conn.execute(text("SELECT set_config('app.current_org', '', true)"))
            rows = (await conn.execute(text("SELECT id FROM outbox"))).scalars().all()
        assert rows == [], "empty app.current_org must also return zero rows"
    finally:
        await app.dispose()


async def test_inbox_is_isolated_between_tenants(migrated_db):
    """The consumer side of the same contract: a dedupe record is tenant-scoped too."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="rls-ia")
    await seed_org(su, org_b, slug_prefix="rls-ib")
    async with su.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                "VALUES ('knowledge-graph-service', :o, :k)"
            ),
            {"o": str(org_a), "k": str(uuid.uuid4())},
        )
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            leaked = (await conn.execute(text("SELECT count(*) FROM inbox"))).scalar_one()
        assert leaked == 0, "tenant B must not see tenant A's inbox records"
    finally:
        await app.dispose()
