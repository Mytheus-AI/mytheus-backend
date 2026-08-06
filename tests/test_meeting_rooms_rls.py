"""RLS isolation for `meeting_rooms` (Task 2, Plan 3-4 section 3.1 step 2).

Same shape as `tests/test_rls.py`'s outbox/inbox coverage and
`tests/test_roles_and_policies.py`'s `mytheus_app` isolation proof, applied to the new
table: tenant A cannot read tenant B's `meeting_rooms` rows, and a session with no
`app.current_org` set reads zero rows rather than raising (fail-closed).

`meeting_rooms` also falls under `test_roles_and_policies.test_every_org_id_table_has_a_
tenant_isolation_policy`'s exceptionless check (any table with an `org_id` column must carry
`tenant_isolation` + FORCE ROW LEVEL SECURITY), so this file's job is the acceptance-check
proof of isolated *reads*, not the policy's mere existence.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import APP_URL, seed_org


async def _seed_meeting_room(engine, org_id: uuid.UUID) -> uuid.UUID:
    """Seed a meeting + its room as the schema owner (mirrors `_seed_outbox_row`'s pattern).

    The FK to `meetings(id)` means a `meeting_rooms` row cannot be conjured for a meeting
    that does not exist, so the meeting is seeded first, same as `seed_org` for the tenant
    root.
    """
    meeting_id = uuid.uuid4()
    room_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO meetings (id, org_id) VALUES (:id, :org)"),
            {"id": str(meeting_id), "org": str(org_id)},
        )
        await conn.execute(
            text(
                "INSERT INTO meeting_rooms (id, org_id, meeting_id, livekit_room) "
                "VALUES (:id, :org, :meeting, :room)"
            ),
            {
                "id": str(room_id),
                "org": str(org_id),
                "meeting": str(meeting_id),
                "room": f"room-{room_id}",
            },
        )
    return room_id


async def test_meeting_rooms_is_isolated_between_tenants(migrated_db):
    """Acceptance check 3 (first half): tenant A cannot read tenant B's meeting_rooms rows."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="mr-a")
    await seed_org(su, org_b, slug_prefix="mr-b")
    await _seed_meeting_room(su, org_a)
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            mine = (
                await conn.execute(
                    text("SELECT count(*) FROM meeting_rooms WHERE org_id = :o"), {"o": org_a}
                )
            ).scalar_one()
        assert mine >= 1, "tenant A must see its own meeting_rooms rows"

        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            leaked = (await conn.execute(text("SELECT count(*) FROM meeting_rooms"))).scalar_one()
        assert leaked == 0, "tenant B must not see tenant A's meeting_rooms rows"
    finally:
        await app.dispose()


async def test_meeting_rooms_fails_closed_with_no_guc(migrated_db):
    """Acceptance check 3 (second half): unset app.current_org reads zero rows, not an error."""
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="mr-fc")
    await _seed_meeting_room(su, org)
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT id FROM meeting_rooms"))).scalars().all()
        assert rows == [], "unset app.current_org must return zero rows"

        async with app.connect() as conn, conn.begin():
            await conn.execute(text("SELECT set_config('app.current_org', '', true)"))
            rows = (await conn.execute(text("SELECT id FROM meeting_rooms"))).scalars().all()
        assert rows == [], "empty app.current_org must also return zero rows"
    finally:
        await app.dispose()


async def test_meeting_rooms_relrowsecurity_and_relforcerowsecurity(migrated_db):
    """Acceptance check 1 (the DB-catalog half): both flags are set on the relation."""
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class "
                        "WHERE relname = 'meeting_rooms'"
                    )
                )
            ).one()
    finally:
        await engine.dispose()
    assert row.relrowsecurity is True
    assert row.relforcerowsecurity is True


async def test_meeting_rooms_tenant_isolation_policy_matches_canonical_shape(migrated_db):
    """Acceptance check 2: same predicate shape as an existing tenant table (here, `outbox`)."""
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT tablename, qual, with_check FROM pg_policies "
                            "WHERE policyname = 'tenant_isolation' "
                            "AND tablename IN ('meeting_rooms', 'outbox')"
                        )
                    )
                )
                .mappings()
                .all()
            )
    finally:
        await engine.dispose()

    by_table = {r["tablename"]: r for r in rows}
    assert "meeting_rooms" in by_table, "meeting_rooms must carry a tenant_isolation policy"
    assert "outbox" in by_table, "outbox is the reference table for the canonical predicate shape"
    assert by_table["meeting_rooms"]["qual"] == by_table["outbox"]["qual"]
    assert by_table["meeting_rooms"]["with_check"] == by_table["outbox"]["with_check"]
