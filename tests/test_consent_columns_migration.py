"""Migration `866f7eedb0e7`: the three ADR-0018 consent columns, plus the carried-over
`ck_meeting_rooms_status_valid` CHECK constraint Task 2's review flagged as missing.

Column shape and defaults, proven directly against the database rather than assumed from the
migration source - the same principle `test_meeting_rooms_rls.py` applies to RLS. Does not
duplicate `tests/test_meeting_token_route.py`'s behavioral consent-gate tests; this file is
about the schema the gate is built on.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import seed_org


async def test_tenant_settings_consent_required_defaults_false(migrated_db):
    """ADR-0018: the mechanism must be inert until someone explicitly opts a tenant in."""
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    try:
        await seed_org(su, org_id, slug_prefix="consent-default")
        async with su.begin() as conn:
            await conn.execute(
                text("INSERT INTO tenant_settings (org_id) VALUES (:o)"), {"o": str(org_id)}
            )
            value = (
                await conn.execute(
                    text("SELECT consent_required FROM tenant_settings WHERE org_id = :o"),
                    {"o": str(org_id)},
                )
            ).scalar_one()
        assert value is False
    finally:
        await su.dispose()


async def test_meeting_participants_consent_columns_are_nullable_and_default_null(migrated_db):
    su = create_async_engine(migrated_db)
    org_id, meeting_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    try:
        await seed_org(su, org_id, slug_prefix="consent-cols")
        async with su.begin() as conn:
            await conn.execute(
                text("INSERT INTO meetings (id, org_id) VALUES (:m, :o)"),
                {"m": str(meeting_id), "o": str(org_id)},
            )
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email) "
                    "VALUES (:u, :o, :cuid, 'x@example.com')"
                ),
                {"u": str(user_id), "o": str(org_id), "cuid": f"cu-{user_id}"},
            )
            await conn.execute(
                text(
                    "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
                    "VALUES (:m, :u, :o)"
                ),
                {"m": str(meeting_id), "u": str(user_id), "o": str(org_id)},
            )
            row = (
                await conn.execute(
                    text(
                        "SELECT consented_at, consent_version FROM meeting_participants "
                        "WHERE meeting_id = :m AND user_id = :u"
                    ),
                    {"m": str(meeting_id), "u": str(user_id)},
                )
            ).one()
        assert row.consented_at is None
        assert row.consent_version is None
    finally:
        await su.dispose()


async def test_meeting_rooms_status_check_constraint_rejects_invalid_values(migrated_db):
    """Carried-over review item from Task 2: `ck_meeting_rooms_status_valid` must exist and
    actually reject a value outside {pending, active, ended}, matching the house-style
    4-for-4 precedent (`meetings.status_valid`, `rbac_grants.tier_valid`,
    `organizations.status_valid`, `data_residency.region_valid`)."""
    su = create_async_engine(migrated_db)
    org_id, meeting_id = uuid.uuid4(), uuid.uuid4()
    try:
        await seed_org(su, org_id, slug_prefix="room-check")
        async with su.begin() as conn:
            await conn.execute(
                text("INSERT INTO meetings (id, org_id) VALUES (:m, :o)"),
                {"m": str(meeting_id), "o": str(org_id)},
            )

        raised = False
        try:
            async with su.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room, status) "
                        "VALUES (:o, :m, 'r', 'not-a-real-status')"
                    ),
                    {"o": str(org_id), "m": str(meeting_id)},
                )
        except Exception as exc:  # noqa: BLE001 - asserting on the DB's own error, not ours
            raised = True
            assert "ck_meeting_rooms_status_valid" in str(exc)
        assert raised, "an invalid meeting_rooms.status value must be rejected by the CHECK"

        # And the three canonical values are all still accepted. A fresh meeting per value:
        # meeting_rooms carries a UNIQUE (org_id, meeting_id), so reusing one meeting_id would
        # collide on the second insert regardless of the CHECK constraint under test here.
        async with su.begin() as conn:
            for status_value in ("pending", "active", "ended"):
                other_meeting_id = uuid.uuid4()
                await conn.execute(
                    text("INSERT INTO meetings (id, org_id) VALUES (:m, :o)"),
                    {"m": str(other_meeting_id), "o": str(org_id)},
                )
                await conn.execute(
                    text(
                        "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room, status) "
                        "VALUES (:o, :m, :room, :status)"
                    ),
                    {
                        "o": str(org_id),
                        "m": str(other_meeting_id),
                        "room": f"room-{status_value}",
                        "status": status_value,
                    },
                )
    finally:
        await su.dispose()
