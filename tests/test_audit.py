"""`audit_logs`: append-only by GRANT, tenant-isolated, and partitioned to not refuse writes.

Plan section 3.7's acceptance checks, run as `mytheus_app`. Tamper-evidence asserted against
a permission error rather than against a trigger, because that is how it is implemented and
because a trigger can be dropped by whoever can write the row.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.audit import ACTION_MEETING_CREATED, audit
from libs.shared.tenancy import set_rls
from tests.conftest import APP_URL, seed_org


def _app_sessions():
    return async_sessionmaker(create_async_engine(APP_URL), expire_on_commit=False)


async def test_audit_writes_inside_the_callers_transaction(migrated_db):
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="audit")
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session, session.begin():
        await set_rls(session, org)
        await audit(
            session,
            org_id=org,
            action=ACTION_MEETING_CREATED,
            target_type="meeting",
            target_id=uuid.uuid4(),
            metadata={"title": "Standup"},
        )

    async with sessions() as session, session.begin():
        await set_rls(session, org)
        row = (
            (
                await session.execute(
                    text("SELECT action, actor_type, metadata FROM audit_logs WHERE org_id = :o"),
                    {"o": str(org)},
                )
            )
            .mappings()
            .one()
        )
    assert row["action"] == ACTION_MEETING_CREATED
    assert row["actor_type"] == "user"


async def test_audit_rolls_back_with_its_operation(migrated_db):
    """The fail-closed contract, from the other side: no orphan audit row for a rolled-back
    change, and no committed change without its audit row."""
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="auditrb")
    await su.dispose()

    sessions = _app_sessions()
    with pytest.raises(RuntimeError):
        async with sessions() as session, session.begin():
            await set_rls(session, org)
            await audit(session, org_id=org, action="meeting.created")
            raise RuntimeError("the operation failed after the audit write")

    async with sessions() as session, session.begin():
        await set_rls(session, org)
        n = (
            await session.execute(
                text("SELECT count(*) FROM audit_logs WHERE org_id = :o"), {"o": str(org)}
            )
        ).scalar_one()
    assert n == 0


async def test_update_and_delete_are_refused(migrated_db):
    """Tamper-evidence. Both must raise a permission error, not silently no-op."""
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="audittamper")
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session, session.begin():
        await set_rls(session, org)
        await audit(session, org_id=org, action="authz.deny")

    for statement in (
        "UPDATE audit_logs SET action = 'tampered'",
        "DELETE FROM audit_logs",
    ):
        with pytest.raises(ProgrammingError) as exc:
            async with sessions() as session, session.begin():
                await set_rls(session, org)
                await session.execute(text(statement))
        assert "permission denied" in str(exc.value).lower(), statement


async def test_audit_is_tenant_isolated(migrated_db):
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await seed_org(su, org_a, slug_prefix="audit-a")
    await seed_org(su, org_b, slug_prefix="audit-b")
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session, session.begin():
        await set_rls(session, org_a)
        await audit(session, org_id=org_a, action="org.provisioned", actor_type="system")

    async with sessions() as session, session.begin():
        await set_rls(session, org_b)
        n = (await session.execute(text("SELECT count(*) FROM audit_logs"))).scalar_one()
    assert n == 0, "tenant B must not read tenant A's audit trail"


async def test_a_write_far_in_the_future_still_lands(migrated_db):
    """The reason for the DEFAULT partition.

    With only the plan's single month, an INSERT past its range RAISES. Audited operations
    fail closed, so that stops the operation being audited: one missed partition-maintenance
    window would have stopped meeting creation for every tenant.
    """
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await seed_org(su, org, slug_prefix="auditfuture")
    await su.dispose()

    far_future = datetime.now(UTC) + timedelta(days=365 * 5)
    sessions = _app_sessions()
    async with sessions() as session, session.begin():
        await set_rls(session, org)
        await session.execute(
            text(
                "INSERT INTO audit_logs (org_id, actor_type, action, occurred_at) "
                "VALUES (:o, 'system', 'retention.sweep', :ts)"
            ),
            {"o": str(org), "ts": far_future},
        )

    # Read through the PARENT and identify the partition via tableoid. Naming the partition
    # directly is deliberately refused: application roles hold no privilege on partitions, so
    # the parent's policy and append-only grants cannot be sidestepped by addressing one.
    async with sessions() as session, session.begin():
        await set_rls(session, org)
        landed_in = (
            await session.execute(
                text(
                    "SELECT tableoid::regclass::text FROM audit_logs "
                    "WHERE org_id = :o AND action = 'retention.sweep'"
                ),
                {"o": str(org)},
            )
        ).scalar_one()
    assert landed_in == "audit_logs_default", (
        "the catch-all partition must accept a write past the last monthly range "
        "rather than refusing it"
    )


async def test_actor_type_is_validated_before_the_database(migrated_db):
    sessions = _app_sessions()
    with pytest.raises(ValueError, match="actor_type"):
        async with sessions() as session:
            await audit(session, org_id=uuid.uuid4(), action="x", actor_type="root")
