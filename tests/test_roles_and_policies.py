"""Database roles and RLS policy shape: the checks that make defect D1 unrepeatable.

Plan 0 reported tenant isolation as proven. It was proven for `app_rls`, a role the
application never connected as; the application connected as `postgres`, which holds
`rolbypassrls` in `supabase/postgres`, so every policy was inert on the real request path.
These tests assert the properties directly against the roles the application actually uses,
and two of them are the CI assertions required as compensating controls for removing the
`organizations` policy (work-done-till-now/02 section 4).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import ADMIN_URL, APP_URL, RELAY_URL

APPLICATION_ROLES = ["mytheus_app", "mytheus_relay", "mytheus_admin"]


async def _seed_org_and_user(engine, org_id: uuid.UUID, slug: str) -> uuid.UUID:
    """Seed as the schema owner. Fixtures may write across tenants; the app may not."""
    user_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO organizations (id, name, slug, clerk_org_id) "
                "VALUES (:id, 'Org', :slug, :cid)"
            ),
            {"id": str(org_id), "slug": slug, "cid": f"org_{org_id}"},
        )
        await conn.execute(
            text(
                "INSERT INTO users (id, org_id, clerk_user_id, email) "
                "VALUES (:uid, :org, :cuid, 'a@b.co')"
            ),
            {"uid": str(user_id), "org": str(org_id), "cuid": f"user_{uuid.uuid4()}"},
        )
    return user_id


# --- CI assertion 1: no application role may bypass RLS -------------------------------------


async def test_no_application_role_is_superuser_or_bypasses_rls(migrated_db):
    """Compensating control 4. This is the assertion that would have caught D1 on day one.

    `rolbypassrls` is the subtle one: `supabase/postgres` grants it to `postgres` in its own
    demote-postgres migration, so the role is not a superuser yet still ignores every policy.
    Checking only `rolsuper` would have passed while the product leaked.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT rolname, rolsuper, rolbypassrls FROM pg_roles "
                            "WHERE rolname = ANY(:names)"
                        ),
                        {"names": APPLICATION_ROLES},
                    )
                )
                .mappings()
                .all()
            )
    finally:
        await engine.dispose()

    found = {r["rolname"] for r in rows}
    assert found == set(APPLICATION_ROLES), f"missing roles: {set(APPLICATION_ROLES) - found}"
    for r in rows:
        assert not r["rolsuper"], f"{r['rolname']} is a superuser"
        assert not r["rolbypassrls"], f"{r['rolname']} holds BYPASSRLS: every policy is inert"


# --- CI assertion 2: org_id implies a tenant_isolation policy --------------------------------


# Partitions are excluded from the policy rule and covered by the separate grant rule below.
# A partition is reached through its parent, where the policy and the grants live; naming one
# directly is what the grant rule forbids. Only `relispartition` earns the exclusion, so a
# plain table can never slip through by being mistaken for one.
_ORG_ID_TABLES = (
    "SELECT c.oid, c.relname FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_attribute a ON a.attrelid = c.oid "
    "WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND NOT c.relispartition "
    "  AND a.attname = 'org_id' AND a.attnum > 0 AND NOT a.attisdropped"
)


async def test_every_org_id_table_has_a_tenant_isolation_policy(migrated_db):
    """Compensating control 3. The rule is exceptionless, which is what makes it checkable.

    Removing the `organizations` policy is what buys this: while a table with no `org_id`
    column carried a policy anyway, the rule needed a hardcoded exception list, and
    hardcoded exception lists rot. Now the predicate is simply "has org_id implies policied".
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            unpolicied = (
                (
                    await conn.execute(
                        text(
                            f"SELECT relname FROM ({_ORG_ID_TABLES}) t "
                            "WHERE NOT EXISTS ("
                            "  SELECT 1 FROM pg_policy p "
                            "  WHERE p.polrelid = t.oid AND p.polname = 'tenant_isolation') "
                            "ORDER BY relname"
                        )
                    )
                )
                .scalars()
                .all()
            )
            unforced = (
                (
                    await conn.execute(
                        text(
                            f"SELECT t.relname FROM ({_ORG_ID_TABLES}) t "
                            "JOIN pg_class c ON c.oid = t.oid "
                            "WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity) "
                            "ORDER BY t.relname"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert unpolicied == [], f"tables with org_id and no tenant_isolation policy: {unpolicied}"
    assert unforced == [], f"tables with org_id not ENABLE+FORCE row level security: {unforced}"


async def test_no_application_role_can_reach_a_partition_directly(migrated_db):
    """The other half of the partition rule, and a real hole this assertion caught.

    A partition is a table in its own right. Privileges are checked against the relation
    NAMED in the query, and the parent's RLS policy is only enforced on access through the
    parent. Migration 0002's `ALTER DEFAULT PRIVILEGES` grants the full DML set on every
    newly created table in the schema, and partitions are newly created tables, so
    `DELETE FROM audit_logs_2026_07` succeeded for `mytheus_app` while
    `DELETE FROM audit_logs` was correctly refused: an append-only audit trail erasable one
    partition at a time, with no tenant isolation on the way.

    Every future partition must be revoked the same way, which is why this is a CI assertion
    and not a comment in the migration that added the last one.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            leaked = (
                (
                    await conn.execute(
                        text(
                            "SELECT c.relname || ' -> ' || g.grantee || ':' || g.privilege_type "
                            "FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "JOIN information_schema.role_table_grants g "
                            "  ON g.table_name = c.relname AND g.table_schema = n.nspname "
                            "WHERE n.nspname = 'public' AND c.relispartition "
                            "  AND g.grantee = ANY(:roles) "
                            "ORDER BY 1"
                        ),
                        {"roles": APPLICATION_ROLES},
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert leaked == [], f"application roles hold direct privileges on partitions: {leaked}"


async def test_organizations_carries_no_tenant_isolation_policy(migrated_db):
    """The tenant root is the deliberate carve-out (canon 02-data-model section 7 rule 7).

    `organizations` has no `org_id` column: its `id` IS the org_id, so the canonical policy
    is not expressible on it. Plan 0 invented one keyed on `id`, which made the pre-tenant
    `clerk_org_id` lookup return zero rows forever and left the resolved tenant-claim
    decision with no executable path.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            policies = (
                (
                    await conn.execute(
                        text("SELECT policyname FROM pg_policies WHERE tablename = 'organizations'")
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()
    assert "tenant_isolation" not in policies


# --- The request-path role -------------------------------------------------------------------


async def test_app_role_is_isolated_and_fails_closed(migrated_db):
    """The isolation proof, as `mytheus_app`. Plan 0 ran this as a role nothing used."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await _seed_org_and_user(su, org_a, f"a-{org_a}")
    await _seed_org_and_user(su, org_b, f"b-{org_b}")
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        async with app.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            rows = (await conn.execute(text("SELECT org_id FROM users"))).scalars().all()
        assert rows, "tenant A should see its own users"
        assert all(str(r) == str(org_a) for r in rows), "tenant A must not see tenant B"

        async with app.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT org_id FROM users"))).scalars().all()
        assert rows == [], "unset app.current_org must return zero rows (fail closed)"
    finally:
        await app.dispose()


async def test_app_role_cannot_write_another_tenants_row(migrated_db):
    """WITH CHECK, not just USING: a tenant must not be able to plant a row in another."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    await _seed_org_and_user(su, org_a, f"wa-{org_a}")
    await _seed_org_and_user(su, org_b, f"wb-{org_b}")
    await su.dispose()

    app = create_async_engine(APP_URL)
    try:
        with pytest.raises(ProgrammingError) as exc:
            async with app.connect() as conn, conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
                )
                await conn.execute(
                    text(
                        "INSERT INTO users (org_id, clerk_user_id, email) "
                        "VALUES (:org, :cuid, 'x@y.co')"
                    ),
                    {"org": str(org_b), "cuid": f"user_{uuid.uuid4()}"},
                )
        assert "row-level security" in str(exc.value).lower()
    finally:
        await app.dispose()


async def test_app_role_has_only_column_grants_on_organizations(migrated_db):
    """Compensating control 1. `SELECT *` on the tenant root must be refused outright.

    The accepted risk of removing the policy is a forgotten WHERE exposing the customer
    directory. Column-level grants bound that: the columns the resolver needs are readable,
    a blanket read is not, and the failure is loud rather than silent.
    """
    app = create_async_engine(APP_URL)
    try:
        with pytest.raises(ProgrammingError) as exc:
            async with app.connect() as conn:
                await conn.execute(text("SELECT * FROM organizations"))
        assert "permission denied" in str(exc.value).lower()

        # The sanctioned columns still read, or the resolver could not work at all.
        async with app.connect() as conn:
            await conn.execute(
                text("SELECT id, clerk_org_id, status, deleted_at FROM organizations LIMIT 1")
            )
    finally:
        await app.dispose()


# --- The relay role --------------------------------------------------------------------------


async def test_relay_role_drains_outbox_across_tenants(migrated_db):
    """Defect D4. The relay must see every tenant's rows with no `app.current_org` set.

    Under the fail-closed policy alone this returned zero rows, which `relay_once` reads as
    an empty queue: total event loss with no error, no log, and nothing distinguishing it
    from idle. The fix is one permissive policy on `outbox` alone, not a BYPASSRLS role, so
    the cross-tenant reach stays visible in `pg_policies`.
    """
    # Every statement below names the orgs this test created. An unscoped `DELETE FROM
    # outbox` (to make the count exact) and an unscoped `UPDATE outbox SET published_at`
    # both used to stand here: against a database anyone else is using, the first discards
    # their queued events and the second marks them published so the relay never sends them.
    # Scoping the count to these three orgs makes the assertion exact without touching a row
    # this test does not own.
    orgs: list[str] = []

    su = create_async_engine(migrated_db)
    async with su.begin() as conn:
        for _ in range(3):
            # Each row belongs to a real tenant root. Since defect D6 was fixed, an outbox
            # row for an org that does not exist is no longer insertable at all.
            org = uuid.uuid4()
            orgs.append(str(org))
            await conn.execute(
                text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
                {"id": str(org), "slug": f"relay-{org}"},
            )
            await conn.execute(
                text(
                    "INSERT INTO outbox (org_id, topic, partition_key, idempotency_key, "
                    "schema_subject, payload) VALUES (:o, 'meeting-events', :pk, :ik, "
                    "'meeting-events-value', '{}'::jsonb)"
                ),
                {"o": str(org), "pk": str(org), "ik": str(uuid.uuid4())},
            )
    await su.dispose()

    relay = create_async_engine(RELAY_URL)
    try:
        async with relay.connect() as conn, conn.begin():
            n = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM outbox "
                        "WHERE published_at IS NULL AND org_id = ANY(CAST(:orgs AS uuid[]))"
                    ),
                    {"orgs": orgs},
                )
            ).scalar_one()
        # Three rows, three distinct tenant roots, with no `app.current_org` set. Under the
        # fail-closed policy alone this is zero, which is the defect the test exists for.
        assert n == 3, "the relay must see every tenant's unpublished rows"

        # And it can mark them published, which is the other half of the drain.
        async with relay.connect() as conn, conn.begin():
            await conn.execute(
                text(
                    "UPDATE outbox SET published_at = now() "
                    "WHERE org_id = ANY(CAST(:orgs AS uuid[]))"
                ),
                {"orgs": orgs},
            )
    finally:
        await relay.dispose()


async def test_relay_role_is_scoped_to_outbox_alone(migrated_db):
    """The permissive policy is on one table. Reaching anywhere else must be refused."""
    relay = create_async_engine(RELAY_URL)
    try:
        for table in ("users", "organizations", "meetings"):
            with pytest.raises(ProgrammingError) as exc:
                async with relay.connect() as conn:
                    await conn.execute(text(f"SELECT * FROM {table}"))  # noqa: S608 - fixed list
            assert "permission denied" in str(exc.value).lower(), table
    finally:
        await relay.dispose()


async def test_admin_role_is_still_subject_to_rls(migrated_db):
    """The elevated role is elevated in grants, not in isolation.

    `mytheus_admin` exists for the operator seed script and RTBF purges. FORCE ROW LEVEL
    SECURITY applies to it like everyone else, so an RTBF purge still has to name the tenant
    it is purging rather than reaching across all of them.
    """
    su = create_async_engine(migrated_db)
    org = uuid.uuid4()
    await _seed_org_and_user(su, org, f"adm-{org}")
    await su.dispose()

    admin = create_async_engine(ADMIN_URL)
    try:
        async with admin.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT org_id FROM users"))).scalars().all()
        assert rows == [], "admin without app.current_org must still see zero tenant rows"
    finally:
        await admin.dispose()
