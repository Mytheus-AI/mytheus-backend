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

    BROADENED in `9b2c4d1e6f30`. This asserted against `APPLICATION_ROLES` - the three
    `mytheus_*` roles - and passed for months while every partition also granted the full DML
    set to `anon`, `authenticated` and `service_role`, which `supabase/postgres` grants on
    every new table in `public` at bootstrap. `service_role` holds `rolbypassrls`. The test
    was scoped to the roles the defect was found on rather than to the property worth holding,
    so it could not see the larger arm of the same defect. It now asserts the property: NO
    grantee other than the relation's owner holds anything on a partition, whatever it is
    called and whoever created it.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            leaked = (
                (
                    await conn.execute(
                        text(
                            "SELECT c.oid::regclass::text || ' -> ' "
                            "       || COALESCE(pg_get_userbyid(NULLIF(a.grantee, 0)), 'PUBLIC') "
                            "       || ':' || a.privilege_type "
                            "FROM pg_class c "
                            "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                            "WHERE c.relnamespace = 'public'::regnamespace "
                            "  AND c.relispartition "
                            "  AND a.grantee <> c.relowner "
                            "ORDER BY 1"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert leaked == [], f"non-owner roles hold direct privileges on partitions: {leaked}"


# --- CI assertion 4: the schema grants nothing by default ------------------------------------


async def test_public_schema_grants_nothing_by_default(migrated_db):
    """The root cause, asserted so it cannot be reintroduced by a well-meaning migration.

    A default privilege on TABLES in `public` is not a convenience, it is the mechanism that
    produced a live cross-tenant read/write/delete: a partition created by
    `CREATE TABLE ... PARTITION OF` is a newly created table, so it inherits the grant, while
    the parent's RLS policy is only enforced on access through the parent. Migration
    `9b2c4d1e6f30` retired it; this is what stops the next person from putting it back because
    a new table was unreadable and a one-line `ALTER DEFAULT PRIVILEGES` looked like the fix.

    Stated as "no default ACL grants to anyone but its own owner" rather than as a list of
    forbidden roles, because the list is what failed last time: the recorded defect named
    `mytheus_app`/`mytheus_admin`, and the grants actually present also included the image's
    `anon`/`authenticated`/`service_role`.

    SEQUENCES are deliberately not covered: a sequence has no rows, no RLS and no partitions,
    so inheriting a grant there costs nothing and removing it would break the next BIGSERIAL
    insert for no gain.

    Scoped to roles that actually own a table here, which is the sentence worth asserting:
    "no role that creates tables in this schema grants anything on them by default." A default
    ACL applies only to objects created BY its owner, so `supabase_admin`'s entry - which the
    image installs, which still grants everything to `anon`/`authenticated`/`service_role`,
    and which the migration runner cannot alter because it is not a member - is inert while
    `supabase_admin` owns nothing here. If it ever creates a table in `public`, this starts
    failing, which is when it stops being inert. That is a live residual, recorded in
    `9b2c4d1e6f30`, not something this test pretends is closed.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            granted = (
                (
                    await conn.execute(
                        text(
                            "SELECT d.defaclrole::regrole::text || ' grants ' "
                            "       || a.privilege_type || ' on new TABLES to ' "
                            "       || COALESCE(pg_get_userbyid(NULLIF(a.grantee, 0)), 'PUBLIC') "
                            "FROM pg_default_acl d "
                            "CROSS JOIN LATERAL aclexplode(d.defaclacl) a "
                            "WHERE d.defaclnamespace = 'public'::regnamespace "
                            "  AND d.defaclobjtype = 'r' "
                            "  AND a.grantee <> d.defaclrole "
                            "  AND d.defaclrole IN ("
                            "    SELECT DISTINCT c.relowner FROM pg_class c "
                            "    WHERE c.relnamespace = 'public'::regnamespace "
                            "      AND c.relkind IN ('r','p')) "
                            "ORDER BY 1"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert granted == [], (
        "public grants privileges on new TABLES by default; a future partition will inherit "
        f"them and the parent's RLS policy will not follow: {granted}"
    )


# --- CI assertion 5: no BYPASSRLS role can reach anything ------------------------------------


async def test_no_bypassrls_role_holds_privileges_on_a_public_table(migrated_db):
    """Defect D1's shape on a role nobody in this program created.

    `test_no_application_role_is_superuser_or_bypasses_rls` checks the three roles we make.
    It cannot see `service_role`, which `supabase/postgres` ships with `rolbypassrls` and
    grants the full DML set on every new table in `public` - including `audit_logs`, whose
    append-only guarantee is a GRANT, and `organizations`, whose carve-out is column-level
    grants. `authenticator` is a LOGIN role and a member of it, so "NOLOGIN" is not the
    control it looks like. Verified live before `9b2c4d1e6f30`: `SET ROLE service_role` then
    `SELECT count(*) FROM audit_logs` returned every tenant's rows with no `app.current_org`
    set, and the matching DELETE succeeded.

    The migration revokes the reach. It cannot revoke the attribute - `ALTER ROLE ...
    NOBYPASSRLS` needs superuser and the migration runner is not one on this image - so the
    role still exists and `supabase_admin` could re-grant. That is why this is a standing CI
    assertion and not a one-time cleanup.

    The relation's owner is excluded: it is the migration runner, it holds everything by
    definition, and it is BYPASSRLS on this image too.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            reachable = (
                (
                    await conn.execute(
                        text(
                            "SELECT DISTINCT r.rolname || ' -> ' || c.oid::regclass::text "
                            "FROM pg_class c "
                            "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                            "JOIN pg_roles r ON r.oid = a.grantee "
                            "WHERE c.relnamespace = 'public'::regnamespace "
                            "  AND c.relkind IN ('r','p') "
                            "  AND a.grantee <> c.relowner "
                            "  AND r.rolbypassrls "
                            "ORDER BY 1"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert reachable == [], (
        "a role holding BYPASSRLS has privileges on a tenant table; every tenant_isolation "
        f"policy is inert for anything that can reach it: {reachable}"
    )


async def test_a_new_partition_inherits_no_privileges(migrated_db):
    """The guard fires, proven by creating a partition rather than by reading a migration.

    This is the property `9b2c4d1e6f30` actually buys, and the only way to know it holds is to
    make a partition the way a future migration would - with no `REVOKE`, no
    `lock_down_partition`, nobody remembering anything - and look at what it came out with.
    Before the migration this partition would have carried the full DML set for five roles.

    A throwaway table rather than a real one: attaching a range to `audit_logs` while a
    DEFAULT partition exists forces a scan of that partition under an ACCESS EXCLUSIVE lock,
    and this assertion is about the schema's behaviour, not about any one table's.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _partition_grant_probe CASCADE"))
            await conn.execute(
                text(
                    "CREATE TABLE _partition_grant_probe (org_id uuid NOT NULL, at date NOT NULL) "
                    "PARTITION BY RANGE (at)"
                )
            )
            await conn.execute(
                text(
                    "CREATE TABLE _partition_grant_probe_p1 PARTITION OF _partition_grant_probe "
                    "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
                )
            )
        try:
            async with engine.connect() as conn:
                inherited = (
                    (
                        await conn.execute(
                            text(
                                "SELECT COALESCE("
                                "         pg_get_userbyid(NULLIF(a.grantee, 0)), 'PUBLIC') "
                                "       || ':' || a.privilege_type "
                                "FROM pg_class c "
                                "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                                "WHERE c.relname = '_partition_grant_probe_p1' "
                                "  AND a.grantee <> c.relowner "
                                "ORDER BY 1"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
        finally:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS _partition_grant_probe CASCADE"))
    finally:
        await engine.dispose()

    assert inherited == [], (
        "a partition created with no explicit revoke still inherited privileges: "
        f"{inherited}. The schema-wide default privilege is back."
    )


async def test_every_org_id_table_grants_read_to_the_request_role(migrated_db):
    """The other half of retiring the default privilege, and the reason it was safe to.

    Migration 0002 defended the schema-wide grant on the grounds that without it "every new
    table is silently unreadable by the application until someone remembers a GRANT, and the
    symptom is a production permission error rather than a failing test." That objection is
    right, and this is the failing test it asked for: nothing is inherited any more, so a
    migration that creates a tenant table and forgets `migrations/_grants.grant_tenant_table`
    fails here instead of at runtime.

    SELECT is the assertion rather than the full DML set, so it stays exceptionless.
    `audit_logs` deliberately holds only SELECT and INSERT - its tamper-evidence is the
    absence of UPDATE and DELETE - and a test demanding all four would need a carve-out for
    it, which is the kind of hardcoded exception list this file avoids everywhere else.
    """
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            ungranted = (
                (
                    await conn.execute(
                        text(
                            f"SELECT t.relname FROM ({_ORG_ID_TABLES}) t "
                            "WHERE NOT EXISTS ("
                            "  SELECT 1 FROM pg_class c "
                            "  CROSS JOIN LATERAL aclexplode(c.relacl) a "
                            "  WHERE c.oid = t.oid AND a.privilege_type = 'SELECT' "
                            "    AND a.grantee = 'mytheus_app'::regrole) "
                            "ORDER BY t.relname"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    assert ungranted == [], (
        "tables with org_id that mytheus_app cannot read; nothing is inherited since "
        f"9b2c4d1e6f30, so these need an explicit grant: {ungranted}"
    )


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


async def test_app_role_insert_on_organizations_is_exactly_the_provisioning_columns(migrated_db):
    """ADR-0025 / migration `1afe4928b214`: `POST /orgs` may create a tenant root, and nothing
    more. Asserted as an exact column set, plus no table-level INSERT, UPDATE or DELETE, so a
    later "just grant INSERT on organizations" widening fails here rather than in review."""
    engine = create_async_engine(migrated_db)
    try:
        async with engine.connect() as conn:
            insertable = set(
                (
                    await conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.column_privileges "
                            "WHERE table_schema = 'public' AND table_name = 'organizations' "
                            "  AND grantee = 'mytheus_app' AND privilege_type = 'INSERT'"
                        )
                    )
                )
                .scalars()
                .all()
            )
            table_level = (
                (
                    await conn.execute(
                        text(
                            "SELECT a.privilege_type FROM pg_class c "
                            "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                            "WHERE c.oid = 'organizations'::regclass "
                            "  AND a.grantee = 'mytheus_app'::regrole ORDER BY 1"
                        )
                    )
                )
                .scalars()
                .all()
            )
            col_update_delete = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM information_schema.column_privileges "
                        "WHERE table_schema = 'public' AND table_name = 'organizations' "
                        "  AND grantee = 'mytheus_app' AND privilege_type = 'UPDATE'"
                    )
                )
            ).scalar_one()
    finally:
        await engine.dispose()

    assert insertable == {"id", "name", "slug", "status", "plan_id", "clerk_org_id"}
    assert table_level == [], f"mytheus_app holds table-level privileges: {table_level}"
    assert col_update_delete == 0, "mytheus_app must not UPDATE any organizations column"


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
