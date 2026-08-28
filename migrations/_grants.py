"""Explicit table grants for migrations, now that nothing is inherited.

Migration `9b2c4d1e6f30` retired the schema-wide `ALTER DEFAULT PRIVILEGES` that migration
`a1c4e7f20b18` (0002) installed. That grant existed for a good reason - without it a new table
is unreadable by the application until someone remembers a GRANT - but a partition created by
`CREATE TABLE ... PARTITION OF` is a newly created table too, so it inherited the grant while
the parent's RLS policy did not follow it, which is how `transcript_chunks` ended up with a
live cross-tenant read/write/delete (Plan 3-4, `work-done-till-now/03` section 3).

So the grant is now explicit, and this module is what makes "explicit" cost one line. Call
`grant_tenant_table(op, "<table>")` in the same migration that creates the table, right after
the RLS policy. `test_every_org_id_table_grants_read_to_the_request_role`, in
`tests/test_roles_and_policies.py`, fails the build if you forget - which is the failing test
0002's comment wanted in place of a production permission error.

DO NOT call any of this for a PARTITION. A partition is reached through its parent, where the
grants and the policy live; a direct grant on a partition is the defect this whole module
exists because of, and `test_no_application_role_can_reach_a_partition_directly` will fail.

IF YOU LANDED HERE FROM "permission denied for table transcript_chunks": that table belongs to
`transcription-service` in `mytheus-realtime`, whose migration `be66698dbefb` predates
`9b2c4d1e6f30` and still relies on the retired default privilege. It needs the equivalent of
`grant_tenant_table` in its own migration; see that migration's docstring for the exact line.
Do not add it from this repo.
"""

from __future__ import annotations

APP_ROLE = "mytheus_app"
RELAY_ROLE = "mytheus_relay"
ADMIN_ROLE = "mytheus_admin"


def grant_tenant_table(op, table: str) -> None:  # type: ignore[no-untyped-def]
    """The ordinary tenant-table grant: full DML for the request path and the operator seam.

    `mytheus_relay` is deliberately absent. It holds a grant on exactly one table, `outbox`,
    paired with the one permissive policy that lets it drain across tenants; giving it reach
    anywhere else is what the `outbox_relay_drain` policy was written to avoid.
    """
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE}, {ADMIN_ROLE};")


def grant_append_only_table(op, table: str) -> None:  # type: ignore[no-untyped-def]
    """SELECT and INSERT only, for a table whose tamper-evidence is a GRANT.

    `audit_logs` is the case: UPDATE and DELETE are withheld from every application role
    because a trigger can be dropped by whoever can write the row, and a missing GRANT cannot
    be recovered by the role that lacks it. Retention there is by DROPping an expired
    partition, which is DDL and reserved to the schema owner.
    """
    op.execute(f"GRANT SELECT, INSERT ON {table} TO {APP_ROLE}, {ADMIN_ROLE};")
