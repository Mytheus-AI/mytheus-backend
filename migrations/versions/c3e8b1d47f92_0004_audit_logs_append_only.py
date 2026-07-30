"""0004 audit_logs: partitioned, RLS-scoped, append-only by GRANT

Plan 1-2 section 3.7, stood up at P2 rather than P13 so the audit trail is live the moment
RLS is. An audit table that arrives after the thing it audits has a hole in it exactly where
the first incident would be.

Two deliberate departures from the plan text, both recorded in the band file:

1. TWELVE monthly partitions plus a DEFAULT catch-all, not the plan's single partition. The
   plan creates one partition for the current month and defers `pg_partman`. The band ends in
   W5 and that partition expires at the end of the month; an INSERT with no matching range
   RAISES, and section 3.7 makes audited operations fail closed, so one missed maintenance
   window would stop meeting creation outright. Twelve months plus a catch-all removes the
   failure class rather than scheduling it. The DEFAULT partition is the backstop for the
   backstop: it accepts rows past the twelfth month so the failure is a slow query and an
   alert, never a refused write.

2. Append-only is enforced by REVOKE rather than by a trigger. A BEFORE UPDATE trigger that
   raises can be dropped by whoever can write the row; a missing GRANT cannot be recovered by
   the role that lacks it.

Revision ID: c3e8b1d47f92
Revises: b7d9f3a55c21
Create Date: 2026-07-30

"""

from datetime import date
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3e8b1d47f92"
down_revision: Union[str, None] = "b7d9f3a55c21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

APP_ROLE = "mytheus_app"
ADMIN_ROLE = "mytheus_admin"

# Fixed, not computed from "today": a migration must produce the same schema on every machine
# and on every replay, so the partition set cannot depend on when it happens to run.
_FIRST_PARTITION_YEAR = 2026
_FIRST_PARTITION_MONTH = 7
_PARTITION_COUNT = 12

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _month_bounds(index: int) -> tuple[str, str]:
    month0 = (_FIRST_PARTITION_MONTH - 1) + index
    start = date(_FIRST_PARTITION_YEAR + month0 // 12, month0 % 12 + 1, 1)
    month1 = month0 + 1
    end = date(_FIRST_PARTITION_YEAR + month1 // 12, month1 % 12 + 1, 1)
    return start.isoformat(), end.isoformat()


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE audit_logs (
          id          uuid NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          actor_id    uuid,
          actor_type  text NOT NULL,
          action      text NOT NULL,
          target_type text,
          target_id   text,
          metadata    jsonb NOT NULL DEFAULT '{}'::jsonb,
          ip          inet,
          occurred_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (id, occurred_at),
          CONSTRAINT ck_audit_logs_actor_type_valid
            CHECK (actor_type IN ('user','agent','system'))
        ) PARTITION BY RANGE (occurred_at);
        """
    )

    partitions = []
    for i in range(_PARTITION_COUNT):
        start, end = _month_bounds(i)
        name = f"audit_logs_{start[:4]}_{start[5:7]}"
        partitions.append(name)
        op.execute(
            f"CREATE TABLE {name} PARTITION OF audit_logs "
            f"FOR VALUES FROM ('{start}') TO ('{end}');"
        )
        op.execute(f"CREATE INDEX ix_{name}_org_occurred ON {name} (org_id, occurred_at);")
        op.execute(f"CREATE INDEX ix_{name}_org_target ON {name} (org_id, target_type, target_id);")

    op.execute("CREATE TABLE audit_logs_default PARTITION OF audit_logs DEFAULT;")
    partitions.append("audit_logs_default")
    op.execute(
        "CREATE INDEX ix_audit_logs_default_org_occurred "
        "ON audit_logs_default (org_id, occurred_at);"
    )

    # A partition is a table in its own right, and that has two consequences that together
    # would have made everything below decorative:
    #
    # 1. Privileges are checked against the relation NAMED in the query. Reached through
    #    `audit_logs` the parent's ACL governs; named directly, the partition's own ACL does.
    # 2. `ALTER DEFAULT PRIVILEGES` in migration 0002 grants the full DML set on every newly
    #    created table in this schema, and these partitions are newly created tables.
    #
    # So without this revoke, `DELETE FROM audit_logs_2026_07` succeeded for `mytheus_app`
    # even though `DELETE FROM audit_logs` was refused, and RLS did not apply either, because
    # the parent's policy is only enforced on access through the parent. The append-only
    # audit trail would have been erasable one partition at a time.
    #
    # Every future partition must be revoked the same way. That is asserted in CI
    # (tests/test_roles_and_policies.py) rather than left as a note for whoever adds the next
    # one.
    for name in partitions:
        op.execute(f"REVOKE ALL ON {name} FROM {APP_ROLE}, {ADMIN_ROLE};")
    op.execute(
        """
        COMMENT ON TABLE audit_logs_default IS
        'Catch-all so an INSERT past the last monthly partition degrades to a slow query and '
        'an alert rather than a refused write. Audited operations fail closed, so a refused '
        'audit write stops the operation it was auditing. Rows landing here mean partition '
        'maintenance is overdue.';
        """
    )

    # RLS on the parent propagates to every partition, existing and future.
    op.execute("ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE audit_logs FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON audit_logs "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )

    # Append-only, enforced by the database. The ALTER DEFAULT PRIVILEGES from 0002 granted
    # the full DML set to new tables, so the REVOKE is what actually makes this table
    # tamper-evident and it must come after.
    op.execute(f"REVOKE ALL ON audit_logs FROM {APP_ROLE};")
    op.execute(f"GRANT SELECT, INSERT ON audit_logs TO {APP_ROLE};")
    # The operator role does not get UPDATE or DELETE either. Retention is implemented by
    # DROPping an expired partition, which is a DDL operation reserved to the schema owner,
    # so nothing in the running system needs row-level deletion here.
    op.execute(f"REVOKE ALL ON audit_logs FROM {ADMIN_ROLE};")
    op.execute(f"GRANT SELECT, INSERT ON audit_logs TO {ADMIN_ROLE};")

    op.execute(
        """
        COMMENT ON TABLE audit_logs IS
        'Append-only compliance trail. UPDATE and DELETE are revoked from every application '
        'role: tamper-evidence is a GRANT, not a convention, because a trigger can be dropped '
        'by whoever can write the row. Retention is by DROPping an expired partition.';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit_logs CASCADE;")
