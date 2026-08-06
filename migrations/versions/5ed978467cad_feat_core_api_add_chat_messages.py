"""feat(core-api): add chat_messages (current-month partition)

Plan 3-4 section 3.3, step 1 (Task 5 of the band). `chat_messages` is the durable write target
for `POST /chat`: `core-api` is the sole writer, `realtime-gateway` relays chat over its socket
for latency but always calls back here for the durable row (`04-service-catalog.md` section
3.5, `03-container-architecture.md` section 5).

Monthly range partition on `created_at`, following `audit_logs`'s (`c3e8b1d47f92`) precedent
for the mechanics: fixed year/month constants rather than `date.today()`, so the migration
produces the same schema on every machine and on every replay; RLS on the parent, which
propagates to every partition, existing and future; and the partition's own grants explicitly
revoked from the application roles so `chat_messages_2026_08` cannot be reached by name,
bypassing both the parent's RLS policy (only enforced through the parent) and, structurally,
tenant isolation.

One deliberate departure from `audit_logs`, per the team lead's resolution of Plan 3-4 section
3.3's ambiguity #1: only the CURRENT month's partition is created here, not twelve months plus
a DEFAULT catch-all. `audit_logs` needed the wider buffer because audited operations fail
closed - a missing partition there would have silently stopped meeting creation, the operation
being audited. Chat has no such coupling: a missing next-month partition fails only chat sends,
loudly (an INSERT past the upper bound raises), and `pg_partman` nightly maintenance to keep
pace with the calendar is explicitly deferred to a later band (the plan's own text). This is a
known, accepted, and explicitly flagged risk, not an oversight - see the task-5 report.

Not append-only, unlike `audit_logs`: chat_messages supports soft-delete (`deleted_at`), so
the migration does NOT revoke UPDATE from `mytheus_app` on the table itself, only on the
partition reached directly (the same partition-bypass risk `audit_logs` closed, independent
of the append-only question).

Revision ID: 5ed978467cad
Revises: 866f7eedb0e7
Create Date: 2026-08-05 23:40:00.000000

"""

from __future__ import annotations

from datetime import date
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5ed978467cad"
down_revision: Union[str, None] = "866f7eedb0e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

APP_ROLE = "mytheus_app"
ADMIN_ROLE = "mytheus_admin"

# Fixed, not computed from "today": see c3e8b1d47f92's rationale. The partition set must not
# depend on when the migration happens to run.
_PARTITION_YEAR = 2026
_PARTITION_MONTH = 8

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _month_bounds() -> tuple[str, str]:
    start = date(_PARTITION_YEAR, _PARTITION_MONTH, 1)
    next_month = _PARTITION_MONTH + 1
    end = date(
        _PARTITION_YEAR + (1 if next_month > 12 else 0),
        1 if next_month > 12 else next_month,
        1,
    )
    return start.isoformat(), end.isoformat()


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE chat_messages (
          id          uuid NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id  uuid NOT NULL REFERENCES meetings(id),
          user_id     uuid NOT NULL REFERENCES users(id),
          content     text NOT NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          deleted_at  timestamptz,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
        """
    )

    start, end = _month_bounds()
    partition = f"chat_messages_{start[:4]}_{start[5:7]}"
    op.execute(
        f"CREATE TABLE {partition} PARTITION OF chat_messages "
        f"FOR VALUES FROM ('{start}') TO ('{end}');"
    )
    # (org_id, meeting_id, created_at): the shape POST /chat and a future GET /meetings/:id/chat
    # both filter and order on.
    op.execute(
        f"CREATE INDEX ix_{partition}_org_meeting_created "
        f"ON {partition} (org_id, meeting_id, created_at);"
    )
    # GIN full-text index per the plan text. `to_tsvector('english', content)` is computed at
    # query time (no generated column) so the index stays a plain expression index, matching
    # how every other index in this schema is a plain CREATE INDEX rather than a stored
    # generated column plus an index on it.
    op.execute(
        f"CREATE INDEX ix_{partition}_content_fts ON {partition} "
        f"USING GIN (to_tsvector('english', content));"
    )

    # Partition-bypass close, independent of RLS: see module docstring. Reachable only through
    # the parent, where the ALTER DEFAULT PRIVILEGES grant from migration a1c4e7f20b18 and the
    # tenant_isolation policy below both actually apply.
    op.execute(f"REVOKE ALL ON {partition} FROM {APP_ROLE}, {ADMIN_ROLE};")

    # RLS on the parent propagates to every partition, existing and future.
    op.execute("ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE chat_messages FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON chat_messages "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )

    op.execute(
        f"""
        COMMENT ON TABLE chat_messages IS
        'Canonical chat_messages, monthly range-partitioned on created_at. Only the '
        '{start[:7]} partition exists by design (Plan 3-4 section 3.3, team-lead resolution '
        '#1): pg_partman maintenance lands in a later band. An INSERT past this partition''s '
        'upper bound will raise until that job (or a follow-up migration) adds the next one - '
        'a known, accepted, and explicitly flagged risk, not an oversight.';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS chat_messages CASCADE;")
