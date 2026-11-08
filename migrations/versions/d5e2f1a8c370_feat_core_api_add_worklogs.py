"""feat(core-api): worklogs, the PersonalListenerAgent notes read model

Plan 7-9 section 3.3 step 3. `worklogs` is owned and written only by `core-api`
(`_canon/01-service-catalog.md`), from the `agent-results` topic; the AI tier never writes it.

Shape: the plan's DDL. `confidence` and `surfaced` are carried beyond the canon column list
(the plan adds them: `surfaced = false` is the 0.4-0.7 silent-log band, which is persisted
but never rendered). `window_id` is added so a note can be traced to the agent window that
produced it, and so a replayed window that slipped past the inbox cannot write a second
note: UNIQUE `(org_id, meeting_id, user_id, window_id, created_at)` cannot do that on its own
because `created_at` is the partition key and differs between attempts, so the consumer's
inbox row is the dedupe authority and `window_id` is for tracing.

Partitioned monthly on `created_at` with a DEFAULT catch-all and fixed months 2026-11 to
2027-12, the same as `usage_events` (`4c1d8e2a9b70`), and registered in
`scripts/ensure_partitions.py`. RLS ENABLE + FORCE with the canonical `tenant_isolation`
policy; explicit grants (ADR-0021); partitions owner-only.

Revision ID: d5e2f1a8c370
Revises: 4c1d8e2a9b70
Create Date: 2026-11-08
"""

from __future__ import annotations

from datetime import date
from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d5e2f1a8c370"
down_revision: Union[str, None] = "4c1d8e2a9b70"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_FIRST = (2026, 11)
_LAST = (2027, 12)
_INDEXES = (
    "CREATE INDEX ix_{p}_org_meeting_user ON {p} (org_id, meeting_id, user_id, created_at)",
)
_LOCKDOWN_ROLES = "mytheus_app, mytheus_admin, mytheus_relay, PUBLIC"
_SWEEP_ONE = """
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT DISTINCT a.grantee AS g
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.oid = '{p}'::regclass AND a.grantee <> c.relowner AND a.grantee <> 0
    LOOP
        EXECUTE format('REVOKE ALL ON {p} FROM %I', pg_get_userbyid(r.g));
    END LOOP;
END $$;
"""


def _months() -> list[tuple[int, int]]:
    y, m = _FIRST
    out = []
    while (y, m) <= _LAST:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _bounds(y: int, m: int) -> tuple[str, str]:
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return date(y, m, 1).isoformat(), end.isoformat()


def _lock_down(p: str) -> None:
    op.execute(f"REVOKE ALL ON {p} FROM {_LOCKDOWN_ROLES};")
    op.execute(_SWEEP_ONE.format(p=p))


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE worklogs (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id       uuid        NOT NULL REFERENCES meetings(id) ON DELETE RESTRICT,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          window_id        text        NOT NULL,
          content          text        NOT NULL,
          relevance_score  real,
          confidence       real,
          created_by_agent boolean     NOT NULL DEFAULT true,
          source_chunk_ids uuid[]      NOT NULL DEFAULT '{}',
          surfaced         boolean     NOT NULL DEFAULT false,
          created_at       timestamptz NOT NULL DEFAULT now(),
          deleted_at       timestamptz,
          CONSTRAINT pk_worklogs PRIMARY KEY (id, created_at),
          CONSTRAINT ck_worklogs_relevance_range
            CHECK (relevance_score IS NULL OR relevance_score BETWEEN 0 AND 1),
          CONSTRAINT ck_worklogs_confidence_range
            CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
        ) PARTITION BY RANGE (created_at);
        """
    )
    op.execute("CREATE TABLE worklogs_default PARTITION OF worklogs DEFAULT;")
    for tmpl in _INDEXES:
        op.execute(tmpl.format(p="worklogs_default") + ";")
    _lock_down("worklogs_default")
    for y, m in _months():
        start, end = _bounds(y, m)
        p = f"worklogs_{y:04d}_{m:02d}"
        op.execute(
            f"CREATE TABLE {p} PARTITION OF worklogs FOR VALUES FROM ('{start}') TO ('{end}');"
        )
        for tmpl in _INDEXES:
            op.execute(tmpl.format(p=p) + ";")
        _lock_down(p)
    op.execute("ALTER TABLE worklogs ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE worklogs FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON worklogs "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "worklogs")
    op.execute(
        "COMMENT ON TABLE worklogs IS "
        "'PersonalListenerAgent notes, written only by core-api from agent-results. "
        "surfaced=false is the 0.4-0.7 silent-log band (persisted, never rendered). "
        "Monthly partitions on created_at with a DEFAULT; scripts/ensure_partitions.py.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS worklogs CASCADE;")
