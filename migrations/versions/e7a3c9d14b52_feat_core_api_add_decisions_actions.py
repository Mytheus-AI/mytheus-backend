"""feat(core-api): decisions, action_items and agent_suggestions

Plan 7-9 section 3.6 steps 1-2 and the Phase 8 Part A contract (section 2). All three tables
are owned and written only by `core-api` (`_canon/01-service-catalog.md`).

`decisions` / `action_items`: the plan's DDL. The `agent-results` consumer inserts a candidate
as `status = 'proposed'` keyed by the candidate's deterministic id (so a replayed window is an
`ON CONFLICT (id) DO NOTHING`); only `POST /decisions/:id/confirm` and
`POST /actions/:id/confirm` move a row to `confirmed` and enqueue the `decisions` / `actions`
outbox row. CHECK constraints pin `confidence` to 0..1 and `status` to the plan's sets. A
decision may not supersede itself. `action_items` also gets `(org_id, meeting_id, status)`,
which is what `GET /meetings/:id/candidates` filters on; the plan's two indexes serve My Tasks
(Part B).

`agent_suggestions`: canon columns (`question`, not the plan's `content`), created now for
QuestionAgent in P9. Partitioned monthly on `created_at` exactly like `worklogs`
(`d5e2f1a8c370`): DEFAULT catch-all, fixed months 2026-11 to 2027-12, owner-only partitions,
registered in `scripts/ensure_partitions.py`.

Every table: RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the
explicit `grant_tenant_table` (ADR-0021). Never a grant on a partition.

Revision ID: e7a3c9d14b52
Revises: d5e2f1a8c370
Create Date: 2026-11-09
"""

from __future__ import annotations

from datetime import date
from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "e7a3c9d14b52"
down_revision: Union[str, None] = "d5e2f1a8c370"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_FIRST = (2026, 11)
_LAST = (2027, 12)
_SUGGESTION_INDEXES = (
    "CREATE INDEX ix_{p}_org_user_status ON {p} (org_id, user_id, status)",
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


def _tenant_policy(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE decisions (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE RESTRICT,
          content      text        NOT NULL,
          rationale    text,
          confidence   real        NOT NULL,
          status       text        NOT NULL DEFAULT 'proposed',
          confirmed_by uuid        REFERENCES users(id) ON DELETE RESTRICT,
          supersedes   uuid        REFERENCES decisions(id) ON DELETE RESTRICT,
          created_at   timestamptz NOT NULL DEFAULT now(),
          deleted_at   timestamptz,
          CONSTRAINT pk_decisions PRIMARY KEY (id),
          CONSTRAINT ck_decisions_confidence_range CHECK (confidence BETWEEN 0 AND 1),
          CONSTRAINT ck_decisions_status
            CHECK (status IN ('proposed', 'confirmed', 'superseded', 'dismissed')),
          CONSTRAINT ck_decisions_not_self_superseding
            CHECK (supersedes IS NULL OR supersedes <> id)
        );
        """
    )
    op.execute("CREATE INDEX ix_decisions_org_meeting ON decisions (org_id, meeting_id);")
    op.execute("CREATE INDEX ix_decisions_org_status ON decisions (org_id, status);")
    _tenant_policy("decisions")
    op.execute(
        "COMMENT ON TABLE decisions IS "
        "'Decision candidates (proposed) and user-confirmed decisions. Written only by "
        "core-api: the agent-results consumer inserts proposed rows; only "
        "POST /decisions/{id}/confirm emits the decisions topic event, via the outbox.';"
    )

    op.execute(
        """
        CREATE TABLE action_items (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE RESTRICT,
          content      text        NOT NULL,
          assignee_id  uuid        REFERENCES users(id) ON DELETE RESTRICT,
          due_date     date,
          status       text        NOT NULL DEFAULT 'proposed',
          confidence   real        NOT NULL,
          confirmed_by uuid        REFERENCES users(id) ON DELETE RESTRICT,
          created_at   timestamptz NOT NULL DEFAULT now(),
          deleted_at   timestamptz,
          CONSTRAINT pk_action_items PRIMARY KEY (id),
          CONSTRAINT ck_action_items_confidence_range CHECK (confidence BETWEEN 0 AND 1),
          CONSTRAINT ck_action_items_status
            CHECK (status IN ('proposed', 'confirmed', 'done', 'dismissed'))
        );
        """
    )
    op.execute(
        "CREATE INDEX ix_action_items_org_assignee_status "
        "ON action_items (org_id, assignee_id, status);"
    )
    op.execute("CREATE INDEX ix_action_items_org_due_date ON action_items (org_id, due_date);")
    op.execute(
        "CREATE INDEX ix_action_items_org_meeting_status "
        "ON action_items (org_id, meeting_id, status);"
    )
    _tenant_policy("action_items")
    op.execute(
        "COMMENT ON TABLE action_items IS "
        "'Action candidates (proposed) and user-confirmed actions. Written only by core-api: "
        "the agent-results consumer inserts proposed rows; only POST /actions/{id}/confirm "
        "emits the actions topic event, via the outbox.';"
    )

    op.execute(
        """
        CREATE TABLE agent_suggestions (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          meeting_id  uuid        NOT NULL REFERENCES meetings(id) ON DELETE RESTRICT,
          question    text        NOT NULL,
          priority    integer     NOT NULL,
          status      text        NOT NULL DEFAULT 'open',
          created_at  timestamptz NOT NULL DEFAULT now(),
          deleted_at  timestamptz,
          CONSTRAINT pk_agent_suggestions PRIMARY KEY (id, created_at),
          CONSTRAINT ck_agent_suggestions_priority CHECK (priority BETWEEN 1 AND 3),
          CONSTRAINT ck_agent_suggestions_status
            CHECK (status IN ('open', 'asked', 'dismissed', 'saved'))
        ) PARTITION BY RANGE (created_at);
        """
    )
    op.execute("CREATE TABLE agent_suggestions_default PARTITION OF agent_suggestions DEFAULT;")
    for tmpl in _SUGGESTION_INDEXES:
        op.execute(tmpl.format(p="agent_suggestions_default") + ";")
    _lock_down("agent_suggestions_default")
    for y, m in _months():
        start, end = _bounds(y, m)
        p = f"agent_suggestions_{y:04d}_{m:02d}"
        op.execute(
            f"CREATE TABLE {p} PARTITION OF agent_suggestions "
            f"FOR VALUES FROM ('{start}') TO ('{end}');"
        )
        for tmpl in _SUGGESTION_INDEXES:
            op.execute(tmpl.format(p=p) + ";")
        _lock_down(p)
    _tenant_policy("agent_suggestions")
    op.execute(
        "COMMENT ON TABLE agent_suggestions IS "
        "'QuestionAgent suggestion cards (P9), written only by core-api. Monthly partitions "
        "on created_at with a DEFAULT; scripts/ensure_partitions.py.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_suggestions CASCADE;")
    op.execute("DROP TABLE IF EXISTS action_items CASCADE;")
    op.execute("DROP TABLE IF EXISTS decisions CASCADE;")
