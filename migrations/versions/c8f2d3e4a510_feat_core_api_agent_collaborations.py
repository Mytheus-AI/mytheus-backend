"""feat(core-api): agent_collaborations, the A2A collaboration log (P10)

Plan 10-13 section 3.4; ADR-0013 (logical-logging note). Owned and written only by `core-api`:
the `agent-results` consumer inserts one row per `ContextGapAgent` collaboration entry
(`ON CONFLICT (id) DO NOTHING`, the entry's deterministic uuid5 id is the key), and the fill
fields (`fill_content`, `confidence`, `surfaced`, `rationale`, `citations`) go only on the row
whose id is the fill's producing collaboration. `dismissed_at` is set by the requester through
`POST /api/v1/context-fills/{id}/dismiss`.

`meeting_id` deliberately has NO foreign key: a responder-side security entry
(`org_spoof` / `foreign_org`) is logged under the requester's meeting when it is known and the
nil uuid otherwise (P10 contract section 5.9), and the nil uuid is not a meeting. The consumer
checks a non-nil meeting against the tenant under RLS before writing, which is the check a
foreign key could not make anyway (gap 31). `requester` references `users`.

RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the explicit
`grant_tenant_table` (ADR-0021).

Revision ID: c8f2d3e4a510
Revises: b4e1a7c2d901
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "c8f2d3e4a510"
down_revision: Union[str, None] = "b4e1a7c2d901"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE agent_collaborations (
          id             uuid        NOT NULL,
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id     uuid        NOT NULL,
          window_id      text,
          correlation_id text,
          requester      uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          responder      text        NOT NULL,
          hop            text        NOT NULL,
          query          text        NOT NULL,
          query_class    text        NOT NULL,
          target_scope   text,
          result         text        NOT NULL,
          success        boolean     NOT NULL,
          latency_ms     integer     NOT NULL,
          fill_content   text,
          confidence     real,
          surfaced       boolean     NOT NULL DEFAULT false,
          rationale      text,
          citations      jsonb       NOT NULL DEFAULT '[]'::jsonb,
          dismissed_at   timestamptz,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_collaborations PRIMARY KEY (id),
          CONSTRAINT ck_agent_collaborations_confidence_range
            CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1)
        );
        """)
    op.execute(
        "CREATE INDEX ix_agent_collaborations_org_meeting_created "
        "ON agent_collaborations (org_id, meeting_id, created_at);"
    )
    op.execute("ALTER TABLE agent_collaborations ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE agent_collaborations FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON agent_collaborations "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "agent_collaborations")
    op.execute(
        "COMMENT ON TABLE agent_collaborations IS "
        "'A2A collaboration log and ContextGapAgent fills (P10). Written only by core-api: "
        "the agent-results consumer inserts rows, the requester may dismiss a fill.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_collaborations CASCADE;")
