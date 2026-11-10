"""feat: agents that learn: agent_feedback, agent_calibration, person_memory

Polish program wave 2, "Agents that learn and wrap up" (learning from confirms and dismisses,
memory across meetings). Three core-api tables, each tenant-scoped with the canonical RLS policy
and the explicit grant:

- `agent_feedback`: one row per confirm, dismiss, edit (with the before/after diff), ask or save
  a person makes on something an agent proposed: a decision or action candidate, a
  QuestionAgent suggestion, a PersonalListenerAgent note (rated useful = confirm, not useful =
  dismiss), a ContextGapAgent fill. Written only by core-api's routes, in the same transaction as
  the state change and the `agent-feedback` outbox row. `content` and `edit_diff` are a short,
  redacted snapshot (the guidance examples the agents see); `DELETE /api/v1/me/memory` nulls
  them for the caller, the counts stay for calibration.
- `agent_calibration`: the adjusted surface threshold per agent, for the workspace
  (`user_id IS NULL`) and per person (user-scoped agents only), recomputed from the decayed
  confirm/dismiss counts whenever feedback is recorded. `surface` is NULL below the minimum
  sample size (the tenant's base threshold applies). Served by `GET /internal/agent-policy` and
  applied by core-api's own reload reads, so the agent's live `surfaced` and the reload agree.
  `UNIQUE NULLS NOT DISTINCT` (Postgres 15) makes the workspace row unique too.
- `person_memory`: one row per person: weighted topics, recurring questions, muted topics and a
  learning switch. Written by core-api's `agent-feedback` consumer (incrementally) and by the
  owner's `PATCH`/`DELETE /api/v1/me/memory`. Open actions and promises are not copied here:
  they are read live from `action_items` and `commitments`, so they are never stale.

Revision ID: f2a3b4c5d6e7
Revises: d0e1f2a3b4c5
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "f2a3b4c5d6e7"
down_revision: str | None = "d0e1f2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _rls(table: str, comment: str) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    op.execute("""
        CREATE TABLE agent_feedback (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          meeting_id   uuid        REFERENCES meetings(id) ON DELETE CASCADE,
          agent        text        NOT NULL,
          kind         text        NOT NULL,
          candidate_id uuid        NOT NULL,
          action       text        NOT NULL,
          confidence   real,
          content      text,
          edit_diff    jsonb,
          created_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_feedback PRIMARY KEY (id),
          CONSTRAINT ck_agent_feedback_agent
            CHECK (char_length(agent) BETWEEN 1 AND 64 AND agent ~ '^[A-Za-z]+Agent$'),
          CONSTRAINT ck_agent_feedback_kind
            CHECK (kind IN ('decision', 'action', 'suggestion', 'note', 'context_fill')),
          CONSTRAINT ck_agent_feedback_action
            CHECK (action IN ('confirm', 'dismiss', 'edit', 'ask', 'save')),
          CONSTRAINT ck_agent_feedback_confidence
            CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
          CONSTRAINT ck_agent_feedback_content
            CHECK (content IS NULL OR char_length(content) <= 280),
          CONSTRAINT ck_agent_feedback_edit_diff
            CHECK (edit_diff IS NULL OR jsonb_typeof(edit_diff) = 'object')
        );
        """)
    op.execute(
        "CREATE INDEX ix_agent_feedback_org_agent_created "
        "ON agent_feedback (org_id, agent, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_agent_feedback_user_agent_created "
        "ON agent_feedback (user_id, agent, created_at DESC);"
    )
    op.execute("CREATE INDEX ix_agent_feedback_candidate ON agent_feedback (candidate_id);")
    _rls(
        "agent_feedback",
        "A person''s confirm, dismiss, edit, ask or save of something an agent proposed (polish "
        "wave 2). Written only by core-api''s routes, with the agent-feedback outbox row.",
    )

    op.execute("""
        CREATE TABLE agent_calibration (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id     uuid        REFERENCES users(id) ON DELETE CASCADE,
          agent       text        NOT NULL,
          surface     real,
          positives   real        NOT NULL DEFAULT 0,
          negatives   real        NOT NULL DEFAULT 0,
          samples     integer     NOT NULL DEFAULT 0,
          computed_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_calibration PRIMARY KEY (id),
          CONSTRAINT uq_agent_calibration_org_agent_user
            UNIQUE NULLS NOT DISTINCT (org_id, agent, user_id),
          CONSTRAINT ck_agent_calibration_agent CHECK (char_length(agent) BETWEEN 1 AND 64),
          CONSTRAINT ck_agent_calibration_surface
            CHECK (surface IS NULL OR (surface >= 0 AND surface <= 1)),
          CONSTRAINT ck_agent_calibration_counts
            CHECK (positives >= 0 AND negatives >= 0 AND samples >= 0)
        );
        """)
    _rls(
        "agent_calibration",
        "Adjusted surface threshold per agent, per workspace (user_id NULL) and per person, from "
        "decayed confirm/dismiss rates (polish wave 2). Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE person_memory (
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          topics           jsonb       NOT NULL DEFAULT '[]'::jsonb,
          questions        jsonb       NOT NULL DEFAULT '[]'::jsonb,
          muted_topics     jsonb       NOT NULL DEFAULT '[]'::jsonb,
          learning_enabled boolean     NOT NULL DEFAULT true,
          created_at       timestamptz NOT NULL DEFAULT now(),
          updated_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_person_memory PRIMARY KEY (org_id, user_id),
          CONSTRAINT ck_person_memory_topics CHECK (jsonb_typeof(topics) = 'array'),
          CONSTRAINT ck_person_memory_questions CHECK (jsonb_typeof(questions) = 'array'),
          CONSTRAINT ck_person_memory_muted CHECK (jsonb_typeof(muted_topics) = 'array')
        );
        """)
    op.execute("CREATE INDEX ix_person_memory_user_id ON person_memory (user_id);")
    _rls(
        "person_memory",
        "What a person cares about across meetings: weighted topics, recurring questions (polish "
        "wave 2). Written only by core-api: its agent-feedback consumer and the owner''s "
        "/api/v1/me/memory. Private to user_id.",
    )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("DROP TABLE IF EXISTS person_memory CASCADE;")
    op.execute("DROP TABLE IF EXISTS agent_calibration CASCADE;")
    op.execute("DROP TABLE IF EXISTS agent_feedback CASCADE;")
