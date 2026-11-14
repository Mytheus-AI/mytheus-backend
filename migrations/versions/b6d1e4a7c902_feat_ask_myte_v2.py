"""feat: Ask Myte v2 peer questions, memories and routines (ADR-0049 F1, F4, F5)

Three core-api tables, all tenant scoped (`org_id`, RLS ENABLE + FORCE, the canonical
`tenant_isolation` policy and the explicit grants). RLS keeps a row inside its workspace; who may
see it inside the workspace is the route's predicate on every statement, as for `myte_threads`.

- `myte_peer_questions` (F1): one person asking a teammate's Myte. `from_user_id` (the asker),
  `to_user_id` (the recipient, never the asker), `question` (1 to 1000 characters), the asker's
  optional `thread_id` (SET NULL when that thread is deleted), `status` `pending` / `answered` /
  `declined`, `answer` (1 to 8000, present exactly when answered), `decline_reason` (at most 500,
  only when declined), `answered_at` (when it was answered or declined). Visible to its asker and
  its recipient only.
- `myte_memories` (F4): what a person's Myte remembers about them. `text` (1 to 300 characters),
  the optional `source_thread_id` it came from (SET NULL with the thread). Owner only; at most 100
  per person (the route).
- `myte_routines` (F5): a recurring question. `question` (1 to 1000), `scope` (a JSON object, as on
  a thread), `cadence` `daily` / `weekdays` / `weekly`, `weekday` (0 Monday to 6, present exactly
  when weekly), `hour` (0 to 23, on the hour), an IANA `timezone`, `enabled`, `next_run_at` (always
  set; computed by the route), `last_run_at`, `last_thread_id` (SET NULL with the thread). Owner
  only; at most 10 per person (the route).

Hard delete everywhere. A person's erasure deletes their memories and routines and every peer
question they asked or were asked; `users` going away cascades the same rows.

Revision ID: b6d1e4a7c902
Revises: e9f0a1b2c3d4 (workspace_logo)
Create Date: 2026-11-14
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "b6d1e4a7c902"
down_revision: Union[str, None] = "e9f0a1b2c3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _tenant_scope(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute("""
        CREATE TABLE myte_peer_questions (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          from_user_id    uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          to_user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          question        text        NOT NULL,
          thread_id       uuid        REFERENCES myte_threads(id) ON DELETE SET NULL,
          status          text        NOT NULL DEFAULT 'pending',
          answer          text,
          decline_reason  text,
          created_at      timestamptz NOT NULL DEFAULT now(),
          answered_at     timestamptz,
          CONSTRAINT pk_myte_peer_questions PRIMARY KEY (id),
          CONSTRAINT ck_myte_peer_questions_not_self CHECK (from_user_id <> to_user_id),
          CONSTRAINT ck_myte_peer_questions_question
            CHECK (char_length(question) BETWEEN 1 AND 1000),
          CONSTRAINT ck_myte_peer_questions_status
            CHECK (status IN ('pending', 'answered', 'declined')),
          CONSTRAINT ck_myte_peer_questions_answer
            CHECK ((status = 'answered') = (answer IS NOT NULL)
                   AND (answer IS NULL OR char_length(answer) BETWEEN 1 AND 8000)),
          CONSTRAINT ck_myte_peer_questions_decline_reason
            CHECK (decline_reason IS NULL
                   OR (status = 'declined' AND char_length(decline_reason) <= 500)),
          CONSTRAINT ck_myte_peer_questions_answered_at
            CHECK ((status = 'pending') = (answered_at IS NULL))
        );
        """)
    op.execute("CREATE INDEX ix_myte_peer_questions_org_id ON myte_peer_questions (org_id);")
    op.execute(
        "CREATE INDEX ix_myte_peer_questions_inbox ON myte_peer_questions "
        "(org_id, to_user_id, status, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_myte_peer_questions_sent ON myte_peer_questions "
        "(org_id, from_user_id, status, created_at DESC);"
    )
    _tenant_scope("myte_peer_questions")
    op.execute(
        "COMMENT ON TABLE myte_peer_questions IS 'Ask another person''s Myte (ADR-0049 F1): "
        "visible to its asker and recipient only; the recipient reviews every answer. Written "
        "only by core-api.';"
    )

    op.execute("""
        CREATE TABLE myte_memories (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id           uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          text              text        NOT NULL,
          source_thread_id  uuid        REFERENCES myte_threads(id) ON DELETE SET NULL,
          created_at        timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_myte_memories PRIMARY KEY (id),
          CONSTRAINT ck_myte_memories_text CHECK (char_length(text) BETWEEN 1 AND 300)
        );
        """)
    op.execute("CREATE INDEX ix_myte_memories_org_id ON myte_memories (org_id);")
    op.execute(
        "CREATE INDEX ix_myte_memories_owner ON myte_memories (org_id, user_id, created_at DESC);"
    )
    _tenant_scope("myte_memories")
    op.execute(
        "COMMENT ON TABLE myte_memories IS 'What a person''s Myte remembers (ADR-0049 F4), owner "
        "only, at most 100 per person. Written only by core-api.';"
    )

    op.execute("""
        CREATE TABLE myte_routines (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          question        text        NOT NULL,
          scope           jsonb,
          cadence         text        NOT NULL,
          weekday         smallint,
          hour            smallint    NOT NULL,
          timezone        text        NOT NULL,
          enabled         boolean     NOT NULL DEFAULT true,
          next_run_at     timestamptz NOT NULL,
          last_run_at     timestamptz,
          last_thread_id  uuid        REFERENCES myte_threads(id) ON DELETE SET NULL,
          created_at      timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_myte_routines PRIMARY KEY (id),
          CONSTRAINT ck_myte_routines_question CHECK (char_length(question) BETWEEN 1 AND 1000),
          CONSTRAINT ck_myte_routines_scope
            CHECK (scope IS NULL OR jsonb_typeof(scope) = 'object'),
          CONSTRAINT ck_myte_routines_cadence CHECK (cadence IN ('daily', 'weekdays', 'weekly')),
          CONSTRAINT ck_myte_routines_weekday
            CHECK ((cadence = 'weekly') = (weekday IS NOT NULL)
                   AND (weekday IS NULL OR weekday BETWEEN 0 AND 6)),
          CONSTRAINT ck_myte_routines_hour CHECK (hour BETWEEN 0 AND 23),
          CONSTRAINT ck_myte_routines_timezone CHECK (char_length(timezone) BETWEEN 1 AND 64)
        );
        """)
    op.execute("CREATE INDEX ix_myte_routines_org_id ON myte_routines (org_id);")
    op.execute(
        "CREATE INDEX ix_myte_routines_due ON myte_routines (org_id, user_id, next_run_at) "
        "WHERE enabled;"
    )
    _tenant_scope("myte_routines")
    op.execute(
        "COMMENT ON TABLE myte_routines IS 'Recurring Ask Myte questions (ADR-0049 F5), owner "
        "only, at most 10 per person; run by the app when the owner next opens Mytheus. Written "
        "only by core-api.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS myte_routines CASCADE;")
    op.execute("DROP TABLE IF EXISTS myte_memories CASCADE;")
    op.execute("DROP TABLE IF EXISTS myte_peer_questions CASCADE;")
