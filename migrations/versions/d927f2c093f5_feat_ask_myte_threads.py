"""feat: Ask Myte threads and messages (ADR-0047 D7)

Ask Myte keeps each person's conversations with their Myte so `/ask`, the `⌘J` sheet and every
entry point read and write the same history.

- `myte_threads` (core-api): one conversation, owned by one person (`user_id`). `title`
  (1 to 120 characters, "New question" by default), `pinned`, the optional `scope` the question
  was asked in (jsonb: `{kind, meeting_id, person_id, as_of}`), `last_message_at` (bumped by every
  append; the list's sort key).
- `myte_messages` (core-api): one turn, `role` `user` or `myte`, its text, and the `payload` the UI
  renders besides text (steps, sources, cards, proposals, follow-ups, abstained/degraded; a JSON
  object, at most 64 KB, enforced by the route), plus the owner's `feedback` (-1, 0, 1) and an
  optional reason.

Both tenant scoped: `org_id`, RLS ENABLE + FORCE, the canonical `tenant_isolation` policy and the
explicit grants. RLS keeps a thread inside its workspace; owner-only access (admins included) is
the route's `user_id = caller` predicate on every statement, and `user_id` is on the message row
too so that predicate never needs a join to hold. Hard delete: deleting a thread cascades to its
messages, and a person's erasure (`DELETE FROM myte_threads WHERE user_id = ...`) takes both.

Revision ID: d927f2c093f5
Revises: d8e3f4a5b6c7 (clerk_user_in_any_tenant)
Create Date: 2026-11-14
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d927f2c093f5"
down_revision: Union[str, None] = "d8e3f4a5b6c7"
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
        CREATE TABLE myte_threads (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          title            text        NOT NULL DEFAULT 'New question',
          pinned           boolean     NOT NULL DEFAULT false,
          scope            jsonb,
          created_at       timestamptz NOT NULL DEFAULT now(),
          updated_at       timestamptz NOT NULL DEFAULT now(),
          last_message_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_myte_threads PRIMARY KEY (id),
          CONSTRAINT ck_myte_threads_title CHECK (char_length(title) BETWEEN 1 AND 120),
          CONSTRAINT ck_myte_threads_scope
            CHECK (scope IS NULL OR jsonb_typeof(scope) = 'object')
        );
        """)
    op.execute("CREATE INDEX ix_myte_threads_org_id ON myte_threads (org_id);")
    op.execute(
        "CREATE INDEX ix_myte_threads_owner_recent ON myte_threads "
        "(org_id, user_id, last_message_at DESC);"
    )
    _tenant_scope("myte_threads")
    op.execute(
        "COMMENT ON TABLE myte_threads IS 'Ask Myte conversations (ADR-0047 D7), private to "
        "their owner (user_id): every route filters user_id = caller. Written only by core-api.';"
    )

    op.execute("""
        CREATE TABLE myte_messages (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          thread_id        uuid        NOT NULL REFERENCES myte_threads(id) ON DELETE CASCADE,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          role             text        NOT NULL,
          content          text        NOT NULL DEFAULT '',
          payload          jsonb,
          feedback         smallint    NOT NULL DEFAULT 0,
          feedback_reason  text,
          created_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_myte_messages PRIMARY KEY (id),
          CONSTRAINT ck_myte_messages_role CHECK (role IN ('user', 'myte')),
          CONSTRAINT ck_myte_messages_feedback CHECK (feedback IN (-1, 0, 1)),
          CONSTRAINT ck_myte_messages_content CHECK (char_length(content) <= 32000),
          CONSTRAINT ck_myte_messages_payload
            CHECK (payload IS NULL OR jsonb_typeof(payload) = 'object'),
          CONSTRAINT ck_myte_messages_feedback_reason
            CHECK (feedback_reason IS NULL OR char_length(feedback_reason) <= 500)
        );
        """)
    op.execute("CREATE INDEX ix_myte_messages_org_id ON myte_messages (org_id);")
    op.execute(
        "CREATE INDEX ix_myte_messages_thread ON myte_messages (thread_id, created_at);"
    )
    _tenant_scope("myte_messages")
    op.execute(
        "COMMENT ON TABLE myte_messages IS 'Ask Myte turns (ADR-0047 D7): the question or the "
        "answer, its rendered payload and the owner''s feedback. Owner-only like its thread. "
        "Written only by core-api.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS myte_messages CASCADE;")
    op.execute("DROP TABLE IF EXISTS myte_threads CASCADE;")
