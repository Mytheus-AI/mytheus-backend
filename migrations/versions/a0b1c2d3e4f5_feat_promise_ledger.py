"""feat: one promise ledger (promises from every channel, sources, date proposals, capacity)

"Beyond the meeting", Layer 2: one promise ledger. Additive only; the existing
`GET/PATCH /api/v1/commitments` keep their shape and keep serving meeting promises.

`commitments` (core-api, private to `user_id`):
- `meeting_id` becomes nullable: a promise can now come from Slack, email or a manual add.
  `ck_commitments_meeting_channel` keeps it honest: a `meeting` / `in_person` promise has a
  meeting, every other channel has none.
- `channel` (`meeting`, `in_person`, `slack`, `email`, `manual`; default `meeting`). Existing
  rows of a meeting recorded on the phone (`meetings.source = 'in_person'`) are backfilled.
- `review_state` (`pending`, `confirmed`; default `confirmed`): a promise the agent extracted
  from a message waits for its owner to confirm or dismiss it. Meeting promises keep landing
  `confirmed`, as before.
- `due_at` (date): the resolved due date (`due_hint` stays the words as said).
- `recipient_user_id` / `recipient_label`: who the promise was made to, when known.
- `source_link`, `source_quote`, `source_ref`, `source_at`: the original moment or message
  (a Slack permalink, the exact sentence, the provider's message id, when it was sent).
- `confidence`: the extractor's confidence for an extracted promise.
- `created_by`: who added it (the owner for a manual add, the giver for a hand-off).
- `handed_off_to` / `handed_off_from`: a hand-off closes the original (`dismissed`,
  `handed_off_to` set) and creates a `pending` copy for the colleague (`handed_off_from`).
- `uq_commitments_source`: one promise per (person, source message, content), the replay guard
  for extracted promises.

`promise_date_proposals` (core-api): "Offer Monday instead?" One row per proposal to move a
promise's or an action's due date, from the person who owes it (`proposer_id`) to the person who
asked for it (`requester_id`). `state` moves once from `pending` to `accepted`, `declined` or
`withdrawn`; at most one `pending` per target.

`promise_capture_preferences` (core-api): per person, whether their agent may look for promises
in their own Slack messages and email (both default on; nothing is captured for a person who is
not a user of the workspace).

`meetings.calendar_event_kind`: the Google Calendar event type of a synced event (`default`,
`focusTime`, `outOfOffice`, ...), so the capacity model can count focus time apart from meeting
time. Null for Mytheus-native meetings.

Revision ID: a0b1c2d3e4f5
Revises: f8a9b0c1d2e3
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "a0b1c2d3e4f5"
down_revision: Union[str, None] = "f8a9b0c1d2e3"
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
    # --- commitments: sources, review, due date, recipient, hand-off --------------------------
    op.execute("ALTER TABLE commitments ALTER COLUMN meeting_id DROP NOT NULL;")
    op.execute("""
        ALTER TABLE commitments
          ADD COLUMN channel           text        NOT NULL DEFAULT 'meeting',
          ADD COLUMN review_state      text        NOT NULL DEFAULT 'confirmed',
          ADD COLUMN due_at            date,
          ADD COLUMN recipient_user_id uuid        REFERENCES users(id) ON DELETE SET NULL,
          ADD COLUMN recipient_label   text,
          ADD COLUMN source_link       text,
          ADD COLUMN source_quote      text,
          ADD COLUMN source_ref        text,
          ADD COLUMN source_at         timestamptz,
          ADD COLUMN confidence        real,
          ADD COLUMN created_by        uuid        REFERENCES users(id) ON DELETE SET NULL,
          ADD COLUMN handed_off_to     uuid        REFERENCES users(id) ON DELETE SET NULL,
          ADD COLUMN handed_off_from   uuid        REFERENCES commitments(id) ON DELETE SET NULL,
          ADD CONSTRAINT ck_commitments_channel
            CHECK (channel IN ('meeting', 'in_person', 'slack', 'email', 'manual')),
          ADD CONSTRAINT ck_commitments_review_state
            CHECK (review_state IN ('pending', 'confirmed')),
          ADD CONSTRAINT ck_commitments_meeting_channel
            CHECK ((channel IN ('meeting', 'in_person')) = (meeting_id IS NOT NULL)),
          ADD CONSTRAINT ck_commitments_recipient_label
            CHECK (recipient_label IS NULL OR char_length(recipient_label) <= 200),
          ADD CONSTRAINT ck_commitments_source_link
            CHECK (source_link IS NULL OR char_length(source_link) <= 2000),
          ADD CONSTRAINT ck_commitments_source_quote
            CHECK (source_quote IS NULL OR char_length(source_quote) <= 1000),
          ADD CONSTRAINT ck_commitments_source_ref
            CHECK (source_ref IS NULL OR char_length(source_ref) <= 300),
          ADD CONSTRAINT ck_commitments_confidence
            CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1);
        """)
    op.execute("""
        UPDATE commitments c SET channel = 'in_person'
        FROM meetings m WHERE m.id = c.meeting_id AND m.source = 'in_person';
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_commitments_source ON commitments "
        "(user_id, source_ref, md5(content)) WHERE source_ref IS NOT NULL;"
    )
    op.execute(
        "CREATE INDEX ix_commitments_user_review_due ON commitments "
        "(user_id, review_state, status, due_at);"
    )
    op.execute(
        "COMMENT ON TABLE commitments IS 'Promises a person made, from every channel (ADR-0039 "
        "B6, promise ledger): meetings, phone recordings, Slack, email, manual adds and "
        "hand-offs. Written only by core-api: its agent-results consumer, the internal "
        "promise-capture route integration-service calls, and the owner. Private to user_id.';"
    )

    # --- promise_date_proposals ---------------------------------------------------------------
    op.execute("""
        CREATE TABLE promise_date_proposals (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          commitment_id  uuid        REFERENCES commitments(id) ON DELETE CASCADE,
          action_id      uuid        REFERENCES action_items(id) ON DELETE CASCADE,
          proposer_id    uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          requester_id   uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          content        text        NOT NULL,
          current_due    date,
          proposed_due   date        NOT NULL,
          note           text,
          state          text        NOT NULL DEFAULT 'pending',
          created_at     timestamptz NOT NULL DEFAULT now(),
          decided_at     timestamptz,
          CONSTRAINT pk_promise_date_proposals PRIMARY KEY (id),
          CONSTRAINT ck_promise_date_proposals_target
            CHECK ((commitment_id IS NULL) <> (action_id IS NULL)),
          CONSTRAINT ck_promise_date_proposals_state
            CHECK (state IN ('pending', 'accepted', 'declined', 'withdrawn')),
          CONSTRAINT ck_promise_date_proposals_people CHECK (proposer_id <> requester_id),
          CONSTRAINT ck_promise_date_proposals_content
            CHECK (char_length(content) BETWEEN 1 AND 500),
          CONSTRAINT ck_promise_date_proposals_note
            CHECK (note IS NULL OR char_length(note) <= 300)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_promise_date_proposals_pending_commitment "
        "ON promise_date_proposals (commitment_id) WHERE state = 'pending';"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_promise_date_proposals_pending_action "
        "ON promise_date_proposals (action_id) WHERE state = 'pending';"
    )
    op.execute(
        "CREATE INDEX ix_promise_date_proposals_requester "
        "ON promise_date_proposals (requester_id, state, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_promise_date_proposals_proposer "
        "ON promise_date_proposals (proposer_id, state, created_at DESC);"
    )
    _rls(
        "promise_date_proposals",
        "A request to move a promise or action due date, from the person who owes it to the "
        "person who asked for it (promise ledger capacity check). Written only by core-api.",
    )

    # --- promise_capture_preferences ----------------------------------------------------------
    op.execute("""
        CREATE TABLE promise_capture_preferences (
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          slack_enabled  boolean     NOT NULL DEFAULT true,
          email_enabled  boolean     NOT NULL DEFAULT true,
          updated_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_promise_capture_preferences PRIMARY KEY (user_id)
        );
        """)
    _rls(
        "promise_capture_preferences",
        "Per person: may their agent look for promises in their own Slack messages and email. "
        "Defaults (both on) when absent. Written only by core-api, by the owner.",
    )

    # --- meetings.calendar_event_kind ---------------------------------------------------------
    op.execute("""
        ALTER TABLE meetings
          ADD COLUMN calendar_event_kind text,
          ADD CONSTRAINT ck_meetings_calendar_event_kind
            CHECK (calendar_event_kind IS NULL OR char_length(calendar_event_kind) <= 40);
        """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE meetings
          DROP CONSTRAINT IF EXISTS ck_meetings_calendar_event_kind,
          DROP COLUMN IF EXISTS calendar_event_kind;
        """)
    op.execute("DROP TABLE IF EXISTS promise_capture_preferences CASCADE;")
    op.execute("DROP TABLE IF EXISTS promise_date_proposals CASCADE;")
    op.execute("DROP INDEX IF EXISTS ix_commitments_user_review_due;")
    op.execute("DROP INDEX IF EXISTS uq_commitments_source;")
    # Promises with no meeting cannot survive the NOT NULL they predate.
    op.execute("DELETE FROM commitments WHERE meeting_id IS NULL;")
    op.execute("""
        ALTER TABLE commitments
          DROP CONSTRAINT IF EXISTS ck_commitments_channel,
          DROP CONSTRAINT IF EXISTS ck_commitments_review_state,
          DROP CONSTRAINT IF EXISTS ck_commitments_meeting_channel,
          DROP CONSTRAINT IF EXISTS ck_commitments_recipient_label,
          DROP CONSTRAINT IF EXISTS ck_commitments_source_link,
          DROP CONSTRAINT IF EXISTS ck_commitments_source_quote,
          DROP CONSTRAINT IF EXISTS ck_commitments_source_ref,
          DROP CONSTRAINT IF EXISTS ck_commitments_confidence,
          DROP COLUMN IF EXISTS handed_off_from,
          DROP COLUMN IF EXISTS handed_off_to,
          DROP COLUMN IF EXISTS created_by,
          DROP COLUMN IF EXISTS confidence,
          DROP COLUMN IF EXISTS source_at,
          DROP COLUMN IF EXISTS source_ref,
          DROP COLUMN IF EXISTS source_quote,
          DROP COLUMN IF EXISTS source_link,
          DROP COLUMN IF EXISTS recipient_label,
          DROP COLUMN IF EXISTS recipient_user_id,
          DROP COLUMN IF EXISTS due_at,
          DROP COLUMN IF EXISTS review_state,
          DROP COLUMN IF EXISTS channel;
        """)
    op.execute("ALTER TABLE commitments ALTER COLUMN meeting_id SET NOT NULL;")
    op.execute(
        "COMMENT ON TABLE commitments IS 'Promises a person made in a meeting (ADR-0039 B6). "
        "Written only by core-api: its agent-results consumer (PersonalListenerAgent "
        "payload_json.commitments) and the owner''s PATCH. Private to user_id.';"
    )
