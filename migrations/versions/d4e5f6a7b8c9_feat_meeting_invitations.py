"""feat: meeting email invitations (ADR-0043)

A host (or the meeting's creator, or a workspace admin) can send a meeting by email: a branded
invitation with a Join button, "Add to calendar" links and an RFC 5545 `.ics` attached, so
Gmail and Outlook show their own RSVP card. When the time changes or the meeting is cancelled,
everyone who was emailed gets an update (SEQUENCE + 1) or a cancellation (METHOD:CANCEL).

- `meeting_invitations` (core-api): one row per meeting and address, the latest delivery state
  (`pending`, `sent`, `failed` with `last_error`, `not_configured` when the server has no email
  provider), how many times it was sent, who sent it and the optional personal note. Tenant
  scoped: `org_id`, RLS ENABLE + FORCE, the canonical `tenant_isolation` policy, explicit grants.
  No token column: the calendar link is a stateless HMAC token over (org_id, meeting_id), so a
  row never stores a credential.
- `meetings.calendar_sequence`: the iCalendar SEQUENCE, bumped by core-api whenever the
  scheduled time changes or the meeting is cancelled, so a calendar replaces the event instead
  of adding a second one.
- `email_deliveries.kind` (notification-service's own table) also allows the three invitation
  kinds; only its CHECK widens.

Revision ID: d4e5f6a7b8c9
Revises: c8d9e0f1a2b3 (departments/teams description)
Create Date: 2026-11-12
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "c8d9e0f1a2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_KINDS_BEFORE = "'meeting_followup', 'weekly_digest'"
_KINDS_AFTER = (
    "'meeting_followup', 'weekly_digest', 'meeting_invitation', 'meeting_update', "
    "'meeting_cancel'"
)


def upgrade() -> None:
    op.execute("""
        CREATE TABLE meeting_invitations (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id  uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          email       text        NOT NULL,
          user_id     uuid        REFERENCES users(id) ON DELETE SET NULL,
          invited_by  uuid        REFERENCES users(id) ON DELETE SET NULL,
          message     text,
          status      text        NOT NULL DEFAULT 'pending',
          last_error  text,
          last_kind   text        NOT NULL DEFAULT 'invite',
          send_count  integer     NOT NULL DEFAULT 0,
          sent_at     timestamptz,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_invitations PRIMARY KEY (id),
          CONSTRAINT uq_meeting_invitations_meeting_email UNIQUE (meeting_id, email),
          CONSTRAINT ck_meeting_invitations_status
            CHECK (status IN ('pending', 'sent', 'failed', 'not_configured')),
          CONSTRAINT ck_meeting_invitations_kind CHECK (last_kind IN ('invite', 'update', 'cancel')),
          CONSTRAINT ck_meeting_invitations_email CHECK (char_length(email) BETWEEN 3 AND 320),
          CONSTRAINT ck_meeting_invitations_message
            CHECK (message IS NULL OR char_length(message) <= 500),
          CONSTRAINT ck_meeting_invitations_error
            CHECK (last_error IS NULL OR char_length(last_error) <= 200),
          CONSTRAINT ck_meeting_invitations_sends CHECK (send_count >= 0)
        );
        """)
    op.execute("CREATE INDEX ix_meeting_invitations_org_id ON meeting_invitations (org_id);")
    op.execute(
        "CREATE INDEX ix_meeting_invitations_meeting ON meeting_invitations "
        "(meeting_id, created_at);"
    )
    op.execute("ALTER TABLE meeting_invitations ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE meeting_invitations FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON meeting_invitations "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "meeting_invitations")
    op.execute(
        "COMMENT ON TABLE meeting_invitations IS 'Meeting invitations sent by email (ADR-0043): "
        "one row per meeting and address with the latest delivery state. Written only by "
        "core-api.';"
    )

    op.execute(
        "ALTER TABLE meetings ADD COLUMN calendar_sequence integer NOT NULL DEFAULT 0, "
        "ADD CONSTRAINT ck_meetings_calendar_sequence CHECK (calendar_sequence >= 0);"
    )
    op.execute(
        "COMMENT ON COLUMN meetings.calendar_sequence IS 'iCalendar SEQUENCE (ADR-0043): bumped "
        "when the scheduled time changes or the meeting is cancelled.';"
    )

    op.execute(f"""
        ALTER TABLE email_deliveries
          DROP CONSTRAINT ck_email_deliveries_kind,
          ADD CONSTRAINT ck_email_deliveries_kind CHECK (kind IN ({_KINDS_AFTER}));
        """)


def downgrade() -> None:
    op.execute(
        "DELETE FROM email_deliveries WHERE kind IN "
        "('meeting_invitation', 'meeting_update', 'meeting_cancel');"
    )
    op.execute(f"""
        ALTER TABLE email_deliveries
          DROP CONSTRAINT ck_email_deliveries_kind,
          ADD CONSTRAINT ck_email_deliveries_kind CHECK (kind IN ({_KINDS_BEFORE}));
        """)
    op.execute("ALTER TABLE meetings DROP CONSTRAINT IF EXISTS ck_meetings_calendar_sequence;")
    op.execute("ALTER TABLE meetings DROP COLUMN IF EXISTS calendar_sequence;")
    op.execute("DROP TABLE IF EXISTS meeting_invitations CASCADE;")
