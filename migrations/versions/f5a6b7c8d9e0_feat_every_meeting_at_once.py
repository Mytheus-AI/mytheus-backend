"""feat: in every meeting at once (attendance choices, live interruptions, evening digest)

"Beyond the meeting", Layer 3: "In every meeting at once". A person marks each meeting they are
invited to as attend, my agent listens or skip (by hand or by a standing rule); while a meeting
they are not in is live, their agent watches it and interrupts them only when something needs
them; by evening one paragraph covers everything it heard. Every table is written only by
core-api.

- `meeting_participants.attendance_choice` (`attend` | `agent_listens` | `skip`, NULL = no
  choice yet, which reads as attend), `attendance_choice_source` (`manual` | `rule`, NULL with
  no choice), `attendance_rule_id` (the rule that chose it) and `attendance_choice_at`. The
  published `attendance` column (`live` | `agent`, ADR-0039 B5) stays the one the agents read:
  core-api keeps it in step (`agent_listens` is `agent`, anything else `live`).
- `attendance_rules`: a person's standing rules ("let my agent listen to every recurring status
  meeting"), first match by `position` wins, a manual choice always beats a rule. A rule matches
  on any of `title_contains`, `template_id`, `recurring_only`, and never meetings the person
  hosts unless `include_hosted`.
- `listening_preferences`: one row per person: interruptions on/off, at most `max_per_hour`
  (1-12), quiet hours, focus blocks (weekly windows), "never interrupt me during focus blocks",
  the evening digest on/off and its local hour, and the IANA time zone those are read in.
- `standing_positions`: "the line you gave me", short pre-approved statements (up to 300
  characters, 25 live per person) the agent may put forward when asked to hold the line.
- `agent_interruptions`: every interruption the agent raised, delivered or held (quiet hours,
  a focus block, throttled), what it was about, and what the person did with it.
  `UNIQUE (user_id, idempotency_key)`: a replayed window raises nothing twice.
- `meeting_views`: "Sushan's view" and "holding the line" notes posted into a live meeting:
  the host sees them first (`pending`) and shares them with the room (`shared`) or not
  (`dismissed`).
- `evening_digests`: one paragraph per person per local day about the meetings their agent
  listened to for them, as linked segments. `UNIQUE (user_id, day)`.

Tenant-scoped like every core-api table: ENABLE + FORCE RLS with the canonical NULLIF
`tenant_isolation` policy and the explicit `grant_tenant_table` (ADR-0021).

Revision ID: f5a6b7c8d9e0
Revises: c2d3e4f5a6b7
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "f5a6b7c8d9e0"
down_revision: Union[str, None] = "c2d3e4f5a6b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

_TABLES = (
    "attendance_rules",
    "listening_preferences",
    "standing_positions",
    "agent_interruptions",
    "meeting_views",
    "evening_digests",
)


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
    # --- attendance_rules -----------------------------------------------------------------------
    op.execute("""
        CREATE TABLE attendance_rules (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          choice          text        NOT NULL,
          title_contains  text,
          template_id     uuid        REFERENCES meeting_templates(id) ON DELETE CASCADE,
          recurring_only  boolean     NOT NULL DEFAULT false,
          include_hosted  boolean     NOT NULL DEFAULT false,
          position        integer     NOT NULL DEFAULT 0,
          created_at      timestamptz NOT NULL DEFAULT now(),
          updated_at      timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_attendance_rules PRIMARY KEY (id),
          CONSTRAINT ck_attendance_rules_choice
            CHECK (choice IN ('attend', 'agent_listens', 'skip')),
          CONSTRAINT ck_attendance_rules_title
            CHECK (title_contains IS NULL OR char_length(title_contains) BETWEEN 2 AND 80),
          CONSTRAINT ck_attendance_rules_matches_something
            CHECK (title_contains IS NOT NULL OR template_id IS NOT NULL OR recurring_only),
          CONSTRAINT ck_attendance_rules_position CHECK (position BETWEEN 0 AND 99)
        );
        """)
    op.execute("CREATE INDEX ix_attendance_rules_user ON attendance_rules (user_id, position);")
    _rls(
        "attendance_rules",
        "A person''s standing attendance rules (attend, my agent listens, skip). Written only "
        "by core-api.",
    )

    # --- meeting_participants: the per-meeting choice -------------------------------------------
    op.execute("""
        ALTER TABLE meeting_participants
          ADD COLUMN attendance_choice text,
          ADD COLUMN attendance_choice_source text,
          ADD COLUMN attendance_rule_id uuid
            REFERENCES attendance_rules(id) ON DELETE SET NULL,
          ADD COLUMN attendance_choice_at timestamptz,
          ADD CONSTRAINT ck_meeting_participants_attendance_choice
            CHECK (attendance_choice IS NULL
                   OR attendance_choice IN ('attend', 'agent_listens', 'skip')),
          ADD CONSTRAINT ck_meeting_participants_attendance_choice_source
            CHECK ((attendance_choice IS NULL) = (attendance_choice_source IS NULL)
                   AND (attendance_choice_source IS NULL
                        OR attendance_choice_source IN ('manual', 'rule')));
        """)
    # Everyone who already sent their agent (ADR-0039 B5) chose it by hand.
    op.execute(
        "UPDATE meeting_participants SET attendance_choice = 'agent_listens', "
        "attendance_choice_source = 'manual', attendance_choice_at = now() "
        "WHERE attendance = 'agent';"
    )

    # --- listening_preferences ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE listening_preferences (
          user_id              uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          org_id               uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          interruptions_enabled boolean    NOT NULL DEFAULT true,
          max_per_hour         integer     NOT NULL DEFAULT 3,
          quiet_start          time,
          quiet_end            time,
          focus_blocks         jsonb       NOT NULL DEFAULT '[]'::jsonb,
          never_during_focus   boolean     NOT NULL DEFAULT true,
          digest_enabled       boolean     NOT NULL DEFAULT true,
          digest_hour          integer     NOT NULL DEFAULT 18,
          time_zone            text        NOT NULL DEFAULT 'UTC',
          created_at           timestamptz NOT NULL DEFAULT now(),
          updated_at           timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_listening_preferences PRIMARY KEY (user_id),
          CONSTRAINT ck_listening_preferences_max CHECK (max_per_hour BETWEEN 1 AND 12),
          CONSTRAINT ck_listening_preferences_quiet
            CHECK ((quiet_start IS NULL) = (quiet_end IS NULL)),
          CONSTRAINT ck_listening_preferences_focus CHECK (jsonb_typeof(focus_blocks) = 'array'),
          CONSTRAINT ck_listening_preferences_hour CHECK (digest_hour BETWEEN 0 AND 23),
          CONSTRAINT ck_listening_preferences_tz CHECK (char_length(time_zone) BETWEEN 1 AND 64)
        );
        """)
    _rls(
        "listening_preferences",
        "When a person''s agent may interrupt them, and their evening digest. Written only by "
        "core-api.",
    )

    # --- standing_positions ---------------------------------------------------------------------
    op.execute("""
        CREATE TABLE standing_positions (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          topic       text,
          text        text        NOT NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now(),
          deleted_at  timestamptz,
          CONSTRAINT pk_standing_positions PRIMARY KEY (id),
          CONSTRAINT ck_standing_positions_text CHECK (char_length(text) BETWEEN 1 AND 300),
          CONSTRAINT ck_standing_positions_topic
            CHECK (topic IS NULL OR char_length(topic) BETWEEN 1 AND 80)
        );
        """)
    op.execute(
        "CREATE INDEX ix_standing_positions_user ON standing_positions (user_id) "
        "WHERE deleted_at IS NULL;"
    )
    _rls(
        "standing_positions",
        "Statements a person pre-approved for their agent to put forward when it holds the "
        "line for them. Written only by core-api.",
    )

    # --- agent_interruptions --------------------------------------------------------------------
    op.execute("""
        CREATE TABLE agent_interruptions (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          meeting_id       uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          idempotency_key  text        NOT NULL,
          reason           text        NOT NULL,
          headline         text        NOT NULL,
          detail           text,
          topic            text,
          quote            text,
          start_ms         integer,
          end_ms           integer,
          confidence       real        NOT NULL,
          prompt_version   text,
          status           text        NOT NULL,
          held_reason      text,
          response         text,
          responded_at     timestamptz,
          created_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_interruptions PRIMARY KEY (id),
          CONSTRAINT uq_agent_interruptions_user_key UNIQUE (user_id, idempotency_key),
          CONSTRAINT ck_agent_interruptions_reason
            CHECK (reason IN ('decision', 'question', 'promise')),
          CONSTRAINT ck_agent_interruptions_status CHECK (status IN ('delivered', 'held')),
          CONSTRAINT ck_agent_interruptions_held
            CHECK ((status = 'held') = (held_reason IS NOT NULL)),
          CONSTRAINT ck_agent_interruptions_response CHECK (response IS NULL OR response IN
            ('joined', 'view_sent', 'holding_line', 'dismissed')),
          CONSTRAINT ck_agent_interruptions_headline CHECK (char_length(headline) BETWEEN 1 AND 240),
          CONSTRAINT ck_agent_interruptions_confidence CHECK (confidence BETWEEN 0 AND 1)
        );
        """)
    op.execute(
        "CREATE INDEX ix_agent_interruptions_user ON agent_interruptions (user_id, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_agent_interruptions_meeting ON agent_interruptions (meeting_id, user_id);"
    )
    _rls(
        "agent_interruptions",
        "Every time a person''s agent interrupted them about a live meeting they were not in, or "
        "held it for later. Written only by core-api.",
    )

    # --- meeting_views --------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_views (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id       uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          interruption_id  uuid        REFERENCES agent_interruptions(id) ON DELETE SET NULL,
          kind             text        NOT NULL,
          text             text        NOT NULL,
          position_ids     uuid[]      NOT NULL DEFAULT '{}',
          status           text        NOT NULL DEFAULT 'pending',
          decided_by       uuid        REFERENCES users(id) ON DELETE SET NULL,
          decided_at       timestamptz,
          created_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_views PRIMARY KEY (id),
          CONSTRAINT ck_meeting_views_kind CHECK (kind IN ('view', 'hold_line')),
          CONSTRAINT ck_meeting_views_status CHECK (status IN ('pending', 'shared', 'dismissed')),
          CONSTRAINT ck_meeting_views_text CHECK (char_length(text) BETWEEN 1 AND 2000)
        );
        """)
    op.execute("CREATE INDEX ix_meeting_views_meeting ON meeting_views (meeting_id, created_at);")
    _rls(
        "meeting_views",
        "A view or a held line a person who was not in a live meeting posted into it: the host "
        "sees it first, then shares it with the room. Written only by core-api.",
    )

    # --- evening_digests ------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE evening_digests (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          day            date        NOT NULL,
          content        jsonb       NOT NULL,
          meeting_count  integer     NOT NULL DEFAULT 0,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_evening_digests PRIMARY KEY (id),
          CONSTRAINT uq_evening_digests_user_day UNIQUE (user_id, day),
          CONSTRAINT ck_evening_digests_content CHECK (jsonb_typeof(content) = 'object')
        );
        """)
    _rls(
        "evening_digests",
        "One paragraph per person per day about the meetings their agent listened to for them. "
        "Written only by core-api.",
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS evening_digests;")
    op.execute("DROP TABLE IF EXISTS meeting_views;")
    op.execute("DROP TABLE IF EXISTS agent_interruptions;")
    op.execute("DROP TABLE IF EXISTS standing_positions;")
    op.execute("DROP TABLE IF EXISTS listening_preferences;")
    op.execute("""
        ALTER TABLE meeting_participants
          DROP CONSTRAINT IF EXISTS ck_meeting_participants_attendance_choice_source,
          DROP CONSTRAINT IF EXISTS ck_meeting_participants_attendance_choice,
          DROP COLUMN IF EXISTS attendance_choice_at,
          DROP COLUMN IF EXISTS attendance_rule_id,
          DROP COLUMN IF EXISTS attendance_choice_source,
          DROP COLUMN IF EXISTS attendance_choice;
        """)
    op.execute("DROP TABLE IF EXISTS attendance_rules;")
