"""feat: room signals (the last word, the agreement check, pick up where we stopped)

"After the meeting" layer 1. Every table is written only by core-api; all are tenant tables
(org_id, RLS tenant_isolation FORCE, grant_tenant_table).

- `room_signals`: the "didn't get in" moments the room itself saw, filed from the
  realtime-gateway's Redis list `nexus:{org_id}:room-signals:{meeting_id}`: an unmute that was
  spoken over (`spoken_over`) and a raised hand that was never called (`hand_not_called`). The
  gateway's own uuid is the primary key, so filing the list twice files nothing twice. Private
  raw material for the analyzer; never served to anyone.
- `room_signal_runs`: one row per meeting once the MeetingAgent's `room_signals` result was
  filed (`analyzed` on the after page), with its generation and status.
- `last_word_nudges`: at most one per (meeting, person): "You started to say something about
  pricing. Want to add it?" Private to its person. `open` until they add a point
  (`contributed`) or say there is nothing to add (`dismissed`).
- `late_contributions`: the point someone added after the meeting, written (`body`) and/or
  recorded (`audio`, at most 2.5 MB, its mime and length), tied to the moment (`at_ms`) and the
  decision it is about. Visible to the meeting's participants. Soft-deleted by its author.
- `decision_agreement_reads`: the analyzer's read of how strongly a decision was agreed, one row
  per decision: the strength and the in-meeting counts only, never who said what.
- `agreement_checks`: one private question per (decision, person) asked because they hedged,
  stayed silent or were talked over: their answer (support | concerns | object), an optional
  note, and whether they put their name to it (anonymous by default). Only the person reads
  their row; the decision owner gets aggregates through the API.
- `resume_points`: at most one open thread per meeting (time ran out, an agenda item unfinished,
  someone cut off mid-explanation): the topic, the last two positions with quotes and the person
  who was cut off. The next meeting in the series pins it until someone continues or the host
  sets it aside (`resolved_in_meeting_id`: the meeting where either happened).

Revision ID: b1c2d3e4f5a6
Revises: e7f8a9b0c1d2
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "b1c2d3e4f5a6"
down_revision: Union[str, None] = "e7f8a9b0c1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_TABLES = (
    "room_signals",
    "room_signal_runs",
    "last_word_nudges",
    "late_contributions",
    "decision_agreement_reads",
    "agreement_checks",
    "resume_points",
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
    op.execute("""
        CREATE TABLE room_signals (
          id           uuid        NOT NULL,
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          kind         text        NOT NULL,
          occurred_at  timestamptz NOT NULL,
          ended_at     timestamptz NOT NULL,
          detail       jsonb       NOT NULL DEFAULT '{}'::jsonb,
          created_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_room_signals PRIMARY KEY (id),
          CONSTRAINT ck_room_signals_kind CHECK (kind IN ('spoken_over', 'hand_not_called')),
          CONSTRAINT ck_room_signals_span CHECK (ended_at >= occurred_at)
        );
        """)
    op.execute("CREATE INDEX ix_room_signals_meeting ON room_signals (meeting_id, occurred_at);")
    _rls(
        "room_signals",
        "Moments the room saw someone not get in (spoken over, a hand never called), filed "
        "from the realtime-gateway Redis list. Private; written only by core-api.",
    )

    op.execute("""
        CREATE TABLE room_signal_runs (
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          generation   text        NOT NULL,
          status       text        NOT NULL,
          analyzed_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_room_signal_runs PRIMARY KEY (meeting_id),
          CONSTRAINT ck_room_signal_runs_status CHECK (status IN ('ready', 'partial'))
        );
        """)
    _rls(
        "room_signal_runs",
        "One row per meeting once the MeetingAgent room_signals result was filed. Written only "
        "by core-api.",
    )

    op.execute("""
        CREATE TABLE last_word_nudges (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          kind         text        NOT NULL,
          topic        text        NOT NULL,
          quote        text,
          at_ms        bigint,
          end_ms       bigint,
          decision_id  uuid        REFERENCES decisions(id) ON DELETE SET NULL,
          signal_ids   uuid[]      NOT NULL DEFAULT '{}',
          status       text        NOT NULL DEFAULT 'open',
          generation   text        NOT NULL,
          created_at   timestamptz NOT NULL DEFAULT now(),
          resolved_at  timestamptz,
          CONSTRAINT pk_last_word_nudges PRIMARY KEY (id),
          CONSTRAINT uq_last_word_nudges_meeting_user UNIQUE (meeting_id, user_id),
          CONSTRAINT ck_last_word_nudges_kind
            CHECK (kind IN ('spoken_over', 'hand_not_called', 'cut_off', 'topic_silent')),
          CONSTRAINT ck_last_word_nudges_status CHECK (status IN ('open', 'contributed', 'dismissed')),
          CONSTRAINT ck_last_word_nudges_topic CHECK (char_length(topic) BETWEEN 1 AND 80),
          CONSTRAINT ck_last_word_nudges_quote CHECK (quote IS NULL OR char_length(quote) <= 300),
          CONSTRAINT ck_last_word_nudges_resolved CHECK ((status = 'open') = (resolved_at IS NULL))
        );
        """)
    op.execute(
        "CREATE INDEX ix_last_word_nudges_user_status "
        "ON last_word_nudges (user_id, status, created_at DESC);"
    )
    _rls(
        "last_word_nudges",
        "A private nudge to add the point someone did not get to make in a meeting. Written "
        "only by core-api.",
    )

    op.execute("""
        CREATE TABLE late_contributions (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id        uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id           uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          nudge_id          uuid        REFERENCES last_word_nudges(id) ON DELETE SET NULL,
          decision_id       uuid        REFERENCES decisions(id) ON DELETE SET NULL,
          at_ms             bigint,
          body              text,
          audio             bytea,
          audio_mime        text,
          audio_duration_ms integer,
          created_at        timestamptz NOT NULL DEFAULT now(),
          deleted_at        timestamptz,
          CONSTRAINT pk_late_contributions PRIMARY KEY (id),
          CONSTRAINT ck_late_contributions_content CHECK (body IS NOT NULL OR audio IS NOT NULL),
          CONSTRAINT ck_late_contributions_body
            CHECK (body IS NULL OR char_length(body) BETWEEN 1 AND 2000),
          CONSTRAINT ck_late_contributions_audio
            CHECK (audio IS NULL OR (octet_length(audio) <= 2621440 AND audio_mime IN
              ('audio/webm', 'audio/ogg', 'audio/mp4', 'audio/mpeg')
              AND audio_duration_ms BETWEEN 1 AND 180000))
        );
        """)
    op.execute(
        "CREATE INDEX ix_late_contributions_meeting "
        "ON late_contributions (meeting_id, created_at) WHERE deleted_at IS NULL;"
    )
    op.execute(
        "CREATE INDEX ix_late_contributions_decision "
        "ON late_contributions (decision_id) WHERE deleted_at IS NULL;"
    )
    _rls(
        "late_contributions",
        "A point added after a meeting (written or recorded), tied to its moment and "
        "decision. Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE decision_agreement_reads (
          decision_id    uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id     uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          strength       text        NOT NULL,
          participants   integer     NOT NULL,
          yes_count      integer     NOT NULL,
          hedged_count   integer     NOT NULL,
          silent_count   integer     NOT NULL,
          objected_count integer     NOT NULL,
          talked_over_count integer  NOT NULL,
          generation     text        NOT NULL,
          analyzed_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_agreement_reads PRIMARY KEY (decision_id),
          CONSTRAINT ck_decision_agreement_reads_strength
            CHECK (strength IN ('strong', 'mixed', 'weak', 'unclear')),
          CONSTRAINT ck_decision_agreement_reads_counts CHECK (
            participants >= 0 AND yes_count >= 0 AND hedged_count >= 0 AND silent_count >= 0
            AND objected_count >= 0 AND talked_over_count >= 0
            AND yes_count + hedged_count + silent_count + objected_count + talked_over_count
                <= participants)
        );
        """)
    _rls(
        "decision_agreement_reads",
        "How strongly a decision was agreed in its meeting: the strength and counts only, "
        "never who said what. Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE agreement_checks (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id  uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          reason       text        NOT NULL,
          status       text        NOT NULL DEFAULT 'asked',
          answer       text,
          note         text,
          share_name   boolean     NOT NULL DEFAULT false,
          answered_at  timestamptz,
          created_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agreement_checks PRIMARY KEY (id),
          CONSTRAINT uq_agreement_checks_decision_user UNIQUE (decision_id, user_id),
          CONSTRAINT ck_agreement_checks_reason CHECK (reason IN ('hedged', 'silent', 'talked_over')),
          CONSTRAINT ck_agreement_checks_status CHECK (status IN ('asked', 'answered')),
          CONSTRAINT ck_agreement_checks_answer
            CHECK (answer IS NULL OR answer IN ('support', 'concerns', 'object')),
          CONSTRAINT ck_agreement_checks_answered
            CHECK ((status = 'answered') = (answer IS NOT NULL AND answered_at IS NOT NULL)),
          CONSTRAINT ck_agreement_checks_note CHECK (note IS NULL OR char_length(note) BETWEEN 1 AND 1000)
        );
        """)
    op.execute(
        "CREATE INDEX ix_agreement_checks_user_status "
        "ON agreement_checks (user_id, status, created_at DESC);"
    )
    _rls(
        "agreement_checks",
        "A private question to someone who hedged, stayed silent or was talked over on a "
        "decision, and their answer (anonymous unless they add their name). Written only by "
        "core-api.",
    )

    op.execute("""
        CREATE TABLE resume_points (
          id                      uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id                  uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id              uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          reason                  text        NOT NULL,
          topic                   text        NOT NULL,
          agenda_item_id          uuid        REFERENCES meeting_agenda_items(id) ON DELETE SET NULL,
          positions               jsonb       NOT NULL DEFAULT '[]'::jsonb,
          cut_off_user_id         uuid        REFERENCES users(id) ON DELETE SET NULL,
          cut_off_quote           text,
          cut_off_start_ms        bigint,
          cut_off_end_ms          bigint,
          status                  text        NOT NULL DEFAULT 'open',
          continued_in_meeting_id uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          resolved_in_meeting_id  uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          resolved_by             uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at             timestamptz,
          generation              text        NOT NULL,
          created_at              timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_resume_points PRIMARY KEY (id),
          CONSTRAINT uq_resume_points_meeting UNIQUE (meeting_id),
          CONSTRAINT ck_resume_points_reason
            CHECK (reason IN ('time_ran_out', 'agenda_unfinished', 'cut_off')),
          CONSTRAINT ck_resume_points_status CHECK (status IN ('open', 'continued', 'dismissed')),
          CONSTRAINT ck_resume_points_topic CHECK (char_length(topic) BETWEEN 1 AND 120),
          CONSTRAINT ck_resume_points_quote
            CHECK (cut_off_quote IS NULL OR char_length(cut_off_quote) <= 300),
          CONSTRAINT ck_resume_points_resolved CHECK ((status = 'open') = (resolved_at IS NULL))
        );
        """)
    op.execute("CREATE INDEX ix_resume_points_status ON resume_points (status, created_at DESC);")
    _rls(
        "resume_points",
        "An open thread a meeting ended on, pinned in the next meeting of the series until "
        "someone continues it. Written only by core-api.",
    )


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table};")
