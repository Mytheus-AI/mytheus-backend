"""feat: agent_briefings and briefing_preferences (a briefing from your agent)

"Beyond the meeting", Layer 1. After a meeting ends (and whenever a person asks, "Brief me"),
each participant gets a personal briefing of about 90 seconds, written by their own agent in its
persona's voice: what changed for them, who is waiting on them, their promises and the questions
left open (including the ones carried to the next agenda). core-api is the one writer of both
tables; `agent-runtime-service` writes the script and posts it back over core-api's internal
route.

`agent_briefings`, one row per briefing:
- `trigger`: `meeting_end` (created when the person's own minutes of a meeting land; at most one
  per (meeting, person), `uq_agent_briefings_meeting_end`) or `on_demand` ("Brief me").
- `status`: `pending` (waiting for the agent), `composing` (the agent took it), `ready`, `empty`
  (nothing worth saying) or `unavailable` (`reason`: budget, model_error, timeout).
- `content`: the script as segments (`{segments: [{id, kind, text, link}], word_count,
  duration_s}`), validated and clipped by core-api; `agent`: the persona it was written in
  (name, doodle seed, personality) at the time, so an old briefing keeps the face it was
  spoken with.
- `length` and `language`: what the person's preferences said when it was asked for.
- `audio_*`: the server-side voice (TTS_PROVIDER, not connected by default). `audio_status`
  `none` (no provider: the apps speak the script on the device), `pending`, `ready` (an object
  under `{org_id}/briefings/{user_id}/` in the briefing audio bucket) or `failed`.
- `rung_at` / `claimed_at` / `attempts`: the hand-off to the agent (a Redis doorbell plus a
  re-ring when a read finds it waiting), bounded to three attempts.
- Private to the person: every route serves a row only to its `user_id`.

`briefing_preferences`, one row per person who changed a default: on or off (default on),
auto-play (default off), length (`short` about 45 s, `standard` about 90 s), language (one of the
caption-translation languages, default English), the device voice they picked (`voice`, a voice
name as the device reports it), the server voice (`server_voice`, a provider voice id) and the
speaking rate.

RLS FORCE `tenant_isolation` on both, like their siblings.

Revision ID: d3e4f5a6b7c8
Revises: b1c2d3e4f5a6
Create Date: 2026-11-11
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d3e4f5a6b7c8"
down_revision: str | None = "b1c2d3e4f5a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute("""
        CREATE TABLE agent_briefings (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          meeting_id     uuid        REFERENCES meetings(id) ON DELETE CASCADE,
          trigger        text        NOT NULL,
          status         text        NOT NULL DEFAULT 'pending',
          reason         text,
          length         text        NOT NULL DEFAULT 'standard',
          language       text        NOT NULL DEFAULT 'en',
          content        jsonb       NOT NULL DEFAULT '{}'::jsonb,
          agent          jsonb       NOT NULL DEFAULT '{}'::jsonb,
          prompt_version text,
          attempts       integer     NOT NULL DEFAULT 0,
          rung_at        timestamptz,
          claimed_at     timestamptz,
          ready_at       timestamptz,
          audio_status   text        NOT NULL DEFAULT 'none',
          audio_key      text,
          audio_mime     text,
          audio_bytes    integer,
          audio_voice    text,
          notified_at    timestamptz,
          listened_at    timestamptz,
          created_at     timestamptz NOT NULL DEFAULT now(),
          updated_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_briefings PRIMARY KEY (id),
          CONSTRAINT ck_agent_briefings_trigger CHECK (trigger IN ('meeting_end', 'on_demand')),
          CONSTRAINT ck_agent_briefings_meeting_end
            CHECK (trigger <> 'meeting_end' OR meeting_id IS NOT NULL),
          CONSTRAINT ck_agent_briefings_status
            CHECK (status IN ('pending', 'composing', 'ready', 'empty', 'unavailable')),
          CONSTRAINT ck_agent_briefings_reason
            CHECK (reason IS NULL OR reason IN ('budget', 'model_error', 'timeout')),
          CONSTRAINT ck_agent_briefings_length CHECK (length IN ('short', 'standard')),
          CONSTRAINT ck_agent_briefings_language CHECK (char_length(language) BETWEEN 2 AND 8),
          CONSTRAINT ck_agent_briefings_content CHECK (jsonb_typeof(content) = 'object'),
          CONSTRAINT ck_agent_briefings_agent CHECK (jsonb_typeof(agent) = 'object'),
          CONSTRAINT ck_agent_briefings_prompt
            CHECK (prompt_version IS NULL OR char_length(prompt_version) BETWEEN 1 AND 64),
          CONSTRAINT ck_agent_briefings_attempts CHECK (attempts BETWEEN 0 AND 10),
          CONSTRAINT ck_agent_briefings_audio_status
            CHECK (audio_status IN ('none', 'pending', 'ready', 'failed')),
          CONSTRAINT ck_agent_briefings_audio_key
            CHECK (audio_status <> 'ready' OR audio_key IS NOT NULL)
        );
        """)
    op.execute("CREATE INDEX ix_agent_briefings_org_id ON agent_briefings (org_id);")
    op.execute(
        "CREATE INDEX ix_agent_briefings_user ON agent_briefings (user_id, created_at DESC);"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_agent_briefings_meeting_end ON agent_briefings "
        "(meeting_id, user_id) WHERE trigger = 'meeting_end';"
    )
    _rls("agent_briefings")
    op.execute(
        "COMMENT ON TABLE agent_briefings IS 'A briefing from your agent: a personal spoken "
        "summary after a meeting or on demand, private to user_id. Written only by core-api; "
        "the script comes from agent-runtime-service.';"
    )

    op.execute("""
        CREATE TABLE briefing_preferences (
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          enabled      boolean     NOT NULL DEFAULT true,
          auto_play    boolean     NOT NULL DEFAULT false,
          length       text        NOT NULL DEFAULT 'standard',
          language     text        NOT NULL DEFAULT 'en',
          voice        text,
          server_voice text,
          rate         real        NOT NULL DEFAULT 1.0,
          updated_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_briefing_preferences PRIMARY KEY (user_id),
          CONSTRAINT ck_briefing_preferences_length CHECK (length IN ('short', 'standard')),
          CONSTRAINT ck_briefing_preferences_language
            CHECK (char_length(language) BETWEEN 2 AND 8),
          CONSTRAINT ck_briefing_preferences_voice
            CHECK (voice IS NULL OR char_length(voice) BETWEEN 1 AND 200),
          CONSTRAINT ck_briefing_preferences_server_voice
            CHECK (server_voice IS NULL OR char_length(server_voice) BETWEEN 1 AND 100),
          CONSTRAINT ck_briefing_preferences_rate CHECK (rate >= 0.5 AND rate <= 2.0)
        );
        """)
    op.execute("CREATE INDEX ix_briefing_preferences_org_id ON briefing_preferences (org_id);")
    _rls("briefing_preferences")
    op.execute(
        "COMMENT ON TABLE briefing_preferences IS 'Per person: whether their agent briefs them, "
        "auto-play, length, language and voice. Written only by core-api.';"
    )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("DROP TABLE IF EXISTS briefing_preferences CASCADE;")
    op.execute("DROP TABLE IF EXISTS agent_briefings CASCADE;")
