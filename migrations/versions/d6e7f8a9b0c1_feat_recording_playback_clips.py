"""feat: recording playback and clips

Polish program wave 3, "Recording playback": transcript-synced playback of the recordings
LiveKit's room-composite egress already writes to `mytheus-recordings`, with clips and clip
shares. Additive only.

- `meeting_recordings` (meeting-service, the bucket's owner): one row per egress, so a meeting
  that was paused for "off the record" (egress stopped, then started again) has one segment per
  stretch that was actually recorded. `status` moves `recording` -> `processing` (the room
  closed, LiveKit is finishing the upload) -> `ready` (the `egress_ended` webhook named the
  uploaded object) or `failed`. `object_key` is the object inside the bucket, always under the
  meeting's `{org_id}/{meeting_id}/` prefix; never a URL (playback is a short-lived presigned GET
  minted per request). `started_at` places the segment on the meeting's clock: the transcript's
  zero is `meeting_rooms.started_at`, so a segment's offset is `started_at - that`.
  UNIQUE (org_id, egress_id) makes a redelivered webhook an upsert of the same row.
- `meeting_rooms.recording_skip_reason`: why a room that could have been recorded was not
  (`consent_missing`: the consent gate refused it because someone had not agreed). Null
  otherwise. Lets the meeting page say "no recording because not everyone consented" instead of
  a bare "no recording".
- `meeting_clips` (core-api): a named span of a meeting (`start_ms`/`end_ms` on the meeting's
  clock), at most 10 minutes, made by a participant.
- `share_links` gains kind `meeting_clip`, `clip_id` (required exactly for that kind) and
  `include_video` (the clip page shows the video segment only when the sharer chose it; only a
  clip link may carry it).

Every new table: org_id, ENABLE + FORCE RLS with the canonical NULLIF `tenant_isolation` policy
and `grant_tenant_table` (ADR-0021).

Revision ID: d6e7f8a9b0c1
Revises: b4c5d6e7f8a9
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d6e7f8a9b0c1"
down_revision: Union[str, None] = "b4c5d6e7f8a9"
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
    # --- meeting_recordings (meeting-service) ---------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_recordings (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          egress_id    text        NOT NULL,
          status       text        NOT NULL DEFAULT 'recording',
          object_key   text,
          media_type   text        NOT NULL DEFAULT 'video/mp4',
          started_at   timestamptz NOT NULL DEFAULT now(),
          ended_at     timestamptz,
          duration_ms  integer,
          size_bytes   bigint,
          error        text,
          created_at   timestamptz NOT NULL DEFAULT now(),
          updated_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_recordings PRIMARY KEY (id),
          CONSTRAINT uq_meeting_recordings_org_egress UNIQUE (org_id, egress_id),
          CONSTRAINT ck_meeting_recordings_status
            CHECK (status IN ('recording', 'processing', 'ready', 'failed')),
          CONSTRAINT ck_meeting_recordings_media
            CHECK (media_type IN ('video/mp4', 'audio/ogg')),
          CONSTRAINT ck_meeting_recordings_ready_has_object
            CHECK (status <> 'ready' OR object_key IS NOT NULL),
          CONSTRAINT ck_meeting_recordings_duration
            CHECK (duration_ms IS NULL OR duration_ms >= 0)
        );
        """)
    op.execute(
        "CREATE INDEX ix_meeting_recordings_meeting ON meeting_recordings (meeting_id, started_at);"
    )
    _rls(
        "meeting_recordings",
        "One row per LiveKit egress of a meeting (a segment of its recording). Written only by "
        "meeting-service, which owns the mytheus-recordings bucket; object_key is never a URL.",
    )
    op.execute(
        "ALTER TABLE meeting_rooms ADD COLUMN recording_skip_reason text, "
        "ADD CONSTRAINT ck_meeting_rooms_recording_skip_reason "
        "CHECK (recording_skip_reason IS NULL OR recording_skip_reason IN ('consent_missing'));"
    )

    # --- meeting_clips (core-api) ---------------------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_clips (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id  uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          created_by  uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          title       text        NOT NULL,
          start_ms    integer     NOT NULL,
          end_ms      integer     NOT NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_clips PRIMARY KEY (id),
          CONSTRAINT ck_meeting_clips_title CHECK (char_length(title) BETWEEN 1 AND 200),
          CONSTRAINT ck_meeting_clips_span
            CHECK (start_ms >= 0 AND end_ms > start_ms AND end_ms - start_ms <= 600000)
        );
        """)
    op.execute("CREATE INDEX ix_meeting_clips_meeting ON meeting_clips (meeting_id, start_ms);")
    _rls(
        "meeting_clips",
        "A named span (at most 10 minutes) of a meeting on its own clock, made by a participant. "
        "Written only by core-api.",
    )

    # --- share_links: clip shares ---------------------------------------------------------------
    op.execute("""
        ALTER TABLE share_links
          ADD COLUMN clip_id uuid REFERENCES meeting_clips(id) ON DELETE CASCADE,
          ADD COLUMN include_video boolean NOT NULL DEFAULT false,
          DROP CONSTRAINT ck_share_links_kind,
          ADD CONSTRAINT ck_share_links_kind
            CHECK (kind IN ('meeting_recap', 'decision_record', 'meeting_clip')),
          ADD CONSTRAINT ck_share_links_clip CHECK ((kind = 'meeting_clip') = (clip_id IS NOT NULL)),
          ADD CONSTRAINT ck_share_links_video CHECK (NOT include_video OR kind = 'meeting_clip');
        """)
    op.execute("CREATE INDEX ix_share_links_clip ON share_links (clip_id) WHERE clip_id IS NOT NULL;")


def downgrade() -> None:
    op.execute("DELETE FROM share_links WHERE kind = 'meeting_clip';")
    op.execute("DROP INDEX IF EXISTS ix_share_links_clip;")
    op.execute("""
        ALTER TABLE share_links
          DROP CONSTRAINT IF EXISTS ck_share_links_video,
          DROP CONSTRAINT IF EXISTS ck_share_links_clip,
          DROP CONSTRAINT ck_share_links_kind,
          ADD CONSTRAINT ck_share_links_kind CHECK (kind IN ('meeting_recap', 'decision_record')),
          DROP COLUMN IF EXISTS include_video,
          DROP COLUMN IF EXISTS clip_id;
        """)
    op.execute("DROP TABLE IF EXISTS meeting_clips;")
    op.execute(
        "ALTER TABLE meeting_rooms DROP CONSTRAINT IF EXISTS ck_meeting_rooms_recording_skip_reason, "
        "DROP COLUMN IF EXISTS recording_skip_reason;"
    )
    op.execute("DROP TABLE IF EXISTS meeting_recordings;")
