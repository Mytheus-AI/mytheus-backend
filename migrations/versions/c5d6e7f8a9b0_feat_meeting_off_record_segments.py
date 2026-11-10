"""feat: meeting_off_record_segments (Off the record)

Polish program wave 3, "Only Mytheus can do these": Off the record. The host (or the meeting's
creator) can pause every agent and all transcript persistence for a stretch of the meeting; any
participant can ask them to. Each stretch is one row here, written only by core-api:

- `started_at` / `started_by`: when and by whom it began; `requested_by` names the participant
  whose request the host granted (null when the host started it unprompted).
- `ended_at` / `ended_by`: null while it runs. A meeting that ends closes its open segment
  (`ended_by` null: the meeting ending closed it).
- `start_ms` / `end_ms`: where the gap sits on the meeting's own clock (`meeting_rooms.started_at`
  is zero, the same origin the transcript's times use), so the transcript, the minutes and the
  replay can show "Off the record, 4 min" in place. Null when the room clock is unknown.

At most one open segment per meeting (`uq_meeting_off_record_segments_open`, partial unique), so
two hosts pressing at once cannot open two. The enforcement itself is the Redis flag
`nexus:{org_id}:off-record:{meeting_id}` core-api sets in the same request (transcription-service,
agent-runtime-service and the indexers read it); this table is the record of what was not
recorded. Tenant-scoped like every core-api table: ENABLE + FORCE RLS with the canonical NULLIF
`tenant_isolation` policy and the explicit `grant_tenant_table` (ADR-0021).

Revision ID: c5d6e7f8a9b0
Revises: b8c9d0e1f2a3
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "c5d6e7f8a9b0"
down_revision: Union[str, None] = "b8c9d0e1f2a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE meeting_off_record_segments (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id    uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          started_at    timestamptz NOT NULL DEFAULT now(),
          started_by    uuid        REFERENCES users(id) ON DELETE SET NULL,
          requested_by  uuid        REFERENCES users(id) ON DELETE SET NULL,
          ended_at      timestamptz,
          ended_by      uuid        REFERENCES users(id) ON DELETE SET NULL,
          start_ms      integer,
          end_ms        integer,
          CONSTRAINT pk_meeting_off_record_segments PRIMARY KEY (id),
          CONSTRAINT ck_meeting_off_record_segments_order
            CHECK (ended_at IS NULL OR ended_at >= started_at),
          CONSTRAINT ck_meeting_off_record_segments_ms
            CHECK (start_ms IS NULL OR start_ms >= 0)
        );
        """)
    op.execute(
        "CREATE INDEX ix_meeting_off_record_segments_meeting "
        "ON meeting_off_record_segments (meeting_id, started_at);"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_meeting_off_record_segments_open "
        "ON meeting_off_record_segments (meeting_id) WHERE ended_at IS NULL;"
    )
    op.execute(
        "CREATE INDEX ix_meeting_off_record_segments_org_id "
        "ON meeting_off_record_segments (org_id);"
    )
    op.execute("ALTER TABLE meeting_off_record_segments ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE meeting_off_record_segments FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON meeting_off_record_segments "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "meeting_off_record_segments")
    op.execute(
        "COMMENT ON TABLE meeting_off_record_segments IS 'Stretches of a meeting that were off "
        "the record: no transcript, no agents, no index (polish wave 3). Written only by "
        "core-api; the host or creator starts and ends one, any participant can ask.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS meeting_off_record_segments;")
