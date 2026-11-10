"""core-api models: the meeting domain. core-api owns it.

Gap 13: Plan 0 left these as two-column shells. `meetings` had no schedule at all, which
makes a "meeting" a row with a title and nothing that places it in time, and every consumer
of `meeting-events` needs the schedule.

`meetings` is deliberately NOT range-partitioned in this band, contrary to the reader docs.
Canon calls it a "range-partition candidate at scale" and canon wins, but the decisive reason
is structural: PostgreSQL requires a unique constraint on a partitioned table to include
every partition-key column, so partitioning by `scheduled_start` makes `UNIQUE (org_id, id)`
impossible, and with it the composite foreign keys that make a cross-tenant dangling
reference unrepresentable. Exit trigger recorded in `mytheus-docs/design/erd.md`.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Index, Text, text
from sqlalchemy.dialects.postgresql import ARRAY, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base, TenantMixin, TimestampMixin


class Meeting(Base, TenantMixin, TimestampMixin):
    __tablename__ = "meetings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'scheduled'"))
    scheduled_start: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    scheduled_end: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    # The LiveKit room name. Nullable because a room is not minted until the meeting starts,
    # which is the real-time band: the same reason `MeetingEvent.livekit_room` had to become
    # a nullable Avro union (mytheus-events 64f4c2d). A non-nullable column here would have
    # made every scheduled-but-not-started meeting unrepresentable.
    livekit_room: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )
    # P12 (migration 7c3e9a1f5b20): the Google Calendar event id a calendar-created meeting came
    # from. The `integration-events` consumer upserts on (org_id, calendar_event_id), so a
    # redelivered or re-synced event patches the one meeting instead of creating another.
    calendar_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # ADR-0038 D5 (migration 0b91a00079a7): the earlier meeting this one follows up. One link
    # per meeting; a chain of follow-ups is a chain of links. SET NULL so deleting the earlier
    # meeting never deletes or blocks its follow-ups.
    previous_meeting_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("meetings.id", ondelete="SET NULL"),
        nullable=True,
    )
    # ADR-0039 B2 (migration d2b3c4d5e6f7): a confidential meeting is never a source for topic
    # watch, the related_meeting hop, or carry-forward shown to non-participants. `template_id`
    # names the meeting type it was made from (`meeting_templates`, raw-SQL DDL, so the FK is
    # declared in the migration only).
    confidential: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    template_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    # Polish program (migration b8c9d0e1f2a3): where the call happens. 'mytheus' is a Mytheus room;
    # 'meet' / 'teams' / 'zoom' is an external call captured by the browser extension, for which
    # meeting-service opens no room and mints no token.
    source: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'mytheus'"))

    __table_args__ = (
        CheckConstraint("status IN ('scheduled','live','ended','cancelled')", name="status_valid"),
        CheckConstraint("source IN ('mytheus', 'meet', 'teams', 'zoom')", name="source"),
        Index(
            "uq_meetings_org_id_calendar_event_id",
            "org_id",
            "calendar_event_id",
            unique=True,
            postgresql_where=text("calendar_event_id IS NOT NULL"),
        ),
        CheckConstraint(
            "scheduled_end IS NULL OR scheduled_start IS NULL OR scheduled_end > scheduled_start",
            name="schedule_ordered",
        ),
        CheckConstraint(
            "previous_meeting_id IS NULL OR previous_meeting_id <> id", name="not_own_follow_up"
        ),
        Index(
            "ix_meetings_previous_meeting_id",
            "previous_meeting_id",
            postgresql_where=text("previous_meeting_id IS NOT NULL"),
        ),
    )


class MeetingParticipant(Base, TenantMixin):
    __tablename__ = "meeting_participants"

    meeting_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("meetings.id"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), primary_key=True
    )
    role_in_meeting: Mapped[str | None] = mapped_column(Text, nullable=True)  # host, attendee
    # ADR-0018 / Plan 3-4 section 8 DECISION-REQUIRED #2, resolved: recording/transcription
    # consent acknowledgment, gated by `tenant_settings.consent_required` (default false, so
    # both columns are inert until a tenant's flag is explicitly set). `consented_at` is the
    # acknowledgment timestamp; `consent_version` records which consent text was acknowledged,
    # so a later text change does not silently count as consent to the new wording.
    consented_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    consent_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    # ADR-0039 B5 (migration d2b3c4d5e6f7): "send your agent instead". `attendance` is `live` or
    # `agent`; `proxy_update` is for the room, `proxy_questions` for the person and the host.
    attendance: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'live'"))
    proxy_update: Mapped[str | None] = mapped_column(Text, nullable=True)
    proxy_questions: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )

    __table_args__ = (
        CheckConstraint("attendance IN ('live', 'agent')", name="attendance"),
        CheckConstraint(
            "proxy_update IS NULL OR char_length(proxy_update) <= 2000", name="proxy_update"
        ),
        CheckConstraint("cardinality(proxy_questions) <= 5", name="proxy_questions"),
    )
