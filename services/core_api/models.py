"""core-api models — P2 stubs this band (extended in Plan 1-2).

Minimal, canon-named Meeting + MeetingParticipant so foreign keys, dashboards, and the later
meeting/transcription bands have a real target. core-api owns the meeting domain.
"""

from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base, TenantMixin, TimestampMixin


class Meeting(Base, TenantMixin, TimestampMixin):
    __tablename__ = "meetings"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'scheduled'"))


class MeetingParticipant(Base, TenantMixin):
    __tablename__ = "meeting_participants"
    meeting_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("meetings.id"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), primary_key=True
    )
