"""Pydantic v2 wire models for core-api.

Every mutating response returns the CANONICAL PERSISTED RESOURCE, never a partial echo of the
request (`19-api-architecture.md` section 5.2). A partial echo lets a client believe a field
was stored when a default, a trigger, or a constraint changed it, and the divergence surfaces
much later as a bug that looks like data corruption.

`org_id` appears in no request model. It is derived from the verified token, never accepted
from a caller, so there is nothing here for a caller to set.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MeetingStatus = Literal["scheduled", "live", "ended", "cancelled"]


class MeetingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a typo'd field is a 422, not a silent no-op

    title: Annotated[str, Field(min_length=1, max_length=500)]
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    participant_ids: list[uuid.UUID] = Field(default_factory=list)

    @model_validator(mode="after")
    def _end_after_start(self) -> MeetingCreate:
        # Mirrors the ck_meetings_schedule_ordered CHECK. Validating here as well turns a
        # 500-shaped IntegrityError into a 422 that names the offending field.
        if (
            self.scheduled_start
            and self.scheduled_end
            and self.scheduled_end <= self.scheduled_start
        ):
            raise ValueError("scheduled_end must be after scheduled_start")
        return self


class MeetingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=500)] | None = None
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    status: MeetingStatus | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> MeetingUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        return self


class Participant(BaseModel):
    user_id: uuid.UUID
    role_in_meeting: str | None = None


class Meeting(BaseModel):
    id: uuid.UUID
    title: str | None
    status: MeetingStatus
    scheduled_start: datetime | None
    scheduled_end: datetime | None
    livekit_room: str | None
    created_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    participants: list[Participant] = Field(default_factory=list)


class MeetingPage(BaseModel):
    """Cursor pagination, never OFFSET.

    OFFSET re-scans every skipped row, so page 500 costs 500 pages of work, and a concurrent
    insert shifts every subsequent page and silently drops a row from the caller's view. The
    cursor is opaque so its encoding can change without breaking clients.
    """

    items: list[Meeting]
    next_cursor: str | None = None


class TeamCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=200)]
    department_id: uuid.UUID | None = None


class Team(BaseModel):
    id: uuid.UUID
    name: str
    department_id: uuid.UUID | None
    created_at: datetime


class DepartmentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=200)]
    parent_id: uuid.UUID | None = None


class Department(BaseModel):
    id: uuid.UUID
    name: str
    parent_id: uuid.UUID | None
    created_at: datetime


class ChatMessageCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meeting_id: uuid.UUID
    content: Annotated[str, Field(min_length=1, max_length=10_000)]
    # Accepted for wire compatibility with `realtime-gateway`'s relay (Task 6), which posts
    # its own resolved `userId` here - but that value is the Clerk `sub` claim, not our
    # internal `users.id`, and is DELIBERATELY IGNORED: the persisted row's `user_id` always
    # comes from the verified bearer token's own resolved identity (`ctx.user_id`), exactly
    # like `org_id` and `meetings.created_by` are never taken from a request body. Typed as a
    # plain string (not `uuid.UUID`) precisely because it is not one on the wire; giving it a
    # UUID type would 422 every gateway-relayed message. See the task-5 report's "contract
    # verification" section.
    user_id: str | None = None


class ChatMessage(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    user_id: uuid.UUID
    content: str
    created_at: datetime


SpeakerAssignment = Literal["manual", "suggested"]


class SpeakerAssign(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: uuid.UUID


class MeetingSpeaker(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    speaker_id: str
    user_id: uuid.UUID | None
    confidence: float | None
    assignment: SpeakerAssignment
    created_at: datetime
    updated_at: datetime
