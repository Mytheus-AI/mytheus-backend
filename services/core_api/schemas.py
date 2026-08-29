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


class ParticipantCheck(BaseModel):
    """`GET /meetings/:id/participants/:clerk_user_id`'s response shape.

    Deliberately NOT `Participant` with an extra field: `Participant` is also embedded in
    `Meeting.participants` (see `routes.meetings._load_participants`/`create_meeting`), and
    every one of those call sites constructs it with no `org_id` in hand at all (it isn't
    resolved per-participant, and the meeting itself already implies the tenant). Adding a
    required `org_id` to `Participant` would break those constructions; adding an optional
    one would put a meaningless `"org_id": null` on every entry of every meeting's
    participants array. This is a distinct resource with its own shape instead.

    `org_id` here is `ctx.org_id` - the CALLER's own tenant, resolved from their own verified
    bearer token, never a lookup on the target resource. `realtime-gateway`'s presence-events
    fix (mytheus-realtime Task 6 follow-up) needs it because the gateway has no database and
    cannot resolve its own JWT's opaque Clerk org id into our internal UUID any other way; this
    endpoint already computes it for its own RLS scoping, so returning it is not a new query.
    """

    user_id: uuid.UUID
    role_in_meeting: str | None = None
    org_id: uuid.UUID


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


class TranscriptChunk(BaseModel):
    """One persisted final from `transcript_chunks`, owned by `transcription-service`.

    `speaker_id` is the raw diarized label as Deepgram emitted it, never a resolved user: the
    `speaker_id` to `user_id` mapping lives in `meeting_speakers` and is served separately, so
    a client relabels turns by joining the two rather than this endpoint guessing. `redacted`
    tells a reader that the text it is seeing had PII replaced, which matters when a transcript
    reads oddly and the reason is redaction rather than mis-transcription.
    """

    id: uuid.UUID
    meeting_id: uuid.UUID
    speaker_id: str
    start_ms: int
    end_ms: int
    text: str
    is_final: bool
    confidence: float | None = None
    redacted: bool = False


class TranscriptPage(BaseModel):
    """Cursor pagination over `(start_ms, id)`, the same tuple the ORDER BY and the table's
    `(meeting_id, start_ms)` index use, so a page is an index range scan and a concurrent
    insert cannot shift rows out of a later page."""

    items: list[TranscriptChunk]
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


class TeamMemberUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Free text rather than an enum: the seeder writes `lead`/`member`, but this column is a
    # label on the membership, NOT an authorization tier. Authorization reads `rbac_grants`
    # and nothing else (the Plan 1-2 decision recorded in the status overview), so constraining
    # this to a fixed vocabulary would imply a permission meaning it does not carry.
    role: Annotated[str, Field(min_length=1, max_length=100)] | None = None


class TeamMember(BaseModel):
    team_id: uuid.UUID
    user_id: uuid.UUID
    role: str | None


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


class MeetingSpeakerList(BaseModel):
    """Not cursor-paginated, deliberately: a meeting has as many rows here as it had distinct
    diarized speakers, which the capacity model bounds at single digits. A cursor would be
    ceremony over a list that is never long enough to need one.

    Declared after `MeetingSpeaker` rather than beside the other page models on purpose: the
    reverse order made `items` a forward reference that Pydantic would have to resolve at
    runtime, which `from __future__ import annotations` hides from every static check.
    """

    items: list[MeetingSpeaker]
