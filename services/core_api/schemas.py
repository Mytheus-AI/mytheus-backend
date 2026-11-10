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
from datetime import date, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MeetingStatus = Literal["scheduled", "live", "ended", "cancelled"]

# --- ADR-0038 D5/D6: the agenda ------------------------------------------------------------

MAX_AGENDA_ITEMS = 20
AgendaStatus = Literal["pending", "in_progress", "covered", "skipped"]
AgendaStatusSource = Literal["agent", "host"]


class AgendaItemCreate(BaseModel):
    """One agenda item as a client writes it. `owner_user_id` must be a participant."""

    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=200)]
    owner_user_id: uuid.UUID | None = None
    duration_minutes: Annotated[int, Field(ge=1, le=240)] | None = None
    # ADR-0039 B3: an `org_goals` id (identity-tenant-service's table; no FK across services).
    goal_id: uuid.UUID | None = None

    @field_validator("title")
    @classmethod
    def _trimmed_title(cls, v: str) -> str:
        value = v.strip()
        if not value:
            raise ValueError("title must not be blank")
        return value


class AgendaItemPut(AgendaItemCreate):
    """`PUT /meetings/{id}/agenda` entry. With the `id` of an existing item of this meeting it
    keeps that item (and its status); without one it is a new `pending` item."""

    id: uuid.UUID | None = None


class AgendaReplace(BaseModel):
    """`PUT /meetings/{id}/agenda` body: the whole agenda, in order. Items left out are
    deleted; an empty list clears the agenda."""

    model_config = ConfigDict(extra="forbid")

    items: Annotated[list[AgendaItemPut], Field(max_length=MAX_AGENDA_ITEMS)]


class AgendaItemStatusUpdate(BaseModel):
    """`PATCH /meetings/{id}/agenda/{item_id}`: the host marks an item. Sets
    `status_source = 'host'`, after which the MeetingAgent never changes it."""

    model_config = ConfigDict(extra="forbid")

    status: AgendaStatus


class AgendaItem(BaseModel):
    id: uuid.UUID
    position: int
    title: str
    owner_user_id: uuid.UUID | None
    duration_minutes: int | None
    status: AgendaStatus
    status_source: AgendaStatusSource | None
    agent_note: str | None
    updated_at: datetime
    goal_id: uuid.UUID | None = None


class MeetingRef(BaseModel):
    """The earlier meeting a meeting follows up (ADR-0038 D6)."""

    id: uuid.UUID
    title: str | None
    scheduled_start: datetime | None


class CarryForwardDecision(BaseModel):
    id: uuid.UUID
    content: str


class LatestStatus(BaseModel):
    """The assignee's most recent answer to a status request for an action (ADR-0039 B7)."""

    text: str
    answered_at: datetime


class CarryForwardAction(BaseModel):
    id: uuid.UUID
    title: str
    owner_user_id: uuid.UUID | None
    due_date: date | None
    latest_status: LatestStatus | None = None


class CarryForwardQuestion(BaseModel):
    id: uuid.UUID
    question: str


class CarryForward(BaseModel):
    """`GET /meetings/{id}/carry-forward`: what the previous meeting left open. Empty lists and
    `previous_meeting: null` when the meeting follows up nothing."""

    previous_meeting: MeetingRef | None
    decisions: list[CarryForwardDecision]
    open_actions: list[CarryForwardAction]
    saved_questions: list[CarryForwardQuestion]


class MeetingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a typo'd field is a 422, not a silent no-op

    title: Annotated[str, Field(min_length=1, max_length=500)]
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    participant_ids: list[uuid.UUID] = Field(default_factory=list)
    # ADR-0038 D6. `host_user_id` defaults to the creator and must be the creator or one of
    # `participant_ids`; `previous_meeting_id` must name another meeting of this tenant.
    host_user_id: uuid.UUID | None = None
    previous_meeting_id: uuid.UUID | None = None
    agenda: Annotated[list[AgendaItemCreate], Field(max_length=MAX_AGENDA_ITEMS)] = Field(
        default_factory=list
    )
    # ADR-0039 B2. `confidential` omitted takes the template's `confidential` (false without a
    # template); `template_id` must name a meeting type of this tenant (422 otherwise).
    confidential: bool | None = None
    template_id: uuid.UUID | None = None

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


class ParticipantsAdd(BaseModel):
    """`POST /meetings/:id/participants` body: tenant users to invite into an existing meeting."""

    model_config = ConfigDict(extra="forbid")

    user_ids: Annotated[list[uuid.UUID], Field(min_length=1, max_length=50)]


class MeetingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=500)] | None = None
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    status: MeetingStatus | None = None
    # ADR-0038 D6: host or creator only. A meeting always has exactly one host, so the host
    # cannot be cleared; `previous_meeting_id: null` removes the follow-up link.
    host_user_id: uuid.UUID | None = None
    previous_meeting_id: uuid.UUID | None = None

    @field_validator("host_user_id")
    @classmethod
    def _host_not_null(cls, v: uuid.UUID | None) -> uuid.UUID | None:
        if v is None:
            raise ValueError("a meeting always has a host; name the new host instead")
        return v

    @model_validator(mode="after")
    def _at_least_one_field(self) -> MeetingUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        return self


Attendance = Literal["live", "agent"]


class Participant(BaseModel):
    """A participant as a meeting response shows it. ADR-0039 B5: `attendance` and
    `proxy_update` are visible to every participant (the update is meant for the room);
    `proxy_questions` only to the person themself and the host, and `null` for anyone else."""

    user_id: uuid.UUID
    role_in_meeting: str | None = None
    attendance: Attendance = "live"
    proxy_update: str | None = None
    proxy_questions: list[str] | None = None


MAX_PROXY_UPDATE = 2000
MAX_PROXY_QUESTIONS = 5
MAX_PROXY_QUESTION = 300


class AttendanceUpdate(BaseModel):
    """`PUT /meetings/{id}/attendance` (ADR-0039 B5): the caller's own attendance. A PUT, so an
    omitted `proxy_update` clears it and omitted `proxy_questions` clears them."""

    model_config = ConfigDict(extra="forbid")

    attendance: Attendance
    proxy_update: Annotated[str, Field(max_length=MAX_PROXY_UPDATE)] | None = None
    proxy_questions: Annotated[
        list[Annotated[str, Field(max_length=MAX_PROXY_QUESTION)]],
        Field(max_length=MAX_PROXY_QUESTIONS),
    ] = Field(default_factory=list)

    @field_validator("proxy_update")
    @classmethod
    def _trim_update(cls, v: str | None) -> str | None:
        return (v.strip() or None) if v is not None else None

    @field_validator("proxy_questions")
    @classmethod
    def _trim_questions(cls, v: list[str]) -> list[str]:
        return [q for q in dict.fromkeys(q.strip() for q in v) if q]


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
    # ADR-0038 D6. `host_user_id` is the participant whose `role_in_meeting` is `host`; null
    # only for a meeting that has none (one created by the system with no creator).
    host_user_id: uuid.UUID | None = None
    previous_meeting_id: uuid.UUID | None = None
    # ADR-0039 B2.
    confidential: bool = False
    template_id: uuid.UUID | None = None


class MeetingDetail(Meeting):
    """`GET /meetings/{id}` (and the create/PATCH responses): the meeting plus the meeting it
    follows up and its agenda, ordered by `position`."""

    previous_meeting: MeetingRef | None = None
    agenda: list[AgendaItem] = Field(default_factory=list)


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


class Worklog(BaseModel):
    """One `PersonalListenerAgent` note from `worklogs` (Plan 7-9 section 3.3).

    Only `surfaced` notes are ever served; the 0.4-0.7 silent-log band stays in the table and
    is not returned. `window_id` ties the note to the 30 s agent window it came from, and
    `source_chunk_ids` to the transcript finals in that window.
    """

    id: uuid.UUID
    meeting_id: uuid.UUID
    user_id: uuid.UUID
    window_id: str
    content: str
    relevance_score: float | None = None
    confidence: float | None = None
    source_chunk_ids: list[uuid.UUID]
    created_at: datetime


class WorklogList(BaseModel):
    items: list[Worklog]


# --- Phase 8 Part A: decision / action candidates (Plan 7-9 section 3.6) --------------------

DecisionStatus = Literal["proposed", "confirmed", "superseded", "dismissed"]
ActionStatus = Literal["proposed", "confirmed", "done", "dismissed"]

_CANDIDATE_TEXT_MAX = 4000


class Decision(BaseModel):
    """One `decisions` row. `proposed` until a participant confirms or dismisses it."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    rationale: str | None = None
    confidence: float
    status: DecisionStatus
    confirmed_by: uuid.UUID | None = None
    supersedes: uuid.UUID | None = None
    created_at: datetime


class ActionItem(BaseModel):
    """One `action_items` row. `proposed` until a participant confirms or dismisses it."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    assignee_id: uuid.UUID | None = None
    due_date: date | None = None
    confidence: float
    status: ActionStatus
    confirmed_by: uuid.UUID | None = None
    created_at: datetime


class CandidateList(BaseModel):
    """`GET /meetings/:id/candidates`: the proposed, surfaced candidates, oldest first."""

    decisions: list[Decision]
    actions: list[ActionItem]


def _reject_null_content(model: DecisionUpdate | ActionItemUpdate) -> None:
    if "content" in model.model_fields_set and model.content is None:
        raise ValueError("content cannot be null")


class DecisionUpdate(BaseModel):
    """PATCH body: edit a proposed decision before confirming it. At least one field."""

    model_config = ConfigDict(extra="forbid")

    content: Annotated[str, Field(min_length=1, max_length=_CANDIDATE_TEXT_MAX)] | None = None
    rationale: Annotated[str, Field(max_length=_CANDIDATE_TEXT_MAX)] | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> DecisionUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        _reject_null_content(self)
        return self


class DecisionConfirm(BaseModel):
    """Confirm body. `supersedes` names an earlier confirmed decision (FOLLOWS_FROM lineage)."""

    model_config = ConfigDict(extra="forbid")

    supersedes: uuid.UUID | None = None


class ActionItemUpdate(BaseModel):
    """PATCH body: edit a proposed action before confirming it. At least one field.

    `assignee_id` and `due_date` may be set to null to clear them.
    """

    model_config = ConfigDict(extra="forbid")

    content: Annotated[str, Field(min_length=1, max_length=_CANDIDATE_TEXT_MAX)] | None = None
    assignee_id: uuid.UUID | None = None
    due_date: date | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> ActionItemUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        _reject_null_content(self)
        return self


# --- P9: QuestionAgent suggestions (ADR-0034) ---------------------------------------------

SuggestionStatus = Literal["open", "asked", "dismissed", "saved"]
CitationKind = Literal["transcript", "document", "decision", "action"]


class SuggestionCitation(BaseModel):
    """One grounding source of a suggestion, in the four ADR-0033 D3 kinds. A transcript
    citation opens the meeting at `start_ms`; a document one opens the document; a decision or
    action one opens the decision lineage page or the meeting."""

    model_config = ConfigDict(extra="ignore")

    kind: CitationKind
    meeting_id: str | None = None
    chunk_id: str | None = None
    start_ms: int | None = None
    document_id: str | None = None
    decision_id: str | None = None
    action_id: str | None = None
    title: str | None = None


class Suggestion(BaseModel):
    """One `agent_suggestions` row, as its owner sees it. Never served to anyone else."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    question: str
    priority: Annotated[int, Field(ge=1, le=3)]
    status: SuggestionStatus
    confidence: float | None = None
    rationale: str | None = None
    citations: list[SuggestionCitation] = Field(default_factory=list)
    created_at: datetime


class SuggestionList(BaseModel):
    """`GET /suggestions?meeting_id=`: the caller's open, surfaced suggestions for that meeting
    (oldest first) and every suggestion they saved for later, from any meeting (newest first)."""

    open: list[Suggestion]
    saved: list[Suggestion]


# --- ADR-0039 B1: meeting types --------------------------------------------------------------

MAX_TEMPLATES = 50


class TemplateAgendaItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=200)]
    duration_minutes: Annotated[int, Field(ge=1, le=240)] | None = None

    @field_validator("title")
    @classmethod
    def _trimmed(cls, v: str) -> str:
        value = v.strip()
        if not value:
            raise ValueError("title must not be blank")
        return value


def _trim_name(v: str | None) -> str | None:
    if v is None:
        return None
    value = v.strip()
    if not value:
        raise ValueError("name must not be blank")
    return value


class MeetingTemplateCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=80)]
    description: Annotated[str, Field(max_length=300)] | None = None
    default_duration_minutes: Annotated[int, Field(ge=5, le=480)] = 30
    agenda: Annotated[list[TemplateAgendaItem], Field(max_length=MAX_AGENDA_ITEMS)] = Field(
        default_factory=list
    )
    confidential: bool = False

    _name = field_validator("name")(_trim_name)


class MeetingTemplateUpdate(BaseModel):
    """PATCH: any subset. `description: null` clears it; the others cannot be null."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=80)] | None = None
    description: Annotated[str, Field(max_length=300)] | None = None
    default_duration_minutes: Annotated[int, Field(ge=5, le=480)] | None = None
    agenda: Annotated[list[TemplateAgendaItem], Field(max_length=MAX_AGENDA_ITEMS)] | None = None
    confidential: bool | None = None

    _name = field_validator("name")(_trim_name)

    @model_validator(mode="after")
    def _fields(self) -> MeetingTemplateUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        for name in ("name", "default_duration_minutes", "agenda", "confidential"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} cannot be null")
        return self


class MeetingTemplate(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    default_duration_minutes: int
    agenda: list[TemplateAgendaItem]
    confidential: bool
    created_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class MeetingTemplateList(BaseModel):
    items: list[MeetingTemplate]


# --- ADR-0039 B4: drafted agenda --------------------------------------------------------------

AgendaDraftSource = Literal["template", "unfinished", "open_action"]


class AgendaDraftRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    previous_meeting_id: uuid.UUID | None = None
    template_id: uuid.UUID | None = None


class AgendaDraftItem(BaseModel):
    title: str
    owner_user_id: uuid.UUID | None
    duration_minutes: int | None
    goal_id: uuid.UUID | None
    source: AgendaDraftSource


class AgendaDraft(BaseModel):
    items: list[AgendaDraftItem]


# --- ADR-0039 B6: promises ------------------------------------------------------------------

CommitmentStatus = Literal["open", "done", "dismissed"]


class Commitment(BaseModel):
    """One of the caller's own promises. Never served to anyone else."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    meeting_scheduled_start: datetime | None = None
    content: str
    due_hint: str | None
    status: CommitmentStatus
    created_at: datetime
    updated_at: datetime
    done_at: datetime | None


class CommitmentList(BaseModel):
    items: list[Commitment]


class CommitmentUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: CommitmentStatus


# --- ADR-0039 B7: status requests -----------------------------------------------------------

StatusRequestState = Literal["pending", "answered", "expired"]
MAX_STATUS_TEXT = 500


class StatusRequest(BaseModel):
    """A status asked of the caller about one of their actions before a follow-up meeting."""

    id: uuid.UUID
    action_id: uuid.UUID
    # The action item's text, under both names the UI reads (`action_title` mirrors
    # `CarryForwardAction.title`).
    action_content: str | None = None
    action_title: str | None = None
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    meeting_scheduled_start: datetime | None = None
    state: StatusRequestState
    status_text: str | None
    requested_at: datetime
    answered_at: datetime | None


class StatusRequestList(BaseModel):
    items: list[StatusRequest]


class StatusAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status_text: Annotated[str, Field(min_length=1, max_length=MAX_STATUS_TEXT)]

    @field_validator("status_text")
    @classmethod
    def _trimmed(cls, v: str) -> str:
        value = v.strip()
        if not value:
            raise ValueError("status_text must not be blank")
        return value
