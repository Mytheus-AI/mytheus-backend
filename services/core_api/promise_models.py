"""Pydantic models for the promise ledger (`/api/v1/promises`). Separate from `schemas.py` so the
existing `Commitment` contract stays exactly as published."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PromiseStatus = Literal["open", "done", "dismissed"]
PromiseChannel = Literal["meeting", "in_person", "slack", "email", "manual"]
ReviewState = Literal["pending", "confirmed"]
CapacityLevel = Literal["ok", "tight", "over", "off"]
ProposalState = Literal["pending", "accepted", "declined", "withdrawn"]
ProposalTarget = Literal["commitment", "action"]

MAX_CONTENT = 500
MAX_LABEL = 200
MAX_NOTE = 300


def _trimmed(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())
    return value or None


class PromiseSource(BaseModel):
    """Where the promise was made, as a chip: the meeting moment, the Slack message, the email,
    or who added it."""

    channel: PromiseChannel
    label: str = Field(description="What the chip says: a meeting title, `Slack`, `Added by you`.")
    link: str | None = Field(
        default=None,
        description="The original moment or message: an app path (`/meetings/...`) or the "
        "provider's permalink. Null when there is nothing to open.",
    )
    external: bool = Field(description="True when `link` leaves Mytheus (a Slack permalink).")
    quote: str | None = Field(default=None, description="The words the promise was made in.")
    at: datetime | None = Field(default=None, description="When it was said or sent.")


class ProposalBrief(BaseModel):
    id: uuid.UUID
    proposed_due: date
    state: ProposalState
    requester_name: str | None


class LedgerPromise(BaseModel):
    id: uuid.UUID
    content: str
    status: PromiseStatus
    review_state: ReviewState
    channel: PromiseChannel
    due_at: date | None
    due_hint: str | None
    recipient_user_id: uuid.UUID | None
    recipient_name: str | None = Field(
        description="Who it was promised to: a colleague's name or the label the source gave."
    )
    source: PromiseSource
    meeting_id: uuid.UUID | None
    moment_id: uuid.UUID | None
    action_item_id: uuid.UUID | None
    confidence: float | None
    handed_off_to: uuid.UUID | None
    handed_off_to_name: str | None
    handed_off_from: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    done_at: datetime | None
    pending_proposal: ProposalBrief | None


class CaptureSettings(BaseModel):
    slack_enabled: bool
    email_enabled: bool
    email_connected: bool = Field(
        description="False: Mytheus has no mail-reading integration yet, so nothing is captured "
        "from email until one is connected."
    )


class PromiseLedger(BaseModel):
    items: list[LedgerPromise]
    pending_review: int
    capture: CaptureSettings


class PromiseCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1, max_length=MAX_CONTENT, description="What you promised.")
    due_at: date | None = Field(default=None, description="When it is due (YYYY-MM-DD).")
    recipient_user_id: uuid.UUID | None = Field(
        default=None, description="A colleague in this workspace you promised it to."
    )
    recipient_label: str | None = Field(
        default=None, max_length=MAX_LABEL, description="Or who, in words (a customer, a team)."
    )
    source_link: str | None = Field(
        default=None, max_length=2000, description="Optional link to where you promised it."
    )

    @field_validator("content")
    @classmethod
    def _content(cls, value: str) -> str:
        trimmed = _trimmed(value)
        if not trimmed:
            raise ValueError("content must not be blank")
        return trimmed

    @field_validator("recipient_label")
    @classmethod
    def _label(cls, value: str | None) -> str | None:
        return _trimmed(value)

    @field_validator("source_link")
    @classmethod
    def _link(cls, value: str | None) -> str | None:
        value = _trimmed(value)
        if value is not None and not value.startswith(("https://", "http://")):
            raise ValueError("source_link must be an http(s) URL")
        return value


class PromiseUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: PromiseStatus | None = Field(
        default=None, description="`done` (kept), `dismissed` (dropped), `open` (undo either)."
    )
    content: str | None = Field(default=None, min_length=1, max_length=MAX_CONTENT)
    due_at: date | None = None
    clear_due: bool = Field(default=False, description="True removes the due date.")
    recipient_user_id: uuid.UUID | None = None
    recipient_label: str | None = Field(default=None, max_length=MAX_LABEL)

    @field_validator("content")
    @classmethod
    def _content(cls, value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = _trimmed(value)
        if not trimmed:
            raise ValueError("content must not be blank")
        return trimmed


class PromiseConfirm(BaseModel):
    model_config = ConfigDict(extra="forbid")

    due_at: date | None = Field(default=None, description="Optionally set or correct the date.")


class CapacityCheck(BaseModel):
    due_date: date
    due_label: str
    level: CapacityLevel = Field(description="The day with this one more thing on it.")
    items_on_day: int = Field(description="Things due that day, counting this one.")
    meeting_minutes: int
    warn: bool = Field(description="True when your agent suggests offering another date.")
    message: str = Field(description="What your agent says, in one or two sentences.")
    suggested_date: date | None
    suggested_label: str | None


class PromiseWithCapacity(BaseModel):
    promise: LedgerPromise
    capacity: CapacityCheck | None


class HandOff(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to_user_id: uuid.UUID
    note: str | None = Field(default=None, max_length=MAX_NOTE)


class HandOffResult(BaseModel):
    promise: LedgerPromise
    handed_to_name: str | None
    copy_id: uuid.UUID


class CapacityDay(BaseModel):
    date: date
    label: str
    promises: int
    actions: int
    items: int
    meeting_minutes: int
    focus_minutes: int
    away: bool
    load_minutes: int
    capacity_minutes: int
    level: CapacityLevel


class Capacity(BaseModel):
    start: date
    today: date
    tz: str
    days: list[CapacityDay]
    overdue: int
    undated: int
    calendar_synced: bool = Field(
        description="True when this person's Google Calendar events reach Mytheus."
    )
    workday_minutes: int
    item_minutes: int


class DateProposalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: ProposalTarget
    target_id: uuid.UUID
    proposed_due: date
    note: str | None = Field(default=None, max_length=MAX_NOTE)

    @field_validator("note")
    @classmethod
    def _note(cls, value: str | None) -> str | None:
        return _trimmed(value)


class DateProposal(BaseModel):
    id: uuid.UUID
    target: ProposalTarget
    target_id: uuid.UUID
    content: str
    current_due: date | None
    proposed_due: date
    note: str | None
    state: ProposalState
    proposer_id: uuid.UUID
    proposer_name: str | None
    requester_id: uuid.UUID
    requester_name: str | None
    created_at: datetime
    decided_at: datetime | None


class DateProposalList(BaseModel):
    items: list[DateProposal]


class RenegotiateResult(BaseModel):
    mode: Literal["proposed", "applied"] = Field(
        description="`proposed`: the requester was asked and decides. `applied`: nobody else "
        "asked for it, so the new date is set at once."
    )
    proposal: DateProposal | None
    target: ProposalTarget
    target_id: uuid.UUID
    due_at: date | None = Field(description="The date it carries now.")
    previous_due: date | None


class CapturePreferencesUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slack_enabled: bool
    email_enabled: bool
