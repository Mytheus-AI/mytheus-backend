"""Shareable records: a meeting recap and a decision record, built as one person can see them.

Phase 4 "shareable recaps" and "shareable decision records". One builder per record, used by
every surface that shows one to someone who may not be a user:

- the public share page (`routes/share_links.py`, `GET /api/v1/public/shares/{token}`),
- the follow-up email (`routes/followups.py`), which links that page and lists its headline,
- the CRM meeting note and the Notion / Confluence / Slack decision exports, which
  integration-service reads through `routes/internal_reach.py`.

What a record may contain is decided here and nowhere else, so the four surfaces cannot drift:

- **Only confirmed facts.** Decisions that are `confirmed` or `superseded`, actions that are
  `confirmed` or `done`. Proposed or dismissed candidates are agent output nobody approved and
  never leave the product (no silent commits).
- **Only what the viewer can see.** The viewer is the person the record is built FOR (the
  creator of a share link, the person exporting). They must be a participant of the meeting,
  and a confidential meeting (ADR-0039 B2) is shareable only by its host. A lineage step from
  a meeting the viewer was not in is kept as a placeholder with its content withheld, so the
  chain stays honest ("replaced by a later decision") without leaking what that decision says.
- **No private material.** Promises (`commitments`), personal notes, suggestions, chat and the
  transcript are private or raw and are never part of a shared record. The recap carries the
  meeting's SHARED minutes (`minutes.py`: summary, decisions with status, open items and
  unanswered questions), never anyone's personal minutes or promises, and people in them by
  name only (no user ids).
- **People by name, not by address.** `users` has no display name, so a person is shown by a
  name derived from the local part of their email (`maya.patel@x` -> `Maya Patel`). An email
  address never appears in a record a stranger can open.
"""

from __future__ import annotations

import re
import uuid
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

MAX_ITEMS = 200
MAX_LINEAGE = 20

RecordStatus = Literal["confirmed", "superseded"]
ActionState = Literal["confirmed", "done"]
LineageRelation = Literal["earlier", "subject", "later"]


class RecordAgendaItem(BaseModel):
    title: str
    status: str
    owner_name: str | None = None


class RecordDecision(BaseModel):
    id: uuid.UUID
    content: str
    rationale: str | None = None
    status: RecordStatus
    decided_at: datetime


class RecordAction(BaseModel):
    id: uuid.UUID
    content: str
    owner_name: str | None = None
    due_date: date | None = None
    status: ActionState


class RecapMinutesDecision(BaseModel):
    text: str
    status: str


class RecapMinutesItem(BaseModel):
    text: str
    owner_name: str | None = None
    due: str | None = None


class RecapMinutesQuestion(BaseModel):
    text: str
    asked_by_name: str | None = None


class RecapMinutes(BaseModel):
    """The meeting's shared minutes as a stranger may read them: no ids, no promises."""

    summary: str
    decisions: list[RecapMinutesDecision]
    open_items: list[RecapMinutesItem]
    unanswered_questions: list[RecapMinutesQuestion]
    updated_at: datetime


class RecapLateContribution(BaseModel):
    """A point someone added after the meeting, as a stranger may read it: text only."""

    author_name: str
    body: str
    created_at: datetime


class MeetingRecap(BaseModel):
    """A meeting's confirmed outcome, as its viewer may see it."""

    kind: Literal["meeting_recap"] = "meeting_recap"
    meeting_id: uuid.UUID
    title: str
    status: str
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    host_name: str | None = None
    participant_names: list[str]
    agenda: list[RecordAgendaItem]
    decisions: list[RecordDecision]
    actions: list[RecordAction]
    minutes: RecapMinutes | None = None
    # "After the meeting" layer 1: the written late points, oldest first.
    late_contributions: list[RecapLateContribution] = Field(default_factory=list)


class LineageStep(BaseModel):
    """One decision in the chain around the subject, oldest first.

    `visible` is false for a decision from a meeting the viewer was not in: its id and content
    are withheld and only its place in the chain is kept.
    """

    relation: LineageRelation
    visible: bool
    decision_id: uuid.UUID | None = None
    content: str | None = None
    status: str | None = None
    meeting_title: str | None = None
    decided_at: datetime | None = None


class DecisionRecord(BaseModel):
    """One confirmed decision with its lineage and the meeting it was made in."""

    kind: Literal["decision_record"] = "decision_record"
    decision_id: uuid.UUID
    content: str
    rationale: str | None = None
    status: RecordStatus
    decided_at: datetime
    confirmed_by_name: str | None = None
    meeting_id: uuid.UUID
    meeting_title: str
    meeting_start: datetime | None = None
    lineage: list[LineageStep]
    meeting_actions: list[RecordAction]


class MeetingAccess(BaseModel):
    """The facts every visibility decision needs about one meeting and one viewer."""

    meeting_id: uuid.UUID
    title: str
    status: str
    confidential: bool
    host_id: uuid.UUID | None
    is_participant: bool

    def viewer_is_host(self, viewer: uuid.UUID) -> bool:
        return self.host_id is not None and self.host_id == viewer

    def shareable_by(self, viewer: uuid.UUID) -> bool:
        """A participant may share; a confidential meeting only its host may share."""
        if not self.is_participant:
            return False
        return not self.confidential or self.viewer_is_host(viewer)


def display_name(email: str | None) -> str | None:
    """`maya.patel+x@acme.com` -> `Maya Patel`. None for an empty or unusable address."""
    if not email or "@" not in email:
        return None
    local = email.split("@", 1)[0].split("+", 1)[0]
    words = [w for w in re.split(r"[._\-]+", local) if w]
    if not words:
        return None
    return " ".join(w[:1].upper() + w[1:] for w in words)[:80]


def _title(raw: Any) -> str:
    return str(raw).strip() if raw and str(raw).strip() else "Untitled meeting"


async def access_for(
    session: AsyncSession, meeting_id: uuid.UUID, viewer: uuid.UUID
) -> MeetingAccess | None:
    """The meeting under RLS (None if invisible or deleted) and the viewer's place in it.

    The host is the participant with `role_in_meeting = 'host'`, falling back to the meeting's
    creator, the same rule core-api's meeting routes use.
    """
    row = (
        (
            await session.execute(
                text(
                    "SELECT m.id, m.title, m.status, m.confidential, "
                    "COALESCE((SELECT p.user_id FROM meeting_participants p "
                    "          WHERE p.meeting_id = m.id AND p.role_in_meeting = 'host' "
                    "          LIMIT 1), m.created_by) AS host_id, "
                    "EXISTS (SELECT 1 FROM meeting_participants p JOIN users u "
                    "        ON u.id = p.user_id AND u.deleted_at IS NULL "
                    "        WHERE p.meeting_id = m.id AND p.user_id = :v) AS is_participant "
                    "FROM meetings m WHERE m.id = :m AND m.deleted_at IS NULL"
                ),
                {"m": str(meeting_id), "v": str(viewer)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    return MeetingAccess(
        meeting_id=row["id"],
        title=_title(row["title"]),
        status=str(row["status"]),
        confidential=bool(row["confidential"]),
        host_id=row["host_id"],
        is_participant=bool(row["is_participant"]),
    )


async def _actions(session: AsyncSession, meeting_id: uuid.UUID) -> list[RecordAction]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT a.id, a.content, a.due_date, a.status, u.email "
                    "FROM action_items a LEFT JOIN users u ON u.id = a.assignee_id "
                    "WHERE a.meeting_id = :m AND a.deleted_at IS NULL "
                    "AND a.status IN ('confirmed', 'done') "
                    "ORDER BY a.created_at, a.id LIMIT :n"
                ),
                {"m": str(meeting_id), "n": MAX_ITEMS},
            )
        )
        .mappings()
        .all()
    )
    return [
        RecordAction(
            id=r["id"],
            content=r["content"],
            owner_name=display_name(r["email"]),
            due_date=r["due_date"],
            status=r["status"],
        )
        for r in rows
    ]


def _allowed(access: MeetingAccess | None, viewer: uuid.UUID, for_sharing: bool) -> bool:
    if access is None:
        return False
    return access.shareable_by(viewer) if for_sharing else access.is_participant


async def build_recap(
    session: AsyncSession, meeting_id: uuid.UUID, viewer: uuid.UUID, *, for_sharing: bool = True
) -> MeetingRecap | None:
    """The recap `viewer` may share (or, with `for_sharing=False`, read themselves), or None
    when they may not: not a participant, a meeting this tenant cannot see, or, for sharing, a
    confidential meeting they do not host."""
    access = await access_for(session, meeting_id, viewer)
    if access is None or not _allowed(access, viewer, for_sharing):
        return None
    m = {"m": str(meeting_id), "n": MAX_ITEMS}
    meeting = (
        (
            await session.execute(
                text("SELECT scheduled_start, scheduled_end FROM meetings WHERE id = :m"), m
            )
        )
        .mappings()
        .one()
    )
    people = (
        (
            await session.execute(
                text(
                    "SELECT u.id, u.email, p.role_in_meeting FROM meeting_participants p "
                    "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                    "WHERE p.meeting_id = :m "
                    "ORDER BY (p.role_in_meeting = 'host') DESC NULLS LAST, u.email LIMIT :n"
                ),
                m,
            )
        )
        .mappings()
        .all()
    )
    host_email = next((p["email"] for p in people if p["id"] == access.host_id), None)
    agenda = (
        (
            await session.execute(
                text(
                    "SELECT i.title, i.status, u.email FROM meeting_agenda_items i "
                    "LEFT JOIN users u ON u.id = i.owner_user_id "
                    "WHERE i.meeting_id = :m ORDER BY i.position LIMIT :n"
                ),
                m,
            )
        )
        .mappings()
        .all()
    )
    decisions = (
        (
            await session.execute(
                text(
                    "SELECT id, content, rationale, status, created_at FROM decisions "
                    "WHERE meeting_id = :m AND deleted_at IS NULL "
                    "AND status IN ('confirmed', 'superseded') "
                    "ORDER BY created_at, id LIMIT :n"
                ),
                m,
            )
        )
        .mappings()
        .all()
    )
    return MeetingRecap(
        meeting_id=access.meeting_id,
        title=access.title,
        status=access.status,
        scheduled_start=meeting["scheduled_start"],
        scheduled_end=meeting["scheduled_end"],
        host_name=display_name(host_email),
        participant_names=[n for n in (display_name(p["email"]) for p in people) if n],
        agenda=[
            RecordAgendaItem(
                title=a["title"], status=str(a["status"]), owner_name=display_name(a["email"])
            )
            for a in agenda
        ],
        decisions=[
            RecordDecision(
                id=d["id"],
                content=d["content"],
                rationale=d["rationale"],
                status=d["status"],
                decided_at=d["created_at"],
            )
            for d in decisions
        ],
        actions=await _actions(session, meeting_id),
        minutes=await recap_minutes(session, meeting_id),
        late_contributions=await _recap_late(session, meeting_id),
    )


async def _recap_late(session: AsyncSession, meeting_id: uuid.UUID) -> list[RecapLateContribution]:
    from services.core_api.room_signals import recap_late_contributions

    return await recap_late_contributions(session, meeting_id)


async def recap_minutes(session: AsyncSession, meeting_id: uuid.UUID) -> RecapMinutes | None:
    """The shared minutes, public-safe; None when there are none worth showing."""
    from services.core_api.minutes import shared_minutes

    shared = await shared_minutes(session, meeting_id)
    if shared is None or shared.status != "ready":
        return None
    return RecapMinutes(
        summary=shared.summary,
        decisions=[RecapMinutesDecision(text=d.text, status=d.status) for d in shared.decisions],
        open_items=[
            RecapMinutesItem(text=o.text, owner_name=o.owner_name, due=o.due)
            for o in shared.open_items
        ],
        unanswered_questions=[
            RecapMinutesQuestion(text=q.text, asked_by_name=q.asked_by_name)
            for q in shared.unanswered_questions
        ],
        updated_at=shared.updated_at,
    )


_LINEAGE_SQL = text("""
    WITH RECURSIVE earlier AS (
      SELECT d.supersedes AS id, 1 AS depth FROM decisions d WHERE d.id = :d
      UNION ALL
      SELECT p.supersedes, e.depth + 1 FROM earlier e JOIN decisions p ON p.id = e.id
      WHERE p.supersedes IS NOT NULL AND e.depth < :lim
    ),
    later AS (
      SELECT n.id, 1 AS depth FROM decisions n
      WHERE n.supersedes = :d AND n.deleted_at IS NULL
      AND n.status IN ('confirmed', 'superseded')
      UNION ALL
      SELECT n.id, l.depth + 1 FROM later l JOIN decisions n ON n.supersedes = l.id
      WHERE n.deleted_at IS NULL AND n.status IN ('confirmed', 'superseded') AND l.depth < :lim
    )
    SELECT 'earlier' AS relation, -e.depth AS ord, d.id, d.content, d.status, d.created_at,
           d.meeting_id, m.title AS meeting_title,
           EXISTS (SELECT 1 FROM meeting_participants p
                   WHERE p.meeting_id = d.meeting_id AND p.user_id = :v) AS visible,
           m.confidential, d.deleted_at
    FROM earlier e JOIN decisions d ON d.id = e.id JOIN meetings m ON m.id = d.meeting_id
    WHERE e.id IS NOT NULL
    UNION ALL
    SELECT 'later', l.depth, d.id, d.content, d.status, d.created_at, d.meeting_id, m.title,
           EXISTS (SELECT 1 FROM meeting_participants p
                   WHERE p.meeting_id = d.meeting_id AND p.user_id = :v),
           m.confidential, d.deleted_at
    FROM later l JOIN decisions d ON d.id = l.id JOIN meetings m ON m.id = d.meeting_id
    ORDER BY ord
    """)


async def build_decision_record(
    session: AsyncSession, decision_id: uuid.UUID, viewer: uuid.UUID, *, for_sharing: bool = True
) -> DecisionRecord | None:
    """The decision record `viewer` may share, or None (not confirmed, invisible to them, or
    from a confidential meeting they do not host)."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT d.id, d.meeting_id, d.content, d.rationale, d.status, d.created_at, "
                    "u.email AS confirmed_by_email, m.scheduled_start "
                    "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                    "LEFT JOIN users u ON u.id = d.confirmed_by "
                    "WHERE d.id = :d AND d.deleted_at IS NULL "
                    "AND d.status IN ('confirmed', 'superseded')"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    access = await access_for(session, row["meeting_id"], viewer)
    if access is None or not _allowed(access, viewer, for_sharing):
        return None
    chain = (
        (
            await session.execute(
                _LINEAGE_SQL, {"d": str(decision_id), "v": str(viewer), "lim": MAX_LINEAGE}
            )
        )
        .mappings()
        .all()
    )
    steps: list[LineageStep] = []
    for c in chain:
        visible = bool(c["visible"]) and c["deleted_at"] is None
        step = LineageStep(relation=c["relation"], visible=visible)
        if visible:
            step = LineageStep(
                relation=c["relation"],
                visible=True,
                decision_id=c["id"],
                content=c["content"],
                status=c["status"],
                meeting_title=_title(c["meeting_title"]),
                decided_at=c["created_at"],
            )
        steps.append(step)
    earlier = [s for s in steps if s.relation == "earlier"]
    later = [s for s in steps if s.relation == "later"]
    subject = LineageStep(
        relation="subject",
        visible=True,
        decision_id=row["id"],
        content=row["content"],
        status=row["status"],
        meeting_title=access.title,
        decided_at=row["created_at"],
    )
    return DecisionRecord(
        decision_id=row["id"],
        content=row["content"],
        rationale=row["rationale"],
        status=row["status"],
        decided_at=row["created_at"],
        confirmed_by_name=display_name(row["confirmed_by_email"]),
        meeting_id=row["meeting_id"],
        meeting_title=access.title,
        meeting_start=row["scheduled_start"],
        lineage=[*earlier, subject, *later],
        meeting_actions=await _actions(session, row["meeting_id"]),
    )
