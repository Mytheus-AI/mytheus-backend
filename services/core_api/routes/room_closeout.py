"""The room's close-out ritual and the late joiner's catch-up line (polish program, Phase 1).

"Close-out ritual". "End for everyone" used to end the meeting at once, and every card nobody
confirmed drifted into a queue nobody owned. Now the host opens a 60-second wrap-up first:

- `POST /api/v1/meetings/{id}/wrap-up/start`  the host (or creator) opens it; idempotent: a
  second start keeps the first start time, so the deadline never moves.
- `POST /api/v1/meetings/{id}/wrap-up/cancel` "Keep the meeting going": clears it.
- `GET  /api/v1/meetings/{id}/wrap-up`        what is left: the surfaced cards still proposed,
  the agenda items still open, which proposed actions have no owner, and the deadline. Any
  participant may read it (each piece is already visible to participants elsewhere).
- `POST /api/v1/meetings/{id}/wrap-up/close`  the host closes: every card still `proposed`
  gets `review_owner_id` (the named owner, by default the host), the meeting becomes `ended`
  with `closed_by`/`closed_at`, pending status requests expire, and the ONE `meeting-events`
  `ended` outbox row is written, all in one transaction. The same event an ordinary end emits,
  so nothing downstream changes. Repeating a close returns the stored result (idempotent);
  closing a meeting that was ended some other way is 409.

The wrap-up deadline is a fact the room shares (`wrap_up_started_at` + 60 s on the meeting), so
every client counts down to the same second and a reload agrees with live; the host's client
closes when it runs out. A host who drops mid-wrap-up leaves the meeting `live` with the wrap-up
open; the next close (by them, or the creator) finishes it.

"Late joiners". `GET /api/v1/meetings/{id}/catch-up-summary`: what has happened so far, for
someone who joins after the start: the confirmed decisions and actions, how far the agenda is,
and the item now in progress. Participants only, straight from the database.

Access everywhere: 404 for a meeting this tenant cannot see (RLS), then 403 `NOT_A_PARTICIPANT`
for reads by a non-participant, 403 `FORBIDDEN_ROLE` for writes by anyone but the host or the
creator (ADR-0038 D6).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_MEETING_CLOSED,
    ACTION_MEETING_WRAP_UP_CANCELLED,
    ACTION_MEETING_WRAP_UP_STARTED,
    audit,
)
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.confidence import thresholds
from services.core_api.routes.chat import _not_a_participant, _require_participant
from services.core_api.routes.meetings import (
    _CLOSED,
    _fetch_meeting,
    _field_error,
    _load_agenda,
    _participant_ids,
    _require_host_or_creator,
    expire_status_requests,
)
from services.core_api.schemas import AgendaItem, MeetingStatus

router = APIRouter(prefix="/api/v1", tags=["meetings"])

PRODUCER = "core-api"
WRAP_UP_SECONDS = 60
MAX_PENDING = 200


class WrapUpCard(BaseModel):
    """A decision or action card nobody has confirmed or dismissed yet."""

    id: uuid.UUID
    kind: Literal["decision", "action"]
    content: str
    rationale: str | None = None
    assignee_id: uuid.UUID | None = None
    due_date: date | None = None
    created_at: datetime


class WrapUp(BaseModel):
    """What is left before the meeting closes, and the shared deadline."""

    meeting_id: uuid.UUID
    status: MeetingStatus
    seconds: int = Field(description="The length of the wrap-up (60).")
    wrap_up_started_at: datetime | None
    wrap_up_by: uuid.UUID | None
    deadline: datetime | None = Field(description="`wrap_up_started_at` + 60 s, or null.")
    closed_at: datetime | None
    closed_by: uuid.UUID | None
    pending: list[WrapUpCard] = Field(description="Surfaced cards still proposed, oldest first.")
    open_items: list[AgendaItem] = Field(description="Agenda items still pending or in progress.")
    unowned_action_ids: list[uuid.UUID] = Field(
        description="The pending action cards that have no owner yet."
    )


class WrapUpClose(BaseModel):
    model_config = ConfigDict(extra="forbid")

    owner_user_id: uuid.UUID | None = Field(
        default=None,
        description="Who reviews the cards nobody confirmed (a participant). Default: you.",
    )
    reason: Literal["host", "timer"] = Field(
        default="host", description="`timer` when the wrap-up ran out, for the audit trail."
    )


class WrapUpClosed(BaseModel):
    meeting_id: uuid.UUID
    status: MeetingStatus
    closed_at: datetime
    closed_by: uuid.UUID | None
    review_owner_id: uuid.UUID | None = Field(
        description="Who now owns reviewing the leftover cards (null when none were left)."
    )
    moved_decisions: int
    moved_actions: int


class CatchUpDecisionLine(BaseModel):
    id: uuid.UUID
    content: str
    confirmed_by: uuid.UUID | None
    created_at: datetime


class CatchUpActionLine(BaseModel):
    id: uuid.UUID
    content: str
    assignee_id: uuid.UUID | None
    due_date: date | None
    created_at: datetime


class CatchUpSummary(BaseModel):
    """`GET /meetings/{id}/catch-up-summary`: so far in this meeting."""

    meeting_id: uuid.UUID
    status: MeetingStatus
    decision_count: int
    action_count: int
    decisions: list[CatchUpDecisionLine]
    actions: list[CatchUpActionLine]
    agenda_done: int = Field(description="Agenda items covered or skipped.")
    agenda_total: int
    current_item: AgendaItem | None = Field(description="The item in progress, if any.")


# --- helpers ---------------------------------------------------------------------------------


async def _participant(session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID) -> Any:
    row = await _fetch_meeting(session, meeting_id)
    if ctx.user_id is None:
        raise _not_a_participant()
    await _require_participant(session, meeting_id, ctx.user_id)
    return row


def _deadline(started: datetime | None) -> datetime | None:
    return started + timedelta(seconds=WRAP_UP_SECONDS) if started is not None else None


async def _wrap_up(session: AsyncSession, row: Any) -> WrapUp:
    meeting_id = row["id"]
    overrides = (
        await session.execute(text("SELECT agent_confidence_overrides FROM tenant_settings"))
    ).scalar()
    decision_surface, _ = thresholds(overrides, "DecisionAgent")
    action_surface, _ = thresholds(overrides, "ActionAgent")
    cards = (
        (
            await session.execute(
                text(
                    "SELECT * FROM ("
                    " SELECT id, 'decision' AS kind, content, rationale, NULL::uuid AS assignee_id,"
                    " NULL::date AS due_date, created_at FROM decisions WHERE meeting_id = :m"
                    " AND status = 'proposed' AND deleted_at IS NULL AND confidence > :ds"
                    " UNION ALL"
                    " SELECT id, 'action', content, NULL, assignee_id, due_date, created_at"
                    " FROM action_items WHERE meeting_id = :m AND status = 'proposed'"
                    " AND deleted_at IS NULL AND confidence > :as"
                    ") c ORDER BY created_at, id LIMIT :limit"
                ),
                {
                    "m": str(meeting_id),
                    "ds": decision_surface,
                    "as": action_surface,
                    "limit": MAX_PENDING,
                },
            )
        )
        .mappings()
        .all()
    )
    pending = [WrapUpCard(**dict(c)) for c in cards]
    agenda = await _load_agenda(session, meeting_id)
    return WrapUp(
        meeting_id=meeting_id,
        status=row["status"],
        seconds=WRAP_UP_SECONDS,
        wrap_up_started_at=row["wrap_up_started_at"],
        wrap_up_by=row["wrap_up_by"],
        deadline=_deadline(row["wrap_up_started_at"]),
        closed_at=row["closed_at"],
        closed_by=row["closed_by"],
        pending=pending,
        open_items=[it for it in agenda if it.status in ("pending", "in_progress")],
        unowned_action_ids=[c.id for c in pending if c.kind == "action" and c.assignee_id is None],
    )


def _ended_conflict() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_409_CONFLICT, ErrorCode.VERSION_CONFLICT, "The meeting has already ended"
    )


async def _refetch(session: AsyncSession, meeting_id: uuid.UUID) -> Any:
    return await _fetch_meeting(session, meeting_id)


# --- wrap-up ---------------------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/wrap-up", response_model=WrapUp)
async def get_wrap_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> WrapUp:
    """What is left before the meeting closes (participants)."""
    row = await _participant(session, ctx, meeting_id)
    return await _wrap_up(session, row)


@router.post("/meetings/{meeting_id}/wrap-up/start", response_model=WrapUp)
async def start_wrap_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> WrapUp:
    """The host opens the 60-second wrap-up. Idempotent: the first start time stands."""
    row = await _fetch_meeting(session, meeting_id, for_update=True)
    await _require_host_or_creator(session, ctx, row, "how the meeting ends")
    if row["status"] in _CLOSED:
        raise _ended_conflict()
    if row["wrap_up_started_at"] is None:
        await session.execute(
            text(
                "UPDATE meetings SET wrap_up_started_at = now(), wrap_up_by = :u, "
                "updated_at = now() WHERE id = :id"
            ),
            {"u": str(ctx.user_id) if ctx.user_id else None, "id": str(meeting_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_MEETING_WRAP_UP_STARTED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={},
            ip=ctx.ip,
        )
    return await _wrap_up(session, await _refetch(session, meeting_id))


@router.post("/meetings/{meeting_id}/wrap-up/cancel", response_model=WrapUp)
async def cancel_wrap_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> WrapUp:
    """ "Keep the meeting going": the wrap-up closes and the meeting carries on."""
    row = await _fetch_meeting(session, meeting_id, for_update=True)
    await _require_host_or_creator(session, ctx, row, "how the meeting ends")
    if row["status"] in _CLOSED:
        raise _ended_conflict()
    if row["wrap_up_started_at"] is not None:
        await session.execute(
            text(
                "UPDATE meetings SET wrap_up_started_at = NULL, wrap_up_by = NULL, "
                "updated_at = now() WHERE id = :id"
            ),
            {"id": str(meeting_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_MEETING_WRAP_UP_CANCELLED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={},
            ip=ctx.ip,
        )
    return await _wrap_up(session, await _refetch(session, meeting_id))


@router.post("/meetings/{meeting_id}/wrap-up/close", response_model=WrapUpClosed)
async def close_meeting(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: WrapUpClose | None = None,
) -> WrapUpClosed:
    """End the meeting for everyone and hand every card nobody confirmed to a named owner."""
    body = body or WrapUpClose()
    row = await _fetch_meeting(session, meeting_id, for_update=True)
    await _require_host_or_creator(session, ctx, row, "how the meeting ends")
    m = str(meeting_id)

    if row["status"] == "ended" and row["closed_at"] is not None:
        # Already closed through the wrap-up: the stored outcome, nothing new written.
        counts = (
            await session.execute(
                text(
                    "SELECT (SELECT count(*) FROM decisions WHERE meeting_id = :m "
                    "  AND status = 'proposed' AND review_owner_id IS NOT NULL "
                    "  AND deleted_at IS NULL), "
                    "(SELECT count(*) FROM action_items WHERE meeting_id = :m "
                    "  AND status = 'proposed' AND review_owner_id IS NOT NULL "
                    "  AND deleted_at IS NULL), "
                    "(SELECT review_owner_id FROM decisions WHERE meeting_id = :m "
                    "  AND review_owner_id IS NOT NULL UNION ALL "
                    " SELECT review_owner_id FROM action_items WHERE meeting_id = :m "
                    "  AND review_owner_id IS NOT NULL LIMIT 1)"
                ),
                {"m": m},
            )
        ).one()
        return WrapUpClosed(
            meeting_id=meeting_id,
            status=row["status"],
            closed_at=row["closed_at"],
            closed_by=row["closed_by"],
            review_owner_id=counts[2],
            moved_decisions=int(counts[0]),
            moved_actions=int(counts[1]),
        )
    if row["status"] in _CLOSED:
        raise _ended_conflict()

    owner = body.owner_user_id or ctx.user_id
    if owner is None or owner not in await _participant_ids(session, meeting_id):
        raise _field_error(["owner_user_id"], "The owner must be a participant of the meeting")

    moved: dict[str, int] = {}
    for table in ("decisions", "action_items"):
        moved[table] = len(
            (
                await session.execute(
                    text(
                        f"UPDATE {table} SET review_owner_id = :o "  # noqa: S608 - closed set
                        "WHERE meeting_id = :m AND status = 'proposed' AND deleted_at IS NULL "
                        "RETURNING id"
                    ),
                    {"o": str(owner), "m": m},
                )
            ).all()
        )
    closed = (
        (
            await session.execute(
                text(
                    "UPDATE meetings SET status = 'ended', closed_at = now(), closed_by = :u, "
                    "updated_at = now() WHERE id = :id "
                    "RETURNING status, closed_at, closed_by, livekit_room, updated_at"
                ),
                {"u": str(ctx.user_id) if ctx.user_id else None, "id": m},
            )
        )
        .mappings()
        .one()
    )
    await expire_status_requests(session, meeting_id)
    # The same `ended` fact `PATCH /meetings/{id}` emits, so every consumer (meeting-service
    # closing the room, the lifecycle workflow, the follow-up) behaves exactly as before.
    await enqueue(
        session,
        EventEnvelope(
            topic="meeting-events",
            payload={
                "meeting_id": m,
                "event_type": "ended",
                "livekit_room": closed["livekit_room"],
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"meeting:{meeting_id}:ended:{closed['updated_at'].isoformat()}",
        ),
    )
    review_owner = owner if moved["decisions"] or moved["action_items"] else None
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_CLOSED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={
            "reason": body.reason,
            "review_owner_id": str(owner),
            "moved_decisions": moved["decisions"],
            "moved_actions": moved["action_items"],
        },
        ip=ctx.ip,
    )
    return WrapUpClosed(
        meeting_id=meeting_id,
        status=closed["status"],
        closed_at=closed["closed_at"],
        closed_by=closed["closed_by"],
        review_owner_id=review_owner,
        moved_decisions=moved["decisions"],
        moved_actions=moved["action_items"],
    )


# --- late joiners ----------------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/catch-up-summary", response_model=CatchUpSummary)
async def catch_up_summary(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CatchUpSummary:
    """So far in this meeting, for someone joining late (participants)."""
    row = await _participant(session, ctx, meeting_id)
    params = {"m": str(meeting_id)}
    decisions = (
        (
            await session.execute(
                text(
                    "SELECT id, content, confirmed_by, created_at FROM decisions "
                    "WHERE meeting_id = :m AND status = 'confirmed' AND deleted_at IS NULL "
                    "ORDER BY created_at, id"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    actions = (
        (
            await session.execute(
                text(
                    "SELECT id, content, assignee_id, due_date, created_at FROM action_items "
                    "WHERE meeting_id = :m AND status IN ('confirmed', 'done') "
                    "AND deleted_at IS NULL ORDER BY created_at, id"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    agenda = await _load_agenda(session, meeting_id)
    current = next((it for it in agenda if it.status == "in_progress"), None)
    return CatchUpSummary(
        meeting_id=meeting_id,
        status=row["status"],
        decision_count=len(decisions),
        action_count=len(actions),
        decisions=[CatchUpDecisionLine(**dict(d)) for d in decisions],
        actions=[CatchUpActionLine(**dict(a)) for a in actions],
        agenda_done=sum(1 for it in agenda if it.status in ("covered", "skipped")),
        agenda_total=len(agenda),
        current_item=current,
    )
