"""Settle threads: two owners of clashing decisions from different teams (Beyond, Layer 1).

A thread is opened by the `agent-results` consumer when the existing ProactiveAgent conflict
check finds that a newly confirmed decision may contradict another team's confirmed decision
(`services/core_api/decision_why.py`, `apply_conflict`). Only its two owners can read or act on
it; anyone else, in any tenant, gets a 404.

What each side sees. A decision is visible to the people who were in its meeting. In the thread
the other owner's wording is hidden until that owner chooses to share it (`share`), unless the
viewer was in that meeting anyway. The agent's reason for the clash quotes both decisions, so it
is shown only when the viewer can see both sides.

Outcomes (each audited; a repeat of the same final outcome is a no-op, a different one is 409):
- `keep-both` `{note}`: both stand, with a note for the other owner;
- `supersede` `{winner_decision_id}`: if the caller owns the LOSING side it applies at once (the
  winner gains `supersedes` through `candidates.supersede_confirmed`, the existing lineage path);
  if the caller owns the winning side it becomes a proposal the other owner `accept`s or
  `decline`s;
- `book` `{start?}`: a 15-minute meeting with both owners (the agent proposes the slot,
  `meeting-slot`).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_SETTLEMENT_DECLINED,
    ACTION_SETTLEMENT_KEPT_BOTH,
    ACTION_SETTLEMENT_MEETING_BOOKED,
    ACTION_SETTLEMENT_MESSAGE,
    ACTION_SETTLEMENT_PROPOSED,
    ACTION_SETTLEMENT_SHARED,
    ACTION_SETTLEMENT_SUPERSEDED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import decision_why as why
from services.core_api.decision_why import Person
from services.core_api.routes.candidates import supersede_confirmed
from services.core_api.routes.decision_why import Booked, BookRequest, SlotProposal

router = APIRouter(prefix="/api/v1", tags=["settlements"])

MAX_LIST = 100
SettleStatus = Literal["open", "kept_both", "superseded", "meeting_booked"]
GENERIC_REASON = "Mytheus thinks these two decisions may pull in different directions."


class SettleSide(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    decided_at: datetime
    owner: Person | None = None
    team: str | None = None
    content: str | None = None
    quote: str | None = None
    quote_speaker: Person | None = None
    visible: bool
    shared: bool
    mine: bool


class Proposal(BaseModel):
    winner_decision_id: uuid.UUID
    by_me: bool
    at: datetime


class Resolution(BaseModel):
    by: Person | None = None
    at: datetime
    note: str | None = None
    winner_decision_id: uuid.UUID | None = None
    meeting_id: uuid.UUID | None = None


class Message(BaseModel):
    id: uuid.UUID
    author: Person | None = None
    mine: bool
    body: str
    created_at: datetime


class Settlement(BaseModel):
    id: uuid.UUID
    status: SettleStatus
    created_at: datetime
    confidence: float
    rationale: str
    mine: SettleSide
    theirs: SettleSide
    proposal: Proposal | None = None
    resolution: Resolution | None = None
    messages: list[Message]


class SettlementList(BaseModel):
    items: list[Settlement]


def _clean(v: str) -> str:
    out = " ".join(v.split())
    if not out:
        raise ValueError("cannot be empty")
    return out


class MessageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    body: Annotated[str, Field(min_length=1, max_length=1000)]

    @field_validator("body")
    @classmethod
    def _body(cls, v: str) -> str:
        return _clean(v)


class KeepBoth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: Annotated[str, Field(min_length=1, max_length=500)]

    @field_validator("note")
    @classmethod
    def _note(cls, v: str) -> str:
        return _clean(v)


class Supersede(BaseModel):
    model_config = ConfigDict(extra="forbid")

    winner_decision_id: uuid.UUID


# --- reading -------------------------------------------------------------------------------

_SQL = (
    "SELECT s.*, c.confidence, c.rationale FROM decision_settlements s "
    "JOIN decision_clashes c ON c.id = s.clash_id "
)


async def _row(
    session: AsyncSession,
    settlement_id: uuid.UUID,
    user_id: uuid.UUID | None,
    *,
    lock: bool = False,
) -> dict[str, Any]:
    if user_id is None:
        raise not_found()
    row = (
        (
            await session.execute(
                text(
                    _SQL
                    + "WHERE s.id = :id AND (s.owner_a = :u OR s.owner_b = :u)"
                    + (" FOR UPDATE OF s" if lock else "")
                ),
                {"id": str(settlement_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return dict(row)


async def _side(
    session: AsyncSession, decision_id: uuid.UUID, shared: bool, viewer: uuid.UUID
) -> SettleSide:
    d = await why.decision_row(session, decision_id)
    if d is None:
        raise not_found()
    mine = d["confirmed_by"] == viewer
    visible = mine or shared or await why.in_meeting(session, d["meeting_id"], viewer)
    return SettleSide(
        decision_id=d["id"],
        meeting_id=d["meeting_id"],
        meeting_title=d["meeting_title"] if visible else None,
        decided_at=d["decided_at"],
        owner=await why.person(session, d["confirmed_by"]),
        team=await why.team_name(session, d["confirmed_by"]),
        content=d["content"] if visible else None,
        quote=d["quote"] if visible else None,
        quote_speaker=await why.person(session, d["quote_speaker_id"]) if visible else None,
        visible=visible,
        shared=shared,
        mine=mine,
    )


async def build(session: AsyncSession, row: dict[str, Any], viewer: uuid.UUID) -> Settlement:
    a = await _side(session, row["decision_a_id"], row["a_shared"], viewer)
    b = await _side(session, row["decision_b_id"], row["b_shared"], viewer)
    viewer_is_a = row["owner_a"] == viewer
    mine, theirs = (a, b) if viewer_is_a else (b, a)
    messages = (
        (
            await session.execute(
                text(
                    "SELECT id, author_id, body, created_at FROM settlement_messages "
                    "WHERE settlement_id = :s ORDER BY created_at, id LIMIT 200"
                ),
                {"s": str(row["id"])},
            )
        )
        .mappings()
        .all()
    )
    names = await why.people(
        session, [m["author_id"] for m in messages] + [row["resolved_by"], row["proposal_by"]]
    )
    proposal = None
    if row["proposal_winner_id"] is not None and row["status"] == "open":
        proposal = Proposal(
            winner_decision_id=row["proposal_winner_id"],
            by_me=row["proposal_by"] == viewer,
            at=row["proposal_at"],
        )
    resolution = None
    if row["status"] != "open" and row["resolved_at"] is not None:
        resolution = Resolution(
            by=names.get(row["resolved_by"]),
            at=row["resolved_at"],
            note=row["resolution_note"],
            winner_decision_id=row["winner_decision_id"],
            meeting_id=row["meeting_id"],
        )
    return Settlement(
        id=row["id"],
        status=row["status"],
        created_at=row["created_at"],
        confidence=float(row["confidence"]),
        rationale=(
            (row["rationale"] or GENERIC_REASON) if (a.visible and b.visible) else GENERIC_REASON
        ),
        mine=mine,
        theirs=theirs,
        proposal=proposal,
        resolution=resolution,
        messages=[
            Message(
                id=m["id"],
                author=names.get(m["author_id"]),
                mine=m["author_id"] == viewer,
                body=m["body"],
                created_at=m["created_at"],
            )
            for m in messages
        ],
    )


@router.get("/settlements", response_model=SettlementList)
async def list_settlements(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_: Annotated[Literal["open", "all"], Query(alias="status")] = "open",
) -> SettlementList:
    if ctx.user_id is None:
        return SettlementList(items=[])
    rows = (
        (
            await session.execute(
                text(
                    _SQL
                    + "WHERE (s.owner_a = :u OR s.owner_b = :u)"
                    + (" AND s.status = 'open'" if status_ == "open" else "")
                    + " ORDER BY s.created_at DESC, s.id LIMIT :n"
                ),
                {"u": str(ctx.user_id), "n": MAX_LIST},
            )
        )
        .mappings()
        .all()
    )
    return SettlementList(items=[await build(session, dict(r), ctx.user_id) for r in rows])


@router.get("/settlements/{settlement_id}", response_model=Settlement)
async def get_settlement(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    row = await _row(session, settlement_id, ctx.user_id)
    assert ctx.user_id is not None
    return await build(session, row, ctx.user_id)


# --- acting -------------------------------------------------------------------------------


def _other(row: dict[str, Any], me: uuid.UUID) -> uuid.UUID:
    other: uuid.UUID = row["owner_b"] if row["owner_a"] == me else row["owner_a"]
    return other


def _final(row: dict[str, Any], wanted: str) -> bool:
    """True when already resolved as `wanted` (a no-op); 409 for any other final outcome."""
    if row["status"] == "open":
        return False
    if row["status"] == wanted:
        return True
    raise ProblemDetail(
        409, ErrorCode.VERSION_CONFLICT, f"This was already settled ({row['status']})"
    )


async def _tell(
    session: AsyncSession, ctx: RequestContext, row: dict[str, Any], key: str, title: str, body: str
) -> None:
    assert ctx.user_id is not None
    await why.notify(
        session,
        ctx.org_id,
        _other(row, ctx.user_id),
        source_event_id=f"settle:{row['id']}:{key}",
        title=title,
        body=body,
        link=f"/work/settle/{row['id']}",
    )


async def _audit(
    session: AsyncSession, ctx: RequestContext, row: dict[str, Any], action: str, **meta: Any
) -> None:
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=action,
        target_type="decision",
        target_id=row["decision_a_id"],
        metadata={"settlement_id": str(row["id"]), **meta},
        ip=ctx.ip,
    )


async def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


@router.post("/settlements/{settlement_id}/messages", response_model=Settlement)
async def post_message(
    settlement_id: uuid.UUID,
    body: MessageIn,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    msg = (
        await session.execute(
            text(
                "INSERT INTO settlement_messages (org_id, settlement_id, author_id, body) "
                "VALUES (:o, :s, :u, :b) RETURNING id"
            ),
            {"o": str(ctx.org_id), "s": str(settlement_id), "u": str(me), "b": body.body},
        )
    ).one()
    author = await why.person(session, me)
    await _tell(
        session,
        ctx,
        row,
        f"msg:{msg[0]}",
        f"{author.name if author else 'The other owner'} replied in your settle thread",
        why.clip(body.body, 300),
    )
    await _audit(session, ctx, row, ACTION_SETTLEMENT_MESSAGE, message_id=str(msg[0]))
    return await build(session, row, me)


@router.post("/settlements/{settlement_id}/share", response_model=Settlement)
async def share_wording(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    column = "a_shared" if row["owner_a"] == me else "b_shared"
    if not row[column]:
        await session.execute(
            text(f"UPDATE decision_settlements SET {column} = true WHERE id = :s"),  # noqa: S608
            {"s": str(settlement_id)},
        )
        row[column] = True
        await _tell(
            session,
            ctx,
            row,
            f"shared:{me}",
            "The other owner shared their decision with you",
            "You can now read both decisions side by side.",
        )
        await _audit(session, ctx, row, ACTION_SETTLEMENT_SHARED)
    return await build(session, row, me)


async def _resolve(
    session: AsyncSession,
    row: dict[str, Any],
    me: uuid.UUID,
    to: str,
    *,
    note: str | None = None,
    winner: uuid.UUID | None = None,
    meeting_id: uuid.UUID | None = None,
) -> None:
    await session.execute(
        text(
            "UPDATE decision_settlements SET status = :st, resolved_by = :u, resolved_at = now(), "
            "resolution_note = :n, winner_decision_id = :w, meeting_id = :m, "
            "proposal_winner_id = NULL, proposal_by = NULL, proposal_at = NULL WHERE id = :s"
        ),
        {
            "st": to,
            "u": str(me),
            "n": note,
            "w": str(winner) if winner else None,
            "m": str(meeting_id) if meeting_id else None,
            "s": str(row["id"]),
        },
    )
    row.update(
        status=to,
        resolved_by=me,
        resolved_at=datetime.now(),
        resolution_note=note,
        winner_decision_id=winner,
        meeting_id=meeting_id,
        proposal_winner_id=None,
    )


@router.post("/settlements/{settlement_id}/keep-both", response_model=Settlement)
async def keep_both(
    settlement_id: uuid.UUID,
    body: KeepBoth,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    if not _final(row, "kept_both"):
        await _resolve(session, row, me, "kept_both", note=body.note)
        await _tell(
            session,
            ctx,
            row,
            "kept_both",
            "Both decisions stand",
            f"The other owner kept both decisions: {why.clip(body.note, 300)}",
        )
        await _audit(session, ctx, row, ACTION_SETTLEMENT_KEPT_BOTH)
    return await get_settlement(settlement_id, ctx, session)


async def _apply_supersede(
    session: AsyncSession,
    ctx: RequestContext,
    row: dict[str, Any],
    me: uuid.UUID,
    winner: uuid.UUID,
) -> None:
    loser = row["decision_b_id"] if winner == row["decision_a_id"] else row["decision_a_id"]
    await supersede_confirmed(session, ctx, winner_id=winner, loser_id=loser, actor_id=me)
    await _resolve(session, row, me, "superseded", winner=winner)
    await _audit(session, ctx, row, ACTION_SETTLEMENT_SUPERSEDED, winner_decision_id=str(winner))


@router.post("/settlements/{settlement_id}/supersede", response_model=Settlement)
async def supersede(
    settlement_id: uuid.UUID,
    body: Supersede,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    winner = body.winner_decision_id
    if winner not in (row["decision_a_id"], row["decision_b_id"]):
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "Choose one of the two decisions in this thread"
        )
    if _final(row, "superseded"):
        if row["winner_decision_id"] != winner:
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "This was already settled")
        return await build(session, row, me)
    mine = row["decision_a_id"] if row["owner_a"] == me else row["decision_b_id"]
    if winner != mine:
        # I give way: mine is replaced by theirs. Mine to give, so it applies now.
        await _apply_supersede(session, ctx, row, me, winner)
        await _tell(
            session,
            ctx,
            row,
            "superseded",
            "Your decision stands: the other one now follows it",
            "The other owner agreed that your decision replaces theirs.",
        )
    elif row["proposal_winner_id"] != winner:
        await session.execute(
            text(
                "UPDATE decision_settlements SET proposal_winner_id = :w, proposal_by = :u, "
                "proposal_at = now() WHERE id = :s"
            ),
            {"w": str(winner), "u": str(me), "s": str(settlement_id)},
        )
        await _tell(
            session,
            ctx,
            row,
            f"proposal:{winner}",
            "Asked: may their decision replace yours?",
            "The other owner proposes that their decision replaces yours. Accept or decline.",
        )
        await _audit(session, ctx, row, ACTION_SETTLEMENT_PROPOSED, winner_decision_id=str(winner))
    return await get_settlement(settlement_id, ctx, session)


@router.post("/settlements/{settlement_id}/accept", response_model=Settlement)
async def accept(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    if row["status"] == "superseded":
        return await build(session, row, me)
    _final(row, "superseded")
    winner = row["proposal_winner_id"]
    if winner is None or row["proposal_by"] == me:
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "There is no proposal for you to accept"
        )
    await _apply_supersede(session, ctx, row, me, winner)
    await _tell(
        session,
        ctx,
        row,
        "accepted",
        "Your proposal was accepted",
        "Your decision now replaces the other one. Its task owners were asked what to do.",
    )
    return await get_settlement(settlement_id, ctx, session)


@router.post("/settlements/{settlement_id}/decline", response_model=Settlement)
async def decline(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Settlement:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    if row["status"] != "open" or row["proposal_winner_id"] is None:
        return await build(session, row, me)
    if row["proposal_by"] == me:
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "You cannot decline your own proposal")
    await session.execute(
        text(
            "UPDATE decision_settlements SET proposal_winner_id = NULL, proposal_by = NULL, "
            "proposal_at = NULL WHERE id = :s"
        ),
        {"s": str(settlement_id)},
    )
    await _tell(
        session,
        ctx,
        row,
        f"declined:{row['proposal_at'].isoformat() if row['proposal_at'] else ''}",
        "Your proposal was declined",
        "The other owner wants both decisions to stand for now, or to meet.",
    )
    await _audit(session, ctx, row, ACTION_SETTLEMENT_DECLINED)
    return await get_settlement(settlement_id, ctx, session)


async def _owners(row: dict[str, Any]) -> list[uuid.UUID]:
    return [row["owner_a"], row["owner_b"]]


def _meet_title(row_a: dict[str, Any] | None) -> str:
    return "Settle two decisions" + (f": {why.clip(row_a['content'], 80)}" if row_a else "")


@router.get("/settlements/{settlement_id}/meeting-slot", response_model=SlotProposal)
async def meeting_slot(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> SlotProposal:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me)
    ids = await _owners(row)
    slot = await why.propose_slot(session, ids)
    names = await why.people(session, list(ids))
    mine = row["decision_a_id"] if row["owner_a"] == me else row["decision_b_id"]
    return SlotProposal(
        start=slot.start,
        end=slot.end,
        people=[names[i] for i in ids if i in names],
        title=_meet_title(await why.decision_row(session, mine)),
    )


@router.post(
    "/settlements/{settlement_id}/book",
    response_model=Booked,
    status_code=status.HTTP_201_CREATED,
)
async def book(
    settlement_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: BookRequest | None = None,
) -> Booked:
    me = await _me(ctx)
    row = await _row(session, settlement_id, me, lock=True)
    if row["status"] == "meeting_booked" and row["meeting_id"] is not None:
        m = (
            (
                await session.execute(
                    text(
                        "SELECT id, title, scheduled_start, scheduled_end FROM meetings "
                        "WHERE id = :m"
                    ),
                    {"m": str(row["meeting_id"])},
                )
            )
            .mappings()
            .first()
        )
        if m is not None and m["scheduled_start"] is not None:
            return Booked(
                meeting_id=m["id"],
                start=m["scheduled_start"],
                end=m["scheduled_end"] or m["scheduled_start"],
                title=m["title"],
            )
    _final(row, "meeting_booked")
    ids = await _owners(row)
    start = why.parse_start(body.start if body else None)
    if start is None:
        start = (await why.propose_slot(session, ids)).start
    elif start < datetime.now(start.tzinfo):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Pick a time in the future")
    mine = row["decision_a_id"] if row["owner_a"] == me else row["decision_b_id"]
    booked = await why.book_meeting(
        session,
        ctx,
        title=_meet_title(await why.decision_row(session, mine)),
        start=start,
        minutes=why.REVISIT_MINUTES,
        attendees=ids,
        agenda_title="Settle the two decisions: keep both, or which one stands",
    )
    await _resolve(session, row, me, "meeting_booked", meeting_id=booked["meeting_id"])
    await _tell(
        session,
        ctx,
        row,
        "meeting_booked",
        "A meeting was booked to settle two decisions",
        f"15 minutes on {start:%a %d %b, %H:%M} UTC with both owners.",
    )
    await _audit(
        session, ctx, row, ACTION_SETTLEMENT_MEETING_BOOKED, meeting_id=str(booked["meeting_id"])
    )
    return Booked(**booked)
