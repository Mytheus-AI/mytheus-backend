"""Decision / action candidates: review, edit, confirm, dismiss. Plan 7-9 section 3.6.

Phase 8 Part A contract section 2. `DecisionAgent` / `ActionAgent` only ever produce
candidates; `core-api`'s `agent-results` consumer stores them as `status = 'proposed'` with no
event. THIS MODULE IS THE ONLY PRODUCER of the `decisions` and `actions` topics, and only on
confirm: `confirm_decision` / `confirm_action` move the row to `confirmed` and enqueue exactly
one outbox row (`decision:{id}:confirmed` / `action:{id}:confirmed`) plus an audit row, in the
same `tenant_session` transaction, so the state change and the event commit or roll back
together. `tests/test_candidates.py::test_only_the_confirm_routes_enqueue_decisions_or_actions`
fails the build if any other file starts producing them.

Access: `require_tier("member")`, and the caller must be a participant of the row's meeting
(403 `NOT_A_PARTICIPANT`). Another tenant's row or meeting is invisible to RLS, so it is a 404,
never a 403. Every caller-supplied id that becomes a foreign key (`assignee_id`, `supersedes`)
is first checked through the tenant session, because a foreign key resolves rows across
tenants (gap 31, `services/core_api/visibility.py`).

Status rules (409 `VERSION_CONFLICT` otherwise):
- PATCH only while `proposed`.
- confirm: `proposed` -> `confirmed`; already `confirmed` -> 200, no new event.
- dismiss: `proposed` -> `dismissed`, no decisions/actions event; already `dismissed` -> 200.

Polish wave 2 (agents that learn): every real transition (an edit that changes a value, a
confirm, a dismiss) also records one `agent_feedback` row and its `agent-feedback` event, in the
same transaction (`services/core_api/learning.py`). A repeat that changes nothing records
nothing. The reload read applies the workspace's calibrated threshold (`learning.band_for`).
Rows are read `FOR UPDATE`, so a confirm racing a dismiss serialises and the loser gets 409.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_ACTION_CONFIRMED,
    ACTION_ACTION_DISMISSED,
    ACTION_DECISION_CONFIRMED,
    ACTION_DECISION_DISMISSED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.learning import band_for, edit_diff, record_feedback
from services.core_api.meeting_ledger import mark_candidate
from services.core_api.routes.chat import (
    _fetch_meeting_exists,
    _not_a_participant,
    _require_participant,
)
from services.core_api.routes.decision_reversals import record_reversals
from services.core_api.schemas import (
    ActionItem,
    ActionItemUpdate,
    CandidateList,
    CandidateState,
    Decision,
    DecisionConfirm,
    DecisionUpdate,
)
from services.core_api.visibility import is_visible

router = APIRouter(prefix="/api/v1")

PRODUCER = "core-api"
_EPOCH = date(1970, 1, 1)

# polish program (one card per moment): the moment, the words and the why line ride every read
_MOMENT_COLS = "moment_id, why, quote, quote_speaker_id, quote_start_ms, quote_end_ms"
_DECISION_COLS = (
    "id, meeting_id, content, rationale, confidence, status, confirmed_by, supersedes, created_at, "
    + _MOMENT_COLS
)
_ACTION_COLS = (
    "id, meeting_id, content, assignee_id, due_date, confidence, status, confirmed_by, created_at, "
    + _MOMENT_COLS
)

# A meeting's candidates are bounded by its windows (~90 per hour per agent), so one page.
MAX_CANDIDATES = 500


def _conflict(detail: str) -> ProblemDetail:
    return ProblemDetail(status.HTTP_409_CONFLICT, ErrorCode.VERSION_CONFLICT, detail)


async def _require_meeting_participant(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> uuid.UUID:
    """404 for a meeting this tenant cannot see, then 403 for a non-participant."""
    await _fetch_meeting_exists(session, meeting_id)
    if ctx.user_id is None:
        raise _not_a_participant()
    await _require_participant(session, meeting_id, ctx.user_id)
    return ctx.user_id


async def _locked_row(
    session: AsyncSession, ctx: RequestContext, table: str, cols: str, row_id: uuid.UUID
) -> tuple[dict[str, Any], uuid.UUID]:
    """The live row, locked for this transaction, plus the caller's verified user id."""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {cols} FROM {table} "  # noqa: S608 - closed set of identifiers
                    "WHERE id = :id AND deleted_at IS NULL FOR UPDATE"
                ),
                {"id": str(row_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    user_id = await _require_meeting_participant(session, ctx, row["meeting_id"])
    return dict(row), user_id


# --- read ----------------------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/candidates", response_model=CandidateList, tags=["candidates"])
async def list_candidates(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    include_resolved: Annotated[bool, Query()] = False,
) -> CandidateList:
    """Proposed, surfaced decision and action candidates for one meeting, oldest first.

    Re-hydrates the agent sidebar on every socket (re)connect. The 0.4-0.7 silent band is
    stored but never served; the surface threshold is the workspace's, per agent: the tenant's
    override or default, moved by the confirm/dismiss calibration (`learning.band_for`).

    `include_resolved=true` (polish program, shared candidate ledger) also returns the
    meeting's CONFIRMED decisions and actions, whatever their confidence (a person confirmed
    them), with `confirmed_by`, so the room can show "Confirmed by Maya" and the live Decisions
    and Actions in Notes mode after a reload. Dismissed rows are never returned.
    """
    await _require_meeting_participant(session, ctx, meeting_id)
    # The workspace's calibrated thresholds, the ones the agent decided `surfaced` with.
    decision_surface, _ = await band_for(session, "DecisionAgent")
    action_surface, _ = await band_for(session, "ActionAgent")
    params = {"m": str(meeting_id), "limit": MAX_CANDIDATES, "resolved": include_resolved}
    decisions = (
        (
            await session.execute(
                text(
                    f"SELECT {_DECISION_COLS} FROM decisions WHERE meeting_id = :m "
                    "AND deleted_at IS NULL AND ((status = 'proposed' AND confidence > :s) "
                    "OR (:resolved AND status = 'confirmed')) "
                    "ORDER BY created_at ASC, id ASC LIMIT :limit"
                ),
                {**params, "s": decision_surface},
            )
        )
        .mappings()
        .all()
    )
    actions = (
        (
            await session.execute(
                text(
                    f"SELECT {_ACTION_COLS} FROM action_items WHERE meeting_id = :m "
                    "AND deleted_at IS NULL AND ((status = 'proposed' AND confidence > :s) "
                    "OR (:resolved AND status = 'confirmed')) "
                    "ORDER BY created_at ASC, id ASC LIMIT :limit"
                ),
                {**params, "s": action_surface},
            )
        )
        .mappings()
        .all()
    )
    return CandidateList(
        decisions=[Decision(**dict(r)) for r in decisions],
        actions=[ActionItem(**dict(r)) for r in actions],
    )


@router.get(
    "/meetings/{meeting_id}/candidates/{candidate_id}",
    response_model=CandidateState,
    tags=["candidates"],
)
async def get_candidate(
    meeting_id: uuid.UUID,
    candidate_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CandidateState:
    """One decision or action candidate of this meeting, in any status.

    The shared candidate ledger: after someone confirms, edits or (as host) dismisses a card,
    their client tells `realtime-gateway`, which reads the row here WITH THAT PERSON'S TOKEN and
    only tells the room what the stored row says. So no client can announce a resolution the
    server does not hold. Participants only; another meeting's id is a 404.
    """
    await _require_meeting_participant(session, ctx, meeting_id)
    params = {"id": str(candidate_id), "m": str(meeting_id)}
    decision = (
        (
            await session.execute(
                text(
                    f"SELECT {_DECISION_COLS} FROM decisions "  # noqa: S608
                    "WHERE id = :id AND meeting_id = :m AND deleted_at IS NULL"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if decision is not None:
        return CandidateState(kind="decision", **{k: decision[k] for k in _STATE_DECISION})
    action = (
        (
            await session.execute(
                text(
                    f"SELECT {_ACTION_COLS} FROM action_items "  # noqa: S608
                    "WHERE id = :id AND meeting_id = :m AND deleted_at IS NULL"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if action is None:
        raise not_found()
    return CandidateState(kind="action", **{k: action[k] for k in _STATE_ACTION})


_MOMENT = ("moment_id", "why", "quote", "quote_speaker_id", "quote_start_ms", "quote_end_ms")
_STATE_DECISION = ("id", "meeting_id", "status", "content", "rationale", "confirmed_by", *_MOMENT)
_STATE_ACTION = (
    "id",
    "meeting_id",
    "status",
    "content",
    "assignee_id",
    "due_date",
    "confirmed_by",
    *_MOMENT,
)


# --- feedback (polish wave 2: agents that learn) -------------------------------------------

_AGENT_OF = {"decision": "DecisionAgent", "action": "ActionAgent"}


async def _feedback(
    session: AsyncSession,
    ctx: RequestContext,
    user_id: uuid.UUID,
    kind: str,
    action: str,
    row: Any,
    diff: dict[str, Any] | None = None,
) -> None:
    """One `agent_feedback` row + `agent-feedback` event for a real transition (a repeated
    confirm or dismiss returned early and records nothing), in this transaction."""
    await record_feedback(
        session,
        org_id=ctx.org_id,
        user_id=user_id,
        meeting_id=row["meeting_id"],
        agent=_AGENT_OF[kind],
        kind=kind,
        candidate_id=row["id"],
        action=action,
        confidence=float(row["confidence"]) if row["confidence"] is not None else None,
        content=row["content"],
        diff=diff,
    )


async def _feedback_edit(
    session: AsyncSession,
    ctx: RequestContext,
    user_id: uuid.UUID,
    kind: str,
    before: dict[str, Any],
    after: Any,
    fields: dict[str, Any],
) -> None:
    """An edit is feedback only when a value actually changed: the diff of the PATCHed fields."""
    diff = edit_diff(
        {k: before.get(k) for k in fields}, {k: after[k] for k in fields if k in after}
    )
    if diff:
        await _feedback(session, ctx, user_id, kind, "edit", after, diff)


# --- decisions -----------------------------------------------------------------------------


@router.patch("/decisions/{decision_id}", response_model=Decision, tags=["decisions"])
async def update_decision(
    decision_id: uuid.UUID,
    body: DecisionUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Decision:
    row, user_id = await _locked_row(session, ctx, "decisions", _DECISION_COLS, decision_id)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} decision can no longer be edited")
    fields = body.model_dump(exclude_unset=True)
    sets = ", ".join(f"{k} = :{k}" for k in fields)  # keys are the model's closed field set
    updated = (
        (
            await session.execute(
                text(
                    f"UPDATE decisions SET {sets}, edited_at = now() "  # noqa: S608
                    f"WHERE id = :id RETURNING {_DECISION_COLS}"
                ),
                {**fields, "id": str(decision_id)},
            )
        )
        .mappings()
        .one()
    )
    # a person's edit is final for the agents: they never update this card again
    await mark_candidate(
        ctx.org_id,
        updated["meeting_id"],
        decision_id,
        kind="decision",
        status="edited",
        text=updated["content"],
    )
    await _feedback_edit(session, ctx, user_id, "decision", row, updated, fields)
    return Decision(**dict(updated))


@router.post("/decisions/{decision_id}/confirm", response_model=Decision, tags=["decisions"])
async def confirm_decision(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: DecisionConfirm | None = None,
) -> Decision:
    """proposed -> confirmed, and the ONE `decisions` outbox row. Idempotent on re-confirm."""
    row, user_id = await _locked_row(session, ctx, "decisions", _DECISION_COLS, decision_id)
    if row["status"] == "confirmed":
        return Decision(**row)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} decision cannot be confirmed")

    supersedes = body.supersedes if body is not None else None
    if supersedes is not None:
        if supersedes == decision_id:
            raise ProblemDetail(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                ErrorCode.VALIDATION_FAILED,
                "A decision cannot supersede itself",
            )
        prior = (
            await session.execute(
                text("SELECT status FROM decisions WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(supersedes)},
            )
        ).first()
        if prior is None:
            raise ProblemDetail(
                status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such decision to supersede"
            )
        if prior[0] != "confirmed":
            raise ProblemDetail(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                ErrorCode.VALIDATION_FAILED,
                "Only a confirmed decision can be superseded",
            )

    confirmed = dict(
        (
            await session.execute(
                text(
                    "UPDATE decisions SET status = 'confirmed', confirmed_by = :u, "
                    f"supersedes = :s WHERE id = :id RETURNING {_DECISION_COLS}"
                ),
                {
                    "u": str(user_id),
                    "s": str(supersedes) if supersedes else None,
                    "id": str(decision_id),
                },
            )
        )
        .mappings()
        .one()
    )
    await enqueue(
        session,
        EventEnvelope(
            topic="decisions",
            payload={
                "decision_id": str(confirmed["id"]),
                "meeting_id": str(confirmed["meeting_id"]),
                "content": confirmed["content"],
                "rationale": confirmed["rationale"] or "",
                "confidence": float(confirmed["confidence"]),
                "status": "confirmed",
                "confirmed_by": str(confirmed["confirmed_by"]),
                "supersedes": str(confirmed["supersedes"]) if confirmed["supersedes"] else None,
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"decision:{decision_id}:confirmed",
        ),
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_DECISION_CONFIRMED,
        target_type="decision",
        target_id=decision_id,
        metadata={
            "meeting_id": str(confirmed["meeting_id"]),
            "supersedes": str(supersedes) if supersedes else None,
        },
        ip=ctx.ip,
    )
    await mark_candidate(
        ctx.org_id, confirmed["meeting_id"], decision_id, kind="decision", status="confirmed"
    )
    await _feedback(session, ctx, user_id, "decision", "confirm", confirmed)
    if supersedes is not None:
        # Decision reversal (polish wave 3): ask the owners of the replaced decision's open
        # actions to keep, re-scope or close them. Same transaction, idempotent per action.
        await record_reversals(
            session, org_id=ctx.org_id, old_decision_id=supersedes, new_decision_id=decision_id
        )
    return Decision(**confirmed)


@router.post("/decisions/{decision_id}/dismiss", response_model=Decision, tags=["decisions"])
async def dismiss_decision(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Decision:
    """proposed -> dismissed. No decisions/actions event; one `agent-feedback` record (polish
    wave 2). Idempotent on re-dismiss (which records nothing)."""
    row, user_id = await _locked_row(session, ctx, "decisions", _DECISION_COLS, decision_id)
    if row["status"] == "dismissed":
        return Decision(**row)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} decision cannot be dismissed")
    dismissed = (
        (
            await session.execute(
                text(
                    "UPDATE decisions SET status = 'dismissed' WHERE id = :id "
                    f"RETURNING {_DECISION_COLS}"
                ),
                {"id": str(decision_id)},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_DECISION_DISMISSED,
        target_type="decision",
        target_id=decision_id,
        metadata={"meeting_id": str(dismissed["meeting_id"])},
        ip=ctx.ip,
    )
    await mark_candidate(
        ctx.org_id, dismissed["meeting_id"], decision_id, kind="decision", status="dismissed"
    )
    await _feedback(session, ctx, user_id, "decision", "dismiss", dismissed)
    return Decision(**dict(dismissed))


# --- actions -------------------------------------------------------------------------------


@router.patch("/actions/{action_id}", response_model=ActionItem, tags=["actions"])
async def update_action(
    action_id: uuid.UUID,
    body: ActionItemUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ActionItem:
    row, user_id = await _locked_row(session, ctx, "action_items", _ACTION_COLS, action_id)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} action can no longer be edited")
    fields = body.model_dump(exclude_unset=True)
    assignee = fields.get("assignee_id")
    if assignee is not None:
        if not await is_visible(session, "users", assignee):
            # 404, not 422: whether a user id exists is tenant-confidential.
            raise ProblemDetail(
                status.HTTP_404_NOT_FOUND,
                ErrorCode.NOT_FOUND,
                "No such user in this organization",
            )
        fields["assignee_id"] = str(assignee)
    sets = ", ".join(f"{k} = :{k}" for k in fields)  # keys are the model's closed field set
    updated = (
        (
            await session.execute(
                text(
                    f"UPDATE action_items SET {sets}, edited_at = now() "  # noqa: S608
                    f"WHERE id = :id RETURNING {_ACTION_COLS}"
                ),
                {**fields, "id": str(action_id)},
            )
        )
        .mappings()
        .one()
    )
    await mark_candidate(
        ctx.org_id,
        updated["meeting_id"],
        action_id,
        kind="action",
        status="edited",
        text=updated["content"],
    )
    await _feedback_edit(session, ctx, user_id, "action", row, updated, fields)
    return ActionItem(**dict(updated))


@router.post("/actions/{action_id}/confirm", response_model=ActionItem, tags=["actions"])
async def confirm_action(
    action_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ActionItem:
    """proposed -> confirmed, and the ONE `actions` outbox row. Idempotent on re-confirm."""
    row, user_id = await _locked_row(session, ctx, "action_items", _ACTION_COLS, action_id)
    if row["status"] == "confirmed":
        return ActionItem(**row)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} action cannot be confirmed")
    confirmed = dict(
        (
            await session.execute(
                text(
                    "UPDATE action_items SET status = 'confirmed', confirmed_by = :u "
                    f"WHERE id = :id RETURNING {_ACTION_COLS}"
                ),
                {"u": str(user_id), "id": str(action_id)},
            )
        )
        .mappings()
        .one()
    )
    due: date | None = confirmed["due_date"]
    await enqueue(
        session,
        EventEnvelope(
            topic="actions",
            payload={
                "action_id": str(confirmed["id"]),
                "meeting_id": str(confirmed["meeting_id"]),
                "content": confirmed["content"],
                "assignee_id": str(confirmed["assignee_id"]) if confirmed["assignee_id"] else None,
                "due_date": (due - _EPOCH).days if due is not None else None,
                "confidence": float(confirmed["confidence"]),
                "status": "confirmed",
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"action:{action_id}:confirmed",
        ),
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_ACTION_CONFIRMED,
        target_type="action_item",
        target_id=action_id,
        metadata={"meeting_id": str(confirmed["meeting_id"])},
        ip=ctx.ip,
    )
    await close_matching_promises(session, confirmed)
    await mark_candidate(
        ctx.org_id, confirmed["meeting_id"], action_id, kind="action", status="confirmed"
    )
    await _feedback(session, ctx, user_id, "action", "confirm", confirmed)
    return ActionItem(**confirmed)


async def close_matching_promises(session: AsyncSession, action: dict[str, Any]) -> int:
    """One card per moment: a confirmed action closes the promise its assignee made at the
    same moment (the PersonalListener commitment and the ActionAgent candidate quote the same
    words, so they carry the same `moment_id`). The promise becomes `done` and points at the
    task that now tracks it (`action_item_id`). Only the assignee's own open promises of this
    meeting; nothing without an assignee or a moment. Returns how many were closed."""
    if action.get("assignee_id") is None or action.get("moment_id") is None:
        return 0
    closed = (
        await session.execute(
            text(
                "UPDATE commitments SET status = 'done', done_at = now(), updated_at = now(), "
                "action_item_id = :a WHERE meeting_id = :m AND moment_id = :mo "
                "AND user_id = :u AND status = 'open' RETURNING id"
            ),
            {
                "a": str(action["id"]),
                "m": str(action["meeting_id"]),
                "mo": str(action["moment_id"]),
                "u": str(action["assignee_id"]),
            },
        )
    ).all()
    return len(closed)


@router.post("/actions/{action_id}/dismiss", response_model=ActionItem, tags=["actions"])
async def dismiss_action(
    action_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ActionItem:
    """proposed -> dismissed. No decisions/actions event; one `agent-feedback` record (polish
    wave 2). Idempotent on re-dismiss (which records nothing)."""
    row, user_id = await _locked_row(session, ctx, "action_items", _ACTION_COLS, action_id)
    if row["status"] == "dismissed":
        return ActionItem(**row)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} action cannot be dismissed")
    dismissed = (
        (
            await session.execute(
                text(
                    "UPDATE action_items SET status = 'dismissed' WHERE id = :id "
                    f"RETURNING {_ACTION_COLS}"
                ),
                {"id": str(action_id)},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_ACTION_DISMISSED,
        target_type="action_item",
        target_id=action_id,
        metadata={"meeting_id": str(dismissed["meeting_id"])},
        ip=ctx.ip,
    )
    await mark_candidate(
        ctx.org_id, dismissed["meeting_id"], action_id, kind="action", status="dismissed"
    )
    await _feedback(session, ctx, user_id, "action", "dismiss", dismissed)
    return ActionItem(**dict(dismissed))


async def enqueue_action_confirmed(
    session: AsyncSession, org_id: uuid.UUID, row: dict[str, Any]
) -> None:
    """The ONE `actions` outbox row for a confirmed action, for a task a person created
    directly (`routes/action_create.py`, Phase 4): the same payload and idempotency key as
    `confirm_action`, so the knowledge graph, the follow-up workflow and Jira treat a created
    task exactly like a confirmed candidate. Lives here because this module is the only
    producer of `actions` (`test_only_the_confirm_routes_enqueue_decisions_or_actions`).

    `actions.meeting_id` is a required uuid in the Avro schema; a task with no meeting carries
    the nil uuid, which consumers read as "no meeting".
    """
    due: date | None = row["due_date"]
    meeting = row["meeting_id"]
    await enqueue(
        session,
        EventEnvelope(
            topic="actions",
            payload={
                "action_id": str(row["id"]),
                "meeting_id": str(meeting) if meeting else str(uuid.UUID(int=0)),
                "content": row["content"],
                "assignee_id": str(row["assignee_id"]) if row["assignee_id"] else None,
                "due_date": (due - _EPOCH).days if due is not None else None,
                "confidence": float(row["confidence"]),
                "status": "confirmed",
            },
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"action:{row['id']}:confirmed",
        ),
    )


# --- settle threads (Beyond, Layer 1) --------------------------------------------------------


async def supersede_confirmed(
    session: AsyncSession,
    ctx: RequestContext,
    *,
    winner_id: uuid.UUID,
    loser_id: uuid.UUID,
    actor_id: uuid.UUID,
) -> None:
    """Record that an already confirmed decision replaces another confirmed one, through the
    same lineage path as a confirm with `supersedes`: `decisions.supersedes`, ONE `decisions`
    outbox row (so knowledge-graph-service MERGEs the `FOLLOWS_FROM` edge; its MERGE makes a
    replay a no-op), the audit row and the reversal questions for the loser's open tasks.

    Lives here because this module is the only producer of the `decisions` topic. The caller
    (a settle thread) has already checked who may do it. 409 when the winner already replaces
    another decision or the loser replaces the winner (a cycle)."""
    rows = {
        r["id"]: dict(r)
        for r in (
            await session.execute(
                text(
                    f"SELECT {_DECISION_COLS} FROM decisions WHERE id IN (:w, :l) "
                    "AND deleted_at IS NULL FOR UPDATE"
                ),
                {"w": str(winner_id), "l": str(loser_id)},
            )
        )
        .mappings()
        .all()
    }
    winner, loser = rows.get(winner_id), rows.get(loser_id)
    if winner is None or loser is None:
        raise not_found()
    if winner["supersedes"] == loser_id:
        return  # already recorded
    if winner["status"] != "confirmed" or loser["status"] != "confirmed":
        raise _conflict("Only two confirmed decisions can replace one another")
    if winner["supersedes"] is not None:
        raise _conflict("That decision already replaces another one")
    if loser["supersedes"] == winner_id:
        raise _conflict("The other decision already replaces this one")
    updated = dict(
        (
            await session.execute(
                text(
                    f"UPDATE decisions SET supersedes = :l WHERE id = :w RETURNING {_DECISION_COLS}"
                ),
                {"l": str(loser_id), "w": str(winner_id)},
            )
        )
        .mappings()
        .one()
    )
    await enqueue(
        session,
        EventEnvelope(
            topic="decisions",
            payload={
                "decision_id": str(updated["id"]),
                "meeting_id": str(updated["meeting_id"]),
                "content": updated["content"],
                "rationale": updated["rationale"] or "",
                "confidence": float(updated["confidence"]),
                "status": "confirmed",
                "confirmed_by": str(updated["confirmed_by"] or actor_id),
                "supersedes": str(loser_id),
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"decision:{winner_id}:supersedes:{loser_id}",
        ),
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=actor_id,
        action=ACTION_DECISION_CONFIRMED,
        target_type="decision",
        target_id=winner_id,
        metadata={
            "meeting_id": str(updated["meeting_id"]),
            "supersedes": str(loser_id),
            "via": "settle_thread",
        },
        ip=ctx.ip,
    )
    await record_reversals(
        session, org_id=ctx.org_id, old_decision_id=loser_id, new_decision_id=winner_id
    )
