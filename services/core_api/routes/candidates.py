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
- dismiss: `proposed` -> `dismissed`, no event; already `dismissed` -> 200.
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
from services.core_api.confidence import thresholds
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

_DECISION_COLS = (
    "id, meeting_id, content, rationale, confidence, status, confirmed_by, supersedes, created_at"
)
_ACTION_COLS = (
    "id, meeting_id, content, assignee_id, due_date, confidence, status, confirmed_by, created_at"
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
    stored but never served; the surface threshold is the tenant's, per agent.

    `include_resolved=true` (polish program, shared candidate ledger) also returns the
    meeting's CONFIRMED decisions and actions, whatever their confidence (a person confirmed
    them), with `confirmed_by`, so the room can show "Confirmed by Maya" and the live Decisions
    and Actions in Notes mode after a reload. Dismissed rows are never returned.
    """
    await _require_meeting_participant(session, ctx, meeting_id)
    overrides = (
        await session.execute(text("SELECT agent_confidence_overrides FROM tenant_settings"))
    ).scalar()
    decision_surface, _ = thresholds(overrides, "DecisionAgent")
    action_surface, _ = thresholds(overrides, "ActionAgent")
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


_STATE_DECISION = ("id", "meeting_id", "status", "content", "rationale", "confirmed_by")
_STATE_ACTION = ("id", "meeting_id", "status", "content", "assignee_id", "due_date", "confirmed_by")


# --- decisions -----------------------------------------------------------------------------


@router.patch("/decisions/{decision_id}", response_model=Decision, tags=["decisions"])
async def update_decision(
    decision_id: uuid.UUID,
    body: DecisionUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Decision:
    row, _ = await _locked_row(session, ctx, "decisions", _DECISION_COLS, decision_id)
    if row["status"] != "proposed":
        raise _conflict(f"A {row['status']} decision can no longer be edited")
    fields = body.model_dump(exclude_unset=True)
    sets = ", ".join(f"{k} = :{k}" for k in fields)  # keys are the model's closed field set
    updated = (
        (
            await session.execute(
                text(
                    f"UPDATE decisions SET {sets} WHERE id = :id "  # noqa: S608
                    f"RETURNING {_DECISION_COLS}"
                ),
                {**fields, "id": str(decision_id)},
            )
        )
        .mappings()
        .one()
    )
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
    """proposed -> dismissed. Emits nothing. Idempotent on re-dismiss."""
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
    return Decision(**dict(dismissed))


# --- actions -------------------------------------------------------------------------------


@router.patch("/actions/{action_id}", response_model=ActionItem, tags=["actions"])
async def update_action(
    action_id: uuid.UUID,
    body: ActionItemUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ActionItem:
    row, _ = await _locked_row(session, ctx, "action_items", _ACTION_COLS, action_id)
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
                    f"UPDATE action_items SET {sets} WHERE id = :id "  # noqa: S608
                    f"RETURNING {_ACTION_COLS}"
                ),
                {**fields, "id": str(action_id)},
            )
        )
        .mappings()
        .one()
    )
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
    return ActionItem(**confirmed)


@router.post("/actions/{action_id}/dismiss", response_model=ActionItem, tags=["actions"])
async def dismiss_action(
    action_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ActionItem:
    """proposed -> dismissed. Emits nothing. Idempotent on re-dismiss."""
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
