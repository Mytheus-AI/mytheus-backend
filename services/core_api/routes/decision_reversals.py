"""Decision reversal: when a decision is superseded, ask the owners of its open actions what now.

Polish program wave 3 ("Only Mytheus can do these").

Recording. `record_reversals` runs inside `POST /decisions/{id}/confirm` (`candidates.py`) in
the same transaction, when the confirm names `supersedes`. The replaced decision's linked open
actions are the `confirmed` (open), owned, live action items of the replaced decision's meeting:
the graph links them the same way (`Decision -PRODUCED-> Meeting <-PRODUCED- Action`), and
`action_items` has no per-decision column. Each gets one `decision_reversals` row
(`ON CONFLICT (new_decision_id, action_id) DO NOTHING`) and its owner one in-app `notifications`
outbox row, `source_event_id = decision-reversal:{id}`, so a replay sends nothing twice.

Routes (the owner's own rows only; anyone else, in any tenant, gets a 404):
- `GET /api/v1/decision-reversals?status=pending|resolved|all&action_id=`
- `GET /api/v1/decision-reversals/{id}`
- `POST /api/v1/decision-reversals/{id}/resolve` `{resolution, content?, due_date?}`:
  `keep` leaves the task, `rescope` edits its text and/or due date, `close` moves it to
  `dismissed` (closed because the decision changed, not done). Repeating the same resolution is
  200 and writes nothing; a different one after resolving is 409. The task edit emits no event:
  the `actions` topic carries confirmations only (as for `POST /actions/{id}/done`).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue

router = APIRouter(prefix="/api/v1", tags=["decision-reversals"])

PRODUCER = "core-api"
ACTION_REVERSAL_RESOLVED = "decision_reversal.resolved"
MAX_ITEMS = 200
SNIPPET = 140

ReversalStatus = Literal["pending", "kept", "rescoped", "closed"]
Resolution = Literal["keep", "rescope", "close"]
_RESOLVED_AS: dict[str, ReversalStatus] = {"keep": "kept", "rescope": "rescoped", "close": "closed"}


class ReversalDecision(BaseModel):
    id: uuid.UUID
    content: str
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    decided_at: datetime


class ReversalAction(BaseModel):
    id: uuid.UUID
    content: str
    due_date: date | None = None
    status: Literal["proposed", "confirmed", "done", "dismissed"]
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None


class DecisionReversal(BaseModel):
    id: uuid.UUID
    status: ReversalStatus
    created_at: datetime
    resolved_at: datetime | None = None
    old_decision: ReversalDecision
    new_decision: ReversalDecision
    action: ReversalAction


class DecisionReversalList(BaseModel):
    items: list[DecisionReversal]


class ReversalResolve(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolution: Resolution
    content: str | None = Field(default=None, max_length=2000)
    due_date: date | None = None

    @field_validator("content")
    @classmethod
    def _content(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("content cannot be empty")
        return value

    @model_validator(mode="after")
    def _rescope_needs_a_change(self) -> ReversalResolve:
        if (
            self.resolution == "rescope"
            and self.content is None
            and "due_date" not in (self.model_fields_set)
        ):
            raise ValueError("re-scoping needs new task text or a due date")
        return self


def _short(value: Any, limit: int = SNIPPET) -> str:
    text_ = " ".join(str(value or "").split())
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


# --- recording (called from confirm_decision) ----------------------------------------------


async def record_reversals(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    old_decision_id: uuid.UUID,
    new_decision_id: uuid.UUID,
) -> int:
    """One reversal row and one owner notification per linked open action. Returns the number
    of NEW rows. Must run in the confirming transaction (RLS already set)."""
    decisions = {
        r["id"]: r
        for r in (
            await session.execute(
                text(
                    "SELECT id, meeting_id, content FROM decisions "
                    "WHERE id IN (:old, :new) AND deleted_at IS NULL"
                ),
                {"old": str(old_decision_id), "new": str(new_decision_id)},
            )
        )
        .mappings()
        .all()
    }
    old, new = decisions.get(old_decision_id), decisions.get(new_decision_id)
    if old is None or new is None:
        return 0
    rows = (
        (
            await session.execute(
                text(
                    "INSERT INTO decision_reversals (org_id, old_decision_id, new_decision_id, "
                    "action_id, owner_user_id) "
                    "SELECT a.org_id, :old, :new, a.id, a.assignee_id FROM action_items a "
                    "JOIN users u ON u.id = a.assignee_id AND u.deleted_at IS NULL "
                    "WHERE a.meeting_id = :m AND a.status = 'confirmed' AND a.deleted_at IS NULL "
                    "ON CONFLICT (new_decision_id, action_id) DO NOTHING "
                    "RETURNING id, action_id, owner_user_id"
                ),
                {
                    "old": str(old_decision_id),
                    "new": str(new_decision_id),
                    "m": str(old["meeting_id"]),
                },
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        return 0
    contents = {
        r[0]: r[1]
        for r in await session.execute(
            text("SELECT id, content FROM action_items WHERE id = ANY(CAST(:ids AS uuid[]))"),
            {"ids": [str(r["action_id"]) for r in rows]},
        )
    }
    for r in rows:
        await enqueue(
            session,
            EventEnvelope(
                topic="notifications",
                org_id=str(org_id),
                producer=PRODUCER,
                idempotency_key=f"notif:decision-reversal:{r['id']}:in_app:{r['owner_user_id']}",
                payload={
                    "source_event_id": f"decision-reversal:{r['id']}",
                    "channel": "in_app",
                    "user_id": str(r["owner_user_id"]),
                    "priority": 2,
                    "title": "Decision changed: re-scope, keep or close this task?",
                    "body": (
                        f'"{_short(old["content"])}" was replaced by "{_short(new["content"])}". '
                        f"Your task: {_short(contents.get(r['action_id']), 200)}"
                    )[:2000],
                    "link": f"/work/tasks?reversal={r['id']}",
                },
            ),
        )
    return len(rows)


# --- reads ---------------------------------------------------------------------------------

_SELECT = """
SELECT r.id, r.status, r.created_at, r.resolved_at,
       od.id AS od_id, od.content AS od_content, od.meeting_id AS od_meeting_id,
       om.title AS od_meeting_title, COALESCE(od.status_changed_at, od.created_at) AS od_at,
       nd.id AS nd_id, nd.content AS nd_content, nd.meeting_id AS nd_meeting_id,
       nm.title AS nd_meeting_title, COALESCE(nd.status_changed_at, nd.created_at) AS nd_at,
       a.id AS a_id, a.content AS a_content, a.due_date AS a_due_date, a.status AS a_status,
       a.meeting_id AS a_meeting_id, am.title AS a_meeting_title
FROM decision_reversals r
JOIN decisions od ON od.id = r.old_decision_id
JOIN decisions nd ON nd.id = r.new_decision_id
JOIN action_items a ON a.id = r.action_id
LEFT JOIN meetings om ON om.id = od.meeting_id
LEFT JOIN meetings nm ON nm.id = nd.meeting_id
LEFT JOIN meetings am ON am.id = a.meeting_id
"""


def _row(r: Any) -> DecisionReversal:
    return DecisionReversal(
        id=r["id"],
        status=r["status"],
        created_at=r["created_at"],
        resolved_at=r["resolved_at"],
        old_decision=ReversalDecision(
            id=r["od_id"],
            content=r["od_content"],
            meeting_id=r["od_meeting_id"],
            meeting_title=r["od_meeting_title"],
            decided_at=r["od_at"],
        ),
        new_decision=ReversalDecision(
            id=r["nd_id"],
            content=r["nd_content"],
            meeting_id=r["nd_meeting_id"],
            meeting_title=r["nd_meeting_title"],
            decided_at=r["nd_at"],
        ),
        action=ReversalAction(
            id=r["a_id"],
            content=r["a_content"],
            due_date=r["a_due_date"],
            status=r["a_status"],
            meeting_id=r["a_meeting_id"],
            meeting_title=r["a_meeting_title"],
        ),
    )


async def _one(
    session: AsyncSession, reversal_id: uuid.UUID, user_id: uuid.UUID, *, lock: bool = False
) -> Any:
    row = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE r.id = :id AND r.owner_user_id = :u"
                    + (" FOR UPDATE OF r" if lock else "")
                ),
                {"id": str(reversal_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


@router.get("/decision-reversals", response_model=DecisionReversalList)
async def list_reversals(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status: Annotated[Literal["pending", "resolved", "all"], Query()] = "pending",
    action_id: Annotated[uuid.UUID | None, Query()] = None,
) -> DecisionReversalList:
    """The caller's own reversals, newest first (at most 200)."""
    if ctx.user_id is None:
        return DecisionReversalList(items=[])
    where = ["r.owner_user_id = :u", "a.deleted_at IS NULL"]
    params: dict[str, Any] = {"u": str(ctx.user_id), "lim": MAX_ITEMS}
    if status == "pending":
        where.append("r.status = 'pending'")
    elif status == "resolved":
        where.append("r.status <> 'pending'")
    if action_id is not None:
        where.append("r.action_id = :a")
        params["a"] = str(action_id)
    rows = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY r.created_at DESC, r.id LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return DecisionReversalList(items=[_row(r) for r in rows])


@router.get("/decision-reversals/{reversal_id}", response_model=DecisionReversal)
async def get_reversal(
    reversal_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionReversal:
    if ctx.user_id is None:
        raise not_found()
    return _row(await _one(session, reversal_id, ctx.user_id))


@router.post("/decision-reversals/{reversal_id}/resolve", response_model=DecisionReversal)
async def resolve_reversal(
    reversal_id: uuid.UUID,
    body: ReversalResolve,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionReversal:
    """Keep, re-scope or close the task. Idempotent per resolution."""
    if ctx.user_id is None:
        raise not_found()
    row = await _one(session, reversal_id, ctx.user_id, lock=True)
    target = _RESOLVED_AS[body.resolution]
    if row["status"] != "pending":
        if row["status"] == target:
            return _row(row)
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"This change was already handled ({row['status']})"
        )
    action_id = row["a_id"]
    if body.resolution != "keep" and row["a_status"] != "confirmed":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"The task is already {row['a_status']}"
        )
    changes: dict[str, Any] = {}
    if body.resolution == "rescope":
        sets: list[str] = []
        params: dict[str, Any] = {"id": str(action_id)}
        if body.content is not None:
            sets.append("content = :c")
            params["c"] = body.content
            changes["content"] = body.content
        if "due_date" in body.model_fields_set:
            sets.append("due_date = :d")
            params["d"] = body.due_date
            changes["due_date"] = body.due_date.isoformat() if body.due_date else None
        await session.execute(
            text(f"UPDATE action_items SET {', '.join(sets)} WHERE id = :id"), params
        )
    elif body.resolution == "close":
        await session.execute(
            text("UPDATE action_items SET status = 'dismissed' WHERE id = :id"),
            {"id": str(action_id)},
        )
    await session.execute(
        text(
            "UPDATE decision_reversals SET status = :s, resolved_by = :u, resolved_at = now() "
            "WHERE id = :id"
        ),
        {"s": target, "u": str(ctx.user_id), "id": str(reversal_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_REVERSAL_RESOLVED,
        target_type="action",
        target_id=action_id,
        metadata={
            "reversal_id": str(reversal_id),
            "resolution": body.resolution,
            "old_decision_id": str(row["od_id"]),
            "new_decision_id": str(row["nd_id"]),
            **changes,
        },
        ip=ctx.ip,
    )
    return _row(await _one(session, reversal_id, ctx.user_id))
