"""My Tasks: the read model over confirmed `action_items` and the mark-done write.

Plan 7-9 section 3.6 step 6 and section 3.8 step 2; ADR-0031 D3.

- `GET /api/v1/actions?assignee_id=me&status=confirmed` lists the caller's own action items over
  the `(org_id, assignee_id, status)` index, soonest due first, undated last. `assignee_id`
  accepts only `me`, so the route cannot be used to read someone else's task list. `status` is
  `confirmed` (open) or `done`; a `proposed` or `dismissed` candidate is never a task.
- `POST /api/v1/actions/{id}/done` moves an action from `confirmed` to `done`, for its assignee
  or a tenant admin. It is idempotent (an already-done action answers 200 unchanged), refuses
  anything not yet confirmed (409), and emits NOTHING: the `actions` topic carries confirmations
  only (its Avro `status` enum is `{confirmed}`), so progress is not a new fact for the graph.

Kept apart from the candidate-confirmation routes (`/decisions|/actions/{id}/confirm`, edit,
dismiss), which own the `actions` topic and live in their own module. `action_items` itself is
created by that work's migration; this module only reads it and changes `status` on a row that
is already confirmed.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_ACTION_COMPLETED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(prefix="/api/v1", tags=["actions"])

MAX_TASKS = 500

TaskStatus = Literal["confirmed", "done"]


class MyTask(BaseModel):
    """One action item as the My Tasks screen needs it: the row plus its meeting's title, so
    the page can say where the task came from without a second round trip."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str
    content: str
    assignee_id: uuid.UUID
    due_date: date | None = None
    status: TaskStatus
    confidence: float
    created_at: datetime


class MyTaskList(BaseModel):
    items: list[MyTask]


# A task created directly (`POST /api/v1/actions`, Phase 4) has no meeting. `MyTask` predates
# that and promises a meeting, so widening it would break its clients (the oasdiff gate agrees):
# the legacy list leaves such tasks out, `POST .../done` answers for one with the nil uuid and
# an empty title, and `Task` / `GET /api/v1/actions/mine` is the shape that says "no meeting"
# honestly. New clients use `Task`.
NO_MEETING = uuid.UUID(int=0)


class Task(BaseModel):
    """One of your action items, with or without a meeting behind it."""

    id: uuid.UUID
    meeting_id: uuid.UUID | None = Field(
        default=None, description="Null for a task created directly, with no meeting."
    )
    meeting_title: str | None = None
    content: str
    assignee_id: uuid.UUID
    due_date: date | None = None
    status: TaskStatus
    confidence: float
    created_at: datetime


class TaskList(BaseModel):
    items: list[Task]


_SELECT = (
    "SELECT a.id, a.meeting_id, m.title AS meeting_title, a.content, a.assignee_id, "
    "a.due_date, a.status, a.confidence, a.created_at "
    "FROM action_items a JOIN meetings m ON m.id = a.meeting_id "
)
_SELECT_ANY = (
    "SELECT a.id, a.meeting_id, m.title AS meeting_title, a.content, a.assignee_id, "
    "a.due_date, a.status, a.confidence, a.created_at "
    "FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id "
)


def _legacy(row: dict[str, object]) -> MyTask:
    """`MyTask` for any row: a task with no meeting gets the nil uuid and an empty title."""
    if row["meeting_id"] is None:
        row = {**row, "meeting_id": NO_MEETING, "meeting_title": ""}
    return MyTask(**row)  # type: ignore[arg-type]


@router.get("/actions", response_model=MyTaskList)
async def list_my_tasks(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    assignee_id: Annotated[Literal["me"], Query()],
    status: Annotated[TaskStatus, Query()] = "confirmed",
) -> MyTaskList:
    if ctx.user_id is None:
        return MyTaskList(items=[])
    rows = (
        (
            await session.execute(
                text(
                    _SELECT + "WHERE a.assignee_id = :u AND a.status = :s "
                    "AND a.deleted_at IS NULL AND m.deleted_at IS NULL "
                    "ORDER BY a.due_date ASC NULLS LAST, a.created_at ASC, a.id ASC LIMIT :limit"
                ),
                {"u": str(ctx.user_id), "s": status, "limit": MAX_TASKS},
            )
        )
        .mappings()
        .all()
    )
    return MyTaskList(items=[MyTask(**dict(r)) for r in rows])


@router.get(
    "/actions/mine",
    response_model=TaskList,
    summary="My tasks, including ones with no meeting",
    description=(
        "Your confirmed (open) or done tasks, soonest due first, undated last. Unlike "
        "`GET /api/v1/actions?assignee_id=me`, this includes tasks created directly with no "
        "meeting (`meeting_id: null`)."
    ),
)
async def list_all_my_tasks(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status: Annotated[TaskStatus, Query()] = "confirmed",
) -> TaskList:
    if ctx.user_id is None:
        return TaskList(items=[])
    rows = (
        (
            await session.execute(
                text(
                    _SELECT_ANY + "WHERE a.assignee_id = :u AND a.status = :s "
                    "AND a.deleted_at IS NULL AND (a.meeting_id IS NULL OR m.deleted_at IS NULL) "
                    "ORDER BY a.due_date ASC NULLS LAST, a.created_at ASC, a.id ASC LIMIT :limit"
                ),
                {"u": str(ctx.user_id), "s": status, "limit": MAX_TASKS},
            )
        )
        .mappings()
        .all()
    )
    return TaskList(items=[Task(**dict(r)) for r in rows])


@router.post("/actions/{action_id}/done", response_model=MyTask)
async def mark_done(
    action_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyTask:
    row = (
        (
            await session.execute(
                text(_SELECT_ANY + "WHERE a.id = :id AND a.deleted_at IS NULL FOR UPDATE OF a"),
                {"id": str(action_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()  # unknown, deleted, or another tenant's: all invisible under RLS
    is_assignee = ctx.user_id is not None and row["assignee_id"] == ctx.user_id
    if not is_assignee and not ctx.satisfies("admin"):
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Only the assignee or an admin")
    if row["status"] == "done":
        return _legacy(dict(row))
    if row["status"] != "confirmed":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "Only a confirmed action can be marked done"
        )

    await session.execute(
        text("UPDATE action_items SET status = 'done' WHERE id = :id AND status = 'confirmed'"),
        {"id": str(action_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_ACTION_COMPLETED,
        target_type="action_item",
        target_id=action_id,
        ip=ctx.ip,
    )
    return _legacy({**dict(row), "status": "done"})
