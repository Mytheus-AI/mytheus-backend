"""`POST /api/v1/actions`: a person creates a task directly (Phase 4: New task, public API, MCP).

A person typing a task is a confirmed intent, not an agent's guess, so the task is created
`confirmed` with `confirmed_by` = the caller, and the usual `actions` confirmation is enqueued
through the outbox in the same transaction (`candidates.enqueue_action_confirmed`, the one
producer of that topic). The knowledge graph, ActionFollowUpWorkflow and the Jira sync then
treat it exactly like a confirmed candidate. No silent commit: the caller's own request IS the
confirmation, and it is audited (`action.created`).

Body rules:
- `content` 1 to 500 characters after trimming.
- `assignee_id` defaults to the caller; anyone else must be a live user of this workspace
  (checked under RLS, so another tenant's user id is a 404 like a missing one).
- `meeting_id` is optional (`action_items.meeting_id` is nullable since a7b8c9d0e1f2). When
  given, the caller must be a participant of it: 404 if the tenant cannot see it, 403
  `NOT_A_PARTICIPANT` otherwise, the same rule as confirming a candidate there.
- A read-only API token is refused before this runs (403, `libs/shared/pat.py`).

`Idempotency-Key` is honoured by the shared middleware: a retried create returns the first
response and creates nothing.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_ACTION_CREATED, audit
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.capacity import warn_on_landing
from services.core_api.routes.candidates import (
    _require_meeting_participant,
    enqueue_action_confirmed,
)
from services.core_api.routes.my_tasks import Task

router = APIRouter(prefix="/api/v1", tags=["actions"])

MAX_CONTENT = 500


class ActionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1, max_length=MAX_CONTENT, description="What needs doing.")
    due_date: date | None = Field(default=None, description="Optional due date (YYYY-MM-DD).")
    assignee_id: uuid.UUID | None = Field(
        default=None, description="Who owns it. Defaults to you; must be in this workspace."
    )
    meeting_id: uuid.UUID | None = Field(
        default=None, description="Optional meeting it came from; you must be a participant."
    )

    @field_validator("content")
    @classmethod
    def _trim(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("content must not be blank")
        return value


async def _require_member(session: AsyncSession, user_id: uuid.UUID) -> None:
    row = (
        await session.execute(
            text("SELECT 1 FROM users WHERE id = :id AND deleted_at IS NULL"), {"id": str(user_id)}
        )
    ).first()
    if row is None:
        # 404, not 422: whether a user id exists is tenant-confidential.
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "No such user in this organization")


@router.post(
    "/actions",
    response_model=Task,
    status_code=status.HTTP_201_CREATED,
    summary="Create a task",
    description=(
        "Create a confirmed task for yourself or a colleague, optionally tied to a meeting you "
        "were in. It is confirmed at once (you asked for it) and flows to the knowledge graph, "
        "follow-up reminders and Jira like a confirmed action. Requires a session or a "
        "`read_write` API token. Honours `Idempotency-Key`."
    ),
)
async def create_action(
    body: ActionCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Task:
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this account")
    if body.meeting_id is not None:
        await _require_meeting_participant(session, ctx, body.meeting_id)
    assignee = body.assignee_id or ctx.user_id
    if assignee != ctx.user_id:
        await _require_member(session, assignee)
    row = dict(
        (
            await session.execute(
                text(
                    "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, due_date, "
                    "status, confidence, confirmed_by) VALUES (:o, :m, :c, :a, :d, 'confirmed', "
                    "1.0, :u) RETURNING id, meeting_id, content, assignee_id, due_date, status, "
                    "confidence, created_at"
                ),
                {
                    "o": str(ctx.org_id),
                    "m": str(body.meeting_id) if body.meeting_id else None,
                    "c": body.content,
                    "a": str(assignee),
                    "d": body.due_date,
                    "u": str(ctx.user_id),
                },
            )
        )
        .mappings()
        .one()
    )
    await enqueue_action_confirmed(session, ctx.org_id, row)
    if assignee != ctx.user_id:
        # Promise ledger: the assignee's own agent warns them when this lands on a full day.
        await warn_on_landing(
            session,
            ctx.org_id,
            user_id=assignee,
            kind="action",
            item_id=row["id"],
            content=row["content"],
            due=row["due_date"],
        )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_ACTION_CREATED,
        target_type="action_item",
        target_id=row["id"],
        metadata={
            "meeting_id": str(body.meeting_id) if body.meeting_id else None,
            "assignee_id": str(assignee),
            "via": ctx.auth_method,
        },
        ip=ctx.ip,
    )
    title = None
    if body.meeting_id is not None:
        title = (
            await session.execute(
                text("SELECT title FROM meetings WHERE id = :m"), {"m": str(body.meeting_id)}
            )
        ).scalar()
    return Task(**row, meeting_title=title)
