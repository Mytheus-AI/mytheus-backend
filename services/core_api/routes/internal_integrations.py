"""core-api internal routes for P12 (integrations and the follow-up workflow).

- `POST /internal/actions/{action_id}/external-status` (caller `integration-service`):
  {status: "done"|"confirmed", source: "jira", external_key}. A Jira status reaches NEXUS here,
  never by publishing `actions` (`actions` carries only user-confirmed facts). Moves
  `action_items.status` confirmed -> done or done -> confirmed, idempotently: the same status
  again is `changed: false` with no write and no audit. Each real change writes one
  `action.status_synced` audit row (actor_type `system`). Emits nothing on `actions`. 404 for an
  unknown, deleted or other tenant's action; 409 for a proposed or dismissed one.
- `GET /internal/actions/{action_id}/status` (caller `workflow-service`):
  {status, assignee_id, meeting_id, organizer_id, due_date, content}, for ActionFollowUpWorkflow.
- `GET /internal/meetings/{meeting_id}/slack-digest` (caller `integration-service`): the
  meeting, its participants (with each one's count of open confirmed actions) and its confirmed
  decisions and confirmed/done actions, for the Slack prep brief and summary. Proposed
  candidates never appear.

Caller tokens: `MYTHEUS_CORE_CALLER_TOKENS`, the `internal_meetings.py` scheme.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.audit import ACTION_ACTION_STATUS_SYNCED, audit
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

router = APIRouter(tags=["core-internal"])


async def integration_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"integration-service"}))


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"workflow-service"}))


def _not_found() -> ProblemDetail:
    return ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")


class ExternalStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["done", "confirmed"]
    source: Literal["jira", "linear"]  # the tracker the status came from (ADR-0044)
    external_key: str = Field(min_length=1, max_length=100)


class ExternalStatusResult(BaseModel):
    action_id: uuid.UUID
    status: str
    changed: bool


@router.post("/internal/actions/{action_id}/external-status", response_model=ExternalStatusResult)
async def external_status(
    action_id: uuid.UUID,
    body: ExternalStatus,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> ExternalStatusResult:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        current = (
            await session.execute(
                text(
                    "SELECT status FROM action_items WHERE id = :id AND deleted_at IS NULL "
                    "FOR UPDATE"
                ),
                {"id": str(action_id)},
            )
        ).scalar()
        if current is None:
            raise _not_found()
        if current == body.status:
            return ExternalStatusResult(action_id=action_id, status=current, changed=False)
        if current not in ("confirmed", "done"):
            raise ProblemDetail(
                409, ErrorCode.VERSION_CONFLICT, f"A {current} action cannot be synced"
            )
        await session.execute(
            text("UPDATE action_items SET status = :s WHERE id = :id"),
            {"s": body.status, "id": str(action_id)},
        )
        await audit(
            session,
            org_id=org_id,
            actor_type="system",
            action=ACTION_ACTION_STATUS_SYNCED,
            target_type="action_item",
            target_id=action_id,
            metadata={
                "source": body.source,
                "external_key": body.external_key,
                "from": current,
                "to": body.status,
            },
        )
    return ExternalStatusResult(action_id=action_id, status=body.status, changed=True)


class ActionStatus(BaseModel):
    status: str
    assignee_id: uuid.UUID | None
    # The nil uuid for a task created directly (Phase 4), the same convention as the `actions`
    # event; `organizer_id` is then the task's creator.
    meeting_id: uuid.UUID
    organizer_id: uuid.UUID | None
    due_date: date | None
    content: str


@router.get("/internal/actions/{action_id}/status", response_model=ActionStatus)
async def action_status(
    action_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> ActionStatus:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        row = (
            (
                await session.execute(
                    text(
                        "SELECT a.status, a.assignee_id, "
                        "COALESCE(a.meeting_id, '00000000-0000-0000-0000-000000000000'::uuid) "
                        "AS meeting_id, "
                        "COALESCE(m.created_by, a.confirmed_by) AS organizer_id, a.due_date, "
                        "a.content FROM action_items a "
                        "LEFT JOIN meetings m ON m.id = a.meeting_id "
                        "WHERE a.id = :id AND a.deleted_at IS NULL"
                    ),
                    {"id": str(action_id)},
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise _not_found()
    return ActionStatus(**dict(row))


class DigestParticipant(BaseModel):
    user_id: uuid.UUID
    email: str
    open_actions: int


class DigestDecision(BaseModel):
    content: str


class DigestAction(BaseModel):
    content: str
    assignee_id: uuid.UUID | None
    due_date: date | None
    status: str


class SlackDigest(BaseModel):
    meeting_id: uuid.UUID
    title: str | None
    status: str
    scheduled_start: datetime | None
    participants: list[DigestParticipant]
    decisions: list[DigestDecision]
    actions: list[DigestAction]


@router.get("/internal/meetings/{meeting_id}/slack-digest", response_model=SlackDigest)
async def slack_digest(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> SlackDigest:
    m = {"m": str(meeting_id)}
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        meeting = (
            (
                await session.execute(
                    text(
                        "SELECT title, status, scheduled_start FROM meetings "
                        "WHERE id = :m AND deleted_at IS NULL"
                    ),
                    m,
                )
            )
            .mappings()
            .first()
        )
        if meeting is None:
            raise _not_found()
        participants = (
            (
                await session.execute(
                    text(
                        "SELECT p.user_id, u.email, (SELECT count(*) FROM action_items a "
                        "  WHERE a.assignee_id = p.user_id AND a.status = 'confirmed' "
                        "  AND a.deleted_at IS NULL) AS open_actions "
                        "FROM meeting_participants p "
                        "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                        "WHERE p.meeting_id = :m ORDER BY u.email, p.user_id"
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
                        "SELECT content FROM decisions WHERE meeting_id = :m "
                        "AND deleted_at IS NULL AND status = 'confirmed' ORDER BY created_at, id"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
        actions = (
            (
                await session.execute(
                    text(
                        "SELECT content, assignee_id, due_date, status FROM action_items "
                        "WHERE meeting_id = :m AND deleted_at IS NULL "
                        "AND status IN ('confirmed', 'done') ORDER BY created_at, id"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
    return SlackDigest(
        meeting_id=meeting_id,
        title=meeting["title"],
        status=meeting["status"],
        scheduled_start=meeting["scheduled_start"],
        participants=[
            DigestParticipant(
                user_id=p["user_id"], email=str(p["email"]), open_actions=int(p["open_actions"])
            )
            for p in participants
        ],
        decisions=[DigestDecision(content=d["content"]) for d in decisions],
        actions=[DigestAction(**dict(a)) for a in actions],
    )
