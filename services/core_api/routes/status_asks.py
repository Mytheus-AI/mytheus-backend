"""Ask for status instead of meeting: `POST /api/v1/status-requests` `{action_ids}`.

Polish wave 3, "Should this be a meeting?". When agenda items are status updates on open
actions, the person scheduling can ask the owners directly in one click. Each request is an
ordinary ADR-0039 B7 `action_status_requests` row (so the owner answers it in "Updates you owe"
exactly like a pre-meeting one), attached to the action's own meeting and carrying
`requested_by`; the owner gets an in-app notification and, when they answer, the asker gets the
answer (`status_requests.answer_status_request`).

Per action, in order: not visible / deleted -> `not_found`; not `confirmed` -> `not_open`; no
owner or no meeting -> `no_owner`; the caller's own action -> `own_action`; a pending request
already exists for it on that meeting -> `already_pending` (nothing new, no second
notification); otherwise `requested`. The caller must be able to see the action: they are a
participant of its meeting, or they assigned it to themselves. Idempotent: a repeat is all
`already_pending`.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.records import display_name

router = APIRouter(prefix="/api/v1", tags=["status-requests"])

PRODUCER = "core-api"
AskReason = Literal[
    "requested", "already_pending", "no_owner", "not_open", "own_action", "not_found"
]


class StatusRequestsAsk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_ids: list[uuid.UUID] = Field(min_length=1, max_length=20)


class StatusAskItem(BaseModel):
    action_id: uuid.UUID
    request_id: uuid.UUID | None = None
    owner_name: str | None = None
    reason: AskReason


class StatusRequestsAsked(BaseModel):
    requested: int
    skipped: int
    items: list[StatusAskItem]


def _short(value: Any, limit: int = 120) -> str:
    text_ = " ".join(str(value or "").split()) or "your action"
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


@router.post("/status-requests", response_model=StatusRequestsAsked)
async def ask_for_status(
    body: StatusRequestsAsk,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StatusRequestsAsked:
    items: list[StatusAskItem] = []
    asker = ctx.user_id
    asker_name = None
    if asker is not None:
        email = (
            await session.execute(text("SELECT email FROM users WHERE id = :u"), {"u": str(asker)})
        ).scalar()
        asker_name = display_name(email)
    for action_id in dict.fromkeys(body.action_ids):
        row = (
            (
                await session.execute(
                    text(
                        "SELECT a.id, a.content, a.status, a.assignee_id, a.meeting_id, "
                        "u.email AS owner_email, "
                        "(a.assignee_id = :me OR EXISTS (SELECT 1 FROM meeting_participants p "
                        "  WHERE p.meeting_id = a.meeting_id AND p.user_id = :me)) AS visible "
                        "FROM action_items a LEFT JOIN users u ON u.id = a.assignee_id "
                        " AND u.deleted_at IS NULL "
                        "WHERE a.id = :a AND a.deleted_at IS NULL"
                    ),
                    {"a": str(action_id), "me": str(asker)},
                )
            )
            .mappings()
            .first()
        )
        if row is None or asker is None or not row["visible"]:
            items.append(StatusAskItem(action_id=action_id, reason="not_found"))
            continue
        owner_name = display_name(row["owner_email"])
        if row["status"] != "confirmed":
            items.append(
                StatusAskItem(action_id=action_id, owner_name=owner_name, reason="not_open")
            )
            continue
        if row["assignee_id"] is None or row["owner_email"] is None or row["meeting_id"] is None:
            items.append(StatusAskItem(action_id=action_id, reason="no_owner"))
            continue
        if row["assignee_id"] == asker:
            items.append(
                StatusAskItem(action_id=action_id, owner_name=owner_name, reason="own_action")
            )
            continue
        created = (
            await session.execute(
                text(
                    "INSERT INTO action_status_requests (org_id, action_id, user_id, meeting_id, "
                    "requested_by) VALUES (:o, :a, :u, :m, :by) "
                    "ON CONFLICT (action_id, meeting_id) DO UPDATE SET state = 'pending', "
                    "requested_by = EXCLUDED.requested_by, requested_at = now() "
                    "WHERE action_status_requests.state <> 'pending' "
                    "RETURNING id, requested_at"
                ),
                {
                    "o": str(ctx.org_id),
                    "a": str(action_id),
                    "u": str(row["assignee_id"]),
                    "m": str(row["meeting_id"]),
                    "by": str(asker),
                },
            )
        ).first()
        if created is None:
            pending = (
                await session.execute(
                    text(
                        "SELECT id FROM action_status_requests WHERE action_id = :a "
                        "AND meeting_id = :m"
                    ),
                    {"a": str(action_id), "m": str(row["meeting_id"])},
                )
            ).scalar()
            items.append(
                StatusAskItem(
                    action_id=action_id,
                    request_id=pending,
                    owner_name=owner_name,
                    reason="already_pending",
                )
            )
            continue
        # One notification per (request, ask): a re-ask after an answer is a new ask.
        request_id, requested_at = created
        asked_key = f"{request_id}:{int(requested_at.timestamp())}"
        await enqueue(
            session,
            EventEnvelope(
                topic="notifications",
                org_id=str(ctx.org_id),
                producer=PRODUCER,
                idempotency_key=f"notif:statusask:{asked_key}:in_app:{row['assignee_id']}",
                payload={
                    "source_event_id": f"statusask:{asked_key}",
                    "channel": "in_app",
                    "user_id": str(row["assignee_id"]),
                    "priority": 2,
                    "title": f"Quick status on {_short(row['content'])}?",
                    "body": (
                        f"{asker_name or 'A colleague'} asked for a one-line status instead of "
                        "a meeting."
                    ),
                    "link": "/work/updates",
                },
            ),
        )
        items.append(
            StatusAskItem(
                action_id=action_id,
                request_id=request_id,
                owner_name=owner_name,
                reason="requested",
            )
        )
    requested = sum(1 for i in items if i.reason == "requested")
    return StatusRequestsAsked(requested=requested, skipped=len(items) - requested, items=items)
