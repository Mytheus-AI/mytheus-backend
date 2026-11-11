"""Promise drift (polish wave 3): "done by Friday" against a Jira ticket still open after Friday.

The daily `PromiseDriftWorkflow` (workflow-service, schedule `promise-drift-daily`) runs per
tenant:
1. `GET /internal/promise-drift/due?as_of=` here: open (`confirmed`) owned actions whose due
   date has passed (within the last 60 days) -> `{items: [{action_id, due_date}]}`;
2. integration-service `POST /internal/jira/issue-status` with those ids -> which are linked to
   a Jira issue that is still open (nothing, `connected: false`, when Jira is not connected);
3. `POST /internal/promise-drift/nudges` here with the still-open ones.

Nudges, per (action, due date), at most once each (UNIQUE (action_id, stage, due_date), so a
re-run or a retried activity sends nothing twice; a new due date starts over):
- `owner`: the first time it is seen overdue and open, a private in-app nudge to the owner:
  "Your promise to finish X by Fri 14 Nov: the Jira ticket is still open".
- `host`: when the owner was nudged at least 2 days earlier and it is still open, a note to the
  meeting's host (the `host` participant, else its creator), unless the host is the owner.

Service-to-service (`internal_meetings.py` scheme), caller `workflow-service`.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.db import get_sessionmaker
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api.records import display_name
from services.core_api.routes.internal_lifecycle import workflow_caller

router = APIRouter(tags=["core-internal"])

PRODUCER = "core-api"
LOOKBACK_DAYS = 60
HOST_AFTER_DAYS = 2
MAX_DUE = 500


class DueAction(BaseModel):
    action_id: uuid.UUID
    due_date: date


class DueActions(BaseModel):
    items: list[DueAction]


class OpenIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: uuid.UUID
    issue_key: str = Field(min_length=1, max_length=100)
    issue_url: str | None = Field(default=None, max_length=500)


class DriftNudgeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: date
    issues: list[OpenIssue] = Field(max_length=MAX_DUE)


class DriftNudgeResult(BaseModel):
    owner: int
    host: int


def _short(value: Any, limit: int = 100) -> str:
    text_ = " ".join(str(value or "").split()) or "your task"
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


def due_label(d: date) -> str:
    """`Fri 14 Nov`."""
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


@router.get("/internal/promise-drift/due", response_model=DueActions)
async def due_actions(
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
    as_of: Annotated[date, Query()],
) -> DueActions:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        rows = await session.execute(
            text(
                "SELECT a.id, a.due_date FROM action_items a "
                "JOIN users u ON u.id = a.assignee_id AND u.deleted_at IS NULL "
                "WHERE a.status = 'confirmed' AND a.deleted_at IS NULL "
                "AND a.due_date < :d AND a.due_date >= :since "
                "ORDER BY a.due_date, a.id LIMIT :lim"
            ),
            {"d": as_of, "since": as_of - timedelta(days=LOOKBACK_DAYS), "lim": MAX_DUE},
        )
        return DueActions(items=[DueAction(action_id=r[0], due_date=r[1]) for r in rows])


async def _notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    user_id: uuid.UUID,
    source: str,
    title: str,
    body: str,
    link: str,
    priority: int,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": title[:200],
                "body": body[:2000],
                "link": link,
            },
        ),
    )


@router.post("/internal/promise-drift/nudges", response_model=DriftNudgeResult)
async def nudge(
    body: DriftNudgeRequest,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> DriftNudgeResult:
    owner_sent = host_sent = 0
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        for issue in body.issues:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT a.id, a.content, a.due_date, a.assignee_id, a.meeting_id, "
                            "u.email AS owner_email, "
                            "(SELECT COALESCE((SELECT p.user_id FROM meeting_participants p "
                            "   WHERE p.meeting_id = m.id AND p.role_in_meeting = 'host' "
                            "   ORDER BY p.user_id LIMIT 1), m.created_by) "
                            " FROM meetings m WHERE m.id = a.meeting_id "
                            " AND m.deleted_at IS NULL) AS host_id, "
                            "(SELECT n.created_at::date FROM promise_drift_nudges n "
                            " WHERE n.action_id = a.id AND n.stage = 'owner' "
                            " AND n.due_date = a.due_date) AS owner_nudged_on "
                            "FROM action_items a "
                            "JOIN users u ON u.id = a.assignee_id AND u.deleted_at IS NULL "
                            "WHERE a.id = :a AND a.status = 'confirmed' AND a.deleted_at IS NULL "
                            "AND a.due_date < :d"
                        ),
                        {"a": str(issue.action_id), "d": body.as_of},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                continue  # closed, gone, re-dated into the future, or never overdue
            due: date = row["due_date"]
            content = _short(row["content"])
            if row["owner_nudged_on"] is None:
                created = (
                    await session.execute(
                        text(
                            "INSERT INTO promise_drift_nudges (org_id, action_id, stage, user_id, "
                            "due_date, issue_key, issue_url) VALUES (:o, :a, 'owner', :u, :d, "
                            ":k, :url) ON CONFLICT (action_id, stage, due_date) DO NOTHING "
                            "RETURNING id"
                        ),
                        {
                            "o": str(org_id),
                            "a": str(row["id"]),
                            "u": str(row["assignee_id"]),
                            "d": due,
                            "k": issue.issue_key,
                            "url": issue.issue_url,
                        },
                    )
                ).first()
                if created is not None:
                    await _notify(
                        session,
                        org_id,
                        user_id=row["assignee_id"],
                        source=f"promise-drift:{row['id']}:{due.isoformat()}:owner",
                        title=(
                            f"Your promise to finish {content} by {due_label(due)}: "
                            "the Jira ticket is still open"
                        ),
                        body=(
                            f"{issue.issue_key} is still open. Close it, or change the due "
                            "date if the plan moved. Only you see this for now."
                        ),
                        link=f"/work/tasks?focus={row['id']}",
                        priority=2,
                    )
                    owner_sent += 1
                continue
            if (body.as_of - row["owner_nudged_on"]).days < HOST_AFTER_DAYS:
                continue
            host = row["host_id"]
            if host is None or host == row["assignee_id"] or row["meeting_id"] is None:
                continue
            created = (
                await session.execute(
                    text(
                        "INSERT INTO promise_drift_nudges (org_id, action_id, stage, user_id, "
                        "due_date, issue_key, issue_url) VALUES (:o, :a, 'host', :u, :d, :k, "
                        ":url) ON CONFLICT (action_id, stage, due_date) DO NOTHING RETURNING id"
                    ),
                    {
                        "o": str(org_id),
                        "a": str(row["id"]),
                        "u": str(host),
                        "d": due,
                        "k": issue.issue_key,
                        "url": issue.issue_url,
                    },
                )
            ).first()
            if created is None:
                continue
            owner = display_name(row["owner_email"]) or "A teammate"
            late = (body.as_of - due).days
            await _notify(
                session,
                org_id,
                user_id=host,
                source=f"promise-drift:{row['id']}:{due.isoformat()}:host",
                title=f'{owner}\'s promise "{content}" is {late} days past due',
                body=(
                    f"Due {due_label(due)}; {issue.issue_key} is still open in Jira. {owner} was "
                    "reminded privately two days ago."
                ),
                link=f"/meetings/{row['meeting_id']}",
                priority=2,
            )
            host_sent += 1
    return DriftNudgeResult(owner=owner_sent, host=host_sent)
