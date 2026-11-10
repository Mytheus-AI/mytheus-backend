"""ActionFollowUpWorkflow activities (P12, plan 3.12).

- `read_action_status`: core-api `GET /internal/actions/{id}/status` (caller `workflow-service`
  on `MYTHEUS_CORE_CALLER_TOKENS`) -> `{status, assignee_id, meeting_id, organizer_id,
  due_date}` (plus `content` when core-api sends it).
- `remind_assignee` (days 1, 3, 7) and `escalate_to_manager` (day 10): emit `notifications`.

Notification path: workflow-service's own transactional outbox. Canon lists workflow-service as
a `notifications` producer, and `libs/shared/outbox.py` is the one outbox every producer here
writes; the relay publishes it. One record per channel (`in_app`, then `slack`) per recipient,
sharing one `source_event_id` (`actionfollowup:{action_id}:day{n}` or `...:escalation`), so
notification-service writes one `notification_log` row and claims each channel once
(`notif:{source_event_id}:{channel}:{user_id}`); the second record is a downstream no-op. The
outbox idempotency key is that same per-channel key, so a re-run activity enqueues nothing new.
Slack delivery itself is notification-service's (it skips when the tenant has no Slack).

Escalation target (the data model has no manager edge, plan section 8; best effort, in order,
never the assignee):
1. `department_admin`: a user holding `owner`/`admin` who sits on any team in a department one
   of the assignee's teams belongs to (`admin` before `owner`, then by user id).
2. `meeting_organizer`: the action's meeting's organizer (`organizer_id` from core-api).
3. `org_admin`: any `admin`, else `owner`, in the tenant.
No target at all records `{escalated: false}`.

The escalation reads `team_members`/`teams`/`departments` (core-api's) and `rbac_grants`/`users`
(identity's) read-only, under RLS with `app.current_org` bound, the same established pattern as
identity's `get_user_context` and billing's budget alerts. It writes only the outbox.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity

from libs.shared.db import get_sessionmaker
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.workflow_service.activities import (
    CORE_TOKENS_ENV,
    SERVICE_NAME,
    call_service,
    own_secret,
)

CHANNELS = ("in_app", "slack")
REMINDER_PRIORITY = 2
ESCALATION_PRIORITY = 1

_USER_EXISTS = text("SELECT 1 FROM users WHERE id = :u AND deleted_at IS NULL")

_DEPARTMENT_ADMIN = text(
    "SELECT g.user_id FROM team_members mine "
    "JOIN teams my_team ON my_team.id = mine.team_id AND my_team.deleted_at IS NULL "
    "JOIN departments d ON d.id = my_team.department_id AND d.deleted_at IS NULL "
    "JOIN teams t ON t.department_id = d.id AND t.deleted_at IS NULL "
    "JOIN team_members tm ON tm.team_id = t.id "
    "JOIN rbac_grants g ON g.user_id = tm.user_id AND g.tier IN ('owner', 'admin') "
    "JOIN users u ON u.id = tm.user_id AND u.deleted_at IS NULL "
    "WHERE mine.user_id = :assignee AND tm.user_id <> :assignee "
    "ORDER BY CASE g.tier WHEN 'admin' THEN 0 ELSE 1 END, g.user_id LIMIT 1"
)

_ORG_ADMIN = text(
    "SELECT g.user_id FROM rbac_grants g "
    "JOIN users u ON u.id = g.user_id AND u.deleted_at IS NULL "
    "WHERE g.tier IN ('owner', 'admin') AND (CAST(:assignee AS uuid) IS NULL "
    "  OR g.user_id <> CAST(:assignee AS uuid)) "
    "ORDER BY CASE g.tier WHEN 'admin' THEN 0 ELSE 1 END, g.user_id LIMIT 1"
)

_CONTENT = text("SELECT content FROM action_items WHERE id = :a")


async def resolve_escalation_target(
    session: AsyncSession, *, assignee_id: str | None, organizer_id: str | None
) -> tuple[str | None, str | None]:
    """(user_id, reason) per the fallback chain in the module docstring. `session` must already
    have `app.current_org` bound."""
    if assignee_id:
        row = (await session.execute(_DEPARTMENT_ADMIN, {"assignee": assignee_id})).first()
        if row is not None:
            return str(row[0]), "department_admin"
    if organizer_id and organizer_id != assignee_id:
        if (await session.execute(_USER_EXISTS, {"u": organizer_id})).first() is not None:
            return organizer_id, "meeting_organizer"
    row = (await session.execute(_ORG_ADMIN, {"assignee": assignee_id})).first()
    if row is not None:
        return str(row[0]), "org_admin"
    return None, None


async def enqueue_notifications(
    session: AsyncSession,
    *,
    org_id: str,
    user_id: str,
    source_event_id: str,
    priority: int,
    title: str,
    body: str,
    link: str | None,
) -> list[int]:
    ids: list[int] = []
    for channel in CHANNELS:
        ids.append(
            await enqueue(
                session,
                EventEnvelope(
                    topic="notifications",
                    org_id=org_id,
                    producer=SERVICE_NAME,
                    idempotency_key=f"notif:{source_event_id}:{channel}:{user_id}",
                    payload={
                        "source_event_id": source_event_id,
                        "channel": channel,
                        "user_id": user_id,
                        "priority": priority,
                        "title": title,
                        "body": body,
                        "link": link,
                    },
                ),
            )
        )
    return ids


def _title_text(content: str, limit: int = 120) -> str:
    content = " ".join(content.split())
    return content if len(content) <= limit else content[: limit - 3].rstrip() + "..."


def _link(status: dict[str, Any]) -> str | None:
    meeting_id = status.get("meeting_id")
    return f"/meetings/{meeting_id}" if meeting_id else None


class FollowUpActivities:
    """Bound-method activities so the worker injects configuration and tests a transport."""

    def __init__(
        self,
        *,
        core_url: str,
        core_secret: str | None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 15.0,
    ) -> None:
        self._core_url = core_url.rstrip("/")
        self._core_secret = core_secret
        self._transport = transport
        self._timeout = timeout

    @classmethod
    def from_env(cls) -> FollowUpActivities:
        return cls(
            core_url=os.environ.get("CORE_API_URL", "http://core-api:8000"),
            core_secret=own_secret(os.environ.get(CORE_TOKENS_ENV, "")),
        )

    @activity.defn(name="read_action_status")
    async def read_action_status(self, action_id: str, org_id: str) -> dict[str, Any]:
        return await call_service(
            "GET",
            f"{self._core_url}/internal/actions/{action_id}/status",
            self._core_secret,
            org_id,
            CORE_TOKENS_ENV,
            transport=self._transport,
            timeout=self._timeout,
        )

    async def _content(
        self, session: AsyncSession, action_id: str, status: dict[str, Any], content: str
    ) -> str:
        if isinstance(status.get("content"), str) and status["content"]:
            return str(status["content"])
        if content:
            return content
        found = (await session.execute(_CONTENT, {"a": action_id})).scalar()
        return str(found) if found else "an action item"

    @activity.defn(name="remind_assignee")
    async def remind_assignee(
        self, action_id: str, org_id: str, day: int, status: dict[str, Any], content: str
    ) -> dict[str, Any]:
        assignee = status.get("assignee_id")
        if not assignee:
            return {"sent": False, "reason": "unassigned"}
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, uuid.UUID(org_id))
            if (await session.execute(_USER_EXISTS, {"u": str(assignee)})).first() is None:
                return {"sent": False, "reason": "assignee_not_found"}
            text_ = _title_text(await self._content(session, action_id, status, content))
            due = status.get("due_date")
            body = f"Still open {day} day{'s' if day != 1 else ''} after it was confirmed."
            if due:
                body += f" Due {due}."
            ids = await enqueue_notifications(
                session,
                org_id=org_id,
                user_id=str(assignee),
                source_event_id=f"actionfollowup:{action_id}:day{day}",
                priority=REMINDER_PRIORITY,
                title=f"Reminder: {text_}",
                body=body,
                link=_link(status),
            )
        return {"sent": True, "user_id": str(assignee), "outbox_ids": ids}

    @activity.defn(name="escalate_to_manager")
    async def escalate_to_manager(
        self, action_id: str, org_id: str, day: int, status: dict[str, Any], content: str
    ) -> dict[str, Any]:
        assignee = status.get("assignee_id")
        organizer = status.get("organizer_id")
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, uuid.UUID(org_id))
            target, reason = await resolve_escalation_target(
                session,
                assignee_id=str(assignee) if assignee else None,
                organizer_id=str(organizer) if organizer else None,
            )
            if target is None:
                return {"escalated": False, "reason": "no_target"}
            text_ = _title_text(await self._content(session, action_id, status, content))
            body = (
                f"This action has been open for {day} days without being marked done."
                if assignee
                else f"This action has been open for {day} days and has no assignee."
            )
            ids = await enqueue_notifications(
                session,
                org_id=org_id,
                user_id=target,
                source_event_id=f"actionfollowup:{action_id}:escalation",
                priority=ESCALATION_PRIORITY,
                title=f"Overdue: {text_}",
                body=body,
                link=_link(status),
            )
        return {"escalated": True, "user_id": target, "reason": reason, "outbox_ids": ids}

    def all(self) -> list[Any]:
        return [self.read_action_status, self.remind_assignee, self.escalate_to_manager]
