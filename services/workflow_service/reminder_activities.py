"""MeetingRemindersWorkflow activities (ADR-0039 B9). REST to core-api, notifications through
workflow-service's own outbox; never a write to another service's table.

- `read_meeting_reminders`: core-api `GET /internal/meetings/{id}/reminders` -> `{title, status,
  scheduled_start, previous_meeting_id, participants: [{user_id, attendance}], ...}`. A 404
  (the meeting is gone, or deleted) is returned as `{"gone": true}` so the workflow can stop
  cleanly instead of failing.
- `send_status_requests` (start minus 60 minutes): core-api
  `POST /internal/meetings/{id}/status-requests`, then one `in_app` `notifications` record per
  pending request: "Quick status on {action}?", link `/status-requests`.
- `send_briefs` (start minus 10 minutes): one per participant attending live: "Your brief for
  {title}", link `/meetings/{id}#brief`.
- `send_catchups` (5 minutes after the end): one per participant whose attendance was `agent`:
  "Your agent's catch-up from {title}", link `/meetings/{id}/catch-up`.

Idempotent: every record's outbox idempotency key is the canon notification key
`notif:{source_event_id}:in_app:{user_id}` and the outbox is unique on it, so an activity that
re-runs (a worker died after the commit but before Temporal recorded the completion) enqueues
nothing new. `source_event_id` is `statusrequest:{request_id}` (one per request, so a reschedule
never asks twice), `meetingbrief:{meeting_id}:{scheduled_start}` (a reschedule gets a brief for
the new time) and `meetingcatchup:{meeting_id}`. notification-service then writes one
`notification_log` row per (source_event_id, user) and delivers it in-app.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.exceptions import ApplicationError

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

CHANNEL = "in_app"
PRIORITY = 2
TITLE_TEXT = 120


def _short(value: Any, fallback: str, limit: int = TITLE_TEXT) -> str:
    text_ = " ".join(str(value or "").split()) or fallback
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


async def _notify(
    session: AsyncSession,
    *,
    org_id: str,
    user_id: str,
    source_event_id: str,
    title: str,
    body: str,
    link: str,
) -> int:
    return await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=org_id,
            producer=SERVICE_NAME,
            idempotency_key=f"notif:{source_event_id}:{CHANNEL}:{user_id}",
            payload={
                "source_event_id": source_event_id,
                "channel": CHANNEL,
                "user_id": user_id,
                "priority": PRIORITY,
                "title": title,
                "body": body,
                "link": link,
            },
        ),
    )


class ReminderActivities:
    """Bound-method activities, so the worker injects configuration and tests a transport."""

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
    def from_env(cls) -> ReminderActivities:
        return cls(
            core_url=os.environ.get("CORE_API_URL", "http://core-api:8000"),
            core_secret=own_secret(os.environ.get(CORE_TOKENS_ENV, "")),
        )

    async def _core(self, method: str, path: str, org_id: str) -> dict[str, Any]:
        return await call_service(
            method,
            f"{self._core_url}{path}",
            self._core_secret,
            org_id,
            CORE_TOKENS_ENV,
            transport=self._transport,
            timeout=self._timeout,
        )

    async def _state(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        try:
            return await self._core("GET", f"/internal/meetings/{meeting_id}/reminders", org_id)
        except ApplicationError as exc:
            if exc.type == "HTTP404":
                return {"gone": True}
            raise

    @activity.defn(name="read_meeting_reminders")
    async def read_meeting_reminders(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._state(meeting_id, org_id)

    @activity.defn(name="send_status_requests")
    async def send_status_requests(
        self, meeting_id: str, org_id: str, scheduled_start: str
    ) -> dict[str, Any]:
        created = await self._core(
            "POST", f"/internal/meetings/{meeting_id}/status-requests", org_id
        )
        requests = created.get("requests") or []
        ids: list[int] = []
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, uuid.UUID(org_id))
            for r in requests:
                ids.append(
                    await _notify(
                        session,
                        org_id=org_id,
                        user_id=str(r["user_id"]),
                        source_event_id=f"statusrequest:{r['id']}",
                        title=f"Quick status on {_short(r.get('action_content'), 'your action')}?",
                        body=(
                            "Your follow-up meeting starts in an hour. One line on where this "
                            "stands goes into its carry-forward."
                        ),
                        link="/status-requests",
                    )
                )
        return {"requests": len(requests), "outbox_ids": ids, "scheduled_start": scheduled_start}

    @activity.defn(name="send_briefs")
    async def send_briefs(
        self, meeting_id: str, org_id: str, scheduled_start: str
    ) -> dict[str, Any]:
        state = await self._state(meeting_id, org_id)
        if state.get("gone"):
            return {"sent": 0, "reason": "meeting_gone"}
        title = _short(state.get("title"), "your meeting")
        live = [p for p in state.get("participants") or [] if p.get("attendance") == "live"]
        ids: list[int] = []
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, uuid.UUID(org_id))
            for p in live:
                ids.append(
                    await _notify(
                        session,
                        org_id=org_id,
                        user_id=str(p["user_id"]),
                        source_event_id=f"meetingbrief:{meeting_id}:{scheduled_start}",
                        title=f"Your brief for {title}",
                        body=(
                            "Starts in 10 minutes: your agenda items, open actions and saved "
                            "questions."
                        ),
                        link=f"/meetings/{meeting_id}#brief",
                    )
                )
        return {"sent": len(ids), "outbox_ids": ids}

    @activity.defn(name="send_catchups")
    async def send_catchups(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        state = await self._state(meeting_id, org_id)
        if state.get("gone") or state.get("status") != "ended":
            return {"sent": 0, "reason": "not_ended"}
        title = _short(state.get("title"), "your meeting")
        proxied = [p for p in state.get("participants") or [] if p.get("attendance") == "agent"]
        ids: list[int] = []
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, uuid.UUID(org_id))
            for p in proxied:
                ids.append(
                    await _notify(
                        session,
                        org_id=org_id,
                        user_id=str(p["user_id"]),
                        source_event_id=f"meetingcatchup:{meeting_id}",
                        title=f"Your Myte's catch-up from {title}",
                        body="What was decided, what is yours, and your Myte's notes.",
                        link=f"/meetings/{meeting_id}/catch-up",
                    )
                )
        return {"sent": len(ids), "outbox_ids": ids}

    def all(self) -> list[Any]:
        return [
            self.read_meeting_reminders,
            self.send_status_requests,
            self.send_briefs,
            self.send_catchups,
        ]
