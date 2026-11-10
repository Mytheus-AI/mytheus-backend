"""`GET /internal/meetings/{meeting_id}/post-meeting`: the MeetingLifecycleWorkflow's
post-processing read (P10, plan 3.1).

Plan 3.1 has `run_post_processing` "trigger summary build + final worklog flush via core-api".
Neither exists to trigger: there is no summary builder in any repo yet, and worklogs are written
per window by the `agent-results` consumer, so there is no buffer to flush. What the activity
can honestly do today is confirm, from core-api (which owns `meetings.status`), that the meeting
really is over, and record what it produced. This route is that read, and nothing more: it
writes nothing.

- 404: no such meeting in this tenant (RLS hides other tenants'), or deleted.
- 409 `VERSION_CONFLICT`: the meeting is not `ended`/`cancelled` yet. The workflow is signalled
  from the `ended` event core-api emits after that commit, so this should not happen; when it
  does (a replayed signal, a re-opened meeting) the activity retries with backoff.
- 200: the digest below, which becomes part of the workflow's result in Temporal history.

ADR-0039 B7 and B9, for the meeting timers (`MeetingRemindersWorkflow`):

- `GET /internal/meetings/{meeting_id}/reminders`: `{meeting_id, title, status, confidential,
  scheduled_start, scheduled_end, previous_meeting_id, participants: [{user_id, attendance}]}`.
  The workflow re-reads it before every timer fires, so a meeting rescheduled or cancelled in
  the meantime gets no notification for the old time.
- `POST /internal/meetings/{meeting_id}/status-requests`: one `action_status_requests` row per
  open (`confirmed`) action item of the meeting's `previous_meeting_id` that has an assignee,
  `ON CONFLICT (action_id, meeting_id) DO NOTHING`, so a retried activity creates nothing new.
  Nothing is created for a meeting that is over or has no previous meeting. Returns every
  pending request of the meeting: `{requests: [{id, action_id, user_id, action_content,
  state}]}`, so the caller can notify per request idempotently (keyed by request id).

Service-to-service, the `internal_meetings.py` scheme: `X-Mytheus-Service: workflow-service`,
its secret in `MYTHEUS_CORE_CALLER_TOKENS`, the tenant in `X-Mytheus-Org`.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

ALLOWED_CALLERS = frozenset({"workflow-service"})

router = APIRouter(tags=["core-internal"])


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, ALLOWED_CALLERS)


class PostMeetingDigest(BaseModel):
    meeting_id: uuid.UUID
    status: str
    participants: int
    worklogs: int
    decisions_proposed: int
    decisions_confirmed: int
    actions_proposed: int
    actions_confirmed: int
    collaborations: int
    context_fills: int


_DIGEST = text("""
    SELECT
      (SELECT count(*) FROM meeting_participants WHERE meeting_id = :m) AS participants,
      (SELECT count(*) FROM worklogs WHERE meeting_id = :m) AS worklogs,
      (SELECT count(*) FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL
         AND status = 'proposed') AS decisions_proposed,
      (SELECT count(*) FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL
         AND status IN ('confirmed', 'superseded')) AS decisions_confirmed,
      (SELECT count(*) FROM action_items WHERE meeting_id = :m AND deleted_at IS NULL
         AND status = 'proposed') AS actions_proposed,
      (SELECT count(*) FROM action_items WHERE meeting_id = :m AND deleted_at IS NULL
         AND status IN ('confirmed', 'done')) AS actions_confirmed,
      (SELECT count(*) FROM agent_collaborations WHERE meeting_id = :m) AS collaborations,
      (SELECT count(*) FROM agent_collaborations WHERE meeting_id = :m
         AND fill_content IS NOT NULL) AS context_fills
    """)


@router.get("/internal/meetings/{meeting_id}/post-meeting", response_model=PostMeetingDigest)
async def post_meeting_digest(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> PostMeetingDigest:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        status = (
            await session.execute(
                text("SELECT status FROM meetings WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(meeting_id)},
            )
        ).scalar()
        if status is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        if status not in ("ended", "cancelled"):
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The meeting has not ended")
        counts = (await session.execute(_DIGEST, {"m": str(meeting_id)})).mappings().one()
    return PostMeetingDigest(meeting_id=meeting_id, status=str(status), **dict(counts))


class ReminderParticipant(BaseModel):
    user_id: uuid.UUID
    attendance: str


class MeetingReminders(BaseModel):
    meeting_id: uuid.UUID
    title: str | None
    status: str
    confidential: bool
    scheduled_start: datetime | None
    scheduled_end: datetime | None
    previous_meeting_id: uuid.UUID | None
    participants: list[ReminderParticipant]


class CreatedStatusRequest(BaseModel):
    id: uuid.UUID
    action_id: uuid.UUID
    user_id: uuid.UUID
    action_content: str
    state: str


class CreatedStatusRequests(BaseModel):
    requests: list[CreatedStatusRequest]


@router.get("/internal/meetings/{meeting_id}/reminders", response_model=MeetingReminders)
async def meeting_reminders(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> MeetingReminders:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        row = (
            (
                await session.execute(
                    text(
                        "SELECT m.id AS meeting_id, m.title, m.status, m.confidential, "
                        "m.scheduled_start, m.scheduled_end, "
                        "(SELECT p.id FROM meetings p WHERE p.id = m.previous_meeting_id "
                        " AND p.deleted_at IS NULL) AS previous_meeting_id "
                        "FROM meetings m WHERE m.id = :id AND m.deleted_at IS NULL"
                    ),
                    {"id": str(meeting_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        people = await session.execute(
            text(
                "SELECT p.user_id, p.attendance FROM meeting_participants p "
                "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                "WHERE p.meeting_id = :id ORDER BY p.user_id"
            ),
            {"id": str(meeting_id)},
        )
        return MeetingReminders(
            **dict(row),
            participants=[ReminderParticipant(user_id=r[0], attendance=r[1]) for r in people],
        )


@router.post(
    "/internal/meetings/{meeting_id}/status-requests", response_model=CreatedStatusRequests
)
async def create_status_requests(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> CreatedStatusRequests:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        meeting = (
            await session.execute(
                text(
                    "SELECT m.status, (SELECT p.id FROM meetings p "
                    "  WHERE p.id = m.previous_meeting_id AND p.deleted_at IS NULL) "
                    "FROM meetings m WHERE m.id = :id AND m.deleted_at IS NULL"
                ),
                {"id": str(meeting_id)},
            )
        ).first()
        if meeting is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        status, previous = meeting
        if previous is not None and status not in ("ended", "cancelled"):
            await session.execute(
                text(
                    "INSERT INTO action_status_requests (org_id, action_id, user_id, meeting_id) "
                    "SELECT a.org_id, a.id, a.assignee_id, :m FROM action_items a "
                    "JOIN users u ON u.id = a.assignee_id AND u.deleted_at IS NULL "
                    "WHERE a.meeting_id = :p AND a.status = 'confirmed' "
                    "AND a.deleted_at IS NULL "
                    "ON CONFLICT (action_id, meeting_id) DO NOTHING"
                ),
                {"m": str(meeting_id), "p": str(previous)},
            )
        rows = await session.execute(
            text(
                "SELECT r.id, r.action_id, r.user_id, a.content, r.state "
                "FROM action_status_requests r JOIN action_items a ON a.id = r.action_id "
                "WHERE r.meeting_id = :m AND r.state = 'pending' AND a.status = 'confirmed' "
                "AND a.deleted_at IS NULL ORDER BY a.created_at, a.id"
            ),
            {"m": str(meeting_id)},
        )
        return CreatedStatusRequests(
            requests=[
                CreatedStatusRequest(
                    id=r[0], action_id=r[1], user_id=r[2], action_content=r[3], state=r[4]
                )
                for r in rows
            ]
        )
