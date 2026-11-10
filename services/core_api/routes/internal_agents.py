"""Internal reads for `agent-runtime-service` (P10 contract sections 5 and 6).

`GET /internal/meetings/{meeting_id}/related-attendees?user_id=`: the ContextGapAgent's
"related_meeting" hop asks which colleagues are most likely to hold context the requester lacks.
Answer: up to 5 user ids who attended the requester's 10 most recent ENDED meetings in this org
(this meeting excluded), minus this meeting's participants and the requester, ranked by the
number of those meetings shared with the requester, then by how recently, then by id (stable).
Response `{"user_ids": [...]}`, possibly empty.

P11 (contract section 3), for `ProactiveAgent`'s deterministic checks:

`GET /internal/actions/{action_id}/context`: the workload and dependency facts for one action.
`{assignee_id, assignee_name, meeting_id, organizer_id, open_action_count, depends_on,
participants, admins}`. `open_action_count` is the assignee's `confirmed` (open, not done, not
dismissed, not deleted) action items in this org, INCLUDING this one when it is confirmed; 0
when the action has no assignee. `assignee_name` is the assignee's email (`users` has no name
column). `depends_on` is always `[]` today: `action_items` has no dependency column and P8's
action candidates never carried one, so there is nothing stored to return; the field is in the
shape so the agent needs no change when one exists. `participants` are the action's meeting's
participants; `admins` are the tenant's owners and admins (the whole org, not only those in the
meeting); `organizer_id` is the meeting's `created_by` (null when unknown).

`GET /internal/meetings/{meeting_id}/participants`:
`{participants: [{user_id, name, rbac_tier}]}`, `name` the email, `rbac_tier` null when the user
holds no grant. Ordered by email then id.

Service-to-service, the scheme `internal_meetings.py` and `internal_retrieval.py` use:
`X-Mytheus-Service: agent-runtime-service`, its secret from `MYTHEUS_CORE_CALLER_TOKENS`
(`agent-runtime-service=<32+ chars>`), the tenant in `X-Mytheus-Org`. Not routed by
`api-gateway`. An unknown meeting or user is 404, as is one of another tenant (RLS hides them).
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Security
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

ALLOWED_CALLERS = frozenset({"agent-runtime-service"})
RECENT_MEETINGS = 10
MAX_RELATED = 5

router = APIRouter(tags=["core-internal"])


async def agent_runtime_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, ALLOWED_CALLERS)


class RelatedAttendees(BaseModel):
    user_ids: list[uuid.UUID]


class DependsOn(BaseModel):
    id: uuid.UUID
    content: str
    status: str


class ActionContext(BaseModel):
    assignee_id: uuid.UUID | None
    assignee_name: str | None
    meeting_id: uuid.UUID
    organizer_id: uuid.UUID | None
    open_action_count: int
    depends_on: list[DependsOn]
    participants: list[uuid.UUID]
    admins: list[uuid.UUID]


class MeetingParticipant(BaseModel):
    user_id: uuid.UUID
    name: str
    rbac_tier: str | None


class MeetingParticipants(BaseModel):
    participants: list[MeetingParticipant]


_RELATED = text("""
    WITH recent AS (
        SELECT m.id, m.updated_at
        FROM meetings m JOIN meeting_participants p ON p.meeting_id = m.id
        WHERE p.user_id = :uid AND m.status = 'ended' AND m.deleted_at IS NULL
          AND m.id <> :mid
        ORDER BY m.updated_at DESC, m.id DESC
        LIMIT :recent
    )
    SELECT p.user_id, count(*) AS shared, max(r.updated_at) AS last_seen
    FROM recent r
    JOIN meeting_participants p ON p.meeting_id = r.id
    JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL
    WHERE p.user_id <> :uid
      AND p.user_id NOT IN (SELECT user_id FROM meeting_participants WHERE meeting_id = :mid)
    GROUP BY p.user_id
    ORDER BY shared DESC, last_seen DESC, p.user_id
    LIMIT :lim
    """)


@router.get("/internal/meetings/{meeting_id}/related-attendees", response_model=RelatedAttendees)
async def related_attendees(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
    user_id: Annotated[uuid.UUID, Query()],
) -> RelatedAttendees:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        known = (
            await session.execute(
                text(
                    "SELECT (SELECT count(*) FROM meetings WHERE id = :m AND deleted_at IS NULL), "
                    "(SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL)"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        ).one()
        if not known[0] or not known[1]:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        rows = await session.execute(
            _RELATED,
            {
                "uid": str(user_id),
                "mid": str(meeting_id),
                "recent": RECENT_MEETINGS,
                "lim": MAX_RELATED,
            },
        )
        return RelatedAttendees(user_ids=[r[0] for r in rows])


@router.get("/internal/actions/{action_id}/context", response_model=ActionContext)
async def action_context(
    action_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> ActionContext:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        action = (
            (
                await session.execute(
                    text(
                        "SELECT a.assignee_id, a.meeting_id, m.created_by, u.email "
                        "FROM action_items a JOIN meetings m ON m.id = a.meeting_id "
                        "LEFT JOIN users u ON u.id = a.assignee_id AND u.deleted_at IS NULL "
                        "WHERE a.id = :id AND a.deleted_at IS NULL"
                    ),
                    {"id": str(action_id)},
                )
            )
            .mappings()
            .first()
        )
        if action is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        assignee = action["assignee_id"]
        open_count = 0
        if assignee is not None:
            open_count = (
                await session.execute(
                    text(
                        "SELECT count(*) FROM action_items WHERE assignee_id = :a "
                        "AND status = 'confirmed' AND deleted_at IS NULL"
                    ),
                    {"a": str(assignee)},
                )
            ).scalar_one()
        participants = (
            await session.execute(
                text(
                    "SELECT p.user_id FROM meeting_participants p "
                    "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                    "WHERE p.meeting_id = :m ORDER BY p.user_id"
                ),
                {"m": str(action["meeting_id"])},
            )
        ).scalars()
        participant_ids = list(participants)
        admins = (
            await session.execute(
                text(
                    "SELECT g.user_id FROM rbac_grants g "
                    "JOIN users u ON u.id = g.user_id AND u.deleted_at IS NULL "
                    "WHERE g.tier IN ('owner', 'admin') ORDER BY g.user_id"
                )
            )
        ).scalars()
        return ActionContext(
            assignee_id=assignee,
            assignee_name=action["email"] if assignee is not None else None,
            meeting_id=action["meeting_id"],
            organizer_id=action["created_by"],
            open_action_count=int(open_count),
            depends_on=[],
            participants=participant_ids,
            admins=list(admins),
        )


@router.get("/internal/meetings/{meeting_id}/participants", response_model=MeetingParticipants)
async def meeting_participants(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> MeetingParticipants:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        known = (
            await session.execute(
                text("SELECT 1 FROM meetings WHERE id = :m AND deleted_at IS NULL"),
                {"m": str(meeting_id)},
            )
        ).first()
        if known is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        rows = await session.execute(
            text(
                "SELECT p.user_id, u.email, g.tier FROM meeting_participants p "
                "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                "LEFT JOIN rbac_grants g ON g.user_id = p.user_id AND g.org_id = p.org_id "
                "WHERE p.meeting_id = :m ORDER BY u.email, p.user_id"
            ),
            {"m": str(meeting_id)},
        )
        return MeetingParticipants(
            participants=[
                MeetingParticipant(user_id=r[0], name=str(r[1]), rbac_tier=r[2]) for r in rows
            ]
        )
