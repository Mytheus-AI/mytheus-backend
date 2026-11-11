"""core-api internal reads for integration-service's Phase 4 "reach" features.

Same caller-token scheme as every core-api internal route (`internal_meetings.authenticate_caller`,
secrets in `MYTHEUS_CORE_CALLER_TOKENS`); only `integration-service` is accepted here. Not routed
by api-gateway (`/internal` is never forwarded), so reachable only inside the network.

- `GET /internal/meetings/{meeting_id}/crm-digest`: what the HubSpot connector pushes to a CRM
  and the addresses it matches companies and contacts by. Confirmed facts only (decisions
  `confirmed`/`superseded`, actions `confirmed`/`done`), people by display name. The email
  addresses are returned because the matcher needs them; they go no further than
  integration-service, which uses them as HubSpot search values and never writes them into the
  note. A **confidential meeting is never pushed to a CRM**: it answers 404 with the problem
  extension `reason: confidential`, so the caller can report "skipped" rather than "missing".
- `GET /internal/decisions/{decision_id}/record?viewer=<users.id>`: the decision record
  (`records.DecisionRecord`) exactly as that user may share it, for the Notion / Confluence /
  Slack exports. 404 when the viewer may not see or share it, the same answer as a decision that
  does not exist (existence is confidential).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Security
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import records
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

router = APIRouter(tags=["core-internal"])

MAX_ROWS = 200


async def integration_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"integration-service"}))


def _not_found(reason: str | None = None) -> ProblemDetail:
    extra = {"reason": reason} if reason else None
    return ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found", extra=extra)


class CrmDecision(BaseModel):
    id: uuid.UUID
    content: str
    status: str


class CrmAction(BaseModel):
    id: uuid.UUID
    content: str
    owner_name: str | None = None
    due_date: date | None = None
    status: str


class CrmDigest(BaseModel):
    meeting_id: uuid.UUID
    title: str
    status: str
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    participant_emails: list[str]
    guest_emails: list[str]
    tenant_domains: list[str]
    decisions: list[CrmDecision]
    actions: list[CrmAction]
    link: str


@router.get("/internal/meetings/{meeting_id}/crm-digest", response_model=CrmDigest)
async def crm_digest(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> CrmDigest:
    m = {"m": str(meeting_id), "n": MAX_ROWS}
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        meeting = (
            (
                await session.execute(
                    text(
                        "SELECT title, status, scheduled_start, scheduled_end, confidential "
                        "FROM meetings WHERE id = :m AND deleted_at IS NULL"
                    ),
                    m,
                )
            )
            .mappings()
            .first()
        )
        if meeting is None:
            raise _not_found()
        if meeting["confidential"]:
            raise _not_found("confidential")
        participants = (
            await session.execute(
                text(
                    "SELECT lower(u.email) FROM meeting_participants p "
                    "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                    "WHERE p.meeting_id = :m ORDER BY 1 LIMIT :n"
                ),
                m,
            )
        ).scalars()
        participant_emails = [str(e) for e in participants]
        guests = (
            await session.execute(
                text("SELECT guest_emails FROM meeting_followups WHERE meeting_id = :m"), m
            )
        ).scalar()
        domains = (
            await session.execute(
                text(
                    "SELECT DISTINCT lower(split_part(email, '@', 2)) FROM users "
                    "WHERE deleted_at IS NULL AND position('@' in email) > 0 ORDER BY 1"
                )
            )
        ).scalars()
        decisions = (
            (
                await session.execute(
                    text(
                        "SELECT id, content, status FROM decisions WHERE meeting_id = :m "
                        "AND deleted_at IS NULL AND status IN ('confirmed', 'superseded') "
                        "ORDER BY created_at, id LIMIT :n"
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
                        "SELECT a.id, a.content, a.due_date, a.status, u.email, u.display_name "
                        "FROM action_items a LEFT JOIN users u ON u.id = a.assignee_id "
                        "WHERE a.meeting_id = :m AND a.deleted_at IS NULL "
                        "AND a.status IN ('confirmed', 'done') ORDER BY a.created_at, a.id "
                        "LIMIT :n"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
    return CrmDigest(
        meeting_id=meeting_id,
        title=(meeting["title"] or "").strip() or "Untitled meeting",
        status=str(meeting["status"]),
        scheduled_start=meeting["scheduled_start"],
        scheduled_end=meeting["scheduled_end"],
        participant_emails=participant_emails,
        guest_emails=sorted({str(g).strip().lower() for g in (guests or []) if g}),
        tenant_domains=[str(d) for d in domains if d],
        decisions=[CrmDecision(**dict(d)) for d in decisions],
        actions=[
            CrmAction(
                id=a["id"],
                content=a["content"],
                owner_name=records.person_name(a["display_name"], a["email"]),
                due_date=a["due_date"],
                status=a["status"],
            )
            for a in actions
        ],
        link=f"/meetings/{meeting_id}",
    )


@router.get("/internal/decisions/{decision_id}/record", response_model=records.DecisionRecord)
async def decision_record(
    decision_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
    viewer: Annotated[uuid.UUID, Query(description="The users.id the record is built for.")],
) -> records.DecisionRecord:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        record = await records.build_decision_record(session, decision_id, viewer)
    if record is None:
        raise _not_found()
    return record
