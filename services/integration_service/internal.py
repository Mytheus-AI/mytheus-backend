"""integration-service internal routes (caller tokens, see `internal_auth.py`).

- `GET /internal/slack/token` (notification-service): the tenant's Slack bot token, decrypted
  for this one request -> {access_token, expires_in, team_id}. `Cache-Control: no-store`. The
  caller uses it for the send it is making and never persists it. 404 when Slack is not
  connected (the caller records the attempt as skipped).
- `POST /internal/slack/prep-brief` and `POST /internal/slack/meeting-summary` (workflow-service)
  {meeting_id} -> {sent: n}: build the per-participant content from core-api's slack digest and
  emit one `notifications` event per participant with channel `slack`, through the outbox.
  {sent: 0} when Slack is not connected. Replays are idempotent (the outbox key is per meeting,
  kind and user).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import ProviderError, get_clients
from services.integration_service.connectors import slack
from services.integration_service.core_client import CoreApiError, get_core
from services.integration_service.internal_auth import notification_caller, workflow_caller

router = APIRouter(tags=["integration-internal"])


class SlackToken(BaseModel):
    access_token: str
    expires_in: int | None
    team_id: str | None


class MeetingRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meeting_id: uuid.UUID


class Sent(BaseModel):
    sent: int


def _not_connected() -> ProblemDetail:
    return ProblemDetail(404, ErrorCode.NOT_FOUND, "Slack is not connected")


@router.get("/internal/slack/token", response_model=SlackToken)
async def slack_token(
    response: Response,
    org_id: Annotated[uuid.UUID, Depends(notification_caller)],
) -> SlackToken:
    response.headers["Cache-Control"] = "no-store"
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        conn = await store.get_connection(session, config.SLACK, active_only=True)
        if conn is None:
            raise _not_connected()
        try:
            tokens = await store.access_token(session, conn, get_clients().oauth[config.SLACK])
        except store.NotConnected:
            raise _not_connected() from None
        except ProviderError as exc:
            raise ProblemDetail(
                503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Slack token refresh failed"
            ) from exc
    expires_in = (
        max(0, int((tokens.expires_at - datetime.now(UTC)).total_seconds()))
        if tokens.expires_at
        else None
    )
    return SlackToken(
        access_token=tokens.access_token, expires_in=expires_in, team_id=conn.external_account_id
    )


async def _emit(org_id: uuid.UUID, meeting_id: uuid.UUID, kind: str) -> Sent:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if await store.get_connection(session, config.SLACK, active_only=True) is None:
            return Sent(sent=0)
    try:
        digest = await get_core().slack_digest(org_id, meeting_id)
    except CoreApiError as exc:
        if exc.status == 404:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found") from None
        raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "core-api unavailable") from exc
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        sent = await slack.emit(session, org_id, kind, digest)
    return Sent(sent=sent)


@router.post("/internal/slack/prep-brief", response_model=Sent)
async def prep_brief(
    body: MeetingRef,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> Sent:
    return await _emit(org_id, body.meeting_id, "prep-brief")


@router.post("/internal/slack/meeting-summary", response_model=Sent)
async def meeting_summary(
    body: MeetingRef,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> Sent:
    return await _emit(org_id, body.meeting_id, "meeting-summary")
