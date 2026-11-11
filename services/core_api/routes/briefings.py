"""A briefing from your agent: the routes ("Beyond the meeting", Layer 1). The model, the source
and the hand-off are in `services/core_api/briefings.py`.

The caller's own briefings only (another person's, or another tenant's, is 404):

- `GET  /api/v1/briefings?limit=`            newest first
- `GET  /api/v1/briefings/latest`            the newest one (any state) and whether theirs are on
- `POST /api/v1/briefings` `{meeting_id?}`   "Brief me": 202 with the briefing being written. For
  a meeting the caller was in (403 `NOT_A_PARTICIPANT` otherwise, 409 while it is still on), or
  without one: since the last briefing, about their latest meeting of the week. Asking again
  while one is being written answers that one; at most 12 an hour (429).
- `GET  /api/v1/briefings/{id}`              poll it (`pending` -> `composing` -> `ready`)
- `POST /api/v1/briefings/{id}/listened`     mark it heard (the "new" dot goes away)
- `GET  /api/v1/briefings/{id}/audio`        a 10-minute URL to the server voice, when
  `audio.status` is `ready` (404 otherwise: speak the script on the device)
- `GET|PUT /api/v1/briefings/preferences`    on/off (after-meeting briefings; "Brief me" always
  works), auto-play, length, language, device voice, server voice, speed; plus whether this server
  renders audio and the voices it offers.
- `GET  /api/v1/meetings/{id}/briefing`      the caller's newest briefing about this meeting

Internal, `agent-runtime-service` only (`MYTHEUS_CORE_CALLER_TOKENS`, the tenant in
`X-Mytheus-Org`):
- `POST /internal/briefings/{id}/claim`  -> the source to write from (409 when there is nothing
  to write: finished, or out of attempts);
- `POST /internal/briefings/{id}/result` <- the script (validated, links checked).

Every read recovers a briefing the agent never picked up (`briefings.recover`).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import briefing_audio, briefings
from services.core_api.briefings import (
    Briefing,
    BriefingPreferences,
    BriefingResult,
    BriefingSettings,
    BriefingSource,
    ResultAccepted,
)
from services.core_api.routes.internal_agents import agent_runtime_caller

router = APIRouter(tags=["briefings"])

MAX_LIST = 50


class BriefingList(BaseModel):
    items: list[Briefing]


class BriefingLatest(BaseModel):
    briefing: Briefing | None = None
    enabled: bool = Field(description="Whether a briefing follows every meeting.")
    auto_play: bool


class MeetingBriefing(BaseModel):
    briefing: Briefing | None = None
    enabled: bool
    auto_play: bool
    can_request: bool = Field(description="The caller was in it and it has ended.")


class BriefMe(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meeting_id: uuid.UUID | None = Field(
        default=None, description="A meeting the caller was in; omitted: since the last one."
    )


class BriefingAudioUrl(BaseModel):
    url: str
    mime: str
    expires_at: datetime


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


async def _view(session: AsyncSession, ctx: RequestContext, row: object) -> Briefing:
    row = await briefings.recover(session, ctx.org_id, row)
    return briefings.from_row(row)


async def _meeting_state(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> tuple[bool, str]:
    """(the caller was in it, its status). 404 for an unknown or deleted meeting."""
    row = (
        await session.execute(
            text(
                "SELECT m.status, EXISTS (SELECT 1 FROM meeting_participants p "
                "WHERE p.meeting_id = m.id AND p.user_id = :u) FROM meetings m "
                "WHERE m.id = :m AND m.deleted_at IS NULL"
            ),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).first()
    if row is None:
        raise not_found()
    return bool(row[1]), str(row[0])


@router.get("/api/v1/briefings", response_model=BriefingList)
async def list_briefings(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIST)] = 10,
) -> BriefingList:
    rows = await briefings.recent(session, _me(ctx), limit)
    return BriefingList(items=[briefings.from_row(r) for r in rows])


@router.get("/api/v1/briefings/latest", response_model=BriefingLatest)
async def latest_briefing(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BriefingLatest:
    me = _me(ctx)
    prefs = await briefings.preferences(session, me)
    row = await briefings.latest(session, me)
    return BriefingLatest(
        briefing=await _view(session, ctx, row) if row is not None else None,
        enabled=prefs.enabled,
        auto_play=prefs.auto_play,
    )


@router.get("/api/v1/briefings/preferences", response_model=BriefingSettings)
async def get_preferences(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BriefingSettings:
    return briefings.settings_view(await briefings.preferences(session, _me(ctx)))


@router.put("/api/v1/briefings/preferences", response_model=BriefingSettings)
async def put_preferences(
    body: BriefingPreferences,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BriefingSettings:
    if body.language not in briefings.LANGUAGES:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Unknown briefing language")
    if body.server_voice is not None and not any(
        v.id == body.server_voice for v in briefing_audio.SERVER_VOICES
    ):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Unknown server voice")
    prefs = body.model_copy(
        update={"voice": " ".join(body.voice.split()) or None if body.voice else None}
    )
    await briefings.save_preferences(session, ctx.org_id, _me(ctx), prefs)
    return briefings.settings_view(prefs)


@router.post("/api/v1/briefings", response_model=Briefing, status_code=202)
async def brief_me(
    body: BriefMe,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Briefing:
    me = _me(ctx)
    # Its own transaction: the doorbell rings only after the row is committed, so the agent's
    # claim always finds it.
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        if body.meeting_id is not None:
            participant, status = await _meeting_state(session, body.meeting_id, me)
            if not participant:
                raise ProblemDetail(
                    403, ErrorCode.NOT_A_PARTICIPANT, "Only people who were in it get a briefing"
                )
            if status != "ended":
                raise ProblemDetail(
                    409, ErrorCode.VERSION_CONFLICT, "The meeting has not ended yet"
                )
        existing = await briefings.open_on_demand(session, me, body.meeting_id)
        if existing is not None:
            response.status_code = 200
            row = await briefings.fetch(session, existing, me)
            return briefings.from_row(row)
        if await briefings.on_demand_count(session, me) >= briefings.ON_DEMAND_PER_HOUR:
            raise ProblemDetail(
                429, ErrorCode.RATE_LIMITED, "That is a lot of briefings. Try again in a while."
            )
        created = await briefings.create(session, ctx.org_id, me, body.meeting_id, "on_demand")
        assert created is not None
        await session.execute(
            text("UPDATE agent_briefings SET rung_at = now() WHERE id = :id"),
            {"id": str(created)},
        )
        row = await briefings.fetch(session, created, me)
    await briefings.ring(ctx.org_id, created)
    return briefings.from_row(row)


@router.get("/api/v1/briefings/{briefing_id}", response_model=Briefing)
async def get_briefing(
    briefing_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Briefing:
    row = await briefings.fetch(session, briefing_id, _me(ctx))
    if row is None:
        raise not_found()
    return await _view(session, ctx, row)


@router.post("/api/v1/briefings/{briefing_id}/listened", response_model=Briefing)
async def mark_listened(
    briefing_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Briefing:
    me = _me(ctx)
    await session.execute(
        text(
            "UPDATE agent_briefings SET listened_at = COALESCE(listened_at, now()) "
            "WHERE id = :id AND user_id = :u"
        ),
        {"id": str(briefing_id), "u": str(me)},
    )
    row = await briefings.fetch(session, briefing_id, me)
    if row is None:
        raise not_found()
    return briefings.from_row(row)


@router.get("/api/v1/briefings/{briefing_id}/audio", response_model=BriefingAudioUrl)
async def briefing_audio_url(
    briefing_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BriefingAudioUrl:
    row = await briefings.fetch(session, briefing_id, _me(ctx))
    if row is None or row["audio_status"] != "ready" or not row["audio_key"]:
        raise not_found()
    signed = briefing_audio.presigned(str(row["audio_key"]))
    if signed is None:
        raise not_found()
    return BriefingAudioUrl(url=signed[0], mime=briefing_audio.MIME, expires_at=signed[1])


@router.get("/api/v1/meetings/{meeting_id}/briefing", response_model=MeetingBriefing)
async def meeting_briefing(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingBriefing:
    me = _me(ctx)
    participant, status = await _meeting_state(session, meeting_id, me)
    prefs = await briefings.preferences(session, me)
    row = await briefings.latest(session, me, meeting_id) if participant else None
    return MeetingBriefing(
        briefing=await _view(session, ctx, row) if row is not None else None,
        enabled=prefs.enabled,
        auto_play=prefs.auto_play,
        can_request=participant and status == "ended",
    )


# --- internal: agent-runtime-service ---------------------------------------------------------


@router.post(
    "/internal/briefings/{briefing_id}/claim",
    response_model=BriefingSource,
    tags=["core-internal"],
)
async def claim_briefing(
    briefing_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> BriefingSource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if await briefings.claim(session, briefing_id) is None:
            row = await briefings.fetch(session, briefing_id)
            if row is None:
                raise not_found()
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "Nothing to write")
        row = await briefings.fetch(session, briefing_id)
        return await briefings.build_source(session, row)


@router.post(
    "/internal/briefings/{briefing_id}/result",
    response_model=ResultAccepted,
    tags=["core-internal"],
)
async def briefing_result(
    briefing_id: uuid.UUID,
    body: BriefingResult,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> ResultAccepted:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        row = await briefings.fetch(session, briefing_id)
        if row is None:
            raise not_found()
        accepted, status, audio = await briefings.apply_result(session, org_id, row, body)
    if audio:
        briefings.schedule_audio(org_id, briefing_id)
    return ResultAccepted(accepted=accepted, status=status)
