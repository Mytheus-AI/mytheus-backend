"""Clips of a meeting, and sharing one (polish wave 3, Recording playback).

A clip is a named span of a meeting (on the meeting's clock, at most 10 minutes) that a
participant makes from the recording or the transcript; see `clip_records.py` for the model and
the off-the-record rule. Every participant of the meeting sees its clips; the clip's creator,
the meeting's host or a workspace admin may delete one. Sharing a clip makes a `share_links`
row of kind `meeting_clip` under the same rules as a recap: participants share, a confidential
meeting only its host (with `confirm_confidential`), signed, expiring, revocable (`DELETE
/api/v1/share-links/{id}`), and listed under their clip (`GET /api/v1/meetings/{id}/clips`).
The public page shows the clip's transcript lines always, and its video only when
`include_video` was chosen; the video is cut from the recording by meeting-service (asked for
right away, so it is usually ready by the time someone opens it).

Routes (member tier; another tenant's meeting is a 404, a non-participant 403):
- `POST   /api/v1/meetings/{id}/clips`          make a clip `{title, start_ms, end_ms}`
- `GET    /api/v1/meetings/{id}/clips`          the meeting's clips, in meeting order
- `DELETE /api/v1/clips/{clip_id}`              remove one (creator, host or admin)
- `POST   /api/v1/clips/{clip_id}/share-links`  share one `{expires_in_days, include_video,
                                                 confirm_confidential}`
- `GET    /api/v1/public/clips/{token}`         the public clip page (no auth: the token)

Clip links are their own resource on the wire (`ClipShareLink`, listed under each clip) rather
than a new `kind` on `ShareLink`/`PublicShare`: adding an enum value to those published
responses would break clients written against them. `GET /api/v1/public/shares/{token}` answers
a clip token with 404 `reason: clip_share`, which tells the share page to read the clip route.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_CLIP_CREATED, ACTION_CLIP_DELETED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import clip_records, recording_client, records, share_tokens
from services.core_api.routes.chat import _not_a_participant
from services.core_api.routes.share_links import (
    LinkState,
    _names,
    count_view,
    insert_link,
    link_state,
    open_public_link,
)
from services.core_api.share_tokens import ShareLinkRow

router = APIRouter(prefix="/api/v1", tags=["clips"])
public_router = APIRouter(prefix="/api/v1/public", tags=["public-shares"])

MAX_CLIPS_LISTED = 200
# Strong references to in-flight pre-render requests (a bare create_task can be collected).
_PRERENDERS: set[asyncio.Task[object]] = set()


class ClipCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=200, description="What the clip shows.")
    start_ms: int = Field(ge=0, description="Start on the meeting's clock (the transcript's).")
    end_ms: int = Field(gt=0, description="End on the meeting's clock; at most 10 minutes on.")

    @field_validator("title")
    @classmethod
    def _strip(cls, value: str) -> str:
        value = " ".join(value.split())
        if not value:
            raise ValueError("title must not be blank")
        return value

    @model_validator(mode="after")
    def _span(self) -> ClipCreate:
        if self.end_ms <= self.start_ms:
            raise ValueError("end_ms must be after start_ms")
        if self.end_ms - self.start_ms > clip_records.MAX_CLIP_MS:
            raise ValueError("a clip is at most 10 minutes long")
        return self


class ClipShareLink(BaseModel):
    """A public link to one clip (`share_links` kind `meeting_clip`). Revoked like any share
    link, with `DELETE /api/v1/share-links/{id}`."""

    id: uuid.UUID
    clip_id: uuid.UUID
    meeting_id: uuid.UUID
    url: str = Field(description="The public page to send: `{APP_BASE_URL}/s/{token}`.")
    token: str
    include_video: bool = Field(description="Whether the page shows the clip's video.")
    created_by: uuid.UUID
    created_by_name: str | None = None
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    state: LinkState
    view_count: int
    last_viewed_at: datetime | None = None
    can_revoke: bool


class Clip(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    title: str
    start_ms: int
    end_ms: int
    duration_ms: int
    created_by: uuid.UUID
    created_by_name: str | None = None
    created_at: datetime
    can_delete: bool = Field(description="Whether the caller may delete this clip.")
    share_links: list[ClipShareLink] = Field(
        default_factory=list, description="The clip's public links, newest first."
    )


class ClipList(BaseModel):
    items: list[Clip]
    max_duration_ms: int = Field(
        default=clip_records.MAX_CLIP_MS, description="The longest clip allowed."
    )


class PublicClipShare(BaseModel):
    """What a public clip page renders."""

    shared_by_name: str | None = None
    shared_at: datetime
    expires_at: datetime
    view_count: int
    clip: clip_records.ClipShare


class ClipShareCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expires_in_days: int = Field(
        default=share_tokens.DEFAULT_TTL_DAYS,
        ge=1,
        le=share_tokens.MAX_TTL_DAYS,
        description="How long the link works, 1 to 365 days. Default 30.",
    )
    include_video: bool = Field(
        default=False,
        description="Also show the clip's video on the public page (the clip alone, never the "
        "rest of the recording). Off: the page shows the clip's transcript lines only.",
    )
    confirm_confidential: bool = Field(
        default=False,
        description="The host confirms publishing a clip of a confidential meeting. Required "
        "(409 `confidential_confirmation_required`) for a confidential meeting.",
    )


async def _participant_access(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> tuple[records.MeetingAccess, uuid.UUID]:
    if ctx.user_id is None:
        raise _not_a_participant()
    access = await records.access_for(session, meeting_id, ctx.user_id)
    if access is None:
        raise not_found()
    if not access.is_participant:
        raise _not_a_participant()
    return access, ctx.user_id


def _may_delete(ctx: RequestContext, created_by: uuid.UUID, access: records.MeetingAccess) -> bool:
    if ctx.user_id is None:
        return False
    return created_by == ctx.user_id or access.viewer_is_host(ctx.user_id) or ctx.satisfies("admin")


def _link_out(
    link: ShareLinkRow, org_id: uuid.UUID, *, name: str | None, can_revoke: bool
) -> ClipShareLink:
    token = share_tokens.mint(org_id, link.id)
    assert link.clip_id is not None
    return ClipShareLink(
        id=link.id,
        clip_id=link.clip_id,
        meeting_id=link.meeting_id,
        url=share_tokens.share_url(token),
        token=token,
        include_video=link.include_video,
        created_by=link.created_by,
        created_by_name=name,
        created_at=link.created_at,
        expires_at=link.expires_at,
        revoked_at=link.revoked_at,
        state=link_state(link),
        view_count=link.view_count,
        last_viewed_at=link.last_viewed_at,
        can_revoke=can_revoke,
    )


def _clip_out(
    row: dict,  # type: ignore[type-arg]
    *,
    name: str | None,
    can_delete: bool,
    links: list[ClipShareLink] | None = None,
) -> Clip:
    return Clip(
        id=row["id"],
        meeting_id=row["meeting_id"],
        title=row["title"],
        start_ms=row["start_ms"],
        end_ms=row["end_ms"],
        duration_ms=row["end_ms"] - row["start_ms"],
        created_by=row["created_by"],
        created_by_name=name,
        created_at=row["created_at"],
        can_delete=can_delete,
        share_links=links or [],
    )


@router.post(
    "/meetings/{meeting_id}/clips",
    response_model=Clip,
    status_code=status.HTTP_201_CREATED,
    summary="Make a clip of a meeting",
    responses={
        403: {"description": "Not a participant of this meeting."},
        409: {"description": "The span crosses a stretch that was off the record."},
    },
)
async def create_clip(
    meeting_id: uuid.UUID,
    body: ClipCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Clip:
    """A participant names a span of the meeting (at most 10 minutes). A span that crosses an
    off-the-record stretch is refused: nothing from it was kept, so nothing can be clipped."""
    access, user_id = await _participant_access(session, ctx, meeting_id)
    if await clip_records.off_record_overlap(session, meeting_id, body.start_ms, body.end_ms):
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "A clip cannot cross a part of the meeting that was off the record",
            extra={"reason": "off_record_overlap"},
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meeting_clips (org_id, meeting_id, created_by, title, start_ms, "
                    "end_ms) VALUES (:o, :m, :u, :t, :s, :e) RETURNING id, meeting_id, "
                    "created_by, title, start_ms, end_ms, created_at"
                ),
                {
                    "o": str(ctx.org_id),
                    "m": str(meeting_id),
                    "u": str(user_id),
                    "t": body.title,
                    "s": body.start_ms,
                    "e": body.end_ms,
                },
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_CLIP_CREATED,
        target_type="clip",
        target_id=row["id"],
        metadata={
            "meeting_id": str(meeting_id),
            "start_ms": body.start_ms,
            "end_ms": body.end_ms,
        },
        ip=ctx.ip,
    )
    names = await _names(session, {user_id})
    return _clip_out(dict(row), name=names.get(user_id), can_delete=True)


@router.get(
    "/meetings/{meeting_id}/clips",
    response_model=ClipList,
    summary="List a meeting's clips",
    responses={403: {"description": "Not a participant of this meeting."}},
)
async def list_clips(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ClipList:
    access, _ = await _participant_access(session, ctx, meeting_id)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, meeting_id, created_by, title, start_ms, end_ms, created_at "
                    "FROM meeting_clips WHERE meeting_id = :m ORDER BY start_ms, created_at "
                    "LIMIT :n"
                ),
                {"m": str(meeting_id), "n": MAX_CLIPS_LISTED},
            )
        )
        .mappings()
        .all()
    )
    link_rows = (
        (
            await session.execute(
                text(
                    f"SELECT {share_tokens.LINK_COLS} FROM share_links "  # noqa: S608
                    "WHERE meeting_id = :m AND kind = 'meeting_clip' ORDER BY created_at DESC"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    links = [share_tokens.row_from(r) for r in link_rows]
    names = await _names(
        session, {r["created_by"] for r in rows} | {link.created_by for link in links}
    )
    by_clip: dict[uuid.UUID, list[ClipShareLink]] = {}
    for link in links:
        if link.clip_id is None:
            continue
        can_revoke = ctx.user_id is not None and (
            link.created_by == ctx.user_id
            or access.viewer_is_host(ctx.user_id)
            or ctx.satisfies("admin")
        )
        by_clip.setdefault(link.clip_id, []).append(
            _link_out(link, ctx.org_id, name=names.get(link.created_by), can_revoke=can_revoke)
        )
    return ClipList(
        items=[
            _clip_out(
                dict(r),
                name=names.get(r["created_by"]),
                can_delete=_may_delete(ctx, r["created_by"], access),
                links=by_clip.get(r["id"]),
            )
            for r in rows
        ]
    )


@router.delete(
    "/clips/{clip_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Delete a clip",
    description="The clip's creator, the meeting's host, or a workspace admin. Its share links "
    "stop working at once. Another person's clip you may not delete is a 404.",
)
async def delete_clip(
    clip_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    clip = await clip_records.clip_row(session, clip_id)
    if clip is None or ctx.user_id is None:
        raise not_found()
    access = await records.access_for(session, clip["meeting_id"], ctx.user_id)
    if (
        access is None
        or not access.is_participant
        or not _may_delete(ctx, clip["created_by"], access)
    ):
        raise not_found()
    # share_links.clip_id is ON DELETE CASCADE: the clip's public pages go with it.
    await session.execute(text("DELETE FROM meeting_clips WHERE id = :c"), {"c": str(clip_id)})
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_CLIP_DELETED,
        target_type="clip",
        target_id=clip_id,
        metadata={"meeting_id": str(clip["meeting_id"])},
        ip=ctx.ip,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/clips/{clip_id}/share-links",
    response_model=ClipShareLink,
    status_code=status.HTTP_201_CREATED,
    summary="Share a clip",
    responses={
        403: {"description": "Not a participant, or a confidential meeting you do not host."},
        409: {"description": "A confidential meeting's clip needs `confirm_confidential`."},
        503: {"description": "Share links are not configured on this server."},
    },
)
async def share_clip(
    clip_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: ClipShareCreate | None = None,
) -> ClipShareLink:
    """A public page for one clip: its transcript lines, and its video when `include_video`."""
    req = body or ClipShareCreate()
    clip = await clip_records.clip_row(session, clip_id)
    if clip is None:
        raise not_found()
    link, name = await insert_link(
        session,
        ctx,
        kind="meeting_clip",
        meeting_id=clip["meeting_id"],
        decision_id=None,
        ttl_days=req.expires_in_days,
        confirm_confidential=req.confirm_confidential,
        clip_id=clip_id,
        include_video=req.include_video,
    )
    if req.include_video:
        # Start cutting the clip now, so the page usually has its video on the first visit. A
        # detached task, not a BackgroundTasks callback: those run before `tenant_session`
        # commits (see meeting_service/routes.py, FIX ROUND 1), and this needs no session.
        task = asyncio.create_task(
            recording_client.clip_video(
                ctx.org_id, clip["meeting_id"], clip_id, clip["start_ms"], clip["end_ms"]
            )
        )
        _PRERENDERS.add(task)
        task.add_done_callback(_PRERENDERS.discard)
    return _link_out(link, ctx.org_id, name=name, can_revoke=True)


@public_router.get(
    # authz: public - the HMAC-signed, expiring, revocable token in the path is the credential.
    "/clips/{token}",
    response_model=PublicClipShare,
    summary="Read a shared clip",
    description=(
        "No authentication: the signed token in the path is the credential. The clip's "
        "transcript lines always; its video (a short-lived presigned URL of the clip alone) only "
        "when the share includes it. 404 for a token that does not verify, a link that is not a "
        "clip, or a clip that is gone; 410 with `reason` `expired` or `revoked`. Each read "
        "increments the link's view count."
    ),
    responses={
        404: {"description": "Unknown or invalid link."},
        410: {"description": "The link expired or was revoked (`reason`)."},
    },
)
async def read_clip_share(
    response: Response,
    token: Annotated[str, Path(min_length=10, max_length=200)],
) -> PublicClipShare:
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    parsed = share_tokens.verify(token)
    if parsed is None:
        raise not_found()
    org_id, link_id = parsed
    async with get_sessionmaker()() as session, session.begin():
        link = await open_public_link(session, org_id, link_id)
        if link.kind != "meeting_clip" or link.clip_id is None:
            raise not_found()
        built = await clip_records.build_clip_share(session, link.clip_id, link.created_by)
        if built is None:
            raise not_found()  # the clip is gone, or its creator can no longer share it
        clip, _ = built
        views = await count_view(session, link_id)
        names = await _names(session, {link.created_by})
    if link.include_video:
        # Outside the transaction: meeting-service cuts and presigns the clip alone.
        clip.video = clip_records.video_view(
            await recording_client.clip_video(
                org_id, link.meeting_id, clip.clip_id, clip.start_ms, clip.end_ms
            )
        )
    return PublicClipShare(
        shared_by_name=names.get(link.created_by),
        shared_at=link.created_at,
        expires_at=link.expires_at,
        view_count=views,
        clip=clip,
    )
