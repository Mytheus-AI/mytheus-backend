"""Share links: a public, read-only page for a meeting recap or a decision record (Phase 4).

Participants create links; anyone holding one can read what it shows, with no account. The
rules, each enforced here:

- **Created by participants.** The creator must be a participant of the meeting (404 if the
  tenant cannot see it, 403 `NOT_A_PARTICIPANT` otherwise). A **confidential** meeting
  (ADR-0039 B2) is never shareable unless its host shares it: only the host may create a link
  for it (403, reason `confidential_host_only`). For a recap the host must also say so
  explicitly: `confirm_confidential: true` in the body, else 409 with reason
  `confidential_confirmation_required` (minutes for everyone: the recap now publishes the
  meeting's shared minutes, so sharing a confidential meeting is never a side effect).
- **Content limited to what the creator can see.** The page is built as the creator sees it
  (`services/core_api/records.py`): confirmed decisions and actions only, no private promises,
  notes, chat or transcript, people by name and never by email, and lineage steps from
  meetings the creator was not in shown with their content withheld. It is built at read
  time, so a decision confirmed later appears, and a creator who loses access (removed from
  the meeting, deleted, or the meeting made confidential when they are not its host) takes
  the link down with them.
- **Signed, expiring, revocable.** The token in the URL is HMAC-signed over (tenant, link id)
  (`share_tokens.py`); `expires_at` is 1 to 365 days out (default 30); the creator, the
  meeting's host, or a workspace admin can revoke it at once. An expired or revoked link
  answers 410 with `reason: expired | revoked` so the page can say so; anything else that is
  wrong (bad signature, unknown link, deleted meeting, creator without access) is a plain 404.
- **View counts.** Each successful public read increments `view_count` and `last_viewed_at`.

Routes:
- `POST /api/v1/meetings/{id}/share-links`, `GET /api/v1/meetings/{id}/share-links`
- `POST /api/v1/decisions/{id}/share-links`, `GET /api/v1/decisions/{id}/share-links`
- `DELETE /api/v1/share-links/{link_id}`
- `GET /api/v1/public/shares/{token}` (no auth: the token is the credential)
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Path, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_SHARE_LINK_CREATED, ACTION_SHARE_LINK_REVOKED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import records, share_tokens
from services.core_api.routes.chat import _not_a_participant
from services.core_api.share_tokens import ShareLinkRow

router = APIRouter(prefix="/api/v1", tags=["share-links"])
public_router = APIRouter(prefix="/api/v1/public", tags=["public-shares"])

LinkState = Literal["active", "expired", "revoked"]
MAX_LINKS_LISTED = 100


class ShareLinkCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expires_in_days: int = Field(
        default=share_tokens.DEFAULT_TTL_DAYS,
        ge=1,
        le=share_tokens.MAX_TTL_DAYS,
        description="How long the link works, 1 to 365 days. Default 30.",
    )
    confirm_confidential: bool = Field(
        default=False,
        description="The host confirms publishing the recap of a confidential meeting. "
        "Required (409 `confidential_confirmation_required`) for a confidential meeting's recap.",
    )


class ShareLink(BaseModel):
    id: uuid.UUID
    kind: share_tokens.ShareKind
    url: str = Field(description="The public page to send: `{APP_BASE_URL}/s/{token}`.")
    token: str = Field(description="The signed token in the URL; the public read takes it.")
    meeting_id: uuid.UUID
    decision_id: uuid.UUID | None = None
    created_by: uuid.UUID
    created_by_name: str | None = None
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    state: LinkState
    view_count: int
    last_viewed_at: datetime | None = None
    can_revoke: bool = Field(description="Whether the caller may revoke this link.")


class ShareLinkList(BaseModel):
    items: list[ShareLink]


class PublicShare(BaseModel):
    """What a share page renders. Exactly one of `recap` / `decision` is set, per `kind`."""

    kind: share_tokens.ShareKind
    shared_by_name: str | None = None
    shared_at: datetime
    expires_at: datetime
    view_count: int
    recap: records.MeetingRecap | None = None
    decision: records.DecisionRecord | None = None


def _state(link: ShareLinkRow) -> LinkState:
    if link.revoked_at is not None:
        return "revoked"
    return "active" if link.active else "expired"


async def _names(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, str | None]:
    if not ids:
        return {}
    rows = await session.execute(
        text("SELECT id, email FROM users WHERE id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": [str(i) for i in ids]},
    )
    return {r[0]: records.display_name(r[1]) for r in rows}


def _out(
    link: ShareLinkRow,
    org_id: uuid.UUID,
    *,
    name: str | None,
    can_revoke: bool,
) -> ShareLink:
    token = share_tokens.mint(org_id, link.id)
    return ShareLink(
        id=link.id,
        kind=link.kind,  # type: ignore[arg-type]
        url=share_tokens.share_url(token),
        token=token,
        meeting_id=link.meeting_id,
        decision_id=link.decision_id,
        created_by=link.created_by,
        created_by_name=name,
        created_at=link.created_at,
        expires_at=link.expires_at,
        revoked_at=link.revoked_at,
        state=_state(link),
        view_count=link.view_count,
        last_viewed_at=link.last_viewed_at,
        can_revoke=can_revoke,
    )


def _may_revoke(ctx: RequestContext, link: ShareLinkRow, access: records.MeetingAccess) -> bool:
    if ctx.user_id is None:
        return False
    return (
        link.created_by == ctx.user_id
        or access.viewer_is_host(ctx.user_id)
        or ctx.satisfies("admin")
    )


async def _access_or_raise(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID, *, to_share: bool
) -> tuple[records.MeetingAccess, uuid.UUID]:
    if ctx.user_id is None:
        raise _not_a_participant()
    access = await records.access_for(session, meeting_id, ctx.user_id)
    if access is None:
        raise not_found()
    if not access.is_participant:
        raise _not_a_participant()
    if to_share and not access.shareable_by(ctx.user_id):
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "This meeting is confidential: only its host can share it",
            extra={"reason": "confidential_host_only"},
        )
    return access, ctx.user_id


async def _decision_meeting(session: AsyncSession, decision_id: uuid.UUID) -> uuid.UUID:
    meeting = (
        await session.execute(
            text(
                "SELECT meeting_id FROM decisions WHERE id = :d AND deleted_at IS NULL "
                "AND status IN ('confirmed', 'superseded')"
            ),
            {"d": str(decision_id)},
        )
    ).scalar()
    if meeting is None:
        # Proposed or dismissed candidates are not records: same answer as a missing one.
        raise not_found()
    return uuid.UUID(str(meeting))


async def _create(
    session: AsyncSession,
    ctx: RequestContext,
    *,
    kind: share_tokens.ShareKind,
    meeting_id: uuid.UUID,
    decision_id: uuid.UUID | None,
    ttl_days: int,
    confirm_confidential: bool = True,
) -> ShareLink:
    if not share_tokens.configured():
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "Share links are not configured on this server",
            extra={"reason": "not_configured"},
        )
    access, user_id = await _access_or_raise(session, ctx, meeting_id, to_share=True)
    if access.confidential and not confirm_confidential:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "This meeting is confidential: confirm that you want to publish its recap",
            extra={"reason": "confidential_confirmation_required"},
        )
    link = await share_tokens.create_link(
        session,
        org_id=ctx.org_id,
        kind=kind,
        meeting_id=meeting_id,
        decision_id=decision_id,
        created_by=user_id,
        ttl_days=ttl_days,
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_SHARE_LINK_CREATED,
        target_type="share_link",
        target_id=link.id,
        metadata={
            "kind": kind,
            "meeting_id": str(meeting_id),
            "decision_id": str(decision_id) if decision_id else None,
            "expires_at": link.expires_at.isoformat(),
        },
        ip=ctx.ip,
    )
    names = await _names(session, {user_id})
    return _out(link, ctx.org_id, name=names.get(user_id), can_revoke=True)


async def _list(
    session: AsyncSession,
    ctx: RequestContext,
    meeting_id: uuid.UUID,
    where: str,
    params: dict[str, str],
) -> ShareLinkList:
    access, _ = await _access_or_raise(session, ctx, meeting_id, to_share=False)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {share_tokens.LINK_COLS} FROM share_links WHERE {where} "
                    "ORDER BY created_at DESC LIMIT :n"
                ),
                {**params, "n": MAX_LINKS_LISTED},
            )
        )
        .mappings()
        .all()
    )
    links = [share_tokens.row_from(r) for r in rows]
    names = await _names(session, {link.created_by for link in links})
    return ShareLinkList(
        items=[
            _out(
                link,
                ctx.org_id,
                name=names.get(link.created_by),
                can_revoke=_may_revoke(ctx, link, access),
            )
            for link in links
        ]
    )


_CREATED = {
    403: {"description": "Not a participant, or a confidential meeting you do not host."},
    503: {"description": "Share links are not configured on this server."},
}


_RECAP_CREATED = {
    **_CREATED,
    409: {"description": "A confidential meeting's recap needs `confirm_confidential`."},
}


@router.post(
    "/meetings/{meeting_id}/share-links",
    response_model=ShareLink,
    status_code=status.HTTP_201_CREATED,
    summary="Share a meeting recap",
    responses=_RECAP_CREATED,  # type: ignore[arg-type]
)
async def create_recap_link(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: ShareLinkCreate | None = None,
) -> ShareLink:
    """The public recap: confirmed decisions and actions and the meeting's shared minutes
    (never anyone's personal minutes or promises)."""
    req = body or ShareLinkCreate()
    return await _create(
        session,
        ctx,
        kind="meeting_recap",
        meeting_id=meeting_id,
        decision_id=None,
        ttl_days=req.expires_in_days,
        confirm_confidential=req.confirm_confidential,
    )


@router.get(
    "/meetings/{meeting_id}/share-links",
    response_model=ShareLinkList,
    summary="List a meeting's share links",
)
async def list_meeting_links(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ShareLinkList:
    return await _list(session, ctx, meeting_id, "meeting_id = :m", {"m": str(meeting_id)})


@router.post(
    "/decisions/{decision_id}/share-links",
    response_model=ShareLink,
    status_code=status.HTTP_201_CREATED,
    summary="Share a decision record",
    responses=_CREATED,  # type: ignore[arg-type]
)
async def create_decision_link(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: ShareLinkCreate | None = None,
) -> ShareLink:
    meeting_id = await _decision_meeting(session, decision_id)
    return await _create(
        session,
        ctx,
        kind="decision_record",
        meeting_id=meeting_id,
        decision_id=decision_id,
        ttl_days=(body or ShareLinkCreate()).expires_in_days,
    )


@router.get(
    "/decisions/{decision_id}/share-links",
    response_model=ShareLinkList,
    summary="List a decision record's share links",
)
async def list_decision_links(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ShareLinkList:
    meeting_id = await _decision_meeting(session, decision_id)
    return await _list(session, ctx, meeting_id, "decision_id = :d", {"d": str(decision_id)})


@router.delete(
    "/share-links/{link_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Revoke a share link",
    description="The link's creator, the meeting's host, or a workspace admin. Idempotent.",
)
async def revoke_link(
    link_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    row = (
        (
            await session.execute(
                text(f"SELECT {share_tokens.LINK_COLS} FROM share_links WHERE id = :id FOR UPDATE"),
                {"id": str(link_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None or ctx.user_id is None:
        raise not_found()
    link = share_tokens.row_from(row)
    access = await records.access_for(session, link.meeting_id, ctx.user_id)
    if access is None or not _may_revoke(ctx, link, access):
        # Whether someone else's link exists is not the caller's to know.
        raise not_found()
    if link.revoked_at is None:
        await session.execute(
            text("UPDATE share_links SET revoked_at = now(), revoked_by = :u WHERE id = :id"),
            {"u": str(ctx.user_id), "id": str(link_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_SHARE_LINK_REVOKED,
            target_type="share_link",
            target_id=link_id,
            metadata={"kind": link.kind, "meeting_id": str(link.meeting_id)},
            ip=ctx.ip,
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _gone(reason: Literal["expired", "revoked"]) -> ProblemDetail:
    return ProblemDetail(
        410,
        ErrorCode.NOT_FOUND,
        "This link has expired" if reason == "expired" else "This link was turned off",
        extra={"reason": reason},
    )


@public_router.get(
    # authz: public - the HMAC-signed, expiring, revocable token in the path is the credential.
    "/shares/{token}",
    response_model=PublicShare,
    summary="Read a shared recap or decision record",
    description=(
        "No authentication: the signed token in the path is the credential. 404 for a token "
        "that does not verify or a record that is gone; 410 with `reason` `expired` or "
        "`revoked` for a link that was valid. Each read increments the link's view count."
    ),
    responses={
        404: {"description": "Unknown or invalid link."},
        410: {"description": "The link expired or was revoked (`reason`)."},
    },
)
async def read_share(
    response: Response,
    token: Annotated[str, Path(min_length=10, max_length=200)],
) -> PublicShare:
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    parsed = share_tokens.verify(token)
    if parsed is None:
        raise not_found()
    org_id, link_id = parsed
    async with get_sessionmaker()() as session, session.begin():
        from libs.shared.org_resolver import resolve_org_by_id

        if await resolve_org_by_id(session, org_id) is None:
            raise not_found()
        await set_rls(session, org_id)
        row = (
            (
                await session.execute(
                    text(
                        f"SELECT {share_tokens.LINK_COLS} FROM share_links WHERE id = :id "
                        "FOR UPDATE"
                    ),
                    {"id": str(link_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found()
        link = share_tokens.row_from(row)
        if link.revoked_at is not None:
            raise _gone("revoked")
        if not link.active:
            raise _gone("expired")
        recap: records.MeetingRecap | None = None
        decision: records.DecisionRecord | None = None
        if link.kind == "meeting_recap":
            recap = await records.build_recap(session, link.meeting_id, link.created_by)
        elif link.decision_id is not None:
            decision = await records.build_decision_record(
                session, link.decision_id, link.created_by
            )
        if recap is None and decision is None:
            raise not_found()  # the creator can no longer see it, or it is gone
        views = (
            await session.execute(
                text(
                    "UPDATE share_links SET view_count = view_count + 1, last_viewed_at = now() "
                    "WHERE id = :id RETURNING view_count"
                ),
                {"id": str(link_id)},
            )
        ).scalar_one()
        names = await _names(session, {link.created_by})
    return PublicShare(
        kind=link.kind,  # type: ignore[arg-type]
        shared_by_name=names.get(link.created_by),
        shared_at=link.created_at,
        expires_at=link.expires_at,
        view_count=int(views),
        recap=recap,
        decision=decision,
    )
