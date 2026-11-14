"""The workspace logo (ADR-0048): upload, read, serve and remove.

- `PUT /api/v1/org/logo` (admin): the image bytes are the request body, with the image's
  `Content-Type`. No multipart: `python-multipart` is not a runtime dependency, and a raw body
  streams through the gateway as it is. Validated by magic bytes and header (`images.py`), 2 MB
  at most, PNG/JPEG/WebP. Rate limited per workspace (`ORG_LOGO_UPLOADS_PER_HOUR`, default 20).
- `DELETE /api/v1/org/logo` (admin): removes it. Idempotent: no logo is still a 204.
- `GET /api/v1/org/logo` (member): `{logo_url, logo_updated_at, ...}`; `logo_url` is null when
  there is no logo. Also returned additively on `GET /orgs`.
- `GET /api/v1/org/logo/image` (member): the bytes. `logo_url` points here with `?v={version}`.
  A versioned request is `Cache-Control: private, max-age=31536000, immutable`; every upload is a
  new version, so a cached copy is never stale. ETag + `If-None-Match` answer 304.

Why a proxy rather than a presigned R2 URL (what recordings and briefings use): a logo is drawn
on every screen. A presigned URL changes on every read, so the browser could never cache it, and
it expires (10 minutes), so a tab left open would show a broken image the next time the sidebar
re-renders. A stable versioned URL behind the tenant check caches for good and never expires;
the bucket stays private. The web app relays it through its own same-origin route
(`/api/org-logo`), the way it relays late-contribution audio, because an `<img>` cannot carry
the bearer token.

Tenant: from the verified token only; there is no parameter that names a workspace, so another
tenant's logo is unreachable by construction, and "no logo" is a 404.

Order of writes on upload: object first (new key), then the row and its audit entry in one
transaction, then the previous object is deleted after COMMIT. A failed transaction deletes the
object it just wrote; a failed old-object delete leaves an orphan under the tenant's prefix
(harmless, listed by any `{org_id}/branding/` sweep) rather than a row pointing at nothing.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.auth import verify_token
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.cache import get_redis, redis_key
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail, problem_response
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.branding import images
from services.identity_tenant_service.branding.store import get_store, logo_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["workspace-logo"], dependencies=[Depends(verify_token)])

ACTION_LOGO_UPDATED = "tenant_settings.logo.updated"
ACTION_LOGO_REMOVED = "tenant_settings.logo.removed"

RATE_ENV = "ORG_LOGO_UPLOADS_PER_HOUR"
DEFAULT_RATE = 20
IMAGE_PATH = "/api/v1/org/logo/image"
IMMUTABLE = "private, max-age=31536000, immutable"

Member = Annotated[RequestContext, Depends(require_tier("member"))]
Admin = Annotated[RequestContext, Depends(require_tier("admin"))]


class OrgLogo(BaseModel):
    logo_url: str | None = Field(
        description="Gateway-relative URL of the logo image (send the bearer token), or null "
        "when the workspace has no logo. Versioned, so it is safe to cache for good."
    )
    logo_updated_at: datetime | None = None
    logo_version: int = Field(
        default=0, description="Bumped on every upload; part of the URL so caches never go stale."
    )
    content_type: str | None = Field(
        default=None, description="image/png, image/jpeg or image/webp"
    )
    size_bytes: int | None = None
    width: int | None = None
    height: int | None = None


_COLS = (
    "logo_key, logo_content_type, logo_version, logo_bytes, logo_width, logo_height, "
    "logo_updated_at"
)


def logo_url(version: int) -> str:
    return f"{IMAGE_PATH}?v={version}"


def to_model(row: Any) -> OrgLogo:
    if row is None or row["logo_key"] is None:
        return OrgLogo(logo_url=None, logo_version=int(row["logo_version"]) if row else 0)
    return OrgLogo(
        logo_url=logo_url(int(row["logo_version"])),
        logo_updated_at=row["logo_updated_at"],
        logo_version=int(row["logo_version"]),
        content_type=row["logo_content_type"],
        size_bytes=row["logo_bytes"],
        width=row["logo_width"],
        height=row["logo_height"],
    )


async def read_logo_row(session: AsyncSession, org_id: Any) -> Any:
    """The caller's logo columns. `session` must already have `app.current_org` bound."""
    return (
        (
            await session.execute(
                text(f"SELECT {_COLS} FROM tenant_settings WHERE org_id = :o"),
                {"o": str(org_id)},
            )
        )
        .mappings()
        .first()
    )


@asynccontextmanager
async def _tx(ctx: RequestContext) -> AsyncIterator[AsyncSession]:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        yield session


def _rejected(status_code: int, reason: str, message: str) -> ProblemDetail:
    return ProblemDetail(
        status_code,
        ErrorCode.VALIDATION_FAILED,
        message,
        extra={
            "reason": reason,
            "errors": [{"loc": ["body"], "msg": message, "type": reason}],
        },
    )


def _status_for(reason: str) -> int:
    return {"too_large": 413, "unsupported_type": 415, "svg_not_accepted": 415}.get(reason, 422)


def hourly_limit() -> int:
    try:
        return max(1, int(os.environ.get(RATE_ENV, DEFAULT_RATE)))
    except ValueError:
        return DEFAULT_RATE


async def take_rate(ctx: RequestContext) -> tuple[bool, int]:
    """A fixed one-hour window per workspace, shared by every replica. Redis down -> 503."""
    now = time.time()
    key = redis_key(ctx.org_id, "ratelimit", "org-logo", int(now // 3600))
    try:
        redis = get_redis()
        count = int(await redis.incr(key))
        if count == 1:
            await redis.expire(key, 7200)
    except Exception:  # noqa: BLE001 - any Redis failure fails closed
        logger.exception("org logo: rate-limit store unavailable")
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Logo uploads are unavailable; retry shortly"
        ) from None
    return count <= hourly_limit(), max(1, 3600 - int(now % 3600))


async def _read_body(request: Request) -> bytes:
    """The body, refusing early past the limit instead of buffering an unbounded upload."""
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > images.MAX_BYTES:
        raise _rejected(413, "too_large", "The logo must be 2 MB or smaller.")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > images.MAX_BYTES:
            raise _rejected(413, "too_large", "The logo must be 2 MB or smaller.")
        chunks.append(chunk)
    return b"".join(chunks)


def _storage_unavailable() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Logo storage is not connected on this server, so logos cannot be uploaded yet.",
        extra={"reason": "storage_not_configured"},
    )


_IMAGE_BODY = {
    "requestBody": {
        "required": True,
        "description": "The image bytes (PNG, JPEG or WebP, 2 MB at most). SVG is refused.",
        "content": {
            images.PNG: {"schema": {"type": "string", "format": "binary"}},
            images.JPEG: {"schema": {"type": "string", "format": "binary"}},
            images.WEBP: {"schema": {"type": "string", "format": "binary"}},
        },
    }
}


@router.get("/org/logo", response_model=OrgLogo, summary="The workspace logo")
async def read_org_logo(ctx: Member) -> OrgLogo:
    """authz: member. `logo_url` is null when the workspace has no logo."""
    async with _tx(ctx) as session:
        row = await read_logo_row(session, ctx.org_id)
    return to_model(row)


@router.put(
    "/org/logo",
    response_model=OrgLogo,
    summary="Upload or replace the workspace logo",
    openapi_extra=_IMAGE_BODY,
    responses={
        413: {"description": "Larger than 2 MB."},
        415: {"description": "Not a PNG, JPEG or WebP (SVG is refused)."},
        429: {"description": "Too many uploads this hour."},
        503: {"description": "Object storage is not connected."},
    },
)
async def put_org_logo(request: Request, ctx: Admin) -> Any:
    """authz: admin. The body is the image; the type is decided from its bytes."""
    if not ctx.can_write:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "This API token is read-only")
    store = get_store()
    if store is None:
        raise _storage_unavailable()
    allowed, reset = await take_rate(ctx)
    if not allowed:
        response = problem_response(
            429,
            ErrorCode.RATE_LIMITED,
            f"Logos can be changed {hourly_limit()} times an hour",
            extra={"retry_after": reset},
        )
        response.headers["Retry-After"] = str(reset)
        return response

    data = await _read_body(request)
    try:
        info = images.inspect(data)
    except images.ImageRejected as exc:
        raise _rejected(_status_for(exc.reason), exc.reason, str(exc)) from None

    previous_key: str | None = None
    new_key: str | None = None
    try:
        async with _tx(ctx) as session:
            # Upsert-and-lock the settings row (provisioning does not create one, see
            # routes/tenant_settings.py), then bump the version under that lock so two
            # concurrent uploads cannot claim the same key.
            row = (
                (
                    await session.execute(
                        text(
                            "INSERT INTO tenant_settings (org_id) VALUES (:o) "
                            "ON CONFLICT (org_id) DO UPDATE SET org_id = EXCLUDED.org_id "
                            "RETURNING logo_key, logo_version"
                        ),
                        {"o": str(ctx.org_id)},
                    )
                )
                .mappings()
                .one()
            )
            previous_key = row["logo_key"]
            version = int(row["logo_version"]) + 1
            new_key = logo_key(ctx.org_id, version, images.EXTENSIONS[info.content_type])
            await store.put(new_key, data, content_type=info.content_type)
            saved = (
                (
                    await session.execute(
                        text(
                            "UPDATE tenant_settings SET logo_key = :k, logo_content_type = :ct, "
                            "logo_version = :v, logo_bytes = :b, logo_width = :w, "
                            "logo_height = :h, logo_updated_at = now(), updated_at = now() "
                            f"WHERE org_id = :o RETURNING {_COLS}"
                        ),
                        {
                            "o": str(ctx.org_id),
                            "k": new_key,
                            "ct": info.content_type,
                            "v": version,
                            "b": len(data),
                            "w": info.width,
                            "h": info.height,
                        },
                    )
                )
                .mappings()
                .one()
            )
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_LOGO_UPDATED,
                target_type="tenant_settings",
                target_id=ctx.org_id,
                metadata={
                    "version": version,
                    "content_type": info.content_type,
                    "bytes": len(data),
                    "width": info.width,
                    "height": info.height,
                    "replaced": previous_key is not None,
                },
                ip=ctx.ip,
            )
    except ProblemDetail:
        raise
    except Exception:
        logger.exception("org logo: upload failed")
        if new_key is not None:
            try:
                await store.delete(new_key)
            except Exception:  # noqa: BLE001 - an orphan under the tenant prefix is harmless
                logger.warning("org logo: could not remove the object of a failed upload")
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "The logo could not be saved; try again"
        ) from None

    if previous_key and previous_key != new_key:
        try:
            await store.delete(previous_key)
        except Exception:  # noqa: BLE001 - after COMMIT; the new logo is already live
            logger.warning("org logo: could not delete the previous logo object")
    return to_model(saved)


@router.delete(
    "/org/logo",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove the workspace logo",
    responses={503: {"description": "Object storage is not connected."}},
)
async def delete_org_logo(ctx: Admin) -> Response:
    """authz: admin. Idempotent: a workspace with no logo answers 204 too."""
    if not ctx.can_write:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "This API token is read-only")
    async with _tx(ctx) as session:
        row = await read_logo_row(session, ctx.org_id)
        previous_key = row["logo_key"] if row else None
        if previous_key is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        await session.execute(
            text(
                "UPDATE tenant_settings SET logo_key = NULL, logo_content_type = NULL, "
                "logo_bytes = NULL, logo_width = NULL, logo_height = NULL, "
                "logo_updated_at = now(), updated_at = now() WHERE org_id = :o"
            ),
            {"o": str(ctx.org_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_LOGO_REMOVED,
            target_type="tenant_settings",
            target_id=ctx.org_id,
            metadata={"version": int(row["logo_version"])},
            ip=ctx.ip,
        )
    store = get_store()
    if store is not None:
        try:
            await store.delete(previous_key)
        except Exception:  # noqa: BLE001 - the row no longer points at it
            logger.warning("org logo: could not delete the removed logo object")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/org/logo/image",
    summary="The workspace logo image",
    response_class=Response,
    responses={
        200: {
            "content": {images.PNG: {}, images.JPEG: {}, images.WEBP: {}},
            "description": "The image. Immutable when `v` names the current version.",
        },
        304: {"description": "Not modified (If-None-Match)."},
    },
)
async def org_logo_image(
    request: Request,
    ctx: Member,
    v: Annotated[int | None, Query(ge=0, description="The version from logo_url")] = None,
) -> Response:
    """authz: member. 404 when the workspace has no logo, or `v` names a replaced version."""
    async with _tx(ctx) as session:
        row = await read_logo_row(session, ctx.org_id)
    if row is None or row["logo_key"] is None:
        raise not_found()
    version = int(row["logo_version"])
    if v is not None and v != version:
        raise not_found()
    etag = f'"logo-{version}"'
    headers = {
        "ETag": etag,
        "Cache-Control": IMMUTABLE if v is not None else "private, no-cache",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; sandbox",
        "Vary": "Authorization",
    }
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    store = get_store()
    if store is None:
        raise _storage_unavailable()
    data = await store.get(str(row["logo_key"]))
    if data is None:
        raise not_found()
    return Response(content=data, media_type=str(row["logo_content_type"]), headers=headers)
