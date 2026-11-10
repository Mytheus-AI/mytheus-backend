"""Personal access tokens: create, list, revoke (Phase 4, public API and MCP).

- `GET /api/v1/api-tokens` lists the caller's own tokens; `?owner=all` (owner/admin) lists the
  whole workspace's, so an admin can find and revoke a token a departed person left behind.
- `POST /api/v1/api-tokens` {name, scope, expires_in_days} creates one and returns the token
  text ONCE (`Cache-Control: no-store`). Only its SHA-256 and last four characters are kept.
- `DELETE /api/v1/api-tokens/{id}` revokes one: its owner, or an owner/admin for anyone's.
  Revocation takes effect on the next request (every request re-reads the row; nothing caches
  a resolved token). Repeating it is a 204 that writes nothing.

These routes take a signed-in SESSION only: `/api/v1/api-tokens` is outside the public API
surface in `libs/shared/pat.py`, so a token can neither mint nor revoke tokens, and a leaked
token cannot make itself permanent.

`last_used_at` comes from Redis (`nexus:{org_id}:pat:last_used:{token_id}`), written by the
gateway on each use; a token never used, or not used in 90 days, shows null.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.asyncio import from_url as redis_from_url
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import pat
from libs.shared.audit import ACTION_API_TOKEN_CREATED, ACTION_API_TOKEN_REVOKED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(prefix="/api/v1", tags=["api-tokens"])

MAX_ACTIVE_PER_USER = 20
DEFAULT_EXPIRY_DAYS = 90
MAX_EXPIRY_DAYS = 365

TokenScope = Literal["read", "read_write"]
TokenState = Literal["active", "expired", "revoked"]

_COLS = (
    "t.id, t.user_id, u.email AS owner_email, t.name, t.scope, t.token_hint, t.created_at, "
    "t.expires_at, t.revoked_at"
)


class ApiTokenCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        min_length=1,
        max_length=80,
        description="What the token is for, e.g. 'Claude Desktop' or 'HubSpot sync script'.",
    )
    scope: TokenScope = Field(
        default="read",
        description=(
            "`read`: GET requests, search and ask, and the MCP read tools. `read_write`: also "
            "creates and updates (confirm, edit, create tasks) as the token's owner."
        ),
    )
    expires_in_days: int = Field(
        default=DEFAULT_EXPIRY_DAYS,
        ge=1,
        le=MAX_EXPIRY_DAYS,
        description="Lifetime in days, 1 to 365. Every token expires.",
    )

    @field_validator("name")
    @classmethod
    def _strip(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value


class ApiToken(BaseModel):
    """A token as listed. Never the token text: only its last four characters."""

    id: uuid.UUID
    user_id: uuid.UUID
    owner_email: str | None = Field(
        default=None, description="Present in the workspace-wide (`owner=all`) listing only."
    )
    name: str
    scope: TokenScope
    token_preview: str = Field(description="`mytheus_pat_...abcd`, to recognise a token.")
    state: TokenState
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    last_used_at: datetime | None = Field(
        default=None, description="Last request the gateway accepted with it (90-day memory)."
    )


class ApiTokenCreated(ApiToken):
    token: str = Field(
        description="The token itself. Shown in this response only; store it now.",
    )


class ApiTokenList(BaseModel):
    items: list[ApiToken]


def _state(row: Any) -> TokenState:
    if row["revoked_at"] is not None:
        return "revoked"
    return "expired" if row["expires_at"] <= datetime.now(UTC) else "active"


def _item(row: Any, *, include_owner: bool, last_used: datetime | None) -> ApiToken:
    return ApiToken(
        id=row["id"],
        user_id=row["user_id"],
        owner_email=row["owner_email"] if include_owner else None,
        name=row["name"],
        scope=row["scope"],
        token_preview=f"{pat.PAT_PREFIX}...{row['token_hint']}",
        state=_state(row),
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
        last_used_at=last_used,
    )


async def _last_used(org_id: uuid.UUID, ids: list[uuid.UUID]) -> dict[uuid.UUID, datetime]:
    """Best effort: a Redis outage shows "unknown" (null), never fails the list."""
    if not ids:
        return {}
    keys = [f"nexus:{org_id}:pat:last_used:{i}" for i in ids]
    try:
        async with redis_from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True
        ) as redis:
            values = await redis.mget(keys)
    except (RedisError, OSError):
        return {}
    out: dict[uuid.UUID, datetime] = {}
    for token_id, value in zip(ids, values, strict=True):
        if value:
            try:
                out[token_id] = datetime.fromisoformat(str(value))
            except ValueError:
                continue
    return out


def _require_user(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this account")
    return ctx.user_id


@router.get("/api-tokens", response_model=ApiTokenList)
async def list_api_tokens(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    owner: Annotated[
        Literal["me", "all"],
        Query(description="`me` (default): your tokens. `all`: everyone's (owner/admin)."),
    ] = "me",
) -> ApiTokenList:
    user_id = _require_user(ctx)
    if owner == "all" and not ctx.satisfies("admin"):
        raise ProblemDetail(
            403, ErrorCode.FORBIDDEN_ROLE, "Listing everyone's tokens requires admin or above"
        )
    sql = f"SELECT {_COLS} FROM api_tokens t JOIN users u ON u.id = t.user_id"
    params: dict[str, Any] = {}
    if owner == "me":
        sql += " WHERE t.user_id = :u"
        params["u"] = str(user_id)
    sql += " ORDER BY (t.revoked_at IS NULL) DESC, t.created_at DESC LIMIT 500"
    rows = (await session.execute(text(sql), params)).mappings().all()
    last = await _last_used(ctx.org_id, [r["id"] for r in rows])
    return ApiTokenList(
        items=[_item(r, include_owner=owner == "all", last_used=last.get(r["id"])) for r in rows]
    )


@router.post(
    "/api-tokens",
    response_model=ApiTokenCreated,
    status_code=status.HTTP_201_CREATED,
    responses={409: {"description": "You already have 20 active tokens; revoke one first."}},
)
async def create_api_token(
    body: ApiTokenCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ApiTokenCreated:
    user_id = _require_user(ctx)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('api_tokens:' || :u))"), {"u": str(user_id)}
    )
    active = (
        await session.execute(
            text(
                "SELECT count(*) FROM api_tokens WHERE user_id = :u AND revoked_at IS NULL "
                "AND expires_at > now()"
            ),
            {"u": str(user_id)},
        )
    ).scalar_one()
    if int(active) >= MAX_ACTIVE_PER_USER:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            f"You already have {MAX_ACTIVE_PER_USER} active tokens; revoke one first",
            extra={"reason": "too_many_tokens"},
        )
    token, digest, hint = pat.generate(ctx.org_id)
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO api_tokens (org_id, user_id, name, token_hash, token_hint, scope, "
                    "expires_at) VALUES (:o, :u, :n, :h, :hint, :s, :e) RETURNING id"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(user_id),
                    "n": body.name,
                    "h": digest,
                    "hint": hint,
                    "s": body.scope,
                    "e": datetime.now(UTC) + timedelta(days=body.expires_in_days),
                },
            )
        )
        .mappings()
        .one()
    )
    created = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM api_tokens t JOIN users u ON u.id = t.user_id "
                    "WHERE t.id = :id"
                ),
                {"id": str(row["id"])},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_API_TOKEN_CREATED,
        target_type="api_token",
        target_id=row["id"],
        metadata={"scope": body.scope, "expires_in_days": body.expires_in_days},
        ip=ctx.ip,
    )
    response.headers["Cache-Control"] = "no-store"
    item = _item(created, include_owner=False, last_used=None)
    return ApiTokenCreated(**item.model_dump(), token=token)


@router.delete(
    "/api-tokens/{token_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response
)
async def revoke_api_token(
    token_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    user_id = _require_user(ctx)
    row = (
        (
            await session.execute(
                text("SELECT user_id, revoked_at FROM api_tokens WHERE id = :id FOR UPDATE"),
                {"id": str(token_id)},
            )
        )
        .mappings()
        .first()
    )
    # Someone else's token is a 404 to a member: whether it exists is not theirs to know.
    if row is None or (row["user_id"] != user_id and not ctx.satisfies("admin")):
        raise not_found()
    if row["revoked_at"] is None:
        await session.execute(
            text("UPDATE api_tokens SET revoked_at = now(), revoked_by = :u WHERE id = :id"),
            {"u": str(user_id), "id": str(token_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=user_id,
            action=ACTION_API_TOKEN_REVOKED,
            target_type="api_token",
            target_id=token_id,
            metadata={"owner_id": str(row["user_id"])},
            ip=ctx.ip,
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
