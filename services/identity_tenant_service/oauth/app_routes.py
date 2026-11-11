"""The signed-in half of the OAuth flow: the consent page's backend, and Connected apps.

- `GET /api/v1/oauth/consent?request=...`: what the consent page shows. The signed request from
  `GET /oauth/authorize` is opened (400 `invalid_request` / `expired_request` when tampered or
  older than 15 minutes), the client is re-loaded (404 `unknown_client` when it is gone or
  revoked), and the person sees the app's name, where it sends them back to, what it may do and
  which workspace it would reach.
- `POST /api/v1/oauth/consent` {request, decision, scope?}: approve records a five-minute,
  single-use authorization code bound to this person, this workspace, the client, the redirect
  URI, the PKCE challenge, the scope and the resource, and returns the redirect URL carrying
  `code`, `state` and `iss`. A person may narrow `read_write` to `read`, never widen it. Deny
  returns the redirect URL with `error=access_denied`. Both are audited.
- `GET /api/v1/oauth/grants`: the caller's connected apps (active grants), with last use.
  `?owner=all` lists the workspace's (owner/admin), like personal access tokens.
- `DELETE /api/v1/oauth/grants/{id}`: disconnect one: its refresh and access tokens die at once.
  Someone else's grant is a 404 to a member; another tenant's is a 404 to everyone (RLS).

These take a Clerk session only: `/api/v1/oauth` is outside the public API surface in
`libs/shared/pat.py`, so no token (personal or OAuth) can approve a client or list grants.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import oauth_config
from libs.shared.audit import (
    ACTION_OAUTH_AUTHORIZATION_DENIED,
    ACTION_OAUTH_AUTHORIZATION_GRANTED,
    ACTION_OAUTH_GRANT_REVOKED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.identity_tenant_service.oauth import core
from services.identity_tenant_service.oauth.public_routes import revoke_grant
from services.identity_tenant_service.routes.api_tokens import _last_used

router = APIRouter(prefix="/api/v1/oauth", tags=["oauth"])

OAuthScope = Literal["read", "read_write"]


class ConsentClient(BaseModel):
    client_id: str
    name: str = Field(description="What the app calls itself at registration. Unverified.")
    client_uri: str | None = None
    redirect_uri: str
    redirect_host: str = Field(description="Where approving sends the browser. Show it.")
    verified_redirect: bool = Field(
        description="The redirect goes to a host Mytheus recognises (Claude's callback)."
    )
    loopback_redirect: bool = Field(
        description="The redirect goes to this computer (a desktop or command-line app)."
    )
    registered_at: datetime


class ConsentWorkspace(BaseModel):
    name: str


class ConsentRequest(BaseModel):
    client: ConsentClient
    scope: OAuthScope = Field(description="What the app asked for, as a token scope.")
    resource: str = Field(description="The MCP server the access is for.")
    workspace: ConsentWorkspace
    account_email: str | None = None
    expires_at: datetime = Field(description="Approve before this, or start again from the app.")


class ConsentDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: str = Field(min_length=1, max_length=8000)
    decision: Literal["approve", "deny"]
    scope: OAuthScope | None = Field(
        default=None, description="Approve with less than was asked (`read`); never more."
    )


class ConsentResult(BaseModel):
    decision: Literal["approve", "deny"]
    redirect_to: str = Field(description="Send the browser here: the app's own callback.")


class ConnectedApp(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    owner_email: str | None = Field(
        default=None, description="Present in the workspace-wide (`owner=all`) listing only."
    )
    client_id: str
    client_name: str
    redirect_host: str
    verified_redirect: bool
    scope: OAuthScope
    connected_at: datetime
    last_refreshed_at: datetime | None = None
    last_used_at: datetime | None = Field(
        default=None, description="Last MCP request the gateway accepted from it."
    )


class ConnectedAppList(BaseModel):
    items: list[ConnectedApp]


def _require_user(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this account")
    return ctx.user_id


def _open(token: str) -> core.AuthRequest:
    try:
        return core.open_request(token)
    except core.RequestExpired:
        raise ProblemDetail(
            400,
            ErrorCode.VALIDATION_FAILED,
            "This sign-in request has expired; start again from the app",
            extra={"reason": "expired_request"},
        ) from None
    except core.RequestInvalid:
        raise ProblemDetail(
            400,
            ErrorCode.VALIDATION_FAILED,
            "This sign-in request is not valid; start again from the app",
            extra={"reason": "invalid_request"},
        ) from None
    except core.SigningNotConfigured:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Sign-in for apps is not configured"
        ) from None


async def _client_for(session: AsyncSession, req: core.AuthRequest) -> dict[str, Any]:
    client = await core.load_client(session, req.client_id)
    if client is None or not core.redirect_matches(list(client["redirect_uris"]), req.redirect_uri):
        raise ProblemDetail(
            404,
            ErrorCode.NOT_FOUND,
            "This app is no longer registered with Mytheus",
            extra={"reason": "unknown_client"},
        )
    return client


@router.get("/consent", response_model=ConsentRequest)
async def get_consent(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    request: Annotated[str, Query(min_length=1, max_length=8000)],
) -> ConsentRequest:
    user_id = _require_user(ctx)
    req = _open(request)
    client = await _client_for(session, req)
    org = (
        (
            await session.execute(
                text("SELECT name FROM organizations WHERE id = :o"), {"o": str(ctx.org_id)}
            )
        )
        .mappings()
        .one()
    )
    email = (
        await session.execute(text("SELECT email FROM users WHERE id = :u"), {"u": str(user_id)})
    ).scalar_one_or_none()
    return ConsentRequest(
        client=ConsentClient(
            client_id=str(client["client_id"]),
            name=str(client["client_name"]),
            client_uri=client["client_uri"],
            redirect_uri=req.redirect_uri,
            redirect_host=core.redirect_host(req.redirect_uri),
            verified_redirect=core.is_trusted_redirect(req.redirect_uri),
            loopback_redirect=core.is_loopback(req.redirect_uri),
            registered_at=client["created_at"],
        ),
        scope=req.scope,  # type: ignore[arg-type]
        resource=req.resource,
        workspace=ConsentWorkspace(name=str(org["name"])),
        account_email=email,
        expires_at=datetime.fromtimestamp(req.exp, UTC),
    )


@router.post("/consent", response_model=ConsentResult)
async def decide_consent(
    body: ConsentDecision,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ConsentResult:
    user_id = _require_user(ctx)
    req = _open(body.request)
    client = await _client_for(session, req)
    response.headers["Cache-Control"] = "no-store"
    base = {"state": req.state, "iss": oauth_config.issuer()}
    if body.decision == "deny":
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=user_id,
            action=ACTION_OAUTH_AUTHORIZATION_DENIED,
            target_type="oauth_client",
            target_id=client["client_id"],
            metadata={"client_name": client["client_name"], "scope": req.scope},
            ip=ctx.ip,
        )
        return ConsentResult(
            decision="deny",
            redirect_to=core.with_params(
                req.redirect_uri,
                {
                    "error": "access_denied",
                    "error_description": "The person declined to connect this app",
                    **base,
                },
            ),
        )

    scope = req.scope
    if body.scope is not None:
        if body.scope == "read_write" and req.scope != "read_write":
            raise ProblemDetail(
                422,
                ErrorCode.VALIDATION_FAILED,
                "You can approve less than the app asked for, not more",
                extra={"reason": "scope_exceeds_request"},
            )
        scope = body.scope
    code, digest = core.mint(core.CODE_PREFIX, ctx.org_id)
    await session.execute(
        text(
            "INSERT INTO oauth_authorization_codes (org_id, user_id, client_id, code_hash, "
            "redirect_uri, code_challenge, scope, resource, expires_at) VALUES (:o, :u, :c, :h, "
            ":r, :cc, :s, :res, now() + make_interval(secs => :ttl))"
        ),
        {
            "o": str(ctx.org_id),
            "u": str(user_id),
            "c": client["client_id"],
            "h": digest,
            "r": req.redirect_uri,
            "cc": req.code_challenge,
            "s": scope,
            "res": req.resource,
            "ttl": core.CODE_TTL_SECONDS,
        },
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_OAUTH_AUTHORIZATION_GRANTED,
        target_type="oauth_client",
        target_id=client["client_id"],
        metadata={
            "client_name": client["client_name"],
            "scope": scope,
            "requested_scope": req.scope,
            "redirect_host": core.redirect_host(req.redirect_uri),
            "client_registered_at": client["created_at"].isoformat(),
        },
        ip=ctx.ip,
    )
    return ConsentResult(
        decision="approve",
        redirect_to=core.with_params(req.redirect_uri, {"code": code, **base}),
    )


_GRANT_COLS = (
    "g.id, g.user_id, u.email AS owner_email, g.client_id, c.client_name, c.redirect_uris, "
    "g.scope, g.created_at, g.last_refreshed_at"
)


@router.get("/grants", response_model=ConnectedAppList)
async def list_connected_apps(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    owner: Annotated[
        Literal["me", "all"],
        Query(description="`me` (default): your connected apps. `all`: everyone's (owner/admin)."),
    ] = "me",
) -> ConnectedAppList:
    user_id = _require_user(ctx)
    if owner == "all" and not ctx.satisfies("admin"):
        raise ProblemDetail(
            403, ErrorCode.FORBIDDEN_ROLE, "Listing everyone's apps requires admin or above"
        )
    # Active means: not revoked, and still holding a refresh token that can be used.
    sql = (
        f"SELECT {_GRANT_COLS} FROM oauth_grants g JOIN users u ON u.id = g.user_id "
        "JOIN oauth_clients c ON c.client_id = g.client_id "
        "WHERE g.revoked_at IS NULL AND c.revoked_at IS NULL AND EXISTS ("
        "  SELECT 1 FROM oauth_refresh_tokens r WHERE r.grant_id = g.id AND r.revoked_at IS NULL "
        "  AND r.rotated_at IS NULL AND r.expires_at > now())"
    )
    params: dict[str, Any] = {}
    if owner == "me":
        sql += " AND g.user_id = :u"
        params["u"] = str(user_id)
    sql += " ORDER BY g.created_at DESC LIMIT 200"
    rows = (await session.execute(text(sql), params)).mappings().all()
    tokens = (
        (
            await session.execute(
                text(
                    "SELECT id, oauth_grant_id FROM api_tokens "
                    "WHERE oauth_grant_id = ANY(CAST(:g AS uuid[]))"
                ),
                {"g": [str(r["id"]) for r in rows]},
            )
        )
        .mappings()
        .all()
        if rows
        else []
    )
    seen = await _last_used(ctx.org_id, [t["id"] for t in tokens])
    last: dict[uuid.UUID, datetime] = {}
    for t in tokens:
        when = seen.get(t["id"])
        if when and (t["oauth_grant_id"] not in last or when > last[t["oauth_grant_id"]]):
            last[t["oauth_grant_id"]] = when
    items = []
    for r in rows:
        uri = str(r["redirect_uris"][0])
        items.append(
            ConnectedApp(
                id=r["id"],
                user_id=r["user_id"],
                owner_email=r["owner_email"] if owner == "all" else None,
                client_id=r["client_id"],
                client_name=r["client_name"],
                redirect_host=core.redirect_host(uri),
                verified_redirect=core.is_trusted_redirect(uri),
                scope=r["scope"],
                connected_at=r["created_at"],
                last_refreshed_at=r["last_refreshed_at"],
                last_used_at=last.get(r["id"]),
            )
        )
    return ConnectedAppList(items=items)


@router.delete(
    "/grants/{grant_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response
)
async def revoke_connected_app(
    grant_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    user_id = _require_user(ctx)
    row = (
        (
            await session.execute(
                text("SELECT user_id, client_id FROM oauth_grants WHERE id = :id FOR UPDATE"),
                {"id": str(grant_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None or (row["user_id"] != user_id and not ctx.satisfies("admin")):
        raise not_found()
    reason = "user" if row["user_id"] == user_id else "admin"
    if await revoke_grant(session, grant_id, reason=reason, by=user_id):
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=user_id,
            action=ACTION_OAUTH_GRANT_REVOKED,
            target_type="oauth_grant",
            target_id=grant_id,
            metadata={
                "reason": reason,
                "client_id": row["client_id"],
                "owner_id": str(row["user_id"]),
            },
            ip=ctx.ip,
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
