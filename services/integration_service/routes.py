"""integration-service public API: list, connect, disconnect, and the OAuth completion.

- `GET /integrations` (owner/admin): one item per connectable provider (the R8 set, then the
  Phase 4 reach providers HubSpot, Notion and Confluence), never token material.
- `POST /integrations/{provider}/connect` (owner/admin) -> {authorize_url}. 404 for a provider
  outside the connectable set (Linear is not built), 409 `PROVIDER_NOT_CONFIGURED` when the
  server has no app credentials for it, 409 `PM_ALREADY_CONNECTED` when another PM tool is
  connected.
- `DELETE /integrations/{provider}` (owner/admin) -> 204: provider-side cleanup and revoke where
  the provider has one (best effort), tokens and cursors deleted, status `disconnected`, audited.
- `POST /oauth/{provider}` {code, state} (owner/admin, JWT) -> {provider, status}: the canon path,
  completing the flow. The provider redirects the browser to the web app page
  `{APP_BASE_URL}/settings/integrations/callback/{provider}`, and that page, signed in, posts the
  code and state here.

Why the completion takes the JWT. A public GET callback authenticated only by `state` lets an
attacker start a connect in their own tenant and get a victim's browser to finish it, attaching
the victim's Slack, Jira or Google account to the attacker's tenant (login CSRF in reverse). Now
the state is honoured only for the signed-in user it was minted for, in the tenant it was minted
in, and only while that user is still owner/admin; the verifier is popped only after those
checks pass, so a mismatched attempt cannot burn the real user's state either.

Errors are RFC 7807 with a `reason` extension the web app maps to copy: invalid_state,
state_expired, exchange_failed (400 VALIDATION_FAILED); not_configured (409
PROVIDER_NOT_CONFIGURED); pm_already_connected (409 PM_ALREADY_CONNECTED);
account_connected_elsewhere (409 ACCOUNT_CONNECTED_ELSEWHERE); post_install_failed (502
DEPENDENCY_UNAVAILABLE; the connection is kept as `degraded`).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from redis.asyncio import from_url as redis_from_url
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_INTEGRATION_CONNECTED,
    ACTION_INTEGRATION_DISCONNECTED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import ProviderError, get_clients
from services.integration_service.connectors import calendar, decision_export, hubspot, jira, slack
from services.integration_service.oauth_state import (
    STATE_TTL_SECONDS,
    StateError,
    challenge_for,
    mint_state,
    new_verifier,
    state_key,
    verify_state,
)
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["integrations"])


def get_redis() -> Redis[bytes]:
    """A short-lived client per use (connect and callback are rare); a cached one would be
    bound to whichever event loop first used it."""
    return redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )


class IntegrationItem(BaseModel):
    provider: str
    configured: bool
    status: str | None
    scopes: list[str]
    external_account_id: str | None
    installed_by: uuid.UUID | None
    installed_at: datetime | None
    last_synced_at: datetime | None
    decision_checks: Literal["on", "needs_reconnect"] | None = Field(
        default=None,
        description=(
            "Slack only: whether messages in channels the app is in are checked against "
            "decisions. `needs_reconnect` for an install from before the history scopes."
        ),
    )


class IntegrationList(BaseModel):
    items: list[IntegrationItem]


class AuthorizeUrl(BaseModel):
    authorize_url: str


def _known(provider: str) -> str:
    if provider not in config.CONNECTABLE_PROVIDERS:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return provider


@router.get("/integrations", response_model=IntegrationList)
async def list_integrations(
    _ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> IntegrationList:
    connections = await store.list_connections(session)
    items = []
    for provider in config.CONNECTABLE_PROVIDERS:
        conn = connections.get(provider)
        items.append(
            IntegrationItem(
                provider=provider,
                configured=config.is_configured(provider),
                status=conn.status if conn else None,
                scopes=conn.scopes if conn else [],
                external_account_id=conn.external_account_id if conn else None,
                installed_by=conn.installed_by if conn else None,
                installed_at=conn.created_at if conn else None,
                last_synced_at=conn.last_synced_at if conn else None,
                decision_checks=(
                    slack.decision_checks_state(conn) if provider == config.SLACK else None
                ),
            )
        )
    return IntegrationList(items=items)


async def _other_pm_connected(session: AsyncSession, provider: str) -> bool:
    if provider not in config.PM_PROVIDERS:
        return False
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM integration_connections WHERE provider = ANY(:p) "
                "AND provider <> :me AND status <> 'disconnected' LIMIT 1"
            ),
            {"p": sorted(config.PM_PROVIDERS), "me": provider},
        )
    ).first()
    return row is not None


@router.post("/integrations/{provider}/connect", response_model=AuthorizeUrl)
async def connect(
    provider: str,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AuthorizeUrl:
    _known(provider)
    if not config.is_configured(provider):
        raise ProblemDetail(
            409, ErrorCode.PROVIDER_NOT_CONFIGURED, f"{provider} is not configured on this server"
        )
    if await _other_pm_connected(session, provider):
        raise ProblemDetail(
            409,
            ErrorCode.PM_ALREADY_CONNECTED,
            "Another project-management tool is connected. Disconnect it first.",
        )
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this account")
    state = mint_state(config.state_secret(), ctx.org_id, ctx.user_id, provider)
    verifier = new_verifier()
    async with get_redis() as redis:
        await redis.set(
            state_key(ctx.org_id, state),
            json.dumps({"verifier": verifier, "provider": provider, "user_id": str(ctx.user_id)}),
            ex=STATE_TTL_SECONDS,
        )
    url = (
        get_clients()
        .oauth[provider]
        .authorize_url(
            state=state,
            code_challenge=challenge_for(verifier),
            redirect_uri=config.redirect_uri(provider),
        )
    )
    return AuthorizeUrl(authorize_url=url)


@router.delete("/integrations/{provider}", status_code=204, response_class=Response)
async def disconnect(
    provider: str,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    _known(provider)
    conn = await store.get_connection(session, provider, for_update=True)
    if conn is None:
        raise not_found()
    if conn.status != "disconnected":
        await _provider_cleanup(session, conn)
        await store.delete_tokens(session, conn.id)
        await store.delete_cursors(session, conn.id)
        await store.set_status(session, conn.id, "disconnected")
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_INTEGRATION_DISCONNECTED,
            target_type="integration_connection",
            target_id=conn.id,
            metadata={"provider": provider},
            ip=ctx.ip,
        )
    return Response(status_code=204)


async def _provider_cleanup(session: AsyncSession, conn: store.Connection) -> None:
    """Stop the push channel / delete the webhooks, then revoke. Best effort: a provider that
    is down must not keep a tenant from disconnecting."""
    clients = get_clients()
    try:
        tokens = await store.load_tokens(session, conn)
    except Exception as exc:  # no tokens, or undecryptable: nothing to revoke with
        logger.warning("disconnect %s: no usable token (%s)", conn.provider, type(exc).__name__)
        return
    try:
        if conn.provider == config.GOOGLE_CALENDAR:
            await calendar.stop(session, conn, clients, tokens.access_token)
        elif conn.provider == config.JIRA:
            await jira.remove_webhooks(session, conn, clients, tokens.access_token)
    except ProviderError as exc:
        logger.warning("disconnect %s: provider cleanup failed: %s", conn.provider, exc)
    try:
        await clients.oauth[conn.provider].revoke(tokens.refresh_token or tokens.access_token)
    except ProviderError as exc:
        logger.warning("disconnect %s: revoke failed: %s", conn.provider, exc)


# --- the OAuth completion --------------------------------------------------------------------


class OAuthCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1, max_length=4096)
    state: str = Field(min_length=1, max_length=2048)


class OAuthResult(BaseModel):
    provider: str
    status: str


def _fail(status: int, code: ErrorCode, reason: str, detail: str) -> ProblemDetail:
    return ProblemDetail(status, code, detail, extra={"reason": reason})


def _bad_state(reason: str = "invalid_state") -> ProblemDetail:
    return _fail(400, ErrorCode.VALIDATION_FAILED, reason, "The authorization state is not valid")


async def _still_admin(session: AsyncSession, user_id: uuid.UUID) -> bool:
    tier = (
        await session.execute(
            text(
                "SELECT g.tier FROM users u JOIN rbac_grants g "
                "ON g.user_id = u.id AND g.org_id = u.org_id "
                "WHERE u.id = :u AND u.deleted_at IS NULL"
            ),
            {"u": str(user_id)},
        )
    ).scalar()
    return tier in ("admin", "owner")


_ACCOUNT_INDEX = "uq_integration_connections_account"


@router.post("/oauth/{provider}", response_model=OAuthResult)
async def oauth_complete(
    provider: str,
    body: OAuthCompletion,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> OAuthResult:
    _known(provider)
    try:
        parsed = verify_state(config.state_secret(), body.state)
    except StateError as exc:
        raise _bad_state(exc.reason) from None
    # Bound to the browser's signed-in user: same tenant, same user, same provider.
    if (
        parsed.provider != provider
        or parsed.org_id != ctx.org_id
        or ctx.user_id is None
        or parsed.user_id != ctx.user_id
    ):
        raise _bad_state()
    async with get_redis() as redis:
        stored = await redis.getdel(state_key(parsed.org_id, body.state))
    if stored is None:
        raise _bad_state()  # expired or already used
    try:
        pending: dict[str, Any] = json.loads(stored)
    except ValueError:
        raise _bad_state() from None
    if pending.get("provider") != provider or pending.get("user_id") != str(parsed.user_id):
        raise _bad_state()
    if not config.is_configured(provider):
        raise _fail(409, ErrorCode.PROVIDER_NOT_CONFIGURED, "not_configured", "Not configured")

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, ctx.org_id)
        if not await _still_admin(session, ctx.user_id):
            raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Owner or admin required")
        if await _other_pm_connected(session, provider):
            raise _fail(
                409,
                ErrorCode.PM_ALREADY_CONNECTED,
                "pm_already_connected",
                "Another project-management tool is connected",
            )

    clients = get_clients()
    try:
        grant = await clients.oauth[provider].exchange(
            code=body.code,
            code_verifier=str(pending.get("verifier", "")),
            redirect_uri=config.redirect_uri(provider),
        )
    except ProviderError as exc:
        logger.warning("oauth exchange failed for %s: %s", provider, exc)
        raise _fail(
            400, ErrorCode.VALIDATION_FAILED, "exchange_failed", "The provider refused the code"
        ) from None

    try:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, ctx.org_id)
            conn = await store.upsert_connection(
                session,
                org_id=ctx.org_id,
                provider=provider,
                external_account_id=grant.external_account_id,
                scopes=grant.scopes or list(clients.oauth[provider].scopes),
                installed_by=ctx.user_id,
            )
            await store.store_tokens(session, conn, grant)
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_INTEGRATION_CONNECTED,
                target_type="integration_connection",
                target_id=conn.id,
                metadata={"provider": provider, "scopes": conn.scopes},
                ip=ctx.ip,
            )
    except IntegrityError as exc:
        if _ACCOUNT_INDEX not in str(exc.orig):
            raise
        # The account belongs to another tenant's live connection. Nothing is written, and the
        # grant is NOT revoked: for Slack it is the same bot install the other tenant uses.
        raise _fail(
            409,
            ErrorCode.ACCOUNT_CONNECTED_ELSEWHERE,
            "account_connected_elsewhere",
            f"This {provider} account is already connected to another workspace",
        ) from None

    try:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, ctx.org_id)
            if provider == config.GOOGLE_CALENDAR:
                await calendar.post_install(session, conn, clients)
            elif provider == config.SLACK:
                await slack.post_install(session, conn, clients)
            elif provider == config.JIRA:
                await jira.post_install(session, conn, clients, grant.extra)
            elif provider == config.HUBSPOT:
                await hubspot.post_install(session, conn, clients)
            elif provider == config.NOTION:
                await decision_export.notion_post_install(session, conn, clients)
            elif provider == config.CONFLUENCE:
                await decision_export.confluence_post_install(session, conn, clients, grant.extra)
    except (ProviderError, store.NotConnected, ProblemDetail) as exc:
        logger.warning("post-install failed for %s: %s", provider, exc)
        async with sessionmaker() as session, session.begin():
            await set_rls(session, ctx.org_id)
            await store.set_status(session, conn.id, "degraded")
        raise _fail(
            502,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "post_install_failed",
            "Connected, but finishing the setup with the provider failed",
        ) from None
    return OAuthResult(provider=provider, status="active")
