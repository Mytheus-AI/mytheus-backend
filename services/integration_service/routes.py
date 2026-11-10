"""integration-service public API: list, connect, disconnect, and the OAuth callback.

- `GET /integrations` (owner/admin): one item per R8 provider, never token material.
- `POST /integrations/{provider}/connect` (owner/admin) -> {authorize_url}. 404 for a provider
  outside the R8 set (Linear is not built), 409 `PROVIDER_NOT_CONFIGURED` when the server has no
  app credentials for it, 409 `PM_ALREADY_CONNECTED` when another PM tool is connected.
- `DELETE /integrations/{provider}` (owner/admin) -> 204: provider-side cleanup and revoke where
  the provider has one (best effort), tokens and cursors deleted, status `disconnected`, audited.
- `GET /oauth/{provider}`: the canon callback. Public at the gateway and here (no Clerk JWT; it
  is a browser redirect from the provider), authenticated by the signed `state` and the
  single-use PKCE verifier. Always answers with a redirect to
  `{APP_BASE_URL}/settings/integrations?connected={provider}` or `?error={reason}`, reason one of
  access_denied, invalid_state, state_expired, exchange_failed, not_configured,
  pm_already_connected, post_install_failed.

RBAC is checked on connect (the `bff`/gateway path) AND re-checked on the callback against the
user the state names, so a demoted admin's half-finished install does not complete.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.asyncio import from_url as redis_from_url
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_INTEGRATION_CONNECTED,
    ACTION_INTEGRATION_DISCONNECTED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import ProviderError, get_clients
from services.integration_service.connectors import calendar, jira, slack
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


class IntegrationList(BaseModel):
    items: list[IntegrationItem]


class AuthorizeUrl(BaseModel):
    authorize_url: str


def _known(provider: str) -> str:
    if provider not in config.R8_PROVIDERS:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return provider


@router.get("/integrations", response_model=IntegrationList)
async def list_integrations(
    _ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> IntegrationList:
    connections = await store.list_connections(session)
    items = []
    for provider in config.R8_PROVIDERS:
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


# --- the callback ----------------------------------------------------------------------------


def _back(provider: str, *, error: str | None = None) -> RedirectResponse:
    query = {"error": error} if error else {"connected": provider}
    return RedirectResponse(
        f"{config.app_base_url()}/settings/integrations?{urlencode(query)}", status_code=302
    )


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


# authz: public - authenticated by the signed state + single-use PKCE verifier, not a JWT.
@router.get("/oauth/{provider}", status_code=302, response_class=RedirectResponse)
async def oauth_callback(
    provider: str,
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
) -> RedirectResponse:
    _known(provider)
    try:
        parsed = verify_state(config.state_secret(), state or "")
    except StateError as exc:
        return _back(provider, error=exc.reason)
    if parsed.provider != provider:
        return _back(provider, error="invalid_state")
    async with get_redis() as redis:
        stored = await redis.getdel(state_key(parsed.org_id, state or ""))
    if stored is None:
        return _back(provider, error="invalid_state")  # expired or already used
    try:
        pending: dict[str, Any] = json.loads(stored)
    except ValueError:
        return _back(provider, error="invalid_state")
    if pending.get("provider") != provider or pending.get("user_id") != str(parsed.user_id):
        return _back(provider, error="invalid_state")
    if error or not code:
        return _back(provider, error="access_denied")
    if not config.is_configured(provider):
        return _back(provider, error="not_configured")

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        if await resolve_org_by_id(session, parsed.org_id) is None:
            return _back(provider, error="invalid_state")
        await set_rls(session, parsed.org_id)
        if not await _still_admin(session, parsed.user_id):
            return _back(provider, error="access_denied")
        if await _other_pm_connected(session, provider):
            return _back(provider, error="pm_already_connected")

    clients = get_clients()
    try:
        grant = await clients.oauth[provider].exchange(
            code=code,
            code_verifier=str(pending.get("verifier", "")),
            redirect_uri=config.redirect_uri(provider),
        )
    except ProviderError as exc:
        logger.warning("oauth exchange failed for %s: %s", provider, exc)
        return _back(provider, error="exchange_failed")

    async with sessionmaker() as session, session.begin():
        await set_rls(session, parsed.org_id)
        conn = await store.upsert_connection(
            session,
            org_id=parsed.org_id,
            provider=provider,
            external_account_id=grant.external_account_id,
            scopes=grant.scopes or list(clients.oauth[provider].scopes),
            installed_by=parsed.user_id,
        )
        await store.store_tokens(session, conn, grant)
        await audit(
            session,
            org_id=parsed.org_id,
            actor_id=parsed.user_id,
            action=ACTION_INTEGRATION_CONNECTED,
            target_type="integration_connection",
            target_id=conn.id,
            metadata={"provider": provider, "scopes": conn.scopes},
        )

    try:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, parsed.org_id)
            if provider == config.GOOGLE_CALENDAR:
                await calendar.post_install(session, conn, clients)
            elif provider == config.SLACK:
                await slack.post_install(session, conn, clients)
            elif provider == config.JIRA:
                await jira.post_install(session, conn, clients, grant.extra)
    except (ProviderError, store.NotConnected, ProblemDetail) as exc:
        logger.warning("post-install failed for %s: %s", provider, exc)
        async with sessionmaker() as session, session.begin():
            await set_rls(session, parsed.org_id)
            await store.set_status(session, conn.id, "degraded")
        return _back(provider, error="post_install_failed")
    return _back(provider)
