"""The unauthenticated OAuth endpoints (MCP authorization, 2025-06-18). All public by protocol:
a client calls them before anyone has signed in. What authenticates each one instead:

- `GET /.well-known/oauth-authorization-server` (RFC 8414): static metadata, no data.
- `POST /oauth/register` (RFC 7591): anyone may register a PUBLIC client (no secret is ever
  issued); it can do nothing until a signed-in person approves it on the consent page. Rate
  limited per IP; redirect URIs must be https or a localhost loopback.
- `GET /oauth/authorize`: validates the request and sends the browser to the web app's consent
  page, where the person signs in with Clerk. Nothing is granted here.
- `POST /oauth/token`: the credential is the authorization code plus its PKCE verifier, or a
  refresh token. Both carry their tenant, so lookups run under RLS.
- `POST /oauth/revoke` (RFC 7009): possession of the token is the authority to revoke it.

Every error is the OAuth `{error, error_description}` shape, never problem+json, because OAuth
clients branch on `error` (Claude treats only `invalid_grant` as "this refresh token is dead").
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import parse_qsl, unquote

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from redis.asyncio import from_url as redis_from_url
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import oauth_config, pat
from libs.shared.audit import (
    ACTION_OAUTH_GRANT_REVOKED,
    ACTION_OAUTH_TOKEN_ISSUED,
    ACTION_OAUTH_TOKEN_REFRESHED,
    ACTION_OAUTH_TOKEN_REVOKED,
    audit,
)
from libs.shared.authz import _client_ip
from libs.shared.db import get_sessionmaker
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.oauth import core

logger = logging.getLogger(__name__)

router = APIRouter(tags=["oauth"])

DEFAULT_REGISTER_RATE_PER_HOUR = 30


# --- models (documented in the spec; bodies are parsed by hand so errors stay RFC-shaped) -------


class AuthorizationServerMetadata(BaseModel):
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    registration_endpoint: str
    revocation_endpoint: str
    scopes_supported: list[str]
    response_types_supported: list[str]
    response_modes_supported: list[str]
    grant_types_supported: list[str]
    token_endpoint_auth_methods_supported: list[str]
    revocation_endpoint_auth_methods_supported: list[str]
    code_challenge_methods_supported: list[str]
    authorization_response_iss_parameter_supported: bool
    service_documentation: str


class ClientRegistrationRequest(BaseModel):
    """RFC 7591 client metadata. Fields not listed are accepted and ignored."""

    model_config = ConfigDict(extra="ignore")

    redirect_uris: list[str] = Field(min_length=1, max_length=10)
    client_name: str | None = Field(default=None, max_length=500)
    grant_types: list[str] | None = None
    response_types: list[str] | None = None
    token_endpoint_auth_method: str | None = None
    scope: str | None = Field(default=None, max_length=500)
    client_uri: str | None = Field(default=None, max_length=2000)
    software_id: str | None = Field(default=None, max_length=200)
    software_version: str | None = Field(default=None, max_length=100)


class ClientInformation(BaseModel):
    """RFC 7591 section 3.2.1. A public client: no `client_secret`, ever."""

    client_id: str
    client_id_issued_at: int
    client_name: str
    redirect_uris: list[str]
    grant_types: list[str]
    response_types: list[str]
    token_endpoint_auth_method: Literal["none"]
    scope: str
    client_uri: str | None = None
    software_id: str | None = None
    software_version: str | None = None


class TokenResponse(BaseModel):
    access_token: str = Field(description="A `mytheus_pat_` bearer bound to the MCP server.")
    token_type: Literal["Bearer"]
    expires_in: int
    refresh_token: str = Field(description="Single use; every refresh returns a new one.")
    scope: Literal["read", "read_write"]


class OAuthErrorBody(BaseModel):
    error: str
    error_description: str | None = None


_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"model": OAuthErrorBody, "description": "OAuth error"}
}


# --- metadata ------------------------------------------------------------------------------------


@router.get(
    "/.well-known/oauth-authorization-server",
    response_model=AuthorizationServerMetadata,
    summary="OAuth authorization server metadata (RFC 8414)",
)
async def authorization_server_metadata() -> JSONResponse:
    # authz: public - discovery metadata, read by MCP clients before any sign-in.
    iss = oauth_config.issuer()
    body = AuthorizationServerMetadata(
        issuer=iss,
        authorization_endpoint=f"{iss}/oauth/authorize",
        token_endpoint=f"{iss}/oauth/token",
        registration_endpoint=f"{iss}/oauth/register",
        revocation_endpoint=f"{iss}/oauth/revoke",
        scopes_supported=[*oauth_config.SCOPES, oauth_config.OFFLINE_ACCESS],
        response_types_supported=["code"],
        response_modes_supported=["query"],
        grant_types_supported=["authorization_code", "refresh_token"],
        token_endpoint_auth_methods_supported=["none"],
        revocation_endpoint_auth_methods_supported=["none"],
        code_challenge_methods_supported=["S256"],
        authorization_response_iss_parameter_supported=True,
        service_documentation=f"{oauth_config.app_base_url()}/settings/api",
    )
    return JSONResponse(body.model_dump(), headers={"Cache-Control": "public, max-age=300"})


# --- dynamic client registration -----------------------------------------------------------------


def register_rate_limit() -> int:
    try:
        return max(1, int(os.environ.get("OAUTH_REGISTER_RATE_LIMIT_PER_HOUR", "")))
    except ValueError:
        return DEFAULT_REGISTER_RATE_PER_HOUR


async def _register_count(ip: str | None) -> int:
    """Registrations from this IP in the current hour, including this one. A fixed window in
    Redis, shared by every replica. Keyed `nexus:oauth:...`, not `nexus:{org_id}:...`:
    registration has no tenant (the same deliberate exception as `oauth_clients`)."""
    window = int(time.time() // 3600)
    key = f"nexus:oauth:ratelimit:register:{ip or 'unknown'}:{window}"
    async with redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True
    ) as redis:
        pipe = redis.pipeline(transaction=False)
        pipe.incr(key)
        pipe.expire(key, 7200)
        count, _ = await pipe.execute()
    return int(count)


def _clean_name(raw: str | None) -> str:
    name = "".join(ch for ch in (raw or "") if ch.isprintable()).strip()
    return name[:120] if name else "Unnamed app"


@router.post(
    "/oauth/register",
    status_code=201,
    response_model=ClientInformation,
    summary="Dynamic Client Registration (RFC 7591)",
    responses={
        **_ERRORS,
        429: {"model": OAuthErrorBody, "description": "Too many registrations from this address"},
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {"schema": ClientRegistrationRequest.model_json_schema()}
            },
        }
    },
)
async def register_client(request: Request) -> Response:
    # authz: public - RFC 7591 open registration of a PUBLIC client; it can do nothing until a
    # signed-in person approves it. Rate limited per IP.
    ip = _client_ip(request)
    limit = register_rate_limit()
    try:
        used = await _register_count(ip)
    except (RedisError, OSError):
        logger.exception("oauth: registration rate-limit store unavailable")
        return core.oauth_error(
            "temporarily_unavailable", "Registration is unavailable; retry shortly", status=503
        )
    if used > limit:
        return core.oauth_error(
            "too_many_requests",
            f"At most {limit} registrations per hour from one address",
            status=429,
            headers={"Retry-After": str(3600 - int(time.time() % 3600))},
        )
    try:
        raw = json.loads(await request.body())
        meta = ClientRegistrationRequest.model_validate(raw)
    except (ValueError, ValidationError) as exc:
        detail = "Body must be a JSON object with redirect_uris (1 to 10 URLs)"
        if isinstance(exc, ValidationError) and exc.errors():
            first = exc.errors()[0]
            detail = f"{'.'.join(str(p) for p in first['loc'])}: {first['msg']}"
        return core.oauth_error("invalid_client_metadata", detail)

    for uri in meta.redirect_uris:
        problem = core.redirect_uri_problem(uri)
        if problem:
            return core.oauth_error("invalid_redirect_uri", problem)

    # RFC 7591 lets the server replace requested values. Only public clients exist here: a
    # client asking for `client_secret_basic`/`_post` is registered as `none` (PKCE) and told
    # so in the response, rather than refused, so a connector that defaults to a secret still
    # connects.
    grant_types = [
        g
        for g in (meta.grant_types or ["authorization_code", "refresh_token"])
        if g in ("authorization_code", "refresh_token")
    ]
    if "authorization_code" not in grant_types:
        return core.oauth_error(
            "invalid_client_metadata", "grant_types must include authorization_code"
        )
    if "refresh_token" not in grant_types:
        grant_types.append("refresh_token")
    if meta.response_types and "code" not in meta.response_types:
        return core.oauth_error("invalid_client_metadata", "response_types must include code")
    try:
        core.effective_scope(meta.scope)
    except core.ScopeError as exc:
        return core.oauth_error("invalid_client_metadata", str(exc))
    client_uri = meta.client_uri if (meta.client_uri or "").startswith("https://") else None

    client_id = core.new_client_id()
    name = _clean_name(meta.client_name)
    scope = " ".join(oauth_config.SCOPES)
    async with get_sessionmaker()() as session, session.begin():
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO oauth_clients (client_id, client_name, redirect_uris, "
                        "grant_types, response_types, scope, client_uri, software_id, "
                        "software_version, registered_ip) VALUES (:id, :n, :r, :g, :rt, :s, "
                        ":cu, :sid, :sv, CAST(:ip AS inet)) RETURNING created_at"
                    ),
                    {
                        "id": client_id,
                        "n": name,
                        "r": meta.redirect_uris,
                        "g": grant_types,
                        "rt": ["code"],
                        "s": scope,
                        "cu": client_uri,
                        "sid": meta.software_id,
                        "sv": meta.software_version,
                        "ip": ip,
                    },
                )
            )
            .mappings()
            .one()
        )
    logger.info(
        "oauth.client_registered client_id=%s name=%r redirect_hosts=%s ip=%s",
        client_id,
        name,
        sorted({core.redirect_host(u) for u in meta.redirect_uris}),
        ip,
    )
    body = ClientInformation(
        client_id=client_id,
        client_id_issued_at=int(row["created_at"].timestamp()),
        client_name=name,
        redirect_uris=meta.redirect_uris,
        grant_types=grant_types,
        response_types=["code"],
        token_endpoint_auth_method="none",
        scope=scope,
        client_uri=client_uri,
        software_id=meta.software_id,
        software_version=meta.software_version,
    )
    return JSONResponse(body.model_dump(exclude_none=True), status_code=201, headers=core.NO_STORE)


# --- authorize -----------------------------------------------------------------------------------


def _consent_error(error: str, description: str) -> RedirectResponse:
    """A request we cannot trust the redirect of (unknown client, unregistered redirect URI)
    is NEVER bounced to that redirect (RFC 6749 section 4.1.2.1, MCP open-redirect rule): the
    browser lands on the web app's consent page, which explains it."""
    url = core.with_params(
        oauth_config.consent_url(), {"error": error, "error_description": description}
    )
    return RedirectResponse(url, status_code=302, headers=core.NO_STORE)


def _client_error(redirect_uri: str, error: str, description: str, state: str | None) -> Response:
    url = core.with_params(
        redirect_uri,
        {
            "error": error,
            "error_description": description,
            "state": state,
            "iss": oauth_config.issuer(),
        },
    )
    return RedirectResponse(url, status_code=302, headers=core.NO_STORE)


@router.get(
    "/oauth/authorize",
    summary="Start an authorization (OAuth 2.1 code flow with PKCE)",
    response_class=RedirectResponse,
    status_code=302,
    responses={302: {"description": "To the consent page, or back to the client with an error"}},
)
async def authorize(
    request: Request,
    response_type: str | None = None,
    client_id: str | None = None,
    redirect_uri: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str | None = None,
    state: str | None = None,
    scope: str | None = None,
    resource: str | None = None,
) -> Response:
    # authz: public - validates the request and hands the browser to the consent page, where
    # the person signs in; nothing is granted here.
    async with get_sessionmaker()() as session:
        client = await core.load_client(session, client_id)
    if client is None:
        return _consent_error("invalid_client", "This app is not registered with Mytheus")
    registered: list[str] = list(client["redirect_uris"])
    if redirect_uri is None and len(registered) == 1:
        redirect_uri = registered[0]
    if not redirect_uri or not core.redirect_matches(registered, redirect_uri):
        return _consent_error(
            "invalid_redirect_uri", "The return address does not match what the app registered"
        )
    if not core.valid_state(state):
        return _client_error(redirect_uri, "invalid_request", "state is malformed", None)
    if response_type != "code":
        return _client_error(
            redirect_uri, "unsupported_response_type", "Only response_type=code", state
        )
    if code_challenge_method != "S256" or not core.valid_challenge(code_challenge):
        return _client_error(
            redirect_uri,
            "invalid_request",
            "PKCE is required: code_challenge with code_challenge_method=S256",
            state,
        )
    try:
        granted = core.effective_scope(scope)
    except core.ScopeError as exc:
        return _client_error(redirect_uri, "invalid_scope", str(exc), state)
    if resource is not None and not core.resource_ok(resource):
        return _client_error(
            redirect_uri,
            "invalid_target",
            f"Tokens are issued only for {oauth_config.mcp_resource()}",
            state,
        )
    try:
        signed = core.sign_request(
            core.new_request(
                client_id=str(client["client_id"]),
                redirect_uri=redirect_uri,
                code_challenge=str(code_challenge),
                scope=granted,
                resource=oauth_config.mcp_resource(),
                state=state,
            )
        )
    except core.SigningNotConfigured:
        logger.error(
            "oauth: no request signing key (OAUTH_REQUEST_SIGNING_KEY/GATEWAY_SIGNING_KEY)"
        )
        return _client_error(
            redirect_uri, "temporarily_unavailable", "Sign-in is not configured", state
        )
    return RedirectResponse(
        core.with_params(oauth_config.consent_url(), {"request": signed}),
        status_code=302,
        headers=core.NO_STORE,
    )


# --- token ---------------------------------------------------------------------------------------


async def _form(request: Request) -> dict[str, str]:
    """`application/x-www-form-urlencoded` per RFC 6749 section 4.1.3 (what Claude sends). A
    JSON object is tolerated too, for clients that get this wrong."""
    body = (await request.body()).decode("utf-8", "replace")
    if "json" in request.headers.get("content-type", "").lower():
        try:
            data = json.loads(body)
        except ValueError:
            return {}
        return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
    return dict(parse_qsl(body, keep_blank_values=False))


def _basic_client_id(request: Request) -> str | None:
    """A client that sends `client_secret_basic` with an empty secret is still identified."""
    scheme, _, value = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "basic" or not value:
        return None
    try:
        decoded = base64.b64decode(value.strip()).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return None
    return unquote(decoded.partition(":")[0]) or None


async def _tenant_session(session: AsyncSession, org_claim: uuid.UUID | None) -> uuid.UUID | None:
    if org_claim is None:
        return None
    org_id = await resolve_org_by_id(session, org_claim)
    if org_id is not None:
        await set_rls(session, org_id)
    return org_id


async def _user_active(session: AsyncSession, user_id: uuid.UUID) -> bool:
    row = await session.execute(
        text("SELECT 1 FROM users WHERE id = :u AND deleted_at IS NULL"), {"u": str(user_id)}
    )
    return row.first() is not None


async def revoke_grant(
    session: AsyncSession, grant_id: uuid.UUID, *, reason: str, by: uuid.UUID | None = None
) -> bool:
    """Kill one connection: the grant, every refresh token in its family, every access token.
    True if it was active. The caller's session already has `app.current_org` set."""
    revoked = (
        await session.execute(
            text(
                "UPDATE oauth_grants SET revoked_at = now(), revoked_reason = :r, revoked_by = :b "
                "WHERE id = :g AND revoked_at IS NULL RETURNING id"
            ),
            {"g": str(grant_id), "r": reason, "b": str(by) if by else None},
        )
    ).first()
    await session.execute(
        text(
            "UPDATE oauth_refresh_tokens SET revoked_at = now() "
            "WHERE grant_id = :g AND revoked_at IS NULL"
        ),
        {"g": str(grant_id)},
    )
    await session.execute(
        text(
            "UPDATE api_tokens SET revoked_at = now(), revoked_by = :b "
            "WHERE oauth_grant_id = :g AND revoked_at IS NULL"
        ),
        {"g": str(grant_id), "b": str(by) if by else None},
    )
    return revoked is not None


async def _issue_pair(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    client: dict[str, Any],
    grant_id: uuid.UUID,
    scope: str,
    parent_id: uuid.UUID | None,
) -> tuple[TokenResponse, uuid.UUID]:
    """Mint an access token (an `api_tokens` row of kind oauth) and the next refresh token."""
    ttl = core.access_ttl_seconds()
    access, digest, hint = pat.generate(org_id)
    now = datetime.now(UTC)
    token_id = (
        await session.execute(
            text(
                "INSERT INTO api_tokens (org_id, user_id, name, token_hash, token_hint, scope, "
                "expires_at, kind, oauth_client_id, oauth_grant_id) VALUES (:o, :u, :n, :h, "
                ":hint, :s, :e, 'oauth', :c, :g) RETURNING id"
            ),
            {
                "o": str(org_id),
                "u": str(user_id),
                "n": str(client["client_name"])[:80],
                "h": digest,
                "hint": hint,
                "s": scope,
                "e": now + timedelta(seconds=ttl),
                "c": client["client_id"],
                "g": str(grant_id),
            },
        )
    ).scalar_one()
    refresh, refresh_hash = core.mint(core.REFRESH_PREFIX, org_id)
    await session.execute(
        text(
            "INSERT INTO oauth_refresh_tokens (org_id, grant_id, token_hash, parent_id, "
            "expires_at) VALUES (:o, :g, :h, :p, :e)"
        ),
        {
            "o": str(org_id),
            "g": str(grant_id),
            "h": refresh_hash,
            "p": str(parent_id) if parent_id else None,
            "e": now + timedelta(days=core.refresh_ttl_days()),
        },
    )
    body = TokenResponse(
        access_token=access,
        token_type="Bearer",
        expires_in=ttl,
        refresh_token=refresh,
        scope=scope,  # type: ignore[arg-type]
    )
    return body, token_id


def _token_ok(body: TokenResponse) -> JSONResponse:
    return JSONResponse(body.model_dump(), headers=core.NO_STORE)


async def _authorization_code(form: dict[str, str], client_id: str, ip: str | None) -> Response:
    code = form.get("code")
    org_claim = core.org_of(core.CODE_PREFIX, code)
    async with get_sessionmaker()() as session, session.begin():
        client = await core.load_client(session, client_id)
        if client is None:
            return core.oauth_error("invalid_client", "Unknown or revoked client", status=401)
        org_id = await _tenant_session(session, org_claim)
        if org_id is None or code is None:
            return core.oauth_error("invalid_grant", "The authorization code is not valid")
        row = (
            (
                await session.execute(
                    text(
                        "SELECT id, user_id, client_id, redirect_uri, code_challenge, scope, "
                        "resource, expires_at, consumed_at, grant_id "
                        "FROM oauth_authorization_codes WHERE code_hash = :h FOR UPDATE"
                    ),
                    {"h": core.hash_secret(code)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return core.oauth_error("invalid_grant", "The authorization code is not valid")
        if row["consumed_at"] is not None:
            # RFC 6749 section 4.1.2: a code used twice may mean it was stolen; revoke what the
            # first use produced. Committed (the block exits normally), then refused.
            if row["grant_id"] is not None and await revoke_grant(
                session, row["grant_id"], reason="code_reuse"
            ):
                await audit(
                    session,
                    org_id=org_id,
                    actor_id=row["user_id"],
                    action=ACTION_OAUTH_GRANT_REVOKED,
                    target_type="oauth_grant",
                    target_id=row["grant_id"],
                    metadata={"reason": "code_reuse", "client_id": client_id},
                    ip=ip,
                )
            return core.oauth_error("invalid_grant", "The authorization code was already used")
        if row["expires_at"] <= datetime.now(UTC):
            return core.oauth_error("invalid_grant", "The authorization code has expired")
        if row["client_id"] != client_id:
            return core.oauth_error("invalid_grant", "The code was issued to another client")
        if form.get("redirect_uri") != row["redirect_uri"]:
            return core.oauth_error("invalid_grant", "redirect_uri does not match the request")
        if not core.pkce_ok(form.get("code_verifier"), str(row["code_challenge"])):
            return core.oauth_error("invalid_grant", "PKCE verification failed")
        resource = form.get("resource")
        if resource is not None and not core.resource_ok(resource):
            return core.oauth_error(
                "invalid_target", f"Tokens are issued only for {oauth_config.mcp_resource()}"
            )
        if not await _user_active(session, row["user_id"]):
            return core.oauth_error("invalid_grant", "The account is no longer active")
        grant_id = (
            await session.execute(
                text(
                    "INSERT INTO oauth_grants (org_id, user_id, client_id, scope, resource) "
                    "VALUES (:o, :u, :c, :s, :r) RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "u": str(row["user_id"]),
                    "c": client_id,
                    "s": row["scope"],
                    "r": row["resource"],
                },
            )
        ).scalar_one()
        await session.execute(
            text(
                "UPDATE oauth_authorization_codes SET consumed_at = now(), grant_id = :g "
                "WHERE id = :id"
            ),
            {"g": str(grant_id), "id": str(row["id"])},
        )
        body, token_id = await _issue_pair(
            session,
            org_id=org_id,
            user_id=row["user_id"],
            client=client,
            grant_id=grant_id,
            scope=str(row["scope"]),
            parent_id=None,
        )
        await audit(
            session,
            org_id=org_id,
            actor_id=row["user_id"],
            action=ACTION_OAUTH_TOKEN_ISSUED,
            target_type="oauth_grant",
            target_id=grant_id,
            metadata={
                "client_id": client_id,
                "client_name": client["client_name"],
                "scope": row["scope"],
                "access_token_id": str(token_id),
            },
            ip=ip,
        )
    return _token_ok(body)


async def _refresh_token(form: dict[str, str], client_id: str, ip: str | None) -> Response:
    presented = form.get("refresh_token")
    org_claim = core.org_of(core.REFRESH_PREFIX, presented)
    async with get_sessionmaker()() as session, session.begin():
        client = await core.load_client(session, client_id)
        if client is None:
            return core.oauth_error("invalid_client", "Unknown or revoked client", status=401)
        org_id = await _tenant_session(session, org_claim)
        if org_id is None or presented is None:
            return core.oauth_error("invalid_grant", "The refresh token is not valid")
        row = (
            (
                await session.execute(
                    text(
                        "SELECT r.id, r.grant_id, r.expires_at, r.rotated_at, r.revoked_at, "
                        "g.user_id, g.client_id, g.scope, g.resource, "
                        "g.revoked_at AS grant_revoked_at "
                        "FROM oauth_refresh_tokens r JOIN oauth_grants g ON g.id = r.grant_id "
                        "WHERE r.token_hash = :h FOR UPDATE OF r, g"
                    ),
                    {"h": core.hash_secret(presented)},
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["grant_revoked_at"] is not None or row["revoked_at"] is not None:
            return core.oauth_error("invalid_grant", "The refresh token is not valid")
        if row["client_id"] != client_id:
            return core.oauth_error("invalid_grant", "The refresh token belongs to another client")
        if row["rotated_at"] is not None:
            # Reuse of a rotated token (OAuth 2.1 section 4.3.1): either the client or an
            # attacker holds a stale copy, and the server cannot tell which, so the whole family
            # dies. Committed, then refused; the person reconnects.
            await revoke_grant(session, row["grant_id"], reason="refresh_reuse")
            await audit(
                session,
                org_id=org_id,
                actor_id=row["user_id"],
                action=ACTION_OAUTH_GRANT_REVOKED,
                target_type="oauth_grant",
                target_id=row["grant_id"],
                metadata={"reason": "refresh_reuse", "client_id": client_id},
                ip=ip,
            )
            return core.oauth_error("invalid_grant", "The refresh token was already used")
        if row["expires_at"] <= datetime.now(UTC):
            return core.oauth_error("invalid_grant", "The refresh token has expired")
        resource = form.get("resource")
        if resource is not None and not core.resource_ok(resource):
            return core.oauth_error(
                "invalid_target", f"Tokens are issued only for {oauth_config.mcp_resource()}"
            )
        scope = str(row["scope"])
        if form.get("scope"):
            try:
                asked = core.effective_scope(form["scope"])
            except core.ScopeError as exc:
                return core.oauth_error("invalid_scope", str(exc))
            if asked == "read_write" and scope != "read_write":
                return core.oauth_error("invalid_scope", "Cannot widen the granted scope")
            scope = asked
        if not await _user_active(session, row["user_id"]):
            return core.oauth_error("invalid_grant", "The account is no longer active")

        await session.execute(
            text("UPDATE oauth_refresh_tokens SET rotated_at = now() WHERE id = :id"),
            {"id": str(row["id"])},
        )
        # The previous access tokens of this connection end now: the client has a new one.
        # Rows long expired are deleted, so an hourly refresh does not grow the table forever.
        await session.execute(
            text(
                "UPDATE api_tokens SET revoked_at = now() "
                "WHERE oauth_grant_id = :g AND revoked_at IS NULL"
            ),
            {"g": str(row["grant_id"])},
        )
        await session.execute(
            text(
                "DELETE FROM api_tokens WHERE oauth_grant_id = :g "
                "AND expires_at < now() - interval '1 day'"
            ),
            {"g": str(row["grant_id"])},
        )
        await session.execute(
            text(
                "DELETE FROM oauth_refresh_tokens WHERE grant_id = :g "
                "AND expires_at < now() - interval '1 day'"
            ),
            {"g": str(row["grant_id"])},
        )
        await session.execute(
            text("UPDATE oauth_grants SET last_refreshed_at = now() WHERE id = :g"),
            {"g": str(row["grant_id"])},
        )
        body, token_id = await _issue_pair(
            session,
            org_id=org_id,
            user_id=row["user_id"],
            client=client,
            grant_id=row["grant_id"],
            scope=scope,
            parent_id=row["id"],
        )
        await audit(
            session,
            org_id=org_id,
            actor_id=row["user_id"],
            action=ACTION_OAUTH_TOKEN_REFRESHED,
            target_type="oauth_grant",
            target_id=row["grant_id"],
            metadata={"client_id": client_id, "scope": scope, "access_token_id": str(token_id)},
            ip=ip,
        )
    return _token_ok(body)


_FORM_DOC = {
    "requestBody": {
        "required": True,
        "content": {
            "application/x-www-form-urlencoded": {
                "schema": {
                    "type": "object",
                    "required": ["grant_type", "client_id"],
                    "properties": {
                        "grant_type": {
                            "type": "string",
                            "enum": ["authorization_code", "refresh_token"],
                        },
                        "client_id": {"type": "string"},
                        "code": {"type": "string"},
                        "redirect_uri": {"type": "string"},
                        "code_verifier": {"type": "string"},
                        "refresh_token": {"type": "string"},
                        "scope": {"type": "string"},
                        "resource": {"type": "string"},
                    },
                }
            }
        },
    }
}


@router.post(
    "/oauth/token",
    response_model=TokenResponse,
    summary="Token endpoint: redeem a code (PKCE) or rotate a refresh token",
    responses={
        **_ERRORS,
        401: {"model": OAuthErrorBody, "description": "Unknown or revoked client"},
    },
    openapi_extra=_FORM_DOC,
)
async def token(request: Request) -> Response:
    # authz: public - the credential is the code plus its PKCE verifier, or a refresh token.
    form = await _form(request)
    client_id = form.get("client_id") or _basic_client_id(request)
    if not client_id:
        return core.oauth_error("invalid_client", "client_id is required", status=401)
    grant_type = form.get("grant_type")
    ip = _client_ip(request)
    if grant_type == "authorization_code":
        return await _authorization_code(form, client_id, ip)
    if grant_type == "refresh_token":
        return await _refresh_token(form, client_id, ip)
    return core.oauth_error(
        "unsupported_grant_type", "grant_type must be authorization_code or refresh_token"
    )


# --- revoke --------------------------------------------------------------------------------------


@router.post(
    "/oauth/revoke",
    status_code=200,
    summary="Revoke a token (RFC 7009)",
    response_class=Response,
    responses={200: {"description": "Revoked, or the token was already unusable"}},
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/x-www-form-urlencoded": {
                    "schema": {
                        "type": "object",
                        "required": ["token"],
                        "properties": {
                            "token": {"type": "string"},
                            "token_type_hint": {
                                "type": "string",
                                "enum": ["access_token", "refresh_token"],
                            },
                            "client_id": {"type": "string"},
                        },
                    }
                }
            },
        }
    },
)
async def revoke(request: Request) -> Response:
    # authz: public - RFC 7009: holding the token is the authority to revoke it.
    form = await _form(request)
    presented = form.get("token") or ""
    client_id = form.get("client_id") or _basic_client_id(request)
    ip = _client_ip(request)
    done = Response(status_code=200, headers=core.NO_STORE)
    if presented.startswith(core.REFRESH_PREFIX):
        org_claim = core.org_of(core.REFRESH_PREFIX, presented)
        async with get_sessionmaker()() as session, session.begin():
            org_id = await _tenant_session(session, org_claim)
            if org_id is None:
                return done
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT r.grant_id, g.user_id, g.client_id FROM oauth_refresh_tokens r "
                            "JOIN oauth_grants g ON g.id = r.grant_id WHERE r.token_hash = :h"
                        ),
                        {"h": core.hash_secret(presented)},
                    )
                )
                .mappings()
                .first()
            )
            if row is None or (client_id and row["client_id"] != client_id):
                return done
            if await revoke_grant(session, row["grant_id"], reason="client"):
                await audit(
                    session,
                    org_id=org_id,
                    actor_id=row["user_id"],
                    action=ACTION_OAUTH_TOKEN_REVOKED,
                    target_type="oauth_grant",
                    target_id=row["grant_id"],
                    metadata={"token": "refresh_token", "client_id": row["client_id"]},
                    ip=ip,
                )
        return done
    if pat.is_pat(presented):
        org_claim = pat.org_of(presented)
        async with get_sessionmaker()() as session, session.begin():
            org_id = await _tenant_session(session, org_claim)
            if org_id is None:
                return done
            row = (
                (
                    await session.execute(
                        text(
                            "UPDATE api_tokens SET revoked_at = now() WHERE token_hash = :h "
                            "AND kind = 'oauth' AND revoked_at IS NULL "
                            "AND (CAST(:c AS text) IS NULL OR oauth_client_id = :c) "
                            "RETURNING id, user_id, oauth_client_id, oauth_grant_id"
                        ),
                        {"h": pat.hash_token(presented), "c": client_id},
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                await audit(
                    session,
                    org_id=org_id,
                    actor_id=row["user_id"],
                    action=ACTION_OAUTH_TOKEN_REVOKED,
                    target_type="api_token",
                    target_id=row["id"],
                    metadata={
                        "token": "access_token",
                        "client_id": row["oauth_client_id"],
                        "grant_id": str(row["oauth_grant_id"]),
                    },
                    ip=ip,
                )
    return done
