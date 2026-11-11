"""OAuth for the MCP server (Claude custom connectors), end to end through the gateway.

One in-process stack: the real api-gateway (edge auth, routing, proxy) forwarding to the real
identity-tenant-service app (the authorization server) and the real bff app (the MCP server,
whose tools call the real core-api app). Real Postgres as `mytheus_app` (RLS on), real Redis.
Only Clerk is faked: a session token `session-<clerk user>` stands for a signed-in person, at
the gateway edge and at identity's `verify_token`.

1. Discovery: protected resource metadata (both forms), authorization server metadata, CORS,
   and the 401 from `/mcp` carrying `WWW-Authenticate: Bearer resource_metadata=...`.
2. The flow: register (DCR), authorize, consent (approve), token, call `/mcp` with the access
   token, refresh (rotation), revoke; Connected apps lists and disconnects.
3. Refusals: PKCE failure, redirect_uri mismatch (authorize and token), code reuse (revokes what
   the first use produced), refresh reuse (revokes the family), expired code, cross-tenant,
   revoked client, deny, expired or tampered consent request, audience (REST refuses the token),
   registration rules and its rate limit.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared import pat
from libs.shared.app_factory import create_app
from services.api_gateway import edge, proxy
from services.bff import mcp as mcp_mod
from services.bff import oauth_metadata
from services.core_api.routes import action_create, commitments, my_tasks, public_api
from services.identity_tenant_service.oauth import app_routes, core, public_routes
from services.identity_tenant_service.routes import api_tokens
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, seed_tenant
from tests.test_share_links import action, decision, meeting_with

CLAUDE_CALLBACK = "https://claude.ai/api/mcp/auth_callback"
RESOURCE = "https://api.mytheus.cloud/mcp"
CONSENT = "https://dashboard.mytheus.cloud/oauth/consent"

_bearer = HTTPBearer(auto_error=False)


def _session_claims(token: str) -> dict[str, Any] | None:
    if not token.startswith("session-"):
        return None
    clerk_user = token.removeprefix("session-")
    row = _sql(
        "SELECT o.clerk_org_id FROM users u JOIN organizations o ON o.id = u.org_id "
        "WHERE u.clerk_user_id = %(c)s",
        {"c": clerk_user},
    )
    return {"sub": clerk_user, "org_id": row[0]["clerk_org_id"]} if row else None


def _fake_edge_verify(credentials: HTTPAuthorizationCredentials) -> dict[str, Any]:
    claims = _session_claims(credentials.credentials)
    if claims is None:
        raise auth_mod.ProblemDetail(401, auth_mod.ErrorCode.AUTH_REQUIRED, "Invalid token")
    return claims


def _fake_service_verify(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),  # noqa: B008
) -> dict[str, Any]:
    if credentials is not None and pat.is_pat(credentials.credentials):
        return auth_mod.verify_token(credentials)
    claims = _session_claims(credentials.credentials) if credentials else None
    if claims is None:
        raise HTTPException(401)
    return claims


@pytest.fixture
def stack(migrated_db, monkeypatch):
    for name in ("API_PUBLIC_URL", "APP_BASE_URL", "OAUTH_REQUEST_SIGNING_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATABASE_URL", APP_URL)
    monkeypatch.setenv("GATEWAY_SIGNING_KEY", "test-gateway-signing-key")
    monkeypatch.setenv("CORE_API_URL", "http://core.test")
    monkeypatch.setenv("OAUTH_REGISTER_RATE_LIMIT_PER_HOUR", "100000")
    monkeypatch.setattr(edge, "verify_token", _fake_edge_verify)
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    identity = create_app("identity-oauth-test")
    identity.include_router(public_routes.router)
    identity.include_router(app_routes.router)
    identity.include_router(api_tokens.router)
    identity.dependency_overrides[auth_mod.verify_token] = _fake_service_verify

    core_app = create_app("core-api-oauth-test")
    for r in (public_api.router, my_tasks.router, commitments.router, action_create.router):
        core_app.include_router(r)
    bff = create_app("bff-oauth-test")
    bff.include_router(mcp_mod.router)
    bff.include_router(oauth_metadata.router)
    mcp_mod.set_core_transport(httpx.ASGITransport(app=core_app))

    gateway = create_app("api-gateway-oauth-test", idempotency=False, pat_auth=False)
    gateway.middleware("http")(edge.edge_auth)
    proxy.install(gateway)
    gateway.state.proxy_client = httpx.AsyncClient(
        mounts={
            "http://bff:8000": httpx.ASGITransport(app=bff),
            "http://identity-tenant-service:8000": httpx.ASGITransport(app=identity),
        },
        timeout=httpx.Timeout(30.0),
    )
    with TestClient(gateway, base_url="https://api.mytheus.cloud") as client:
        yield client
    mcp_mod.set_core_transport(None)
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


# --- helpers -------------------------------------------------------------------------------------


def session(t: Tenant) -> dict[str, str]:
    return {"Authorization": f"Bearer session-{t.clerk_user}"}


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def pkce() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(48)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    return verifier, challenge


def register(client: TestClient, redirect: str = CLAUDE_CALLBACK, **extra: Any) -> dict:
    r = client.post(
        "/oauth/register",
        json={
            "client_name": "Claude",
            "redirect_uris": [redirect],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            **extra,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


def authorize(
    client: TestClient,
    client_id: str,
    challenge: str,
    *,
    redirect: str = CLAUDE_CALLBACK,
    scope: str = "read read_write",
    state: str = "st-123",
    **extra: str,
) -> httpx.Response:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
        "scope": scope,
        "resource": RESOURCE,
        **extra,
    }
    return client.get("/oauth/authorize", params=params, follow_redirects=False)


def query_of(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def consent_request(client: TestClient, client_id: str, challenge: str, **kw: Any) -> str:
    r = authorize(client, client_id, challenge, **kw)
    assert r.status_code == 302, r.text
    location = r.headers["location"]
    assert location.startswith(CONSENT + "?")
    return query_of(location)["request"]


def approve(client: TestClient, t: Tenant, request: str, scope: str | None = None) -> dict:
    body: dict[str, Any] = {"request": request, "decision": "approve"}
    if scope:
        body["scope"] = scope
    r = client.post("/api/v1/oauth/consent", json=body, headers=session(t))
    assert r.status_code == 200, r.text
    return r.json()


def redeem(
    client: TestClient,
    client_id: str,
    code: str,
    verifier: str,
    redirect: str = CLAUDE_CALLBACK,
) -> httpx.Response:
    return client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect,
            "client_id": client_id,
            "code_verifier": verifier,
            "resource": RESOURCE,
        },
    )


def refresh(client: TestClient, client_id: str, token: str) -> httpx.Response:
    return client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": token,
            "client_id": client_id,
            "resource": RESOURCE,
        },
    )


def connect(
    client: TestClient, t: Tenant, *, scope: str | None = None, redirect: str = CLAUDE_CALLBACK
) -> tuple[str, dict]:
    """Register, authorize, approve and redeem: (client_id, token response)."""
    reg = register(client, redirect=redirect)
    verifier, challenge = pkce()
    req = consent_request(client, reg["client_id"], challenge, redirect=redirect)
    code = query_of(approve(client, t, req, scope=scope)["redirect_to"])["code"]
    r = redeem(client, reg["client_id"], code, verifier, redirect=redirect)
    assert r.status_code == 200, r.text
    return reg["client_id"], r.json()


def mcp(client: TestClient, token: str, method: str, params: dict | None = None) -> httpx.Response:
    return client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
        headers={**bearer(token), "MCP-Protocol-Version": "2025-06-18"},
    )


def audit_actions(t: Tenant) -> list[str]:
    rows = _sql(
        "SELECT action FROM audit_logs WHERE org_id = %(o)s AND action LIKE 'oauth.%%' "
        "ORDER BY occurred_at",
        {"o": str(t.org_id)},
    )
    return [r["action"] for r in rows]


# --- 1. discovery --------------------------------------------------------------------------------


def test_discovery_documents_are_public_and_agree(stack):
    for path in (
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
    ):
        r = stack.get(path)
        assert r.status_code == 200, r.text
        prm = r.json()
        assert prm["resource"] == RESOURCE
        assert prm["authorization_servers"] == ["https://api.mytheus.cloud"]
        assert prm["scopes_supported"] == ["read", "read_write"]
        assert r.headers["access-control-allow-origin"] == "*"

    r = stack.get("/.well-known/oauth-authorization-server")
    assert r.status_code == 200, r.text
    meta = r.json()
    assert meta["issuer"] == "https://api.mytheus.cloud"
    assert meta["authorization_endpoint"] == "https://api.mytheus.cloud/oauth/authorize"
    assert meta["token_endpoint"] == "https://api.mytheus.cloud/oauth/token"
    assert meta["registration_endpoint"] == "https://api.mytheus.cloud/oauth/register"
    assert meta["revocation_endpoint"] == "https://api.mytheus.cloud/oauth/revoke"
    assert meta["code_challenge_methods_supported"] == ["S256"]
    assert set(meta["grant_types_supported"]) == {"authorization_code", "refresh_token"}
    assert meta["token_endpoint_auth_methods_supported"] == ["none"]
    assert {"read", "read_write"} <= set(meta["scopes_supported"])

    pre = stack.options(
        "/oauth/token",
        headers={"Origin": "https://inspector.example", "Access-Control-Request-Method": "POST"},
    )
    assert pre.status_code == 204 and pre.headers["access-control-allow-origin"] == "*"


def test_public_urls_follow_env(stack, monkeypatch):
    monkeypatch.setenv("API_PUBLIC_URL", "https://api.staging.example/")
    prm = stack.get("/.well-known/oauth-protected-resource").json()
    assert prm["resource"] == "https://api.staging.example/mcp"
    assert prm["authorization_servers"] == ["https://api.staging.example"]


def test_unauthenticated_mcp_is_401_with_resource_metadata(stack):
    r = stack.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == (
        'Bearer resource_metadata="https://api.mytheus.cloud/.well-known/oauth-protected-resource"'
    )
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["code"] == "AUTH_REQUIRED"

    bad = stack.post("/mcp", json={}, headers=bearer(pat.generate(uuid.uuid4())[0]))
    assert bad.status_code == 401
    assert 'error="invalid_token"' in bad.headers["www-authenticate"]

    # Other routes keep their plain 401: the challenge describes the MCP resource only.
    assert "www-authenticate" not in stack.get("/api/v1/decisions").headers


# --- 2. the whole flow ---------------------------------------------------------------------------


def test_register_authorize_consent_token_mcp_refresh_revoke(stack):
    me = seed_tenant()
    m = meeting_with(me)
    decision(me, m, "Adopt the Atlas pricing")
    action(me, m, "Draft the Atlas FAQ")

    # Register. A client asking for a secret is registered public (PKCE) and told so.
    reg = register(stack, token_endpoint_auth_method="client_secret_post")
    assert reg["token_endpoint_auth_method"] == "none" and "client_secret" not in reg
    assert reg["redirect_uris"] == [CLAUDE_CALLBACK]
    client_id = reg["client_id"]
    assert _sql("SELECT 1 FROM oauth_clients WHERE client_id = %(c)s", {"c": client_id})

    # Authorize: straight to the consent page; nothing granted yet.
    verifier, challenge = pkce()
    req = consent_request(stack, client_id, challenge, scope="read read_write offline_access")

    # The consent page's read: who is asking, where it returns, what, which workspace.
    shown = stack.get("/api/v1/oauth/consent", params={"request": req}, headers=session(me))
    assert shown.status_code == 200, shown.text
    details = shown.json()
    assert details["client"]["name"] == "Claude"
    assert details["client"]["redirect_host"] == "claude.ai"
    assert details["client"]["verified_redirect"] is True
    assert details["scope"] == "read_write"
    assert details["resource"] == RESOURCE
    assert details["workspace"]["name"] == "Org"

    # Approve: a code for this person, bound to everything, back to Claude with state and iss.
    result = approve(stack, me, req)
    back = query_of(result["redirect_to"])
    assert result["redirect_to"].startswith(CLAUDE_CALLBACK + "?")
    assert back["state"] == "st-123" and back["iss"] == "https://api.mytheus.cloud"
    assert back["code"].startswith("mytheus_oac_" + me.org_id.hex)

    # Token: form-encoded, PKCE, resource.
    tok = redeem(stack, client_id, back["code"], verifier)
    assert tok.status_code == 200, tok.text
    assert tok.headers["cache-control"] == "no-store"
    issued = tok.json()
    assert issued["token_type"] == "Bearer" and issued["expires_in"] == 3600
    assert issued["scope"] == "read_write"
    assert issued["access_token"].startswith("mytheus_pat_" + me.org_id.hex)
    assert issued["refresh_token"].startswith("mytheus_ort_")
    row = _sql(
        "SELECT kind, oauth_client_id, scope, expires_at FROM api_tokens WHERE token_hash = %(h)s",
        {"h": pat.hash_token(issued["access_token"])},
    )[0]
    assert row["kind"] == "oauth" and row["oauth_client_id"] == client_id
    assert row["expires_at"] < datetime.now(UTC) + timedelta(minutes=61)
    assert not _sql(
        "SELECT 1 FROM oauth_refresh_tokens WHERE token_hash = %(h)s",
        {"h": issued["refresh_token"]},
    ), "refresh tokens are stored hashed"

    # Call the MCP server through the gateway with the access token.
    init = mcp(stack, issued["access_token"], "initialize", {"protocolVersion": "2025-06-18"})
    assert init.status_code == 200, init.text
    assert init.json()["result"]["serverInfo"]["name"] == "mytheus"
    found = mcp(
        stack,
        issued["access_token"],
        "tools/call",
        {"name": "search_meetings", "arguments": {"query": "atlas"}},
    ).json()["result"]
    assert not found["isError"]
    assert {h["kind"] for h in found["structuredContent"]["items"]} == {"decision", "action"}
    created = mcp(
        stack,
        issued["access_token"],
        "tools/call",
        {"name": "create_action", "arguments": {"content": "Send the Atlas contract"}},
    ).json()["result"]
    assert not created["isError"], created

    # Audience: the token is for /mcp only, never the REST API.
    rest = stack.get("/api/v1/decisions", headers=bearer(issued["access_token"]))
    assert rest.status_code == 403 and rest.json()["reason"] == "oauth_token_mcp_only"

    # Connected apps lists it; personal tokens do not.
    apps = stack.get("/api/v1/oauth/grants", headers=session(me)).json()["items"]
    assert [a["client_name"] for a in apps] == ["Claude"]
    assert apps[0]["scope"] == "read_write" and apps[0]["redirect_host"] == "claude.ai"
    assert apps[0]["last_used_at"] is not None
    personal = stack.get("/api/v1/api-tokens", headers=session(me)).json()["items"]
    assert personal == []

    # Refresh: rotation. The old access token ends, the new pair works.
    ref = refresh(stack, client_id, issued["refresh_token"])
    assert ref.status_code == 200, ref.text
    fresh = ref.json()
    assert fresh["refresh_token"] != issued["refresh_token"]
    assert mcp(stack, issued["access_token"], "ping").status_code == 401
    assert mcp(stack, fresh["access_token"], "ping").status_code == 200

    # Revoke (RFC 7009) the refresh token: the whole connection ends.
    rv = stack.post(
        "/oauth/revoke",
        data={
            "token": fresh["refresh_token"],
            "token_type_hint": "refresh_token",
            "client_id": client_id,
        },
    )
    assert rv.status_code == 200
    assert mcp(stack, fresh["access_token"], "ping").status_code == 401
    dead = refresh(stack, client_id, fresh["refresh_token"])
    assert dead.status_code == 400 and dead.json()["error"] == "invalid_grant"
    assert stack.get("/api/v1/oauth/grants", headers=session(me)).json()["items"] == []
    # Revoking again, or an unknown token, is still a 200.
    assert stack.post("/oauth/revoke", data={"token": "nonsense"}).status_code == 200

    assert audit_actions(me) == [
        "oauth.authorization_granted",
        "oauth.token_issued",
        "oauth.token_refreshed",
        "oauth.token_revoked",
    ]


def test_revoking_an_access_token_and_disconnecting_from_settings(stack):
    me = seed_tenant()
    client_id, issued = connect(stack, me)
    rv = stack.post("/oauth/revoke", data={"token": issued["access_token"]})
    assert rv.status_code == 200
    assert mcp(stack, issued["access_token"], "ping").status_code == 401
    # The refresh token still works: revoking an access token ends only that token.
    fresh = refresh(stack, client_id, issued["refresh_token"]).json()
    assert mcp(stack, fresh["access_token"], "ping").status_code == 200

    grant = stack.get("/api/v1/oauth/grants", headers=session(me)).json()["items"][0]
    gone = stack.delete(f"/api/v1/oauth/grants/{grant['id']}", headers=session(me))
    assert gone.status_code == 204
    assert mcp(stack, fresh["access_token"], "ping").status_code == 401
    assert refresh(stack, client_id, fresh["refresh_token"]).json()["error"] == "invalid_grant"
    assert "oauth.grant_revoked" in audit_actions(me)


def test_approving_read_only_narrows_the_scope(stack):
    me = seed_tenant()
    _, issued = connect(stack, me, scope="read")
    assert issued["scope"] == "read"
    created = mcp(
        stack,
        issued["access_token"],
        "tools/call",
        {"name": "create_action", "arguments": {"content": "Should not exist"}},
    ).json()["result"]
    assert created["isError"] and "read-only" in created["content"][0]["text"]


def test_consent_cannot_widen_a_read_request(stack):
    me = seed_tenant()
    reg = register(stack)
    _, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge, scope="read")
    r = stack.post(
        "/api/v1/oauth/consent",
        json={"request": req, "decision": "approve", "scope": "read_write"},
        headers=session(me),
    )
    assert r.status_code == 422 and r.json()["reason"] == "scope_exceeds_request"


def test_deny_returns_access_denied_and_is_audited(stack):
    me = seed_tenant()
    reg = register(stack)
    _, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    r = stack.post(
        "/api/v1/oauth/consent", json={"request": req, "decision": "deny"}, headers=session(me)
    )
    assert r.status_code == 200
    back = query_of(r.json()["redirect_to"])
    assert back["error"] == "access_denied" and back["state"] == "st-123"
    assert "code" not in back
    assert audit_actions(me) == ["oauth.authorization_denied"]


def test_loopback_redirects_match_on_any_port(stack):
    me = seed_tenant()
    reg = register(stack, redirect="http://localhost:3118/callback")
    verifier, challenge = pkce()
    other_port = "http://localhost:51234/callback"
    req = consent_request(stack, reg["client_id"], challenge, redirect=other_port)
    shown = stack.get("/api/v1/oauth/consent", params={"request": req}, headers=session(me))
    assert shown.json()["client"]["loopback_redirect"] is True
    assert shown.json()["client"]["verified_redirect"] is False
    code = query_of(approve(stack, me, req)["redirect_to"])["code"]
    assert redeem(stack, reg["client_id"], code, verifier, redirect=other_port).status_code == 200


# --- 3. refusals ---------------------------------------------------------------------------------


def test_pkce_failure(stack):
    me = seed_tenant()
    reg = register(stack)
    _, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    code = query_of(approve(stack, me, req)["redirect_to"])["code"]
    wrong, _ = pkce()
    r = redeem(stack, reg["client_id"], code, wrong)
    assert r.status_code == 400 and r.json()["error"] == "invalid_grant"
    missing = authorize(stack, reg["client_id"], challenge, code_challenge_method="plain")
    assert missing.status_code == 302
    assert query_of(missing.headers["location"])["error"] == "invalid_request"


def test_redirect_uri_mismatch(stack):
    me = seed_tenant()
    reg = register(stack)
    verifier, challenge = pkce()
    # Authorize with an unregistered redirect: never sent there; the consent page explains.
    r = authorize(stack, reg["client_id"], challenge, redirect="https://evil.example/cb")
    assert r.status_code == 302
    assert r.headers["location"].startswith(CONSENT + "?")
    assert query_of(r.headers["location"])["error"] == "invalid_redirect_uri"
    # Token with a different redirect than the code was issued for.
    req = consent_request(stack, reg["client_id"], challenge)
    code = query_of(approve(stack, me, req)["redirect_to"])["code"]
    bad = redeem(stack, reg["client_id"], code, verifier, redirect="https://claude.com/cb")
    assert bad.status_code == 400 and bad.json()["error"] == "invalid_grant"


def test_code_reuse_is_refused_and_revokes_what_the_first_use_produced(stack):
    me = seed_tenant()
    reg = register(stack)
    verifier, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    code = query_of(approve(stack, me, req)["redirect_to"])["code"]
    first = redeem(stack, reg["client_id"], code, verifier).json()
    again = redeem(stack, reg["client_id"], code, verifier)
    assert again.status_code == 400 and again.json()["error"] == "invalid_grant"
    assert mcp(stack, first["access_token"], "ping").status_code == 401
    assert refresh(stack, reg["client_id"], first["refresh_token"]).status_code == 400


def test_refresh_reuse_revokes_the_family(stack):
    me = seed_tenant()
    client_id, issued = connect(stack, me)
    rotated = refresh(stack, client_id, issued["refresh_token"]).json()
    reuse = refresh(stack, client_id, issued["refresh_token"])
    assert reuse.status_code == 400 and reuse.json()["error"] == "invalid_grant"
    # The legitimate-looking newer pair dies with it.
    assert mcp(stack, rotated["access_token"], "ping").status_code == 401
    assert refresh(stack, client_id, rotated["refresh_token"]).json()["error"] == "invalid_grant"
    reason = _sql(
        "SELECT revoked_reason FROM oauth_grants WHERE client_id = %(c)s", {"c": client_id}
    )[0]["revoked_reason"]
    assert reason == "refresh_reuse"


def test_expired_code(stack):
    me = seed_tenant()
    reg = register(stack)
    verifier, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    code = query_of(approve(stack, me, req)["redirect_to"])["code"]
    _sql(
        "UPDATE oauth_authorization_codes SET created_at = now() - interval '20 minutes', "
        "expires_at = now() - interval '10 minutes' WHERE code_hash = %(h)s",
        {"h": core.hash_secret(code)},
    )
    r = redeem(stack, reg["client_id"], code, verifier)
    assert r.status_code == 400 and r.json()["error"] == "invalid_grant"


def test_cross_tenant(stack):
    a, b = seed_tenant(), seed_tenant()
    reg = register(stack)
    verifier, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    code = query_of(approve(stack, a, req)["redirect_to"])["code"]
    # The same secret claiming tenant B finds nothing there.
    forged = code.replace(a.org_id.hex, b.org_id.hex)
    r = redeem(stack, reg["client_id"], forged, verifier)
    assert r.status_code == 400 and r.json()["error"] == "invalid_grant"
    issued = redeem(stack, reg["client_id"], code, verifier).json()
    forged_refresh = issued["refresh_token"].replace(a.org_id.hex, b.org_id.hex)
    assert refresh(stack, reg["client_id"], forged_refresh).json()["error"] == "invalid_grant"

    # B neither sees A's connection nor can disconnect it: 404, not 403.
    grant = stack.get("/api/v1/oauth/grants", headers=session(a)).json()["items"][0]
    assert stack.get("/api/v1/oauth/grants", headers=session(b)).json()["items"] == []
    assert (
        stack.delete(f"/api/v1/oauth/grants/{grant['id']}", headers=session(b)).status_code == 404
    )
    # A colleague in A's own workspace (a member) gets the same 404.
    colleague = _colleague(a)
    assert (
        stack.delete(f"/api/v1/oauth/grants/{grant['id']}", headers=session(colleague)).status_code
        == 404
    )
    # A's token calls /mcp as A: B's data is not reachable.
    assert mcp(stack, issued["access_token"], "ping").status_code == 200


def _colleague(t: Tenant) -> Tenant:
    user_id, clerk_user = uuid.uuid4(), f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s, %(o)s, %(c)s, "
        "'c@example.com')",
        {"u": str(user_id), "o": str(t.org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, 'member')",
        {"o": str(t.org_id), "u": str(user_id)},
    )
    return Tenant(t.org_id, t.clerk_org, user_id, clerk_user)


def test_revoked_client(stack):
    me = seed_tenant()
    client_id, issued = connect(stack, me)
    _sql("UPDATE oauth_clients SET revoked_at = now() WHERE client_id = %(c)s", {"c": client_id})
    # Its tokens stop at once, it cannot refresh, and it cannot start a new authorization.
    assert mcp(stack, issued["access_token"], "ping").status_code == 401
    r = refresh(stack, client_id, issued["refresh_token"])
    assert r.status_code == 401 and r.json()["error"] == "invalid_client"
    _, challenge = pkce()
    start = authorize(stack, client_id, challenge)
    assert start.headers["location"].startswith(CONSENT + "?")
    assert query_of(start.headers["location"])["error"] == "invalid_client"
    assert stack.get("/api/v1/oauth/grants", headers=session(me)).json()["items"] == []


def test_consent_request_expired_tampered_or_for_a_vanished_client(stack):
    me = seed_tenant()
    reg = register(stack)
    _, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    tampered = req[:-3] + ("aaa" if not req.endswith("aaa") else "bbb")
    r = stack.get("/api/v1/oauth/consent", params={"request": tampered}, headers=session(me))
    assert r.status_code == 400 and r.json()["reason"] == "invalid_request"

    stale = core.sign_request(
        core.AuthRequest(
            client_id=reg["client_id"],
            redirect_uri=CLAUDE_CALLBACK,
            code_challenge=challenge,
            scope="read",
            resource=RESOURCE,
            state=None,
            exp=int(datetime.now(UTC).timestamp()) - 1,
        )
    )
    r = stack.post(
        "/api/v1/oauth/consent", json={"request": stale, "decision": "approve"}, headers=session(me)
    )
    assert r.status_code == 400 and r.json()["reason"] == "expired_request"

    _sql(
        "UPDATE oauth_clients SET revoked_at = now() WHERE client_id = %(c)s",
        {"c": reg["client_id"]},
    )
    r = stack.get("/api/v1/oauth/consent", params={"request": req}, headers=session(me))
    assert r.status_code == 404 and r.json()["reason"] == "unknown_client"

    # Signed out: the consent backend needs a session (the page sends people through Clerk).
    assert stack.get("/api/v1/oauth/consent", params={"request": req}).status_code == 401


def test_a_token_cannot_approve_consent(stack):
    me = seed_tenant()
    _, issued = connect(stack, me)
    reg = register(stack)
    _, challenge = pkce()
    req = consent_request(stack, reg["client_id"], challenge)
    r = stack.post(
        "/api/v1/oauth/consent",
        json={"request": req, "decision": "approve"},
        headers=bearer(issued["access_token"]),
    )
    assert r.status_code == 403


def test_authorize_refuses_bad_scope_and_resource_back_to_the_client(stack):
    reg = register(stack)
    _, challenge = pkce()
    bad_scope = authorize(stack, reg["client_id"], challenge, scope="admin")
    assert query_of(bad_scope.headers["location"])["error"] == "invalid_scope"
    assert bad_scope.headers["location"].startswith(CLAUDE_CALLBACK)
    bad_target = authorize(stack, reg["client_id"], challenge, resource="https://other.example/mcp")
    assert query_of(bad_target.headers["location"])["error"] == "invalid_target"
    wrong_type = authorize(stack, reg["client_id"], challenge, response_type="token")
    assert query_of(wrong_type.headers["location"])["error"] == "unsupported_response_type"


def test_registration_rules(stack):
    def attempt(uris: list[str]) -> httpx.Response:
        return stack.post("/oauth/register", json={"client_name": "x", "redirect_uris": uris})

    assert attempt(["http://example.com/cb"]).json()["error"] == "invalid_redirect_uri"
    assert attempt(["cursor://anysphere/cb"]).json()["error"] == "invalid_redirect_uri"
    assert attempt(["https://claude.ai/cb#frag"]).json()["error"] == "invalid_redirect_uri"
    assert attempt([]).json()["error"] == "invalid_client_metadata"
    assert attempt(["http://127.0.0.1/callback"]).status_code == 201
    assert attempt(["http://[::1]:8080/callback"]).status_code == 201
    implicit = stack.post(
        "/oauth/register",
        json={"redirect_uris": [CLAUDE_CALLBACK], "grant_types": ["implicit"]},
    )
    assert implicit.json()["error"] == "invalid_client_metadata"
    unnamed = stack.post("/oauth/register", json={"redirect_uris": [CLAUDE_CALLBACK]})
    assert unnamed.json()["client_name"] == "Unnamed app"


def test_registration_is_rate_limited_per_ip(stack, monkeypatch):
    monkeypatch.setenv("OAUTH_REGISTER_RATE_LIMIT_PER_HOUR", "2")
    ip = f"10.{secrets.randbelow(250)}.{secrets.randbelow(250)}.{secrets.randbelow(250)}"
    body = {"client_name": "x", "redirect_uris": [CLAUDE_CALLBACK]}
    headers = {"X-Forwarded-For": ip}
    assert stack.post("/oauth/register", json=body, headers=headers).status_code == 201
    assert stack.post("/oauth/register", json=body, headers=headers).status_code == 201
    third = stack.post("/oauth/register", json=body, headers=headers)
    assert third.status_code == 429 and "retry-after" in third.headers
    other = {"X-Forwarded-For": "10.255.255.1"}
    assert stack.post("/oauth/register", json=body, headers=other).status_code in (201, 429)
    registered = _sql(
        "SELECT registered_ip::text AS ip FROM oauth_clients WHERE registered_ip = %(ip)s::inet",
        {"ip": ip},
    )
    assert len(registered) == 2


def test_token_endpoint_errors_are_rfc_shaped(stack):
    r = stack.post("/oauth/token", data={"grant_type": "password", "client_id": "x"})
    assert r.status_code == 400 and r.json()["error"] == "unsupported_grant_type"
    r = stack.post("/oauth/token", data={"grant_type": "authorization_code"})
    assert r.status_code == 401 and r.json()["error"] == "invalid_client"
    reg = register(stack)
    r = stack.post(
        "/oauth/token",
        data={"grant_type": "authorization_code", "client_id": reg["client_id"], "code": "nope"},
    )
    assert r.status_code == 400 and r.json()["error"] == "invalid_grant"


# --- units ---------------------------------------------------------------------------------------


def test_core_rules():
    assert core.redirect_uri_problem("https://claude.ai/api/mcp/auth_callback") is None
    assert core.redirect_uri_problem("http://localhost:3118/callback") is None
    assert core.redirect_uri_problem("http://claude.ai/cb") is not None
    assert core.redirect_uri_problem("https://user:pw@claude.ai/cb") is not None
    assert core.redirect_matches(["http://127.0.0.1/callback"], "http://127.0.0.1:9999/callback")
    assert not core.redirect_matches(["http://127.0.0.1/callback"], "http://127.0.0.1:9/other")
    assert not core.redirect_matches(["https://claude.ai/cb"], "https://claude.ai/cb2")
    assert core.effective_scope("read read_write") == "read_write"
    assert core.effective_scope("offline_access") == "read"
    assert core.effective_scope(None) == "read"
    with pytest.raises(core.ScopeError):
        core.effective_scope("read admin")
    assert core.resource_ok("https://API.mytheus.cloud/mcp/")
    assert not core.resource_ok("https://api.mytheus.cloud/")
    assert not core.resource_ok("https://api.mytheus.cloud/mcp#x")
    org = uuid.uuid4()
    code, digest = core.mint(core.CODE_PREFIX, org)
    assert core.org_of(core.CODE_PREFIX, code) == org and digest == core.hash_secret(code)
    assert core.org_of(core.REFRESH_PREFIX, code) is None
    assert core.with_params("https://x.example/cb?a=1", {"code": "c", "state": None}) == (
        "https://x.example/cb?a=1&code=c"
    )


def test_gateway_routes_and_public_paths():
    assert edge.route_for("/oauth/token") == "identity-tenant-service"
    assert edge.route_for("/oauth/register") == "identity-tenant-service"
    assert edge.route_for("/oauth/authorize") == "identity-tenant-service"
    assert edge.route_for("/oauth/revoke") == "identity-tenant-service"
    assert edge.route_for("/api/v1/oauth/grants") == "identity-tenant-service"
    assert edge.route_for("/.well-known/oauth-protected-resource/mcp") == "bff"
    assert edge.route_for("/.well-known/oauth-authorization-server") == "identity-tenant-service"
    # integration-service keeps its provider flows, which stay bearer-gated.
    assert edge.route_for("/oauth/google_calendar") == "integration-service"
    assert edge.route_for("/oauth") is None
    assert not edge.is_public("/oauth/google_calendar")
    assert not edge.is_public("/api/v1/oauth/consent")
    assert edge.is_public("/oauth/token") and edge.is_public(
        "/.well-known/oauth-authorization-server"
    )


def test_every_integration_provider_is_routed():
    """`/oauth` is spelled out per provider (it is shared with the authorization server), so a
    provider added to integration-service without a gateway entry would 404 at the edge."""
    from services.integration_service import config as integration_config

    providers = {
        integration_config.GOOGLE_CALENDAR,
        integration_config.SLACK,
        integration_config.JIRA,
        integration_config.LINEAR,
        *integration_config.CONNECTABLE_PROVIDERS,
        *integration_config.PM_PROVIDERS,
    }
    for provider in providers:
        assert edge.route_for(f"/oauth/{provider}") == "integration-service", provider
