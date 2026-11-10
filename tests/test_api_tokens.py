"""Personal access tokens (Phase 4): management routes, the service tier, and the gateway.

1. Management (identity `/api/v1/api-tokens`, session only): create returns the token once and
   stores only its hash; list shows a preview and state; a member cannot see or revoke someone
   else's token (404) while an admin can; 20 active tokens is the cap; RLS hides other tenants.
2. Service tier (`PatAuthMiddleware` + `verify_token` + `tenant_context`, NO auth override):
   a token resolves to its owner's context; revoked, expired, tampered (another tenant's id),
   unknown and deleted-user tokens are all the same 401; a read token cannot write (403); a
   token cannot reach anything outside the public API (403), including token management.
3. Gateway: forwards the signed headers for a token, refuses outside the public API, and rate
   limits per token with 429 + Retry-After.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import Request
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared import pat
from libs.shared.app_factory import create_app
from services.api_gateway import edge
from services.core_api.routes import decision_log, my_tasks
from services.identity_tenant_service.routes import api_tokens
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user


@pytest.fixture(scope="module")
def app_db(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ.setdefault("GATEWAY_SIGNING_KEY", "test-gateway-signing-key")
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def manage(app_db):
    _fresh_engine()
    """identity's token routes, signed in as PRINCIPAL (a session, not a token)."""
    app = _create_test_app("identity-api-tokens-test")
    app.include_router(api_tokens.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c


@pytest.fixture
def service(app_db):
    _fresh_engine()
    """A real service app: the factory's PatAuthMiddleware, no auth override at all."""
    app = create_app("core-api-pat-test")
    app.include_router(decision_log.router)
    app.include_router(my_tasks.router)
    app.include_router(api_tokens.router)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def gateway(app_db):
    _fresh_engine()
    app = create_app("api-gateway-pat-test", idempotency=False, pat_auth=False)
    app.middleware("http")(edge.edge_auth)

    @app.get("/api/v1/decisions")
    async def echo(request: Request) -> dict[str, str | None]:
        # authz: public - observes what the edge forwarded.
        return {
            "org": request.headers.get(edge.ORG_HEADER),
            "user": request.headers.get(edge.USER_HEADER),
            "sig": request.headers.get(edge.SIGNATURE_HEADER),
        }

    with TestClient(app) as c:
        yield c


def _fresh_engine() -> None:
    """Each TestClient runs its own event loop; a pooled engine is bound to the first one."""
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def mint(
    t: Tenant,
    *,
    scope: str = "read",
    expires: datetime | None = None,
    revoked: bool = False,
    org_override: uuid.UUID | None = None,
) -> str:
    token, digest, hint = pat.generate(org_override or t.org_id)
    _sql(
        "INSERT INTO api_tokens (org_id, user_id, name, token_hash, token_hint, scope, "
        "created_at, expires_at, revoked_at) VALUES (%(o)s, %(u)s, 'test', %(h)s, %(hint)s, "
        "%(s)s, %(c)s, %(e)s, CASE WHEN %(r)s THEN now() END)",
        {
            "o": str(t.org_id),
            "u": str(t.user_id),
            "h": digest,
            "hint": hint,
            "s": scope,
            "c": datetime.now(UTC) - timedelta(days=2),
            "e": expires or datetime.now(UTC) + timedelta(days=30),
            "r": revoked,
        },
    )
    return token


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def confirmed_decision(t: Tenant, meeting: uuid.UUID, content: str) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, 'confirmed') RETURNING id",
        {"o": str(t.org_id), "m": str(meeting), "c": content},
    )
    return uuid.UUID(str(row[0]["id"]))


# --- token format -----------------------------------------------------------------------------


def test_token_format_carries_the_tenant_and_only_the_hash_is_needed():
    org = uuid.uuid4()
    token, digest, hint = pat.generate(org)
    assert token.startswith("mytheus_pat_") and pat.org_of(token) == org
    assert digest == pat.hash_token(token) and len(digest) == 64 and hint == token[-4:]
    assert pat.org_of("mytheus_pat_nothex_abc") is None
    assert pat.generate(org)[0] != token


def test_scope_rules():
    assert pat.allows("GET", "/api/v1/meetings/x", "read") == "ok"
    assert pat.allows("POST", "/api/v1/actions", "read") == "scope"
    assert pat.allows("POST", "/api/v1/actions", "read_write") == "ok"
    assert pat.allows("POST", "/api/v1/ask", "read") == "ok"
    assert pat.allows("POST", "/api/rag/query", "read") == "not_public"
    assert pat.allows("POST", "/mcp", "read") == "ok"
    assert pat.allows("GET", "/api/v1/api-tokens", "read_write") == "not_public"
    assert pat.allows("GET", "/bff/dashboard", "read_write") == "not_public"
    assert pat.allows("GET", "/api/v1/meetingsx", "read") == "not_public"


# --- management -------------------------------------------------------------------------------


def test_create_shows_the_token_once_and_stores_only_its_hash(manage):
    me = seed_tenant()
    sign_in(me)
    res = manage.post(
        "/api/v1/api-tokens", json={"name": "Claude", "scope": "read", "expires_in_days": 30}
    )
    assert res.status_code == 201, res.text
    assert res.headers["cache-control"] == "no-store"
    body = res.json()
    token = body["token"]
    assert token.startswith(f"mytheus_pat_{me.org_id.hex}_")
    assert body["token_preview"] == f"mytheus_pat_...{token[-4:]}" and body["state"] == "active"
    [row] = _sql("SELECT token_hash FROM api_tokens WHERE id = %(i)s", {"i": body["id"]})
    assert row["token_hash"] == pat.hash_token(token)
    listed = manage.get("/api/v1/api-tokens").json()["items"]
    assert [i["id"] for i in listed] == [body["id"]]
    assert "token" not in listed[0]
    audits = _sql(
        "SELECT 1 FROM audit_logs WHERE action = 'api_token.created' AND target_id = %(t)s",
        {"t": body["id"]},
    )
    assert len(audits) == 1


def test_create_validation_and_cap(manage):
    me = seed_tenant()
    sign_in(me)
    assert manage.post("/api/v1/api-tokens", json={"name": " "}).status_code == 422
    bad_days = {"name": "x", "expires_in_days": 400}
    assert manage.post("/api/v1/api-tokens", json=bad_days).status_code == 422
    bad_scope = {"name": "x", "scope": "admin"}
    assert manage.post("/api/v1/api-tokens", json=bad_scope).status_code == 422
    for _ in range(api_tokens.MAX_ACTIVE_PER_USER):
        mint(me)
    res = manage.post("/api/v1/api-tokens", json={"name": "one too many"})
    assert res.status_code == 409 and res.json()["reason"] == "too_many_tokens"


def test_revoke_is_owner_or_admin_and_idempotent(manage):
    owner = seed_tenant()
    other = add_user(owner)
    admin = add_user(owner, tier="admin")
    sign_in(owner)
    token_id = manage.post("/api/v1/api-tokens", json={"name": "mine"}).json()["id"]
    sign_in(other)
    assert manage.delete(f"/api/v1/api-tokens/{token_id}").status_code == 404
    assert manage.get("/api/v1/api-tokens", params={"owner": "all"}).status_code == 403
    sign_in(admin)
    everyone = manage.get("/api/v1/api-tokens", params={"owner": "all"}).json()["items"]
    assert [i["id"] for i in everyone] == [token_id]
    assert everyone[0]["owner_email"] == "u@example.com"
    assert manage.delete(f"/api/v1/api-tokens/{token_id}").status_code == 204
    assert manage.delete(f"/api/v1/api-tokens/{token_id}").status_code == 204
    sign_in(owner)
    [item] = manage.get("/api/v1/api-tokens").json()["items"]
    assert item["state"] == "revoked"
    audits = _sql(
        "SELECT 1 FROM audit_logs WHERE action = 'api_token.revoked' AND target_id = %(t)s",
        {"t": token_id},
    )
    assert len(audits) == 1


def test_rls_hides_another_tenants_tokens(manage):
    a = seed_tenant()
    mint(a)
    b = seed_tenant()
    sign_in(b)
    assert manage.get("/api/v1/api-tokens").json()["items"] == []
    [row] = _sql("SELECT id FROM api_tokens WHERE org_id = %(o)s", {"o": str(a.org_id)})
    assert manage.delete(f"/api/v1/api-tokens/{row['id']}").status_code == 404


# --- service tier -----------------------------------------------------------------------------


def test_a_token_reads_as_its_owner(service):
    me = seed_tenant()
    m = seed_meeting(me.org_id)
    add_participant(m, me.org_id, me.user_id)
    decision = confirmed_decision(me, m, "Ship on Friday")
    other = seed_tenant()
    confirmed_decision(other, seed_meeting(other.org_id), "not yours")
    res = service.get("/api/v1/decisions", headers=bearer(mint(me)))
    assert res.status_code == 200, res.text
    assert [i["id"] for i in res.json()["items"]] == [str(decision)]


@pytest.mark.parametrize("variant", ["revoked", "expired", "tampered", "unknown", "garbage"])
def test_every_bad_token_is_the_same_401(service, variant):
    me = seed_tenant()
    other = seed_tenant()
    if variant == "revoked":
        token = mint(me, revoked=True)
    elif variant == "expired":
        token = mint(me, expires=datetime.now(UTC) - timedelta(minutes=1))
    elif variant == "tampered":
        # A valid token of tenant A, relabelled with tenant B's id: B has no such hash.
        token = mint(me).replace(me.org_id.hex, other.org_id.hex)
    elif variant == "unknown":
        token = pat.generate(me.org_id)[0]
    else:
        token = "mytheus_pat_not-a-token"
    res = service.get("/api/v1/decisions", headers=bearer(token))
    assert res.status_code == 401
    assert res.json()["code"] == "AUTH_REQUIRED" and res.json()["detail"] == "Invalid token"


def test_a_deleted_user_or_suspended_tenant_token_stops_working(service):
    me = seed_tenant()
    token = mint(me)
    assert service.get("/api/v1/decisions", headers=bearer(token)).status_code == 200
    _sql("UPDATE organizations SET status = 'suspended' WHERE id = %(o)s", {"o": str(me.org_id)})
    assert service.get("/api/v1/decisions", headers=bearer(token)).status_code == 401
    _sql("UPDATE organizations SET status = 'active' WHERE id = %(o)s", {"o": str(me.org_id)})
    _sql("UPDATE users SET deleted_at = now() WHERE id = %(u)s", {"u": str(me.user_id)})
    assert service.get("/api/v1/decisions", headers=bearer(token)).status_code == 401


def test_a_read_token_cannot_write_and_a_read_write_token_can(service):
    me = seed_tenant()
    m = seed_meeting(me.org_id)
    add_participant(m, me.org_id, me.user_id)
    row = _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, status, confidence) "
        "VALUES (%(o)s, %(m)s, 'do it', %(u)s, 'confirmed', 1.0) RETURNING id",
        {"o": str(me.org_id), "m": str(m), "u": str(me.user_id)},
    )
    path = f"/api/v1/actions/{row[0]['id']}/done"
    res = service.post(path, headers=bearer(mint(me, scope="read")))
    assert res.status_code == 403 and res.json()["reason"] == "read_only_token"
    res = service.post(path, headers=bearer(mint(me, scope="read_write")))
    assert res.status_code == 200, res.text


def test_a_token_cannot_reach_beyond_the_public_api(service):
    me = seed_tenant()
    token = mint(me, scope="read_write")
    for method, path in (("GET", "/api/v1/api-tokens"), ("POST", "/api/v1/api-tokens")):
        res = service.request(method, path, headers=bearer(token), json={"name": "x"})
        assert res.status_code == 403 and res.json()["reason"] == "not_public_api"


def test_verify_token_refuses_an_unresolved_token():
    from fastapi.security import HTTPAuthorizationCredentials

    from libs.shared.errors import ProblemDetail

    token = pat.generate(uuid.uuid4())[0]
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token))
    assert exc.value.status == 401
    with pytest.raises(ProblemDetail):
        auth_mod.clerk_org_id_from({"sub": "u", pat.PAT_CLAIM: {}})


# --- gateway ----------------------------------------------------------------------------------


def test_gateway_forwards_a_token_as_its_owner(gateway):
    me = seed_tenant()
    res = gateway.get("/api/v1/decisions", headers=bearer(mint(me)))
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["org"] == str(me.org_id) and body["user"] == me.clerk_user
    assert edge.verify_context(body["org"], body["user"], body["sig"])
    assert res.headers["x-ratelimit-limit"] == str(edge.pat_rate_limit())


def test_gateway_refuses_tokens_outside_the_public_api(gateway):
    me = seed_tenant()
    token = mint(me, scope="read_write")
    for path in ("/api/v1/api-tokens", "/admin/ops/tenants", "/bff/dashboard", "/orgs/self-serve"):
        res = gateway.post(path, headers=bearer(token))
        assert res.status_code == 403, path
    assert gateway.get("/api/v1/decisions", headers=bearer("mytheus_pat_x")).status_code == 401


def test_gateway_rate_limits_per_token(gateway, monkeypatch):
    monkeypatch.setenv(edge.PAT_RATE_ENV, "3")
    me = seed_tenant()
    token, spare = mint(me), mint(me)
    codes = [gateway.get("/api/v1/decisions", headers=bearer(token)).status_code for _ in range(4)]
    assert codes[:3] == [200, 200, 200]
    if codes[3] == 200:  # the minute rolled over mid-test; one more is enough to prove it
        codes.append(gateway.get("/api/v1/decisions", headers=bearer(token)).status_code)
    limited = gateway.get("/api/v1/decisions", headers=bearer(token))
    assert limited.status_code == 429 or 429 in codes
    if limited.status_code == 429:
        assert int(limited.headers["retry-after"]) >= 1
        assert limited.json()["code"] == "RATE_LIMITED"
    # Another token of the same person has its own budget.
    assert gateway.get("/api/v1/decisions", headers=bearer(spare)).status_code == 200


def test_gateway_lets_the_share_page_through_without_a_token(gateway):
    """`/api/v1/public/` is the share-link read: no bearer, the signed token is the credential.
    The edge passes it on (404 here only because this test app has no such route)."""
    res = gateway.get("/api/v1/public/shares/abc.def")
    assert res.status_code == 404 and res.json()["code"] != "AUTH_REQUIRED"
    assert edge.route_for("/api/v1/public/shares/x") == "core-api"
    assert edge.route_for("/mcp") == "bff" and edge.route_for("/api/v1/ask") == "bff"
    assert edge.route_for("/api/v1/api-tokens/1") == "identity-tenant-service"
    assert edge.route_for("/api/v1/search") == "core-api"
    assert edge.route_for("/api/v1/share-links/1") == "core-api"
