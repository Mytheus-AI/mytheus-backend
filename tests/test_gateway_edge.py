"""api-gateway edge: pre-validation, tenant resolution, header integrity, routing.

Plan section 3.8 acceptance checks 1, 2 and 5.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import jwt as pyjwt
import psycopg2
import psycopg2.extras
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.api_gateway import edge
from tests.conftest import APP_URL, BOOTSTRAP_URL

_ISSUER = "https://test.clerk.accounts.dev"
_SIGNING_KEY = "test-gateway-signing-key"


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return (
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ),
    )


class _FakeJWKS:
    def __init__(self, pub):
        self._pub = pub

    def get_signing_key_from_jwt(self, token):  # noqa: ARG002
        return type("K", (), {"key": self._pub})()


@pytest.fixture(scope="module")
def keys():
    return _keypair()


@pytest.fixture(scope="module")
def client(migrated_db, keys):
    """The gateway with a real edge middleware; only the JWKS fetch is faked."""
    import os

    priv, pub = keys
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ["CLERK_ISSUER"] = _ISSUER
    os.environ["GATEWAY_SIGNING_KEY"] = _SIGNING_KEY
    os.environ.pop("CLERK_JWT_ORG_CLAIM", None)
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    original = auth_mod._jwks_client
    auth_mod._jwks_client = lambda: _FakeJWKS(pub)  # type: ignore[assignment]

    app = create_app("api-gateway-test")
    app.middleware("http")(edge.edge_auth)

    @app.get("/api/v1/echo")
    async def echo(request: Request) -> dict[str, str | None]:
        # authz: public - the point of this route is to observe what the edge forwarded.
        # The annotation is load-bearing: FastAPI resolves `Request` by type, and an
        # unannotated parameter would be treated as a query parameter instead.
        return {
            "org": request.headers.get(edge.ORG_HEADER),
            "user": request.headers.get(edge.USER_HEADER),
            "sig": request.headers.get(edge.SIGNATURE_HEADER),
        }

    @app.post(edge.SELF_SERVE_ORG_PATH)
    async def self_serve_echo(request: Request) -> dict[str, str | None]:
        # authz: public - observes what the edge forwards on the tenant-exempt path.
        return {
            "org": request.headers.get(edge.ORG_HEADER),
            "user": request.headers.get(edge.USER_HEADER),
            "sig": request.headers.get(edge.SIGNATURE_HEADER),
        }

    @app.get("/admin/ops/whoami")
    async def ops_echo(request: Request) -> dict[str, str | None]:
        # authz: public - observes what the edge forwards for the superadmin console.
        return {
            "org": request.headers.get(edge.ORG_HEADER),
            "user": request.headers.get(edge.USER_HEADER),
            "sig": request.headers.get(edge.SIGNATURE_HEADER),
        }

    with TestClient(app) as c:
        yield c

    auth_mod._jwks_client = original  # type: ignore[assignment]
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _seed_org() -> tuple[uuid.UUID, str]:
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"gw-{org_id}", "cid": clerk_org},
    )
    return org_id, clerk_org


def _token(priv, clerk_org: str, sub: str = "user_abc", **overrides) -> str:
    now = int(time.time())
    claims = {
        "sub": sub,
        "org_id": clerk_org,
        "iss": _ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + 3600,
    }
    claims.update(overrides)
    return pyjwt.encode(claims, priv, algorithm="RS256")


# --- rejection at the edge --------------------------------------------------------------------


def test_no_token_is_401(client):
    """Plan 3.8 acceptance 1. Rejected at the edge; no service worker is touched."""
    resp = client.get("/api/v1/echo")
    assert resp.status_code == 401
    assert resp.json()["code"] == "AUTH_REQUIRED"
    assert resp.headers["content-type"].startswith("application/problem+json")


def test_expired_token_is_401(client, keys):
    priv, _pub = keys
    _org_id, clerk_org = _seed_org()
    now = int(time.time())
    token = _token(priv, clerk_org, exp=now - 10, iat=now - 3600, nbf=now - 3600)
    resp = client.get("/api/v1/echo", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401
    assert resp.json()["code"] == "TOKEN_EXPIRED"


def test_forged_token_is_401(client):
    """Signed with the wrong key: the JWKS public key must not verify it."""
    other_priv, _ = _keypair()
    _org_id, clerk_org = _seed_org()
    token = _token(other_priv, clerk_org)
    resp = client.get("/api/v1/echo", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


def test_a_token_for_an_unknown_organization_is_401(client, keys):
    priv, _pub = keys
    token = _token(priv, f"org_{uuid.uuid4().hex}")
    resp = client.get("/api/v1/echo", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Token carries no usable organization"


def test_a_suspended_organization_is_401(client, keys):
    priv, _pub = keys
    org_id, clerk_org = _seed_org()
    _sql("UPDATE organizations SET status='suspended' WHERE id=%(o)s", {"o": str(org_id)})
    resp = client.get(
        "/api/v1/echo", headers={"Authorization": f"Bearer {_token(priv, clerk_org)}"}
    )
    assert resp.status_code == 401


# --- resolution and header integrity -----------------------------------------------------------


def test_a_valid_token_resolves_and_is_forwarded_signed(client, keys):
    """Plan 3.8 acceptance 2, first half."""
    priv, _pub = keys
    org_id, clerk_org = _seed_org()

    body = client.get(
        "/api/v1/echo", headers={"Authorization": f"Bearer {_token(priv, clerk_org)}"}
    ).json()

    assert body["org"] == str(org_id), "the opaque Clerk id must be resolved to our org_id"
    assert body["user"] == "user_abc"
    assert edge.verify_context(body["org"], body["user"], body["sig"])


def test_an_inbound_internal_header_is_stripped(client, keys):
    """Plan 3.8 acceptance 2, second half, and the attack this design exists to stop.

    A client asserting its own `X-Mytheus-Org` must not survive the edge. If it did, anything
    downstream that trusted the header without checking the signature would be handing over
    another tenant's data on request.
    """
    priv, _pub = keys
    org_id, clerk_org = _seed_org()
    attacker_org = str(uuid.uuid4())

    body = client.get(
        "/api/v1/echo",
        headers={
            "Authorization": f"Bearer {_token(priv, clerk_org)}",
            edge.ORG_HEADER: attacker_org,
            edge.USER_HEADER: "user_attacker",
            edge.SIGNATURE_HEADER: "forged",
        },
    ).json()

    assert body["org"] == str(org_id) != attacker_org
    assert body["user"] == "user_abc"
    assert edge.verify_context(body["org"], body["user"], body["sig"])


def test_a_tampered_signature_does_not_verify(client, keys):
    """The signature covers org and user TOGETHER, so the pair cannot be mixed and matched."""
    priv, _pub = keys
    org_a, clerk_org_a = _seed_org()
    org_b, _clerk_org_b = _seed_org()

    import os

    os.environ["GATEWAY_SIGNING_KEY"] = _SIGNING_KEY
    good = edge.sign_context(str(org_a), "user_abc")

    assert edge.verify_context(str(org_a), "user_abc", good)
    assert not edge.verify_context(str(org_b), "user_abc", good), "org swapped"
    assert not edge.verify_context(str(org_a), "user_other", good), "user swapped"


# --- public paths ------------------------------------------------------------------------------


def test_healthz_needs_no_token(client):
    """A liveness probe must answer while the database and Clerk are both down."""
    assert client.get("/healthz").status_code == 200


def test_the_clerk_webhook_is_public_at_the_edge(client):
    """It authenticates by Svix HMAC over the body, and its sender holds no Clerk session."""
    assert edge.is_public("/clerk/webhook")
    assert not edge.is_public("/api/v1/meetings")


# --- routing -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/bff/dashboard", "bff"),
        ("/api/v1/meetings", "core-api"),
        ("/api/v1/meetings/abc-123", "core-api"),
        ("/api/v1/teams", "core-api"),
        # ADR-0047 Ask Myte threads are core-api's; the answer itself streams from rag-service.
        ("/api/v1/myte/threads", "core-api"),
        ("/api/v1/myte/threads/abc-123/messages", "core-api"),
        ("/api/v1/myte/messages/abc-123", "core-api"),
        # ADR-0049 Ask Myte v2: peer questions, memories and routines are core-api's too.
        ("/api/v1/myte/peer-questions", "core-api"),
        ("/api/v1/myte/peer-questions/abc-123/answer", "core-api"),
        ("/api/v1/myte/memories/abc-123", "core-api"),
        ("/api/v1/myte/routines/due", "core-api"),
        ("/api/v1/myte/routines/abc-123/ran", "core-api"),
        ("/api/rag/myte", "rag-service"),
        ("/scim/v2/Users", "identity-tenant-service"),
        ("/clerk/webhook", "identity-tenant-service"),
        ("/billing/subscriptions", "billing-metering-service"),
        ("/admin/ops/flags", "admin-ops-service"),
        ("/nope", None),
    ],
)
def test_routing_map(path, expected):
    assert edge.route_for(path) == expected


def test_no_two_services_share_a_prefix():
    """The catalog rule that makes routing a lookup instead of an ordered rule chain."""
    prefixes = list(edge.ROUTE_MAP)
    for outer in prefixes:
        for inner in prefixes:
            if outer != inner and inner.startswith(outer):
                assert (
                    edge.ROUTE_MAP[inner] == edge.ROUTE_MAP[outer]
                ), f"{inner} and {outer} overlap but resolve to different services"


@pytest.mark.parametrize(
    "path", ["/pii-tokens/mint", "/pii-tokens/resolve", "/pii-tokens", "/pii-tokens/anything"]
)
def test_the_gateway_never_routes_the_pii_vault(path):
    """`/pii-tokens/mint` is INTERNAL (per-caller service secret, ADR-0020 D2/ADR-0024): it must
    never be reachable from the public edge. `resolve` is user-facing in principle but is not
    exposed through the gateway either until a deliberate, reviewed change adds it. Asserted
    against the map itself AND the longest-prefix resolver, so neither a direct entry nor a
    shorter prefix that happens to capture `/pii-tokens` can sneak it in."""
    assert edge.route_for(path) is None, f"api-gateway routes {path} to {edge.route_for(path)}"
    assert not any(
        p.startswith("/pii") or path.startswith(p) for p in edge.ROUTE_MAP
    ), "a ROUTE_MAP prefix captures the pii vault"


# --- the one authenticated-but-tenant-exempt path ----------------------------------------------


def test_self_serve_passes_a_valid_token_for_an_unprovisioned_org_without_tenant_headers(
    client, keys
):
    priv, _pub = keys
    token = _token(priv, f"org_{uuid.uuid4().hex}")
    resp = client.post(
        edge.SELF_SERVE_ORG_PATH,
        headers={"Authorization": f"Bearer {token}", edge.ORG_HEADER: str(uuid.uuid4())},
    )
    assert resp.status_code == 200, resp.text
    # No tenant was resolved, so none is asserted downstream - and a client-sent copy is gone.
    assert resp.json() == {"org": None, "user": None, "sig": None}


def test_self_serve_still_requires_a_token(client):
    resp = client.post(edge.SELF_SERVE_ORG_PATH)
    assert resp.status_code == 401
    assert resp.json()["code"] == "AUTH_REQUIRED"


def test_self_serve_still_requires_an_org_claim(client, keys):
    priv, _pub = keys
    token = pyjwt.encode(
        {"sub": "user_abc", "iss": _ISSUER, "iat": int(time.time()), "exp": int(time.time()) + 600},
        priv,
        algorithm="RS256",
    )
    resp = client.post(edge.SELF_SERVE_ORG_PATH, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


def _orgless_token(priv, **overrides) -> str:
    now = int(time.time())
    claims = {"sub": "user_staff", "iss": _ISSUER, "iat": now, "nbf": now, "exp": now + 600}
    claims.update(overrides)
    return pyjwt.encode(claims, priv, algorithm="RS256")


def test_ops_console_passes_a_workspace_less_session_without_tenant_headers(client, keys):
    """ADR-0046 D1: superadmins belong to no workspace, so /admin/ops/* is verified without an
    organization and nothing tenant-shaped is attached (a client-sent copy is stripped)."""
    priv, _pub = keys
    resp = client.get(
        "/admin/ops/whoami",
        headers={"Authorization": f"Bearer {_orgless_token(priv)}", edge.ORG_HEADER: "x"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"org": None, "user": None, "sig": None}


def test_a_workspace_less_session_reaches_nothing_but_the_ops_console(client, keys):
    priv, _pub = keys
    auth = {"Authorization": f"Bearer {_orgless_token(priv)}"}
    for path in ("/admin/tenant/members", "/admin/opsx", "/api/v1/meetings", "/bff/dashboard"):
        assert client.get(path, headers=auth).status_code == 401, path


def test_the_ops_console_still_requires_a_valid_token(client, keys):
    priv, _pub = keys
    assert client.get("/admin/ops/whoami").status_code == 401
    expired = _orgless_token(priv, exp=int(time.time()) - 10)
    resp = client.get("/admin/ops/whoami", headers={"Authorization": f"Bearer {expired}"})
    assert resp.status_code == 401


def test_the_exemption_is_post_only(client, keys):
    """Any other method on the same path goes through normal tenant resolution."""
    priv, _pub = keys
    token = _token(priv, f"org_{uuid.uuid4().hex}")
    resp = client.get(edge.SELF_SERVE_ORG_PATH, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Token carries no usable organization"
