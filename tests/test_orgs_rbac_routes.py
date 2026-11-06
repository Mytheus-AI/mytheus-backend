"""`/orgs` and `/rbac/grants` route wiring, without a database.

The gate chain is exercised for real down to `tenant_context`, which is overridden with a fixed
`RequestContext` (the DB-backed tests in `test_orgs_provisioning_db.py` cover the real chain).
The request session is a recording fake, so these tests assert WHAT the routes ask Postgres to
do and what they return, including every denial path, with nothing running.

A bare FastAPI app plus the shared RFC 7807 handlers is used instead of `create_app`, so the
Idempotency middleware (Redis) and telemetry stay out of a test that is about route logic.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from libs.shared import authz as authz_mod
from libs.shared.authz import RequestContext
from libs.shared.errors import add_exception_handlers
from services.identity_tenant_service.provisioning import TenantConflict, TenantProvisioned
from services.identity_tenant_service.routes import orgs as orgs_mod
from services.identity_tenant_service.routes import rbac as rbac_mod

OPERATOR_ORG = uuid.uuid4()
CUSTOMER_ORG = uuid.uuid4()
CALLER = uuid.uuid4()
TARGET = uuid.uuid4()
NOW = datetime(2026, 11, 6, tzinfo=UTC)


class _Row(dict[str, Any]):
    """Both access styles a SQLAlchemy row offers: by column name, and positionally."""

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = [_Row(r) for r in rows]

    def mappings(self) -> _Result:
        return self

    def first(self) -> Any:
        return self._rows[0] if self._rows else None

    def one(self) -> Any:
        assert len(self._rows) == 1, self._rows
        return self._rows[0]

    def all(self) -> list[Any]:
        return list(self._rows)


Handler = Callable[[str, dict[str, Any]], list[dict[str, Any]]]


class FakeSession:
    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        sql = str(stmt)
        self.statements.append((sql, params or {}))
        return _Result(self.handler(sql, params or {}))

    def sql(self) -> str:
        return "\n".join(s for s, _ in self.statements)

    def begin(self) -> _NullTx:
        return _NullTx()


class _NullTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> None:
        return None


class FakeSessionmaker:
    """Stands in for `get_sessionmaker()()`: `async with sm() as s, s.begin():`."""

    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def __call__(self) -> FakeSessionmaker:
        return self

    async def __aenter__(self) -> FakeSession:
        return self.session

    async def __aexit__(self, *exc: object) -> None:
        return None


def _ctx(tier: str, org: uuid.UUID = CUSTOMER_ORG) -> RequestContext:
    return RequestContext(
        org_id=org, user_id=CALLER, clerk_user_id="user_x", rbac_tier=tier, ip=None
    )


def _client(ctx: RequestContext, session: FakeSession) -> TestClient:
    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(orgs_mod.router)
    app.include_router(rbac_mod.router)
    app.dependency_overrides[authz_mod.tenant_context] = lambda: ctx

    async def _session() -> AsyncIterator[FakeSession]:
        yield session

    app.dependency_overrides[authz_mod.tenant_session] = _session
    return TestClient(app)


@pytest.fixture(autouse=True)
def _no_denial_db_writes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Denials are audited in their own session; record them instead of opening one."""
    denied: list[str] = []

    async def _tier(ctx: RequestContext, *, route: str, required: str) -> None:
        denied.append(f"tier:{required}")

    async def _op(ctx: RequestContext, route: str) -> None:
        denied.append("operator")

    monkeypatch.setattr(authz_mod, "_audit_denial", _tier)
    monkeypatch.setattr(orgs_mod, "_audit_operator_denial", _op)
    return denied


def _audit_actions(session: FakeSession) -> list[str]:
    return [p["action"] for s, p in session.statements if "INSERT INTO audit_logs" in s]


# --- /orgs ----------------------------------------------------------------------------------

_BODY = {
    "name": "Acme",
    "slug": "acme-co",
    "clerk_org_id": "org_abc123",
    "owner": {"clerk_user_id": "user_owner1", "email": "owner@acme.test"},
}


def test_get_orgs_returns_only_the_callers_own_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        assert params == {"id": str(CUSTOMER_ORG)}
        return [
            {
                "id": CUSTOMER_ORG,
                "name": "Acme",
                "slug": "acme",
                "status": "active",
                "clerk_org_id": "org_a",
            }
        ]

    session = FakeSession(handler)
    monkeypatch.setattr(orgs_mod, "get_sessionmaker", lambda: FakeSessionmaker(session))
    r = _client(_ctx("member"), session).get("/orgs")
    assert r.status_code == 200
    assert r.json()["id"] == str(CUSTOMER_ORG)
    # Only columns `mytheus_app` holds SELECT on; `created_at` would be a permission error.
    assert "created_at" not in session.sql()


def test_get_orgs_with_no_grant_is_403(_no_denial_db_writes: list[str]) -> None:
    r = _client(_ctx(""), FakeSession(lambda s, p: [])).get("/orgs")
    assert r.status_code == 403
    assert r.json()["code"] == "FORBIDDEN_ROLE"
    assert _no_denial_db_writes == ["tier:member"]


def test_post_orgs_denies_an_owner_who_is_not_a_platform_operator(
    monkeypatch: pytest.MonkeyPatch, _no_denial_db_writes: list[str]
) -> None:
    monkeypatch.setenv(orgs_mod.OPERATOR_ORGS_ENV, str(OPERATOR_ORG))
    r = _client(_ctx("owner", CUSTOMER_ORG), FakeSession(lambda s, p: [])).post("/orgs", json=_BODY)
    assert r.status_code == 403
    assert r.headers["content-type"].startswith("application/problem+json")
    assert _no_denial_db_writes == ["operator"]


def test_post_orgs_is_denied_to_everyone_when_no_operator_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(orgs_mod.OPERATOR_ORGS_ENV, raising=False)
    r = _client(_ctx("owner", OPERATOR_ORG), FakeSession(lambda s, p: [])).post("/orgs", json=_BODY)
    assert r.status_code == 403


def test_post_orgs_requires_owner_even_in_an_operator_tenant(
    monkeypatch: pytest.MonkeyPatch, _no_denial_db_writes: list[str]
) -> None:
    monkeypatch.setenv(orgs_mod.OPERATOR_ORGS_ENV, str(OPERATOR_ORG))
    r = _client(_ctx("admin", OPERATOR_ORG), FakeSession(lambda s, p: [])).post("/orgs", json=_BODY)
    assert r.status_code == 403
    assert _no_denial_db_writes == ["tier:owner"]


def test_operator_env_ignores_garbage_and_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(orgs_mod.OPERATOR_ORGS_ENV, f"not-a-uuid, ,{OPERATOR_ORG}")
    assert orgs_mod.operator_org_ids() == frozenset({OPERATOR_ORG})


def _operator_session(monkeypatch: pytest.MonkeyPatch) -> FakeSession:
    monkeypatch.setenv(orgs_mod.OPERATOR_ORGS_ENV, str(OPERATOR_ORG))
    session = FakeSession(lambda s, p: [])
    monkeypatch.setattr(orgs_mod, "get_sessionmaker", lambda: FakeSessionmaker(session))
    return session


def test_post_orgs_provisions_audits_both_tenants_and_syncs_clerk_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _operator_session(monkeypatch)
    calls: list[str] = []
    owner_id = uuid.uuid4()

    async def fake_provision(s: Any, **kw: Any) -> TenantProvisioned:
        calls.append("provision")
        assert kw["clerk_org_id"] == "org_abc123"
        assert kw["owner"].clerk_user_id == "user_owner1"
        return TenantProvisioned(org_id=kw["org_id"], owner_user_id=owner_id)

    async def fake_sync(result: Any, clerk_org_id: str | None) -> bool:
        calls.append("clerk_sync")
        return True

    monkeypatch.setattr(orgs_mod, "provision_tenant", fake_provision)
    monkeypatch.setattr(orgs_mod, "sync_internal_org_id_to_clerk", fake_sync)

    r = _client(_ctx("owner", OPERATOR_ORG), session).post("/orgs", json=_BODY)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["slug"] == "acme-co" and body["owner_user_id"] == str(owner_id)
    assert calls == ["provision", "clerk_sync"]
    audit_orgs = [p["org_id"] for s, p in session.statements if "INSERT INTO audit_logs" in s]
    assert audit_orgs == [str(OPERATOR_ORG), body["id"]]
    assert _audit_actions(session) == ["org.provisioned", "org.provisioned"]


def test_post_orgs_conflict_is_409_and_echoes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    _operator_session(monkeypatch)

    async def conflict(s: Any, **kw: Any) -> TenantProvisioned:
        raise TenantConflict

    monkeypatch.setattr(orgs_mod, "provision_tenant", conflict)
    r = _client(_ctx("owner", OPERATOR_ORG), FakeSession(lambda s, p: [])).post("/orgs", json=_BODY)
    assert r.status_code == 409
    text = r.text
    assert "acme-co" not in text and "org_abc123" not in text


@pytest.mark.parametrize(
    "patch",
    [
        {"clerk_org_id": "3f1e0a3c-0000-0000-0000-000000000000"},  # internal UUID, not Clerk's
        {"slug": "Has Spaces"},
        {"org_id": str(uuid.uuid4())},  # never client-supplied
        {"owner": {"clerk_user_id": "nope", "email": "a@b.c"}},
    ],
)
def test_post_orgs_validates_body(monkeypatch: pytest.MonkeyPatch, patch: dict[str, Any]) -> None:
    _operator_session(monkeypatch)
    r = _client(_ctx("owner", OPERATOR_ORG), FakeSession(lambda s, p: [])).post(
        "/orgs", json={**_BODY, **patch}
    )
    assert r.status_code == 422
    assert r.json()["code"] == "VALIDATION_FAILED"


# --- /rbac/grants ---------------------------------------------------------------------------


def _rbac_handler(
    *, user_exists: bool = True, current: str | None = "member", owners: int = 2
) -> Handler:
    def handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if sql.startswith("SELECT 1 FROM users"):
            return [{"x": 1}] if user_exists else []
        if sql.startswith("SELECT tier FROM rbac_grants"):
            return [{"tier": current}] if current else []
        if "WHERE tier = 'owner'" in sql:
            return [{"id": i} for i in range(owners)]
        if sql.startswith("INSERT INTO rbac_grants"):
            return [
                {"user_id": TARGET, "tier": params["t"], "granted_by": CALLER, "created_at": NOW}
            ]
        return []

    return handler


def test_list_grants_requires_admin(_no_denial_db_writes: list[str]) -> None:
    r = _client(_ctx("member"), FakeSession(lambda s, p: [])).get("/rbac/grants")
    assert r.status_code == 403
    assert _no_denial_db_writes == ["tier:admin"]


def test_list_grants_is_keyset_paginated() -> None:
    ids = sorted(uuid.uuid4() for _ in range(3))
    rows = [{"user_id": i, "tier": "member", "granted_by": None, "created_at": NOW} for i in ids]
    session = FakeSession(lambda s, p: rows[: p["limit"]])
    client = _client(_ctx("admin"), session)

    first = client.get("/rbac/grants", params={"limit": 2}).json()
    assert [g["user_id"] for g in first["items"]] == [str(i) for i in ids[:2]]
    assert first["next_cursor"]

    client.get("/rbac/grants", params={"limit": 2, "cursor": first["next_cursor"]})
    assert session.statements[-1][1]["after"] == str(ids[1])
    assert "org_id" not in session.statements[-1][0]  # RLS scopes it, not a predicate


def test_list_grants_rejects_a_malformed_cursor() -> None:
    r = _client(_ctx("admin"), FakeSession(lambda s, p: [])).get("/rbac/grants?cursor=%%%")
    assert r.status_code == 400
    assert r.json()["code"] == "INVALID_CURSOR"


def test_set_grant_unknown_user_is_404_before_any_tier_check() -> None:
    # A member-of-another-tenant id is invisible under RLS. Even a would-be escalation
    # (admin -> owner) must 404, not 403, so the route cannot be used as an existence probe.
    session = FakeSession(_rbac_handler(user_exists=False))
    r = _client(_ctx("admin"), session).put(f"/rbac/grants/{TARGET}", json={"tier": "owner"})
    assert r.status_code == 404
    assert "INSERT INTO rbac_grants" not in session.sql()


def test_admin_cannot_escalate_to_owner() -> None:
    session = FakeSession(_rbac_handler(current="member"))
    r = _client(_ctx("admin"), session).put(f"/rbac/grants/{TARGET}", json={"tier": "owner"})
    assert r.status_code == 403
    assert "INSERT INTO rbac_grants" not in session.sql()
    assert _audit_actions(session) == []


def test_set_grant_writes_the_grant_and_audits_the_change() -> None:
    session = FakeSession(_rbac_handler(current="member"))
    r = _client(_ctx("admin"), session).put(f"/rbac/grants/{TARGET}", json={"tier": "admin"})
    assert r.status_code == 200, r.text
    assert r.json()["tier"] == "admin"
    insert = next(p for s, p in session.statements if s.startswith("INSERT INTO rbac_grants"))
    assert insert["o"] == str(CUSTOMER_ORG) and insert["by"] == str(CALLER)
    assert _audit_actions(session) == ["rbac.granted"]


def test_set_grant_to_the_same_tier_writes_no_audit_row() -> None:
    session = FakeSession(_rbac_handler(current="member"))
    r = _client(_ctx("admin"), session).put(f"/rbac/grants/{TARGET}", json={"tier": "member"})
    assert r.status_code == 200
    assert _audit_actions(session) == []


def test_set_grant_rejects_an_unknown_tier() -> None:
    r = _client(_ctx("owner"), FakeSession(_rbac_handler())).put(
        f"/rbac/grants/{TARGET}", json={"tier": "superadmin"}
    )
    assert r.status_code == 422


def test_revoking_the_last_owner_is_409() -> None:
    session = FakeSession(_rbac_handler(current="owner", owners=1))
    r = _client(_ctx("owner"), session).delete(f"/rbac/grants/{TARGET}")
    assert r.status_code == 409
    assert "DELETE FROM rbac_grants" not in session.sql()


def test_revoke_deletes_and_audits() -> None:
    session = FakeSession(_rbac_handler(current="member"))
    r = _client(_ctx("admin"), session).delete(f"/rbac/grants/{TARGET}")
    assert r.status_code == 204
    assert "DELETE FROM rbac_grants" in session.sql()
    assert _audit_actions(session) == ["rbac.revoked"]


def test_revoking_a_user_with_no_grant_is_a_silent_no_op() -> None:
    session = FakeSession(_rbac_handler(current=None))
    r = _client(_ctx("admin"), session).delete(f"/rbac/grants/{TARGET}")
    assert r.status_code == 204
    assert "DELETE FROM rbac_grants" not in session.sql()
    assert _audit_actions(session) == []


def test_owner_count_is_locked_for_update() -> None:
    session = FakeSession(_rbac_handler(current="owner", owners=2))
    r = _client(_ctx("owner"), session).put(f"/rbac/grants/{TARGET}", json={"tier": "admin"})
    assert r.status_code == 200
    owner_sql = next(s for s, _ in session.statements if "WHERE tier = 'owner'" in s)
    assert "FOR UPDATE" in owner_sql
