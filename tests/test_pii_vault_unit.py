"""Unit tests for `services/identity_tenant_service/pii_vault` - no database.

`_FakeSession` interprets exactly the statements the vault issues, keyed on the statement
objects themselves, and enforces the two UNIQUE constraints and the tenant check the real
table does. The real-Postgres counterpart (RLS, grants, constraints) is
`tests/test_pii_tokens_oauth_tokens_db.py`.
"""

from __future__ import annotations

import base64
import os
import uuid
from typing import Any

import pytest

from libs.shared import audit as audit_mod
from libs.shared.crypto import BlindIndex, DecryptionError, EnvelopeCipher, LocalKeyProvider
from services.identity_tenant_service.pii_vault import vault as vault_mod
from services.identity_tenant_service.pii_vault.internal_auth import (
    authenticate_service,
    parse_caller_tokens,
)
from services.identity_tenant_service.pii_vault.normalize import EntityType, normalize
from services.identity_tenant_service.pii_vault.policy import FinalDisposition, final_disposition
from services.identity_tenant_service.pii_vault.vault import PiiVault, VaultError


class _Result:
    def __init__(self, rows: list[dict[str, Any]] | None = None, scalar: Any = None) -> None:
        self._rows = rows or []
        self._scalar = scalar

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def mappings(self) -> _Result:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSession:
    """A shared table plus a per-session `app.current_org`, like RLS would give."""

    def __init__(self, table: list[dict[str, Any]], current_org: uuid.UUID | None) -> None:
        self.table = table
        self.current_org = current_org
        self.audits: list[dict[str, Any]] = []

    def _visible(self) -> list[dict[str, Any]]:
        return [r for r in self.table if r["org"] == self.current_org]

    async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        p = params or {}
        if stmt is vault_mod._CURRENT_ORG:
            return _Result(scalar=str(self.current_org) if self.current_org else None)
        if stmt is vault_mod._SELECT_BY_HMAC:
            for r in self._visible():
                if r["et"] == p["et"] and r["h"] == p["h"]:
                    return _Result(scalar=r["token"])
            return _Result()
        if stmt is vault_mod._INSERT:
            assert p["org"] == self.current_org, "WITH CHECK would refuse this"
            for r in self.table:
                if r["org"] == p["org"] and r["et"] == p["et"] and r["h"] == p["h"]:
                    return _Result()  # ON CONFLICT DO NOTHING
                if r["org"] == p["org"] and r["token"] == p["token"]:
                    raise RuntimeError("unique violation (org_id, token)")
            self.table.append(dict(p))
            return _Result(scalar=p["token"])
        if stmt is vault_mod._SELECT_BY_TOKENS:
            rows = [
                {
                    "token": r["token"],
                    "entity_type": r["et"],
                    "value_ciphertext": r["ct"],
                    "value_hmac": r["h"],
                }
                for r in self._visible()
                if r["token"] in p["tokens"]
            ]
            return _Result(rows=rows)
        if stmt is audit_mod._INSERT:
            self.audits.append(dict(p))
            return _Result()
        raise AssertionError(f"unexpected statement: {stmt}")


def _vault() -> PiiVault:
    env = {
        "ENVIRONMENT": "test",
        "MYTHEUS_KEK_BASE64": base64.b64encode(os.urandom(32)).decode(),
        "MYTHEUS_BLIND_INDEX_KEY_BASE64": base64.b64encode(os.urandom(32)).decode(),
    }
    provider = LocalKeyProvider.from_env(env)
    return PiiVault(EnvelopeCipher(provider), BlindIndex(provider))


@pytest.fixture
def vault() -> PiiVault:
    return _vault()


# --- tokenize -----------------------------------------------------------------------------


async def test_tokenize_is_stable_and_normalizes(vault: PiiVault) -> None:
    org = uuid.uuid4()
    s = _FakeSession([], org)
    a = await vault.tokenize(s, org, "Alice@Example.com", EntityType.EMAIL)
    b = await vault.tokenize(s, org, "  alice@example.com ", EntityType.EMAIL)
    assert a == b
    assert len(s.table) == 1
    assert a.placeholder == f"‹EMAIL:{a.token}›"
    assert len(a.token) == 16 and int(a.token, 16) >= 0


async def test_ciphertext_holds_the_raw_value_not_plaintext(vault: PiiVault) -> None:
    org = uuid.uuid4()
    s = _FakeSession([], org)
    await vault.tokenize(s, org, "Alice@Example.com", EntityType.EMAIL)
    row = s.table[0]
    assert b"Alice" not in row["ct"] and b"alice" not in row["ct"]
    assert len(row["h"]) == 32


async def test_entity_type_separates_tokens(vault: PiiVault) -> None:
    org = uuid.uuid4()
    s = _FakeSession([], org)
    a = await vault.tokenize(s, org, "Jordan", EntityType.PERSON)
    b = await vault.tokenize(s, org, "Jordan", EntityType.LOC)
    assert a.token != b.token


async def test_same_value_different_tenants_mints_unrelated_tokens(vault: PiiVault) -> None:
    table: list[dict[str, Any]] = []
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    a = await vault.tokenize(_FakeSession(table, org_a), org_a, "a@b.co", EntityType.EMAIL)
    b = await vault.tokenize(_FakeSession(table, org_b), org_b, "a@b.co", EntityType.EMAIL)
    assert a.token != b.token
    assert table[0]["h"] != table[1]["h"]


async def test_tokenize_refuses_mismatched_rls(vault: PiiVault) -> None:
    with pytest.raises(VaultError):
        await vault.tokenize(_FakeSession([], uuid.uuid4()), uuid.uuid4(), "x", EntityType.ORG)
    with pytest.raises(VaultError):
        await vault.tokenize(_FakeSession([], None), uuid.uuid4(), "x", EntityType.ORG)


async def test_concurrent_mint_converges(vault: PiiVault) -> None:
    """Another pod inserted between our SELECT miss and our INSERT: ON CONFLICT path."""
    org = uuid.uuid4()
    table: list[dict[str, Any]] = []
    first = await vault.tokenize(_FakeSession(table, org), org, "Acme", EntityType.ORG)

    class _Racing(_FakeSession):
        hidden_once = True

        async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
            if stmt is vault_mod._SELECT_BY_HMAC and self.hidden_once:
                self.hidden_once = False
                return _Result()
            return await super().execute(stmt, params)

    second = await vault.tokenize(_Racing(table, org), org, "ACME", EntityType.ORG)
    assert second == first and len(table) == 1


# --- detokenize ---------------------------------------------------------------------------


async def test_detokenize_round_trip_and_audit(vault: PiiVault) -> None:
    org, actor = uuid.uuid4(), uuid.uuid4()
    s = _FakeSession([], org)
    t = await vault.tokenize(s, org, "Alice Smith", EntityType.PERSON)
    resolved, unresolved = await vault.detokenize(s, org, [t.token, "0" * 16], actor_id=actor)
    assert [(r.token, r.value) for r in resolved] == [(t.token, "Alice Smith")]
    assert unresolved == ["0" * 16]
    actions = [a["action"] for a in s.audits]
    assert actions == ["pii.token_resolved", "pii.token_resolve_miss"]
    assert all("Alice" not in str(a) for a in s.audits)
    assert s.audits[0]["actor_id"] == str(actor)


async def test_detokenize_cannot_see_another_tenant(vault: PiiVault) -> None:
    table: list[dict[str, Any]] = []
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    t = await vault.tokenize(_FakeSession(table, org_a), org_a, "secret", EntityType.PERSON)
    resolved, unresolved = await vault.detokenize(
        _FakeSession(table, org_b), org_b, [t.token], actor_id=None
    )
    assert resolved == [] and unresolved == [t.token]


async def test_ciphertext_moved_between_rows_fails(vault: PiiVault) -> None:
    org = uuid.uuid4()
    s = _FakeSession([], org)
    a = await vault.tokenize(s, org, "Alice", EntityType.PERSON)
    await vault.tokenize(s, org, "Bob", EntityType.PERSON)
    s.table[0]["ct"], s.table[1]["ct"] = s.table[1]["ct"], s.table[0]["ct"]
    with pytest.raises(DecryptionError):
        await vault.detokenize(s, org, [a.token], actor_id=None)


async def test_cross_tenant_row_transplant_fails(vault: PiiVault) -> None:
    """Even with RLS bypassed (row copied into tenant B), the ciphertext will not open."""
    table: list[dict[str, Any]] = []
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    t = await vault.tokenize(_FakeSession(table, org_a), org_a, "secret", EntityType.PERSON)
    table.append({**table[0], "org": org_b})
    with pytest.raises(DecryptionError):
        await vault.detokenize(_FakeSession(table, org_b), org_b, [t.token], actor_id=None)


# --- normalize ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("et", "a", "b"),
    [
        (EntityType.PHONE, "+1 (415) 555-0100", "+14155550100"),
        (EntityType.CARD, "4111 1111-1111 1111", "4111111111111111"),
        (EntityType.IBAN, "de89 3704 0044 0532 0130 00", "DE89370400440532013000"),
        (EntityType.PERSON, "  Alice   SMITH ", "alice smith"),
    ],
)
def test_normalize_equivalences(et: EntityType, a: str, b: str) -> None:
    assert normalize(et, a) == normalize(et, b)


def test_normalize_keeps_international_prefix_distinct() -> None:
    assert normalize(EntityType.PHONE, "+44 20") != normalize(EntityType.PHONE, "44 20")


@pytest.mark.parametrize("bad", ["", "   ", "x" * 1025])
def test_normalize_refuses_empty_or_huge(bad: str) -> None:
    with pytest.raises(ValueError):
        normalize(EntityType.ORG, bad)


# --- policy -------------------------------------------------------------------------------


def test_unavailable_ner_policy_fails_closed() -> None:
    assert (
        final_disposition(pii_redaction_enabled=True, ner_available=False) is FinalDisposition.HOLD
    )
    assert (
        final_disposition(pii_redaction_enabled=True, ner_available=True)
        is FinalDisposition.PERSIST
    )
    assert (
        final_disposition(pii_redaction_enabled=False, ner_available=False)
        is FinalDisposition.PERSIST
    )


# --- internal service auth ----------------------------------------------------------------


_SECRET = "s" * 40


def test_caller_tokens_parse_and_authenticate() -> None:
    tokens = parse_caller_tokens(f"transcription-service={_SECRET}")
    headers = {"x-mytheus-service": "transcription-service", "x-mytheus-service-token": _SECRET}
    assert authenticate_service(headers, tokens) == "transcription-service"


@pytest.mark.parametrize(
    "raw",
    ["core-api=" + _SECRET, "transcription-service=short", "transcription-service", "=x"],
)
def test_caller_tokens_refuse_bad_config(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_caller_tokens(raw)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"x-mytheus-service": "transcription-service", "x-mytheus-service-token": "wrong"},
        {"x-mytheus-service": "ingestion-service", "x-mytheus-service-token": _SECRET},
        {"x-mytheus-service-token": _SECRET},
    ],
)
def test_authenticate_service_refuses(headers: dict[str, str]) -> None:
    from libs.shared.errors import ProblemDetail

    tokens = parse_caller_tokens(f"transcription-service={_SECRET}")
    with pytest.raises(ProblemDetail) as exc:
        authenticate_service(headers, tokens)
    assert exc.value.status == 401


# --- routes (no database: dependencies overridden) -----------------------------------------


def _route_app() -> Any:
    from fastapi import FastAPI

    from libs.shared.errors import add_exception_handlers
    from services.identity_tenant_service.pii_vault import routes

    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(routes.router)
    return app, routes


def test_mint_route_refuses_unauthenticated_callers(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    app, _ = _route_app()
    body = {"spans": [{"entity_type": "EMAIL", "value": "a@b.co"}]}
    monkeypatch.delenv("MYTHEUS_PII_VAULT_CALLER_TOKENS", raising=False)
    with TestClient(app) as client:
        assert client.post("/pii-tokens/mint", json=body).status_code == 401
        monkeypatch.setenv("MYTHEUS_PII_VAULT_CALLER_TOKENS", f"ingestion-service={_SECRET}")
        r = client.post(
            "/pii-tokens/mint",
            json=body,
            headers={"x-mytheus-service": "ingestion-service", "x-mytheus-service-token": "no"},
        )
        assert r.status_code == 401
        r = client.post(
            "/pii-tokens/mint",
            json=body,
            headers={
                "x-mytheus-service": "ingestion-service",
                "x-mytheus-service-token": _SECRET,
                "x-mytheus-org": "not-a-uuid",
            },
        )
        assert r.status_code == 401


def test_mint_route_happy_path_with_overrides() -> None:
    from fastapi.testclient import TestClient

    from services.identity_tenant_service.pii_vault.internal_auth import (
        InternalCaller,
        internal_caller,
    )

    app, routes = _route_app()
    org = uuid.uuid4()
    session = _FakeSession([], org)
    v = _vault()

    async def _session() -> Any:
        yield session

    app.dependency_overrides[internal_caller] = lambda: InternalCaller("ingestion-service", org)
    app.dependency_overrides[routes.internal_tenant_session] = _session
    app.dependency_overrides[routes.get_vault] = lambda: v
    body = {
        "spans": [
            {"entity_type": "EMAIL", "value": "a@b.co"},
            {"entity_type": "EMAIL", "value": "A@B.CO"},
            {"entity_type": "PERSON", "value": "Alice"},
        ]
    }
    with TestClient(app) as client:
        r = client.post("/pii-tokens/mint", json=body)
    assert r.status_code == 200, r.text
    toks = r.json()["tokens"]
    assert toks[0]["token"] == toks[1]["token"] != toks[2]["token"]
    assert toks[2]["placeholder"].startswith("‹PERSON:")
    assert "a@b.co" not in r.text


def test_mint_route_validates_body() -> None:
    from fastapi.testclient import TestClient

    from services.identity_tenant_service.pii_vault.internal_auth import (
        InternalCaller,
        internal_caller,
    )

    app, routes = _route_app()
    org = uuid.uuid4()

    async def _session() -> Any:
        yield _FakeSession([], org)

    app.dependency_overrides[internal_caller] = lambda: InternalCaller("rag-service", org)
    app.dependency_overrides[routes.internal_tenant_session] = _session
    app.dependency_overrides[routes.get_vault] = _vault
    with TestClient(app) as client:
        for bad in (
            {"spans": []},
            {"spans": [{"entity_type": "SSN", "value": "x"}]},
            {"spans": [{"entity_type": "EMAIL", "value": ""}]},
        ):
            assert client.post("/pii-tokens/mint", json=bad).status_code == 422
