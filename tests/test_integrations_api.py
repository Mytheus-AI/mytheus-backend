"""integration-service: tables + RLS, the OAuth engine, the settings API, webhooks, the internal
Slack routes and the gateway's routing of `/integrations`, `/oauth`, `/webhooks` (P12 contract
section 1; plan 10-13 sections 3.10 and 3.11).

Every provider is a fake (`tests/_integration_fakes.py`); the database and Redis are real.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Any
from urllib.parse import parse_qs, urlparse

import jwt as pyjwt
import psycopg2
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.api_gateway import edge
from services.integration_service import clients as clients_mod
from services.integration_service import core_client, internal, routes, store, webhooks
from services.integration_service.oauth_state import mint_state, sign_token, state_key
from services.integration_service.redaction import get_logger
from tests import _integration_fakes as fx
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, add_participant, seed_meeting, seed_tenant
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")


class _Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = _Principal()


def sign_in(t: Tenant) -> None:
    PRINCIPAL.clerk_org = t.clerk_org
    PRINCIPAL.clerk_user = t.clerk_user


@pytest.fixture(scope="module")
def fakes():
    return fx.fake_clients()


@pytest.fixture(scope="module")
def client(migrated_db, fakes):
    previous_env = fx.install_env()
    previous_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    clients_mod.set_clients(fakes)

    app = create_app("integration-service-test")
    app.include_router(routes.router)
    app.include_router(webhooks.router)
    app.include_router(internal.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c

    clients_mod.set_clients(None)
    core_client.set_core(None)
    fx.restore_env(previous_env)
    if previous_db is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous_db
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def admin_tenant() -> Tenant:
    t = seed_tenant("owner")
    sign_in(t)
    return t


def connection_rows(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql("SELECT * FROM integration_connections WHERE org_id = %(o)s", {"o": str(org_id)})


def start(client: TestClient, provider: str) -> str:
    r = client.post(f"/integrations/{provider}/connect")
    assert r.status_code == 200, r.text
    return fx.state_from(r.json()["authorize_url"])


def complete(client: TestClient, provider: str, state: str, code: str = "auth-code") -> Any:
    """What the web app's callback page does, signed in, after the provider redirect."""
    return client.post(f"/oauth/{provider}", json={"code": code, "state": state})


def connect_and_callback(client: TestClient, provider: str) -> Any:
    return complete(client, provider, start(client, provider))


def ok(response: Any, provider: str) -> None:
    assert response.status_code == 200, response.text
    assert response.json() == {"provider": provider, "status": "active"}


def refused(response: Any, status: int, reason: str) -> None:
    assert response.status_code == status, response.text
    assert response.json()["reason"] == reason


# --- RLS -------------------------------------------------------------------------------------


NEW_TABLES = (
    "integration_connections",
    "oauth_tokens",
    "integration_id_map",
    "integration_cursors",
    "integration_webhook_inbox",
)


def _seed_all_tables(t: Tenant) -> None:
    conn_id = uuid.uuid4()
    _sql(
        "INSERT INTO integration_connections (id, org_id, provider, external_account_id, "
        "installed_by) VALUES (%(c)s, %(o)s, 'jira', %(e)s, %(u)s)",
        {"c": str(conn_id), "o": str(t.org_id), "e": f"cloud-{t.org_id}", "u": str(t.user_id)},
    )
    _sql(
        "INSERT INTO oauth_tokens (org_id, connection_id, access_token_enc) "
        "VALUES (%(o)s, %(c)s, '\\x4d59450100'::bytea)",
        {"o": str(t.org_id), "c": str(conn_id)},
    )
    _sql(
        "INSERT INTO integration_id_map (org_id, provider, nexus_kind, nexus_id, external_id) "
        "VALUES (%(o)s, 'jira', 'action', %(n)s, 'NEX-1')",
        {"o": str(t.org_id), "n": str(uuid.uuid4())},
    )
    _sql(
        "INSERT INTO integration_cursors (org_id, connection_id, kind) VALUES (%(o)s, %(c)s, 'x')",
        {"o": str(t.org_id), "c": str(conn_id)},
    )
    _sql(
        "INSERT INTO integration_webhook_inbox (org_id, provider, delivery_id, payload) "
        "VALUES (%(o)s, 'jira', 'd1', '{}'::jsonb)",
        {"o": str(t.org_id)},
    )


def _app_sql(query: str, org: uuid.UUID | None, route: str | None = None) -> list[Any]:
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            if org is not None:
                cur.execute("SELECT set_config('app.current_org', %s, true)", (str(org),))
            if route is not None:
                cur.execute("SELECT set_config('app.integration_route', %s, true)", (route,))
            cur.execute(query)
            rows = cur.fetchall()
        conn.rollback()
        return rows
    finally:
        conn.close()


def test_every_new_table_is_isolated_per_tenant_and_fails_closed(client):
    a, b = seed_tenant(), seed_tenant()
    _seed_all_tables(a)
    _seed_all_tables(b)
    for table in NEW_TABLES:
        seen = {str(r[0]) for r in _app_sql(f"SELECT org_id FROM {table}", a.org_id)}
        assert seen == {str(a.org_id)}, table
        assert _app_sql(f"SELECT org_id FROM {table}", None) == [], table
        forced = _sql(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = %(t)s",
            {"t": table},
        )[0]
        assert forced["relrowsecurity"] and forced["relforcerowsecurity"], table


def test_the_webhook_route_policy_reveals_only_the_named_connection(client):
    a, b = seed_tenant(), seed_tenant()
    _seed_all_tables(a)
    _seed_all_tables(b)
    rows = _app_sql(
        "SELECT org_id FROM integration_connections", None, route=f"jira:cloud-{a.org_id}"
    )
    assert [str(r[0]) for r in rows] == [str(a.org_id)]
    assert _app_sql("SELECT org_id FROM integration_connections", None, route="jira:") == []
    # The route GUC never reaches any other table.
    assert _app_sql("SELECT org_id FROM oauth_tokens", None, route=f"jira:cloud-{a.org_id}") == []


# --- authorization and the connect preconditions ---------------------------------------------


def test_a_member_cannot_list_connect_or_disconnect(client):
    t = seed_tenant("member")
    sign_in(t)
    assert client.get("/integrations").status_code == 403
    r = client.post("/integrations/slack/connect")
    assert r.status_code == 403
    assert r.json()["code"] == "FORBIDDEN_ROLE"
    assert client.delete("/integrations/slack").status_code == 403


def test_a_provider_outside_r8_is_404(client):
    admin_tenant()
    # Linear is connectable since ADR-0044; a provider no ADR names is still 404.
    assert client.post("/integrations/github/connect").status_code == 404


def test_an_unconfigured_provider_is_409(client, monkeypatch):
    admin_tenant()
    monkeypatch.delenv("SLACK_CLIENT_SECRET")
    r = client.post("/integrations/slack/connect")
    assert r.status_code == 409
    assert r.json()["code"] == "PROVIDER_NOT_CONFIGURED"
    listed = {i["provider"]: i for i in client.get("/integrations").json()["items"]}
    assert listed["slack"]["configured"] is False
    assert listed["jira"]["configured"] is True


def test_a_second_pm_connection_is_409(client):
    t = admin_tenant()
    _sql(
        "INSERT INTO integration_connections (org_id, provider, installed_by) "
        "VALUES (%(o)s, 'linear', %(u)s)",
        {"o": str(t.org_id), "u": str(t.user_id)},
    )
    r = client.post("/integrations/jira/connect")
    assert r.status_code == 409
    assert r.json()["code"] == "PM_ALREADY_CONNECTED"
    # Calendar and Slack are not PM tools and are unaffected.
    assert client.post("/integrations/slack/connect").status_code == 200


# --- the OAuth round trip --------------------------------------------------------------------


def test_connect_stores_the_verifier_server_side_and_sends_only_the_challenge(client):
    t = admin_tenant()
    r = client.post("/integrations/slack/connect")
    url = r.json()["authorize_url"]
    query = parse_qs(urlparse(url).query)
    # The provider sends the browser back to the web app, never straight to the API.
    assert query["redirect_uri"] == [
        "https://app.mytheus.test/settings/integrations/callback/slack"
    ]
    state = query["state"][0]
    import redis as sync_redis

    stored = sync_redis.from_url(os.environ["REDIS_URL"]).get(state_key(t.org_id, state))
    assert stored is not None
    pending = json.loads(stored)
    assert pending["provider"] == "slack" and pending["user_id"] == str(t.user_id)
    assert pending["verifier"] not in url  # only the S256 challenge leaves the server
    ttl = sync_redis.from_url(os.environ["REDIS_URL"]).ttl(state_key(t.org_id, state))
    assert 0 < ttl <= 600


def test_completion_connects_encrypts_tokens_and_never_returns_them(client, fakes):
    t = admin_tenant()
    ok(connect_and_callback(client, "slack"), "slack")

    [row] = connection_rows(t.org_id)
    assert row["status"] == "active" and row["external_account_id"].startswith("T0TEAM")
    assert str(row["installed_by"]) == str(t.user_id)
    [tok] = _sql(
        "SELECT access_token_enc, token_version FROM oauth_tokens WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    blob = bytes(tok["access_token_enc"])
    assert blob[:4] == b"MYE\x01"
    assert b"xoxb" not in blob  # ciphertext, not the token

    listed = client.get("/integrations")
    assert listed.status_code == 200
    body = listed.text
    for secret in ("xoxb-slack-ACCESS-secret", "access_token", "refresh_token"):
        assert secret not in body
    item = {i["provider"]: i for i in listed.json()["items"]}["slack"]
    assert set(item) == {
        "provider",
        "configured",
        "status",
        "scopes",
        "external_account_id",
        "installed_by",
        "installed_at",
        "last_synced_at",
        "decision_checks",  # A1: Slack only; the fake install predates the history scopes
    }
    assert item["status"] == "active" and "chat:write" in item["scopes"]
    assert item["decision_checks"] == "needs_reconnect"
    audits = _sql(
        "SELECT action FROM audit_logs WHERE org_id = %(o)s AND action = 'integration.connected'",
        {"o": str(t.org_id)},
    )
    assert len(audits) == 1


async def test_tokens_round_trip_through_the_envelope(client):
    t = admin_tenant()
    ok(connect_and_callback(client, "jira"), "jira")
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from libs.shared.tenancy import set_rls

    engine = create_async_engine(APP_URL)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            await set_rls(session, t.org_id)
            conn = await store.get_connection(session, "jira")
            assert conn is not None
            tokens = await store.load_tokens(session, conn)
            assert tokens.access_token == "jira-ACCESS-secret-token"
            assert tokens.refresh_token == "jira-REFRESH-secret-token"
            assert "secret" not in repr(tokens)
    finally:
        await engine.dispose()


def test_jira_post_install_registers_the_webhook_with_the_cloud_id(client, fakes):
    t = admin_tenant()
    ok(connect_and_callback(client, "jira"), "jira")
    [row] = connection_rows(t.org_id)
    hook = fakes.jira.webhooks[-1]
    cloud = row["external_account_id"]
    assert hook["url"] == f"https://api.mytheus.test/webhooks/jira?cloud_id={cloud}"
    [cursor] = _sql(
        "SELECT cursor, expires_at FROM integration_cursors WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert cursor["cursor"]["webhook_ids"] == [101] and cursor["expires_at"] is not None


def test_calendar_post_install_watches_and_queues_the_initial_sync(client, fakes):
    t = admin_tenant()
    ok(connect_and_callback(client, "google_calendar"), "google_calendar")
    watch = fakes.calendar.watches[-1]
    assert watch["address"] == "https://api.mytheus.test/webhooks/google_calendar"
    inbox = _sql(
        "SELECT event_type FROM integration_webhook_inbox WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert [r["event_type"] for r in inbox] == ["initial_sync"]


def test_a_tampered_state_is_rejected(client):
    admin_tenant()
    state = start(client, "slack")
    body, sig = state.split(".")
    refused(complete(client, "slack", f"{body[:-2]}AA.{sig}"), 400, "invalid_state")
    refused(complete(client, "slack", "x"), 400, "invalid_state")


def test_an_expired_state_is_rejected(client):
    t = admin_tenant()
    old = mint_state(fx.STATE_SECRET.encode(), t.org_id, t.user_id, "slack", now=time.time() - 3600)
    refused(complete(client, "slack", old), 400, "state_expired")
    assert connection_rows(t.org_id) == []


def test_a_state_is_single_use(client):
    t = admin_tenant()
    state = start(client, "slack")
    ok(complete(client, "slack", state), "slack")
    refused(complete(client, "slack", state), 400, "invalid_state")
    assert len(connection_rows(t.org_id)) == 1


def test_a_state_for_another_provider_is_rejected(client):
    admin_tenant()
    state = start(client, "slack")
    refused(complete(client, "jira", state), 400, "invalid_state")


def test_a_pkce_mismatch_fails_the_exchange(client):
    t = admin_tenant()
    state = start(client, "slack")
    import redis as sync_redis

    key = state_key(t.org_id, state)
    redis = sync_redis.from_url(os.environ["REDIS_URL"])
    pending = json.loads(redis.get(key))
    pending["verifier"] = "a-different-verifier-" + "x" * 40
    redis.set(key, json.dumps(pending), ex=600)
    refused(complete(client, "slack", state), 400, "exchange_failed")
    assert connection_rows(t.org_id) == []


def test_another_admin_of_the_same_tenant_cannot_complete_my_state(client):
    t = admin_tenant()
    state = start(client, "slack")
    other_admin = seed_user(t, "admin")
    sign_in(other_admin)
    refused(complete(client, "slack", state), 400, "invalid_state")
    assert connection_rows(t.org_id) == []
    # The refused attempt did not burn the state: its owner can still finish.
    sign_in(t)
    ok(complete(client, "slack", state), "slack")
    [row] = connection_rows(t.org_id)
    assert str(row["installed_by"]) == str(t.user_id)


def test_a_user_of_another_tenant_cannot_complete_my_state(client):
    """The login-CSRF shape: the attacker starts a connect in their tenant and gets a victim's
    signed-in browser to finish it. The victim's session must not attach their account to the
    attacker's tenant, nor connect anything in the victim's own tenant."""
    attacker = admin_tenant()
    state = start(client, "jira")
    victim = admin_tenant()
    refused(complete(client, "jira", state), 400, "invalid_state")
    assert connection_rows(attacker.org_id) == [] and connection_rows(victim.org_id) == []


def test_completion_requires_a_signed_in_owner_or_admin(client):
    t = admin_tenant()
    state = start(client, "slack")
    _sql("UPDATE rbac_grants SET tier = 'member' WHERE user_id = %(u)s", {"u": str(t.user_id)})
    r = complete(client, "slack", state)
    assert r.status_code == 403 and r.json()["code"] == "FORBIDDEN_ROLE"
    assert connection_rows(t.org_id) == []


def test_one_provider_account_is_live_in_one_tenant_at_a_time(client, fakes):
    first = admin_tenant()
    fakes.oauth["slack"].account = f"TSHARED{uuid.uuid4().hex[:6]}"
    try:
        ok(connect_and_callback(client, "slack"), "slack")
        second = admin_tenant()
        r = connect_and_callback(client, "slack")
        refused(r, 409, "account_connected_elsewhere")
        assert r.json()["code"] == "ACCOUNT_CONNECTED_ELSEWHERE"
        assert connection_rows(second.org_id) == []
        assert fakes.oauth["slack"].revoked == []  # the first tenant's install is untouched
        # Once the first tenant disconnects, the account can move.
        sign_in(first)
        assert client.delete("/integrations/slack").status_code == 204
        sign_in(second)
        ok(connect_and_callback(client, "slack"), "slack")
    finally:
        fakes.oauth["slack"].account = None


def test_disconnect_revokes_deletes_tokens_and_audits(client, fakes):
    t = admin_tenant()
    ok(connect_and_callback(client, "jira"), "jira")
    r = client.delete("/integrations/jira")
    assert r.status_code == 204
    [row] = connection_rows(t.org_id)
    assert row["status"] == "disconnected"
    assert _sql("SELECT 1 FROM oauth_tokens WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []
    assert (
        _sql("SELECT 1 FROM integration_cursors WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []
    )
    assert fakes.jira.deleted[-1] == [101]
    assert fakes.oauth["jira"].revoked[-1] == "jira-REFRESH-secret-token"
    audits = _sql(
        "SELECT 1 FROM audit_logs WHERE org_id = %(o)s AND action = 'integration.disconnected'",
        {"o": str(t.org_id)},
    )
    assert len(audits) == 1
    # Idempotent, and a never-connected provider is 404.
    assert client.delete("/integrations/jira").status_code == 204
    assert client.delete("/integrations/slack").status_code == 404
    # With the PM disconnected, Jira can be connected again.
    assert client.post("/integrations/jira/connect").status_code == 200


# --- log redaction ---------------------------------------------------------------------------


def test_a_log_line_carrying_a_token_is_redacted(caplog):
    log = get_logger("services.integration_service.test")
    with caplog.at_level(logging.INFO, logger="services.integration_service.test"):
        log.info("exchange body access_token=ya29.SECRETVALUE&code=4/abc123")
        log.info("provider said %s", {"refresh_token": "1//REFRESHSECRET", "ok": True})
        log.info('raw {"client_secret": "CLIENTSECRET", "bot": "xoxb-1234-SLACKSECRET"}')
        log.info("header Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig")
    text = caplog.text
    for secret in (
        "ya29.SECRETVALUE",
        "4/abc123",
        "1//REFRESHSECRET",
        "CLIENTSECRET",
        "xoxb-1234-SLACKSECRET",
        "eyJhbGciOiJIUzI1NiJ9",
    ):
        assert secret not in text
    assert text.count("[REDACTED]") >= 5


# --- webhooks --------------------------------------------------------------------------------


def _slack_headers(body: bytes, ts: int | None = None, secret: str = fx.SLACK_SIGNING) -> dict:
    ts = ts if ts is not None else int(time.time())
    sig = hmac.new(secret.encode(), f"v0:{ts}:".encode() + body, hashlib.sha256).hexdigest()
    return {
        "X-Slack-Request-Timestamp": str(ts),
        "X-Slack-Signature": f"v0={sig}",
        "Content-Type": "application/json",
    }


def _inbox(org_id: uuid.UUID, provider: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT delivery_id, event_type, status FROM integration_webhook_inbox "
        "WHERE org_id = %(o)s AND provider = %(p)s",
        {"o": str(org_id), "p": provider},
    )


def _connect_row(t: Tenant, provider: str, account: str) -> uuid.UUID:
    cid = uuid.uuid4()
    _sql(
        "INSERT INTO integration_connections (id, org_id, provider, external_account_id, "
        "installed_by) VALUES (%(c)s, %(o)s, %(p)s, %(e)s, %(u)s)",
        {"c": str(cid), "o": str(t.org_id), "p": provider, "e": account, "u": str(t.user_id)},
    )
    return cid


def test_the_database_holds_one_live_connection_per_provider_account():
    a, b = seed_tenant(), seed_tenant()
    account = f"TONE{uuid.uuid4().hex[:6]}"
    _connect_row(a, "slack", account)
    with pytest.raises(psycopg2.errors.UniqueViolation):
        _connect_row(b, "slack", account)
    _sql(
        "UPDATE integration_connections SET status = 'disconnected' WHERE org_id = %(o)s",
        {"o": str(a.org_id)},
    )
    _connect_row(b, "slack", account)  # a disconnected row no longer holds the account
    rows = _app_sql("SELECT org_id FROM integration_connections", None, route=f"slack:{account}")
    assert [str(r[0]) for r in rows] == [str(b.org_id)]


def test_slack_webhook_verifies_dedupes_and_acks_fast(client, fakes):
    t = seed_tenant()
    team = f"T{uuid.uuid4().hex[:8].upper()}"
    _connect_row(t, "slack", team)
    event = {
        "type": "event_callback",
        "team_id": team,
        "event_id": "Ev123",
        "event": {"type": "app_uninstalled"},
    }
    body = json.dumps(event).encode()
    before = fakes.oauth["slack"].exchanges
    started = time.monotonic()
    r = client.post("/webhooks/slack", content=body, headers=_slack_headers(body))
    elapsed = time.monotonic() - started
    assert r.status_code == 200 and r.json() == {"status": "accepted"}
    assert elapsed < 1.5  # well inside Slack's 3 s
    assert fakes.oauth["slack"].exchanges == before  # no provider call on the receive path

    replay = client.post("/webhooks/slack", content=body, headers=_slack_headers(body))
    assert replay.status_code == 200 and replay.json() == {"status": "duplicate"}
    assert _inbox(t.org_id, "slack") == [
        {"delivery_id": "Ev123", "event_type": "app_uninstalled", "status": "pending"}
    ]


def test_slack_webhook_rejects_a_bad_signature_and_a_stale_timestamp(client):
    body = json.dumps({"type": "event_callback", "team_id": "TX", "event_id": "E1"}).encode()
    bad = client.post("/webhooks/slack", content=body, headers=_slack_headers(body, secret="no"))
    assert bad.status_code == 401
    stale = client.post(
        "/webhooks/slack", content=body, headers=_slack_headers(body, ts=int(time.time()) - 600)
    )
    assert stale.status_code == 401
    tampered = client.post("/webhooks/slack", content=body + b" ", headers=_slack_headers(body))
    assert tampered.status_code == 401


def test_slack_url_verification_and_unknown_workspace(client):
    body = json.dumps({"type": "url_verification", "challenge": "abc"}).encode()
    r = client.post("/webhooks/slack", content=body, headers=_slack_headers(body))
    assert r.json() == {"challenge": "abc"}
    body = json.dumps({"type": "event_callback", "team_id": "TNOPE", "event_id": "E9"}).encode()
    r = client.post("/webhooks/slack", content=body, headers=_slack_headers(body))
    assert r.status_code == 200 and r.json() == {"status": "ignored"}


def test_google_webhook_verifies_the_channel_token(client):
    t = seed_tenant()
    cid = _connect_row(t, "google_calendar", f"{uuid.uuid4().hex}@example.com")
    token = sign_token(
        fx.STATE_SECRET.encode(), "gcal-channel", {"o": str(t.org_id), "c": str(cid), "ch": "ch-1"}
    )
    headers = {
        "X-Goog-Channel-ID": "ch-1",
        "X-Goog-Channel-Token": token,
        "X-Goog-Resource-State": "exists",
        "X-Goog-Message-Number": "7",
    }
    r = client.post("/webhooks/google_calendar", headers=headers)
    assert r.status_code == 200 and r.json() == {"status": "accepted"}
    assert client.post("/webhooks/google_calendar", headers=headers).json() == {
        "status": "duplicate"
    }
    assert [r["delivery_id"] for r in _inbox(t.org_id, "google_calendar")] == ["ch-1:7"]
    wrong_channel = {**headers, "X-Goog-Channel-ID": "ch-2"}
    assert client.post("/webhooks/google_calendar", headers=wrong_channel).status_code == 401
    forged = {**headers, "X-Goog-Channel-Token": token[:-3] + "AAA"}
    assert client.post("/webhooks/google_calendar", headers=forged).status_code == 401


def test_jira_webhook_verifies_the_atlassian_jwt(client):
    t = seed_tenant()
    cloud = f"cloud-{uuid.uuid4().hex[:8]}"
    _connect_row(t, "jira", cloud)
    body = json.dumps({"webhookEvent": "jira:issue_updated", "issue": {"key": "NEX-1"}}).encode()
    exp = int(time.time()) + 60

    def signed(claims: dict[str, Any], secret: str = fx.JIRA_SECRET) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {pyjwt.encode(claims, secret)}",
            "X-Atlassian-Webhook-Identifier": "wh-1",
            "Content-Type": "application/json",
        }

    good = signed({"exp": exp, "context": {"cloudId": cloud}})
    url = f"/webhooks/jira?cloud_id={cloud}"
    assert client.post(url, content=body, headers=good).json() == {"status": "accepted"}
    assert client.post(url, content=body, headers=good).json() == {"status": "duplicate"}
    forged = signed({"exp": exp, "context": {"cloudId": cloud}}, "not-the-secret-" + "x" * 30)
    assert client.post(url, content=body, headers=forged).status_code == 401
    no_exp = signed({"context": {"cloudId": cloud}})
    assert client.post(url, content=body, headers=no_exp).status_code == 401
    other_cloud = signed({"exp": exp, "context": {"cloudId": "cloud-someone-else"}})
    assert client.post(url, content=body, headers=other_cloud).status_code == 401
    no_cloud_claim = signed({"exp": exp})
    assert client.post(url, content=body, headers=no_cloud_claim).status_code == 401
    missing = client.post(url, content=body, headers={"X-Atlassian-Webhook-Identifier": "x"})
    assert missing.status_code == 401
    no_route = client.post("/webhooks/jira", content=body, headers=good)
    assert no_route.status_code == 401
    assert len(_inbox(t.org_id, "jira")) == 1


# --- internal Slack routes -------------------------------------------------------------------


def _caller(t: Tenant, service: str, secret: str) -> dict[str, str]:
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(t.org_id),
    }


def test_slack_token_is_for_notification_service_only(client):
    t = admin_tenant()
    notif = _caller(t, "notification-service", fx.CALLER_SECRET_NOTIF)
    assert client.get("/internal/slack/token", headers=notif).status_code == 404  # not connected
    connect_and_callback(client, "slack")
    r = client.get("/internal/slack/token", headers=notif)
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    body = r.json()
    assert body["access_token"] == "xoxb-slack-ACCESS-secret"
    assert body["team_id"].startswith("T0TEAM") and body["expires_in"] > 0
    wf = _caller(t, "workflow-service", fx.CALLER_SECRET_WF)
    assert client.get("/internal/slack/token", headers=wf).status_code == 401
    wrong = _caller(t, "notification-service", "x" * 40)
    assert client.get("/internal/slack/token", headers=wrong).status_code == 401


def test_prep_brief_and_summary_emit_one_slack_notification_per_participant(client):
    t = admin_tenant()
    other = seed_user(t)
    meeting = seed_meeting(t.org_id)
    for p in (t, other):
        add_participant(meeting, t.org_id, p.user_id)
    digest = {
        "meeting_id": str(meeting),
        "title": "Roadmap review",
        "status": "scheduled",
        "scheduled_start": "2026-11-12T10:00:00+00:00",
        "participants": [
            {"user_id": str(t.user_id), "email": "u@example.com", "open_actions": 2},
            {"user_id": str(other.user_id), "email": "v@example.com", "open_actions": 0},
        ],
        "decisions": [{"content": "Ship v2 on Friday"}],
        "actions": [
            {
                "content": "Write the spec",
                "assignee_id": str(t.user_id),
                "due_date": None,
                "status": "confirmed",
            }
        ],
    }
    core_client.set_core(fx.FakeCore(digest))  # type: ignore[arg-type]
    wf = _caller(t, "workflow-service", fx.CALLER_SECRET_WF)
    body = {"meeting_id": str(meeting)}
    assert client.post("/internal/slack/prep-brief", json=body, headers=wf).json() == {"sent": 0}

    connect_and_callback(client, "slack")
    assert client.post("/internal/slack/prep-brief", json=body, headers=wf).json() == {"sent": 2}
    assert client.post("/internal/slack/prep-brief", json=body, headers=wf).json() == {"sent": 2}
    r = client.post("/internal/slack/meeting-summary", json=body, headers=wf)
    assert r.json() == {"sent": 2}
    rows = _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
        "AND topic = 'notifications' ORDER BY id",
        {"o": str(t.org_id)},
    )
    assert len(rows) == 4  # 2 briefs + 2 summaries; the replayed brief added nothing
    brief = rows[0]["payload"]
    assert brief["channel"] == "slack" and brief["link"] == f"/meetings/{meeting}"
    assert brief["title"] == "Prep brief: Roadmap review"
    summary = rows[2]["payload"]
    assert "Ship v2 on Friday" in summary["body"] and "Write the spec" in summary["body"]
    notif = _caller(t, "notification-service", fx.CALLER_SECRET_NOTIF)
    assert client.post("/internal/slack/prep-brief", json=body, headers=notif).status_code == 401


# --- the gateway -----------------------------------------------------------------------------


def test_gateway_routes_the_three_integration_prefixes():
    assert edge.route_for("/integrations") == "integration-service"
    assert edge.route_for("/integrations/jira/connect") == "integration-service"
    assert edge.route_for("/oauth/slack") == "integration-service"
    assert edge.route_for("/webhooks/jira") == "integration-service"
    assert edge.route_for("/internal/slack/token") is None


def _edge_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    from fastapi import FastAPI

    monkeypatch.setenv("GATEWAY_SIGNING_KEY", "k")
    app = FastAPI()
    app.middleware("http")(edge.edge_auth)
    app.state.reached = []

    # authz: public - a stand-in upstream that only records what the edge let through.
    @app.api_route("/{path:path}", methods=["GET", "POST", "DELETE"])
    async def upstream(path: str) -> dict[str, str]:
        app.state.reached.append(path)
        return {"reached": path}

    return app


def test_gateway_lets_only_webhooks_through_without_a_jwt(monkeypatch):
    """Through the real `edge_auth` middleware: webhooks reach the service with no bearer
    token; the OAuth completion and the settings API are refused at the edge."""
    with TestClient(_edge_app(monkeypatch)) as gw:
        for provider in ("slack", "google_calendar", "jira"):
            r = gw.post(f"/webhooks/{provider}", content=b"{}")
            assert r.status_code == 200, provider
        for method, path in (
            ("POST", "/oauth/slack"),
            ("GET", "/oauth/google_calendar?code=x&state=y"),
            ("GET", "/integrations"),
            ("POST", "/integrations/slack/connect"),
            ("DELETE", "/integrations/slack"),
        ):
            r = gw.request(method, path)
            assert r.status_code == 401, path
            assert r.json()["code"] == "AUTH_REQUIRED"
    assert edge.is_public("/webhooks/slack")
    assert not edge.is_public("/oauth/jira")
    assert not edge.is_public("/integrations") and not edge.is_public("/integrationsx")


TRAVERSALS = [
    "/oauth/../internal/slack/token",
    "/webhooks/../internal/slack/token",
    "/webhooks/slack/../../internal/slack/token",
    "/webhooks/%2e%2e/internal/slack/token",
    "/webhooks/%2E%2E/internal/slack/token",
    "/webhooks/%2e%2E/internal/slack/token",
    "/webhooks/.%2e/internal/slack/token",
    "/webhooks/./slack",
    "/webhooks/%2e/slack",
    "/webhooks/slack%2f..%2finternal",
    "/webhooks/slack%2F..%2Finternal",
    "/webhooks/slack%5c..%5cinternal",
    "/webhooks/slack%5C..",
    "/webhooks\\..\\internal/slack/token",
    "/webhooks//slack",
    "//internal/slack/token",
    "/webhooks/%252e%252e/internal/slack/token",
    "/internal/slack/token",
    "/internal",
    "/api/v1/meetings/../../internal/slack/token",
]


@pytest.mark.parametrize("raw", TRAVERSALS)
def test_normalize_path_refuses_every_ambiguous_form(raw):
    assert edge.normalize_path(raw) is None


@pytest.mark.parametrize(
    "raw", ["/webhooks/slack", "/integrations/jira/connect", "/oauth/slack", "/api/v1/meetings"]
)
def test_normalize_path_keeps_ordinary_paths(raw):
    assert edge.normalize_path(raw) == raw


async def _asgi(app: Any, method: str, raw_path: str, headers: list | None = None) -> int:
    """Drive the ASGI app with an un-normalized path, as a raw socket client can send it
    (httpx and the TestClient would normalize `..` away before it ever left the test)."""
    from urllib.parse import unquote

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": unquote(raw_path),
        "raw_path": raw_path.encode(),
        "query_string": b"",
        "headers": headers or [],
        "client": ("127.0.0.1", 1),
        "server": ("gateway", 80),
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return int(next(m["status"] for m in sent if m["type"] == "http.response.start"))


@pytest.mark.parametrize("raw", TRAVERSALS)
async def test_the_edge_refuses_traversal_before_auth_or_routing(raw, monkeypatch):
    app = _edge_app(monkeypatch)
    assert await _asgi(app, "GET", raw) == 404
    assert app.state.reached == []  # nothing reached the upstream


async def test_the_proxy_never_forwards_internal_or_dot_segments(monkeypatch):
    """Belt and braces: the forwarding hop applies the same rule even without the edge."""
    from fastapi import FastAPI

    from services.api_gateway import proxy

    app = FastAPI()
    proxy.install(app)
    for raw in ("/oauth/../internal/slack/token", "/internal/slack/token"):
        assert await _asgi(app, "GET", raw) == 404
