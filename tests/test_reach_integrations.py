"""integration-service Phase 4 reach: HubSpot, Notion and Confluence connections, the HubSpot
meeting note, and decision record exports to Notion, Confluence and Slack.

Providers are in-memory fakes; the database, Redis and core-api's internal reach routes are
real (core-api runs in-process behind an httpx ASGI transport, so the visibility rules the
exports depend on are the real ones).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx
import psycopg2
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import internal_reach
from services.integration_service import clients as clients_mod
from services.integration_service import core_client, reach, routes
from services.integration_service.clients import ProviderError, TokenGrant
from tests import _integration_fakes as fx
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, add_participant, seed_meeting, seed_tenant
from tests.test_reach_internal_core import action, decision, set_email
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

REACH_ENV = {
    "HUBSPOT_CLIENT_ID": "hubspot-client",
    "HUBSPOT_CLIENT_SECRET": "hubspot-secret",
    "NOTION_CLIENT_ID": "notion-client",
    "NOTION_CLIENT_SECRET": "notion-secret",
    "CONFLUENCE_CLIENT_ID": "confluence-client",
    "CONFLUENCE_CLIENT_SECRET": "confluence-secret",
}
WF = {"X-Mytheus-Service": "workflow-service", "X-Mytheus-Service-Token": fx.CALLER_SECRET_WF}


# --- fakes ------------------------------------------------------------------------------------


class FakeReachOAuth(fx.FakeOAuth):
    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        self.exchanges += 1
        return TokenGrant(
            access_token=f"{self.provider}-ACCESS-secret",
            refresh_token=f"{self.provider}-REFRESH-secret",
            expires_in=3600,
            scopes=list(self.scopes),
            external_account_id=f"{self.provider}-{uuid.uuid4().hex[:8]}",
            extra=(
                {"site_url": "https://acme.atlassian.net"} if self.provider == "confluence" else {}
            ),
        )


class FakeHubSpot:
    def __init__(self) -> None:
        self.companies: dict[str, dict[str, Any]] = {}
        self.contacts: dict[str, dict[str, Any]] = {}
        self.notes: dict[str, dict[str, Any]] = {}
        self.searches: list[tuple[str, list[str]]] = []
        self.associations: list[tuple[str, str, str]] = []
        self.accounts = 0
        self.next_id = 9000

    async def account(self, token: str) -> dict[str, Any]:
        self.accounts += 1
        return {}

    async def companies_by_domain(self, token: str, domains: list[str]) -> list[dict[str, Any]]:
        self.searches.append(("companies", domains))
        return [c for d, c in self.companies.items() if d in domains]

    async def contacts_by_email(self, token: str, emails: list[str]) -> list[dict[str, Any]]:
        self.searches.append(("contacts", emails))
        return [c for e, c in self.contacts.items() if e in emails]

    async def create_note(
        self,
        token: str,
        *,
        body_html: str,
        timestamp: str,
        company_ids: list[str],
        contact_ids: list[str],
    ) -> str:
        self.next_id += 1
        note_id = str(self.next_id)
        self.notes[note_id] = {
            "body": body_html,
            "companies": list(company_ids),
            "contacts": list(contact_ids),
            "updates": 0,
        }
        return note_id

    async def update_note(
        self, token: str, note_id: str, *, body_html: str, timestamp: str
    ) -> bool:
        if note_id not in self.notes:
            return False
        self.notes[note_id]["body"] = body_html
        self.notes[note_id]["updates"] += 1
        return True

    async def associate_note(self, token: str, note_id: str, to_object: str, to_id: str) -> None:
        self.associations.append((note_id, to_object, to_id))


class FakeNotion:
    def __init__(self) -> None:
        self.pages = [{"id": "aaaa1111", "title": "Decisions", "url": "https://notion.so/d"}]
        self.created: list[dict[str, Any]] = []
        self.archived: list[str] = []

    async def me(self, token: str) -> dict[str, Any]:
        return {"object": "user"}

    async def search_pages(self, token: str, limit: int = 50) -> list[dict[str, Any]]:
        return list(self.pages)

    async def create_page(
        self, token: str, *, parent_page_id: str, title: str, blocks: list[dict[str, Any]]
    ) -> dict[str, Any]:
        page_id = f"page{len(self.created)}"
        self.created.append(
            {"id": page_id, "parent": parent_page_id, "title": title, "blocks": blocks}
        )
        return {"id": page_id, "url": f"https://notion.so/{page_id}"}

    async def archive_page(self, token: str, page_id: str) -> None:
        self.archived.append(page_id)


class FakeConfluence:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.updated: list[str] = []
        self.space_calls = 0

    async def spaces(self, token: str, cloud_id: str) -> list[dict[str, Any]]:
        self.space_calls += 1
        return [{"key": "ENG", "name": "Engineering"}]

    async def create_page(
        self, token: str, cloud_id: str, *, space_key: str, title: str, storage: str
    ) -> dict[str, Any]:
        page_id = str(500 + len(self.created))
        self.created.append({"id": page_id, "space": space_key, "storage": storage})
        return {"id": page_id, "webui": f"/spaces/{space_key}/pages/{page_id}"}

    async def update_page(
        self, token: str, cloud_id: str, page_id: str, *, title: str, storage: str
    ) -> dict[str, Any] | None:
        self.updated.append(page_id)
        return {"id": page_id, "webui": f"/spaces/X/pages/{page_id}"}


class FakeSlackExport(fx.FakeSlack):
    def __init__(self) -> None:
        self.users = {"me@acme.io": "U1"}
        self.posts: list[dict[str, Any]] = []

    async def user_id_for_email(self, token: str, email: str) -> str | None:
        return self.users.get(email)

    async def open_dm(self, token: str, user_id: str) -> str:
        return f"D{user_id}"

    async def post_message(
        self, token: str, *, channel: str, text: str, blocks: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if channel == "CNOTIN":
            raise ProviderError("slack", "post_message", "not_in_channel")
        self.posts.append({"channel": channel, "text": text, "blocks": blocks})
        return {"channel": channel, "ts": f"1700000000.{len(self.posts):06d}"}

    async def permalink(self, token: str, *, channel: str, ts: str) -> str | None:
        return f"https://slack.test/{channel}/{ts}"


# --- harness ----------------------------------------------------------------------------------


class _Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = _Principal()


def sign_in(t: Tenant) -> None:
    PRINCIPAL.clerk_org = t.clerk_org
    PRINCIPAL.clerk_user = t.clerk_user


@pytest.fixture(scope="module")
def fakes():
    c = fx.fake_clients()
    c.oauth["hubspot"] = FakeReachOAuth("hubspot", ("oauth",))
    c.oauth["notion"] = FakeReachOAuth("notion", ())
    c.oauth["confluence"] = FakeReachOAuth("confluence", ("write:confluence-content",))
    c.hubspot = FakeHubSpot()  # type: ignore[assignment]
    c.notion = FakeNotion()  # type: ignore[assignment]
    c.confluence = FakeConfluence()  # type: ignore[assignment]
    c.slack = FakeSlackExport()  # type: ignore[assignment]
    return c


@pytest.fixture(scope="module")
def client(migrated_db, fakes):
    previous_env = fx.install_env()
    previous_reach = {k: os.environ.get(k) for k in REACH_ENV}
    os.environ.update(REACH_ENV)
    previous_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    clients_mod.set_clients(fakes)

    core_app = create_app("core-api-reach")
    core_app.include_router(internal_reach.router)
    core_client.set_core(
        core_client.CoreApiClient(
            base_url="http://core-api",
            secret=fx.CORE_SECRET_INTEG,
            transport=httpx.ASGITransport(app=core_app),
        )
    )

    app = create_app("integration-service-reach-test")
    app.include_router(routes.router)
    app.include_router(reach.internal_router)
    app.include_router(reach.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c

    clients_mod.set_clients(None)
    core_client.set_core(None)
    fx.restore_env(previous_env)
    fx.restore_env(previous_reach)
    if previous_db is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous_db
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def connect(client: TestClient, provider: str) -> None:
    r = client.post(f"/integrations/{provider}/connect")
    assert r.status_code == 200, r.text
    state = fx.state_from(r.json()["authorize_url"])
    done = client.post(f"/oauth/{provider}", json={"code": "c", "state": state})
    assert done.status_code == 200, done.text
    assert done.json() == {"provider": provider, "status": "active"}


def admin(email: str = "me@acme.io") -> Tenant:
    t = seed_tenant("owner")
    set_email(t, email)
    sign_in(t)
    return t


def meeting(*people: Tenant, guests: list[str] | None = None, confidential: bool = False):
    m = seed_meeting(people[0].org_id)
    for p in people:
        add_participant(m, p.org_id, p.user_id)
    if guests is not None:
        _sql(
            "INSERT INTO meeting_followups (meeting_id, org_id, guest_emails) "
            "VALUES (%(m)s, %(o)s, %(g)s)",
            {"m": str(m), "o": str(people[0].org_id), "g": guests},
        )
    if confidential:
        _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(m)})
    return m


def note(client: TestClient, t: Tenant, m: uuid.UUID) -> Any:
    return client.post(
        "/internal/crm/meeting-note",
        json={"meeting_id": str(m)},
        headers={**WF, "X-Mytheus-Org": str(t.org_id)},
    )


# --- connections ------------------------------------------------------------------------------


def test_the_reach_providers_are_listed_and_follow_configured(client, monkeypatch):
    admin()
    items = {i["provider"]: i for i in client.get("/integrations").json()["items"]}
    for provider in ("hubspot", "notion", "confluence"):
        assert items[provider]["configured"] is True
        assert items[provider]["status"] is None
    monkeypatch.delenv("NOTION_CLIENT_SECRET")
    r = client.post("/integrations/notion/connect")
    assert r.status_code == 409
    assert r.json()["code"] == "PROVIDER_NOT_CONFIGURED"
    items = {i["provider"]: i for i in client.get("/integrations").json()["items"]}
    assert items["notion"]["configured"] is False


def test_connect_proves_each_token_and_disconnect_revokes(client, fakes):
    t = admin()
    accounts = fakes.hubspot.accounts
    connect(client, "hubspot")
    assert fakes.hubspot.accounts == accounts + 1
    connect(client, "notion")
    calls = fakes.confluence.space_calls
    connect(client, "confluence")
    assert fakes.confluence.space_calls == calls + 1
    cursor = _sql(
        "SELECT c.cursor FROM integration_cursors c JOIN integration_connections i "
        "ON i.id = c.connection_id WHERE i.org_id = %(o)s AND c.kind = 'confluence_site'",
        {"o": str(t.org_id)},
    )
    assert cursor[0]["cursor"]["site_url"] == "https://acme.atlassian.net"
    # Reach providers are not PM tools: Jira still connects alongside them.
    connect(client, "jira")

    assert client.delete("/integrations/hubspot").status_code == 204
    assert "hubspot-REFRESH-secret" in fakes.oauth["hubspot"].revoked
    status = _sql(
        "SELECT status FROM integration_connections WHERE org_id = %(o)s AND provider = 'hubspot'",
        {"o": str(t.org_id)},
    )
    assert status[0]["status"] == "disconnected"


# --- HubSpot meeting note ---------------------------------------------------------------------


def test_the_note_route_is_for_workflow_service_only(client):
    t = admin()
    m = meeting(t)
    notif = {
        "X-Mytheus-Service": "notification-service",
        "X-Mytheus-Service-Token": fx.CALLER_SECRET_NOTIF,
        "X-Mytheus-Org": str(t.org_id),
    }
    r = client.post("/internal/crm/meeting-note", json={"meeting_id": str(m)}, headers=notif)
    assert r.status_code == 401


def test_no_hubspot_connection_is_not_connected(client, monkeypatch):
    t = admin()
    m = meeting(t, guests=["buyer@customer.com"])
    assert note(client, t, m).json()["status"] == "not_connected"
    connect(client, "hubspot")
    monkeypatch.delenv("HUBSPOT_CLIENT_ID")
    assert note(client, t, m).json()["status"] == "not_connected"


def test_one_note_per_meeting_matched_by_external_domain_and_updated_on_replay(client, fakes):
    t = admin("maya.patel@acme.io")
    connect(client, "hubspot")
    fakes.hubspot.companies["customer.com"] = {"id": "c-1"}
    fakes.hubspot.companies["acme.io"] = {"id": "c-own"}  # our own company must never match
    fakes.hubspot.companies["gmail.com"] = {"id": "c-gmail"}
    fakes.hubspot.contacts["buyer@customer.com"] = {"id": "p-1"}
    fakes.hubspot.contacts["pal@gmail.com"] = {"id": "p-2"}
    m = meeting(t, guests=["buyer@customer.com", "pal@gmail.com"])
    decision(t, m, "Ship the pilot in March")
    decision(t, m, "Not confirmed", status="proposed")
    action(t, m, "Send the <contract>")

    first = note(client, t, m)
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["status"] == "created"
    assert (body["companies"], body["contacts"]) == (1, 2)
    stored = fakes.hubspot.notes[body["note_id"]]
    assert stored["companies"] == ["c-1"]
    assert sorted(stored["contacts"]) == ["p-1", "p-2"]
    assert "Ship the pilot in March" in stored["body"]
    assert "Not confirmed" not in stored["body"]
    assert "Send the &lt;contract&gt;" in stored["body"]
    assert "Maya Patel" in stored["body"]
    assert "@" not in stored["body"]  # addresses are search values, never note text
    assert ("companies", ["customer.com"]) in fakes.hubspot.searches

    decision(t, m, "Add a second site")
    again = note(client, t, m).json()
    assert again["status"] == "updated"
    assert again["note_id"] == body["note_id"]
    assert len([n for n in fakes.hubspot.notes if n == body["note_id"]]) == 1
    assert "Add a second site" in fakes.hubspot.notes[body["note_id"]]["body"]
    links = _sql(
        "SELECT external_id, last_synced_status FROM integration_id_map "
        "WHERE org_id = %(o)s AND provider = 'hubspot' AND nexus_kind = 'meeting'",
        {"o": str(t.org_id)},
    )
    assert links == [{"external_id": body["note_id"], "last_synced_status": "updated"}]

    del fakes.hubspot.notes[body["note_id"]]  # someone deleted it in HubSpot
    third = note(client, t, m).json()
    assert third["status"] == "created"
    assert third["note_id"] != body["note_id"]


def test_no_match_writes_no_note(client, fakes):
    t = admin()
    connect(client, "hubspot")
    m = meeting(t, guests=["someone@unknown-co.example"])
    before = len(fakes.hubspot.notes)
    assert note(client, t, m).json() == {
        "status": "no_match",
        "companies": 0,
        "contacts": 0,
        "note_id": None,
    }
    assert len(fakes.hubspot.notes) == before


def test_a_confidential_meeting_is_skipped(client, fakes):
    t = admin()
    connect(client, "hubspot")
    fakes.hubspot.companies["secret-customer.com"] = {"id": "c-9"}
    m = meeting(t, guests=["x@secret-customer.com"], confidential=True)
    before = len(fakes.hubspot.notes)
    assert note(client, t, m).json()["status"] == "skipped_confidential"
    assert len(fakes.hubspot.notes) == before


def test_another_tenants_meeting_is_404(client):
    a = admin()
    m = meeting(a)
    b = admin()
    connect(client, "hubspot")
    assert note(client, b, m).status_code == 404


# --- decision exports -------------------------------------------------------------------------


def test_export_refuses_not_configured_and_not_connected(client, monkeypatch):
    t = admin()
    m = meeting(t)
    d = decision(t, m, "Adopt the RFC")
    r = client.post(f"/integrations/exports/decisions/{d}", json={"provider": "slack"})
    assert r.status_code == 409
    assert (r.json()["code"], r.json()["reason"]) == ("PROVIDER_NOT_CONFIGURED", "not_connected")
    dest = client.get("/integrations/exports/destinations", params={"provider": "notion"})
    assert dest.json()["configured"] is True
    assert dest.json()["connected"] is False
    monkeypatch.delenv("CONFLUENCE_CLIENT_ID")
    r = client.post(
        f"/integrations/exports/decisions/{d}", json={"provider": "confluence", "space_key": "E"}
    )
    assert r.json()["reason"] == "not_configured"
    dest = client.get("/integrations/exports/destinations", params={"provider": "confluence"})
    assert dest.json()["configured"] is False


def test_export_to_notion_with_lineage_and_replace_on_reexport(client, fakes):
    t = admin()
    connect(client, "notion")
    m = meeting(t)
    earlier = decision(t, m, "Use vendor A")
    d = decision(t, m, "Use vendor B", supersedes=earlier)

    dest = client.get("/integrations/exports/destinations", params={"provider": "notion"}).json()
    assert dest["connected"] is True
    assert dest["destinations"][0] == {
        "id": "aaaa1111",
        "name": "Decisions",
        "kind": "page",
        "url": "https://notion.so/d",
    }

    r = client.post(
        f"/integrations/exports/decisions/{d}",
        json={"provider": "notion", "parent_page_id": "aaaa1111"},
    )
    assert r.status_code == 200, r.text
    first = r.json()
    assert first["replaced"] is False
    created = fakes.notion.created[-1]
    rendered = str(created["blocks"])
    assert "Use vendor B" in rendered
    assert "Earlier: Use vendor A" in rendered
    assert f"https://app.mytheus.test/decisions/{d}" in rendered
    assert f"https://app.mytheus.test/decisions/{earlier}" in rendered

    again = client.post(
        f"/integrations/exports/decisions/{d}",
        json={"provider": "notion", "parent_page_id": "aaaa1111"},
    ).json()
    assert again["replaced"] is True
    assert first["external_id"] in fakes.notion.archived
    audits = _sql(
        "SELECT metadata FROM audit_logs WHERE action = 'decision.exported' "
        "AND target_id = %(d)s",
        {"d": str(d)},
    )
    assert len(audits) == 2
    listed = client.get(f"/integrations/exports/decisions/{d}").json()["items"]
    assert [(i["provider"], i["external_url"]) for i in listed] == [
        ("notion", again["external_url"])
    ]

    missing = client.post(f"/integrations/exports/decisions/{d}", json={"provider": "notion"})
    assert missing.status_code == 422


def test_export_is_404_for_someone_who_cannot_see_the_decision(client):
    t = admin()
    connect(client, "notion")
    other = seed_user(t)
    m = meeting(other)  # the caller is not a participant
    d = decision(other, m, "Private to that meeting")
    body = {"provider": "notion", "parent_page_id": "aaaa1111"}
    assert client.post(f"/integrations/exports/decisions/{d}", json=body).status_code == 404
    assert client.get(f"/integrations/exports/decisions/{d}").status_code == 404
    # Another tenant's decision is the same 404.
    stranger = admin()
    connect(client, "notion")
    assert client.post(f"/integrations/exports/decisions/{d}", json=body).status_code == 404
    assert stranger.org_id != t.org_id


def test_export_to_confluence_updates_the_same_page_in_the_same_space(client, fakes):
    t = admin()
    connect(client, "confluence")
    m = meeting(t)
    d = decision(t, m, "Freeze the API on Friday")
    body = {"provider": "confluence", "space_key": "ENG"}
    first = client.post(f"/integrations/exports/decisions/{d}", json=body).json()
    assert first["external_id"].startswith("ENG:")
    assert first["external_url"].startswith("https://acme.atlassian.net/wiki/spaces/ENG/")
    assert "Freeze the API on Friday" in fakes.confluence.created[-1]["storage"]
    page_id = first["external_id"].split(":", 1)[1]
    again = client.post(f"/integrations/exports/decisions/{d}", json=body).json()
    assert again["external_id"] == first["external_id"]
    assert fakes.confluence.updated[-1] == page_id
    assert again["replaced"] is True


def test_export_to_slack_dm_and_a_channel_the_app_is_not_in(client, fakes):
    t = admin("me@acme.io")
    connect(client, "slack")
    m = meeting(t)
    d = decision(t, m, "Move standup to 10:00")
    r = client.post(f"/integrations/exports/decisions/{d}", json={"provider": "slack"})
    assert r.status_code == 200, r.text
    assert r.json()["destination"] == "dm"
    assert fakes.slack.posts[-1]["channel"] == "DU1"
    assert "Move standup to 10:00" in str(fakes.slack.posts[-1]["blocks"])

    refused = client.post(
        f"/integrations/exports/decisions/{d}", json={"provider": "slack", "channel": "CNOTIN"}
    )
    assert refused.status_code == 409
    assert refused.json()["reason"] == "slack_not_in_channel"

    set_email(t, "nobody@acme.io")
    unknown = client.post(f"/integrations/exports/decisions/{d}", json={"provider": "slack"})
    assert unknown.status_code == 409
    assert unknown.json()["reason"] == "slack_user_not_found"
    dest = client.get("/integrations/exports/destinations", params={"provider": "slack"}).json()
    assert dest["destinations"] == [
        {"id": "dm", "name": "Direct message to you", "kind": "dm", "url": None}
    ]


def test_export_maps_are_isolated_per_tenant(client):
    a = admin()
    connect(client, "notion")
    d = decision(a, meeting(a), "A's decision")
    client.post(
        f"/integrations/exports/decisions/{d}",
        json={"provider": "notion", "parent_page_id": "aaaa1111"},
    )
    b = admin()
    rows = _sql("SELECT org_id FROM integration_id_map WHERE nexus_id = %(d)s", {"d": str(d)})
    assert {str(r["org_id"]) for r in rows} == {str(a.org_id)}
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('app.current_org', %s, false)", (str(b.org_id),))
        cur.execute("SELECT count(*) FROM integration_id_map WHERE nexus_id = %s", (str(d),))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT set_config('app.current_org', '', false)")
        cur.execute("SELECT count(*) FROM integration_id_map")
        assert cur.fetchone()[0] == 0  # no tenant set: fail closed
    conn.close()
