"""Linear connector (ADR-0044 D2, D3): confirmed actions become Linear issues, status comes back.

Real Postgres (as `mytheus_app`), fakes for Linear and core-api. Mirrors the Jira cases in
`test_integrations_sync.py`.
"""

# ruff: noqa: F811 - pytest fixtures are imported and then requested by name.
from __future__ import annotations

import uuid
from typing import Any

import pytest

from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import TokenGrant
from services.integration_service.connectors import linear
from tests import _integration_fakes as fx
from tests.test_chat_api import Tenant, seed_meeting, seed_tenant
from tests.test_integrations_sync import _env_and_engine, action_value, factory  # noqa: F401
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")


async def connect_linear(factory: Any, t: Tenant, account: str | None = None) -> store.Connection:
    account = account or f"lin-org-{uuid.uuid4().hex[:8]}"
    access, refresh = fx.SECRET_TOKENS["linear"]
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        conn = await store.upsert_connection(
            session,
            org_id=t.org_id,
            provider=config.LINEAR,
            external_account_id=account,
            scopes=["read", "write", "issues:create"],
            installed_by=t.user_id,
        )
        await store.store_tokens(
            session, conn, TokenGrant(access, refresh, 3600, ["read"], account)
        )
        await store.put_cursor(
            session,
            t.org_id,
            conn.id,
            linear.TEAM_KIND,
            {"team_id": "team-1", "team_key": "ENG", "url_key": "acme"},
            None,
        )
    return conn


async def push_linear(factory: Any, t: Tenant, value: dict[str, Any], clients: Any) -> str:
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        return await linear.push_action(session, t.org_id, value, clients)


async def inbound(factory: Any, t: Tenant, payload: dict[str, Any], clients: Any, core: Any) -> str:
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        return await linear.process_webhook(session, t.org_id, payload, core, clients)


def update(identifier: str, state_type: str = "completed") -> dict[str, Any]:
    return {
        "type": "Issue",
        "action": "update",
        "organizationId": "ignored-here",
        "data": {"id": "x", "identifier": identifier, "state": {"type": state_type}},
    }


async def test_confirmed_action_creates_one_issue_with_back_link_and_attachment(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    aid = uuid.uuid4()
    assert await push_linear(factory, t, action_value(t, aid, meeting), clients) == "created"
    created = clients.linear.created
    assert len(created) == 1
    assert created[0]["team_id"] == "team-1"
    assert created[0]["title"] == "Send the revised budget to finance"
    assert created[0]["due_date"] == "2025-11-12"
    assert f"https://app.mytheus.test/meetings/{meeting}" in created[0]["description"]
    assert clients.linear.attachments[linear.action_url(aid)] == "ENG-1"
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        link = await store.link_for_nexus(session, config.LINEAR, "action", aid)
    assert link is not None and link.external_id == "ENG-1"
    assert link.last_synced_status == "confirmed"


async def test_redelivered_action_is_exists_not_a_second_issue(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    aid = uuid.uuid4()
    await push_linear(factory, t, action_value(t, aid, meeting), clients)
    assert await push_linear(factory, t, action_value(t, aid, meeting), clients) == "exists"
    assert len(clients.linear.created) == 1


async def test_crash_window_links_issue_found_by_attachment_url(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    aid = uuid.uuid4()
    # The issue exists in Linear (created before a crash) but no id-map row was committed.
    issue = await clients.linear.create_issue(
        "t", team_id="team-1", title="x", description="", due_date=None, assignee_id=None
    )
    await clients.linear.attach_url("t", issue_id=issue["id"], url=linear.action_url(aid), title="")
    assert await push_linear(factory, t, action_value(t, aid, meeting), clients) == "linked"
    assert len(clients.linear.created) == 1  # only the pre-crash one


async def test_unknown_assignee_email_creates_unassigned_issue(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    stranger = seed_user(t)  # an email Linear does not know
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    value = action_value(t, uuid.uuid4(), meeting, assignee_id=str(stranger.user_id))
    assert await push_linear(factory, t, value, clients) == "created"
    assert clients.linear.created[0]["assignee_id"] is None


async def test_no_linear_connection_means_no_issue(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    clients = fx.fake_clients()
    value = action_value(t, uuid.uuid4(), meeting)
    assert await push_linear(factory, t, value, clients) == "not_connected"
    assert clients.linear.created == []


async def test_webhook_for_issue_not_in_id_map_is_skipped(factory):
    t = seed_tenant("owner")
    await connect_linear(factory, t)
    clients, core = fx.fake_clients(), fx.FakeCore()
    await clients.linear.create_issue(
        "t", team_id="team-1", title="stray", description="", due_date=None, assignee_id=None
    )
    assert await inbound(factory, t, update("ENG-1"), clients, core) == "skipped"
    assert core.status_calls == []


async def test_webhook_takes_status_from_reread_not_payload(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients, core = fx.fake_clients(), fx.FakeCore()
    await push_linear(factory, t, action_value(t, uuid.uuid4(), meeting), clients)
    clients.linear.set_state("ENG-1", "started", "In Progress")
    # The body claims completed; Linear says started.
    assert await inbound(factory, t, update("ENG-1", "completed"), clients, core) == "skipped"
    assert core.status_calls == []


async def test_completed_maps_done_and_canceled_maps_confirmed(factory):
    assert linear.map_status({"state": {"type": "completed"}}) == "done"
    assert linear.map_status({"state": {"type": "canceled"}}) == "confirmed"
    assert linear.map_status({"state": {"type": "started"}}) == "confirmed"
    assert linear.map_status({}) == "confirmed"


async def test_done_in_linear_reaches_core_once_then_echo_is_suppressed(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients, core = fx.fake_clients(), fx.FakeCore()
    aid = uuid.uuid4()
    await push_linear(factory, t, action_value(t, aid, meeting), clients)
    clients.linear.set_state("ENG-1", "completed")
    assert await inbound(factory, t, update("ENG-1"), clients, core) == "processed"
    assert core.status_calls == [(t.org_id, aid, "done", "ENG-1")]
    assert core.sources == ["linear"]
    # The echo of a status Mytheus already has.
    assert await inbound(factory, t, update("ENG-1"), clients, core) == "skipped"
    assert len(core.status_calls) == 1
