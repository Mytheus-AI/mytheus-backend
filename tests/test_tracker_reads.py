"""Issue status and watch sources for whichever tracker the tenant uses (ADR-0044 D2).

Real Postgres, fakes for Jira and Linear.
"""

# ruff: noqa: F811 - pytest fixtures are imported and then requested by name.
from __future__ import annotations

import uuid

import pytest

from services.integration_service import jira_status, watch_sources
from tests import _integration_fakes as fx
from tests.test_chat_api import seed_meeting, seed_tenant
from tests.test_integrations_sync import _env_and_engine, action_value, factory  # noqa: F401
from tests.test_linear_sync import connect_linear, push_linear

pytestmark = pytest.mark.usefixtures("migrated_db")


async def test_issue_status_reads_linear_when_linear_is_the_tracker(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    aid = uuid.uuid4()
    await push_linear(factory, t, action_value(t, aid, meeting), clients)
    clients.linear.set_state("ENG-1", "completed")
    result = await jira_status.issue_statuses(t.org_id, [aid], clients)
    assert result.connected is True
    assert result.provider == "linear"
    assert [(i.issue_key, i.open, i.source) for i in result.issues] == [("ENG-1", False, "linear")]
    assert result.issues[0].issue_url == "https://linear.app/acme/issue/ENG-1"


async def test_issue_status_falls_back_to_watermark_when_linear_unreachable(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    aid = uuid.uuid4()
    await push_linear(factory, t, action_value(t, aid, meeting), clients)
    clients.linear.fail_reads = True
    result = await jira_status.issue_statuses(t.org_id, [aid], clients)
    assert [(i.open, i.source) for i in result.issues] == [(True, "watermark")]


async def test_issue_status_says_not_connected_without_a_tracker(factory):
    t = seed_tenant("owner")
    result = await jira_status.issue_statuses(t.org_id, [uuid.uuid4()], fx.fake_clients())
    assert result.connected is False and result.provider is None and result.issues == []


async def test_watch_sources_reads_keys_from_linear(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    await push_linear(factory, t, action_value(t, uuid.uuid4(), meeting), clients)
    body = watch_sources.SourcesRequest(jira_keys=["ENG-1", "ENG-404"])
    found = await watch_sources.watch_sources(t.org_id, body, clients)
    assert found.jira.connected is False
    assert found.linear.connected is True
    assert [(i.key, i.provider, i.status_category) for i in found.linear.issues] == [
        ("ENG-1", "linear", "new")
    ]
    assert found.linear.issues[0].due_date is not None


async def test_watch_sources_linear_completed_reads_done(factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    await connect_linear(factory, t)
    clients = fx.fake_clients()
    await push_linear(factory, t, action_value(t, uuid.uuid4(), meeting), clients)
    clients.linear.set_state("ENG-1", "completed")
    found = await watch_sources.watch_sources(
        t.org_id, watch_sources.SourcesRequest(jira_keys=["ENG-1"]), clients
    )
    assert found.linear.issues[0].status_category == "done"
    assert found.linear.issues[0].status == "Done"
