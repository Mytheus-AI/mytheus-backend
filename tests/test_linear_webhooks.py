"""Linear wired in (ADR-0044 D1, D3): the webhook door, the connect flow, one tracker at a time.

Real Postgres, the integration-service app under TestClient, fakes for every provider.
"""

# ruff: noqa: F811 - pytest fixtures are imported and then requested by name.
from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid

import fakeredis.aioredis
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared import db as db_mod
from services.integration_service.consumer import handle_action
from tests import _integration_fakes as fx
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant
from tests.test_integrations_api import (  # noqa: F401 - fixtures
    _connect_row,
    _inbox,
    admin_tenant,
    client,
    connect_and_callback,
    fakes,
    ok,
)
from tests.test_integrations_sync import action_value

pytestmark = pytest.mark.usefixtures("migrated_db")


def signed(body: bytes, secret: str = fx.LINEAR_WEBHOOK, delivery: str = "d-1") -> dict[str, str]:
    return {
        "Linear-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
        "Linear-Delivery": delivery,
        "Content-Type": "application/json",
    }


def issue_update(org: str, *, ts_ms: int | None = None) -> bytes:
    return json.dumps(
        {
            "type": "Issue",
            "action": "update",
            "organizationId": org,
            "webhookTimestamp": ts_ms if ts_ms is not None else int(time.time() * 1000),
            "data": {"id": "i1", "identifier": "ENG-1", "state": {"type": "completed"}},
        }
    ).encode()


def test_valid_signature_writes_one_inbox_row_and_a_replay_is_a_noop(client):
    t = seed_tenant()
    org = f"lin-org-{uuid.uuid4().hex[:8]}"
    _connect_row(t, "linear", org)
    body = issue_update(org)
    first = client.post("/webhooks/linear", content=body, headers=signed(body))
    assert first.json() == {"status": "accepted"}
    again = client.post("/webhooks/linear", content=body, headers=signed(body))
    assert again.status_code == 200 and again.json() == {"status": "duplicate"}
    rows = _inbox(t.org_id, "linear")
    assert len(rows) == 1 and rows[0]["event_type"] == "Issue:update"


def test_bad_signature_is_401(client):
    t = seed_tenant()
    org = f"lin-org-{uuid.uuid4().hex[:8]}"
    _connect_row(t, "linear", org)
    body = issue_update(org)
    forged = signed(body, secret="not-the-secret")
    assert client.post("/webhooks/linear", content=body, headers=forged).status_code == 401
    unsigned = {"Linear-Delivery": "x", "Content-Type": "application/json"}
    assert client.post("/webhooks/linear", content=body, headers=unsigned).status_code == 401
    assert _inbox(t.org_id, "linear") == []


def test_stale_timestamp_is_401(client):
    t = seed_tenant()
    org = f"lin-org-{uuid.uuid4().hex[:8]}"
    _connect_row(t, "linear", org)
    body = issue_update(org, ts_ms=int((time.time() - 120) * 1000))
    assert client.post("/webhooks/linear", content=body, headers=signed(body)).status_code == 401


def test_unknown_organization_is_ignored(client):
    body = issue_update("lin-org-nobody")
    r = client.post("/webhooks/linear", content=body, headers=signed(body, delivery="d-x"))
    assert r.status_code == 200 and r.json() == {"status": "ignored"}


def test_connect_linear_records_its_team(client, fakes):
    t = admin_tenant()
    ok(connect_and_callback(client, "linear"), "linear")
    rows = _sql(
        "SELECT cursor FROM integration_cursors WHERE org_id = %(o)s AND kind = 'linear_team'",
        {"o": str(t.org_id)},
    )
    assert rows and rows[0]["cursor"]["team_id"] == "team-1"


def test_connect_linear_while_jira_connected_is_409(client):
    t = admin_tenant()
    _connect_row(t, "jira", f"cloud-{uuid.uuid4().hex[:8]}")
    r = client.post("/integrations/linear/connect")
    assert r.status_code == 409
    assert r.json()["code"] == "PM_ALREADY_CONNECTED"


def test_linear_is_listed_as_connectable(client):
    admin_tenant()
    listed = {i["provider"]: i for i in client.get("/integrations").json()["items"]}
    assert listed["linear"]["configured"] is True


def test_disconnect_revokes_linear_token(client, fakes):
    admin_tenant()
    ok(connect_and_callback(client, "linear"), "linear")
    assert client.delete("/integrations/linear").status_code == 204
    assert fakes.oauth["linear"].revoked[-1] == "lin-REFRESH-secret-token"


@pytest.fixture
async def factory():
    engine = create_async_engine(APP_URL)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def test_after_switching_trackers_action_goes_only_to_linear(client, factory):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    jira_conn = _connect_row(t, "jira", f"cloud-{uuid.uuid4().hex[:8]}")
    _sql(
        "UPDATE integration_connections SET status = 'disconnected' WHERE id = %(c)s",
        {"c": str(jira_conn)},
    )
    from tests.test_linear_sync import connect_linear

    await connect_linear(factory, t)
    clients = fx.fake_clients()
    async with factory() as session, session.begin():
        outcome = await handle_action(
            session,
            fakeredis.aioredis.FakeRedis(),
            action_value(t, uuid.uuid4(), meeting),
            clients=clients,
        )
    assert outcome == "created"
    assert len(clients.linear.created) == 1
    assert clients.jira.created == []


async def test_inbox_row_is_dispatched_to_the_linear_connector(client, factory):
    from services.integration_service import worker
    from tests.test_linear_sync import connect_linear, push_linear

    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    conn = await connect_linear(factory, t)
    clients, core = fx.fake_clients(), fx.FakeCore()
    aid = uuid.uuid4()
    await push_linear(factory, t, action_value(t, aid, meeting), clients)
    clients.linear.set_state("ENG-1", "completed")
    body = issue_update(str(conn.external_account_id))
    assert client.post(
        "/webhooks/linear", content=body, headers=signed(body, delivery="d-w")
    ).json() == {"status": "accepted"}
    row = _sql(
        "SELECT id FROM integration_webhook_inbox WHERE org_id = %(o)s AND provider = 'linear'",
        {"o": str(t.org_id)},
    )[0]
    # The TestClient ran on its own event loop; drop the engine it cached before awaiting here.
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    assert await worker.process_row(t.org_id, row["id"], clients=clients, core=core) == "processed"
    assert core.status_calls == [(t.org_id, aid, "done", "ENG-1")]
