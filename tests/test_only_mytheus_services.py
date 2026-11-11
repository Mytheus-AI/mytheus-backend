"""Polish wave 3 outside core-api: integration-service's Jira issue status, notification-service's
user email for the digest, and workflow-service's scheduled activities (real Postgres for the
first two, an in-process transport for the third)."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import date
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from services.integration_service import internal_auth as integ_auth
from services.integration_service import jira_status
from services.notification_service import internal as notif_internal
from services.notification_service import user_email
from services.notification_service.internal_auth import TOKENS_ENV as NOTIF_TOKENS_ENV
from services.workflow_service.scheduled_activities import ScheduledActivities
from services.workflow_service.workflows import only_mytheus
from tests.conftest import APP_URL
from tests.test_chat_api import _create_test_app, _sql, seed_tenant
from tests.test_email_provider import FakeSender

SECRET = "s" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[NOTIF_TOKENS_ENV] = f"core-api={SECRET}"
    os.environ[integ_auth.TOKENS_ENV] = f"workflow-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("only-mytheus-services-test")
    app.include_router(user_email.router)
    app.include_router(jira_status.router)
    with TestClient(app) as c:
        yield c
    notif_internal.set_sender(None, active=False)
    for key in (NOTIF_TOKENS_ENV, integ_auth.TOKENS_ENV):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def headers(service: str, org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def test_jira_status_is_not_connected_without_jira(client):
    t = seed_tenant()
    res = client.post(
        "/internal/jira/issue-status",
        json={"action_ids": [str(uuid.uuid4())]},
        headers=headers("workflow-service", t.org_id),
    )
    assert res.status_code == 200, res.text
    assert res.json() == {"connected": False, "issues": []}
    assert (
        client.post(
            "/internal/jira/issue-status",
            json={"action_ids": []},
            headers=headers("notification-service", t.org_id),
        ).status_code
        == 401
    )


def test_jira_status_falls_back_to_the_watermark(client):
    t = seed_tenant()
    _sql(
        "INSERT INTO integration_connections (org_id, provider, external_account_id, "
        "installed_by) VALUES (%(o)s, 'jira', %(c)s, %(u)s)",
        {"o": str(t.org_id), "u": str(t.user_id), "c": f"cloud-{t.org_id}"},
    )
    open_id, done_id, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    for nexus, key, status in ((open_id, "WEB-1", "confirmed"), (done_id, "WEB-2", "done")):
        _sql(
            "INSERT INTO integration_id_map (org_id, provider, nexus_kind, nexus_id, "
            "external_id, external_url, last_synced_status) VALUES (%(o)s, 'jira', 'action', "
            "%(n)s, %(k)s, %(url)s, %(s)s)",
            {"o": str(t.org_id), "n": str(nexus), "k": key, "url": f"https://j/{key}", "s": status},
        )
    res = client.post(
        "/internal/jira/issue-status",
        json={"action_ids": [str(open_id), str(done_id), str(other)]},
        headers=headers("workflow-service", t.org_id),
    ).json()
    assert res["connected"] is True
    got = {i["issue_key"]: (i["open"], i["source"]) for i in res["issues"]}
    assert got == {"WEB-1": (True, "watermark"), "WEB-2": (False, "watermark")}


def test_digest_email_to_a_user(client):
    t = seed_tenant()
    body = {
        "kind": "weekly_digest",
        "user_id": str(t.user_id),
        "idempotency_key": "weekly-digest:2026-11-02",
        "subject": "What changed for you",
        "text": "Decisions: ...",
        "html": "<p>Decisions</p>",
    }
    notif_internal.set_sender(None)
    res = client.post("/internal/email/user", json=body, headers=headers("core-api", t.org_id))
    assert res.status_code == 200, res.text
    assert res.json()["configured"] is False
    assert res.json()["results"][0]["status"] == "skipped"
    # A provider configured later does not resend the recorded week.
    fake = FakeSender()
    notif_internal.set_sender(fake)
    again = client.post("/internal/email/user", json=body, headers=headers("core-api", t.org_id))
    assert again.json()["results"][0]["replayed"] is True and fake.sent == []
    fresh = {**body, "idempotency_key": "weekly-digest:2026-11-09"}
    sent = client.post("/internal/email/user", json=fresh, headers=headers("core-api", t.org_id))
    assert sent.json()["results"][0]["status"] == "sent"
    assert [s[0] for s in fake.sent] == ["u@example.com"]
    other = seed_tenant()
    assert (
        client.post(
            "/internal/email/user", json=fresh, headers=headers("core-api", other.org_id)
        ).status_code
        == 404
    )


def test_drift_activity_wires_core_and_jira():
    calls: list[tuple[str, str, Any]] = []
    action = str(uuid.uuid4())

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if request.url.path == "/internal/promise-drift/due":
            return httpx.Response(
                200, json={"items": [{"action_id": action, "due_date": "2026-11-06"}]}
            )
        if request.url.path == "/internal/jira/issue-status":
            return httpx.Response(
                200,
                json={
                    "connected": True,
                    "issues": [
                        {
                            "action_id": action,
                            "issue_key": "WEB-1",
                            "issue_url": None,
                            "open": True,
                            "source": "jira",
                        }
                    ],
                },
            )
        return httpx.Response(200, json={"owner": 1, "host": 0})

    acts = ScheduledActivities(
        core_url="http://core",
        core_secret=SECRET,
        integration_url="http://integ",
        integration_secret=SECRET,
        transport=httpx.MockTransport(handler),
    )
    org = str(uuid.uuid4())
    result = asyncio.run(acts.drift_for_tenant(org, date(2026, 11, 11)))
    assert result == {"due": 1, "open": 1, "owner": 1, "host": 0, "jira": 1}
    assert calls[2][2] == {
        "as_of": "2026-11-11",
        "issues": [{"action_id": action, "issue_key": "WEB-1", "issue_url": None}],
    }


def test_drift_activity_skips_a_tenant_without_jira():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/promise-drift/due":
            return httpx.Response(
                200, json={"items": [{"action_id": str(uuid.uuid4()), "due_date": "2026-11-06"}]}
            )
        if request.url.path == "/internal/jira/issue-status":
            return httpx.Response(200, json={"connected": False, "issues": []})
        raise AssertionError("no nudge call without Jira")

    acts = ScheduledActivities(
        core_url="http://core",
        core_secret=SECRET,
        integration_url="http://integ",
        integration_secret=SECRET,
        transport=httpx.MockTransport(handler),
    )
    assert asyncio.run(acts.drift_for_tenant(str(uuid.uuid4()), date(2026, 11, 11)))["jira"] == 0


def test_schedules_are_created_once():
    class FakeClient:
        def __init__(self) -> None:
            self.ids: list[str] = []

        async def create_schedule(self, schedule_id: str, schedule: Any) -> None:
            from temporalio.client import ScheduleAlreadyRunningError

            if schedule_id in self.ids:
                raise ScheduleAlreadyRunningError()
            self.ids.append(schedule_id)

    fake = FakeClient()
    first = asyncio.run(only_mytheus.ensure_schedules(fake, task_queue="q"))
    assert first == ["promise-drift-daily", "weekly-digest"]
    assert asyncio.run(only_mytheus.ensure_schedules(fake, task_queue="q")) == []
    assert only_mytheus.WEEKLY_DIGEST_CRON == "0 7 * * 1"


async def test_a_delegated_answer_in_a_context_fill_is_logged_once(migrated_db):
    from tests.test_agent_collaborations import entry, fill_for, fill_result
    from tests.test_chat_api import add_participant, seed_meeting
    from tests.test_my_tasks import add_user
    from tests.test_worklogs import apply

    t = seed_tenant()
    sarah = add_user(t)
    m = seed_meeting(t.org_id)
    add_participant(m, t.org_id, t.user_id)
    add_participant(m, t.org_id, sarah.user_id)
    _sql(
        "UPDATE meeting_participants SET attendance = 'agent' WHERE meeting_id = %(m)s "
        "AND user_id = %(u)s",
        {"m": str(m), "u": str(sarah.user_id)},
    )
    answered = entry(t.user_id)
    value = fill_result(t, m, [answered], fill_for(answered))
    payload = json.loads(value["payload_json"])
    answer_id = str(uuid.uuid4())
    payload["delegated"] = {
        "id": answer_id,
        "delegator_user_id": str(sarah.user_id),
        "statement_id": None,
        "statement_text": "Either vendor under $40k is fine",
        "question": "Is vendor B at $35k OK for Sarah?",
        "answer": "Yes, Sarah pre-approved either vendor under $40k.",
        "covered": True,
        "asked_by": str(t.user_id),
    }
    value["payload_json"] = json.dumps(payload)
    assert await apply(value) is True
    logged = _sql("SELECT * FROM delegated_answer_log WHERE id = %(i)s", {"i": answer_id})
    assert len(logged) == 1 and logged[0]["covered"] is True
    assert logged[0]["statement_text"] == "Either vendor under $40k is fine"
