"""MeetingLifecycleWorkflow's Phase 4 touches: the follow-up and the CRM meeting note.

The real workflow on the time-skipping test server, with the REAL core-api follow-up dispatch
(`POST /internal/meetings/{id}/follow-up/dispatch`) over a real Postgres, and
integration-service's `POST /internal/crm/meeting-note` faked in-process with the contract shape
(`{meeting_id}` in, `{status}` out) and the workflow-service caller headers checked.

- on `ended`, after the summary: `send_followup` then `send_crm_note`, each once;
- the follow-up writes one `notifications` outbox row per participant and records the result;
- a host who turned the follow-up off gets `{status: disabled}`;
- integration-service down never fails the run (recorded under `crm_note_error`).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from temporalio.testing import WorkflowEnvironment

from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.core_api.routes import followups, internal_lifecycle, internal_meetings
from services.meeting_service import internal as meeting_internal
from services.meeting_service.routes import get_transport
from services.meeting_service.transport import FakeTransport
from services.workflow_service.activities import LifecycleActivities
from services.workflow_service.consumer import handle_meeting_event
from services.workflow_service.workflows.meeting_lifecycle import workflow_id_for
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant
from tests.test_meeting_lifecycle_slack import QUEUE, SECRET, FakeIntegration, worker
from tests.test_meeting_lifecycle_workflow import end_meeting, event

pytestmark = pytest.mark.usefixtures("migrated_db")


class FakeCrm(FakeIntegration):
    def __init__(self, crm_fail: bool = False) -> None:
        super().__init__()
        self.crm_fail = crm_fail

    def mount(self, app: FastAPI) -> None:
        super().mount(app)

        async def handle(request: Request) -> JSONResponse:
            if request.headers.get("x-mytheus-service-token") != SECRET:
                return JSONResponse({"code": "AUTH_REQUIRED"}, status_code=401)
            if self.crm_fail:
                return JSONResponse({"code": "DEPENDENCY_UNAVAILABLE"}, status_code=503)
            body = await request.json()
            self.calls.append(
                ("crm-note", body["meeting_id"], request.headers["x-mytheus-org"], None)
            )
            return JSONResponse({"status": "not_connected"})

        app.add_api_route("/internal/crm/meeting-note", handle, methods=["POST"])


@pytest.fixture(scope="module")
def base_app(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={SECRET}"
    os.environ[meeting_internal.TOKENS_ENV] = f"workflow-service={SECRET}"
    os.environ["SHARE_LINK_SIGNING_KEY"] = "k" * 40
    yield
    for key in (
        internal_meetings.TOKENS_ENV,
        meeting_internal.TOKENS_ENV,
        "SHARE_LINK_SIGNING_KEY",
    ):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous


@pytest.fixture(autouse=True)
def _fresh_engine():
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def build(integration: FakeIntegration) -> LifecycleActivities:
    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(internal_lifecycle.router)
    app.include_router(followups.router)
    app.include_router(meeting_internal.router)
    app.dependency_overrides[get_transport] = lambda: FakeTransport()
    integration.mount(app)
    return LifecycleActivities(
        core_url="http://core-api.test",
        meeting_url="http://meeting-service.test",
        integration_url="http://integration-service.test",
        core_secret=SECRET,
        meeting_secret=SECRET,
        integration_secret=SECRET,
        transport=httpx.ASGITransport(app=app),
    )


def host(meeting_id: uuid.UUID, org_id: uuid.UUID, user_id: uuid.UUID) -> None:
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(meeting_id), "u": str(user_id), "o": str(org_id)},
    )


def followup_rows(meeting_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT payload FROM outbox WHERE topic = 'notifications' AND idempotency_key LIKE %(k)s",
        {"k": f"notif:followup:{meeting_id}:%"},
    )


async def run_to_end(integration: FakeIntegration, org_id: uuid.UUID, m: uuid.UUID) -> Any:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env, build(integration)):
            end_meeting(m)
            await handle_meeting_event(env.client, event(org_id, m, "ended"), task_queue=QUEUE)
            return await env.client.get_workflow_handle(workflow_id_for(str(m))).result()


async def test_ended_sends_the_followup_and_the_crm_note_once(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    host(m, t.org_id, t.user_id)
    integration = FakeCrm()
    out = await run_to_end(integration, t.org_id, m)
    assert out["followup"]["status"] == "sent"
    assert out["followup"]["participants_notified"] == 1
    assert out["followup"]["share_url"].startswith("http")
    assert out["crm_note"] == {"status": "not_connected"}
    assert [c[0] for c in integration.calls] == ["meeting-summary", "crm-note"]
    [row] = followup_rows(m)
    assert row["payload"]["user_id"] == str(t.user_id)
    assert row["payload"]["channel"] == "email"
    stored = _sql(
        "SELECT last_sent_at, last_result FROM meeting_followups WHERE meeting_id = %(m)s",
        {"m": str(m)},
    )[0]
    assert stored["last_sent_at"] is not None and stored["last_result"]["status"] == "sent"


async def test_a_disabled_followup_is_a_no_op(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    host(m, t.org_id, t.user_id)
    _sql(
        "INSERT INTO meeting_followups (meeting_id, org_id, enabled) VALUES (%(m)s, %(o)s, false)",
        {"m": str(m), "o": str(t.org_id)},
    )
    out = await run_to_end(FakeCrm(), t.org_id, m)
    assert out["followup"] == {
        "status": "disabled",
        "sent_at": None,
        "participants_notified": 0,
        "guests": [],
        "email_configured": None,
        "share_url": None,
    }
    assert followup_rows(m) == []


async def test_integration_service_down_never_fails_the_run(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    host(m, t.org_id, t.user_id)
    out = await run_to_end(FakeCrm(crm_fail=True), t.org_id, m)
    assert "HTTP503" in out["crm_note_error"]
    assert out["followup"]["status"] == "sent"
    assert out["post_meeting"]["status"] == "ended"
