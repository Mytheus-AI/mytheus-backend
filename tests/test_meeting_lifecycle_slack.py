"""MeetingLifecycleWorkflow's P12 Slack touches (plan 3.12/3.13 neighbour, P12 contract section 2).

The real workflow on the time-skipping test server with the real meeting-service and core-api
internal routes (as in `test_meeting_lifecycle_workflow.py`). integration-service's
`POST /internal/slack/prep-brief` and `POST /internal/slack/meeting-summary` are not on main yet
(another P12 branch adds them), so they are faked in-process with the contract shape
(`{meeting_id}` in, `{sent: n}` out, `{sent: 0}` when Slack is not connected) and the
workflow-service caller-token headers checked.

- the prep brief fires once at T-15m before the scheduled start (not before), the summary once on
  ended;
- a meeting created inside the 15-minute window gets its brief at once; one with no scheduled
  start gets none; one that ends before T-15m gets none;
- integration-service down does not fail the run;
- the activities send the caller headers and `{meeting_id}`.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.core_api.routes import internal_lifecycle, internal_meetings
from services.meeting_service import internal as meeting_internal
from services.meeting_service.routes import get_transport
from services.meeting_service.transport import FakeTransport
from services.workflow_service.activities import LifecycleActivities
from services.workflow_service.consumer import handle_meeting_event
from services.workflow_service.reminder_activities import ReminderActivities
from services.workflow_service.workflows.meeting_lifecycle import (
    PREP_BRIEF_LEAD,
    MeetingLifecycleWorkflow,
    prep_brief_delay,
    workflow_id_for,
)
from services.workflow_service.workflows.meeting_reminders import MeetingRemindersWorkflow
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant
from tests.test_meeting_lifecycle_workflow import end_meeting, event, wait_for_room

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "s" * 40
QUEUE = "lifecycle-slack-test"


class FakeIntegration:
    def __init__(self, sent: int = 1, fail: bool = False) -> None:
        self.calls: list[tuple[str, str, str, Any]] = []
        self.env: WorkflowEnvironment | None = None
        self.sent, self.fail = sent, fail

    def mount(self, app: FastAPI) -> None:
        for kind in ("prep-brief", "meeting-summary"):
            app.add_api_route(f"/internal/slack/{kind}", self._handler(kind), methods=["POST"])

    def _handler(self, kind: str) -> Any:
        async def handle(request: Request) -> JSONResponse:
            if (
                request.headers.get("x-mytheus-service") != "workflow-service"
                or request.headers.get("x-mytheus-service-token") != SECRET
            ):
                return JSONResponse({"code": "AUTH_REQUIRED"}, status_code=401)
            if self.fail:
                return JSONResponse({"code": "DEPENDENCY_UNAVAILABLE"}, status_code=503)
            body = await request.json()
            at = await self.env.get_current_time() if self.env else None
            self.calls.append((kind, body["meeting_id"], request.headers["x-mytheus-org"], at))
            return JSONResponse({"sent": self.sent})

        return handle


@pytest.fixture(scope="module")
def base_app(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={SECRET}"
    os.environ[meeting_internal.TOKENS_ENV] = f"workflow-service={SECRET}"
    yield
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    os.environ.pop(meeting_internal.TOKENS_ENV, None)
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


def worker(env: WorkflowEnvironment, acts: LifecycleActivities) -> Worker:
    # The consumer also starts MeetingRemindersWorkflow (ADR-0039 B9) on `created`.
    reminders = ReminderActivities(
        core_url="http://core-api.test",
        core_secret=SECRET,
        transport=acts._transport,  # noqa: SLF001
    )
    return Worker(
        env.client,
        task_queue=QUEUE,
        workflows=[MeetingLifecycleWorkflow, MeetingRemindersWorkflow],
        activities=[*acts.all(), *reminders.all()],
    )


def schedule(meeting_id: uuid.UUID, minutes: int) -> None:
    _sql(
        "UPDATE meetings SET scheduled_start = now() + %(m)s * interval '1 minute' "
        "WHERE id = %(id)s",
        {"m": minutes, "id": str(meeting_id)},
    )


def scheduled_start(meeting_id: uuid.UUID) -> Any:
    return _sql("SELECT scheduled_start FROM meetings WHERE id = %(m)s", {"m": str(meeting_id)})[0][
        "scheduled_start"
    ]


async def wait_for_calls(integration: FakeIntegration, n: int, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if len(integration.calls) >= n:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected {n} integration calls, saw {integration.calls}")


async def test_prep_brief_at_t_minus_15m_and_summary_on_ended(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    schedule(m, 120)
    start = scheduled_start(m)
    integration = FakeIntegration()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        integration.env = env
        async with worker(env, build(integration)):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            await wait_for_room(m)
            await env.sleep(
                start - await env.get_current_time() - PREP_BRIEF_LEAD - timedelta(minutes=1)
            )
            await asyncio.sleep(0.3)
            assert integration.calls == []  # not before T-15m
            await env.sleep(timedelta(minutes=2))
            await wait_for_calls(integration, 1)
            kind, meeting, org, at = integration.calls[0]
            assert (kind, meeting, org) == ("prep-brief", str(m), str(t.org_id))
            assert abs(start - PREP_BRIEF_LEAD - at) < timedelta(seconds=5)

            await env.sleep(timedelta(hours=1))
            end_meeting(m)
            await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert [c[0] for c in integration.calls] == ["prep-brief", "meeting-summary"]
    assert out["prep_brief"] == {"sent": 1} and out["meeting_summary"] == {"sent": 1}
    assert out["post_meeting"]["status"] == "ended" and out["room_close"]["changed"] is True


async def test_inside_the_window_the_brief_goes_at_once_and_slack_off_is_a_no_op(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    schedule(m, 5)
    integration = FakeIntegration(sent=0)  # the tenant has no Slack: integration-service no-ops
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env, build(integration)):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            await wait_for_calls(integration, 1)
            end_meeting(m)
            await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert out["prep_brief"] == {"sent": 0} and out["meeting_summary"] == {"sent": 0}


async def test_no_scheduled_start_or_ending_early_means_no_brief(base_app):
    t = seed_tenant()
    unscheduled, early = seed_meeting(t.org_id), seed_meeting(t.org_id)
    schedule(early, 24 * 60)
    integration = FakeIntegration()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env, build(integration)):
            for m in (unscheduled, early):
                await handle_meeting_event(
                    env.client, event(t.org_id, m, "created"), task_queue=QUEUE
                )
                await wait_for_room(m)
                end_meeting(m)  # ends long before its T-15m
                await handle_meeting_event(
                    env.client, event(t.org_id, m, "ended"), task_queue=QUEUE
                )
                out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
                assert "prep_brief" not in out and out["meeting_summary"] == {"sent": 1}
    assert [(c[0], c[1]) for c in integration.calls] == [
        ("meeting-summary", str(unscheduled)),
        ("meeting-summary", str(early)),
    ]


async def test_integration_service_down_never_fails_the_run(base_app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    end_meeting(m)
    integration = FakeIntegration(fail=True)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env, build(integration)):
            await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert "HTTP503" in out["meeting_summary_error"]
    assert out["post_meeting"]["status"] == "ended"


def test_prep_brief_delay_is_pure_and_bounded():
    from datetime import UTC, datetime

    now = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
    assert prep_brief_delay(now, None) is None
    assert prep_brief_delay(now, "garbage") is None
    assert prep_brief_delay(now, "2026-11-10T11:00:00Z") is None  # already started
    assert prep_brief_delay(now, "2026-11-10T12:05:00+00:00") == timedelta(0)
    assert prep_brief_delay(now, "2026-11-10T14:00:00") == timedelta(hours=1, minutes=45)
