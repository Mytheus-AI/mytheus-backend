"""MeetingLifecycleWorkflow: plan 10-13 section 3.1, P10 contract section 9.

Runs the real workflow on Temporal's time-skipping test server
(`temporalio.testing.WorkflowEnvironment`), with the real activities calling the real
meeting-service and core-api internal routes in-process (httpx `ASGITransport`), against the
real Postgres as `mytheus_app`. Nothing is mocked except the network hop.

- `created` starts `meetinglifecycle-{meeting_id}` once; a redelivered `created` is a duplicate;
  `ended` signals it; post-processing then close run; a replay after the run finished starts
  nothing (REJECT_DUPLICATE). `updated` is ignored; a bad message is poison.
- `ended` with no prior run starts one already ended (signal-with-start).
- a meeting that never ends is closed after `MAX_OPEN` and its run fails `MeetingNeverEnded`;
  an `ended` that arrives later starts a fresh run that post-processes, exactly once.
- the bound counts from the meeting's scheduled start, so a meeting booked weeks ahead is not
  closed before it happens.
- the run survives a worker restart while it waits for the signal.
- a transient 503 is retried by the RetryPolicy; a 404 (another tenant's meeting) is not.
- the internal routes the activities call refuse a caller without the workflow-service token.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.core_api.routes import internal_lifecycle, internal_meetings
from services.meeting_service import internal as meeting_internal
from services.meeting_service.routes import get_transport
from services.meeting_service.transport import FakeTransport
from services.workflow_service.activities import LifecycleActivities
from services.workflow_service.consumer import PoisonMessage, handle_meeting_event
from services.workflow_service.workflows.meeting_lifecycle import (
    MAX_OPEN,
    NEVER_ENDED,
    MeetingLifecycleWorkflow,
    wait_bound,
    workflow_id_for,
)
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "w" * 40
QUEUE = "lifecycle-test"
CORE = "http://core-api.test"
MEETING = "http://meeting-service.test"


@pytest.fixture(scope="module")
def app(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={SECRET}"
    os.environ[meeting_internal.TOKENS_ENV] = f"workflow-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    application = FastAPI()
    add_exception_handlers(application)
    application.include_router(internal_lifecycle.router)  # core-api
    application.include_router(meeting_internal.router)  # meeting-service
    application.dependency_overrides[get_transport] = lambda: FakeTransport()
    yield application
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    os.environ.pop(meeting_internal.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def _fresh_engine():
    """Each async test runs on its own event loop; an engine cached by an earlier test is bound
    to that loop's connections, so start every test with a new one."""
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


async def wait_for_room(meeting_id: uuid.UUID) -> None:
    """The open activity runs concurrently with the test; wait until it has written the row."""
    for _ in range(200):
        if room(meeting_id) is not None:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"no meeting_rooms row for {meeting_id}")


class FlakyTransport(httpx.AsyncBaseTransport):
    """503 for the first `failures` calls to a path containing `fail_on`, then the real app."""

    def __init__(self, inner: httpx.AsyncBaseTransport, fail_on: str, failures: int) -> None:
        self.inner, self.fail_on, self.left = inner, fail_on, failures
        self.calls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request.url.path)
        if self.fail_on in request.url.path and self.left > 0:
            self.left -= 1
            return httpx.Response(503, json={"title": "busy"})
        return await self.inner.handle_async_request(request)


def activities(app: FastAPI, transport: httpx.AsyncBaseTransport | None = None) -> Any:
    return LifecycleActivities(
        core_url=CORE,
        meeting_url=MEETING,
        core_secret=SECRET,
        meeting_secret=SECRET,
        transport=transport or httpx.ASGITransport(app=app),
    )


def worker(client: Client, acts: LifecycleActivities, **options: Any) -> Worker:
    return Worker(
        client,
        task_queue=QUEUE,
        workflows=[MeetingLifecycleWorkflow],
        activities=acts.all(),
        **options,
    )


def event(org_id: uuid.UUID, meeting_id: uuid.UUID, event_type: str) -> dict[str, Any]:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": f"meeting:{meeting_id}:{event_type}:{uuid.uuid4()}",
            "org_id": str(org_id),
            "schema_version": 1,
            "producer": "core-api",
            "occurred_at": 0,
        },
        "meeting_id": str(meeting_id),
        "event_type": event_type,
        "livekit_room": f"meeting-{meeting_id}",
    }


def room(meeting_id: uuid.UUID) -> dict[str, Any] | None:
    found = _sql(
        "SELECT status, livekit_room FROM meeting_rooms WHERE meeting_id = %(m)s",
        {"m": str(meeting_id)},
    )
    return found[0] if found else None


def end_meeting(meeting_id: uuid.UUID) -> None:
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(meeting_id)})


async def test_created_starts_once_and_ended_runs_post_processing_then_close(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) VALUES (%(m)s,%(u)s,%(o)s)",
        {"m": str(m), "u": str(t.user_id), "o": str(t.org_id)},
    )
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app)):
            created = event(t.org_id, m, "created")
            handle = env.client.get_workflow_handle(workflow_id_for(str(m)))
            assert handle.id == f"meetinglifecycle-{m}"
            assert await handle_meeting_event(env.client, created, task_queue=QUEUE) == "started"
            first_run = (await handle.describe()).run_id
            assert await handle_meeting_event(env.client, created, task_queue=QUEUE) == "duplicate"
            assert (await handle.describe()).run_id == first_run  # still the one execution
            await wait_for_room(m)
            assert (
                await handle_meeting_event(
                    env.client, event(t.org_id, m, "updated"), task_queue=QUEUE
                )
                == "ignored"
            )

            end_meeting(m)
            ended = event(t.org_id, m, "ended")
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "signalled"
            out = await handle.result()

            assert out["room_open"] == {
                "meeting_id": str(m),
                "room": f"meeting-{m}",
                "opened": True,
                "scheduled_start": None,
            }
            assert out["ended"] is True
            digest = out["post_meeting"]
            assert digest["status"] == "ended" and digest["participants"] == 1
            assert out["room_close"] == {"meeting_id": str(m), "changed": True}
            assert room(m) == {"status": "ended", "livekit_room": f"meeting-{m}"}

            # replays after the run closed start nothing
            assert await handle_meeting_event(env.client, created, task_queue=QUEUE) == "duplicate"
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "duplicate"
            assert (await handle.describe()).run_id == first_run


async def test_ended_without_a_run_starts_one_already_ended(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    end_meeting(m)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app)):
            ended = event(t.org_id, m, "ended")
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "signalled"
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert out["ended"] is True and "room_open" not in out
    assert out["post_meeting"]["status"] == "ended"
    assert out["room_close"] == {"meeting_id": str(m), "changed": False}  # never had a room


async def test_a_meeting_that_never_ends_fails_its_run_and_a_late_ended_still_post_processes(
    app,
):
    t = seed_tenant()
    m = seed_meeting(t.org_id)  # no scheduled start: the bound counts from the run's start
    wid = workflow_id_for(str(m))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app)):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            before = await env.get_current_time()
            await wait_for_room(m)
            # A handle from get_workflow_handle is not auto-time-skipping (only start_workflow's
            # is), so skip the 7 days explicitly.
            await env.sleep(MAX_OPEN + timedelta(minutes=1))
            with pytest.raises(WorkflowFailureError) as failed:
                await env.client.get_workflow_handle(wid).result()
            assert await env.get_current_time() - before >= MAX_OPEN
            cause = failed.value.cause
            assert isinstance(cause, ApplicationError) and cause.type == NEVER_ENDED
            assert room(m)["status"] == "ended"  # type: ignore[index]  # closed anyway

            # the meeting does end, later: a fresh run does the post-processing
            end_meeting(m)
            ended = event(t.org_id, m, "ended")
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "signalled"
            out = await env.client.get_workflow_handle(wid).result()
            assert out["ended"] is True and out["post_meeting"]["status"] == "ended"
            assert "room_open" not in out

            # and exactly once: neither a replayed ended nor a replayed created starts another
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "duplicate"
            created = event(t.org_id, m, "created")
            assert await handle_meeting_event(env.client, created, task_queue=QUEUE) == "duplicate"


async def test_the_bound_counts_from_the_scheduled_start_not_from_created(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    _sql(
        "UPDATE meetings SET scheduled_start = now() + interval '20 days' WHERE id = %(m)s",
        {"m": str(m)},
    )
    wid = workflow_id_for(str(m))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app)):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            await wait_for_room(m)
            # well past 7 days after `created`, still 13 days before the meeting even starts
            await env.sleep(MAX_OPEN + timedelta(days=1))
            desc = await env.client.get_workflow_handle(wid).describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING
            assert room(m)["status"] != "ended"  # type: ignore[index]

            end_meeting(m)
            ended = event(t.org_id, m, "ended")
            assert await handle_meeting_event(env.client, ended, task_queue=QUEUE) == "signalled"
            out = await env.client.get_workflow_handle(wid).result()
    assert out["room_open"]["scheduled_start"] is not None
    assert out["ended"] is True and out["post_meeting"]["status"] == "ended"
    assert out["room_close"]["changed"] is True


def test_wait_bound_is_measured_from_the_later_of_now_and_the_scheduled_start():
    now = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
    assert wait_bound(now, None) == MAX_OPEN
    assert wait_bound(now, "garbage") == MAX_OPEN
    assert wait_bound(now, "2026-11-01T09:00:00Z") == MAX_OPEN  # already past
    assert wait_bound(now, "2026-11-30T12:00:00+00:00") == MAX_OPEN + timedelta(days=20)
    assert wait_bound(now, "2026-11-11T12:00:00") == MAX_OPEN + timedelta(days=1)  # naive: UTC


async def test_the_run_survives_a_worker_restart(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        handle = env.client.get_workflow_handle(workflow_id_for(str(m)))
        # No workflow cache, so no sticky queue pinning the run to this worker: whatever runs it
        # next must rebuild it from Temporal history, which is exactly what a restart does.
        async with worker(env.client, activities(app), max_cached_workflows=0):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            await wait_for_room(m)  # the first worker opened it and is parked on the signal
        # first worker is gone; the meeting ends while no worker runs
        end_meeting(m)
        await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
        async with worker(env.client, activities(app)):  # a fresh worker resumes from history
            out = await handle.result()
    assert out["room_open"]["opened"] is True
    assert out["ended"] is True and out["post_meeting"]["status"] == "ended"
    assert room(m)["status"] == "ended"  # type: ignore[index]


async def test_a_transient_failure_is_retried(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    flaky = FlakyTransport(httpx.ASGITransport(app=app), "/room/open", failures=2)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app, flaky)):
            await handle_meeting_event(env.client, event(t.org_id, m, "created"), task_queue=QUEUE)
            await wait_for_room(m)
            end_meeting(m)
            await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert flaky.calls.count(f"/internal/meetings/{m}/room/open") == 3
    assert out["room_open"]["opened"] is True and "room_open_error" not in out


async def test_post_processing_waits_for_core_api_to_agree_the_meeting_ended(app):
    """A replayed `ended` for a meeting core-api still has live: 409 is retried, then given up
    on after the RetryPolicy's attempts; the room is still closed."""
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, activities(app)):
            await handle_meeting_event(env.client, event(t.org_id, m, "ended"), task_queue=QUEUE)
            out = await env.client.get_workflow_handle(workflow_id_for(str(m))).result()
    assert "post_meeting" not in out and "HTTP409" in out["post_meeting_error"]
    assert out["room_close"]["changed"] is False


async def test_activities_call_the_real_routes_and_a_404_is_not_retryable(app):
    t, other = seed_tenant(), seed_tenant()
    m = seed_meeting(t.org_id)
    acts = activities(app)
    env = ActivityEnvironment()
    opened = await env.run(acts.open_meeting_room, str(m), str(t.org_id))
    assert opened == {
        "meeting_id": str(m),
        "room": f"meeting-{m}",
        "opened": True,
        "scheduled_start": None,
    }
    assert room(m) is not None and room(m)["livekit_room"] == f"meeting-{m}"  # type: ignore[index]
    with pytest.raises(ApplicationError) as exc:
        await env.run(acts.open_meeting_room, str(m), str(other.org_id))  # wrong tenant
    assert exc.value.non_retryable and exc.value.type == "HTTP404"
    with pytest.raises(ApplicationError) as live:
        await env.run(acts.run_post_processing, str(m), str(t.org_id))  # not ended yet
    assert not live.value.non_retryable and live.value.type == "HTTP409"
    closed = await env.run(acts.close_meeting_room, str(m), str(t.org_id))
    assert closed == {"meeting_id": str(m), "changed": True}
    end_meeting(m)
    reopened = await env.run(acts.open_meeting_room, str(m), str(t.org_id))
    assert reopened["opened"] is False  # an ended meeting is never re-opened

    unconfigured = LifecycleActivities(
        core_url=CORE, meeting_url=MEETING, core_secret=None, meeting_secret=None
    )
    with pytest.raises(ApplicationError) as missing:
        await env.run(unconfigured.close_meeting_room, str(m), str(t.org_id))
    assert missing.value.non_retryable and missing.value.type == "NotConfigured"


def test_the_internal_routes_require_the_workflow_service_token(app):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    good = {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(t.org_id),
    }
    with TestClient(app) as c:
        for method, path in (
            ("post", f"/internal/meetings/{m}/room/open"),
            ("post", f"/internal/meetings/{m}/room/close"),
            ("get", f"/internal/meetings/{m}/post-meeting"),
        ):
            assert getattr(c, method)(path).status_code == 401
            bad = {**good, "X-Mytheus-Service-Token": "x" * 40}
            assert getattr(c, method)(path, headers=bad).status_code == 401
            other = {**good, "X-Mytheus-Service": "meeting-service"}
            assert getattr(c, method)(path, headers=other).status_code == 401
        assert c.post(f"/internal/meetings/{m}/room/open", headers=good).status_code == 200
        assert c.get(f"/internal/meetings/{m}/post-meeting", headers=good).status_code == 409


async def test_a_message_that_can_never_apply_is_poison():
    t = seed_tenant()
    m = uuid.uuid4()
    for bad in (
        {**event(t.org_id, m, "created"), "event_type": "exploded"},
        {**event(t.org_id, m, "created"), "meeting_id": "nope"},
        {**event(t.org_id, m, "created"), "header": {}},
    ):
        with pytest.raises(PoisonMessage):
            await handle_meeting_event(None, bad, task_queue=QUEUE)  # type: ignore[arg-type]
