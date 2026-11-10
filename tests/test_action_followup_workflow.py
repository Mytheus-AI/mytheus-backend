"""ActionFollowUpWorkflow: plan 10-13 section 3.12, P12 contract section 2.

The real workflow on Temporal's time-skipping test server, the real activities, the real
Postgres (as `mytheus_app`) for the escalation reads and the `notifications` outbox. core-api's
`GET /internal/actions/{id}/status` is not on main yet (another P12 branch adds it), so it is
faked here in-process with the contract shape `{status, assignee_id, meeting_id, organizer_id,
due_date}` and the real caller-token headers checked.

- reminders at day 1, 3, 7 and the escalation at day 10, each step reading the status first;
- `done` / `dismissed` completes early; a 404 (action gone) completes too;
- the `actions` consumer starts `actionfollowup-{action_id}` once (REJECT_DUPLICATE);
- a worker restart mid-timer neither drops nor duplicates a reminder, and a re-run activity
  enqueues nothing new;
- the escalation target falls back department admin -> meeting organizer -> org admin -> none.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from temporalio.client import Client
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from libs.shared import db as db_mod
from services.workflow_service.actions_consumer import PoisonAction, handle_action_event
from services.workflow_service.followup_activities import FollowUpActivities
from services.workflow_service.workflows.action_followup import (
    ActionFollowUpWorkflow,
    workflow_id_for,
)
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "f" * 40
QUEUE = "followup-test"
CORE = "http://core-api.test"


class FakeCore:
    """core-api's `GET /internal/actions/{id}/status`, with the workflow-service token check."""

    def __init__(self) -> None:
        self.status: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, Any]] = []
        self.env: WorkflowEnvironment | None = None
        self.app = FastAPI()

        @self.app.get("/internal/actions/{action_id}/status")
        async def status(action_id: str, request: Request) -> JSONResponse:
            ok = (
                request.headers.get("x-mytheus-service") == "workflow-service"
                and request.headers.get("x-mytheus-service-token") == SECRET
            )
            if not ok:
                return JSONResponse({"code": "AUTH_REQUIRED"}, status_code=401)
            at = await self.env.get_current_time() if self.env else None
            self.calls.append((action_id, at))
            found = self.status.get(action_id)
            if found is None or found["org_id"] != request.headers.get("x-mytheus-org"):
                return JSONResponse({"code": "NOT_FOUND"}, status_code=404)
            return JSONResponse({k: v for k, v in found.items() if k != "org_id"})


@pytest.fixture(autouse=True)
def _app_db():
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def core() -> FakeCore:
    return FakeCore()


def acts(core: FakeCore) -> FollowUpActivities:
    return FollowUpActivities(
        core_url=CORE, core_secret=SECRET, transport=httpx.ASGITransport(app=core.app)
    )


def worker(client: Client, core: FakeCore, **options: Any) -> Worker:
    return Worker(
        client,
        task_queue=QUEUE,
        workflows=[ActionFollowUpWorkflow],
        activities=acts(core).all(),
        **options,
    )


def add_user(org_id: uuid.UUID, tier: str | None) -> uuid.UUID:
    user_id = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s,%(o)s,%(c)s,%(e)s)",
        {"u": str(user_id), "o": str(org_id), "c": f"user_{user_id.hex}", "e": "x@example.com"},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s,%(u)s,%(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return user_id


def team(org_id: uuid.UUID, department_id: uuid.UUID | None, *members: uuid.UUID) -> uuid.UUID:
    team_id = uuid.uuid4()
    _sql(
        "INSERT INTO teams (id, org_id, name, department_id) VALUES (%(t)s,%(o)s,'T',%(d)s)",
        {"t": str(team_id), "o": str(org_id), "d": str(department_id) if department_id else None},
    )
    for member in members:
        _sql(
            "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s,%(u)s,%(o)s)",
            {"t": str(team_id), "u": str(member), "o": str(org_id)},
        )
    return team_id


def department(org_id: uuid.UUID) -> uuid.UUID:
    dept = uuid.uuid4()
    _sql(
        "INSERT INTO departments (id, org_id, name) VALUES (%(d)s,%(o)s,'D')",
        {"d": str(dept), "o": str(org_id)},
    )
    return dept


class Scenario:
    """A tenant, an assignee (member), an organizer (member) and one org admin."""

    def __init__(self, core: FakeCore) -> None:
        tenant = seed_tenant("member")
        self.org_id = tenant.org_id
        self.assignee = add_user(self.org_id, "member")
        self.organizer = add_user(self.org_id, "member")
        self.meeting = seed_meeting(self.org_id)
        self.action_id = uuid.uuid4()
        core.status[str(self.action_id)] = {
            "org_id": str(self.org_id),
            "status": "confirmed",
            "assignee_id": str(self.assignee),
            "meeting_id": str(self.meeting),
            "organizer_id": str(self.organizer),
            "due_date": "2026-11-20",
        }
        self.core = core

    def set(self, **fields: Any) -> None:
        self.core.status[str(self.action_id)].update(fields)

    def event(self) -> dict[str, Any]:
        return {
            "header": {
                "event_id": str(uuid.uuid4()),
                "idempotency_key": f"action:{self.action_id}:confirmed",
                "org_id": str(self.org_id),
                "schema_version": 1,
                "producer": "core-api",
                "occurred_at": 0,
            },
            "action_id": str(self.action_id),
            "meeting_id": str(self.meeting),
            "content": "Send the  revised budget\nto finance",
            "assignee_id": str(self.assignee),
            "due_date": (date(2026, 11, 20) - date(1970, 1, 1)).days,
            "confidence": 0.9,
            "status": "confirmed",
        }

    def notifications(self) -> list[dict[str, Any]]:
        rows = _sql(
            "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
            "AND topic = 'notifications' AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"o": str(self.org_id), "p": f"actionfollowup:{self.action_id}:%"},
        )
        return [r["payload"] | {"_key": r["idempotency_key"]} for r in rows]

    def sources(self) -> list[str]:
        return [n["source_event_id"].rsplit(":", 1)[1] for n in self.notifications()]


async def wait_for_rows(scenario: Scenario, n: int, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if len(scenario.notifications()) >= n:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected {n} notification rows, have {len(scenario.notifications())}")


async def test_reminders_on_day_1_3_7_and_escalation_on_day_10(core):
    s = Scenario(core)
    manager = add_user(s.org_id, "admin")
    dept = department(s.org_id)
    team(s.org_id, dept, s.assignee)
    team(s.org_id, dept, manager)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        core.env = env
        async with worker(env.client, core):
            started = await env.get_current_time()
            assert await handle_action_event(env.client, s.event(), task_queue=QUEUE) == "started"
            await env.sleep(timedelta(days=10, hours=1))
            out = await env.client.get_workflow_handle(workflow_id_for(str(s.action_id))).result()

    days = [round((at - started) / timedelta(days=1), 2) for _, at in core.calls]
    assert days == [1, 3, 7, 10]
    assert out["completed"] == "escalated"
    assert [(st["day"], st["kind"]) for st in out["steps"]] == [
        (1, "remind"),
        (3, "remind"),
        (7, "remind"),
        (10, "escalate"),
    ]
    assert out["steps"][-1]["reason"] == "department_admin"

    notes = s.notifications()
    assert s.sources() == ["day1", "day1", "day3", "day3", "day7", "day7"] + ["escalation"] * 2
    assert [n["channel"] for n in notes] == ["in_app", "slack"] * 4
    reminders, escalation = notes[:6], notes[6:]
    assert {n["user_id"] for n in reminders} == {str(s.assignee)}
    assert {n["user_id"] for n in escalation} == {str(manager)}
    assert reminders[0]["title"] == "Reminder: Send the revised budget to finance"
    assert reminders[0]["link"] == f"/meetings/{s.meeting}" and reminders[0]["priority"] == 2
    assert escalation[0]["title"].startswith("Overdue: ") and escalation[0]["priority"] == 1
    assert notes[0]["header"]["producer"] == "workflow-service"
    assert notes[0]["_key"] == f"notif:actionfollowup:{s.action_id}:day1:in_app:{s.assignee}"


@pytest.mark.parametrize("terminal", ["done", "dismissed"])
async def test_done_or_dismissed_completes_early(core, terminal):
    s = Scenario(core)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, core):
            await handle_action_event(env.client, s.event(), task_queue=QUEUE)
            handle = env.client.get_workflow_handle(workflow_id_for(str(s.action_id)))
            await env.sleep(timedelta(days=1, hours=1))
            await wait_for_rows(s, 2)
            s.set(status=terminal)  # marked done in the UI or by the Jira sync
            await env.sleep(timedelta(days=10))
            out = await handle.result()
    assert out["completed"] == terminal and out["closed_on_day"] == 3
    assert s.sources() == ["day1", "day1"]  # nothing after it closed


async def test_an_action_that_disappears_completes_the_run(core):
    s = Scenario(core)
    del core.status[str(s.action_id)]
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, core):
            handle = await env.client.start_workflow(
                ActionFollowUpWorkflow.run,
                args=[str(s.action_id), str(s.org_id), "x"],
                id=workflow_id_for(str(s.action_id)),
                task_queue=QUEUE,
            )
            out = await handle.result()
    assert out["completed"] == "action_gone" and out["closed_on_day"] == 1
    assert s.notifications() == []


async def test_a_duplicate_start_is_rejected_and_bad_records_are_poison(core):
    s = Scenario(core)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, core):
            assert await handle_action_event(env.client, s.event(), task_queue=QUEUE) == "started"
            handle = env.client.get_workflow_handle(workflow_id_for(str(s.action_id)))
            run_id = (await handle.describe()).run_id
            assert handle.id == f"actionfollowup-{s.action_id}"
            again = s.event() | {"header": s.event()["header"] | {"event_id": "other"}}
            assert await handle_action_event(env.client, again, task_queue=QUEUE) == "duplicate"
            assert (await handle.describe()).run_id == run_id
            await env.sleep(timedelta(days=11))
            await handle.result()
            # a replay after the run finished starts nothing either
            assert await handle_action_event(env.client, s.event(), task_queue=QUEUE) == "duplicate"
    for bad in (
        s.event() | {"status": "proposed"},
        s.event() | {"action_id": "nope"},
        s.event() | {"header": {}},
    ):
        with pytest.raises(PoisonAction):
            await handle_action_event(None, bad, task_queue=QUEUE)  # type: ignore[arg-type]


async def test_a_worker_restart_mid_timer_neither_drops_nor_duplicates_a_reminder(core):
    s = Scenario(core)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        handle = env.client.get_workflow_handle(workflow_id_for(str(s.action_id)))
        # No workflow cache, so the next worker must rebuild the run from history.
        async with worker(env.client, core, max_cached_workflows=0):
            await handle_action_event(env.client, s.event(), task_queue=QUEUE)
            await env.sleep(timedelta(days=1, hours=1))
            await wait_for_rows(s, 2)
            # Stop only once history holds the day-1 completion and the day-3 timer: the run is
            # parked mid-timer with nothing in flight, which is what a restart interrupts.
            for _ in range(200):
                raw = (await handle.describe()).raw_description
                if not raw.pending_activities and not raw.HasField("pending_workflow_task"):
                    break
                await asyncio.sleep(0.05)
        # the worker is gone for most of the day-3 timer (the test server does not skip time
        # past a timer that fires with no worker to run it, so the new one starts just before)
        await env.sleep(timedelta(days=1, hours=20))
        assert s.sources() == ["day1", "day1"]
        async with worker(env.client, core):
            await env.sleep(timedelta(days=8))
            out = await handle.result()
    assert out["completed"] == "escalated"
    assert s.sources() == ["day1", "day1", "day3", "day3", "day7", "day7"] + ["escalation"] * 2


async def test_a_rerun_activity_enqueues_nothing_new(core):
    s = Scenario(core)
    env = ActivityEnvironment()
    status = await env.run(acts(core).read_action_status, str(s.action_id), str(s.org_id))
    assert status["assignee_id"] == str(s.assignee)
    first = await env.run(
        acts(core).remind_assignee, str(s.action_id), str(s.org_id), 3, status, "x"
    )
    again = await env.run(
        acts(core).remind_assignee, str(s.action_id), str(s.org_id), 3, status, "x"
    )
    assert first["outbox_ids"] == again["outbox_ids"]
    assert s.sources() == ["day3", "day3"]

    unassigned = await env.run(
        acts(core).remind_assignee,
        str(s.action_id),
        str(s.org_id),
        7,
        status | {"assignee_id": None},
        "x",
    )
    assert unassigned == {"sent": False, "reason": "unassigned"}


async def _escalate(core: FakeCore, s: Scenario) -> dict[str, Any]:
    env = ActivityEnvironment()
    status = await env.run(acts(core).read_action_status, str(s.action_id), str(s.org_id))
    result: dict[str, Any] = await env.run(
        acts(core).escalate_to_manager, str(s.action_id), str(s.org_id), 10, status, "x"
    )
    return result


async def test_escalation_target_falls_back_department_admin_organizer_org_admin(core):
    # 1. an admin in the assignee's department (an owner there too: admin is preferred)
    s = Scenario(core)
    dept, elsewhere = department(s.org_id), department(s.org_id)
    dept_admin, dept_owner = add_user(s.org_id, "admin"), add_user(s.org_id, "owner")
    other_admin = add_user(s.org_id, "admin")
    team(s.org_id, dept, s.assignee)
    team(s.org_id, dept, dept_owner, dept_admin)
    team(s.org_id, elsewhere, other_admin)
    out = await _escalate(core, s)
    assert (out["user_id"], out["reason"]) == (str(dept_admin), "department_admin")

    # 2. no admin in the department (only in another one): the meeting organizer
    s = Scenario(core)
    dept, elsewhere = department(s.org_id), department(s.org_id)
    team(s.org_id, dept, s.assignee, add_user(s.org_id, "member"))
    team(s.org_id, elsewhere, add_user(s.org_id, "admin"))
    out = await _escalate(core, s)
    assert (out["user_id"], out["reason"]) == (str(s.organizer), "meeting_organizer")

    # 3. the organizer is the assignee (and the assignee has no team): an org admin
    s = Scenario(core)
    s.set(organizer_id=str(s.assignee))
    org_admin = add_user(s.org_id, "admin")
    out = await _escalate(core, s)
    assert (out["user_id"], out["reason"]) == (str(org_admin), "org_admin")

    # 3b. no organizer at all, and the only admin-tier user is the seeded owner
    s = Scenario(core)
    s.set(organizer_id=None)
    owner = _sql(
        "SELECT user_id FROM rbac_grants WHERE org_id = %(o)s AND tier = 'member' LIMIT 1",
        {"o": str(s.org_id)},
    )
    assert owner  # seed_tenant's own user, promoted below
    _sql(
        "UPDATE rbac_grants SET tier = 'owner' WHERE user_id = %(u)s",
        {"u": str(owner[0]["user_id"])},
    )
    _sql("UPDATE rbac_grants SET tier = 'member' WHERE user_id = %(u)s", {"u": str(s.assignee)})
    out = await _escalate(core, s)
    assert (out["user_id"], out["reason"]) == (str(owner[0]["user_id"]), "org_admin")

    # 4. nobody to escalate to: recorded, nothing emitted
    s = Scenario(core)
    s.set(organizer_id=None)
    out = await _escalate(core, s)
    assert out == {"escalated": False, "reason": "no_target"}
    assert s.notifications() == []
