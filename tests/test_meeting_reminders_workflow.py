"""MeetingRemindersWorkflow (ADR-0039 B9) on Temporal's time-skipping test server, with the real
activities calling core-api's internal routes in-process (httpx `ASGITransport`) and writing the
real `notifications` outbox as `mytheus_app`. Nothing is mocked except the network hop.

- start minus 60 minutes (with a previous meeting): status requests created in core-api and one
  in-app notification per request, "Quick status on {action}?", link `/status-requests`;
- start minus 10 minutes: "Your brief for {title}" to each live participant, link
  `/meetings/{id}#brief`;
- 5 minutes after `ended`: "Your agent's catch-up from {title}" to each `agent` participant, link
  `/meetings/{id}/catch-up`;
- the records go through notification-service into `notification_log` as in-app rows with links;
- rescheduled (signalled, or the signal lost) or cancelled before a timer: nothing for it;
- created inside the window: the due timer fires at once; no previous meeting, no status timer;
- the consumer starts the run once on `created` and signals it on `updated`/`ended`; a signal
  to a run that does not exist is a no-op; activities are idempotent.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis.aioredis
import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from temporalio.client import Client, WorkflowHandle
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.core_api.routes import internal_lifecycle, internal_meetings
from services.notification_service.consumer import handle_message
from services.workflow_service.consumer import _reminders
from services.workflow_service.reminder_activities import ReminderActivities
from services.workflow_service.workflows.meeting_reminders import (
    MeetingRemindersWorkflow,
    next_timer,
    workflow_id_for,
)
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "r" * 40
QUEUE = "reminders-test"
CORE = "http://core-api.test"


@pytest.fixture(scope="module")
def app(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    application = FastAPI()
    add_exception_handlers(application)
    application.include_router(internal_lifecycle.router)
    yield application
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def _fresh_engine():
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def acts(app: FastAPI) -> ReminderActivities:
    return ReminderActivities(
        core_url=CORE, core_secret=SECRET, transport=httpx.ASGITransport(app=app)
    )


def worker(client: Client, app: FastAPI) -> Worker:
    return Worker(
        client,
        task_queue=QUEUE,
        workflows=[MeetingRemindersWorkflow],
        activities=acts(app).all(),
    )


class Scenario:
    """A follow-up meeting: owner (host, live), bob (live, assignee of an open action from the
    previous meeting), carol (sent her agent)."""

    def __init__(self, start_in: timedelta | None, *, previous: bool = True) -> None:
        self.owner = seed_tenant()
        self.bob = seed_user(self.owner)
        self.carol = seed_user(self.owner)
        org = str(self.owner.org_id)
        self.previous = seed_meeting(self.owner.org_id)
        self.action = str(
            _sql(
                "INSERT INTO action_items (org_id, meeting_id, content, confidence, status, "
                "assignee_id) VALUES (%(o)s, %(m)s, 'Draft the plan', 0.9, 'confirmed', %(a)s) "
                "RETURNING id",
                {"o": org, "m": str(self.previous), "a": str(self.bob.user_id)},
            )[0]["id"]
        )
        self.meeting = seed_meeting(self.owner.org_id)
        self.id = str(self.meeting)
        start = datetime.now(UTC) + start_in if start_in is not None else None
        _sql(
            "UPDATE meetings SET title = 'Planning', scheduled_start = %(s)s, "
            "previous_meeting_id = %(p)s WHERE id = %(m)s",
            {"s": start, "p": str(self.previous) if previous else None, "m": self.id},
        )
        for user, attendance in (
            (self.owner, "live"),
            (self.bob, "live"),
            (self.carol, "agent"),
        ):
            _sql(
                "INSERT INTO meeting_participants (meeting_id, user_id, org_id, attendance) "
                "VALUES (%(m)s, %(u)s, %(o)s, %(a)s)",
                {"m": self.id, "u": str(user.user_id), "o": org, "a": attendance},
            )

    @property
    def org(self) -> str:
        return str(self.owner.org_id)

    def move_start(self, start_in: timedelta) -> None:
        _sql(
            "UPDATE meetings SET scheduled_start = %(s)s WHERE id = %(m)s",
            {"s": datetime.now(UTC) + start_in, "m": self.id},
        )

    def set_status(self, status: str) -> None:
        _sql("UPDATE meetings SET status = %(s)s WHERE id = %(m)s", {"s": status, "m": self.id})

    def notifications(self) -> list[dict[str, Any]]:
        return [
            r["payload"]
            for r in _sql(
                "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = 'notifications' "
                "ORDER BY id",
                {"o": self.org},
            )
        ]

    def kinds(self) -> list[tuple[str, str]]:
        """(source kind, recipient) per record, recipients named."""
        names = {
            str(self.owner.user_id): "owner",
            str(self.bob.user_id): "bob",
            str(self.carol.user_id): "carol",
        }
        return [
            (n["source_event_id"].split(":", 1)[0], names[n["user_id"]])
            for n in self.notifications()
        ]


async def start(env: WorkflowEnvironment, s: Scenario) -> WorkflowHandle[Any, Any]:
    await _reminders(env.client, s.id, s.org, "created", QUEUE)
    return env.client.get_workflow_handle(workflow_id_for(s.id))


async def test_the_three_timers_fire_and_deliver_in_app_with_links(app):
    s = Scenario(timedelta(hours=2))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, app):
            handle = await start(env, s)
            await _reminders(env.client, s.id, s.org, "created", QUEUE)  # redelivery: no-op
            await env.sleep(timedelta(minutes=50))
            assert s.kinds() == []
            await env.sleep(timedelta(minutes=20))  # past start - 60
            assert s.kinds() == [("statusrequest", "bob")]
            await env.sleep(timedelta(minutes=45))  # past start - 10
            assert sorted(s.kinds()) == sorted(
                [("statusrequest", "bob"), ("meetingbrief", "owner"), ("meetingbrief", "bob")]
            )
            s.set_status("ended")
            await _reminders(env.client, s.id, s.org, "ended", QUEUE)
            await env.sleep(timedelta(minutes=4))
            assert len(s.notifications()) == 3  # the catch-up waits 5 minutes after the end
            # Skipped explicitly: after `env.sleep`, the test server's history long-poll in
            # `result()` does not always skip a pending timer on its own.
            await env.sleep(timedelta(minutes=2))
            out = await handle.result()
    assert out["completed"] == "ended"
    assert [x["kind"] for x in out["sent"]] == ["status_requests", "brief", "catch_up"]

    by_kind = {n["source_event_id"].split(":", 1)[0]: n for n in s.notifications()}
    request_id = _sql(
        "SELECT id FROM action_status_requests WHERE meeting_id = %(m)s", {"m": s.id}
    )[0]["id"]
    status = by_kind["statusrequest"]
    assert status["title"] == "Quick status on Draft the plan?"
    assert status["link"] == "/status-requests" and status["channel"] == "in_app"
    assert status["source_event_id"] == f"statusrequest:{request_id}"
    assert by_kind["meetingbrief"]["title"] == "Your brief for Planning"
    assert by_kind["meetingbrief"]["link"] == f"/meetings/{s.id}#brief"
    catch_up = by_kind["meetingcatchup"]
    assert catch_up["title"] == "Your Myte's catch-up from Planning"
    assert catch_up["link"] == f"/meetings/{s.id}/catch-up"
    assert catch_up["user_id"] == str(s.carol.user_id)
    assert len(s.notifications()) == 4

    # notification-service turns each into an in-app notification_log row with its link.
    engine = create_async_engine(APP_URL)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        for record in s.notifications():
            async with factory() as session, session.begin():
                assert await handle_message(
                    session, fakeredis.aioredis.FakeRedis(), "notifications", record
                )
    finally:
        await engine.dispose()
    rows = _sql(
        "SELECT user_id, title, link, source FROM notification_log WHERE org_id = %(o)s "
        "ORDER BY title",
        {"o": s.org},
    )
    assert [(r["title"], r["link"]) for r in rows if r["user_id"] == str(s.carol.user_id)] == [
        ("Your Myte's catch-up from Planning", f"/meetings/{s.id}/catch-up")
    ]
    assert {r["link"] for r in rows} == {
        "/status-requests",
        f"/meetings/{s.id}#brief",
        f"/meetings/{s.id}/catch-up",
    }
    receipts = _sql(
        "SELECT payload->>'channel' AS c, payload->>'status' AS st FROM outbox "
        "WHERE org_id = %(o)s AND topic = 'notification-delivery-events'",
        {"o": s.org},
    )
    assert {(r["c"], r["st"]) for r in receipts} == {("in_app", "sent")}


async def test_a_signalled_reschedule_moves_every_timer(app):
    s = Scenario(timedelta(hours=2))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, app):
            handle = await start(env, s)
            await env.sleep(timedelta(minutes=30))
            s.move_start(timedelta(hours=5))
            await _reminders(env.client, s.id, s.org, "updated", QUEUE)
            await env.sleep(timedelta(hours=2))  # the old start has come and gone
            assert s.kinds() == []
            await env.sleep(timedelta(hours=2, minutes=55))  # past new start - 10
            assert sorted(s.kinds()) == sorted(
                [("statusrequest", "bob"), ("meetingbrief", "owner"), ("meetingbrief", "bob")]
            )
            s.set_status("cancelled")
            await _reminders(env.client, s.id, s.org, "updated", QUEUE)
            out = await handle.result()
    assert out["completed"] == "cancelled"
    assert len(s.notifications()) == 3


async def test_a_reschedule_whose_signal_was_lost_still_cancels_the_old_timer(app):
    s = Scenario(timedelta(hours=2))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, app):
            handle = await start(env, s)
            await env.sleep(timedelta(minutes=30))
            s.move_start(timedelta(hours=3, minutes=30))  # no `updated` signal
            await env.sleep(timedelta(minutes=45))  # the old start - 60 fires, re-reads
            assert s.kinds() == []
            await env.sleep(timedelta(hours=2, minutes=15))  # past the new start - 10
            assert sorted(s.kinds()) == sorted(
                [("statusrequest", "bob"), ("meetingbrief", "owner"), ("meetingbrief", "bob")]
            )
            s.set_status("cancelled")
            await _reminders(env.client, s.id, s.org, "updated", QUEUE)
            out = await handle.result()
    assert {"kind": "status_requests"}.items() <= out["skipped"][0].items()


async def test_cancelled_before_any_timer_sends_nothing(app):
    s = Scenario(timedelta(hours=2))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, app):
            handle = await start(env, s)
            await env.sleep(timedelta(minutes=20))
            s.set_status("cancelled")
            await _reminders(env.client, s.id, s.org, "updated", QUEUE)
            out = await handle.result()
    assert out["completed"] == "cancelled" and s.notifications() == []


async def test_created_inside_the_window_and_without_a_previous_meeting(app):
    soon = Scenario(timedelta(minutes=30))
    plain = Scenario(timedelta(hours=1), previous=False)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with worker(env.client, app):
            await start(env, soon)
            await start(env, plain)
            await env.sleep(timedelta(minutes=1))
            assert soon.kinds() == [("statusrequest", "bob")]  # due already: fired at once
            await env.sleep(timedelta(minutes=55))
            assert sorted(soon.kinds()) == sorted(
                [("statusrequest", "bob"), ("meetingbrief", "owner"), ("meetingbrief", "bob")]
            )
            assert sorted(plain.kinds()) == [("meetingbrief", "bob"), ("meetingbrief", "owner")]
            for s in (soon, plain):
                s.set_status("cancelled")
                await _reminders(env.client, s.id, s.org, "updated", QUEUE)


async def test_signals_to_a_missing_run_are_no_ops():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        missing = str(uuid.uuid4())
        await _reminders(env.client, missing, str(uuid.uuid4()), "updated", QUEUE)
        await _reminders(env.client, missing, str(uuid.uuid4()), "ended", QUEUE)


async def test_activities_are_idempotent(app):
    s = Scenario(timedelta(hours=2))
    a = acts(app)
    env = ActivityEnvironment()
    start_iso = "2026-12-01T10:00:00+00:00"
    first = await env.run(a.send_status_requests, s.id, s.org, start_iso)
    again = await env.run(a.send_status_requests, s.id, s.org, start_iso)
    assert first["outbox_ids"] == again["outbox_ids"] and len(first["outbox_ids"]) == 1
    b1 = await env.run(a.send_briefs, s.id, s.org, start_iso)
    b2 = await env.run(a.send_briefs, s.id, s.org, start_iso)
    assert b1["outbox_ids"] == b2["outbox_ids"] and b1["sent"] == 2
    assert (await env.run(a.send_catchups, s.id, s.org))["reason"] == "not_ended"
    s.set_status("ended")
    c1 = await env.run(a.send_catchups, s.id, s.org)
    assert c1["sent"] == 1 and (await env.run(a.send_catchups, s.id, s.org)) == c1
    assert len(s.notifications()) == 4
    gone = await env.run(a.read_meeting_reminders, str(uuid.uuid4()), s.org)
    assert gone == {"gone": True}


def test_next_timer_is_pure_and_skips_a_started_meeting():
    now = datetime(2026, 12, 1, 9, 0, tzinfo=UTC)
    state = {"scheduled_start": "2026-12-01T10:00:00Z", "previous_meeting_id": "x"}
    kind, at, start_iso = next_timer(state, now, set())  # type: ignore[misc]
    assert (kind, at) == ("status_requests", datetime(2026, 12, 1, 9, 0, tzinfo=UTC))
    assert next_timer(state, now, {f"status_requests@{start_iso}"})[0] == "brief"  # type: ignore[index]
    assert next_timer({**state, "previous_meeting_id": None}, now, set())[0] == "brief"  # type: ignore[index]
    assert next_timer(state, datetime(2026, 12, 1, 10, 0, tzinfo=UTC), set()) is None
    assert next_timer({"scheduled_start": None}, now, set()) is None
