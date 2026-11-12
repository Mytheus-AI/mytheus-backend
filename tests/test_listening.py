"""In every meeting at once (`core-api` `listening.py`, `routes/listening.py`,
`routes/internal_listening.py`), against a real Postgres.

- per-meeting choices: attend / my agent listens / skip keep `attendance` in step; confidential
  takes no agent (422); over is 409; non-participant 403; another tenant 404; a live room hears
  the new listener count; the old "send your agent" route keeps the choice in step;
- rules: recurring + title + meeting type, hosted meetings skipped, manual beats rule, delete
  reverts, the week's totals and hours saved;
- the gate: quiet hours (wrapping midnight), focus blocks, interruptions off, the throttle;
- the live watch's routes: listeners (teams, projects, listen-for, owns; confidential is empty;
  caller auth), interruptions (delivered = notification + toast; held = neither; suppressed;
  idempotent);
- the three answers: join (attend), send a view (to the host), hold the line (only the person's
  own positions, copied into the meeting's delegated answers); host share / dismiss;
- the evening digest: built from minutes and decisions with moment links, stored once, one
  notification; a quiet day stores without notifying;
- RLS: another tenant's interruption is a 404.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import listening
from services.core_api.routes import internal_agents, internal_listening, internal_meetings
from services.core_api.routes import listening as listening_routes
from services.core_api.routes import meetings as meetings_routes
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user

AGENT_SECRET = "a" * 40
WORKFLOW_SECRET = "w" * 40


class FakeRedis:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.presence: dict[tuple[str, str], float] = {}

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, json.loads(message)))
        return 1

    async def zscore(self, key: str, member: str) -> float | None:
        return self.presence.get((key, member))

    def of(self, kind: str) -> list[tuple[str, dict[str, Any]]]:
        return [(c, m) for c, m in self.published if m.get("type") == kind]


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},workflow-service={WORKFLOW_SECRET}"
    )
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-listening-test")
    for router in (
        listening_routes.router,
        internal_listening.router,
        meetings_routes.router,
        internal_agents.router,
    ):
        app.include_router(router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def redis(monkeypatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr(listening, "get_redis", lambda: fake)
    return fake


def agent_headers(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": AGENT_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def workflow_headers(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": WORKFLOW_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def meeting(
    *,
    status: str = "live",
    title: str = "Pricing review",
    confidential: bool = False,
    start: datetime | None = None,
) -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    member = add_user(host)
    _sql(
        "UPDATE users SET email = 'sushan.kumar@acme.test', listen_for = ARRAY['pricing'] "
        "WHERE id = %(u)s",
        {"u": str(member.user_id)},
    )
    m = new_meeting(
        host, member, status=status, title=title, confidential=confidential, start=start
    )
    return host, member, m


def new_meeting(
    host: Tenant,
    member: Tenant,
    *,
    status: str = "scheduled",
    title: str = "Pricing review",
    confidential: bool = False,
    start: datetime | None = None,
    previous: uuid.UUID | None = None,
    member_hosts: bool = False,
) -> uuid.UUID:
    m = seed_meeting(host.org_id)
    start = start or datetime.now(UTC) + timedelta(hours=2)
    _sql(
        "UPDATE meetings SET title = %(t)s, status = %(s)s, confidential = %(c)s, "
        "scheduled_start = %(a)s, scheduled_end = %(b)s, created_by = %(h)s, "
        "previous_meeting_id = %(p)s WHERE id = %(m)s",
        {
            "t": title,
            "s": status,
            "c": confidential,
            "a": start,
            "b": start + timedelta(minutes=45),
            "h": str(member.user_id if member_hosts else host.user_id),
            "p": str(previous) if previous else None,
            "m": str(m),
        },
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    return m


def participant(m: uuid.UUID, u: uuid.UUID) -> dict[str, Any]:
    return _sql(
        "SELECT attendance, attendance_choice, attendance_choice_source, attendance_rule_id "
        "FROM meeting_participants WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(u)},
    )[0]


def notifications(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


def propose(
    client: TestClient, org: uuid.UUID, m: uuid.UUID, user: uuid.UUID, key: str = "w1", **over: Any
) -> httpx.Response:
    body: dict[str, Any] = {
        "user_id": str(user),
        "key": key,
        "reason": "decision",
        "headline": "Pricing for your segment is being decided",
        "detail": "They are settling the SMB discount floor.",
        "topic": "pricing",
        "quote": "Let's lock the SMB tier at 15% off.",
        "start_ms": 125_000,
        "end_ms": 131_000,
        "confidence": 0.86,
        "prompt": "interrupt_gate@v1",
        **over,
    }
    return client.post(
        f"/internal/meetings/{m}/interruptions", json=body, headers=agent_headers(org)
    )


# --- per-meeting choices ----------------------------------------------------------------------


def test_choice_keeps_attendance_in_step_and_tells_a_live_room(client, redis):
    host, member, m = meeting()
    sign_in(member)
    res = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["choice"] == "agent_listens" and body["source"] == "manual"
    assert body["attendance"] == "agent" and body["agents_listening"] == 1
    assert participant(m, member.user_id)["attendance"] == "agent"
    channel, msg = redis.of("listeners")[-1]
    assert channel == f"nexus:{host.org_id}:pubsub:meeting:{m}"
    assert msg["payload"] == {"count": 1, "userIds": [str(member.user_id)]}

    skip = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "skip"}).json()
    assert skip["choice"] == "skip" and skip["attendance"] == "live"
    assert redis.of("listeners")[-1][1]["payload"]["count"] == 0

    undo = client.delete(f"/api/v1/meetings/{m}/attendance-choice").json()
    assert undo["choice"] == "attend" and undo["source"] == "default"
    assert participant(m, member.user_id)["attendance_choice"] is None


def test_choice_guards(client, redis):
    host, member, m = meeting(confidential=True)
    sign_in(member)
    res = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    assert res.status_code == 422 and "confidential" in res.json()["detail"]
    assert (
        client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "skip"}).status_code
        == 200
    )

    _h, member2, ended = meeting(status="ended")
    sign_in(member2)
    res = client.put(f"/api/v1/meetings/{ended}/attendance-choice", json={"choice": "skip"})
    assert res.status_code == 409

    outsider = add_user(host)
    sign_in(outsider)
    res = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "skip"})
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    sign_in(seed_tenant())
    res = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "skip"})
    assert res.status_code == 404
    bad = client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "maybe"})
    assert bad.status_code == 422


def test_send_your_agent_route_keeps_the_choice_in_step(client, redis):
    _host, member, m = meeting(status="scheduled")
    sign_in(member)
    res = client.put(
        f"/api/v1/meetings/{m}/attendance",
        json={
            "attendance": "agent",
            "proxy_update": "Shipped the pricing page.",
            "proxy_questions": [],
        },
    )
    assert res.status_code == 200, res.text
    row = participant(m, member.user_id)
    assert (
        row["attendance_choice"] == "agent_listens" and row["attendance_choice_source"] == "manual"
    )
    client.put(f"/api/v1/meetings/{m}/attendance", json={"attendance": "live"})
    assert participant(m, member.user_id)["attendance_choice"] == "attend"


# --- rules and the week -----------------------------------------------------------------------


def test_rules_apply_to_recurring_meetings_and_the_week_adds_up(client, redis):
    host = seed_tenant()
    member = add_user(host)
    monday = date.today() - timedelta(days=date.today().weekday())
    at = datetime.combine(monday, datetime.min.time(), tzinfo=UTC) + timedelta(days=1, hours=15)
    first = new_meeting(host, member, title="Weekly status", start=at - timedelta(days=7))
    status_mtg = new_meeting(host, member, title="Weekly status", start=at, previous=first)
    design = new_meeting(host, member, title="Design crit", start=at + timedelta(hours=2))
    secret = new_meeting(
        host,
        member,
        title="Weekly status",
        start=at + timedelta(days=1),
        confidential=True,
        previous=status_mtg,
    )
    mine = new_meeting(
        host,
        member,
        title="Weekly status",
        start=at + timedelta(days=2),
        previous=status_mtg,
        member_hosts=True,
    )
    for mid in (first, status_mtg, design, secret, mine):
        _sql("UPDATE meetings SET status = 'scheduled' WHERE id = %(m)s", {"m": str(mid)})
    sign_in(member)
    bad = client.post("/api/v1/listening/rules", json={"choice": "skip"})
    assert bad.status_code == 422
    res = client.post(
        "/api/v1/listening/rules",
        json={"choice": "agent_listens", "recurring_only": True, "title_contains": "status"},
    )
    assert res.status_code == 201, res.text
    rule = res.json()["rule"]
    applied = res.json()["applied"]
    assert applied >= 1
    assert participant(status_mtg, member.user_id)["attendance"] == "agent"
    assert participant(status_mtg, member.user_id)["attendance_choice_source"] == "rule"
    assert participant(design, member.user_id)["attendance_choice"] is None
    assert participant(secret, member.user_id)["attendance_choice"] is None  # confidential
    assert participant(mine, member.user_id)["attendance_choice"] is None  # they host it

    # a manual choice beats the rule and survives re-application
    client.put(f"/api/v1/meetings/{status_mtg}/attendance-choice", json={"choice": "attend"})
    client.put(f"/api/v1/meetings/{design}/attendance-choice", json={"choice": "skip"})
    week = client.get(f"/api/v1/listening/week?start={monday.isoformat()}").json()
    assert participant(status_mtg, member.user_id)["attendance_choice"] == "attend"
    titles = {w["meeting_id"]: w for w in week["meetings"]}
    assert titles[str(design)]["choice"] == "skip" and titles[str(design)]["source"] == "manual"
    assert titles[str(secret)]["can_agent_listen"] is False
    assert titles[str(status_mtg)]["recurring"] is True
    s = week["summary"]
    assert s["invited"] == s["attend"] + s["agent_listens"] + s["skip"]
    assert s["skip"] >= 1 and s["minutes_saved"] >= 45
    assert week["rules"][0]["id"] == rule["id"]

    # deleting the rule reverts what it chose
    client.put(f"/api/v1/meetings/{status_mtg}/attendance-choice", json={"choice": "agent_listens"})
    client.delete(f"/api/v1/meetings/{status_mtg}/attendance-choice")
    assert participant(status_mtg, member.user_id)["attendance_choice_source"] == "rule"
    assert client.delete(f"/api/v1/listening/rules/{rule['id']}").status_code == 204
    assert participant(status_mtg, member.user_id)["attendance"] == "live"
    assert client.get("/api/v1/listening/rules").json()["items"] == []
    sign_in(seed_tenant())
    assert client.delete(f"/api/v1/listening/rules/{rule['id']}").status_code == 404


# --- the gate ---------------------------------------------------------------------------------


def test_quiet_hours_and_focus_blocks_unit():
    prefs = listening.ListeningPreferences(
        quiet_start="22:00",
        quiet_end="07:00",
        focus_blocks=[listening.FocusBlock(days=[0, 1, 2, 3, 4], start="09:00", end="11:00")],
        time_zone="Asia/Kolkata",
    )
    zone = ZoneInfo("Asia/Kolkata")
    late = datetime(2026, 11, 11, 23, 30, tzinfo=zone).astimezone(UTC)  # a Wednesday
    reason, until = listening.quiet_until(prefs, late)  # type: ignore[misc]
    assert reason == "quiet_hours"
    assert until == datetime(2026, 11, 12, 7, 0, tzinfo=zone).astimezone(UTC)
    early = datetime(2026, 11, 12, 6, 0, tzinfo=zone).astimezone(UTC)
    assert listening.quiet_until(prefs, early)[1] == until  # type: ignore[index]
    focus = datetime(2026, 11, 11, 10, 15, tzinfo=zone).astimezone(UTC)
    assert listening.quiet_until(prefs, focus) == (
        "focus_block",
        datetime(2026, 11, 11, 11, 0, tzinfo=zone).astimezone(UTC),
    )
    saturday = datetime(2026, 11, 14, 10, 15, tzinfo=zone).astimezone(UTC)
    assert listening.quiet_until(prefs, saturday) is None
    prefs.never_during_focus = False
    assert listening.quiet_until(prefs, focus) is None
    with pytest.raises(ValueError):
        listening.ListeningPreferences(time_zone="Mars/Olympus")
    with pytest.raises(ValueError):
        listening.ListeningPreferences(quiet_start="22:00")
    with pytest.raises(ValueError):
        listening.FocusBlock(days=[0], start="11:00", end="09:00")


def test_copy_unit():
    zone = ZoneInfo("Europe/London")
    at = datetime(2026, 11, 11, 15, 0, tzinfo=zone)
    assert listening.clock_label(at, zone) == "3pm"
    assert listening.clock_label(at + timedelta(minutes=30), zone) == "3:30pm"
    assert (
        listening.interruption_title("pricing for your segment is being decided.", "X", at, zone)
        == "Pricing for your segment is being decided in the 3pm right now."
    )
    assert listening.interruption_title("A question for you", "Design crit", None, zone) == (
        "A question for you in “Design crit” right now."
    )


def test_preferences_round_trip(client, redis):
    _host, member, _m = meeting()
    sign_in(member)
    got = client.get("/api/v1/listening/preferences").json()
    assert got["interruptions_enabled"] is True and got["max_per_hour"] == 3
    assert got["interruptible_now"] is True
    body = {
        "interruptions_enabled": False,
        "max_per_hour": 2,
        "quiet_start": "21:00",
        "quiet_end": "08:00",
        "focus_blocks": [{"days": [0, 2], "start": "09:00", "end": "10:30", "label": "Deep work"}],
        "never_during_focus": True,
        "digest_enabled": True,
        "digest_hour": 17,
        "time_zone": "America/New_York",
    }
    res = client.put("/api/v1/listening/preferences", json=body)
    assert res.status_code == 200, res.text
    assert res.json()["interruptible_now"] is False and res.json()["quiet_reason"] == "disabled"
    again = client.get("/api/v1/listening/preferences").json()
    assert again["focus_blocks"][0]["label"] == "Deep work" and again["digest_hour"] == 17
    bad = client.put("/api/v1/listening/preferences", json={**body, "time_zone": "Nowhere"})
    assert bad.status_code == 422


# --- the live watch's routes ------------------------------------------------------------------


def test_listeners_route(client, redis):
    host, member, m = meeting()
    team = uuid.uuid4()
    _sql(
        "INSERT INTO teams (id, org_id, name) VALUES (%(t)s, %(o)s, 'Growth')",
        {"t": str(team), "o": str(host.org_id)},
    )
    _sql(
        "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s, %(u)s, %(o)s)",
        {"t": str(team), "u": str(member.user_id), "o": str(host.org_id)},
    )
    _sql(
        "INSERT INTO org_goals (org_id, title, owner_user_id) VALUES (%(o)s, 'Self-serve pricing', "
        "%(u)s)",
        {"o": str(host.org_id), "u": str(member.user_id)},
    )
    assert client.get(f"/internal/meetings/{m}/listeners").status_code in (401, 403)
    empty = client.get(f"/internal/meetings/{m}/listeners", headers=agent_headers(host.org_id))
    assert empty.status_code == 200 and empty.json()["listeners"] == []
    sign_in(member)
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    body = client.get(
        f"/internal/meetings/{m}/listeners", headers=agent_headers(host.org_id)
    ).json()
    [one] = body["listeners"]
    assert one["user_id"] == str(member.user_id)
    assert one["name"] == "Sushan Kumar" and one["first_name"] == "Sushan"
    assert one["teams"] == ["Growth"] and one["projects"] == ["Self-serve pricing"]
    assert one["listen_for"] == ["pricing"] and one["interruptible"] is True
    other = seed_tenant()
    res = client.get(f"/internal/meetings/{m}/listeners", headers=agent_headers(other.org_id))
    assert res.status_code == 404

    _h2, member2, secret = meeting(confidential=True)
    _sql(
        "UPDATE meeting_participants SET attendance = 'agent' WHERE meeting_id = %(m)s",
        {"m": str(secret)},
    )
    conf = client.get(
        f"/internal/meetings/{secret}/listeners", headers=agent_headers(_h2.org_id)
    ).json()
    assert conf["confidential"] is True and conf["listeners"] == []


def test_interruption_delivered_once_with_notification_and_toast(client, redis):
    host, member, m = meeting()
    sign_in(member)
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    res = propose(client, host.org_id, m, member.user_id)
    assert res.status_code == 200, res.text
    decision = res.json()
    assert decision["status"] == "delivered" and decision["reason"] is None
    [note] = notifications(f"interrupt:{decision['id']}")
    assert note["priority"] == 1 and note["link"] == f"/inbox?interruption={decision['id']}"
    assert note["title"].startswith("Pricing for your segment is being decided in the ")
    assert note["title"].endswith("right now.")
    channel, toast = redis.of("agent-interrupt")[-1]
    assert channel == f"nexus:{host.org_id}:pubsub:user:{member.user_id}"
    assert toast["payload"]["id"] == decision["id"]
    assert toast["payload"]["join_url"] == f"/meetings/{m}/room"
    assert toast["payload"]["can_respond"] is True

    again = propose(client, host.org_id, m, member.user_id).json()
    assert again["id"] == decision["id"] and len(notifications(f"interrupt:{decision['id']}")) == 1
    # one per meeting per gap: the next window is held, not delivered
    held = propose(client, host.org_id, m, member.user_id, key="w2").json()
    assert held["status"] == "held" and held["reason"] == "throttled"
    assert held["next_allowed_at"] is not None
    assert notifications(f"interrupt:{held['id']}") == []

    listed = client.get("/api/v1/listening/interruptions").json()["items"]
    assert [i["status"] for i in listed] == ["held", "delivered"]
    assert listed[1]["moment_url"] == f"/meetings/{m}/transcript#t=125"
    held_only = client.get("/api/v1/listening/interruptions?status=held").json()["items"]
    assert len(held_only) == 1


def test_interruption_held_in_quiet_hours_and_suppressed_cases(client, redis):
    host, member, m = meeting()
    sign_in(member)
    assert propose(client, host.org_id, m, member.user_id).json() == {
        "id": None,
        "status": "suppressed",
        "reason": "not_listening",
        "next_allowed_at": None,
    }
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    redis.presence[(f"nexus:{host.org_id}:presence:{m}", str(member.user_id))] = (
        datetime.now(UTC).timestamp() * 1000
    )
    assert propose(client, host.org_id, m, member.user_id).json()["reason"] == "in_room"
    redis.presence.clear()
    client.put(
        "/api/v1/listening/preferences",
        json={"quiet_start": "00:00", "quiet_end": "23:59", "time_zone": "UTC"},
    )
    held = propose(client, host.org_id, m, member.user_id, key="quiet").json()
    if datetime.now(UTC).strftime("%H:%M") < "23:59":
        assert held["status"] == "held" and held["reason"] == "quiet_hours"
        assert redis.of("agent-interrupt") == []
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    assert propose(client, host.org_id, m, member.user_id, key="late").json()["reason"] == "ended"
    other = seed_tenant()
    assert propose(client, other.org_id, m, member.user_id).status_code == 404


def test_throttle_per_hour(client, redis):
    host, member, m = meeting()
    sign_in(member)
    r = client.put("/api/v1/listening/preferences", json={"max_per_hour": 1})
    assert r.status_code == 200 and r.json()["max_per_hour"] == 1, r.text
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    m2 = new_meeting(host, member, status="live", title="Roadmap")
    client.put(f"/api/v1/meetings/{m2}/attendance-choice", json={"choice": "agent_listens"})
    assert propose(client, host.org_id, m, member.user_id).json()["status"] == "delivered"
    second = propose(client, host.org_id, m2, member.user_id, key="w2").json()
    assert second["status"] == "held" and second["reason"] == "throttled"
    listeners = client.get(
        f"/internal/meetings/{m2}/listeners", headers=agent_headers(host.org_id)
    ).json()["listeners"]
    assert listeners[0]["interruptible"] is False and listeners[0]["quiet_reason"] == "throttled"


# --- the three answers ------------------------------------------------------------------------


def _delivered(client, redis) -> tuple[Tenant, Tenant, uuid.UUID, str]:
    host, member, m = meeting()
    sign_in(member)
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    decision = propose(client, host.org_id, m, member.user_id).json()
    return host, member, m, decision["id"]


def test_join_now_turns_the_choice_to_attend(client, redis):
    _host, member, m, iid = _delivered(client, redis)
    res = client.post(f"/api/v1/listening/interruptions/{iid}/join")
    assert res.status_code == 200, res.text
    assert res.json()["response"] == "joined" and res.json()["can_respond"] is False
    assert participant(m, member.user_id)["attendance_choice"] == "attend"
    assert redis.of("listeners")[-1][1]["payload"]["count"] == 0


def test_send_a_view_reaches_the_host_who_shares_it(client, redis):
    host, member, m, iid = _delivered(client, redis)
    bad = client.post(f"/api/v1/listening/interruptions/{iid}/view", json={"text": "  "})
    assert bad.status_code == 422
    res = client.post(
        f"/api/v1/listening/interruptions/{iid}/view",
        json={"text": "Keep SMB at 10% off; 15% breaks the margin model."},
    )
    assert res.status_code == 200, res.text
    view = res.json()
    assert view["status"] == "pending" and view["kind"] == "view"
    assert view["chat_text"] == "Sushan's view: Keep SMB at 10% off; 15% breaks the margin model."
    channel, msg = redis.of("meeting-view")[-1]
    assert channel == f"nexus:{host.org_id}:pubsub:meeting:{m}"
    assert msg["userId"] == str(host.user_id) and msg["payload"]["id"] == view["id"]

    assert client.post(f"/api/v1/meetings/{m}/views/{view['id']}/share").status_code == 403
    sign_in(host)
    listed = client.get(f"/api/v1/meetings/{m}/views").json()
    assert listed["is_host"] is True and len(listed["items"]) == 1
    shared = client.post(f"/api/v1/meetings/{m}/views/{view['id']}/share").json()
    assert shared["status"] == "shared"
    status_channel, status_msg = redis.of("view-status")[-1]
    assert status_channel == f"nexus:{host.org_id}:pubsub:user:{member.user_id}"
    assert status_msg["payload"]["status"] == "shared"
    # deciding again changes nothing
    assert client.post(f"/api/v1/meetings/{m}/views/{view['id']}/dismiss").json()["status"] == (
        "shared"
    )
    sign_in(member)
    assert client.get(f"/api/v1/listening/interruptions/{iid}").json()["response"] == "view_sent"


def test_hold_the_line_only_with_pre_approved_positions(client, redis):
    host, member, m, iid = _delivered(client, redis)
    pos = client.post(
        "/api/v1/listening/positions",
        json={"topic": "Pricing", "text": "No discount above 10% for SMB without finance."},
    )
    assert pos.status_code == 201, pos.text
    pid = pos.json()["id"]
    stranger = add_user(host)
    sign_in(stranger)
    theirs = client.post("/api/v1/listening/positions", json={"text": "Ship it."}).json()["id"]
    sign_in(member)
    res = client.post(
        f"/api/v1/listening/interruptions/{iid}/hold", json={"position_ids": [theirs]}
    )
    assert res.status_code == 422
    res = client.post(f"/api/v1/listening/interruptions/{iid}/hold", json={"position_ids": [pid]})
    assert res.status_code == 200, res.text
    view = res.json()
    assert view["kind"] == "hold_line"
    assert view["text"] == "On Pricing: No discount above 10% for SMB without finance."
    assert view["chat_text"].startswith("Sushan's Myte is holding the line for Sushan: ")
    statements = _sql(
        "SELECT text FROM delegated_answer_statements WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(member.user_id)},
    )
    assert [s["text"] for s in statements] == ["No discount above 10% for SMB without finance."]
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    late = client.post(f"/api/v1/listening/interruptions/{iid}/dismiss")
    assert late.status_code == 200 and late.json()["can_respond"] is False


def test_positions_crud_and_isolation(client, redis):
    _host, member, _m = meeting()
    sign_in(member)
    p = client.post("/api/v1/listening/positions", json={"text": "  Ship   weekly. "}).json()
    assert p["text"] == "Ship weekly."
    upd = client.put(f"/api/v1/listening/positions/{p['id']}", json={"text": "Ship daily."})
    assert upd.json()["text"] == "Ship daily."
    sign_in(seed_tenant())
    assert (
        client.put(f"/api/v1/listening/positions/{p['id']}", json={"text": "x"}).status_code == 404
    )
    sign_in(member)
    assert client.delete(f"/api/v1/listening/positions/{p['id']}").status_code == 204
    assert client.get("/api/v1/listening/positions").json()["items"] == []


def test_interruptions_are_tenant_isolated(client, redis):
    _host, _member, _m, iid = _delivered(client, redis)
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/listening/interruptions/{iid}").status_code == 404
    assert client.post(f"/api/v1/listening/interruptions/{iid}/join").status_code == 404
    assert client.get("/api/v1/listening/interruptions").json()["items"] == []


# --- the evening digest -----------------------------------------------------------------------


def test_evening_digest_is_built_stored_once_and_notified(client, redis):
    host, member, m = meeting(status="live", start=datetime.now(UTC) - timedelta(minutes=30))
    sign_in(member)
    client.put(f"/api/v1/meetings/{m}/attendance-choice", json={"choice": "agent_listens"})
    propose(client, host.org_id, m, member.user_id)
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    _sql(
        "INSERT INTO meeting_minutes (org_id, meeting_id, user_id, scope, status, content, "
        "prompt_version, generation) VALUES (%(o)s, %(m)s, %(u)s, 'personal', 'ready', "
        "%(c)s, 'minutes@v1', 'g1')",
        {
            "o": str(host.org_id),
            "m": str(m),
            "u": str(member.user_id),
            "c": json.dumps(
                {"summary": "The team agreed the SMB floor and asked for your margin model."}
            ),
        },
    )
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, status, confidence, quote_start_ms) "
        "VALUES (%(o)s, %(m)s, 'SMB tier discount floor is 15%%', 'confirmed', 0.9, 125000)",
        {"o": str(host.org_id), "m": str(m)},
    )
    skipped = new_meeting(
        host,
        member,
        status="ended",
        title="Design crit",
        start=datetime.now(UTC) - timedelta(minutes=10),
    )
    client.put(f"/api/v1/meetings/{skipped}/attendance-choice", json={"choice": "skip"})
    _sql(
        "UPDATE meeting_participants SET attendance_choice = 'skip', attendance_choice_source = "
        "'manual' WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(skipped), "u": str(member.user_id)},
    )
    client.put("/api/v1/listening/preferences", json={"digest_hour": 0, "time_zone": "UTC"})

    today = datetime.now(UTC).date()
    preview = client.get(f"/api/v1/listening/digests/{today.isoformat()}").json()
    assert preview["id"] is None and preview["status"] == "ready"
    text_ = "".join(s["text"] for s in preview["paragraph"])
    assert text_.startswith("Your Myte sat in on 1 meeting for you today.")
    assert "the team agreed the SMB floor" in text_
    assert "It pulled you in once, about pricing." in text_
    assert "You also skipped Design crit" in text_
    links = [s["href"] for s in preview["paragraph"] if s["href"]]
    assert f"/meetings/{m}" in links and f"/meetings/{m}/transcript#t=125" in links
    assert preview["minutes_saved"] == 90

    res = client.post("/internal/listening/evening", json={}, headers=workflow_headers(host.org_id))
    assert res.status_code == 200, res.text
    assert res.json()["built"] >= 1 and res.json()["notified"] == 1
    again = client.post(
        "/internal/listening/evening", json={}, headers=workflow_headers(host.org_id)
    )
    assert again.json()["built"] == 0
    stored = client.get("/api/v1/listening/digests").json()["items"]
    assert len(stored) == 1 and stored[0]["meeting_count"] == 2
    latest = client.get("/api/v1/listening/digests/latest").json()
    assert latest["id"] == stored[0]["id"]
    notes = notifications("evening-digest:")
    mine = [n for n in notes if n["user_id"] == str(member.user_id)]
    assert len(mine) == 1 and mine[0]["link"] == f"/meetings/listening?day={today.isoformat()}"
    assert mine[0]["body"].startswith("1 meeting covered for you")


def test_evening_waits_for_the_digest_hour_and_a_quiet_day_sends_nothing(client, redis):
    host, member, _m = meeting(status="scheduled")
    sign_in(member)
    client.put("/api/v1/listening/preferences", json={"digest_hour": 23, "time_zone": "UTC"})
    early = datetime.now(UTC).replace(hour=10, minute=5)
    res = client.post(
        "/internal/listening/evening",
        json={"now": early.isoformat()},
        headers=workflow_headers(host.org_id),
    ).json()
    assert res["built"] == 0
    late = early.replace(hour=23)
    res = client.post(
        "/internal/listening/evening",
        json={"now": late.isoformat()},
        headers=workflow_headers(host.org_id),
    ).json()
    assert res["built"] >= 1
    rows = _sql(
        "SELECT content FROM evening_digests WHERE user_id = %(u)s", {"u": str(member.user_id)}
    )
    assert rows[0]["content"]["status"] == "quiet"
    assert [
        n for n in notifications("evening-digest:") if n["user_id"] == str(member.user_id)
    ] == []


def test_whoami(client, redis):
    t = seed_tenant()
    sign_in(t)
    assert client.get("/api/v1/listening/whoami").json() == {
        "user_id": str(t.user_id),
        "org_id": str(t.org_id),
    }


def test_the_evening_schedule_calls_core_per_tenant(monkeypatch):
    from services.workflow_service import scheduled_activities
    from services.workflow_service.workflows import listening as listening_wf

    calls: list[tuple[str, dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"built": 2, "notified": 1, "rules_applied": 3})

    async def tenants() -> list[str]:
        return [str(uuid.uuid4()), str(uuid.uuid4())]

    monkeypatch.setattr(scheduled_activities, "tenant_ids", tenants)
    acts = scheduled_activities.ScheduledActivities(
        core_url="http://core",
        core_secret="s" * 40,
        integration_url="http://integ",
        integration_secret=None,
        transport=httpx.MockTransport(handler),
    )
    out = asyncio.run(acts.listening_evening("2026-11-11T18:05:00+00:00"))
    assert out == {"tenants": 2, "failed": 0, "built": 4, "notified": 2, "rules_applied": 6}
    assert calls[0] == ("/internal/listening/evening", {"now": "2026-11-11T18:05:00+00:00"})
    assert acts.listening_evening in acts.all()
    assert listening_wf.LISTENING_EVENING_CRON == "5 * * * *"
