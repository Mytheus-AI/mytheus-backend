"""The dashboard composite. Plan section 3.9 acceptance checks."""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.bff import dashboard as dashboard_mod
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _seed_tenant(tier: str = "member"):
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"bff-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(org_id), "u": str(user_id), "t": tier},
    )
    PRINCIPAL.clerk_org = clerk_org
    PRINCIPAL.clerk_user = clerk_user
    return org_id, user_id


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("bff-test")
    app.include_router(dashboard_mod.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c

    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def test_composite_shape_with_the_real_meetings_arm(client):
    """Plan 3.9 acceptance 1: real meetings, stable empty arms for the rest."""
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        "VALUES (%(o)s, 'Standup', 'scheduled', now() + interval '1 day', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )

    body = client.get("/bff/dashboard").json()
    assert set(body) == {
        "upcoming_meetings",
        "action_items",
        "worklogs",
        "decisions",
        "recent_meetings",
        "needs_review",
    }

    assert body["upcoming_meetings"]["status"] == "ok"
    assert [m["title"] for m in body["upcoming_meetings"]["items"]] == ["Standup"]

    for arm in ("action_items", "worklogs", "decisions", "recent_meetings", "needs_review"):
        # `empty`, not `degraded`: this caller genuinely has nothing in them, which is a
        # different empty state from "we could not load this".
        assert body[arm]["status"] == "empty", arm
        assert body[arm]["items"] == []


def test_arms_are_org_scoped(client):
    """One tenant's dashboard never shows another's meetings."""
    org_a, user_a = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'A-only', 'scheduled', %(u)s)",
        {"o": str(org_a), "u": str(user_a)},
    )

    _seed_tenant()  # signs in as a different tenant
    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["items"] == []
    assert body["upcoming_meetings"]["status"] == "empty"


def test_ended_meetings_are_not_upcoming(client):
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'Finished', 'ended', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["items"] == []


def test_undated_meetings_sort_after_dated_ones(client):
    """NULLS LAST. Default ASC puts NULLs first and they would monopolise the panel."""
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'No date', 'scheduled', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        "VALUES (%(o)s, 'Tomorrow', 'scheduled', now() + interval '1 day', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    titles = [m["title"] for m in client.get("/bff/dashboard").json()["upcoming_meetings"]["items"]]
    assert titles == ["Tomorrow", "No date"]


def test_a_slow_arm_degrades_and_does_not_500(client, monkeypatch):
    """Plan 3.9 acceptance 3. The screen survives one broken panel."""
    _seed_tenant()
    monkeypatch.setattr(dashboard_mod, "ARM_TIMEOUT_SECONDS", 0.01)

    import asyncio

    original = dashboard_mod._arm

    async def slow_arm(name, arm, load):
        if name == "upcoming_meetings":

            async def _hang():
                await asyncio.sleep(5)
                return []

            return await original(name, arm, _hang)
        return await original(name, arm, load)

    monkeypatch.setattr(dashboard_mod, "_arm", slow_arm)

    resp = client.get("/bff/dashboard")
    assert resp.status_code == 200, "a slow arm must not fail the screen"
    body = resp.json()
    assert body["upcoming_meetings"]["status"] == "degraded"
    assert body["upcoming_meetings"]["items"] == []
    # The other panels still resolved, which is the entire point of degrading per arm.
    assert body["decisions"]["status"] == "empty"


def test_a_failing_arm_degrades_and_does_not_500(client, monkeypatch):
    """A raising arm is handled identically to a slow one: show the rest of the screen."""
    _seed_tenant()
    original = dashboard_mod._arm

    async def broken_arm(name, arm, load):
        if name == "upcoming_meetings":

            async def _boom():
                raise RuntimeError("core-api is down")

            return await original(name, arm, _boom)
        return await original(name, arm, load)

    monkeypatch.setattr(dashboard_mod, "_arm", broken_arm)

    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["status"] == "degraded"
    assert body["worklogs"]["status"] == "empty"


def test_bff_exposes_no_mutating_routes(client):
    """Plan 3.9 acceptance 2. A write path here would make bff a second writer."""
    routes = [r for r in client.app.routes if hasattr(r, "methods")]
    for route in routes:
        if str(getattr(route, "path", "")).startswith("/bff"):
            assert route.methods <= {"GET", "HEAD", "OPTIONS"}, f"{route.path} mutates"


def test_my_notes_shows_only_my_surfaced_notes_newest_first(client):
    """The worklogs arm is real since Plan 7: the caller's own surfaced notes, with the meeting
    each came from. A silent note, or anyone else's note, never appears."""
    org_id, me = _seed_tenant()
    other = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'x@y.z')",
        {"u": str(other), "o": str(org_id), "c": f"user_{other.hex[:16]}"},
    )
    meeting = _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'Design review', 'ended', %(u)s) RETURNING id",
        {"o": str(org_id), "u": str(me)},
    )[0]["id"]

    def note(user, content, surfaced, minutes_ago):
        _sql(
            "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, surfaced, "
            "created_at) VALUES (%(o)s, %(m)s, %(u)s, %(w)s, %(c)s, %(s)s, "
            "now() - make_interval(mins => %(a)s))",
            {
                "o": str(org_id),
                "m": str(meeting),
                "u": str(user),
                "w": f"{meeting}:{minutes_ago}",
                "c": content,
                "s": surfaced,
                "a": minutes_ago,
            },
        )

    note(me, "- You own the migration.", True, 10)
    note(me, "- Ship by Friday.", True, 2)
    note(me, "- maybe relevant", False, 1)
    note(other, "- someone else's note", True, 1)

    body = client.get("/bff/dashboard").json()

    assert body["worklogs"]["status"] == "ok"
    assert [(n["content"], n["meeting_title"]) for n in body["worklogs"]["items"]] == [
        ("- Ship by Friday.", "Design review"),
        ("- You own the migration.", "Design review"),
    ]


def _meeting(org_id, user_id, title, status="ended", start="now() - interval '1 day'"):
    mid = _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        f"VALUES (%(o)s, %(t)s, %(s)s, {start}, %(u)s) RETURNING id",
        {"o": str(org_id), "t": title, "s": status, "u": str(user_id)},
    )[0]["id"]
    return mid


def _join(meeting_id, org_id, user_id):
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": str(meeting_id), "u": str(user_id), "o": str(org_id)},
    )


def _decision(org_id, meeting_id, content, status="confirmed", confidence=0.9):
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, %(f)s, %(s)s)",
        {"o": str(org_id), "m": str(meeting_id), "c": content, "f": confidence, "s": status},
    )


def _action(org_id, meeting_id, content, assignee, status="confirmed", confidence=0.9, due=None):
    _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, status, confidence, "
        "due_date) VALUES (%(o)s, %(m)s, %(c)s, %(a)s, %(s)s, %(f)s, %(d)s)",
        {
            "o": str(org_id),
            "m": str(meeting_id),
            "c": content,
            "a": str(assignee) if assignee else None,
            "s": status,
            "f": confidence,
            "d": due,
        },
    )


def _other_user(org_id):
    other = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'o@example.com')",
        {"u": str(other), "o": str(org_id), "c": f"user_{other.hex[:16]}"},
    )
    return other


def test_tasks_arm_is_my_open_tasks_soonest_due_first(client):
    org_id, me = _seed_tenant()
    other = _other_user(org_id)
    m = _meeting(org_id, me, "Planning")
    _action(org_id, m, "Later", me, due="2030-01-10")
    _action(org_id, m, "Sooner", me, due="2030-01-02")
    _action(org_id, m, "Undated", me)
    _action(org_id, m, "Already done", me, status="done")
    _action(org_id, m, "Only proposed", me, status="proposed")
    _action(org_id, m, "Someone else's", other)

    arm = client.get("/bff/dashboard").json()["action_items"]
    assert arm["status"] == "ok"
    assert [(t["content"], t["meeting_title"]) for t in arm["items"]] == [
        ("Sooner", "Planning"),
        ("Later", "Planning"),
        ("Undated", "Planning"),
    ]


def test_decisions_arm_is_confirmed_decisions_from_my_meetings(client):
    org_id, me = _seed_tenant()
    mine = _meeting(org_id, me, "Mine")
    _join(mine, org_id, me)
    not_mine = _meeting(org_id, me, "Not mine")
    _decision(org_id, mine, "We ship Friday")
    _decision(org_id, mine, "Candidate only", status="proposed")
    _decision(org_id, not_mine, "Private to others")

    arm = client.get("/bff/dashboard").json()["decisions"]
    assert arm["status"] == "ok"
    assert [(d["content"], d["meeting_title"]) for d in arm["items"]] == [
        ("We ship Friday", "Mine")
    ]


def test_recent_meetings_arm_counts_what_was_confirmed(client):
    org_id, me = _seed_tenant()
    other = _other_user(org_id)
    m = _meeting(org_id, me, "Retro")
    _join(m, org_id, me)
    _join(m, org_id, other)
    _decision(org_id, m, "Keep the retro weekly")
    _decision(org_id, m, "Rejected", status="dismissed")
    _action(org_id, m, "Book the room", me)
    _action(org_id, m, "Closed already", other, status="done")
    _action(org_id, m, "Unconfirmed", me, status="proposed")
    upcoming = _meeting(
        org_id, me, "Tomorrow", status="scheduled", start="now() + interval '1 day'"
    )
    _join(upcoming, org_id, me)
    absent = _meeting(org_id, me, "I was not there")
    _join(absent, org_id, other)

    arm = client.get("/bff/dashboard").json()["recent_meetings"]
    assert arm["status"] == "ok"
    assert [
        (r["title"], r["participant_count"], r["decision_count"], r["action_count"])
        for r in arm["items"]
    ] == [("Retro", 2, 1, 2)]


def test_needs_review_arm_lists_surfaced_candidates_in_my_meetings(client):
    org_id, me = _seed_tenant()
    mine = _meeting(org_id, me, "Sync", status="live", start="now()")
    _join(mine, org_id, me)
    not_mine = _meeting(org_id, me, "Elsewhere")
    _decision(org_id, mine, "Adopt the new pricing", status="proposed", confidence=0.9)
    _action(org_id, mine, "Draft the memo", None, status="proposed", confidence=0.85)
    _decision(org_id, mine, "Too unsure to surface", status="proposed", confidence=0.5)
    _decision(org_id, mine, "Already confirmed")
    _decision(org_id, not_mine, "Not my meeting", status="proposed", confidence=0.95)

    arm = client.get("/bff/dashboard").json()["needs_review"]
    assert arm["status"] == "ok"
    assert sorted((i["kind"], i["content"]) for i in arm["items"]) == [
        ("action", "Draft the memo"),
        ("decision", "Adopt the new pricing"),
    ]
    assert {i["meeting_title"] for i in arm["items"]} == {"Sync"}


def test_every_arm_resolves_ok_together(client):
    """All six arms share one session; none of them may degrade because another ran at the
    same time."""
    org_id, me = _seed_tenant()
    m = _meeting(org_id, me, "Everything")
    _join(m, org_id, me)
    _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        "VALUES (%(o)s, 'Next', 'scheduled', now() + interval '1 day', %(u)s)",
        {"o": str(org_id), "u": str(me)},
    )
    _decision(org_id, m, "Confirmed")
    _decision(org_id, m, "Proposed", status="proposed")
    _action(org_id, m, "Mine", me)
    _sql(
        "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, surfaced) "
        "VALUES (%(o)s, %(m)s, %(u)s, 'w1', '- a note', true)",
        {"o": str(org_id), "m": str(m), "u": str(me)},
    )

    body = client.get("/bff/dashboard").json()
    assert {name: arm["status"] for name, arm in body.items()} == {
        "upcoming_meetings": "ok",
        "action_items": "ok",
        "worklogs": "ok",
        "decisions": "ok",
        "recent_meetings": "ok",
        "needs_review": "ok",
    }


def test_another_tenants_tasks_decisions_and_candidates_are_invisible(client):
    org_a, user_a = _seed_tenant()
    m = _meeting(org_a, user_a, "A only")
    _join(m, org_a, user_a)
    _decision(org_a, m, "A decision")
    _decision(org_a, m, "A candidate", status="proposed")
    _action(org_a, m, "A task", user_a)

    _seed_tenant()
    body = client.get("/bff/dashboard").json()
    for arm in ("action_items", "decisions", "recent_meetings", "needs_review"):
        assert body[arm]["items"] == [], arm
