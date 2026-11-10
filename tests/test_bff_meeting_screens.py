"""ADR-0039 B8: the brief, the health card and the catch-up, over HTTP as `mytheus_app`.

- every screen: 404 for another tenant's meeting, 403 for a non-participant;
- brief: the caller's agenda items, their open actions from the previous meeting with the
  latest status, their saved questions, proxies, and nothing from a confidential previous
  meeting the caller was not in;
- health: 409 before the end; duration, people-hours (live participants), coverage, counts,
  async suggestion, the trend through `previous_meeting_id`, and `talk_balance` from
  `transcript_chunks` + `meeting_speakers` (null when nothing is stored);
- catch-up: agenda with agent notes, decisions, actions assigned to me, my notes, my questions.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import meeting_screens
from services.core_api.routes import meetings
from tests.conftest import APP_URL
from tests.test_chat_api import PRINCIPAL, _create_test_app, _sql, seed_tenant, sign_in
from tests.test_suggestions import seed_user


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("bff-meeting-screens-test")
    app.include_router(meetings.router)
    app.include_router(meeting_screens.router)
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


@pytest.fixture
def transcript_table(migrated_db):
    """`transcript_chunks` is `mytheus-realtime`'s; this repo's database may not have it. Make a
    minimal RLS'd stand-in for the test when absent, and drop only what was made here."""
    made = not _sql("SELECT to_regclass('public.transcript_chunks') IS NOT NULL AS p")[0]["p"]
    if made:
        _sql("""
            CREATE TABLE transcript_chunks (
              id uuid NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
              org_id uuid NOT NULL, meeting_id uuid NOT NULL, speaker_id text NOT NULL,
              start_ms integer NOT NULL, end_ms integer NOT NULL, text text NOT NULL,
              is_final boolean NOT NULL DEFAULT true, confidence real,
              redacted boolean NOT NULL DEFAULT false,
              created_at timestamptz NOT NULL DEFAULT now());
            ALTER TABLE transcript_chunks ENABLE ROW LEVEL SECURITY;
            ALTER TABLE transcript_chunks FORCE ROW LEVEL SECURITY;
            CREATE POLICY tenant_isolation ON transcript_chunks
              USING (org_id = NULLIF(current_setting('app.current_org', true), '')::uuid);
            GRANT SELECT ON transcript_chunks TO mytheus_app;
            """)
    yield
    if made:
        _sql("DROP TABLE transcript_chunks")


def create(client: TestClient, **body: Any) -> dict[str, Any]:
    resp = client.post("/api/v1/meetings", json={"title": "Weekly", **body})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _action(org: Any, meeting: str, content: str, assignee: Any, status: str = "confirmed") -> str:
    return str(
        _sql(
            "INSERT INTO action_items (org_id, meeting_id, content, confidence, status, "
            "assignee_id, due_date) VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(a)s, "
            "'2026-12-01') RETURNING id",
            {"o": str(org), "m": meeting, "c": content, "s": status, "a": str(assignee)},
        )[0]["id"]
    )


def _decision(org: Any, meeting: str, content: str) -> None:
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, 'confirmed')",
        {"o": str(org), "m": meeting, "c": content},
    )


def _end(meeting_id: str) -> None:
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": meeting_id})


@pytest.mark.parametrize("screen", ["brief", "health", "catch-up"])
def test_every_screen_is_for_participants_only(client, screen):
    owner = seed_tenant()
    outsider = seed_user(owner)
    sign_in(owner)
    m = create(client)
    _end(m["id"])
    assert client.get(f"/bff/meetings/{m['id']}/{screen}").status_code == 200
    sign_in(outsider)
    resp = client.get(f"/bff/meetings/{m['id']}/{screen}")
    assert resp.status_code == 403 and resp.json()["code"] == "NOT_A_PARTICIPANT"
    sign_in(seed_tenant())
    assert client.get(f"/bff/meetings/{m['id']}/{screen}").status_code == 404


def test_brief(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    earlier = create(client, title="Kickoff", participant_ids=[str(bob.user_id)])
    mine = _action(owner.org_id, earlier["id"], "Draft the plan", owner.user_id)
    _action(owner.org_id, earlier["id"], "Bob's task", bob.user_id)
    _action(owner.org_id, earlier["id"], "Done already", owner.user_id, status="done")
    _sql(
        "INSERT INTO action_status_requests (org_id, action_id, user_id, meeting_id, state, "
        "status_text, answered_at) VALUES (%(o)s, %(a)s, %(u)s, %(m)s, 'answered', 'On track', "
        "now())",
        {"o": str(owner.org_id), "a": mine, "u": str(owner.user_id), "m": earlier["id"]},
    )
    _sql(
        "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, question, priority, status) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'Who owns pricing?', 2, 'saved'), "
        "(%(o)s, %(b)s, %(m)s, 'Bob asks', 2, 'saved')",
        {
            "o": str(owner.org_id),
            "u": str(owner.user_id),
            "b": str(bob.user_id),
            "m": earlier["id"],
        },
    )
    m = create(
        client,
        title="Follow-up",
        previous_meeting_id=earlier["id"],
        participant_ids=[str(bob.user_id)],
        scheduled_start="2026-12-02T09:00:00Z",
        agenda=[
            {"title": "Mine", "owner_user_id": str(owner.user_id)},
            {"title": "Bob's", "owner_user_id": str(bob.user_id)},
        ],
    )
    sign_in(bob)
    client.put(
        f"/api/v1/meetings/{m['id']}/attendance",
        json={"attendance": "agent", "proxy_update": "Out sick", "proxy_questions": ["Secret?"]},
    )
    sign_in(owner)
    body = client.get(f"/bff/meetings/{m['id']}/brief").json()
    assert body["meeting"]["title"] == "Follow-up"
    assert body["meeting"]["scheduled_start"].startswith("2026-12-02T09:00:00")
    assert [i["title"] for i in body["my_agenda_items"]] == ["Mine"]
    assert [(a["title"], a["latest_status"]["text"]) for a in body["my_open_actions"]] == [
        ("Draft the plan", "On track")
    ]
    assert [q["question"] for q in body["saved_questions"]] == ["Who owns pricing?"]
    assert body["proxies"] == [{"user_id": str(bob.user_id), "proxy_update": "Out sick"}]
    assert body["previous_meeting"] == {"id": earlier["id"], "title": "Kickoff"}

    # A confidential previous meeting the caller was not in contributes nothing.
    carol = seed_user(owner)
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": earlier["id"]})
    _action(owner.org_id, earlier["id"], "Carol's", carol.user_id)
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": m["id"], "u": str(carol.user_id), "o": str(owner.org_id)},
    )
    sign_in(carol)
    body = client.get(f"/bff/meetings/{m['id']}/brief").json()
    assert body["previous_meeting"] is None and body["my_open_actions"] == []


def test_health(client, transcript_table):
    owner = seed_tenant()
    bob = seed_user(owner)
    carol = seed_user(owner)
    sign_in(owner)
    chain: list[dict[str, Any]] = []
    previous = None
    for i in range(8):
        extra = {"previous_meeting_id": previous} if previous else {}
        mt = create(client, title=f"M{i}", agenda=[{"title": "A"}, {"title": "B"}], **extra)
        chain.append(mt)
        previous = mt["id"]
    for mt in chain[:-1]:
        _sql(
            "UPDATE meeting_agenda_items SET status = 'covered' WHERE meeting_id = %(m)s "
            "AND title = 'A'",
            {"m": mt["id"]},
        )
    _decision(owner.org_id, chain[5]["id"], "Ship it")
    m = create(
        client,
        title="Now",
        previous_meeting_id=chain[-1]["id"],
        participant_ids=[str(bob.user_id), str(carol.user_id)],
        scheduled_start="2026-12-02T09:00:00Z",
        scheduled_end="2026-12-02T09:45:00Z",
        agenda=[{"title": "X"}, {"title": "Y"}, {"title": "Z"}],
    )
    sign_in(carol)
    client.put(f"/api/v1/meetings/{m['id']}/attendance", json={"attendance": "agent"})
    sign_in(owner)
    assert client.get(f"/bff/meetings/{m['id']}/health").status_code == 409
    _end(m["id"])
    _sql(
        "UPDATE meeting_agenda_items SET status = 'covered' WHERE meeting_id = %(m)s "
        "AND title = 'X'",
        {"m": m["id"]},
    )

    body = client.get(f"/bff/meetings/{m['id']}/health").json()
    assert body["duration_minutes"] == 45
    assert body["people_hours"] == 1.5  # two live participants x 45 minutes
    assert body["agenda"] == {"covered": 1, "total": 3}
    assert (body["decisions"], body["actions"]) == (0, 0)
    assert body["async_suggestion"] is True
    assert body["talk_balance"] is None  # nothing stored for this meeting
    trend = body["trend"]
    assert [p["title"] for p in trend] == ["M2", "M3", "M4", "M5", "M6", "M7", "Now"]
    assert trend[0]["coverage"] == 0.5 and trend[-2]["coverage"] == 0.0
    assert [p["decisions"] for p in trend] == [0, 0, 0, 1, 0, 0, 0]
    assert trend[-1]["coverage"] == 0.333

    # Speaker timing: finals mapped to users through meeting_speakers.
    for speaker, user, start, end in (
        ("spk_0", owner.user_id, 0, 30_000),
        ("spk_1", bob.user_id, 30_000, 40_000),
        ("spk_2", None, 40_000, 90_000),  # unmapped: not counted
    ):
        _sql(
            "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id) "
            "VALUES (%(o)s, %(m)s, %(s)s, %(u)s)",
            {"o": str(owner.org_id), "m": m["id"], "s": speaker, "u": str(user) if user else None},
        )
        _sql(
            "INSERT INTO transcript_chunks (org_id, meeting_id, speaker_id, start_ms, end_ms, "
            "text, is_final) VALUES (%(o)s, %(m)s, %(s)s, %(st)s, %(en)s, 'hi', true)",
            {"o": str(owner.org_id), "m": m["id"], "s": speaker, "st": start, "en": end},
        )
    _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, 'Follow up', 0.9, 'confirmed')",
        {"o": str(owner.org_id), "m": m["id"]},
    )
    body = client.get(f"/bff/meetings/{m['id']}/health").json()
    assert {(s["user_id"], s["share"]) for s in body["talk_balance"]} == {
        (str(owner.user_id), 0.75),
        (str(bob.user_id), 0.25),
    }
    assert body["actions"] == 1 and body["async_suggestion"] is False


def test_catch_up(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    m = create(
        client,
        participant_ids=[str(bob.user_id)],
        agenda=[{"title": "Budget", "owner_user_id": str(bob.user_id)}, {"title": "Hiring"}],
    )
    sign_in(bob)
    client.put(
        f"/api/v1/meetings/{m['id']}/attendance",
        json={"attendance": "agent", "proxy_update": "Away", "proxy_questions": ["Budget cap?"]},
    )
    _sql(
        "UPDATE meeting_agenda_items SET status = 'covered', status_source = 'agent', "
        "agent_note = 'Cap agreed at 50k' WHERE meeting_id = %(m)s AND title = 'Budget'",
        {"m": m["id"]},
    )
    _decision(owner.org_id, m["id"], "Cap at 50k")
    _action(owner.org_id, m["id"], "Bob drafts the budget", bob.user_id)
    _action(owner.org_id, m["id"], "Owner's task", owner.user_id)
    _sql(
        "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, confidence, "
        "surfaced, created_by_agent) VALUES "
        "(%(o)s, %(m)s, %(b)s, 'w0', 'Budget discussed', 0.9, true, true), "
        "(%(o)s, %(m)s, %(b)s, 'w1', 'Silent band', 0.5, false, true), "
        "(%(o)s, %(m)s, %(u)s, 'w0', 'Owner note', 0.9, true, true)",
        {"o": str(owner.org_id), "m": m["id"], "b": str(bob.user_id), "u": str(owner.user_id)},
    )
    _end(m["id"])

    body = client.get(f"/bff/meetings/{m['id']}/catch-up").json()
    assert body["attendance"] == "agent"
    assert [(a["title"], a["status"], a["agent_note"]) for a in body["agenda"]] == [
        ("Budget", "covered", "Cap agreed at 50k"),
        ("Hiring", "pending", None),
    ]
    assert [d["content"] for d in body["decisions"]] == ["Cap at 50k"]
    assert [a["content"] for a in body["assigned_to_me"]] == ["Bob drafts the budget"]
    assert [n["content"] for n in body["my_notes"]] == ["Budget discussed"]
    assert body["my_questions"] == ["Budget cap?"]

    sign_in(owner)
    body = client.get(f"/bff/meetings/{m['id']}/catch-up").json()
    assert body["my_questions"] == [] and [n["content"] for n in body["my_notes"]] == ["Owner note"]
    assert uuid.UUID(body["agenda"][0]["id"])
