"""A briefing from your agent, against a real Postgres as `mytheus_app`.

- the consumer: a person's own minutes create one `meeting_end` briefing (theirs on), none when
  they turned briefings off, never twice; the doorbell rings after the commit;
- "Brief me": participants of an ended meeting only (403 / 409 / 404 across tenants), asking
  twice answers the one being written, the hourly cap;
- the internal hand-off: claim -> the source (their promises, who waits on them, what changed,
  the questions left open and where they were carried), a second claim while composing is 409,
  the result is validated (links to things the person may not open are dropped, the length is
  bounded), an after-meeting briefing notifies once, an on-demand one never;
- recovery: a briefing nobody picked up is rung again, and given up after three attempts;
- the server voice: not connected by default (`audio.status` none, no audio URL); connected
  (a fake provider), the audio is rendered and stored under the person's prefix, the
  notification waits for it, the URL is presigned; a provider failure falls back to the device;
- preferences: defaults, update, unknown language or voice refused;
- privacy: another person's briefing is 404; RLS isolates both tables.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import fakeredis
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import briefing_audio, briefings, consumer
from services.core_api.routes import briefings as briefing_routes
from services.core_api.routes import internal_meetings
from services.core_api.routes import voice as voice_routes
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
from tests.test_meeting_minutes import minutes_record, payload
from tests.test_my_tasks import add_user
from tests.test_worklogs import apply

AGENT_SECRET = "b" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"agent-runtime-service={AGENT_SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-briefings-test")
    app.include_router(briefing_routes.router)
    app.include_router(voice_routes.router)
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


@pytest.fixture(autouse=True)
def doorbell(monkeypatch):
    """A fake Redis for the doorbell, listening on every tenant's ring channel."""
    redis = fakeredis.aioredis.FakeRedis()
    rung: list[tuple[str, str]] = []

    async def ring(org_id: uuid.UUID, briefing_id: uuid.UUID) -> bool:
        rung.append((str(org_id), str(briefing_id)))
        await redis.publish(briefings.ring_channel(org_id), str(briefing_id))
        return True

    monkeypatch.setattr(briefings, "ring", ring)
    briefing_audio.set_backend(None)  # not connected unless a test says otherwise
    consumer.AFTER_COMMIT.clear()
    yield rung
    briefing_audio.reset_backend()
    consumer.AFTER_COMMIT.clear()


async def render(org_id: uuid.UUID, briefing: str) -> str:
    """`render_audio` on this test's event loop (the app's engine lives on the client's loop)."""
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    try:
        return await briefings.render_audio(org_id, uuid.UUID(briefing))
    finally:
        await db_mod.get_engine().dispose()
        db_mod.get_engine.cache_clear()
        db_mod.get_sessionmaker.cache_clear()


def _agent_headers(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": AGENT_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def meeting(
    *, ended: bool = True, title: str = "Pricing review"
) -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    _sql(
        "UPDATE users SET email = 'maya.patel@example.com', agent_name = 'Juniper', "
        "agent_personality = 'friendly' WHERE id = %(u)s",
        {"u": str(host.user_id)},
    )
    member = add_user(host)
    _sql(
        "UPDATE users SET email = 'sam.lee@example.com' WHERE id = %(u)s",
        {"u": str(member.user_id)},
    )
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = %(t)s, status = %(s)s WHERE id = %(m)s",
        {"t": title, "s": "ended" if ended else "live", "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    return host, member, m


def seed_records(host: Tenant, member: Tenant, m: uuid.UUID) -> dict[str, str]:
    """A decision, the member's task (due tomorrow, with an unanswered status ask from the
    host), their open promise, a saved question and a follow-up meeting."""
    o = str(host.org_id)
    decision = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, status, confidence, quote_start_ms) "
        "VALUES (%(o)s, %(m)s, 'Raise the base price by 8%%', 'confirmed', 0.9, 61000) "
        "RETURNING id",
        {"o": o, "m": str(m)},
    )[0]["id"]
    action = _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, due_date, status, "
        "confidence, quote_start_ms) VALUES (%(o)s, %(m)s, 'Send the vendor comparison', "
        "%(u)s, (now() + interval '1 day')::date, 'confirmed', 0.9, 90000) RETURNING id",
        {"o": o, "m": str(m), "u": str(member.user_id)},
    )[0]["id"]
    _sql(
        "INSERT INTO action_status_requests (org_id, action_id, user_id, meeting_id, state, "
        "requested_by) VALUES (%(o)s, %(a)s, %(u)s, %(m)s, 'pending', %(by)s)",
        {"o": o, "a": str(action), "u": str(member.user_id), "m": str(m), "by": str(host.user_id)},
    )
    promise = _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, due_hint, status, "
        "action_item_id, source_window_id) VALUES (%(o)s, %(u)s, %(m)s, "
        "'Share the draft with legal', 'Friday', 'open', %(a)s, 'w1') RETURNING id",
        {"o": o, "u": str(member.user_id), "m": str(m), "a": str(action)},
    )[0]["id"]
    _sql(
        "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, question, priority, "
        "status, confidence) VALUES (%(o)s, %(u)s, %(m)s, 'Who owns the reseller terms?', "
        "2, 'saved', 0.8)",
        {"o": o, "u": str(member.user_id), "m": str(m)},
    )
    follow = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = 'Pricing follow-up', previous_meeting_id = %(p)s, "
        "status = 'scheduled', scheduled_start = now() + interval '2 days' WHERE id = %(f)s",
        {"p": str(m), "f": str(follow)},
    )
    return {"decision": str(decision), "action": str(action), "promise": str(promise)}


def briefing_rows(user_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM agent_briefings WHERE user_id = %(u)s ORDER BY created_at",
        {"u": str(user_id)},
    )


def notifications_for(briefing_id: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT payload FROM outbox WHERE topic = 'notifications' "
        "AND payload->>'source_event_id' = %(s)s",
        {"s": f"briefing:{briefing_id}"},
    )


async def personal_minutes(host: Tenant, member: Tenant, m: uuid.UUID) -> None:
    body = payload(
        "personal",
        unanswered_questions=[{"text": "Does legal need to review?", "asked_by_name": "Ana"}],
    )
    assert await apply(minutes_record(host, m, member.user_id, body)) is True


def script(segments: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "status": "ready",
        "prompt_version": "briefing@v1",
        "language": "en",
        "segments": segments
        or [
            {"kind": "opening", "text": "Hi Sam, it's Juniper with your pricing review briefing."},
            {"kind": "changed", "text": "The base price goes up by eight percent."},
        ],
    }


# --- after a meeting --------------------------------------------------------------------------


async def test_own_minutes_create_one_after_meeting_briefing(client, doorbell):
    host, member, m = meeting()
    await personal_minutes(host, member, m)
    rows = briefing_rows(member.user_id)
    assert [(r["trigger"], r["status"], str(r["meeting_id"])) for r in rows] == [
        ("meeting_end", "pending", str(m))
    ]
    assert rows[0]["agent"]["name"] == "Sam's Myte"  # no agent name: "{first}'s Myte"
    # Rung only after the commit.
    assert doorbell == []
    await consumer.drain_after_commit()
    assert doorbell == [(str(host.org_id), str(rows[0]["id"]))]
    # The shared minutes create nothing; a later generation does not create a second one.
    assert await apply(minutes_record(host, m, uuid.UUID(int=0), payload("shared"))) is True
    later = payload("personal", summary="Second run.")
    assert await apply(minutes_record(host, m, member.user_id, later, "g2")) is True
    assert len(briefing_rows(member.user_id)) == 1


async def test_briefings_off_means_no_after_meeting_briefing(client):
    host, member, m = meeting()
    sign_in(member)
    res = client.put(
        "/api/v1/briefings/preferences",
        json={"enabled": False, "auto_play": True, "length": "short", "language": "es"},
    )
    assert res.status_code == 200, res.text
    await personal_minutes(host, member, m)
    assert briefing_rows(member.user_id) == []


# --- Brief me ---------------------------------------------------------------------------------


async def test_brief_me_rules(client, doorbell):
    host, member, m = meeting()
    sign_in(member)
    res = client.post("/api/v1/briefings", json={"meeting_id": str(m)})
    assert res.status_code == 202, res.text
    first = res.json()
    assert first["status"] == "pending" and first["trigger"] == "on_demand"
    assert first["meeting_title"] == "Pricing review" and first["audio"]["status"] == "none"
    assert doorbell[-1] == (str(host.org_id), first["id"])
    again = client.post("/api/v1/briefings", json={"meeting_id": str(m)})
    assert again.status_code == 200 and again.json()["id"] == first["id"]
    # Without a meeting: about the latest one, a separate briefing.
    assert client.post("/api/v1/briefings", json={}).json()["id"] != first["id"]

    live_host, live_member, live = meeting(ended=False)
    sign_in(live_member)
    assert client.post("/api/v1/briefings", json={"meeting_id": str(live)}).status_code == 409
    outsider = add_user(live_host)
    sign_in(outsider)
    res = client.post("/api/v1/briefings", json={"meeting_id": str(live)})
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    other = seed_tenant()
    sign_in(other)
    assert client.post("/api/v1/briefings", json={"meeting_id": str(m)}).status_code == 404


async def test_brief_me_is_capped_per_hour(client):
    host, member, m = meeting()
    sign_in(member)
    for _ in range(briefings.ON_DEMAND_PER_HOUR):
        _sql(
            "INSERT INTO agent_briefings (org_id, user_id, trigger, status) "
            "VALUES (%(o)s, %(u)s, 'on_demand', 'ready')",
            {"o": str(host.org_id), "u": str(member.user_id)},
        )
    res = client.post("/api/v1/briefings", json={"meeting_id": str(m)})
    assert res.status_code == 429 and res.json()["code"] == "RATE_LIMITED"


# --- the hand-off -----------------------------------------------------------------------------


async def test_claim_answers_the_source_and_result_is_validated(client):
    host, member, m = meeting()
    ids = seed_records(host, member, m)
    await personal_minutes(host, member, m)
    briefing = str(briefing_rows(member.user_id)[0]["id"])
    url = f"/internal/briefings/{briefing}"

    assert client.post(f"{url}/claim").status_code == 401  # no caller token
    res = client.post(f"{url}/claim", headers=_agent_headers(host.org_id))
    assert res.status_code == 200, res.text
    src = res.json()
    assert src["person"] == {"name": "Sam Lee", "first_name": "Sam"}
    assert src["agent"]["name"] == "Sam's Myte" and src["target_words"] == 220
    assert src["meeting"]["title"] == "Pricing review"
    assert src["minutes"]["summary"].startswith("Agreed the price rise")
    assert [c["text"] for c in src["changed"]][:1] == ["Raise the base price by 8%"]
    assert src["changed"][0]["at_ms"] == 61000
    waiting = src["waiting"][0]
    assert (waiting["text"], waiting["who"], waiting["status"]) == (
        "Send the vendor comparison",
        "Maya Patel",
        "asked you for an update",
    )
    assert len(src["waiting"]) == 1  # the same task is not listed twice
    assert "Send the vendor comparison" not in [c["text"] for c in src["changed"]]
    assert src["promises"][0]["text"] == "Share the draft with legal"
    assert src["promises"][0]["at_ms"] == 90000
    questions = {q["text"]: q for q in src["questions"]}
    assert questions["Does legal need to review?"]["carried_to"].startswith("Pricing follow-up")
    assert questions["Who owns the reseller terms?"]["status"] == "your Myte saved it for next time"
    # Taken: nothing more to claim while it is being written.
    assert client.post(f"{url}/claim", headers=_agent_headers(host.org_id)).status_code == 409
    # Another tenant's agent token cannot see it.
    other = seed_tenant()
    assert client.post(f"{url}/claim", headers=_agent_headers(other.org_id)).status_code == 404

    foreign_host, _, foreign_meeting = meeting()
    foreign_decision = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, status, confidence) "
        "VALUES (%(o)s, %(m)s, 'x', 'confirmed', 0.9) RETURNING id",
        {"o": str(foreign_host.org_id), "m": str(foreign_meeting)},
    )[0]["id"]
    segments = [
        {"kind": "opening", "text": "  Hi Sam,   it's your Myte.  "},
        {
            "kind": "changed",
            "text": "Price is up eight percent.",
            "link": {
                "type": "decision",
                "meeting_id": str(m),
                "id": ids["decision"],
                "at_ms": 61000,
            },
        },
        {
            "kind": "waiting",
            "text": "Maya is waiting on the vendor comparison.",
            "link": {"type": "action", "meeting_id": str(m), "id": ids["action"], "at_ms": -5},
        },
        {
            "kind": "promise",
            "text": "A foreign link is dropped, the sentence kept.",
            "link": {"type": "decision", "meeting_id": str(m), "id": str(foreign_decision)},
        },
        {"kind": "weird", "text": "Unknown kinds become changes."},
        {"kind": "closing", "text": "   "},
    ]
    res = client.post(f"{url}/result", json=script(segments), headers=_agent_headers(host.org_id))
    assert res.status_code == 200, res.text
    assert res.json() == {"accepted": True, "status": "ready"}

    sign_in(member)
    got = client.get(f"/api/v1/briefings/{briefing}").json()
    assert got["status"] == "ready" and got["prompt_version"] == "briefing@v1"
    assert [s["id"] for s in got["segments"]] == ["s1", "s2", "s3", "s4", "s5"]
    assert got["segments"][0]["text"] == "Hi Sam, it's your Myte."
    assert got["segments"][1]["link"] == {
        "type": "decision",
        "meeting_id": str(m),
        "id": ids["decision"],
        "at_ms": 61000,
    }
    assert got["segments"][2]["link"]["at_ms"] is None  # out of range
    assert got["segments"][3]["link"] is None
    assert got["segments"][4]["kind"] == "changed"
    assert got["word_count"] > 0 and got["duration_s"] >= 1
    # The after-meeting briefing notifies once, with a link to it.
    sent = notifications_for(briefing)
    assert len(sent) == 1
    assert sent[0]["payload"]["link"] == f"/briefings/{briefing}"
    assert sent[0]["payload"]["title"] == "Sam's Myte has your briefing"
    # A second answer is ignored; the first one stands.
    res = client.post(f"{url}/result", json=script(), headers=_agent_headers(host.org_id))
    assert res.json() == {"accepted": False, "status": "ready"}

    # It is the person's own: the host cannot read it.
    sign_in(host)
    assert client.get(f"/api/v1/briefings/{briefing}").status_code == 404
    # Heard.
    sign_in(member)
    heard = client.post(f"/api/v1/briefings/{briefing}/listened").json()
    assert heard["listened_at"] is not None
    latest = client.get(f"/api/v1/meetings/{m}/briefing").json()
    assert latest["briefing"]["id"] == briefing and latest["can_request"] is True
    assert client.get("/api/v1/briefings/latest").json()["briefing"]["id"] == briefing
    assert client.get("/api/v1/briefings").json()["items"][0]["id"] == briefing
    # Not connected: no audio.
    assert client.get(f"/api/v1/briefings/{briefing}/audio").status_code == 404


async def test_the_briefing_greets_by_the_real_name_not_the_email(client):
    """A person whose email is `nnm23ad071@nmamit.in` and whose name is Samar is greeted as
    Samar, and their agent is "Samar's Myte"; whoever waits on them is named the same way."""
    host, member, m = meeting()
    _sql(
        "UPDATE users SET email = 'nnm23ad071@nmamit.in', display_name = 'Samar Rao' "
        "WHERE id = %(u)s",
        {"u": str(member.user_id)},
    )
    _sql("UPDATE users SET display_name = 'Maya P.' WHERE id = %(u)s", {"u": str(host.user_id)})
    seed_records(host, member, m)
    await personal_minutes(host, member, m)
    briefing = str(briefing_rows(member.user_id)[0]["id"])

    res = client.post(f"/internal/briefings/{briefing}/claim", headers=_agent_headers(host.org_id))
    assert res.status_code == 200, res.text
    src = res.json()
    assert src["person"] == {"name": "Samar Rao", "first_name": "Samar"}
    assert src["agent"]["name"] == "Samar's Myte"
    assert src["waiting"][0]["who"] == "Maya P."


async def test_on_demand_briefings_do_not_notify_and_length_is_bounded(client):
    host, member, m = meeting()
    sign_in(member)
    client.put("/api/v1/briefings/preferences", json={"length": "short"})
    briefing = client.post("/api/v1/briefings", json={"meeting_id": str(m)}).json()["id"]
    src = client.post(
        f"/internal/briefings/{briefing}/claim", headers=_agent_headers(host.org_id)
    ).json()
    assert src["length"] == "short" and src["target_words"] == 110
    long_text = " ".join(["word"] * 60)
    segs = [{"kind": "changed", "text": long_text} for _ in range(10)]
    client.post(
        f"/internal/briefings/{briefing}/result",
        json=script(segs),
        headers=_agent_headers(host.org_id),
    )
    got = client.get(f"/api/v1/briefings/{briefing}").json()
    assert got["status"] == "ready" and got["word_count"] <= 176  # 1.6x the short target
    assert notifications_for(briefing) == []


async def test_empty_and_unavailable_results(client):
    host, member, m = meeting()
    sign_in(member)
    a = client.post("/api/v1/briefings", json={"meeting_id": str(m)}).json()["id"]
    client.post(f"/internal/briefings/{a}/claim", headers=_agent_headers(host.org_id))
    client.post(
        f"/internal/briefings/{a}/result",
        json={"status": "ready", "prompt_version": "briefing@v1", "segments": []},
        headers=_agent_headers(host.org_id),
    )
    assert client.get(f"/api/v1/briefings/{a}").json()["status"] == "empty"
    b = client.post("/api/v1/briefings", json={}).json()["id"]
    client.post(
        f"/internal/briefings/{b}/result",
        json={"status": "unavailable", "reason": "budget", "prompt_version": "briefing@v1"},
        headers=_agent_headers(host.org_id),
    )
    got = client.get(f"/api/v1/briefings/{b}").json()
    assert (got["status"], got["reason"]) == ("unavailable", "budget")


# --- recovery ---------------------------------------------------------------------------------


async def test_a_briefing_nobody_picked_up_is_rung_again_then_given_up(client, doorbell):
    host, member, m = meeting()
    sign_in(member)
    briefing = client.post("/api/v1/briefings", json={"meeting_id": str(m)}).json()["id"]
    rings = len(doorbell)
    _sql(
        "UPDATE agent_briefings SET rung_at = now() - interval '1 minute' WHERE id = %(b)s",
        {"b": briefing},
    )
    assert client.get(f"/api/v1/briefings/{briefing}").json()["status"] == "pending"
    assert len(doorbell) == rings + 1
    # Taken, then the agent went away: back to pending and rung.
    client.post(f"/internal/briefings/{briefing}/claim", headers=_agent_headers(host.org_id))
    _sql(
        "UPDATE agent_briefings SET claimed_at = now() - interval '10 minutes' WHERE id = %(b)s",
        {"b": briefing},
    )
    assert client.get(f"/api/v1/briefings/{briefing}").json()["status"] == "pending"
    # Out of attempts.
    _sql(
        "UPDATE agent_briefings SET attempts = 3, rung_at = now() - interval '1 minute' "
        "WHERE id = %(b)s",
        {"b": briefing},
    )
    got = client.get(f"/api/v1/briefings/{briefing}").json()
    assert (got["status"], got["reason"]) == ("unavailable", "timeout")
    assert (
        client.post(
            f"/internal/briefings/{briefing}/claim", headers=_agent_headers(host.org_id)
        ).status_code
        == 409
    )


# --- the server voice -------------------------------------------------------------------------


async def test_server_voice_renders_stores_and_then_notifies(client):
    fake = briefing_audio.FakeAudio()
    briefing_audio.set_backend(fake)
    host, member, m = meeting()
    await personal_minutes(host, member, m)
    briefing = str(briefing_rows(member.user_id)[0]["id"])
    client.post(f"/internal/briefings/{briefing}/claim", headers=_agent_headers(host.org_id))
    _sql(
        "INSERT INTO briefing_preferences (user_id, org_id, server_voice) "
        "VALUES (%(u)s, %(o)s, 'bm_george')",
        {"u": str(member.user_id), "o": str(host.org_id)},
    )
    sign_in(member)
    # Render it in-process (the route schedules the same function after its response).
    import services.core_api.briefings as mod

    scheduled: list[tuple[uuid.UUID, uuid.UUID]] = []
    original = mod.schedule_audio
    mod.schedule_audio = lambda o, b: scheduled.append((o, b))  # type: ignore[assignment]
    try:
        client.post(
            f"/internal/briefings/{briefing}/result",
            json=script(),
            headers=_agent_headers(host.org_id),
        )
    finally:
        mod.schedule_audio = original  # type: ignore[assignment]
    assert scheduled == [(host.org_id, uuid.UUID(briefing))]
    assert client.get(f"/api/v1/briefings/{briefing}").json()["audio"]["status"] == "pending"
    assert notifications_for(briefing) == []  # waits for the voice
    assert await render(host.org_id, briefing) == "ready"
    key = f"{host.org_id}/briefings/{member.user_id}/{briefing}.mp3"
    assert key in fake.objects
    text_, voice, language = fake.spoken[0]
    assert text_.startswith("Hi Sam") and voice == "bm_george" and language == "en"
    got = client.get(f"/api/v1/briefings/{briefing}").json()
    assert got["audio"] == {"status": "ready", "voice": "bm_george"}
    url = client.get(f"/api/v1/briefings/{briefing}/audio").json()
    assert url["url"].startswith("https://storage.test/") and url["mime"] == "audio/mpeg"
    assert len(notifications_for(briefing)) == 1
    settings = client.get("/api/v1/briefings/preferences").json()
    assert settings["server_voice_connected"] is True and len(settings["server_voices"]) >= 4


async def test_a_failed_voice_falls_back_to_the_device_and_still_notifies(client):
    briefing_audio.set_backend(briefing_audio.FakeAudio(fail=True))
    host, member, m = meeting()
    await personal_minutes(host, member, m)
    briefing = str(briefing_rows(member.user_id)[0]["id"])
    import services.core_api.briefings as mod

    original = mod.schedule_audio
    mod.schedule_audio = lambda o, b: None  # type: ignore[assignment]
    try:
        client.post(
            f"/internal/briefings/{briefing}/result",
            json=script(),
            headers=_agent_headers(host.org_id),
        )
    finally:
        mod.schedule_audio = original  # type: ignore[assignment]
    assert await render(host.org_id, briefing) == "failed"
    sign_in(member)
    assert client.get(f"/api/v1/briefings/{briefing}").json()["audio"]["status"] == "failed"
    assert len(notifications_for(briefing)) == 1


# --- preferences and isolation ----------------------------------------------------------------


async def test_preferences_defaults_update_and_validation(client):
    t = seed_tenant()
    sign_in(t)
    got = client.get("/api/v1/briefings/preferences").json()
    assert {k: got[k] for k in ("enabled", "auto_play", "length", "language", "rate")} == {
        "enabled": True,
        "auto_play": False,
        "length": "standard",
        "language": "en",
        "rate": 1.0,
    }
    assert got["server_voice_connected"] is False
    assert {"code": "hi", "name": "Hindi"} in got["languages"]
    res = client.put(
        "/api/v1/briefings/preferences",
        json={"auto_play": True, "voice": "  Samantha  (Enhanced) ", "rate": 1.25},
    )
    assert res.status_code == 200
    assert res.json()["voice"] == "Samantha (Enhanced)" and res.json()["rate"] == 1.25
    again = client.get("/api/v1/briefings/preferences").json()
    assert again["auto_play"] is True and again["voice"] == "Samantha (Enhanced)"
    assert client.put("/api/v1/briefings/preferences", json={"language": "xx"}).status_code == 422
    assert (
        client.put("/api/v1/briefings/preferences", json={"server_voice": "nope"}).status_code
        == 422
    )
    assert client.put("/api/v1/briefings/preferences", json={"rate": 3}).status_code == 422
    assert client.put("/api/v1/briefings/preferences", json={"bogus": 1}).status_code == 422


async def test_rls_isolates_briefings_and_preferences(client):
    a_host, a_member, a_m = meeting()
    await personal_minutes(a_host, a_member, a_m)
    _sql(
        "INSERT INTO briefing_preferences (user_id, org_id) VALUES (%(u)s, %(o)s)",
        {"u": str(a_member.user_id), "o": str(a_host.org_id)},
    )
    b = seed_tenant()
    import psycopg2

    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org', %s, false)", (str(b.org_id),))
            cur.execute("SELECT count(*) FROM agent_briefings")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM briefing_preferences")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT set_config('app.current_org', '', false)")
            cur.execute("SELECT count(*) FROM agent_briefings")
            assert cur.fetchone()[0] == 0  # fail closed with no tenant
            cur.execute("SELECT set_config('app.current_org', %s, false)", (str(a_host.org_id),))
            cur.execute("SELECT count(*) FROM agent_briefings")
            assert cur.fetchone()[0] >= 1
    finally:
        conn.close()


def test_payload_shapes_are_json_safe():
    assert json.loads(json.dumps(script()))["segments"][0]["kind"] == "opening"


# --- every Myte's voice (Kokoro, /api/v1/voice) -------------------------------------------------


def test_kokoro_is_the_default_provider_with_curated_voices(monkeypatch):
    monkeypatch.delenv("TTS_PROVIDER", raising=False)
    assert briefing_audio.provider() == "kokoro"
    ids = [v.id for v in briefing_audio.server_voices()]
    assert ids[0] == "af_heart" and "bm_george" in ids and len(ids) >= 8
    assert briefing_audio.voice_for(None, "friendly") == "af_bella"
    assert briefing_audio.voice_for("am_puck", "friendly") == "am_puck"
    assert briefing_audio.voice_for("not-a-voice", None) == "af_heart"
    # Another language speaks with that language's own Kokoro voice.
    assert briefing_audio.voice_for("am_puck", "friendly", "es") == "ef_dora"
    monkeypatch.setenv("TTS_PROVIDER", "none")
    assert briefing_audio.server_voices() == () and briefing_audio.speaker() is None


def test_voices_lists_the_choice_and_speech_streams_in_the_chosen_voice(client):
    fake = briefing_audio.FakeSpeaker()
    briefing_audio.set_speaker(fake)
    try:
        host, member, _m = meeting()
        sign_in(host)  # host's Myte is 'friendly'
        got = client.get("/api/v1/voice/voices").json()
        assert got["connected"] is True and got["chosen"] is None
        assert got["selected"] == "af_bella"
        assert {"id", "name", "description", "accent", "gender"} <= set(got["voices"][0])
        assert client.put("/api/v1/voice/preference", json={"voice": "nope"}).status_code == 422
        chosen = client.put("/api/v1/voice/preference", json={"voice": "bm_george"}).json()
        assert chosen["chosen"] == "bm_george" and chosen["selected"] == "bm_george"
        # The same choice is the briefing's server voice.
        assert client.get("/api/v1/briefings/preferences").json()["server_voice"] == "bm_george"
        res = client.post("/api/v1/voice/speech", json={"text": "Hello  there. "})
        assert res.status_code == 200 and res.headers["content-type"] == "audio/mpeg"
        assert res.headers["x-myte-voice"] == "bm_george"
        assert res.content.startswith(b"ID3fake-mp3:")
        assert fake.spoken[-1] == ("Hello there.", "bm_george", 1.0)
        assert client.post("/api/v1/voice/speech", json={"text": ""}).status_code == 422
        assert client.post("/api/v1/voice/speech", json={"text": "x" * 1501}).status_code == 422
    finally:
        briefing_audio.reset_speaker()


def test_speech_without_a_voice_service_is_a_clear_503(client):
    briefing_audio.set_speaker(briefing_audio.FakeSpeaker(fail=True))
    try:
        host, _member, _m = meeting()
        sign_in(host)
        res = client.post("/api/v1/voice/speech", json={"text": "Hi"})
        assert res.status_code == 503 and res.json()["reason"] == "voice_not_connected"
        briefing_audio.set_speaker(None)
        assert client.post("/api/v1/voice/speech", json={"text": "Hi"}).status_code == 503
        assert client.get("/api/v1/voice/voices").json()["connected"] is False
    finally:
        briefing_audio.reset_speaker()


def test_briefing_speech_is_split_into_short_ordered_pieces():
    text = (
        "Someone asked for an update on the login API, which is due November 18th. "
        "It's the earliest deadline on your list. " * 6
    )
    pieces = briefing_audio.split_for_speech(text, limit=160)
    assert len(pieces) > 3 and all(len(p) <= 160 for p in pieces)
    assert " ".join(pieces) == " ".join(text.split())
    long = "word " * 120
    assert all(len(p) <= 160 for p in briefing_audio.split_for_speech(long, limit=160))


async def test_kokoro_renders_pieces_concurrently_and_joins_them_in_order(monkeypatch):
    calls: list[str] = []

    async def fake_speech(base_url, text, *, voice, speed=1.0):  # type: ignore[no-untyped-def]
        calls.append(text)
        return f"<{text[:6]}>".encode()

    monkeypatch.setattr(briefing_audio, "kokoro_speech", fake_speech)
    backend = briefing_audio.KokoroS3(
        {"S3_ACCESS_KEY_ID": "k", "S3_SECRET_ACCESS_KEY": "s", "S3_ENDPOINT_URL": "http://s3.test"}
    )
    text = "First one here. Second one here. Third one here."
    monkeypatch.setattr(briefing_audio, "PIECE_CHARS", 16)
    data = await backend.synthesize(text, voice="af_heart", language="en")
    assert data == b"<First ><Second><Third >"
    assert len(calls) == 3


def test_transcribe_turns_a_recording_into_text(client, monkeypatch):
    host, _member, _m = meeting()
    sign_in(host)

    async def fake(data, content_type, language):  # type: ignore[no-untyped-def]
        assert data == b"webm-bytes" and content_type == "audio/webm"
        return "What did we decide about pricing?"

    monkeypatch.setattr(voice_routes, "transcribe_audio", fake)
    res = client.post(
        "/api/v1/voice/transcribe",
        content=b"webm-bytes",
        headers={"content-type": "audio/webm;codecs=opus"},
    )
    assert res.status_code == 200 and res.json() == {"text": "What did we decide about pricing?"}
    bad = client.post(
        "/api/v1/voice/transcribe", content=b"x", headers={"content-type": "text/plain"}
    )
    assert bad.status_code == 422

    async def down(data, content_type, language):  # type: ignore[no-untyped-def]
        import httpx

        raise httpx.ConnectError("down")

    monkeypatch.setattr(voice_routes, "transcribe_audio", down)
    res = client.post(
        "/api/v1/voice/transcribe", content=b"a", headers={"content-type": "audio/webm"}
    )
    assert res.status_code == 503 and res.json()["reason"] == "listening_not_connected"


def test_short_phrases_are_cached_so_fillers_come_back_instantly(client):
    fake = briefing_audio.FakeSpeaker()
    briefing_audio.set_speaker(fake)
    try:
        host, _member, _m = meeting()
        sign_in(host)
        phrase = f"Let me check, {uuid.uuid4().hex[:6]}."
        first = client.post("/api/v1/voice/speech", json={"text": phrase})
        assert first.status_code == 200 and "x-voice-cache" not in first.headers
        spoken = len(fake.spoken)
        again = client.post("/api/v1/voice/speech", json={"text": phrase})
        assert again.status_code == 200 and again.headers["x-voice-cache"] == "hit"
        assert again.content == first.content and len(fake.spoken) == spoken
        long = "word " * 60
        client.post("/api/v1/voice/speech", json={"text": long})
        client.post("/api/v1/voice/speech", json={"text": long})
        assert len(fake.spoken) == spoken + 2  # long text is never cached
    finally:
        briefing_audio.reset_speaker()


def test_speech_spells_out_dates_and_times_so_kokoro_reads_them_naturally():
    say = briefing_audio.speakable
    assert say("Due Nov 18th, before Fri, Nov 20 at 11:00 PM [2].") == (
        "Due November 18th, before Friday, November 20 at 11 PM."
    )
    assert say("Starts 9:30 am, e.g. the sync & retro") == (
        "Starts 9:30 AM, for example the sync and retro"
    )
    # Words that only look like abbreviations are left alone.
    assert say("The Sun rose; Mar is a name") == "The Sun rose; Mar is a name"
    assert say("**Pricing** vs the plan") == "Pricing versus the plan"
