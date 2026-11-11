"""The last word, the agreement check and pick up where we stopped, against a real Postgres as
`mytheus_app` (and the local Redis for the gateway's list).

- the analyzer's source: 409 before the meeting ended, 404 across tenants; files the gateway's
  Redis list into `room_signals` once (participants only), on the meeting clock; turns,
  decisions with their owner and moment, agenda on the meeting clock, listen-for topics;
- the consumer: a `room_signals` result becomes at most one nudge per participant, agreement
  reads, private checks (never the owner, never a non-participant) and one resume point, each
  new nudge and check with its in-app notification; redelivery is a no-op; another tenant's
  meeting is poison;
- the routes: after-signals, the last word (list, dismiss, other people's nudges are 404), late
  contributions (written, recorded, Range, the 7-day window, the host notified, delete reopens
  the nudge), agreement (strength for all, aggregates and notes only for the owner, unsigned
  notes only from three answers), resume points (follow-up link, same people, a confidential
  source kept out, continue, host-only dismiss, conflicts), minutes and the recap carry the
  late points; RLS isolation of every new table.
"""

from __future__ import annotations

import base64
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api.consumer import PoisonMessage
from services.core_api.routes import (
    internal_agents,
    internal_meetings,
    internal_room_signals,
    minutes,
    room_signals,
    share_links,
)
from tests.conftest import APP_URL
from tests.test_bff_meeting_screens import transcript_table  # noqa: F401 - a fixture
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
from tests.test_worklogs import apply, result

AGENT_SECRET = "a" * 40
NIL = uuid.UUID(int=0)
T0 = datetime(2026, 11, 10, 9, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ["SHARE_LINK_SIGNING_KEY"] = "k" * 40
    os.environ[internal_meetings.TOKENS_ENV] = f"agent-runtime-service={AGENT_SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-room-signals-test")
    for router in (
        room_signals.router,
        internal_room_signals.router,
        internal_agents.router,
        minutes.router,
        share_links.router,
        share_links.public_router,
    ):
        app.include_router(router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    for key in ("SHARE_LINK_SIGNING_KEY", internal_meetings.TOKENS_ENV):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _agent(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": AGENT_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def _named(t: Tenant, email: str) -> Tenant:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})
    return t


class Room:
    """A meeting with a host and three members, all named."""

    def __init__(self, *, ended: bool = True, title: str = "Pricing review") -> None:
        self.host = _named(seed_tenant(), "maya.patel@acme.test")
        self.raj = _named(add_user(self.host), "raj.iyer@acme.test")
        self.ana = _named(add_user(self.host), "ana.silva@acme.test")
        self.sam = _named(add_user(self.host), "sam.lee@acme.test")
        self.meeting = seed_meeting(self.host.org_id)
        _sql(
            "UPDATE meetings SET title = %(t)s, status = %(s)s, closed_at = %(c)s, "
            "created_by = %(h)s WHERE id = %(m)s",
            {
                "t": title,
                "s": "ended" if ended else "live",
                "c": datetime.now(UTC) if ended else None,
                "h": str(self.host.user_id),
                "m": str(self.meeting),
            },
        )
        _sql(
            "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
            "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
            {"m": str(self.meeting), "u": str(self.host.user_id), "o": str(self.host.org_id)},
        )
        for p in (self.raj, self.ana, self.sam):
            add_participant(self.meeting, self.host.org_id, p.user_id)
        _sql(
            "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room, status, started_at) "
            "VALUES (%(o)s, %(m)s, %(r)s, 'ended', %(t)s)",
            {
                "o": str(self.host.org_id),
                "m": str(self.meeting),
                "r": f"meeting-{self.meeting}",
                "t": T0,
            },
        )
        self.decision = self.add_decision("Raise the base price by 8%", owner=self.host.user_id)

    @property
    def org(self) -> uuid.UUID:
        return self.host.org_id

    def add_decision(self, content: str, *, owner: uuid.UUID | None = None) -> uuid.UUID:
        d = uuid.uuid4()
        _sql(
            "INSERT INTO decisions (id, org_id, meeting_id, content, confidence, status, "
            "confirmed_by, quote, quote_speaker_id, quote_start_ms, quote_end_ms) VALUES "
            "(%(d)s, %(o)s, %(m)s, %(c)s, 0.9, 'confirmed', %(owner)s, 'Let us do 8', %(owner)s, "
            "600000, 604000)",
            {
                "d": str(d),
                "o": str(self.org),
                "m": str(self.meeting),
                "c": content,
                "owner": str(owner) if owner else None,
            },
        )
        return d

    def payload(self, **over: Any) -> dict[str, Any]:
        body: dict[str, Any] = {
            "generation": "g1",
            "status": "ready",
            "prompt_versions": {
                "last_word": "last_word@v1",
                "consensus": "consensus_read@v1",
                "resume_point": "resume_point@v1",
            },
            "last_word": [
                {
                    "user_id": str(self.raj.user_id),
                    "kind": "cut_off",
                    "topic": "pricing",
                    "quote": "I just want to say the enterprise tier",
                    "at_ms": 601_000,
                    "end_ms": 603_000,
                    "decision_id": str(self.decision),
                    "signal_ids": [],
                },
                {
                    "user_id": str(self.ana.user_id),
                    "kind": "hand_not_called",
                    "topic": "the migration risk",
                    "quote": None,
                    "at_ms": 900_000,
                    "end_ms": 960_000,
                    "decision_id": None,
                    "signal_ids": [],
                },
            ],
            "consensus": [
                {
                    "decision_id": str(self.decision),
                    "strength": "weak",
                    "participants": 4,
                    "yes": 1,
                    "hedged": 1,
                    "silent": 1,
                    "objected": 0,
                    "talked_over": 1,
                    "ask": [
                        {"user_id": str(self.raj.user_id), "reason": "talked_over"},
                        {"user_id": str(self.ana.user_id), "reason": "hedged"},
                        {"user_id": str(self.sam.user_id), "reason": "silent"},
                        {"user_id": str(self.host.user_id), "reason": "silent"},
                    ],
                },
            ],
            "resume_point": {
                "reason": "cut_off",
                "topic": "the migration risk",
                "agenda_item_id": None,
                "positions": [
                    {
                        "user_id": str(self.ana.user_id),
                        "quote": "Move the billing tables first.",
                        "start_ms": 3_300_000,
                        "end_ms": 3_304_000,
                    },
                    {
                        "user_id": str(self.raj.user_id),
                        "quote": "The problem is the replication lag",
                        "start_ms": 3_306_000,
                        "end_ms": 3_309_000,
                    },
                ],
                "cut_off": {
                    "user_id": str(self.raj.user_id),
                    "quote": "The problem is the replication lag",
                    "start_ms": 3_306_000,
                    "end_ms": 3_309_000,
                },
            },
        }
        body.update(over)
        return body

    async def file(self, payload: dict[str, Any] | None = None, *, key: str | None = None) -> bool:
        value = result(
            self.host,
            self.meeting,
            1.0,
            user_id=NIL,
            agent="MeetingAgent",
            kind="room_signals",
            key=key or f"agentresult:{self.meeting}:room-signals:g1:{NIL}:MeetingAgent",
        )
        value["payload_json"] = json.dumps(payload or self.payload())
        return await apply(value)


def _notifications(org_id: uuid.UUID, prefix: str) -> list[dict[str, Any]]:
    rows = _sql(
        "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = 'notifications' "
        "ORDER BY created_at",
        {"o": str(org_id)},
    )
    out = []
    for r in rows:
        p = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        if str(p.get("source_event_id", "")).startswith(prefix):
            out.append(p)
    return out


# --- the analyzer's source --------------------------------------------------------------------


def test_source_files_the_room_signals_once_on_the_meeting_clock(
    client, transcript_table, monkeypatch  # noqa: F811 - the fixture
):
    # The process-wide client may be bound to another test's loop or URL: a fresh one per call.
    from redis import asyncio as aioredis

    from services.core_api import room_signals as rs

    monkeypatch.setattr(rs, "get_redis", lambda: aioredis.from_url(os.environ["REDIS_URL"]))
    room = Room(ended=False)
    _sql(
        "UPDATE users SET listen_for = ARRAY['pricing'] WHERE id = %(u)s",
        {"u": str(room.sam.user_id)},
    )
    _sql(
        "INSERT INTO transcript_chunks (org_id, meeting_id, speaker_id, start_ms, end_ms, text) "
        "VALUES (%(o)s, %(m)s, %(s)s, 1000, 4000, 'Pricing first.')",
        {"o": str(room.org), "m": str(room.meeting), "s": str(room.host.user_id)},
    )
    _sql(
        "INSERT INTO meeting_agenda_items (org_id, meeting_id, position, title, status, "
        "started_at) "
        "VALUES (%(o)s, %(m)s, 0, 'Pricing', 'in_progress', %(t)s)",
        {"o": str(room.org), "m": str(room.meeting), "t": T0 + timedelta(seconds=30)},
    )
    at = int((T0 + timedelta(minutes=5)).timestamp() * 1000)
    r = redis.Redis.from_url(os.environ["REDIS_URL"])
    key = f"nexus:{room.org}:room-signals:{room.meeting}"
    sid = uuid.uuid4()
    entries = [
        {
            "v": 1,
            "id": str(sid),
            "kind": "spoken_over",
            "user_id": str(room.raj.user_id),
            "at": at,
            "end": at + 1500,
            "detail": {"over_user_id": str(room.host.user_id), "spoke_ms": 1200},
        },
        {
            "v": 1,
            "id": str(uuid.uuid4()),
            "kind": "hand_not_called",
            "user_id": str(uuid.uuid4()),
            "at": at,
            "end": at + 9000,
            "detail": {},
        },  # not a participant: never filed
        {"v": 1, "id": "nope", "kind": "spoken_over"},  # malformed
        {
            "v": 1,
            "id": str(uuid.uuid4()),
            "kind": "shouted",
            "user_id": str(room.raj.user_id),
            "at": at,
            "end": at,
        },
    ]
    r.delete(key)
    r.rpush(key, *[json.dumps(e) for e in entries])
    url = f"/internal/meetings/{room.meeting}/room-signals-source"
    assert client.get(url, headers=_agent(room.org)).status_code == 409
    _sql(
        "UPDATE meetings SET status = 'ended', closed_at = %(c)s WHERE id = %(m)s",
        {"c": T0 + timedelta(minutes=50), "m": str(room.meeting)},
    )
    body = client.get(url, headers=_agent(room.org)).json()
    again = client.get(url, headers=_agent(room.org)).json()
    r.delete(key)
    assert (
        [s["id"] for s in body["room_signals"]]
        == [str(sid)]
        == [s["id"] for s in again["room_signals"]]
    )
    assert (
        body["room_signals"][0]["at_ms"] == 300_000 and body["room_signals"][0]["end_ms"] == 301_500
    )
    assert body["call_ms"] == 50 * 60 * 1000
    assert body["agenda"][0]["started_ms"] == 30_000 and body["agenda"][0]["ended_ms"] is None
    sam = next(p for p in body["participants"] if p["user_id"] == str(room.sam.user_id))
    assert sam["listen_for"] == ["pricing"] and sam["name"] == "Sam Lee"
    d = body["decisions"][0]
    assert d["owner_user_id"] == str(room.host.user_id) and d["quote_start_ms"] == 600_000
    assert body["turns"][0]["text"] == "Pricing first."
    other = seed_tenant()
    assert client.get(url, headers=_agent(other.org_id)).status_code == 404
    assert (
        client.get(
            url, headers={**_agent(room.org), "X-Mytheus-Service-Token": "x" * 40}
        ).status_code
        == 401
    )


# --- the consumer -----------------------------------------------------------------------------


async def test_the_result_files_nudges_reads_checks_and_the_resume_point(client):
    room = Room()
    outsider = seed_tenant()
    payload = room.payload()
    payload["last_word"].append(  # a second entry for Raj and one for a stranger: both skipped
        {**payload["last_word"][0], "kind": "spoken_over"}
    )
    payload["last_word"].append({**payload["last_word"][1], "user_id": str(outsider.user_id)})
    assert await room.file(payload) is True
    nudges = _sql(
        "SELECT user_id, kind, topic, decision_id, status FROM last_word_nudges "
        "WHERE meeting_id = %(m)s ORDER BY kind",
        {"m": str(room.meeting)},
    )
    assert [(str(n["user_id"]), n["kind"]) for n in nudges] == [
        (str(room.raj.user_id), "cut_off"),
        (str(room.ana.user_id), "hand_not_called"),
    ]
    assert str(nudges[0]["decision_id"]) == str(room.decision)
    read = _sql(
        "SELECT * FROM decision_agreement_reads WHERE decision_id = %(d)s",
        {"d": str(room.decision)},
    )[0]
    assert (read["strength"], read["participants"], read["yes_count"]) == ("weak", 4, 1)
    asked = {
        uuid.UUID(str(r["user_id"]))
        for r in _sql(
            "SELECT user_id FROM agreement_checks WHERE decision_id = %(d)s",
            {"d": str(room.decision)},
        )
    }
    assert asked == {room.raj.user_id, room.ana.user_id, room.sam.user_id}  # never the owner
    rp = _sql("SELECT * FROM resume_points WHERE meeting_id = %(m)s", {"m": str(room.meeting)})[0]
    assert str(rp["cut_off_user_id"]) == str(room.raj.user_id) and len(rp["positions"]) == 2
    lw = _notifications(room.org, "lastword:")
    assert len(lw) == 2 and {n["user_id"] for n in lw} == {
        str(room.raj.user_id),
        str(room.ana.user_id),
    }
    raj = next(n for n in lw if n["user_id"] == str(room.raj.user_id))
    assert raj["title"] == "You started to say something about pricing. Want to add it?"
    assert raj["link"] == f"/meetings/{room.meeting}#last-word" and raj["channel"] == "in_app"
    ag = _notifications(room.org, "agreement:")
    assert len(ag) == 3 and all("anonymous" in n["body"] for n in ag)
    # redelivery (the same key) and a replay with a new key both write nothing new
    assert await room.file(payload) is False
    assert await room.file(payload, key=f"replay-{uuid.uuid4()}") is False
    assert len(_notifications(room.org, "lastword:")) == 2


async def test_a_partial_result_with_garbage_is_filed_carefully(client):
    room = Room()
    bad = room.payload(
        status="weird",
        last_word=[
            {"user_id": str(room.raj.user_id), "kind": "yelled", "topic": "x"},
            {"user_id": "nope", "kind": "cut_off", "topic": "x"},
            {
                "user_id": str(room.sam.user_id),
                "kind": "topic_silent",
                "topic": "Q1 launch",
                "decision_id": str(uuid.uuid4()),
            },
        ],
        consensus=[
            {
                "decision_id": str(room.decision),
                "strength": "weak",
                "participants": 1,
                "yes": 3,
                "hedged": 0,
                "silent": 0,
                "objected": 0,
                "talked_over": 0,
                "ask": [],
            }
        ],
        resume_point={
            "reason": "agenda_unfinished",
            "topic": "Migration plan",
            "agenda_item_id": str(uuid.uuid4()),
            "positions": [],
            "cut_off": None,
        },
    )
    assert await room.file(bad) is True
    assert (
        _sql(
            "SELECT status FROM room_signal_runs WHERE meeting_id = %(m)s", {"m": str(room.meeting)}
        )[0]["status"]
        == "partial"
    )
    n = _sql(
        "SELECT kind, decision_id FROM last_word_nudges WHERE meeting_id = %(m)s",
        {"m": str(room.meeting)},
    )
    assert n == [{"kind": "topic_silent", "decision_id": None}]
    assert not _sql(
        "SELECT 1 FROM decision_agreement_reads WHERE decision_id = %(d)s",
        {"d": str(room.decision)},
    )
    rp = _sql(
        "SELECT agenda_item_id, cut_off_user_id FROM resume_points WHERE meeting_id = %(m)s",
        {"m": str(room.meeting)},
    )[0]
    assert rp == {"agenda_item_id": None, "cut_off_user_id": None}
    sam_nudge = _notifications(room.org, "lastword:")[0]
    assert sam_nudge["title"] == "Q1 launch came up and you listen for it. Want to add your view?"


async def test_another_tenants_meeting_is_poison(client):
    room = Room()
    other = seed_tenant()
    value = result(other, room.meeting, 1.0, user_id=NIL, agent="MeetingAgent", kind="room_signals")
    value["payload_json"] = json.dumps(room.payload())
    with pytest.raises(PoisonMessage):
        await apply(value)


# --- the last word and late contributions -----------------------------------------------------


async def test_after_signals_and_the_last_word_are_private(client):
    room = Room()
    sign_in(room.raj)
    before = client.get(f"/api/v1/meetings/{room.meeting}/after-signals").json()
    assert before["analyzed"] is False and before["my_nudge"] is None
    await room.file()
    body = client.get(f"/api/v1/meetings/{room.meeting}/after-signals").json()
    assert body["analyzed"] is True
    assert (
        body["my_nudge"]["prompt"] == "You started to say something about pricing. Want to add it?"
    )
    assert body["my_nudge"]["decision_content"] == "Raise the base price by 8%"
    dec = body["decisions"][0]
    assert dec["agreement"] == "weak" and dec["in_window"] is True and dec["you_own"] is False
    assert (
        body["resume_point"]["headline"] == "Raj was halfway through explaining the migration risk"
    )
    mine = client.get("/api/v1/me/last-word").json()["items"]
    assert [n["meeting_title"] for n in mine] == ["Pricing review"]
    sign_in(room.ana)
    assert (
        client.get(f"/api/v1/meetings/{room.meeting}/after-signals").json()["my_nudge"]["kind"]
        == "hand_not_called"
    )
    # someone else's nudge is a 404, never theirs to close
    assert client.post(f"/api/v1/last-word/{mine[0]['id']}/dismiss").status_code == 404
    sign_in(room.raj)
    done = client.post(f"/api/v1/last-word/{mine[0]['id']}/dismiss").json()
    assert done["status"] == "dismissed"
    assert client.get("/api/v1/me/last-word").json()["items"] == []
    assert len(client.get("/api/v1/me/last-word?status=all").json()["items"]) == 1
    stranger = seed_tenant()
    sign_in(stranger)
    assert client.get(f"/api/v1/meetings/{room.meeting}/after-signals").status_code == 404
    outsider = add_user(room.host)
    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{room.meeting}/after-signals").status_code == 403


async def test_a_late_point_written_and_recorded(client):
    room = Room()
    await room.file()
    sign_in(room.raj)
    nudge = client.get("/api/v1/me/last-word").json()["items"][0]
    url = f"/api/v1/meetings/{room.meeting}/late-contributions"
    assert client.post(url, json={}).status_code == 422
    res = client.post(
        url, json={"body": "  The enterprise tier needs its own price.  ", "nudge_id": nudge["id"]}
    )
    assert res.status_code == 201, res.text
    c = res.json()
    assert c["body"] == "The enterprise tier needs its own price." and c["mine"] is True
    assert (
        c["decision_id"] == str(room.decision)
        and c["at_ms"] == 601_000
        and c["author_name"] == "Raj Iyer"
    )
    assert (
        client.get("/api/v1/me/last-word?status=all").json()["items"][0]["status"] == "contributed"
    )
    host_note = _notifications(room.org, "latecontribution:")
    assert len(host_note) == 1 and host_note[0]["user_id"] == str(room.host.user_id)
    assert host_note[0]["title"] == "Raj Iyer added a late point to Pricing review"
    audio = bytes(range(256)) * 4
    rec = client.post(
        url,
        json={
            "audio": {
                "data_base64": base64.b64encode(audio).decode(),
                "mime": "audio/webm",
                "duration_ms": 4200,
            }
        },
    ).json()
    assert rec["has_audio"] is True and rec["body"] is None and rec["audio_duration_ms"] == 4200
    sign_in(room.sam)
    got = client.get(f"/api/v1/late-contributions/{rec['id']}/audio")
    assert (
        got.status_code == 200
        and got.content == audio
        and got.headers["content-type"] == "audio/webm"
    )
    part = client.get(
        f"/api/v1/late-contributions/{rec['id']}/audio", headers={"Range": "bytes=10-19"}
    )
    assert part.status_code == 206 and part.content == audio[10:20]
    assert part.headers["content-range"] == f"bytes 10-19/{len(audio)}"
    assert (
        client.get(
            f"/api/v1/late-contributions/{rec['id']}/audio", headers={"Range": "bytes=99999-"}
        ).status_code
        == 416
    )
    listed = client.get(url).json()["items"]
    assert [x["mine"] for x in listed] == [False, False]
    assert client.delete(f"/api/v1/late-contributions/{c['id']}").status_code == 403
    after = client.get(f"/api/v1/meetings/{room.meeting}/after-signals").json()
    assert after["decisions"][0]["late_contribution_count"] == 1
    sign_in(room.raj)
    assert client.delete(f"/api/v1/late-contributions/{c['id']}").status_code == 204
    assert client.get("/api/v1/me/last-word").json()["items"][0]["status"] == "open"
    assert len(client.get(url).json()["items"]) == 1
    # minutes and the recap carry the written points
    client.post(url, json={"body": "Second thoughts on tiers."})
    view = client.get(f"/api/v1/meetings/{room.meeting}/minutes").json()
    assert [x["body"] for x in view["late_contributions"]] == [None, "Second thoughts on tiers."]
    big = base64.b64encode(b"x" * 2_700_000).decode()
    too_big = client.post(
        url, json={"audio": {"data_base64": big, "mime": "audio/ogg", "duration_ms": 1000}}
    )
    assert too_big.status_code in (413, 422)


async def test_the_late_window(client):
    live = Room(ended=False)
    sign_in(live.raj)
    res = client.post(f"/api/v1/meetings/{live.meeting}/late-contributions", json={"body": "x"})
    assert res.status_code == 409 and res.json()["reason"] == "not_ended"
    old = Room()
    _sql(
        "UPDATE meetings SET closed_at = now() - interval '8 days' WHERE id = %(m)s",
        {"m": str(old.meeting)},
    )
    sign_in(old.raj)
    res = client.post(f"/api/v1/meetings/{old.meeting}/late-contributions", json={"body": "x"})
    assert res.status_code == 409 and res.json()["reason"] == "late_window_closed"
    other_room = Room()
    await other_room.file()
    sign_in(other_room.raj)
    foreign = client.get("/api/v1/me/last-word").json()["items"][0]["id"]
    room = Room()
    sign_in(room.raj)
    res = client.post(
        f"/api/v1/meetings/{room.meeting}/late-contributions",
        json={"body": "x", "nudge_id": foreign},
    )
    assert res.status_code == 422


async def test_the_recap_shows_written_late_points(client):
    room = Room()
    sign_in(room.raj)
    client.post(
        f"/api/v1/meetings/{room.meeting}/late-contributions", json={"body": "Tiers need a floor."}
    )
    sign_in(room.host)
    link = client.post(f"/api/v1/meetings/{room.meeting}/share-links", json={})
    assert link.status_code == 201, link.text
    token = link.json()["token"]
    public = client.get(f"/api/v1/public/shares/{token}")
    assert public.status_code == 200, public.text
    late = public.json()["recap"]["late_contributions"]
    assert late == [
        {
            "author_name": "Raj Iyer",
            "body": "Tiers need a floor.",
            "created_at": late[0]["created_at"],
        }
    ]


# --- agreement --------------------------------------------------------------------------------


async def test_agreement_aggregates_for_the_owner_only(client):
    room = Room()
    await room.file()
    sign_in(room.raj)
    view = client.get(f"/api/v1/decisions/{room.decision}/agreement").json()
    assert view["strength"] == "weak" and view["summary"] is None and view["notes"] == []
    check = view["my_check"]
    assert check["status"] == "asked" and check["share_name"] is False
    assert [c["id"] for c in client.get("/api/v1/me/agreement-checks").json()["items"]] == [
        check["id"]
    ]
    ans = client.post(
        f"/api/v1/agreement-checks/{check['id']}/answer",
        json={"answer": "concerns", "note": "  Enterprise needs its own tier.  "},
    ).json()
    assert ans["status"] == "answered" and ans["note"] == "Enterprise needs its own tier."
    assert client.get("/api/v1/me/agreement-checks").json()["items"] == []
    sign_in(room.ana)
    assert (
        client.post(
            f"/api/v1/agreement-checks/{check['id']}/answer", json={"answer": "object"}
        ).status_code
        == 404
    )
    ana = client.get(f"/api/v1/decisions/{room.decision}/agreement").json()["my_check"]
    client.post(
        f"/api/v1/agreement-checks/{ana['id']}/answer",
        json={"answer": "support", "note": "Fine by me.", "share_name": True},
    )
    sign_in(room.host)
    owner = client.get(f"/api/v1/decisions/{room.decision}/agreement").json()
    assert owner["you_own"] is True and owner["my_check"] is None
    s = owner["summary"]
    assert (s["participants"], s["support"], s["concerns"], s["object"], s["pending"]) == (
        4,
        2,
        1,
        0,
        1,
    )
    # two answers: only the signed note is shown, with its name
    assert owner["notes"] == [
        {
            "text": "Fine by me.",
            "author_name": "Ana Silva",
            "answer": "support",
            "at": owner["notes"][0]["at"],
        }
    ]
    sign_in(room.sam)
    sam = client.get(f"/api/v1/decisions/{room.decision}/agreement").json()["my_check"]
    client.post(f"/api/v1/agreement-checks/{sam['id']}/answer", json={"answer": "support"})
    sign_in(room.host)
    owner = client.get(f"/api/v1/decisions/{room.decision}/agreement").json()
    assert {(n["author_name"], n["text"]) for n in owner["notes"]} == {
        ("Ana Silva", "Fine by me."),
        (None, "Enterprise needs its own tier."),
    }
    assert owner["summary"]["support"] == 3 and owner["summary"]["pending"] == 0
    # nothing per person anywhere in the owner's view
    assert "raj" not in json.dumps(owner).lower()
    # answering again replaces; the audit row never carries the answer
    sign_in(room.raj)
    again = client.post(
        f"/api/v1/agreement-checks/{check['id']}/answer", json={"answer": "support"}
    ).json()
    assert again["answer"] == "support" and again["note"] is None
    audit_rows = _sql(
        "SELECT metadata FROM audit_logs WHERE action = 'agreement_check.answered' "
        "AND target_id = %(k)s",
        {"k": check["id"]},
    )
    assert audit_rows and all("answer" not in json.dumps(r["metadata"]) for r in audit_rows)


# --- resume points ----------------------------------------------------------------------------


def _next_meeting(room: Room, *, follows: bool, people: list[Tenant]) -> uuid.UUID:
    m = seed_meeting(room.org)
    _sql(
        "UPDATE meetings SET title = 'Pricing review 2', status = 'live', created_by = %(h)s, "
        "previous_meeting_id = %(p)s WHERE id = %(m)s",
        {"h": str(room.host.user_id), "p": str(room.meeting) if follows else None, "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(room.host.user_id), "o": str(room.org)},
    )
    for p in people:
        add_participant(m, room.org, p.user_id)
    return m


async def test_the_next_meeting_picks_up_the_thread(client):
    room = Room()
    await room.file()
    nxt = _next_meeting(room, follows=True, people=[room.raj])
    sign_in(room.raj)
    read = client.get(f"/api/v1/meetings/{nxt}/resume-point").json()
    point = read["incoming"]
    assert read["saved"] is None and point["meeting_id"] == str(room.meeting)
    assert point["headline"] == "Raj was halfway through explaining the migration risk"
    assert [p["name"] for p in point["positions"]] == ["Ana Silva", "Raj Iyer"]
    assert point["cut_off"]["quote"] == "The problem is the replication lag"
    assert (
        client.post(
            f"/api/v1/resume-points/{point['id']}/dismiss", json={"meeting_id": str(nxt)}
        ).status_code
        == 403
    )
    done = client.post(
        f"/api/v1/resume-points/{point['id']}/continue", json={"meeting_id": str(nxt)}
    ).json()
    assert done["status"] == "continued" and done["continued_in_meeting_id"] == str(nxt)
    again = client.post(
        f"/api/v1/resume-points/{point['id']}/continue", json={"meeting_id": str(nxt)}
    )
    assert again.status_code == 200
    assert (
        client.get(f"/api/v1/meetings/{nxt}/resume-point").json()["incoming"]["status"]
        == "continued"
    )
    sign_in(room.host)
    assert (
        client.post(
            f"/api/v1/resume-points/{point['id']}/dismiss", json={"meeting_id": str(nxt)}
        ).status_code
        == 409
    )
    # another meeting cannot resolve a thread it does not pick up
    third = _next_meeting(room, follows=False, people=[])
    assert (
        client.post(
            f"/api/v1/resume-points/{point['id']}/continue", json={"meeting_id": str(third)}
        ).status_code
        == 404
    )


async def test_same_people_and_confidential_sources(client):
    room = Room()
    await room.file()
    same = _next_meeting(room, follows=False, people=[room.raj, room.ana, room.sam])
    sign_in(room.host)
    point = client.get(f"/api/v1/meetings/{same}/resume-point").json()["incoming"]
    assert point is not None and point["meeting_id"] == str(room.meeting)
    assert (
        client.post(
            f"/api/v1/resume-points/{point['id']}/dismiss", json={"meeting_id": str(same)}
        ).json()["status"]
        == "dismissed"
    )
    few = Room()
    await few.file()
    other = _next_meeting(few, follows=False, people=[])  # host alone: not the same people
    sign_in(few.host)
    assert client.get(f"/api/v1/meetings/{other}/resume-point").json()["incoming"] is None
    secret = Room()
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(secret.meeting)})
    await secret.file()
    stranger = add_user(secret.host)
    nxt = _next_meeting(secret, follows=True, people=[secret.raj, stranger])
    sign_in(secret.host)
    assert client.get(f"/api/v1/meetings/{nxt}/resume-point").json()["incoming"] is None


# --- isolation --------------------------------------------------------------------------------


async def test_every_new_table_is_tenant_isolated(client):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    room = Room()
    await room.file()
    sign_in(room.raj)
    client.post(f"/api/v1/meetings/{room.meeting}/late-contributions", json={"body": "x"})
    other = seed_tenant()
    engine = create_async_engine(APP_URL)
    tables = (
        "room_signal_runs",
        "last_word_nudges",
        "late_contributions",
        "decision_agreement_reads",
        "agreement_checks",
        "resume_points",
        "room_signals",
    )
    try:
        async with engine.connect() as conn:
            for t in tables:
                async with conn.begin():
                    none = (await conn.execute(text(f"SELECT count(*) FROM {t}"))).scalar()
                    await conn.execute(
                        text("SELECT set_config('app.current_org', :o, true)"),
                        {"o": str(other.org_id)},
                    )
                    theirs = (await conn.execute(text(f"SELECT count(*) FROM {t}"))).scalar()
                assert none == 0 and theirs == 0, t
            async with conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(room.org)}
                )
                assert (
                    await conn.execute(text("SELECT count(*) FROM last_word_nudges"))
                ).scalar() == 2
    finally:
        await engine.dispose()


def test_prompt_copy():
    from services.core_api import room_signals as rs

    assert (
        rs.nudge_prompt("spoken_over", "pricing")
        == "You started to say something about pricing. Want to add it?"
    )
    assert rs.nudge_prompt("hand_not_called", "pricing").startswith(
        "Your hand was up while the room discussed pricing"
    )
    assert (
        rs.resume_headline("time_ran_out", "the migration risk", None)
        == "Time ran out on the migration risk"
    )
    assert (
        rs.resume_headline("agenda_unfinished", "migration plan", None)
        == "Migration plan was left unfinished"
    )
    assert time.time() > 0
