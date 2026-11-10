"""Minutes for everyone, against a real Postgres as `mytheus_app`.

- consumer: MeetingAgent `minutes` records become one `meeting_minutes` row per participant and
  one shared row (nil user); texts are clipped and ids checked; a non-participant's minutes are
  dropped; the shared minutes never keep promises; a later generation replaces, an
  `unavailable` set never replaces real minutes; redelivery is a no-op;
- `GET /api/v1/meetings/{id}/minutes`: participants only (403 / 404), mine is mine only, the
  state machine (not_ended, pending, ready, unavailable);
- the minutes feed the next meeting: carry-forward, the internal agenda route, the brief and
  the drafted agenda;
- the public recap carries the shared minutes, never promises; a confidential recap needs the
  host's explicit confirmation;
- the follow-up: a draft from the minutes, the composer's subject and body, the automatic
  send deferred until the minutes land and then sent by the consumer after commit;
- the internal minutes source for the agent runtime; RLS isolation of `meeting_minutes`.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import meeting_screens
from services.core_api import consumer, notification_client
from services.core_api.routes import (
    followups,
    internal_agents,
    internal_meetings,
    meetings,
    minutes,
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
from tests.test_followups import FakeNotifications
from tests.test_my_tasks import add_user
from tests.test_worklogs import apply, result

AGENT_SECRET = "a" * 40
WORKFLOW_SECRET = "w" * 40
NIL = uuid.UUID(int=0)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ["SHARE_LINK_SIGNING_KEY"] = "k" * 40
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},workflow-service={WORKFLOW_SECRET}"
    )
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-minutes-test")
    for router in (
        minutes.router,
        meetings.router,
        internal_agents.router,
        share_links.router,
        share_links.public_router,
        followups.router,
        meeting_screens.router,
    ):
        app.include_router(router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    notification_client.set_notifications(None)
    for key in ("SHARE_LINK_SIGNING_KEY", internal_meetings.TOKENS_ENV):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def fake() -> FakeNotifications:
    f = FakeNotifications()
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    return f


def meeting(
    *, ended: bool = True, confidential: bool = False, title: str = "Pricing review"
) -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    member = add_user(host)
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = %(t)s, status = %(s)s, confidential = %(c)s WHERE id = %(m)s",
        {"t": title, "s": "ended" if ended else "live", "c": confidential, "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    return host, member, m


def payload(scope: str = "shared", **over: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "scope": scope,
        "prompt_version": "minutes@v1",
        "generation": "g1",
        "status": "ready",
        "reason": None,
        "summary": "Agreed the price rise; the vendor question stays open.",
        "decisions": [{"text": "Raise the base price by 8%", "status": "agreed"}],
        "open_items": [{"text": "Confirm the vendor split", "owner_name": "Sam", "due": "Friday"}],
        "unanswered_questions": [{"text": "Does legal need to review?", "asked_by_name": "Ana"}],
        "promises": [{"text": "Send the comparison", "to_name": "Ana", "due": None}],
    }
    body.update(over)
    return body


def minutes_record(
    t: Tenant, m: uuid.UUID, user_id: uuid.UUID, body: dict[str, Any], generation: str = "g1"
) -> dict[str, Any]:
    window = f"minutes:{generation}"
    value = result(
        t,
        m,
        1.0,
        user_id=user_id,
        window=window,
        agent="MeetingAgent",
        kind="minutes",
        key=f"agentresult:{m}:{window}:{user_id}:MeetingAgent",
    )
    value["window_id"] = window
    value["content"] = body.get("summary", "")
    value["payload_json"] = json.dumps({**body, "generation": generation})
    return value


def rows(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT user_id, scope, status, reason, content, generation FROM meeting_minutes "
        "WHERE meeting_id = %(m)s ORDER BY user_id NULLS FIRST",
        {"m": str(m)},
    )


async def write_minutes(host: Tenant, member: Tenant, m: uuid.UUID, generation: str = "g1") -> None:
    await apply(minutes_record(host, m, NIL, payload("shared"), generation))
    owned = payload(
        "personal",
        open_items=[
            {"text": "Confirm the vendor split", "owner_user_id": str(member.user_id)},
        ],
    )
    await apply(minutes_record(host, m, member.user_id, owned, generation))


# --- consumer ---------------------------------------------------------------------------------


async def test_consumer_files_personal_and_shared_minutes(client):
    host, member, m = meeting()
    stranger = add_user(host)
    shared = payload("shared", summary="x" * 900)
    assert await apply(minutes_record(host, m, NIL, shared)) is True
    personal = payload(
        "personal",
        open_items=[
            {"text": "Mine", "owner_user_id": str(member.user_id)},
            {"text": "Foreign", "owner_user_id": str(stranger.user_id)},
            {"text": "  "},
        ],
        decisions=[{"text": "Linked?", "status": "maybe", "decision_id": str(uuid.uuid4())}],
    )
    assert await apply(minutes_record(host, m, member.user_id, personal)) is True
    # Minutes for someone who was not in the meeting are dropped.
    assert await apply(minutes_record(host, m, stranger.user_id, payload("personal"))) is False

    found = rows(m)
    assert [(r["user_id"], r["scope"], r["status"]) for r in found] == [
        (None, "shared", "ready"),
        (str(member.user_id), "personal", "ready"),
    ]
    shared_row, mine = found[0]["content"], found[1]["content"]
    assert shared_row["promises"] == []  # never on the shared minutes
    assert len(shared_row["summary"]) == 600
    assert mine["promises"][0]["text"] == "Send the comparison"
    assert [o["owner_user_id"] for o in mine["open_items"]] == [str(member.user_id), None]
    assert mine["decisions"] == [{"text": "Linked?", "status": "agreed", "decision_id": None}]

    # Redelivery is a no-op.
    assert await apply(minutes_record(host, m, NIL, shared)) is False


async def test_later_generation_replaces_and_unavailable_never_replaces_real_minutes(client):
    host, member, m = meeting()
    await apply(minutes_record(host, m, NIL, payload("shared")))
    gone = payload(
        "shared",
        status="unavailable",
        reason="budget",
        summary="",
        decisions=[],
        open_items=[],
        unanswered_questions=[],
        promises=[],
    )
    assert await apply(minutes_record(host, m, NIL, gone, "g2")) is False
    assert rows(m)[0]["status"] == "ready" and rows(m)[0]["generation"] == "g1"
    newer = payload("shared", summary="Second time round.")
    assert await apply(minutes_record(host, m, NIL, newer, "g3")) is True
    assert rows(m)[0]["content"]["summary"] == "Second time round."

    host2, _, m2 = meeting()
    assert await apply(minutes_record(host2, m2, NIL, gone, "g1")) is True
    assert (rows(m2)[0]["status"], rows(m2)[0]["reason"]) == ("unavailable", "budget")
    empty = payload("shared", summary="", decisions=[], open_items=[], unanswered_questions=[])
    assert await apply(minutes_record(host2, m2, NIL, empty, "g2")) is True
    assert rows(m2)[0]["status"] == "empty"


# --- the route --------------------------------------------------------------------------------


async def test_get_minutes_is_participant_scoped(client):
    host, member, m = meeting(ended=False)
    sign_in(member)
    assert client.get(f"/api/v1/meetings/{m}/minutes").json()["state"] == "not_ended"
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    assert client.get(f"/api/v1/meetings/{m}/minutes").json()["state"] == "pending"
    await write_minutes(host, member, m)

    mine = client.get(f"/api/v1/meetings/{m}/minutes").json()
    assert mine["state"] == "ready"
    assert mine["mine"]["scope"] == "personal" and mine["mine"]["promises"]
    assert mine["shared"]["scope"] == "shared" and mine["shared"]["promises"] == []
    assert mine["shared"]["decisions"][0]["status"] == "agreed"

    sign_in(host)  # the host has no personal minutes, and never sees the member's
    theirs = client.get(f"/api/v1/meetings/{m}/minutes").json()
    assert theirs["mine"] is None and theirs["shared"] is not None

    outsider = add_user(host)
    sign_in(outsider)
    res = client.get(f"/api/v1/meetings/{m}/minutes")
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/minutes").status_code == 404


# --- the next meeting -------------------------------------------------------------------------


def _agent_headers(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": AGENT_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


async def test_minutes_feed_carry_forward_agenda_brief_and_draft(client):
    host, member, earlier = meeting()
    await write_minutes(host, member, earlier)
    sign_in(host)
    nxt = client.post(
        "/api/v1/meetings",
        json={
            "title": "Pricing follow-up",
            "participant_ids": [str(member.user_id)],
            "previous_meeting_id": str(earlier),
        },
    )
    assert nxt.status_code == 201, nxt.text
    n = nxt.json()["id"]

    carry = client.get(f"/api/v1/meetings/{n}/carry-forward").json()
    assert carry["open_items"] == [
        {
            "text": "Confirm the vendor split",
            "owner_user_id": None,
            "owner_name": "Sam",
            "due": "Friday",
        }
    ]
    assert carry["unanswered_questions"] == [
        {"text": "Does legal need to review?", "asked_by_name": "Ana"}
    ]

    internal = client.get(
        f"/internal/meetings/{n}/agenda", headers=_agent_headers(host.org_id)
    ).json()
    assert internal["carry_forward"]["open_items"] == ["Confirm the vendor split (owner: Sam)"]
    assert internal["carry_forward"]["questions"] == ["Does legal need to review?"]

    sign_in(member)
    brief = client.get(f"/bff/meetings/{n}/brief").json()["previous_minutes"]
    assert brief["unanswered_questions"] == ["Does legal need to review?"]
    assert brief["my_promises"] == [{"text": "Send the comparison", "to_name": "Ana", "due": None}]
    sign_in(host)
    assert client.get(f"/bff/meetings/{n}/brief").json()["previous_minutes"]["my_promises"] == []

    draft = client.post(
        "/api/v1/meetings/agenda-draft", json={"previous_meeting_id": str(earlier)}
    ).json()
    assert draft["open_questions"] == ["Does legal need to review?"]


# --- recap, share and follow-up ---------------------------------------------------------------


async def test_public_recap_carries_shared_minutes_never_promises(client):
    host, member, m = meeting()
    await write_minutes(host, member, m)
    sign_in(member)
    link = client.post(f"/api/v1/meetings/{m}/share-links").json()
    page = client.get(f"/api/v1/public/shares/{link['token']}").json()
    recap = page["recap"]["minutes"]
    assert recap["summary"].startswith("Agreed the price rise")
    assert recap["open_items"] == [
        {"text": "Confirm the vendor split", "owner_name": "Sam", "due": "Friday"}
    ]
    raw = json.dumps(page)
    assert "promises" not in raw and "Send the comparison" not in raw
    assert str(member.user_id) not in raw


async def test_confidential_recap_needs_the_hosts_confirmation(client):
    host, member, m = meeting(confidential=True)
    sign_in(host)
    res = client.post(f"/api/v1/meetings/{m}/share-links", json={})
    assert res.status_code == 409 and res.json()["reason"] == "confidential_confirmation_required"
    ok = client.post(f"/api/v1/meetings/{m}/share-links", json={"confirm_confidential": True})
    assert ok.status_code == 201


async def test_follow_up_draft_and_composer(client, fake):
    host, member, m = meeting()
    await write_minutes(host, member, m)
    sign_in(member)
    assert client.get(f"/api/v1/meetings/{m}/follow-up").json()["draft"] is None
    sign_in(host)
    view = client.get(f"/api/v1/meetings/{m}/follow-up").json()
    assert view["email_configured"] is True and view["minutes_ready"] is True
    assert view["draft"]["subject"] == "Recap: Pricing review"
    body = view["draft"]["body"]
    assert "Agreed the price rise" in body and "Still open" in body
    assert "Does legal need to review? (asked by Ana)" in body
    assert "Send the comparison" not in body  # a promise is never in a shared email

    fake.email = False
    assert client.get(f"/api/v1/meetings/{m}/follow-up").json()["email_configured"] is False

    sent = client.post(
        f"/api/v1/meetings/{m}/follow-up/send",
        json={
            "subject": "Notes from today",
            "body": "Thanks all.\n\nNext steps\n- Confirm the vendor split",
            "guest_emails": ["Ana@Client.com"],
        },
    )
    assert sent.status_code == 200, sent.text
    [call] = fake.calls
    assert call["subject"] == "Notes from today" and call["recipients"] == ["ana@client.com"]
    assert call["text"].startswith("Thanks all.") and sent.json()["share_url"] in call["text"]
    assert "<li" in call["html"] and "Confirm the vendor split" in call["html"]
    assert client.get(f"/api/v1/meetings/{m}/follow-up").json()["guest_emails"] == [
        "ana@client.com"
    ]
    notes = _sql(
        "SELECT payload FROM outbox WHERE idempotency_key LIKE %(k)s",
        {"k": f"notif:followup:{m}:%"},
    )
    assert {r["payload"]["title"] for r in notes} == {"Notes from today"}
    bad = client.post(f"/api/v1/meetings/{m}/follow-up/send", json={"guest_emails": ["nope"]})
    assert bad.status_code == 422


def _transcript(org_id: uuid.UUID, m: uuid.UUID, speaker: uuid.UUID) -> None:
    _sql(
        "INSERT INTO transcript_chunks (org_id, meeting_id, speaker_id, start_ms, end_ms, text) "
        "VALUES (%(o)s, %(m)s, %(s)s, 0, 1000, 'We agreed the price.'), "
        "(%(o)s, %(m)s, %(s)s, 2000, 3000, 'Who confirms the vendor split?')",
        {"o": str(org_id), "m": str(m), "s": str(speaker)},
    )


@pytest.mark.usefixtures("transcript_table")
async def test_automatic_follow_up_waits_for_the_minutes(client, fake):
    host, member, m = meeting()
    _transcript(host.org_id, m, host.user_id)
    url = f"/internal/meetings/{m}/follow-up/dispatch"
    headers = {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": WORKFLOW_SECRET,
        "X-Mytheus-Org": str(host.org_id),
    }
    res = client.post(url, headers=headers)
    assert res.status_code == 200 and res.json()["waiting_for_minutes"] is True
    assert res.json()["status"] == "unavailable"
    sign_in(host)
    assert client.get(f"/api/v1/meetings/{m}/follow-up").json()["awaiting_minutes"] is True

    consumer.AFTER_COMMIT.clear()
    await apply(minutes_record(host, m, NIL, payload("shared")))
    assert len(consumer.AFTER_COMMIT) == 1
    # The queued send runs on this test's event loop: give it its own engine.
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    try:
        await consumer.drain_after_commit()
    finally:
        await db_mod.get_engine().dispose()
        db_mod.get_engine.cache_clear()
        db_mod.get_sessionmaker.cache_clear()
    view = client.get(f"/api/v1/meetings/{m}/follow-up").json()
    assert view["awaiting_minutes"] is False and view["last_result"]["status"] == "sent"
    notes = _sql(
        "SELECT payload FROM outbox WHERE idempotency_key LIKE %(k)s",
        {"k": f"notif:followup:{m}:%"},
    )
    assert len(notes) == 2 and "Agreed the price rise" in notes[0]["payload"]["body"]
    # The personal minutes landing later queue nothing.
    await apply(minutes_record(host, m, member.user_id, payload("personal")))
    assert consumer.AFTER_COMMIT == []

    # Without a transcript there is nothing to wait for: it is sent at once.
    host2, _, m2 = meeting()
    headers["X-Mytheus-Org"] = str(host2.org_id)
    out = client.post(f"/internal/meetings/{m2}/follow-up/dispatch", headers=headers).json()
    assert out["status"] == "sent"


# --- the minutes source -----------------------------------------------------------------------


@pytest.mark.usefixtures("transcript_table")
async def test_internal_minutes_source(client):
    host, member, m = meeting(ended=False)
    _transcript(host.org_id, m, host.user_id)
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) VALUES "
        "(%(o)s, %(m)s, 'Kept', 0.9, 'confirmed'), (%(o)s, %(m)s, 'Gone', 0.9, 'dismissed')",
        {"o": str(host.org_id), "m": str(m)},
    )
    url = f"/internal/meetings/{m}/minutes-source"
    assert client.get(url, headers=_agent_headers(host.org_id)).status_code == 409
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    res = client.get(url, headers=_agent_headers(host.org_id))
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["title"] == "Pricing review" and body["host_user_id"] == str(host.user_id)
    assert [p["user_id"] for p in body["participants"]][0] == str(host.user_id)
    assert all("@" not in (p["name"] or "") for p in body["participants"])
    assert [t["text"] for t in body["turns"]] == [
        "We agreed the price.",
        "Who confirms the vendor split?",
    ]
    assert [d["content"] for d in body["decisions"]] == ["Kept"]
    assert client.get(url).status_code == 401
    assert client.get(url, headers=_agent_headers(seed_tenant().org_id)).status_code == 404


def test_meeting_minutes_rows_are_isolated_by_rls(client):
    import psycopg2

    host, _, m = meeting()
    _sql(
        "INSERT INTO meeting_minutes (org_id, meeting_id, scope, status, content, "
        "prompt_version, generation) VALUES (%(o)s, %(m)s, 'shared', 'ready', '{}', "
        "'minutes@v1', 'g')",
        {"o": str(host.org_id), "m": str(m)},
    )
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM meeting_minutes")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT set_config('app.current_org', %s, false)", (str(seed_tenant().org_id),)
            )
            cur.execute("SELECT count(*) FROM meeting_minutes WHERE meeting_id = %s", (str(m),))
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()


# --- notification-service: the email status ---------------------------------------------------


def test_notification_service_reports_email_status(monkeypatch):
    from services.notification_service import internal
    from services.notification_service.internal_auth import core_caller

    app = _create_test_app("notification-email-status-test")
    app.include_router(internal.router)
    app.dependency_overrides[core_caller] = lambda: uuid.uuid4()
    with TestClient(app) as c:
        internal.set_sender(None)
        assert c.get("/internal/email/status").json() == {"configured": False, "provider": None}
        internal.set_sender(type("S", (), {"provider": "resend"})())  # type: ignore[arg-type]
        assert c.get("/internal/email/status").json() == {"configured": True, "provider": "resend"}
    internal.set_sender(None, active=False)
