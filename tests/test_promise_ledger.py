"""One promise ledger (Beyond the meeting, Layer 2): promises from every channel, their sources,
the capacity check at the moment of commitment, date proposals and hand-offs.

- Pure: the due-hint parser, the day model and the agent's sentence ("That's your seventh
  promise due Friday. Offer Monday instead?").
- DB (`migrated_db`): RLS on the new tables; the ledger across channels while
  `/api/v1/commitments` keeps its published shape; manual add with the capacity answer; kept /
  drop / undo; confirm of an extracted promise; hand-off and its undo; renegotiation applied at
  once or proposed to the requester who accepts or declines; the internal capture door
  integration-service uses (author check, preferences, dedupe, recipients); the consumer's
  in-person channel, parsed due date and landing warning; the action landing warning; the
  capacity read with meetings and focus time.
- Slack: the message pre-gate and the capture flow with fakes (no DB, no network).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import capacity as cap
from services.core_api.promise_dates import day_label, ordinal, parse_due_hint
from services.core_api.routes import (
    action_create,
    candidates,
    commitments,
    internal_meetings,
    internal_promises,
    promises,
)
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
from tests.test_worklogs import apply, result

INTEGRATION_SECRET = "i" * 40

# --- pure: dates ----------------------------------------------------------------------------

WED = date(2026, 11, 11)  # a Wednesday


@pytest.mark.parametrize(
    ("hint", "expected"),
    [
        ("by Friday", date(2026, 11, 13)),
        ("Friday", date(2026, 11, 13)),
        ("this Friday", date(2026, 11, 13)),
        ("next Friday", date(2026, 11, 20)),
        ("Wednesday", WED),
        ("on Mon", date(2026, 11, 16)),
        ("tomorrow", date(2026, 11, 12)),
        ("today", WED),
        ("end of day", WED),
        ("EOD", WED),
        ("end of the week", date(2026, 11, 13)),
        ("next week", date(2026, 11, 16)),
        ("end of next week", date(2026, 11, 20)),
        ("end of month", date(2026, 11, 30)),
        ("next month", date(2026, 12, 1)),
        ("in 3 days", date(2026, 11, 14)),
        ("in two weeks", date(2026, 11, 25)),
        ("Nov 20", date(2026, 11, 20)),
        ("20th November", date(2026, 11, 20)),
        ("January 5", date(2027, 1, 5)),
        ("2026-12-01", date(2026, 12, 1)),
        ("before the board meeting", None),
        ("soon", None),
        ("", None),
        (None, None),
    ],
)
def test_due_hints_become_dates_only_when_sure(hint: str | None, expected: date | None) -> None:
    assert parse_due_hint(hint, WED) == expected


def test_day_labels_and_ordinals() -> None:
    assert day_label(WED, WED) == "today"
    assert day_label(WED + timedelta(days=1), WED) == "tomorrow"
    assert day_label(date(2026, 11, 13), WED) == "Friday"
    assert day_label(date(2026, 11, 23), WED) == "Mon 23 Nov"
    assert ordinal(7) == "seventh" and ordinal(23) == "23rd" and ordinal(11) == "eleventh"


# --- pure: the day model and the sentence ----------------------------------------------------


def test_day_levels_follow_items_meetings_and_weekends() -> None:
    fri = cap.DayLoad(date(2026, 11, 13))
    assert fri.level() == "ok" and fri.level(1) == "ok"
    fri.promises, fri.actions = 2, 1
    assert fri.level(1) == "tight"  # four things due
    fri.promises = 5
    assert fri.level(1) == "over"  # seven
    busy = cap.DayLoad(date(2026, 11, 13), meeting_minutes=6 * 60 + 30)
    assert busy.level(1) == "tight"  # 30 minutes left after one more
    sat = cap.DayLoad(date(2026, 11, 14))
    assert sat.level() == "off" and sat.level(1) == "over"
    away = cap.DayLoad(date(2026, 11, 13), away=True)
    assert away.capacity_minutes == 0 and away.level(1) == "over"


def test_the_agent_says_seventh_promise_and_offers_another_day() -> None:
    fri = cap.DayLoad(date(2026, 11, 13), promises=4, actions=2)
    warn, message = cap.compose(fri, WED, date(2026, 11, 16))
    assert warn is True
    assert message == "That's your seventh promise due Friday. Offer Monday instead?"
    calm = cap.DayLoad(date(2026, 11, 13), promises=1)
    assert cap.compose(calm, WED, None) == (
        False,
        "That makes 2 things due on Friday; your day still has room.",
    )
    meetings = cap.DayLoad(date(2026, 11, 12), meeting_minutes=7 * 60)
    warn, message = cap.compose(meetings, WED, date(2026, 11, 13))
    assert message == "Tomorrow already has 7 hours of meetings. Offer Friday instead?"


# --- DB fixtures -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"integration-service={INTEGRATION_SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-promise-ledger-test")
    for module in (
        promises,
        internal_promises,
        commitments,
        candidates,
        action_create,
    ):
        app.include_router(module.router)
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


def person(t: Tenant, email: str) -> Tenant:
    """Another user of the same tenant, with their own email."""
    uid, clerk_user = uuid.uuid4(), f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s, %(o)s, %(c)s, %(e)s)",
        {"u": str(uid), "o": str(t.org_id), "c": clerk_user, "e": email},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, 'member')",
        {"o": str(t.org_id), "u": str(uid)},
    )
    return Tenant(t.org_id, t.clerk_org, uid, clerk_user)


def tenant_with_email(email: str) -> Tenant:
    t = seed_tenant()
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})
    return t


def internal_headers(t: Tenant) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "integration-service",
        "X-Mytheus-Service-Token": INTEGRATION_SECRET,
        "X-Mytheus-Org": str(t.org_id),
    }


def next_weekday(d: date, weekday: int) -> date:
    return d + timedelta(days=(weekday - d.weekday()) % 7 or 7)


def notifications(user: uuid.UUID, prefix: str) -> list[dict[str, Any]]:
    rows = _sql(
        "SELECT payload FROM outbox WHERE topic = 'notifications' "
        "AND payload->>'user_id' = %(u)s AND payload->>'source_event_id' LIKE %(p)s "
        "ORDER BY created_at",
        {"u": str(user), "p": f"{prefix}%"},
    )
    return [
        r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]) for r in rows
    ]


def manual(t: Tenant, content: str, due: date | None = None, **extra: Any) -> str:
    return str(
        _sql(
            "INSERT INTO commitments (org_id, user_id, content, due_at, channel, "
            "source_window_id, created_by, recipient_user_id, review_state) VALUES "
            "(%(o)s, %(u)s, %(c)s, %(d)s, 'manual', %(w)s, %(u)s, %(r)s, %(rs)s) RETURNING id",
            {
                "o": str(t.org_id),
                "u": str(t.user_id),
                "c": content,
                "d": due,
                "w": f"manual:{uuid.uuid4()}",
                "r": str(extra["recipient"]) if extra.get("recipient") else None,
                "rs": extra.get("review_state", "confirmed"),
            },
        )[0]["id"]
    )


# --- RLS -------------------------------------------------------------------------------------


def test_new_tables_are_tenant_isolated_and_fail_closed(client) -> None:
    import psycopg2

    a, b = seed_tenant(), seed_tenant()
    for t in (a, b):
        colleague = person(t, f"c-{uuid.uuid4().hex[:6]}@example.com")
        promise = manual(t, "Ship it", recipient=colleague.user_id)
        _sql(
            "INSERT INTO promise_date_proposals (org_id, commitment_id, proposer_id, "
            "requester_id, content, proposed_due) VALUES (%(o)s, %(c)s, %(p)s, %(r)s, 'x', "
            "'2026-12-01')",
            {"o": str(t.org_id), "c": promise, "p": str(t.user_id), "r": str(colleague.user_id)},
        )
        _sql(
            "INSERT INTO promise_capture_preferences (org_id, user_id) VALUES (%(o)s, %(u)s)",
            {"o": str(t.org_id), "u": str(t.user_id)},
        )
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            for table in ("promise_date_proposals", "promise_capture_preferences", "commitments"):
                cur.execute("BEGIN")
                cur.execute(f"SELECT count(*) FROM {table}")
                assert cur.fetchone()[0] == 0, f"{table} without app.current_org"
                cur.execute("SELECT set_config('app.current_org', %s, true)", (str(a.org_id),))
                cur.execute(f"SELECT DISTINCT org_id FROM {table}")
                assert {str(r[0]) for r in cur.fetchall()} == {str(a.org_id)}, table
                cur.execute("ROLLBACK")
    finally:
        conn.close()


# --- the ledger and the old contract ---------------------------------------------------------


def test_the_ledger_spans_channels_and_commitments_keeps_its_shape(client) -> None:
    t = tenant_with_email("asha.rao@example.com")
    sign_in(t)
    m = seed_meeting(t.org_id)
    phone = seed_meeting(t.org_id)
    _sql(
        "UPDATE meetings SET source = 'in_person', title = 'Hallway' WHERE id = %(m)s",
        {"m": str(phone)},
    )
    for meeting, channel in ((m, "meeting"), (phone, "in_person")):
        _sql(
            "INSERT INTO commitments (org_id, user_id, meeting_id, content, source_window_id, "
            "channel) VALUES (%(o)s, %(u)s, %(m)s, %(c)s, 'w0', %(ch)s)",
            {
                "o": str(t.org_id),
                "u": str(t.user_id),
                "m": str(meeting),
                "c": f"Promise from {channel}",
                "ch": channel,
            },
        )
    added = manual(t, "Call Acme back")
    ledger = client.get("/api/v1/promises").json()
    by_channel = {i["channel"]: i for i in ledger["items"]}
    assert set(by_channel) == {"meeting", "in_person", "manual"}
    assert by_channel["in_person"]["source"]["label"] == "Hallway"
    assert by_channel["in_person"]["source"]["link"] == f"/meetings/{phone}"
    assert by_channel["manual"]["source"]["label"] == "Added by you"
    assert by_channel["manual"]["id"] == added
    assert ledger["capture"] == {
        "slack_enabled": True,
        "email_enabled": True,
        "email_connected": False,
    }
    only_manual = client.get("/api/v1/promises", params={"channel": "manual"}).json()["items"]
    assert [i["id"] for i in only_manual] == [added]
    # The published contract: meeting promises only, every field it always had.
    old = client.get("/api/v1/commitments").json()["items"]
    assert {i["content"] for i in old} == {"Promise from meeting", "Promise from in_person"}
    assert all(i["meeting_id"] for i in old)


def test_another_persons_promise_is_a_404(client) -> None:
    owner = tenant_with_email("owner@example.com")
    other = person(owner, "nosy@example.com")
    pid = manual(owner, "Private thing")
    sign_in(other)
    assert client.patch(f"/api/v1/promises/{pid}", json={"status": "done"}).status_code == 404
    assert client.post(f"/api/v1/promises/{pid}/confirm", json={}).status_code == 404
    assert all(i["id"] != pid for i in client.get("/api/v1/promises").json()["items"])


# --- manual add, kept, drop, undo ------------------------------------------------------------


def test_manual_add_answers_with_the_capacity_check(client) -> None:
    t = tenant_with_email("busy.bee@example.com")
    sign_in(t)
    friday = next_weekday(date.today(), 4)
    for i in range(6):
        manual(t, f"Thing {i}", friday)
    resp = client.post(
        "/api/v1/promises",
        json={
            "content": "  Send   the deck  ",
            "due_at": friday.isoformat(),
            "recipient_label": "Acme",
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["promise"]["content"] == "Send the deck"
    assert body["promise"]["recipient_name"] == "Acme"
    assert body["promise"]["channel"] == "manual" and body["promise"]["review_state"] == "confirmed"
    check = body["capacity"]
    assert check["warn"] is True and check["items_on_day"] == 7 and check["level"] == "over"
    assert check["message"].startswith("That's your seventh promise due")
    assert check["suggested_date"] is not None
    assert date.fromisoformat(check["suggested_date"]).weekday() < 5
    assert client.post("/api/v1/promises", json={"content": "   "}).status_code == 422
    assert (
        client.post(
            "/api/v1/promises", json={"content": "x", "recipient_user_id": str(t.user_id)}
        ).status_code
        == 422
    )


def test_kept_drop_and_undo(client) -> None:
    t = tenant_with_email("kd@example.com")
    sign_in(t)
    pid = manual(t, "Review the spec")
    done = client.patch(f"/api/v1/promises/{pid}", json={"status": "done"}).json()
    assert done["status"] == "done" and done["done_at"]
    reopened = client.patch(f"/api/v1/promises/{pid}", json={"status": "open"}).json()
    assert reopened["status"] == "open" and reopened["done_at"] is None
    dropped = client.patch(f"/api/v1/promises/{pid}", json={"status": "dismissed"}).json()
    assert dropped["status"] == "dismissed"
    edited = client.patch(
        f"/api/v1/promises/{pid}", json={"status": "open", "due_at": "2026-12-24"}
    ).json()
    assert edited["due_at"] == "2026-12-24"
    cleared = client.patch(f"/api/v1/promises/{pid}", json={"clear_due": True}).json()
    assert cleared["due_at"] is None


# --- capture from Slack (the internal door) and confirm --------------------------------------


def test_captured_promises_land_pending_once_with_their_source(client) -> None:
    t = tenant_with_email("Maya.Patel@example.com")
    sam = person(t, "sam.lee@example.com")
    check = client.post(
        "/internal/promises/capture-check",
        json={"channel": "slack", "author_email": "maya.patel@EXAMPLE.com"},
        headers=internal_headers(t),
    ).json()
    assert check == {"user_id": str(t.user_id), "enabled": True}
    stranger = client.post(
        "/internal/promises/capture-check",
        json={"channel": "slack", "author_email": "nobody@example.com"},
        headers=internal_headers(t),
    ).json()
    assert stranger == {"user_id": None, "enabled": False}
    body = {
        "channel": "slack",
        "author_email": "maya.patel@example.com",
        "source_ref": "slack:T1:C1:1731300000.000100",
        "source_link": "https://acme.slack.com/archives/C1/p1731300000000100",
        "occurred_at": "2026-11-11T09:00:00+00:00",
        "prompt_version": "promise_extract@v1",
        "promises": [
            {
                "content": "Send Sam the revised pricing deck",
                "due_hint": "Friday",
                "due_date": "2026-11-13",
                "recipient_label": "Sam",
                "recipient_email": "sam.lee@example.com",
                "quote": "I'll send you the revised deck by Friday",
                "confidence": 0.92,
            },
            {"content": "Maybe look at churn", "confidence": 0.5},
        ],
    }
    first = client.post("/internal/promises/captured", json=body, headers=internal_headers(t))
    assert first.json() == {"user_id": str(t.user_id), "written": 1}
    again = client.post("/internal/promises/captured", json=body, headers=internal_headers(t))
    assert again.json()["written"] == 0  # the replay guard
    assert len(notifications(t.user_id, "promise-found:")) == 1

    sign_in(t)
    ledger = client.get("/api/v1/promises").json()
    assert ledger["pending_review"] == 1
    [item] = ledger["items"]
    assert item["review_state"] == "pending" and item["channel"] == "slack"
    assert item["recipient_user_id"] == str(sam.user_id) and item["recipient_name"] == "Sam Lee"
    assert item["source"] == {
        "channel": "slack",
        "label": "Slack",
        "link": "https://acme.slack.com/archives/C1/p1731300000000100",
        "external": True,
        "quote": "I'll send you the revised deck by Friday",
        "at": "2026-11-11T09:00:00Z",
    }
    confirmed = client.post(f"/api/v1/promises/{item['id']}/confirm", json={}, params={"tz": "UTC"})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["promise"]["review_state"] == "confirmed"
    assert confirmed.json()["capacity"]["due_date"] == "2026-11-13"

    # Turned off: nothing is captured, and the check says so before any model call.
    assert (
        client.put(
            "/api/v1/promises/capture-preferences",
            json={"slack_enabled": False, "email_enabled": True},
        ).json()["slack_enabled"]
        is False
    )
    off = client.post(
        "/internal/promises/capture-check",
        json={"channel": "slack", "author_email": "maya.patel@example.com"},
        headers=internal_headers(t),
    ).json()
    assert off == {"user_id": str(t.user_id), "enabled": False}
    body["source_ref"] = "slack:T1:C1:other"
    assert (
        client.post("/internal/promises/captured", json=body, headers=internal_headers(t)).json()[
            "written"
        ]
        == 0
    )


def test_the_capture_door_needs_the_integration_caller(client) -> None:
    t = seed_tenant()
    bad = {**internal_headers(t), "X-Mytheus-Service-Token": "x" * 40}
    resp = client.post(
        "/internal/promises/capture-check",
        json={"channel": "slack", "author_email": "u@example.com"},
        headers=bad,
    )
    assert resp.status_code in (401, 403)


# --- hand off ----------------------------------------------------------------------------------


def test_hand_off_creates_a_pending_copy_and_undo_takes_it_back(client) -> None:
    t = tenant_with_email("giver.one@example.com")
    taker = person(t, "taker.two@example.com")
    sign_in(t)
    pid = manual(t, "Write the launch post", date(2026, 12, 3))
    resp = client.post(
        f"/api/v1/promises/{pid}/hand-off",
        json={"to_user_id": str(taker.user_id), "note": "You know it best"},
    )
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["handed_to_name"] == "Taker Two"
    assert out["promise"]["status"] == "dismissed"
    assert out["promise"]["handed_off_to"] == str(taker.user_id)
    [note] = notifications(taker.user_id, "promise-handoff:")
    assert note["title"].startswith("Giver One handed you a promise")
    assert client.patch(f"/api/v1/promises/{pid}", json={"status": "open"}).status_code == 409

    sign_in(taker)
    [copy] = client.get("/api/v1/promises").json()["items"]
    assert copy["review_state"] == "pending" and copy["handed_off_from"] == pid
    assert copy["source"]["label"] == "Handed off by Giver One"
    assert copy["due_at"] == "2026-12-03"

    sign_in(t)
    back = client.post(f"/api/v1/promises/{pid}/hand-off/undo")
    assert back.status_code == 200 and back.json()["status"] == "open"
    sign_in(taker)
    assert client.get("/api/v1/promises").json()["items"] == []

    # Once the colleague confirmed it, it is theirs: the undo is refused.
    sign_in(t)
    copy_id = client.post(
        f"/api/v1/promises/{pid}/hand-off", json={"to_user_id": str(taker.user_id)}
    ).json()["copy_id"]
    sign_in(taker)
    assert client.post(f"/api/v1/promises/{copy_id}/confirm", json={}).status_code == 200
    sign_in(t)
    assert client.post(f"/api/v1/promises/{pid}/hand-off/undo").status_code == 409
    assert (
        client.post(
            f"/api/v1/promises/{pid}/hand-off", json={"to_user_id": str(t.user_id)}
        ).status_code
        == 422
    )


# --- renegotiate ------------------------------------------------------------------------------


def test_renegotiating_my_own_promise_applies_at_once(client) -> None:
    t = tenant_with_email("solo@example.com")
    sign_in(t)
    pid = manual(t, "Tidy the backlog", date.today() + timedelta(days=2))
    new_due = date.today() + timedelta(days=6)
    resp = client.post(
        "/api/v1/promises/date-proposals",
        json={"target": "commitment", "target_id": pid, "proposed_due": new_due.isoformat()},
    )
    assert resp.status_code == 201, resp.text
    out = resp.json()
    assert out["mode"] == "applied" and out["due_at"] == new_due.isoformat()
    assert out["previous_due"] == (date.today() + timedelta(days=2)).isoformat()
    same = client.post(
        "/api/v1/promises/date-proposals",
        json={"target": "commitment", "target_id": pid, "proposed_due": new_due.isoformat()},
    )
    assert same.status_code == 422


def test_renegotiating_asks_the_requester_who_accepts_or_declines(client) -> None:
    t = tenant_with_email("owes.it@example.com")
    sam = person(t, "asked.for.it@example.com")
    sign_in(t)
    friday = date.today() + timedelta(days=3)
    monday = date.today() + timedelta(days=6)
    pid = manual(t, "Send the contract", friday, recipient=sam.user_id)
    asked = client.post(
        "/api/v1/promises/date-proposals",
        json={
            "target": "commitment",
            "target_id": pid,
            "proposed_due": monday.isoformat(),
            "note": "Legal needs a day more",
        },
    ).json()
    assert asked["mode"] == "proposed" and asked["due_at"] == friday.isoformat()
    proposal = asked["proposal"]
    assert proposal["requester_name"] == "Asked For It" and proposal["state"] == "pending"
    [note] = notifications(sam.user_id, f"promise-date:{proposal['id']}:asked")
    assert "Owes It asks to move a promise" in note["title"]
    mine = client.get("/api/v1/promises").json()["items"][0]
    assert mine["pending_proposal"]["id"] == proposal["id"]

    # Withdraw (the proposer's undo), then ask again.
    assert (
        client.post(f"/api/v1/promises/date-proposals/{proposal['id']}/withdraw").json()["state"]
        == "withdrawn"
    )
    again = client.post(
        "/api/v1/promises/date-proposals",
        json={"target": "commitment", "target_id": pid, "proposed_due": monday.isoformat()},
    ).json()["proposal"]

    # The proposer cannot accept their own proposal.
    assert client.post(f"/api/v1/promises/date-proposals/{again['id']}/accept").status_code == 404

    sign_in(sam)
    incoming = client.get("/api/v1/promises/date-proposals", params={"role": "incoming"}).json()
    assert [p["id"] for p in incoming["items"]] == [again["id"]]
    accepted = client.post(f"/api/v1/promises/date-proposals/{again['id']}/accept")
    assert accepted.status_code == 200 and accepted.json()["state"] == "accepted"
    assert client.post(f"/api/v1/promises/date-proposals/{again['id']}/decline").status_code == 409
    [told] = notifications(t.user_id, f"promise-date:{again['id']}:accepted")
    assert told["title"].startswith("Asked For It accepted")
    sign_in(t)
    assert client.get("/api/v1/promises").json()["items"][0]["due_at"] == monday.isoformat()

    # A decline keeps the date.
    later = monday + timedelta(days=7)
    third = client.post(
        "/api/v1/promises/date-proposals",
        json={"target": "commitment", "target_id": pid, "proposed_due": later.isoformat()},
    ).json()["proposal"]
    sign_in(sam)
    assert (
        client.post(f"/api/v1/promises/date-proposals/{third['id']}/decline").json()["state"]
        == "declined"
    )
    sign_in(t)
    assert client.get("/api/v1/promises").json()["items"][0]["due_at"] == monday.isoformat()


def test_an_assigned_action_is_renegotiated_with_its_assigner(client) -> None:
    t = tenant_with_email("assignee@example.com")
    boss = person(t, "assigner@example.com")
    friday = date.today() + timedelta(days=3)
    action = str(
        _sql(
            "INSERT INTO action_items (org_id, content, assignee_id, due_date, status, confidence, "
            "confirmed_by) VALUES (%(o)s, 'Fix the export', %(a)s, %(d)s, 'confirmed', 1, %(b)s) "
            "RETURNING id",
            {"o": str(t.org_id), "a": str(t.user_id), "d": friday, "b": str(boss.user_id)},
        )[0]["id"]
    )
    sign_in(boss)
    # Only the person who owes it may renegotiate.
    assert (
        client.post(
            "/api/v1/promises/date-proposals",
            json={"target": "action", "target_id": action, "proposed_due": friday.isoformat()},
        ).status_code
        == 404
    )
    sign_in(t)
    out = client.post(
        "/api/v1/promises/date-proposals",
        json={
            "target": "action",
            "target_id": action,
            "proposed_due": (friday + timedelta(days=3)).isoformat(),
        },
    ).json()
    assert out["mode"] == "proposed" and out["proposal"]["target"] == "action"
    sign_in(boss)
    client.post(f"/api/v1/promises/date-proposals/{out['proposal']['id']}/accept")
    due = _sql("SELECT due_date FROM action_items WHERE id = %(a)s", {"a": action})[0]["due_date"]
    assert due == friday + timedelta(days=3)


# --- capacity --------------------------------------------------------------------------------


def test_capacity_counts_due_items_meetings_and_focus(client) -> None:
    t = tenant_with_email("cap@example.com")
    sign_in(t)
    monday = date.today() - timedelta(days=date.today().weekday())
    tuesday = monday + timedelta(days=1)
    manual(t, "One", tuesday)
    manual(t, "Pending is not counted", tuesday, review_state="pending")
    _sql(
        "INSERT INTO action_items (org_id, content, assignee_id, due_date, status, confidence) "
        "VALUES (%(o)s, 'Two', %(a)s, %(d)s, 'confirmed', 1)",
        {"o": str(t.org_id), "a": str(t.user_id), "d": tuesday},
    )
    start = datetime.combine(tuesday, datetime.min.time(), tzinfo=UTC) + timedelta(hours=10)
    for title, hours, kind in (("Sync", 2, None), ("Deep work", 3, "focusTime")):
        m = seed_meeting(t.org_id)
        add_participant(m, t.org_id, t.user_id)
        _sql(
            "UPDATE meetings SET title = %(t)s, scheduled_start = %(s)s, scheduled_end = %(e)s, "
            "calendar_event_kind = %(k)s, calendar_event_id = %(ce)s WHERE id = %(m)s",
            {
                "t": title,
                "s": start,
                "e": start + timedelta(hours=hours),
                "k": kind,
                "ce": f"evt-{uuid.uuid4().hex[:8]}" if kind else None,
                "m": str(m),
            },
        )
    body = client.get("/api/v1/promises/capacity", params={"tz": "UTC"}).json()
    assert body["start"] == monday.isoformat() and len(body["days"]) == 7
    tue = body["days"][1]
    assert (tue["promises"], tue["actions"], tue["items"]) == (1, 1, 2)
    assert tue["meeting_minutes"] == 120 and tue["focus_minutes"] == 180
    assert tue["load_minutes"] == 2 * 60 + 120 and tue["level"] == "ok"
    assert body["calendar_synced"] is True
    assert body["days"][5]["level"] == "off"
    check = client.get(
        "/api/v1/promises/capacity/check",
        params={"due_date": tuesday.isoformat(), "tz": "Nowhere/Bad"},
    ).json()
    assert check["items_on_day"] == 3 and check["warn"] is False


# --- landing warnings --------------------------------------------------------------------------


async def test_a_meeting_promise_gets_its_channel_date_and_landing_warning(client) -> None:
    t = tenant_with_email("phone.person@example.com")
    phone = seed_meeting(t.org_id)
    _sql("UPDATE meetings SET source = 'in_person' WHERE id = %(m)s", {"m": str(phone)})
    tomorrow = date.today() + timedelta(days=1)
    for i in range(5):
        manual(t, f"Already due {i}", tomorrow)
    value = result(t, phone, 0.9, window="w-phone")
    value["payload_json"] = json.dumps(
        {"commitments": [{"content": "You will call the landlord", "due_hint": "tomorrow"}]}
    )
    assert await apply(value) is True
    row = _sql(
        "SELECT channel, due_at FROM commitments WHERE meeting_id = %(m)s", {"m": str(phone)}
    )[0]
    assert row["channel"] == "in_person" and row["due_at"] == tomorrow
    if tomorrow.weekday() < 5:
        [warning] = notifications(t.user_id, "capacity:commitment:")
        assert warning["title"].startswith("That's your sixth promise due tomorrow")
        assert warning["link"].startswith("/work/promises?offer=commitment:")


def test_an_action_assigned_by_someone_else_warns_the_assignee_only(client) -> None:
    t = tenant_with_email("lead@example.com")
    dev = person(t, "dev@example.com")
    due = date.today() + timedelta(days=1)
    while due.weekday() >= 5:
        due += timedelta(days=1)
    for i in range(6):
        manual(dev, f"Dev thing {i}", due)
    sign_in(t)
    resp = client.post(
        "/api/v1/actions",
        json={
            "content": "Patch the login bug",
            "assignee_id": str(dev.user_id),
            "due_date": due.isoformat(),
        },
    )
    assert resp.status_code == 201, resp.text
    [warning] = notifications(dev.user_id, "capacity:action:")
    assert "seventh promise" in warning["title"]
    assert notifications(t.user_id, "capacity:") == []
    # Assigning to yourself warns nobody (you checked before saying yes).
    client.post("/api/v1/actions", json={"content": "Mine", "due_date": due.isoformat()})
    assert notifications(t.user_id, "capacity:") == []
