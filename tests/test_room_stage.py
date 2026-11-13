"""Polish program, Phase 1: the room's stage bar, close-out ritual, catch-up line and stand-ins.

Over HTTP as `mytheus_app` (real RLS, real transactions; only `verify_token` stubbed):

- agenda clock: PATCH `in_progress` stamps `started_at` once, `covered` stamps `ended_at`,
  `pending` clears both; `extend_minutes` adds to `extended_minutes` (422 past 240, 409 once
  ended); "Start next" (`POST /agenda/advance`) finishes the item in progress and starts the
  first pending one, atomically, host only; the MeetingAgent's in-progress update stamps the
  clock too.
- wrap-up: start is host-only and idempotent (the deadline never moves), cancel clears it, the
  GET lists surfaced proposed cards / open items / unowned actions, close ends the meeting,
  hands every proposed card to the named owner (a participant), writes ONE `meeting-events`
  `ended` outbox row, is idempotent, and 409s on a meeting ended some other way; the Home
  "Needs review" queue then shows those cards to that owner only.
- catch-up summary: confirmed decisions/actions and the current item; participants only;
  another tenant's meeting is 404.
- stand-ins: deny by default (no grant -> `denied`, nothing read), allow -> the target's own
  records of the class's kind only, confidential meetings never a source for a non-member;
  each ask writes one collaboration row (an answered one re-hydrates as the asker's context
  fill); asking someone who came in person, or yourself, is 404.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import dashboard
from services.core_api.routes import collaborations, meetings, room_closeout, stand_ins
from tests.conftest import APP_URL
from tests.test_a2a_grants import seed_grant
from tests.test_candidates import outbox_for, seed_action, seed_decision
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
from tests.test_suggestions import seed_user


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-room-stage-test")
    app.include_router(meetings.router)
    app.include_router(room_closeout.router)
    app.include_router(stand_ins.router)
    app.include_router(collaborations.router)
    app.include_router(dashboard.router)
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


def hosted_meeting(host: Tenant, *others: Tenant) -> uuid.UUID:
    m = seed_meeting(host.org_id)
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    for p in others:
        add_participant(m, p.org_id, p.user_id)
    return m


def seed_items(t: Tenant, m: uuid.UUID, *titles: str, minutes: int = 10) -> list[str]:
    ids = []
    for i, title in enumerate(titles):
        iid = uuid.uuid4()
        _sql(
            "INSERT INTO meeting_agenda_items (id, org_id, meeting_id, position, title, "
            "duration_minutes) VALUES (%(i)s, %(o)s, %(m)s, %(p)s, %(t)s, %(d)s)",
            {"i": str(iid), "o": str(t.org_id), "m": str(m), "p": i, "t": title, "d": minutes},
        )
        ids.append(str(iid))
    return ids


def item(m: uuid.UUID, iid: str) -> dict[str, Any]:
    return _sql("SELECT * FROM meeting_agenda_items WHERE id = %(i)s", {"i": iid})[0]


# --- the agenda clock ------------------------------------------------------------------------


def test_status_changes_drive_the_items_clock(client):
    host = seed_tenant()
    m = hosted_meeting(host)
    (a,) = seed_items(host, m, "Roadmap")
    sign_in(host)
    started = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"status": "in_progress"})
    assert started.status_code == 200, started.text
    body = started.json()
    assert body["started_at"] is not None and body["ended_at"] is None
    assert body["extended_minutes"] == 0 and body["status_source"] == "host"
    again = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"status": "in_progress"})
    assert again.json()["started_at"] == body["started_at"]  # stamped once

    covered = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"status": "covered"}).json()
    assert covered["ended_at"] is not None and covered["started_at"] == body["started_at"]
    reopened = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"status": "pending"}).json()
    assert reopened["started_at"] is None and reopened["ended_at"] is None


def test_extend_adds_minutes_and_has_limits(client):
    host = seed_tenant()
    guest = seed_user(host)
    m = hosted_meeting(host, guest)
    (a,) = seed_items(host, m, "Budget")
    sign_in(host)
    resp = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 5})
    assert resp.status_code == 200, resp.text
    assert resp.json()["extended_minutes"] == 5 and resp.json()["duration_minutes"] == 10
    both = client.patch(
        f"/api/v1/meetings/{m}/agenda/{a}", json={"status": "in_progress", "extend_minutes": 60}
    )
    assert both.json()["extended_minutes"] == 65 and both.json()["status"] == "in_progress"
    assert client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={}).status_code == 422
    assert (
        client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 61}).status_code
        == 422
    )
    for _ in range(2):
        client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 60})
    over = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 60})
    assert over.status_code == 422 and item(m, a)["extended_minutes"] == 185

    sign_in(guest)
    assert (
        client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 5}).status_code
        == 403
    )
    sign_in(host)
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    ended = client.patch(f"/api/v1/meetings/{m}/agenda/{a}", json={"extend_minutes": 5})
    assert ended.status_code == 409


def test_start_next_finishes_the_current_item_and_starts_the_next(client):
    host = seed_tenant()
    guest = seed_user(host)
    m = hosted_meeting(host, guest)
    a, b, c = seed_items(host, m, "One", "Two", "Three")
    sign_in(host)
    first = client.post(f"/api/v1/meetings/{m}/agenda/advance", json={})
    assert first.status_code == 200, first.text
    assert first.json()["current"]["id"] == a
    assert [it["id"] for it in first.json()["items"]] == [a]

    second = client.post(f"/api/v1/meetings/{m}/agenda/advance", json={"outcome": "skipped"})
    body = second.json()
    assert body["current"]["id"] == b
    assert {it["id"]: it["status"] for it in body["items"]} == {a: "skipped", b: "in_progress"}
    assert item(m, a)["ended_at"] is not None and item(m, b)["started_at"] is not None

    client.post(f"/api/v1/meetings/{m}/agenda/advance", json={})
    last = client.post(f"/api/v1/meetings/{m}/agenda/advance", json={}).json()
    assert last["current"] is None and [it["id"] for it in last["items"]] == [c]
    assert item(m, c)["status"] == "covered"

    sign_in(guest)
    assert client.post(f"/api/v1/meetings/{m}/agenda/advance", json={}).status_code == 403
    other = seed_tenant()
    sign_in(other)
    assert client.post(f"/api/v1/meetings/{m}/agenda/advance", json={}).status_code == 404


async def test_the_meeting_agent_starting_an_item_stamps_its_clock(migrated_db):
    from tests.test_meeting_agendas import meeting_update
    from tests.test_worklogs import apply

    host = seed_tenant()
    m = hosted_meeting(host)
    (a,) = seed_items(host, m, "Hiring")
    await apply(meeting_update(host, m, [{"item_id": a, "status": "in_progress"}]))
    row = item(m, a)
    assert row["status"] == "in_progress" and row["started_at"] is not None


# --- wrap-up ---------------------------------------------------------------------------------


def test_wrap_up_start_cancel_and_what_is_left(client):
    host = seed_tenant()
    guest = seed_user(host)
    m = hosted_meeting(host, guest)
    a, b = seed_items(host, m, "Open", "Done")
    _sql("UPDATE meeting_agenda_items SET status = 'covered' WHERE id = %(i)s", {"i": b})
    d = seed_decision(host, m)
    act = seed_action(host, m)
    owned = seed_action(host, m, assignee=guest.user_id)
    seed_decision(host, m, confidence=0.5)  # silent band: never shown
    seed_decision(host, m, status="confirmed")

    sign_in(guest)
    assert client.post(f"/api/v1/meetings/{m}/wrap-up/start").status_code == 403
    left = client.get(f"/api/v1/meetings/{m}/wrap-up").json()
    assert left["wrap_up_started_at"] is None and left["deadline"] is None

    sign_in(host)
    started = client.post(f"/api/v1/meetings/{m}/wrap-up/start")
    assert started.status_code == 200, started.text
    body = started.json()
    assert body["seconds"] == 60 and body["wrap_up_by"] == str(host.user_id)
    assert {c["id"] for c in body["pending"]} == {str(d), str(act), str(owned)}
    assert body["unowned_action_ids"] == [str(act)]
    assert [it["id"] for it in body["open_items"]] == [a]
    again = client.post(f"/api/v1/meetings/{m}/wrap-up/start").json()
    assert again["wrap_up_started_at"] == body["wrap_up_started_at"]

    sign_in(guest)  # the room reads the same deadline
    assert client.get(f"/api/v1/meetings/{m}/wrap-up").json()["deadline"] == body["deadline"]

    sign_in(host)
    cancelled = client.post(f"/api/v1/meetings/{m}/wrap-up/cancel").json()
    assert cancelled["wrap_up_started_at"] is None
    meeting = client.get(f"/api/v1/meetings/{m}").json()
    assert meeting["wrap_up_started_at"] is None and meeting["status"] != "ended"


def test_close_ends_the_meeting_and_hands_leftovers_to_a_named_owner(client):
    host = seed_tenant()
    guest = seed_user(host)
    stranger = seed_user(host)
    m = hosted_meeting(host, guest)
    d = seed_decision(host, m)
    act = seed_action(host, m)
    hosts_task = seed_action(host, m, assignee=host.user_id)
    strangers_task = seed_action(host, m, assignee=stranger.user_id)
    kept = seed_decision(host, m, status="confirmed")

    sign_in(host)
    client.post(f"/api/v1/meetings/{m}/wrap-up/start")
    bad = client.post(
        f"/api/v1/meetings/{m}/wrap-up/close", json={"owner_user_id": str(stranger.user_id)}
    )
    assert bad.status_code == 422
    sign_in(guest)
    assert client.post(f"/api/v1/meetings/{m}/wrap-up/close", json={}).status_code == 403

    sign_in(host)
    closed = client.post(
        f"/api/v1/meetings/{m}/wrap-up/close",
        json={"owner_user_id": str(guest.user_id), "reason": "timer"},
    )
    assert closed.status_code == 200, closed.text
    body = closed.json()
    assert body["status"] == "ended" and body["closed_by"] == str(host.user_id)
    assert body["review_owner_id"] == str(guest.user_id)
    assert (body["moved_decisions"], body["moved_actions"]) == (1, 3)
    owners = _sql(
        "SELECT id, review_owner_id FROM decisions WHERE meeting_id = %(m)s", {"m": str(m)}
    )
    by_id = {str(r["id"]): r["review_owner_id"] for r in owners}
    by_id = {k: (str(v) if v else None) for k, v in by_id.items()}
    assert by_id[str(d)] == str(guest.user_id) and by_id[str(kept)] is None
    assert _sql("SELECT review_owner_id FROM action_items WHERE id = %(i)s", {"i": str(act)})[0][
        "review_owner_id"
    ] == str(guest.user_id)
    # A task stays with the person it is for; one whose owner is not in the meeting goes to
    # the named owner like a task with no owner.
    task_owner = {
        str(r["id"]): str(r["review_owner_id"])
        for r in _sql(
            "SELECT id, review_owner_id FROM action_items WHERE meeting_id = %(m)s",
            {"m": str(m)},
        )
    }
    assert task_owner[str(hosts_task)] == str(host.user_id)
    assert task_owner[str(strangers_task)] == str(guest.user_id)

    ended = [
        r for r in outbox_for(host.org_id, "meeting-events") if r["payload"]["meeting_id"] == str(m)
    ]
    assert len(ended) == 1 and ended[0]["payload"]["event_type"] == "ended"
    audit = _sql(
        "SELECT metadata FROM audit_logs WHERE target_id = %(m)s AND action = 'meeting.closed'",
        {"m": str(m)},
    )
    assert audit[0]["metadata"]["reason"] == "timer"

    repeat = client.post(f"/api/v1/meetings/{m}/wrap-up/close", json={})
    assert repeat.status_code == 200
    assert repeat.json()["closed_at"] == body["closed_at"]
    after = [
        r for r in outbox_for(host.org_id, "meeting-events") if r["payload"]["meeting_id"] == str(m)
    ]
    assert len(after) == 1  # a repeated close writes nothing
    assert client.post(f"/api/v1/meetings/{m}/wrap-up/start").status_code == 409

    # The Home queue: the leftover cards are the named owner's, nobody else's.
    def queue_ids() -> set[str]:
        resp = client.get("/bff/dashboard")
        assert resp.status_code == 200, resp.text
        return {r["id"] for r in resp.json()["needs_review"]["items"]}

    sign_in(guest)
    assert {str(d), str(act)} <= queue_ids()
    sign_in(host)
    assert not ({str(d), str(act)} & queue_ids())


def test_closing_a_meeting_ended_another_way_is_409(client):
    host = seed_tenant()
    m = hosted_meeting(host)
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/wrap-up/close", json={}).status_code == 409


# --- catch-up summary ------------------------------------------------------------------------


def test_catch_up_summary_counts_and_current_item(client):
    host = seed_tenant()
    late = seed_user(host)
    outsider = seed_user(host)
    m = hosted_meeting(host, late)
    a, b, _c = seed_items(host, m, "One", "Two", "Three")
    _sql("UPDATE meeting_agenda_items SET status = 'covered' WHERE id = %(i)s", {"i": a})
    _sql("UPDATE meeting_agenda_items SET status = 'in_progress' WHERE id = %(i)s", {"i": b})
    seed_decision(host, m, status="confirmed")
    seed_decision(host, m, status="confirmed")
    seed_decision(host, m)  # proposed: not counted

    sign_in(late)
    body = client.get(f"/api/v1/meetings/{m}/catch-up-summary").json()
    assert body["decision_count"] == 2 and len(body["decisions"]) == 2
    assert body["action_count"] == 0
    assert (body["agenda_done"], body["agenda_total"]) == (1, 3)
    assert body["current_item"]["id"] == b

    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{m}/catch-up-summary").status_code == 403
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/catch-up-summary").status_code == 404


# --- stand-ins -------------------------------------------------------------------------------


def stand_in_meeting() -> tuple[Tenant, Tenant, uuid.UUID]:
    asker = seed_tenant()
    sarah = seed_user(asker)
    m = hosted_meeting(asker, sarah)
    _sql(
        "UPDATE meeting_participants SET attendance = 'agent', proxy_update = 'Pricing page "
        "ships Friday.' WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(sarah.user_id)},
    )
    return asker, sarah, m


def collab_rows(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM agent_collaborations WHERE meeting_id = %(m)s ORDER BY created_at",
        {"m": str(m)},
    )


def test_a_stand_in_ask_is_denied_by_default_and_logged(client):
    asker, sarah, m = stand_in_meeting()
    sign_in(asker)
    resp = client.post(
        f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask",
        json={"question": "What did we decide about pricing?"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["outcome"] == "denied" and body["sources"] == []
    assert body["query_class"] == "decision_lineage" and body["target_scope"] == "org"
    rows = collab_rows(m)
    assert len(rows) == 1 and rows[0]["result"] == "authz_denied"
    assert rows[0]["hop"] == "peer" and rows[0]["responder"] == f"user:{sarah.user_id}"
    assert rows[0]["fill_content"] is None


def test_an_allowed_ask_answers_from_the_targets_own_records_only(client):
    asker, sarah, m = stand_in_meeting()
    seed_grant(asker.org_id, "member", "decision_lineage", "org", "allow")
    seed_grant(asker.org_id, "member", "meeting_context", "org", "allow")
    # Sarah's earlier meeting (not the asker's), and a confidential one the asker was not in.
    earlier = seed_meeting(asker.org_id)
    add_participant(earlier, asker.org_id, sarah.user_id)
    _sql(
        "INSERT INTO decisions (id, org_id, meeting_id, content, confidence, status) VALUES "
        "(gen_random_uuid(), %(o)s, %(m)s, 'Pricing stays flat for Q1', 0.9, 'confirmed')",
        {"o": str(asker.org_id), "m": str(earlier)},
    )
    secret = seed_meeting(asker.org_id)
    add_participant(secret, asker.org_id, sarah.user_id)
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(secret)})
    _sql(
        "INSERT INTO decisions (id, org_id, meeting_id, content, confidence, status) VALUES "
        "(gen_random_uuid(), %(o)s, %(m)s, 'Pricing secret plan', 0.9, 'confirmed')",
        {"o": str(asker.org_id), "m": str(secret)},
    )
    # Someone else's decision about pricing, in a meeting Sarah was not in.
    elsewhere = seed_meeting(asker.org_id)
    add_participant(elsewhere, asker.org_id, asker.user_id)
    _sql(
        "INSERT INTO decisions (id, org_id, meeting_id, content, confidence, status) VALUES "
        "(gen_random_uuid(), %(o)s, %(m)s, 'Pricing elsewhere', 0.9, 'confirmed')",
        {"o": str(asker.org_id), "m": str(elsewhere)},
    )

    sign_in(asker)
    body = client.post(
        f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask",
        json={"question": "What did we decide about pricing?"},
    ).json()
    assert body["outcome"] == "answered"
    assert [s["text"] for s in body["sources"]] == ["Pricing stays flat for Q1"]

    context = client.post(
        f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask",
        json={"question": "Anything on the launch?"},
    ).json()
    assert context["query_class"] == "meeting_context"
    assert context["sources"] == [
        {
            "kind": "update",
            "id": None,
            "text": "Pricing page ships Friday.",
            "meeting_id": None,
            "meeting_title": None,
            "status": None,
            "due_date": None,
            "occurred_at": None,
        }
    ]
    # An answered ask is the asker's own context fill: it re-hydrates with their sidebar.
    fills = client.get(f"/api/v1/meetings/{m}/context-fills").json()["items"]
    assert {f["id"] for f in fills} == {body["fill_id"], context["fill_id"]}
    assert all(f["hop"] == "peer" for f in fills)

    # A deny row wins over the allow.
    seed_grant(asker.org_id, "member", "action_status", "org", "deny")
    denied = client.post(
        f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask",
        json={"question": "What tasks is she on?"},
    ).json()
    assert denied["outcome"] == "denied"


def test_only_someone_who_sent_their_agent_can_be_asked(client):
    asker, sarah, m = stand_in_meeting()
    present = seed_user(asker)
    add_participant(m, asker.org_id, present.user_id)
    outsider = seed_user(asker)
    sign_in(asker)
    q = {"question": "Status?"}
    assert (
        client.post(f"/api/v1/meetings/{m}/stand-ins/{present.user_id}/ask", json=q).status_code
        == 404
    )
    assert (
        client.post(f"/api/v1/meetings/{m}/stand-ins/{asker.user_id}/ask", json=q).status_code
        == 404
    )
    sign_in(outsider)
    assert (
        client.post(f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask", json=q).status_code
        == 403
    )
    sign_in(seed_tenant())
    assert (
        client.post(f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask", json=q).status_code
        == 404
    )
    sign_in(asker)
    blank = client.post(
        f"/api/v1/meetings/{m}/stand-ins/{sarah.user_id}/ask", json={"question": "  "}
    )
    assert blank.status_code == 422


def test_classifier_matches_the_agent_runtime_pep():
    assert stand_ins.classify_query("Why did we approve the vendor?") == "decision_lineage"
    assert stand_ins.classify_query("Who owns the migration ticket?") == "action_status"
    assert stand_ins.classify_query("How is the launch going?") == "meeting_context"
    assert stand_ins.keywords("What about the pricing page and pricing?") == ["pricing", "page"]
