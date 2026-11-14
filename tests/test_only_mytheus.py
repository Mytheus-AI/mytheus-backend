"""Polish wave 3, "Only Mytheus can do these", against a real Postgres.

- decision reversal: superseding a decision records one reversal per linked open action of the
  replaced decision's meeting, notifies each owner once, and the owner keeps, re-scopes or
  closes the task (idempotent, 409 on a different second answer, 404 for anyone else);
- delegated answers: the statement list (participants only, frozen once the meeting is over,
  ids kept for unchanged text), the internal read for agent-runtime-service (attendance
  `agent` only), the consumer's log write (once per answer id) and the delegator-only log read;
- status asks: one click asks the owners, a repeat asks nobody twice, the answer goes back to
  the asker;
- promise drift: owner first, host only two days later, never twice, nothing for a closed task;
- weekly digest: built from the graph, filtered by listen-for topics, one per week, notified
  once, quiet weeks silent, preferences honoured, email not connected recorded;
- precheck: the keyword fallback finds an earlier decision; status items map to open actions;
- integration-service answers `connected: false` without Jira; notification-service records a
  digest email as skipped without a provider.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import precheck as precheck_mod
from services.core_api import notification_client
from services.core_api.delegation_log import log_delegated_answer
from services.core_api.digest import build_digest, previous_week_start, week_start_of
from services.core_api.routes import (
    candidates,
    decision_reversals,
    delegated_answers,
    digests,
    internal_meetings,
    internal_promise_drift,
    status_asks,
    status_requests,
)
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    make_creator,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user, task

SECRET = "o" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"workflow-service={SECRET},agent-runtime-service={'a' * 40}"
    )
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-only-mytheus-test")
    for module in (
        candidates,
        decision_reversals,
        delegated_answers,
        status_asks,
        status_requests,
        internal_promise_drift,
        digests,
    ):
        app.include_router(module.router)
    app.include_router(precheck_mod.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    notification_client.set_notifications(None)
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def wf(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def rt(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": "a" * 40,
        "X-Mytheus-Org": str(org_id),
    }


def set_email(t: Tenant, email: str) -> None:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})


def decision(t: Tenant, m: uuid.UUID, content: str, status: str = "confirmed") -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s) RETURNING id",
        {"o": str(t.org_id), "m": str(m), "c": content, "s": status},
    )
    return uuid.UUID(str(row[0]["id"]))


def notifications(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


# --- 1. decision reversal --------------------------------------------------------------------


def _reversal_world() -> tuple[Tenant, Tenant, uuid.UUID, uuid.UUID, uuid.UUID]:
    host = seed_tenant()
    owner = add_user(host)
    set_email(owner, "raj.mehta@example.com")
    m1, m2 = seed_meeting(host.org_id), seed_meeting(host.org_id)
    for m in (m1, m2):
        add_participant(m, host.org_id, host.user_id)
        make_creator(m, host.user_id)
        add_participant(m, host.org_id, owner.user_id)
    _sql("UPDATE meetings SET title = 'Vendor review' WHERE id = %(m)s", {"m": str(m1)})
    old = decision(host, m1, "Go with vendor A")
    action = task(host, m1, owner.user_id, "Draft the vendor A contract")
    task(host, m1, owner.user_id, "Already finished", status="done")
    new = decision(host, m2, "Switch to vendor B", status="proposed")
    return host, owner, old, new, action


def test_superseding_a_decision_asks_each_linked_owner_once(client):
    host, owner, old, new, action = _reversal_world()
    sign_in(host)
    res = client.post(f"/api/v1/decisions/{new}/confirm", json={"supersedes": str(old)})
    assert res.status_code == 200, res.text
    rows = _sql("SELECT * FROM decision_reversals WHERE new_decision_id = %(n)s", {"n": str(new)})
    assert [(str(r["action_id"]), str(r["owner_user_id"]), r["status"]) for r in rows] == [
        (str(action), str(owner.user_id), "pending")
    ]
    sent = notifications(f"decision-reversal:{rows[0]['id']}")
    assert len(sent) == 1
    assert sent[0]["user_id"] == str(owner.user_id)
    assert sent[0]["link"] == f"/work/tasks?reversal={rows[0]['id']}"
    assert "Go with vendor A" in sent[0]["body"] and "Switch to vendor B" in sent[0]["body"]
    # Re-confirm is a no-op: no second row, no second notification.
    again = client.post(f"/api/v1/decisions/{new}/confirm", json={"supersedes": str(old)})
    assert again.status_code == 200
    assert len(notifications(f"decision-reversal:{rows[0]['id']}")) == 1


def test_owner_lists_and_resolves_a_reversal(client):
    host, owner, old, new, action = _reversal_world()
    sign_in(host)
    client.post(f"/api/v1/decisions/{new}/confirm", json={"supersedes": str(old)})
    sign_in(owner)
    listed = client.get("/api/v1/decision-reversals").json()["items"]
    assert len(listed) == 1
    item = listed[0]
    assert item["old_decision"]["content"] == "Go with vendor A"
    assert item["old_decision"]["meeting_title"] == "Vendor review"
    assert item["new_decision"]["content"] == "Switch to vendor B"
    assert item["action"]["id"] == str(action)
    rid = item["id"]
    # 422: a re-scope with nothing to change.
    assert (
        client.post(
            f"/api/v1/decision-reversals/{rid}/resolve", json={"resolution": "rescope"}
        ).status_code
        == 422
    )
    res = client.post(
        f"/api/v1/decision-reversals/{rid}/resolve",
        json={
            "resolution": "rescope",
            "content": "Draft the vendor B contract",
            "due_date": "2026-12-01",
        },
    )
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "rescoped"
    assert res.json()["action"]["content"] == "Draft the vendor B contract"
    assert res.json()["action"]["due_date"] == "2026-12-01"
    # Same answer again: 200. A different one: 409.
    again = client.post(
        f"/api/v1/decision-reversals/{rid}/resolve",
        json={"resolution": "rescope", "content": "x"},
    )
    assert again.status_code == 200 and again.json()["action"]["content"].startswith("Draft")
    assert (
        client.post(
            f"/api/v1/decision-reversals/{rid}/resolve", json={"resolution": "close"}
        ).status_code
        == 409
    )
    assert client.get("/api/v1/decision-reversals").json()["items"] == []
    assert len(client.get("/api/v1/decision-reversals?status=resolved").json()["items"]) == 1
    # Someone else (the host, another tenant) sees nothing.
    sign_in(host)
    assert client.get(f"/api/v1/decision-reversals/{rid}").status_code == 404
    sign_in(seed_tenant())
    assert (
        client.post(
            f"/api/v1/decision-reversals/{rid}/resolve", json={"resolution": "keep"}
        ).status_code
        == 404
    )


def test_close_dismisses_the_task_and_keep_leaves_it(client):
    host, owner, old, new, action = _reversal_world()
    second = task(
        host,
        _sql("SELECT meeting_id FROM decisions WHERE id = %(d)s", {"d": str(old)})[0]["meeting_id"],
        owner.user_id,
        "Book the vendor A kickoff",
    )
    sign_in(host)
    client.post(f"/api/v1/decisions/{new}/confirm", json={"supersedes": str(old)})
    sign_in(owner)
    items = {
        i["action"]["id"]: i["id"] for i in client.get("/api/v1/decision-reversals").json()["items"]
    }
    closed = client.post(
        f"/api/v1/decision-reversals/{items[str(action)]}/resolve", json={"resolution": "close"}
    ).json()
    assert closed["status"] == "closed" and closed["action"]["status"] == "dismissed"
    kept = client.post(
        f"/api/v1/decision-reversals/{items[str(second)]}/resolve", json={"resolution": "keep"}
    ).json()
    assert kept["status"] == "kept" and kept["action"]["status"] == "confirmed"
    assert _sql(
        "SELECT status_changed_at IS NOT NULL AS t FROM action_items WHERE id = %(a)s",
        {"a": str(action)},
    )[0]["t"]


def run_log(org_id: uuid.UUID, m: uuid.UUID, window: str | None, payload: dict[str, Any]) -> bool:
    """The consumer's log write, on its own engine (the app's engine is bound to the
    TestClient's event loop)."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from libs.shared.tenancy import set_rls

    async def go() -> bool:
        engine = create_async_engine(APP_URL)
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                await set_rls(session, org_id)
                return await log_delegated_answer(session, org_id, m, window, payload)
        finally:
            await engine.dispose()

    return asyncio.run(go())


# --- 2. delegated answers --------------------------------------------------------------------


def _delegation_world(status: str = "scheduled") -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    sarah = add_user(host)
    set_email(sarah, "sarah.chen@example.com")
    m = seed_meeting(host.org_id)
    _sql("UPDATE meetings SET status = %(s)s WHERE id = %(m)s", {"s": status, "m": str(m)})
    add_participant(m, host.org_id, host.user_id)
    add_participant(m, host.org_id, sarah.user_id)
    _sql(
        "UPDATE meeting_participants SET attendance = 'agent' WHERE meeting_id = %(m)s "
        "AND user_id = %(u)s",
        {"m": str(m), "u": str(sarah.user_id)},
    )
    return host, sarah, m


def test_statements_are_a_list_the_delegator_edits(client):
    host, sarah, m = _delegation_world()
    sign_in(sarah)
    empty = client.get(f"/api/v1/meetings/{m}/delegated-answers").json()
    assert empty["attendance"] == "agent" and empty["editable"] and empty["statements"] == []
    first = client.put(
        f"/api/v1/meetings/{m}/delegated-answers",
        json={
            "statements": [
                "Either vendor under $40k is fine",
                "  Ship date can slip a week ",
                "either VENDOR under $40k is fine",
            ]
        },
    )
    assert first.status_code == 200, first.text
    texts = [s["text"] for s in first.json()["statements"]]
    assert texts == ["Either vendor under $40k is fine", "Ship date can slip a week"]
    keep_id = first.json()["statements"][0]["id"]
    second = client.put(
        f"/api/v1/meetings/{m}/delegated-answers",
        json={"statements": ["Hire one contractor", "Either vendor under $40k is fine"]},
    ).json()
    assert [s["text"] for s in second["statements"]] == [
        "Hire one contractor",
        "Either vendor under $40k is fine",
    ]
    assert second["statements"][1]["id"] == keep_id
    too_long = client.put(
        f"/api/v1/meetings/{m}/delegated-answers", json={"statements": ["x" * 301]}
    )
    assert too_long.status_code == 422
    assert (
        client.put(
            f"/api/v1/meetings/{m}/delegated-answers",
            json={"statements": [str(i) for i in range(11)]},
        ).status_code
        == 422
    )
    # A non-participant is 403, another tenant 404.
    sign_in(add_user(host))
    assert client.get(f"/api/v1/meetings/{m}/delegated-answers").status_code == 403
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/delegated-answers").status_code == 404


def test_statements_freeze_when_the_meeting_is_over(client):
    _, sarah, m = _delegation_world(status="ended")
    sign_in(sarah)
    assert client.get(f"/api/v1/meetings/{m}/delegated-answers").json()["editable"] is False
    assert (
        client.put(
            f"/api/v1/meetings/{m}/delegated-answers", json={"statements": ["ok"]}
        ).status_code
        == 409
    )


def test_internal_read_and_the_answer_log(client):
    host, sarah, m = _delegation_world()
    sign_in(sarah)
    stmt = client.put(
        f"/api/v1/meetings/{m}/delegated-answers",
        json={"statements": ["Either vendor under $40k is fine"]},
    ).json()["statements"][0]
    internal = client.get(f"/internal/meetings/{m}/delegated-answers", headers=rt(host.org_id))
    assert internal.status_code == 200, internal.text
    assert internal.json() == {
        "delegates": [
            {
                "user_id": str(sarah.user_id),
                "name": "Sarah Chen",
                "statements": [{"id": stmt["id"], "text": "Either vendor under $40k is fine"}],
            }
        ]
    }
    assert (
        client.get(f"/internal/meetings/{m}/delegated-answers", headers=wf(host.org_id)).status_code
        == 401
    )

    entry = str(uuid.uuid4())
    payload = {
        "delegated": {
            "id": entry,
            "delegator_user_id": str(sarah.user_id),
            "statement_id": stmt["id"],
            "statement_text": "Either vendor under $40k is fine",
            "question": "Is Sarah OK with vendor B at $35k?",
            "answer": "Yes: Sarah pre-approved either vendor under $40k.",
            "covered": True,
            "asked_by": str(host.user_id),
        }
    }
    refusal = {
        "delegated": {
            "id": str(uuid.uuid4()),
            "delegator_user_id": str(sarah.user_id),
            "statement_id": None,
            "question": "Can Sarah present on Friday?",
            "answer": "Sarah didn't pre-approve an answer to that.",
            "covered": False,
            "asked_by": None,
        }
    }

    def write(p: dict[str, Any]) -> bool:
        return run_log(host.org_id, m, "w1", p)

    assert write(payload) is True
    assert write(payload) is False  # the same answer to a second participant
    assert write(refusal) is True
    log = client.get(f"/api/v1/meetings/{m}/delegated-answers/log").json()["items"]
    assert [(e["covered"], e["statement_text"], e["asked_by_name"]) for e in log] == [
        (True, "Either vendor under $40k is fine", "U"),
        (False, None, None),
    ]
    # The host is a participant but sees only their own (empty) log.
    sign_in(host)
    assert client.get(f"/api/v1/meetings/{m}/delegated-answers/log").json()["items"] == []


def test_a_live_participant_cannot_be_logged_as_a_delegate(client):
    host, sarah, m = _delegation_world()

    entry = {
        "delegated": {
            "id": str(uuid.uuid4()),
            "delegator_user_id": str(host.user_id),
            "question": "q",
            "answer": "a",
            "covered": False,
        }
    }
    assert run_log(host.org_id, m, None, entry) is False


# --- 3. status asks ---------------------------------------------------------------------------


def test_one_click_status_requests_and_the_answer_goes_back(client):
    host = seed_tenant()
    set_email(host, "maya.patel@example.com")
    raj = add_user(host)
    set_email(raj, "raj.mehta@example.com")
    m = seed_meeting(host.org_id)
    add_participant(m, host.org_id, host.user_id)
    add_participant(m, host.org_id, raj.user_id)
    a = task(host, m, raj.user_id, "Ship the onboarding emails")
    mine = task(host, m, host.user_id, "My own thing")
    done = task(host, m, raj.user_id, "Done thing", status="done")
    sign_in(host)
    res = client.post(
        "/api/v1/status-requests",
        json={"action_ids": [str(a), str(mine), str(done), str(uuid.uuid4())]},
    )
    assert res.status_code == 200, res.text
    reasons = [(i["reason"], i["owner_name"]) for i in res.json()["items"]]
    assert reasons == [
        ("requested", "Raj Mehta"),
        ("own_action", "Maya Patel"),
        ("not_open", "Raj Mehta"),
        ("not_found", None),
    ]
    assert res.json()["requested"] == 1
    again = client.post("/api/v1/status-requests", json={"action_ids": [str(a)]}).json()
    assert again["items"][0]["reason"] == "already_pending"
    sent = notifications("statusask:")
    assert sum(1 for n in sent if n["user_id"] == str(raj.user_id)) >= 1
    owner_count = len([n for n in sent if n["user_id"] == str(raj.user_id)])

    sign_in(raj)
    pending = client.get("/api/v1/status-requests?state=pending").json()["items"]
    assert [p["action_id"] for p in pending] == [str(a)]
    answered = client.post(
        f"/api/v1/status-requests/{pending[0]['id']}/answer",
        json={"status_text": "Drafts done, sending Thursday"},
    )
    assert answered.status_code == 200, answered.text
    back = [n for n in notifications("statusanswer:") if n["user_id"] == str(host.user_id)]
    assert len(back) == 1 and back[0]["body"] == "Drafts done, sending Thursday"
    assert back[0]["title"].startswith("Raj Mehta answered")
    assert (
        len([n for n in notifications("statusask:") if n["user_id"] == str(raj.user_id)])
        == owner_count
    )


# --- 4. promise drift ------------------------------------------------------------------------


def test_promise_drift_nudges_the_owner_then_the_host(client):
    host = seed_tenant()
    raj = add_user(host)
    set_email(raj, "raj.mehta@example.com")
    m = seed_meeting(host.org_id)
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, raj.user_id)
    today = datetime.now(UTC).date()
    due = today - timedelta(days=5)
    a = task(host, m, raj.user_id, "Finish the pricing page", due=due.isoformat())
    task(host, m, raj.user_id, "Not due yet", due=(today + timedelta(days=30)).isoformat())
    as_of = today
    listed = client.get(
        f"/internal/promise-drift/due?as_of={as_of}", headers=wf(host.org_id)
    ).json()["items"]
    assert [i["action_id"] for i in listed] == [str(a)]
    body = {
        "as_of": as_of.isoformat(),
        "issues": [{"action_id": str(a), "issue_key": "WEB-12", "issue_url": "https://x/WEB-12"}],
    }
    first = client.post("/internal/promise-drift/nudges", json=body, headers=wf(host.org_id))
    assert first.json() == {"owner": 1, "host": 0}
    assert client.post(
        "/internal/promise-drift/nudges", json=body, headers=wf(host.org_id)
    ).json() == {"owner": 0, "host": 0}
    owner_note = notifications(f"promise-drift:{a}:{due}:owner")
    assert len(owner_note) == 1 and owner_note[0]["user_id"] == str(raj.user_id)
    assert owner_note[0]["title"] == (
        "Your promise to finish Finish the pricing page by "
        f"{internal_promise_drift.due_label(due)}: the Jira ticket is still open"
    )
    assert internal_promise_drift.due_label(date(2026, 11, 14)) == "Sat 14 Nov"
    # Pretend the owner nudge went out two days ago.
    _sql(
        "UPDATE promise_drift_nudges SET created_at = now() - interval '2 days' "
        "WHERE action_id = %(a)s",
        {"a": str(a)},
    )
    later = {**body, "as_of": datetime.now(UTC).date().isoformat()}
    assert client.post(
        "/internal/promise-drift/nudges", json=later, headers=wf(host.org_id)
    ).json() == {"owner": 0, "host": 1}
    host_note = notifications(f"promise-drift:{a}:{due}:host")
    assert len(host_note) == 1 and host_note[0]["user_id"] == str(host.user_id)
    assert host_note[0]["link"] == f"/meetings/{m}"
    assert client.post(
        "/internal/promise-drift/nudges", json=later, headers=wf(host.org_id)
    ).json() == {"owner": 0, "host": 0}
    # A closed task is never nudged.
    b = task(host, m, raj.user_id, "Closed one", due=due.isoformat(), status="done")
    body_b = {"as_of": as_of.isoformat(), "issues": [{"action_id": str(b), "issue_key": "WEB-13"}]}
    assert client.post(
        "/internal/promise-drift/nudges", json=body_b, headers=wf(host.org_id)
    ).json() == {"owner": 0, "host": 0}
    # Only workflow-service may call it.
    assert (
        client.post(
            "/internal/promise-drift/nudges", json=body, headers=rt(host.org_id)
        ).status_code
        == 401
    )


# --- 5. weekly digest ------------------------------------------------------------------------


class FakeNotifications:
    def __init__(self) -> None:
        self.user_emails: list[dict[str, Any]] = []

    async def send_user_email(self, org_id: uuid.UUID, **kwargs: Any) -> dict[str, Any]:
        self.user_emails.append(kwargs)
        return {
            "configured": False,
            "provider": None,
            "results": [{"recipient": "x", "status": "skipped", "detail": "not_configured"}],
        }


def test_weekly_digest_is_built_filtered_and_sent_once(client):
    host = seed_tenant()
    set_email(host, "maya.patel@example.com")
    _sql(
        "UPDATE users SET listen_for = ARRAY['pricing'] WHERE id = %(u)s", {"u": str(host.user_id)}
    )
    quiet = add_user(host)
    off = add_user(host)
    m = seed_meeting(host.org_id)
    add_participant(m, host.org_id, host.user_id)
    week = previous_week_start(datetime.now(UTC).date())
    inside = datetime.combine(week, datetime.min.time(), tzinfo=UTC) + timedelta(days=2)
    d1 = decision(host, m, "Raise pricing for the team plan")
    decision(host, m, "Move the offsite to May")
    _sql(
        "UPDATE decisions SET created_at = %(t)s WHERE meeting_id = %(m)s",
        {"t": inside, "m": str(m)},
    )
    d2 = decision(host, m, "Pricing stays flat until Q2")
    _sql(
        "UPDATE decisions SET supersedes = %(o)s, created_at = %(t)s WHERE id = %(d)s",
        {"o": str(d1), "t": inside, "d": str(d2)},
    )
    t1 = task(host, m, host.user_id, "Update the pricing page")
    _sql("UPDATE action_items SET created_at = %(t)s WHERE id = %(a)s", {"t": inside, "a": str(t1)})
    _sql("UPDATE action_items SET status = 'done' WHERE id = %(a)s", {"a": str(t1)})
    _sql(
        "UPDATE action_items SET status_changed_at = %(t)s WHERE id = %(a)s",
        {"t": inside, "a": str(t1)},
    )
    task(
        host,
        m,
        host.user_id,
        "Send the invoice",
        # Inside the digest's horizon (the seven days after the digested week) whatever weekday
        # the suite runs on: "today + 2" fell on next Monday, the horizon's open end, on weekends.
        due=(week + timedelta(days=8)).isoformat(),
    )

    sign_in(off)
    assert client.put(
        "/api/v1/digests/preferences", json={"weekly_enabled": False, "email_enabled": True}
    ).json() == {
        "weekly_enabled": False,
        "email_enabled": False,
    }
    sign_in(host)
    assert client.get("/api/v1/digests/preferences").json() == {
        "weekly_enabled": True,
        "email_enabled": False,
    }
    client.put("/api/v1/digests/preferences", json={"weekly_enabled": True, "email_enabled": True})
    fake = FakeNotifications()
    notification_client.set_notifications(fake)  # type: ignore[arg-type]

    run = client.post(
        "/internal/digests/weekly", json={"week_start": week.isoformat()}, headers=wf(host.org_id)
    )
    assert run.status_code == 200, run.text
    assert run.json()["built"] == 2 and run.json()["notified"] == 1 and run.json()["skipped"] == 1
    assert len(fake.user_emails) == 1 and fake.user_emails[0]["kind"] == "weekly_digest"
    again = client.post(
        "/internal/digests/weekly", json={"week_start": week.isoformat()}, headers=wf(host.org_id)
    ).json()
    assert again["built"] == 0 and again["notified"] == 0
    assert len(fake.user_emails) == 1

    latest = client.get("/api/v1/digests/latest").json()
    digest = latest["digest"]
    assert digest["topics"] == ["pricing"]
    assert [d["content"] for d in digest["decisions"]] == ["Raise pricing for the team plan"]
    assert digest["decisions"][0]["matched_topics"] == ["pricing"]
    assert [(c["old_content"], c["new_content"]) for c in digest["decision_changes"]] == [
        ("Raise pricing for the team plan", "Pricing stays flat until Q2")
    ]
    assert [a["content"] for a in digest["actions_closed"]] == ["Update the pricing page"]
    assert "Send the invoice" in [p["content"] for p in digest["promises_due"]]
    assert (
        _sql("SELECT email_status FROM weekly_digests WHERE id = %(i)s", {"i": digest["id"]})[0][
            "email_status"
        ]
        == "not_configured"
    )
    note = notifications(f"weekly-digest:{digest['id']}")
    assert len(note) == 1 and note[0]["link"] == "/digest"
    listed = client.get("/api/v1/digests").json()["items"]
    assert listed[0]["id"] == digest["id"]
    assert client.get(f"/api/v1/digests/{digest['id']}").json()["week_start"] == week.isoformat()
    assert client.get("/api/v1/digests/preview").status_code == 200
    # The quiet member got a stored, unannounced digest; another person cannot read mine.
    sign_in(quiet)
    q = client.get("/api/v1/digests/latest").json()["digest"]
    assert q["item_count"] == 0 and notifications(f"weekly-digest:{q['id']}") == []
    assert client.get(f"/api/v1/digests/{digest['id']}").status_code == 404
    # Only a past Monday.
    bad = client.post(
        "/internal/digests/weekly",
        json={"week_start": (week + timedelta(days=1)).isoformat()},
        headers=wf(host.org_id),
    )
    assert bad.status_code == 422


def test_week_helpers():
    assert week_start_of(date(2026, 11, 11)) == date(2026, 11, 9)
    assert previous_week_start(date(2026, 11, 9)) == date(2026, 11, 2)
    assert asyncio.iscoroutinefunction(build_digest)


# --- 6. precheck -----------------------------------------------------------------------------


def test_precheck_finds_a_decision_and_status_items(client, monkeypatch):
    monkeypatch.delenv("MYTHEUS_RAG_CALLER_TOKENS", raising=False)
    host = seed_tenant()
    raj = add_user(host)
    set_email(raj, "raj.mehta@example.com")
    m = seed_meeting(host.org_id)
    _sql("UPDATE meetings SET title = 'Vendor selection' WHERE id = %(m)s", {"m": str(m)})
    add_participant(m, host.org_id, host.user_id)
    add_participant(m, host.org_id, raj.user_id)
    d = decision(host, m, "We chose Acme as the analytics vendor for the data warehouse")
    a = task(host, m, raj.user_id, "Migrate the billing dashboards")
    sign_in(host)
    res = client.post(
        "/bff/meetings/precheck",
        json={
            "title": "Pick analytics vendor for data warehouse",
            "agenda": ["Status of billing dashboards migration", "Team lunch ideas"],
            "participant_ids": [str(raj.user_id)],
        },
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["retrieval"] == "keyword"
    assert [h["decision_id"] for h in body["already_decided"]] == [str(d)]
    assert body["status_updates"] == [
        {
            "agenda_index": 0,
            "text": "Status of billing dashboards migration",
            "action": {
                "id": str(a),
                "content": "Migrate the billing dashboards",
                "owner_user_id": str(raj.user_id),
                "owner_name": "Raj Mehta",
                "due_date": None,
            },
        }
    ]
    assert [r["meeting_id"] for r in body["recent_meetings"]] == []
    short = client.post("/bff/meetings/precheck", json={"title": "Hi", "agenda": []}).json()
    assert short["already_decided"] == [] and short["status_updates"] == []
    # Another tenant's person sees nothing of it.
    sign_in(seed_tenant())
    assert (
        client.post(
            "/bff/meetings/precheck", json={"title": "Pick analytics vendor for data warehouse"}
        ).json()["already_decided"]
        == []
    )


def test_status_heuristic():
    assert precheck_mod.is_status_like("Where are we with the launch?")
    assert precheck_mod.is_status_like("Progress on hiring")
    assert not precheck_mod.is_status_like("Choose the launch date")
    assert precheck_mod.terms("The vendors and Pricing") == {"vendor", "pricing"}


def test_semantic_precheck_uses_rag_decisions(client, monkeypatch):
    import httpx

    host = seed_tenant()
    m = seed_meeting(host.org_id)
    add_participant(m, host.org_id, host.user_id)
    d = decision(host, m, "Annual offsite is in Lisbon")
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "answer_passages": [
                    {
                        "n": 1,
                        "text": "x",
                        "rerank_score": 0.91,
                        "citation": {"kind": "decision", "decision_id": str(d)},
                    },
                    {
                        "n": 2,
                        "text": "y",
                        "rerank_score": 0.2,
                        "citation": {"kind": "decision", "decision_id": str(uuid.uuid4())},
                    },
                ]
            },
        )

    monkeypatch.setenv("MYTHEUS_RAG_CALLER_TOKENS", f"bff={'r' * 40}")
    from services.bff import ask as ask_mod

    ask_mod.set_transport(httpx.MockTransport(handler))
    try:
        sign_in(host)
        body = client.post(
            "/bff/meetings/precheck", json={"title": "Where should the offsite be"}
        ).json()
    finally:
        ask_mod.set_transport(None)
    assert body["retrieval"] == "semantic"
    assert [h["decision_id"] for h in body["already_decided"]] == [str(d)]
    assert seen[0]["source"] == "decisions" and seen[0]["generate"] is False
