"""Decisions that follow you ("Beyond the meeting", Layer 2), core-api against a real Postgres.

rag-service is an in-process fake (`httpx.MockTransport`) that checks core-api's service headers
and answers what the judge would; everything after it is real: the participant scope, the owner,
the dismissal memory, the challenges and the reopen flow, the outbox and audit rows, RLS.

1. The check: the caller's own decisions only (confirmed, from meetings they attended, in their
   tenant), enriched with the owner, meeting and date; dismissed sentences gone; the text is never
   stored (the audit row keeps a hash); 503 `check_not_connected`; 429 with `Retry-After`.
2. Challenges: recorded once per (decision, sentence) from Slack, the owner told at most once a
   day, "Challenged in N places", dismissed by the author or owner only.
3. Reopen: owner notified, one pending request per person, 409 for a replaced decision; accept
   as a follow-up task (an `actions` outbox row) or a revisit meeting (participants, agenda,
   `meeting-events`); decline; withdraw; nobody else can answer; same answer twice is 200.
4. The lexical arm for rag-service: confirmed, participant-scoped, rag-service only.
5. Tenant isolation: another tenant gets 404 everywhere and the app role sees no foreign rows.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import decision_check as dc
from services.core_api.routes import (
    decision_challenges,
    internal_decision_check,
    internal_meetings,
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
from tests.test_my_tasks import add_user

RAG_SECRET = "k" * 40
INTEGRATION_SECRET = "i" * 40
RAG_CALLER_SECRET = "g" * 40


class FakeRag:
    """rag-service's `/internal/decisions/check`: answers `self.conflicts`, records each call."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.conflicts: list[dict[str, Any]] = []
        self.status = "ok"
        self.reason: str | None = None
        self.http_status = 200

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/decisions/check"
        assert request.headers["x-mytheus-service"] == "core-api"
        assert request.headers["x-mytheus-service-token"] == RAG_SECRET
        self.calls.append(
            {
                "org": request.headers["x-mytheus-org"],
                "user": request.headers["x-mytheus-user"],
                "body": json.loads(request.content),
            }
        )
        if self.http_status != 200:
            return httpx.Response(self.http_status, json={"detail": "x"})
        return httpx.Response(
            200,
            json={
                "status": self.status,
                "reason": self.reason,
                "candidates": len(self.conflicts),
                "prompt": "decision_conflict_check@v1",
                "conflicts": self.conflicts,
            },
        )


@pytest.fixture(scope="module")
def rag() -> FakeRag:
    return FakeRag()


@pytest.fixture(scope="module")
def client(migrated_db, rag):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"integration-service={INTEGRATION_SECRET},rag-service={RAG_CALLER_SECRET}"
    )
    os.environ[dc.TOKENS_ENV] = f"core-api={RAG_SECRET}"
    os.environ[dc.URL_ENV] = "http://rag.test"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    dc.set_transport(httpx.MockTransport(rag.handler))
    app = _create_test_app("core-api-decisions-follow-test")
    app.include_router(decision_challenges.router)
    app.include_router(internal_decision_check.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    dc.set_transport(None)
    for key in (internal_meetings.TOKENS_ENV, dc.TOKENS_ENV, dc.URL_ENV):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def _reset(rag):
    rag.calls.clear()
    rag.conflicts = []
    rag.status, rag.reason, rag.http_status = "ok", None, 200


# --- world -------------------------------------------------------------------------------------


class World:
    def __init__(self) -> None:
        self.owner = seed_tenant()
        self.writer = add_user(self.owner)
        self.outsider = add_user(self.owner)  # same tenant, never in the meeting
        tag = uuid.uuid4().hex[:8]
        for who, name in (
            (self.owner, "dana.owner"),
            (self.writer, "sam.writer"),
            (self.outsider, "olu.outsider"),
        ):
            _sql(
                "UPDATE users SET email = %(e)s WHERE id = %(u)s",
                {"e": f"{name}+{tag}@example.com", "u": str(who.user_id)},
            )
        self.tag = tag
        self.meeting = seed_meeting(self.owner.org_id)
        _sql(
            "UPDATE meetings SET title = 'Pricing review' WHERE id = %(m)s",
            {"m": str(self.meeting)},
        )
        for who in (self.owner, self.writer):
            add_participant(self.meeting, self.owner.org_id, who.user_id)
        self.decision = self.add_decision("Annual plans only for new customers from Q1")
        self.other_meeting = seed_meeting(self.owner.org_id)
        add_participant(self.other_meeting, self.owner.org_id, self.owner.user_id)
        self.hidden = self.add_decision("Keep the free tier", meeting=self.other_meeting)

    def email(self, who: Tenant) -> str:
        name = {
            self.owner.user_id: "dana.owner",
            self.writer.user_id: "sam.writer",
            self.outsider.user_id: "olu.outsider",
        }[who.user_id]
        return f"{name}+{self.tag}@example.com"

    def add_decision(
        self, content: str, *, meeting: uuid.UUID | None = None, status: str = "confirmed"
    ) -> uuid.UUID:
        row = _sql(
            "INSERT INTO decisions (org_id, meeting_id, content, rationale, confidence, status, "
            "confirmed_by, status_changed_at) VALUES (%(o)s, %(m)s, %(c)s, 'Churn data', 0.9, "
            "%(s)s, %(by)s, '2026-10-03T15:00:00Z') RETURNING id",
            {
                "o": str(self.owner.org_id),
                "m": str(meeting or self.meeting),
                "c": content,
                "s": status,
                "by": str(self.owner.user_id),
            },
        )
        return uuid.UUID(str(row[0]["id"]))


def conflict(decision_id: uuid.UUID, quote: str, relation: str = "contradicts") -> dict[str, Any]:
    return {
        "decision_id": str(decision_id),
        "meeting_id": None,
        "decision_content": "as the judge saw it",
        "meeting_title": None,
        "decided_at": None,
        "quote": quote,
        "relation": relation,
        "confidence": 0.91,
        "why": "The decision says annual plans only; this offers monthly.",
    }


def outbox(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


def integ(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "integration-service",
        "X-Mytheus-Service-Token": INTEGRATION_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


QUOTE = "We can offer Acme a monthly plan to close this quarter."


# --- 1. the check --------------------------------------------------------------------------------


def test_check_returns_the_callers_own_confirmed_decisions_enriched(client, rag):
    w = World()
    foreign = World()  # another tenant's decision id must never come back
    superseded = w.add_decision("Old pricing", status="superseded")
    rag.conflicts = [
        conflict(w.decision, QUOTE),
        conflict(w.hidden, "free tier"),  # a meeting the writer was not in
        conflict(superseded, "old pricing"),
        conflict(foreign.decision, "foreign"),
        {"decision_id": "not-a-uuid"},
    ]
    sign_in(w.writer)
    res = client.post(
        "/api/v1/decisions/check",
        json={
            "text": f"Draft: {QUOTE}",
            "source": "extension",
            "context": {"place_label": "Gmail"},
        },
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "ok"
    assert [c["decision_id"] for c in body["conflicts"]] == [str(w.decision)]
    c = body["conflicts"][0]
    assert c["decision_content"] == "Annual plans only for new customers from Q1"
    assert c["meeting_title"] == "Pricing review"
    assert c["decided_at"] == "2026-10-03"
    assert c["owner"] == {"id": str(w.owner.user_id), "name": "Dana Owner"}
    assert c["quote"] == QUOTE and c["relation"] == "contradicts"
    assert c["link"] == f"/meetings/{w.meeting}/decisions/{w.decision}"
    assert c["challenge_id"] is None and c["reopen_request_id"] is None
    # rag-service was asked AS the writer, in the writer's tenant, with the context.
    call = rag.calls[-1]
    assert call["user"] == str(w.writer.user_id) and call["org"] == str(w.owner.org_id)
    assert call["body"]["source"] == "extension"
    assert call["body"]["context"]["place_label"] == "Gmail"
    # Nothing recorded; the audit row has a fingerprint, never the text.
    assert (
        _sql("SELECT 1 FROM decision_challenges WHERE decision_id = %(d)s", {"d": str(w.decision)})
        == []
    )
    audit = _sql(
        "SELECT metadata FROM audit_logs WHERE org_id = %(o)s AND action = 'decision.checked'",
        {"o": str(w.owner.org_id)},
    )
    assert len(audit) == 1
    assert QUOTE not in json.dumps(audit[0]["metadata"])
    assert len(audit[0]["metadata"]["text_sha256"]) == 64


def test_check_passes_skips_through(client, rag):
    w = World()
    rag.status, rag.reason = "skipped", "never_record"
    sign_in(w.writer)
    body = client.post("/api/v1/decisions/check", json={"text": "About project falcon"}).json()
    assert body["status"] == "skipped" and body["reason"] == "never_record"
    assert body["conflicts"] == []


def test_check_validation(client):
    w = World()
    sign_in(w.writer)
    assert client.post("/api/v1/decisions/check", json={"text": "   "}).status_code == 422
    assert client.post("/api/v1/decisions/check", json={"text": "x" * 4001}).status_code == 422
    assert (
        client.post("/api/v1/decisions/check", json={"text": "ok", "source": "slack"}).status_code
        == 422
    )


def test_check_not_connected_is_a_503_with_a_reason(client, rag, monkeypatch):
    w = World()
    sign_in(w.writer)
    monkeypatch.delenv(dc.TOKENS_ENV)
    res = client.post("/api/v1/decisions/check", json={"text": QUOTE})
    assert res.status_code == 503 and res.json()["reason"] == "check_not_connected"
    monkeypatch.setenv(dc.TOKENS_ENV, f"core-api={RAG_SECRET}")
    rag.http_status = 401
    res = client.post("/api/v1/decisions/check", json={"text": QUOTE})
    assert res.status_code == 503 and res.json()["reason"] == "check_not_connected"


def test_check_is_rate_limited_per_person(client, monkeypatch):
    w = World()
    sign_in(w.writer)
    monkeypatch.setenv(dc.RATE_ENV, "2")
    codes = [client.post("/api/v1/decisions/check", json={"text": QUOTE}) for _ in range(3)]
    assert [r.status_code for r in codes[:2]] == [200, 200]
    limited = codes[2]
    assert limited.status_code == 429
    assert limited.json()["code"] == "RATE_LIMITED"
    assert 1 <= int(limited.headers["Retry-After"]) <= 60
    # Another person has their own budget.
    sign_in(w.owner)
    assert client.post("/api/v1/decisions/check", json={"text": QUOTE}).status_code == 200


def test_a_dismissed_sentence_is_not_flagged_to_that_person_again(client, rag):
    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    sign_in(w.writer)
    assert (
        len(client.post("/api/v1/decisions/check", json={"text": QUOTE}).json()["conflicts"]) == 1
    )
    res = client.post(
        f"/api/v1/decisions/{w.decision}/check-dismissals",
        json={
            "quote": "  we can offer ACME a monthly plan, to close this quarter ",
            "source": "extension",
        },
    )
    assert res.status_code == 200 and res.json() == {
        "decision_id": str(w.decision),
        "dismissed": True,
    }
    assert client.post("/api/v1/decisions/check", json={"text": QUOTE}).json()["conflicts"] == []
    stored = _sql(
        "SELECT quote_hash FROM decision_check_dismissals WHERE decision_id = %(d)s",
        {"d": str(w.decision)},
    )
    assert stored == [{"quote_hash": dc.quote_hash(QUOTE)}]  # a hash, never the words
    # The owner still sees it: dismissals are per person.
    sign_in(w.owner)
    assert (
        len(client.post("/api/v1/decisions/check", json={"text": QUOTE}).json()["conflicts"]) == 1
    )
    # A decision the caller cannot see is a 404.
    sign_in(w.outsider)
    assert (
        client.post(
            f"/api/v1/decisions/{w.decision}/check-dismissals", json={"quote": QUOTE}
        ).status_code
        == 404
    )


# --- 2. challenges (Slack) -----------------------------------------------------------------------


def slack_check(client, w: World, who: Tenant, quote: str = QUOTE, ref: str = "T1:C1:1.0"):
    return client.post(
        "/internal/decisions/check",
        headers=integ(w.owner.org_id),
        json={
            "author_email": w.email(who).upper(),
            "text": f"Heads up team: {quote}",
            "place_label": "#sales",
            "link": "https://acme.slack.com/archives/C1/p1",
            "external_ref": ref,
        },
    )


def test_a_slack_message_is_recorded_once_and_the_owner_told_once(client, rag):
    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    first = slack_check(client, w, w.writer)
    assert first.status_code == 200, first.text
    c = first.json()["conflicts"][0]
    assert c["challenge_id"]
    assert rag.calls[-1]["user"] == str(w.writer.user_id)  # checked AS the author
    rows = _sql(
        "SELECT * FROM decision_challenges WHERE decision_id = %(d)s", {"d": str(w.decision)}
    )
    assert len(rows) == 1
    assert rows[0]["source"] == "slack" and rows[0]["place_label"] == "#sales"
    assert str(rows[0]["author_user_id"]) == str(w.writer.user_id)
    sent = outbox(f"decision-challenge:{c['challenge_id']}")
    assert len(sent) == 1 and sent[0]["user_id"] == str(w.owner.user_id)
    assert sent[0]["title"] == "Your decision was challenged"
    assert QUOTE in sent[0]["body"] and "#sales" in sent[0]["body"]
    assert sent[0]["link"].endswith("#challenges")
    # Redelivery: same challenge, no second row, no second notification.
    again = slack_check(client, w, w.writer)
    assert again.json()["conflicts"][0]["challenge_id"] == c["challenge_id"]
    assert len(outbox(f"decision-challenge:{c['challenge_id']}")) == 1
    # A second sentence the same day is recorded but the owner is not pinged twice.
    rag.conflicts = [conflict(w.decision, "Monthly is fine for Acme.", "reopens")]
    second = slack_check(client, w, w.writer, "Monthly is fine for Acme.")
    sid = second.json()["conflicts"][0]["challenge_id"]
    assert sid != c["challenge_id"]
    assert outbox(f"decision-challenge:{sid}") == []


def test_unknown_slack_author_is_not_checked(client, rag):
    w = World()
    res = client.post(
        "/internal/decisions/check",
        headers=integ(w.owner.org_id),
        json={"author_email": "nobody@example.com", "text": QUOTE},
    )
    assert res.status_code == 404 and res.json()["reason"] == "unknown_author"
    assert rag.calls == []


def test_internal_routes_need_the_integration_caller(client):
    w = World()
    bad = {**integ(w.owner.org_id), "X-Mytheus-Service-Token": "nope" * 10}
    assert (
        client.post(
            "/internal/decisions/check",
            headers=bad,
            json={"author_email": w.email(w.writer), "text": QUOTE},
        ).status_code
        == 401
    )


def test_challenged_in_n_places_and_who_may_dismiss(client, rag):
    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    cid = slack_check(client, w, w.writer).json()["conflicts"][0]["challenge_id"]
    # A reopen from a Google Doc is a second place.
    sign_in(w.writer)
    client.post(
        f"/api/v1/decisions/{w.decision}/reopen-requests",
        json={
            "quote": "Offer monthly billing to all SMB customers.",
            "source": "extension",
            "place_label": "Google Docs: SMB plan",
            "link": "https://docs.google.com/document/d/abc",
        },
    )
    sign_in(w.owner)
    body = client.get(f"/api/v1/decisions/{w.decision}/challenges").json()
    assert body["viewer_is_owner"] is True
    assert body["owner"]["name"] == "Dana Owner"
    assert body["places"] == 2 and body["open_count"] == 1
    by_source = {i["source"]: i for i in body["items"]}
    assert by_source["slack"]["viewer_can_dismiss"] is True  # the owner may
    assert by_source["extension"]["status"] == "reopened"
    assert by_source["slack"]["author"]["name"] == "Sam Writer"
    assert len(body["reopen_requests"]) == 1
    # The owner's Inbox read.
    mine = client.get("/api/v1/decision-challenges").json()["items"]
    assert [i["id"] for i in mine] == [cid]
    # Someone who is neither author nor owner cannot dismiss (and cannot even see it).
    sign_in(w.outsider)
    assert client.post(f"/api/v1/decision-challenges/{cid}/dismiss").status_code == 404
    assert client.get(f"/api/v1/decisions/{w.decision}/challenges").status_code == 404
    # The author dismisses: the sentence is remembered for them.
    sign_in(w.writer)
    viewer = client.get(f"/api/v1/decisions/{w.decision}/challenges").json()
    assert viewer["viewer_is_owner"] is False
    res = client.post(f"/api/v1/decision-challenges/{cid}/dismiss")
    assert res.status_code == 200 and res.json()["status"] == "dismissed"
    assert (
        client.post(f"/api/v1/decision-challenges/{cid}/dismiss").status_code == 200
    )  # again: no-op
    assert client.post("/api/v1/decisions/check", json={"text": QUOTE}).json()["conflicts"] == []
    sign_in(w.owner)
    after = client.get(f"/api/v1/decisions/{w.decision}/challenges").json()
    assert after["places"] == 1 and after["items"][-1]["status"] == "dismissed"


def test_slack_buttons_act_only_for_the_author(client, rag):
    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    cid = slack_check(client, w, w.writer).json()["conflicts"][0]["challenge_id"]
    other = client.post(
        f"/internal/decision-challenges/{cid}/reopen",
        headers=integ(w.owner.org_id),
        json={"acting_email": w.email(w.owner)},
    )
    assert other.status_code == 404
    res = client.post(
        f"/internal/decision-challenges/{cid}/reopen",
        headers=integ(w.owner.org_id),
        json={"acting_email": w.email(w.writer)},
    )
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "pending" and res.json()["source"] == "slack"
    assert res.json()["quote"] == QUOTE
    status = _sql("SELECT status FROM decision_challenges WHERE id = %(c)s", {"c": cid})
    assert status == [{"status": "reopened"}]
    assert len(outbox(f"decision-reopen:{res.json()['id']}")) == 1


# --- 3. reopen ---------------------------------------------------------------------------------


def test_reopen_request_notifies_the_owner_once_and_is_one_per_person(client):
    w = World()
    sign_in(w.writer)
    res = client.post(
        f"/api/v1/decisions/{w.decision}/reopen-requests",
        json={"reason": "Acme will churn without monthly.", "source": "app"},
    )
    assert res.status_code == 201, res.text
    r = res.json()
    assert r["status"] == "pending" and r["viewer_role"] == "requester"
    assert r["owner"]["name"] == "Dana Owner" and r["requested_by"]["name"] == "Sam Writer"
    sent = outbox(f"decision-reopen:{r['id']}")
    assert len(sent) == 1 and sent[0]["user_id"] == str(w.owner.user_id)
    assert sent[0]["title"] == "Sam Writer asked to reopen your decision"
    assert f"?reopen={r['id']}" in sent[0]["link"]
    again = client.post(f"/api/v1/decisions/{w.decision}/reopen-requests", json={})
    assert again.status_code == 200 and again.json()["id"] == r["id"]
    # The owner's list; a viewer does not see the private reason.
    sign_in(w.owner)
    items = client.get("/api/v1/decision-reopen-requests").json()["items"]
    assert [i["id"] for i in items] == [r["id"]]
    assert items[0]["viewer_role"] == "owner" and items[0]["reason"]
    # Not a participant: 404. A replaced decision: 409.
    sign_in(w.outsider)
    assert (
        client.post(f"/api/v1/decisions/{w.decision}/reopen-requests", json={}).status_code == 404
    )
    assert client.get(f"/api/v1/decision-reopen-requests/{r['id']}").status_code == 404
    old = w.add_decision("Old", status="superseded")
    sign_in(w.writer)
    conflict_res = client.post(f"/api/v1/decisions/{old}/reopen-requests", json={})
    assert conflict_res.status_code == 409 and conflict_res.json()["reason"] == "not_confirmed"
    # A link must be http(s).
    assert (
        client.post(
            f"/api/v1/decisions/{w.decision}/reopen-requests", json={"link": "javascript:x"}
        ).status_code
        == 422
    )


def test_accept_as_a_follow_up_task(client):
    w = World()
    sign_in(w.writer)
    rid = client.post(f"/api/v1/decisions/{w.decision}/reopen-requests", json={}).json()["id"]
    # Only the owner may answer.
    assert (
        client.post(
            f"/api/v1/decision-reopen-requests/{rid}/accept", json={"outcome": "follow_up"}
        ).status_code
        == 404
    )
    sign_in(w.owner)
    res = client.post(
        f"/api/v1/decision-reopen-requests/{rid}/accept",
        json={"outcome": "follow_up", "due_date": "2026-12-01", "note": "Fair point."},
    )
    assert res.status_code == 200, res.text
    r = res.json()
    assert r["status"] == "accepted" and r["outcome"] == "follow_up"
    task = _sql("SELECT * FROM action_items WHERE id = %(a)s", {"a": r["follow_up_action_id"]})[0]
    assert task["status"] == "confirmed" and str(task["assignee_id"]) == str(w.owner.user_id)
    assert task["content"].startswith("Revisit the decision: Annual plans only")
    assert str(task["due_date"]) == "2026-12-01"
    assert _sql(
        "SELECT 1 FROM outbox WHERE topic = 'actions' AND payload->>'action_id' = %(a)s",
        {"a": r["follow_up_action_id"]},
    )
    told = outbox(f"decision-reopen-resolved:{rid}")
    assert len(told) == 1 and told[0]["user_id"] == str(w.writer.user_id)
    assert "Fair point." in told[0]["body"]
    # Same answer again: 200, nothing new. A different one: 409.
    same = client.post(
        f"/api/v1/decision-reopen-requests/{rid}/accept", json={"outcome": "follow_up"}
    )
    assert (
        same.status_code == 200 and same.json()["follow_up_action_id"] == r["follow_up_action_id"]
    )
    assert (
        client.post(f"/api/v1/decision-reopen-requests/{rid}/decline", json={}).status_code == 409
    )


def test_accept_as_a_revisit_meeting(client):
    w = World()
    sign_in(w.writer)
    rid = client.post(
        f"/api/v1/decisions/{w.decision}/reopen-requests", json={"quote": QUOTE}
    ).json()["id"]
    sign_in(w.owner)
    res = client.post(
        f"/api/v1/decision-reopen-requests/{rid}/accept",
        json={
            "outcome": "revisit",
            "scheduled_start": "2026-11-20T15:00:00Z",
            "duration_minutes": 45,
        },
    )
    assert res.status_code == 200, res.text
    mid = res.json()["revisit_meeting_id"]
    meeting = _sql("SELECT * FROM meetings WHERE id = %(m)s", {"m": mid})[0]
    assert meeting["title"].startswith("Revisit: Annual plans only")
    assert str(meeting["previous_meeting_id"]) == str(w.meeting)
    assert (meeting["scheduled_end"] - meeting["scheduled_start"]).total_seconds() == 45 * 60
    people = {
        (str(r["user_id"]), r["role_in_meeting"])
        for r in _sql("SELECT * FROM meeting_participants WHERE meeting_id = %(m)s", {"m": mid})
    }
    assert people == {(str(w.owner.user_id), "host"), (str(w.writer.user_id), "attendee")}
    agenda = [
        r["title"]
        for r in _sql(
            "SELECT title FROM meeting_agenda_items WHERE meeting_id = %(m)s ORDER BY position",
            {"m": mid},
        )
    ]
    assert agenda[0].startswith("Revisit: Annual plans only") and QUOTE in agenda[1]
    assert _sql(
        "SELECT 1 FROM outbox WHERE topic = 'meeting-events' AND payload->>'meeting_id' = %(m)s",
        {"m": mid},
    )


def test_decline_and_withdraw(client):
    w = World()
    sign_in(w.writer)
    rid = client.post(
        f"/api/v1/decisions/{w.decision}/reopen-requests", json={"quote": QUOTE}
    ).json()["id"]
    # The requester can withdraw (the Undo of "Reopen it"); the challenge stays, as open.
    res = client.post(f"/api/v1/decision-reopen-requests/{rid}/withdraw")
    assert res.status_code == 200 and res.json()["status"] == "withdrawn"
    assert _sql(
        "SELECT status FROM decision_challenges WHERE decision_id = %(d)s", {"d": str(w.decision)}
    ) == [{"status": "open"}]
    sign_in(w.owner)
    assert (
        client.post(f"/api/v1/decision-reopen-requests/{rid}/decline", json={}).status_code == 409
    )
    sign_in(w.writer)
    rid2 = client.post(f"/api/v1/decisions/{w.decision}/reopen-requests", json={}).json()["id"]
    assert rid2 != rid
    sign_in(w.owner)
    res = client.post(f"/api/v1/decision-reopen-requests/{rid2}/decline", json={"note": "Not now."})
    assert res.status_code == 200 and res.json()["status"] == "declined"
    told = outbox(f"decision-reopen-resolved:{rid2}")
    assert told[0]["title"] == "Your reopen request was declined"
    sign_in(w.writer)
    assert client.post(f"/api/v1/decision-reopen-requests/{rid2}/withdraw").status_code == 409
    actions = {
        r["action"]
        for r in _sql(
            "SELECT action FROM audit_logs WHERE org_id = %(o)s", {"o": str(w.owner.org_id)}
        )
    }
    assert {
        "decision_reopen.requested",
        "decision_reopen.withdrawn",
        "decision_reopen.declined",
    } <= actions


# --- 4. the lexical arm --------------------------------------------------------------------------


def rag_caller(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "rag-service",
        "X-Mytheus-Service-Token": RAG_CALLER_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def test_lexical_arm_is_confirmed_and_participant_scoped(client):
    w = World()
    w.add_decision("Monthly pricing is withdrawn", status="superseded")
    body = {"user_id": str(w.writer.user_id), "query": "annual plans pricing; DROP TABLE x"}
    res = client.post(
        "/internal/retrieval/decisions/search", headers=rag_caller(w.owner.org_id), json=body
    )
    assert res.status_code == 200, res.text
    items = res.json()["items"]
    assert [i["id"] for i in items] == [str(w.decision)]
    assert items[0]["meeting_title"] == "Pricing review" and items[0]["rank"] > 0
    # The owner attended the other meeting too: "free tier" finds the hidden decision for them only.
    free = {"user_id": str(w.owner.user_id), "query": "free tier"}
    got = client.post(
        "/internal/retrieval/decisions/search", headers=rag_caller(w.owner.org_id), json=free
    )
    assert [i["id"] for i in got.json()["items"]] == [str(w.hidden)]
    writer_free = {"user_id": str(w.writer.user_id), "query": "free tier"}
    none = client.post(
        "/internal/retrieval/decisions/search", headers=rag_caller(w.owner.org_id), json=writer_free
    )
    assert none.json()["items"] == []
    # Only rag-service may ask.
    assert (
        client.post(
            "/internal/retrieval/decisions/search", headers=integ(w.owner.org_id), json=body
        ).status_code
        == 401
    )
    assert internal_decision_check.or_query("a, of; to") is None
    assert internal_decision_check.or_query("Annual-plans & pricing!") == "annual | plans | pricing"


# --- 5. tenant isolation -------------------------------------------------------------------------


def test_another_tenant_sees_nothing(client, rag):
    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    cid = slack_check(client, w, w.writer).json()["conflicts"][0]["challenge_id"]
    sign_in(w.writer)
    rid = client.post(f"/api/v1/decisions/{w.decision}/reopen-requests", json={}).json()["id"]
    stranger = seed_tenant()
    sign_in(stranger)
    assert client.get(f"/api/v1/decisions/{w.decision}/challenges").status_code == 404
    assert client.post(f"/api/v1/decision-challenges/{cid}/dismiss").status_code == 404
    assert client.get(f"/api/v1/decision-reopen-requests/{rid}").status_code == 404
    assert (
        client.post(
            f"/api/v1/decision-reopen-requests/{rid}/accept", json={"outcome": "revisit"}
        ).status_code
        == 404
    )
    assert client.get("/api/v1/decision-challenges?status=all").json()["items"] == []
    assert client.get("/api/v1/decision-reopen-requests?role=all&status=all").json()["items"] == []
    # The stranger's internal call with their own org cannot reach the other tenant's challenge.
    res = client.post(
        f"/internal/decision-challenges/{cid}/dismiss",
        headers=integ(stranger.org_id),
        json={"acting_email": w.email(w.writer)},
    )
    assert res.status_code == 404


def test_rls_hides_rows_from_the_app_role_under_another_org(client, rag):
    import psycopg2

    w = World()
    rag.conflicts = [conflict(w.decision, QUOTE)]
    slack_check(client, w, w.writer)
    other = seed_tenant()
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org', %s, true)", (str(other.org_id),))
            for table in (
                "decision_challenges",
                "decision_reopen_requests",
                "decision_check_dismissals",
            ):
                cur.execute(
                    f"SELECT count(*) FROM {table} WHERE decision_id = %s", (str(w.decision),)
                )
                assert cur.fetchone()[0] == 0, table
        with conn, conn.cursor() as cur:  # no tenant at all: fail closed
            cur.execute("SELECT count(*) FROM decision_challenges")
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()


# --- pure helpers --------------------------------------------------------------------------------


def test_normalised_quote_hash():
    assert dc.quote_hash("We can offer ACME a monthly plan!") == dc.quote_hash(
        "  we can offer acme a monthly   plan "
    )
    assert dc.quote_hash("monthly plan") != dc.quote_hash("annual plan")


def test_next_weekday_morning():
    from datetime import UTC, datetime

    friday = datetime(2026, 11, 13, 17, 0, tzinfo=UTC)
    assert decision_challenges.next_weekday_morning(friday) == datetime(
        2026, 11, 16, 10, 0, tzinfo=UTC
    )
