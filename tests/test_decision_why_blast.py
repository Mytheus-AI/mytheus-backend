"""Beyond, Layer 1: decisions that know why, blast radius and settle threads (real Postgres).

- the `agent-results` consumer applies a `decision_why` record (assumptions, goals, actions,
  customer actions) once, an `assumption_broken` record (one open break, one owner
  notification, never from the decision's own meeting) and the existing `conflict` alert (a
  clash; a settle thread and two notifications when the owners are on different teams);
- `GET /decisions/{id}/why`: pending until read, participants only (404 otherwise, any tenant),
  evidence from a meeting the viewer was not in is redacted;
- the owner adds, edits and removes reasons (403 for other participants), re-confirms, reopens
  the decision (reversal rows of kind `reopened`, once) and books a 15-minute revisit;
- blast radius: three rings, the summary line, the meeting's one-line list;
- settle threads: only the two owners; share, message, keep both, supersede (applied at once by
  the losing owner, a proposal for the winning one, accept writes the lineage and its event);
- internal: why-source, open assumptions, the watch list and results (Jira, HubSpot rules),
  integration-service's not-connected answer; the slot finder.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import fakeredis
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api.consumer import handle_agent_result
from services.core_api.decision_why import first_free, summary_line
from services.core_api.routes import (
    blast_radius,
    candidates,
    decision_reversals,
    decision_why,
    internal_assumptions,
    internal_meetings,
    meetings,
    settlements,
)
from services.core_api.routes.internal_assumptions import Deal, Issue, deal_break, jira_break
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
from tests.test_my_tasks import add_user, task

WF = "w" * 40
RT = "r" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={WF},agent-runtime-service={RT}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-decision-why-test")
    for module in (
        candidates,
        decision_reversals,
        decision_why,
        blast_radius,
        settlements,
        internal_assumptions,
        meetings,
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


def headers(service: str, org: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": WF if service == "workflow-service" else RT,
        "X-Mytheus-Org": str(org),
    }


def named(t: Tenant, email: str) -> Tenant:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})
    return t


def decision(
    t: Tenant, m: uuid.UUID, content: str, owner: uuid.UUID, status: str = "confirmed"
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, rationale, confidence, status, "
        "confirmed_by) VALUES (%(o)s, %(m)s, %(c)s, 'because it is under 40k', 0.9, %(s)s, "
        "%(by)s) RETURNING id",
        {"o": str(t.org_id), "m": str(m), "c": content, "s": status, "by": str(owner)},
    )
    return uuid.UUID(str(row[0]["id"]))


def meeting_with(t: Tenant, *people: Tenant, title: str = "Vendor review") -> uuid.UUID:
    m = seed_meeting(t.org_id)
    _sql("UPDATE meetings SET title = %(t)s WHERE id = %(m)s", {"t": title, "m": str(m)})
    for p in people:
        add_participant(m, t.org_id, p.user_id)
    return m


def record(
    t: Tenant,
    meeting: uuid.UUID,
    user: uuid.UUID,
    payload: dict[str, Any],
    window: str,
) -> dict[str, Any]:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": f"agentresult:{meeting}:{window}:{user}:ProactiveAgent",
            "org_id": str(t.org_id),
            "schema_version": 1,
            "producer": "agent-runtime-service",
            "occurred_at": 0,
            "correlation_id": None,
            "causation_id": None,
        },
        "meeting_id": str(meeting),
        "window_id": window,
        "user_id": str(user),
        "agent": "ProactiveAgent",
        "kind": "alert",
        "relevance_score": 0.8,
        "confidence": 0.8,
        "content": "x",
        "payload_json": json.dumps(payload),
        "source_chunk_ids": [],
    }


def alert(alert_type: str, subject: uuid.UUID, **extra: Any) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "priority": 3,
        "surfaced": False,
        "title": "t",
        "body": "",
        "confidence": 0.8,
        "rationale": "",
        "subject": {"kind": "decision", "id": str(subject)},
        "related": [],
        "link": "",
        **extra,
    }


def apply(value: dict[str, Any], redis: Any | None = None) -> bool:
    async def run() -> bool:
        engine = create_async_engine(APP_URL)
        try:
            async with engine.connect() as conn, conn.begin():
                return await handle_agent_result(
                    conn, redis or fakeredis.aioredis.FakeRedis(), value  # type: ignore[arg-type]
                )
        finally:
            await engine.dispose()

    return asyncio.run(run())


def why_record(
    t: Tenant, m: uuid.UUID, d: uuid.UUID, owner: uuid.UUID, **ripple: Any
) -> dict[str, Any]:
    return record(
        t,
        m,
        owner,
        {
            "alert": alert("decision_why", d),
            "why": {
                "decision_id": str(d),
                "assumptions": [
                    {
                        "kind": "price",
                        "statement": "The vendor stays under 40k a year",
                        "quote": "because it's under 40k",
                        "speaker_user_id": str(owner),
                        "confidence": 0.9,
                        "watch": {"value": 40000, "comparator": "lt", "unit": "USD"},
                    },
                    {
                        "kind": "dependency",
                        "statement": "They ship SSO by March",
                        "quote": "they ship SSO by March",
                        "speaker_user_id": None,
                        "confidence": 0.85,
                        "watch": {"date": "2026-03-31", "issue_key": "sso-142"},
                    },
                    {"kind": "other", "statement": "Too weak", "confidence": 0.2},
                    {"kind": "nonsense", "statement": "Bad kind", "confidence": 0.9},
                ],
                "ripple": ripple or {"goals": [], "actions": []},
            },
        },
        f"why:{d}",
    )


def notifications(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


def world() -> tuple[Tenant, Tenant, uuid.UUID, uuid.UUID]:
    owner = named(seed_tenant(), "maya.patel@acme.com")
    raj = named(add_user(owner), "raj.mehta@acme.com")
    m = meeting_with(owner, owner, raj)
    d = decision(owner, m, "Go with vendor A", owner.user_id)
    return owner, raj, m, d


# --- the consumer and the why read ------------------------------------------------------------


def test_why_is_pending_then_ready_and_applies_once(client):
    owner, raj, m, d = world()
    sign_in(raj)
    first = client.get(f"/api/v1/decisions/{d}/why")
    assert first.status_code == 200, first.text
    assert first.json()["state"] == "pending" and first.json()["can_edit"] is False
    assert apply(why_record(owner, m, d, owner.user_id)) is True
    # a replay (new idempotency key, same content) writes no second row
    again = why_record(owner, m, d, owner.user_id)
    again["header"]["idempotency_key"] += ":replay"
    apply(again)
    body = client.get(f"/api/v1/decisions/{d}/why").json()
    assert body["state"] == "ready"
    assert [a["statement"] for a in body["assumptions"]] == [
        "The vendor stays under 40k a year",
        "They ship SSO by March",
    ]
    price, sso = body["assumptions"]
    assert price["kind"] == "price" and price["quote"] == "because it's under 40k"
    assert price["stated_by"]["name"] == "Maya Patel"
    assert price["watch"]["value"] == 40000 and price["watch"]["comparator"] == "lt"
    assert sso["watch"]["jira_key"] == "SSO-142" and sso["watch"]["date"] == "2026-03-31"
    assert body["owner"]["name"] == "Maya Patel"
    assert body["sources"]["linear"] == "unknown"
    sign_in(owner)
    assert client.get(f"/api/v1/decisions/{d}/why").json()["can_edit"] is True


def test_why_is_404_for_a_non_participant_and_another_tenant(client):
    owner, _raj, _m, d = world()
    outsider = add_user(owner)
    sign_in(outsider)
    assert client.get(f"/api/v1/decisions/{d}/why").status_code == 404
    assert client.get(f"/api/v1/decisions/{d}/blast-radius").status_code == 404
    other = seed_tenant()
    sign_in(other)
    assert client.get(f"/api/v1/decisions/{d}/why").status_code == 404


def test_a_proposed_decision_has_no_why(client):
    owner = seed_tenant()
    m = meeting_with(owner, owner)
    d = decision(owner, m, "Maybe", owner.user_id, status="proposed")
    sign_in(owner)
    assert client.get(f"/api/v1/decisions/{d}/why").status_code == 404
    assert apply(why_record(owner, m, d, owner.user_id)) is False


# --- owner edits ------------------------------------------------------------------------------


def test_owner_adds_edits_and_removes_a_reason_others_cannot(client):
    owner, raj, _m, d = world()
    sign_in(raj)
    denied = client.post(
        f"/api/v1/decisions/{d}/assumptions", json={"kind": "other", "statement": "x"}
    )
    assert denied.status_code == 403
    sign_in(owner)
    bad = client.post(
        f"/api/v1/decisions/{d}/assumptions",
        json={"kind": "date", "statement": "SSO ships", "watch": {"jira_key": "not a key"}},
    )
    assert bad.status_code == 422
    made = client.post(
        f"/api/v1/decisions/{d}/assumptions",
        json={
            "kind": "person",
            "statement": "  Raj owns the migration  ",
            "stated_by_user_id": str(raj.user_id),
            "watch": {"jira_key": "mig-7"},
        },
    )
    assert made.status_code == 201, made.text
    a = made.json()
    assert a["statement"] == "Raj owns the migration" and a["source"] == "owner"
    assert a["stated_by"]["name"] == "Raj Mehta" and a["watch"]["jira_key"] == "MIG-7"
    edited = client.patch(
        f"/api/v1/assumptions/{a['id']}", json={"statement": "Raj leads it", "kind": "other"}
    )
    assert edited.status_code == 200 and edited.json()["statement"] == "Raj leads it"
    assert edited.json()["watch"]["jira_key"] == "MIG-7"  # untouched fields stay
    sign_in(raj)
    assert client.patch(f"/api/v1/assumptions/{a['id']}", json={"kind": "date"}).status_code == 403
    assert client.delete(f"/api/v1/assumptions/{a['id']}").status_code == 403
    sign_in(owner)
    assert client.delete(f"/api/v1/assumptions/{a['id']}").status_code == 204
    assert client.delete(f"/api/v1/assumptions/{a['id']}").status_code == 404
    assert client.get(f"/api/v1/decisions/{d}/why").json()["assumptions"] == []
    audit = _sql(
        "SELECT action FROM audit_logs WHERE target_id = %(d)s ORDER BY occurred_at",
        {"d": str(d)},
    )
    assert [r["action"] for r in audit] == [
        "assumption.added",
        "assumption.edited",
        "assumption.removed",
    ]


# --- breaks -----------------------------------------------------------------------------------


def _break(
    t: Tenant, evidence_meeting: uuid.UUID, speaker: uuid.UUID, assumption: str, **over: Any
) -> dict[str, Any]:
    item = {
        "assumption_id": assumption,
        "confidence": 0.9,
        "evidence_quote": "The renewal came in at 46k",
        "rationale": "The price is now above 40k.",
        "source": "meeting",
        "source_meeting_id": str(evidence_meeting),
        "source_decision_id": None,
        "speaker_user_id": str(speaker),
        **over,
    }
    return record(
        t,
        evidence_meeting,
        speaker,
        {"alert": alert("assumption_broken", evidence_meeting), "breaks": [item]},
        f"assumption-check:meeting:{evidence_meeting}:{uuid.uuid4()}",
    )


def _price_assumption(owner: Tenant, m: uuid.UUID, d: uuid.UUID) -> str:
    apply(why_record(owner, m, d, owner.user_id))
    row = _sql(
        "SELECT id FROM decision_assumptions WHERE decision_id = %(d)s AND kind = 'price'",
        {"d": str(d)},
    )
    return str(row[0]["id"])


def test_a_later_meeting_breaks_a_reason_once_and_tells_the_owner(client):
    owner, raj, m, d = world()
    a = _price_assumption(owner, m, d)
    # the decision's own meeting never breaks its reasons
    assert apply(_break(owner, m, raj.user_id, a)) is False
    later = meeting_with(owner, raj, title="Finance sync")  # the owner was NOT in it
    assert apply(_break(owner, later, raj.user_id, a, confidence=0.6)) is False  # below 0.7
    assert apply(_break(owner, later, raj.user_id, a)) is True
    assert apply(_break(owner, later, raj.user_id, a)) is False  # one open break at a time
    sent = notifications("assumption-break:")
    mine = [n for n in sent if n["user_id"] == str(owner.user_id) and a in n["link"]]
    assert len(mine) == 1
    assert mine[0]["title"] == 'A reason for your decision "Go with vendor A" is no longer true'
    assert mine[0]["link"] == f"/meetings/{m}/decisions/{d}?assumption={a}#why"
    assert "46k" not in mine[0]["body"]  # the owner was not in the finance sync
    sign_in(owner)
    item = next(x for x in client.get(f"/api/v1/decisions/{d}/why").json()["assumptions"])
    assert item["id"] == a and item["status"] == "broken"
    assert item["open_break"]["redacted"] is True and item["open_break"]["quote"] is None
    assert item["open_break"]["source_meeting"]["title"] is None
    sign_in(raj)  # Raj was in both meetings: he sees the words
    seen = client.get(f"/api/v1/decisions/{d}/why").json()["assumptions"][0]["open_break"]
    assert seen["quote"] == "The renewal came in at 46k" and seen["speaker"]["name"] == "Raj Mehta"
    assert seen["source_meeting"]["title"] == "Finance sync"


def test_reconfirm_closes_the_break_and_watching_resumes(client):
    owner, raj, m, d = world()
    a = _price_assumption(owner, m, d)
    later = meeting_with(owner, owner, raj)
    apply(_break(owner, later, raj.user_id, a))
    sign_in(owner)
    assert client.post(f"/api/v1/assumptions/{a}/reconfirm", json={"note": " "}).status_code == 422
    res = client.post(
        f"/api/v1/assumptions/{a}/reconfirm", json={"note": "46k includes support; still fine"}
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "watching" and body["open_break"] is None
    assert body["history"][0]["status"] == "reconfirmed"
    assert body["history"][0]["resolution_note"] == "46k includes support; still fine"
    assert body["reconfirm_note"] == "46k includes support; still fine"


def test_reopen_asks_task_owners_once_through_the_reversal_path(client):
    owner, raj, m, d = world()
    a = _price_assumption(owner, m, d)
    t = task(owner, m, raj.user_id, "Sign the vendor A contract")
    later = meeting_with(owner, owner, raj)
    apply(_break(owner, later, raj.user_id, a))
    sign_in(owner)
    res = client.post(f"/api/v1/assumptions/{a}/reopen-decision", json={"note": "Price moved"})
    assert res.status_code == 200, res.text
    assert res.json()["reopened"]["note"] == "Price moved"
    assert res.json()["reopened"]["by"]["name"] == "Maya Patel"
    again = client.post(f"/api/v1/assumptions/{a}/reopen-decision", json={})
    assert again.status_code == 200
    rows = _sql(
        "SELECT id, kind, old_decision_id, new_decision_id FROM decision_reversals "
        "WHERE action_id = %(a)s",
        {"a": str(t)},
    )
    assert len(rows) == 1 and rows[0]["kind"] == "reopened"
    assert str(rows[0]["old_decision_id"]) == str(rows[0]["new_decision_id"]) == str(d)
    sent = notifications(f"decision-reversal:{rows[0]['id']}")
    assert len(sent) == 1 and "reopened" in sent[0]["title"]
    sign_in(raj)
    listed = client.get("/api/v1/decision-reversals").json()["items"]
    assert listed[0]["kind"] == "reopened"
    assert listed[0]["old_decision"]["id"] == listed[0]["new_decision"]["id"] == str(d)
    assert (
        _sql("SELECT status FROM assumption_evidence WHERE assumption_id = %(a)s", {"a": a})[0][
            "status"
        ]
        == "reopened"
    )


def test_revisit_slot_and_booking(client):
    owner, raj, m, d = world()
    a = _price_assumption(owner, m, d)
    sign_in(owner)
    slot = client.get(f"/api/v1/assumptions/{a}/revisit-slot")
    assert slot.status_code == 200, slot.text
    body = slot.json()
    assert {p["name"] for p in body["people"]} == {"Maya Patel", "Raj Mehta"}
    start = datetime.fromisoformat(body["start"])
    assert datetime.fromisoformat(body["end"]) - start == timedelta(minutes=15)
    assert body["title"] == "Revisit: Go with vendor A"
    past = client.post(
        f"/api/v1/assumptions/{a}/book-revisit", json={"start": "2020-01-01T10:00:00Z"}
    )
    assert past.status_code == 422
    booked = client.post(f"/api/v1/assumptions/{a}/book-revisit", json={})
    assert booked.status_code == 201, booked.text
    meeting = booked.json()["meeting_id"]
    people = _sql(
        "SELECT user_id FROM meeting_participants WHERE meeting_id = %(m)s", {"m": meeting}
    )
    assert {str(r["user_id"]) for r in people} == {str(owner.user_id), str(raj.user_id)}
    agenda = _sql(
        "SELECT title, duration_minutes FROM meeting_agenda_items WHERE meeting_id = %(m)s",
        {"m": meeting},
    )
    assert agenda[0]["duration_minutes"] == 15 and "40k" in agenda[0]["title"]
    sign_in(raj)
    assert client.get(f"/api/v1/assumptions/{a}/revisit-slot").status_code == 403


def test_first_free_skips_busy_times_weekends_and_evenings():
    friday_late = datetime(2026, 11, 13, 15, 40, tzinfo=UTC)  # a Friday
    slot = first_free(friday_late, [])
    assert slot.start == datetime(2026, 11, 16, 9, 0, tzinfo=UTC)  # Monday 09:00
    monday = datetime(2026, 11, 16, 7, 0, tzinfo=UTC)
    busy = [(datetime(2026, 11, 16, 9, 0, tzinfo=UTC), datetime(2026, 11, 16, 10, 0, tzinfo=UTC))]
    assert first_free(monday, busy).start == datetime(2026, 11, 16, 10, 0, tzinfo=UTC)


# --- blast radius -----------------------------------------------------------------------------


def test_blast_radius_rings_summary_and_meeting_line(client):
    owner, raj, m, d = world()
    other_meeting = meeting_with(owner, owner, raj, title="Onboarding")
    act = task(owner, other_meeting, raj.user_id, "Migrate billing to vendor A")
    customer_meeting = meeting_with(owner, owner, title="Acme QBR")
    _sql(
        "INSERT INTO meeting_followups (org_id, meeting_id, guest_emails) "
        "VALUES (%(o)s, %(m)s, ARRAY['buyer@acme.io'])",
        {"o": str(owner.org_id), "m": str(customer_meeting)},
    )
    promise = task(owner, customer_meeting, owner.user_id, "Send Acme the security review")
    goal = uuid.uuid4()
    apply(
        why_record(
            owner,
            m,
            d,
            owner.user_id,
            goals=[
                {
                    "goal_id": str(goal),
                    "label": "Cut infrastructure cost 15%",
                    "effect": "advances",
                    "why": "Vendor A is cheaper.",
                    "confidence": 0.8,
                }
            ],
            actions=[
                {
                    "action_id": str(act),
                    "label": "x",
                    "effect": "changes",
                    "why": "Moves it.",
                    "confidence": 0.9,
                    "source": "agent",
                },
                {
                    "action_id": str(promise),
                    "label": "y",
                    "effect": "blocks",
                    "why": "Waits.",
                    "confidence": 0.7,
                    "source": "graph",
                },
                {"action_id": str(uuid.uuid4()), "effect": "changes", "confidence": 0.9},
            ],
        )
    )
    sign_in(raj)  # in the vendor review and onboarding, not the Acme QBR
    res = client.get(f"/api/v1/decisions/{d}/blast-radius")
    assert res.status_code == 200, res.text
    b = res.json()
    assert b["state"] == "ready"
    assert [r["label"] for r in b["rings"]] == [
        "Work it changes",
        "Goals and other teams",
        "Customers",
    ]
    ring1, ring2, ring3 = (r["items"] for r in b["rings"])
    assert [(i["kind"], i["label"], i["effect"]) for i in ring1] == [
        ("action", "Migrate billing to vendor A", "changes")
    ]
    assert (
        ring1[0]["owner"]["name"] == "Raj Mehta"
        and ring1[0]["href"] == f"/meetings/{other_meeting}"
    )
    assert [(i["kind"], i["label"]) for i in ring2] == [("goal", "Cut infrastructure cost 15%")]
    assert ring3[0]["kind"] == "customer_action" and ring3[0]["redacted"] is True
    assert ring3[0]["label"] is None
    assert b["counts"] == {"actions": 1, "goals": 1, "decisions": 0, "customers": 1}
    assert b["summary"] == "Changes 1 task, moves 1 goal, touches 1 customer commitment"
    line = client.get(f"/api/v1/meetings/{m}/blast-radius").json()["items"]
    assert line == [
        {
            "decision_id": str(d),
            "content": "Go with vendor A",
            "state": "ready",
            "summary": b["summary"],
            "counts": b["counts"],
        }
    ]
    assert client.get(f"/api/v1/meetings/{customer_meeting}/blast-radius").status_code == 404


def test_summary_line_copy():
    assert summary_line(0, 0, 0, 0, []) == "No ripple found"
    assert (
        summary_line(3, 0, 1, 0, ["Sales"]) == "Changes 3 tasks, clashes with 1 decision from Sales"
    )
    assert summary_line(0, 2, 2, 2, ["Sales", "Ops"]).startswith("Moves 2 goals, clashes with 2")


# --- clashes and settle threads -----------------------------------------------------------------


def _teams(owner: Tenant, *pairs: tuple[Tenant, str]) -> None:
    for person, name in pairs:
        team = _sql(
            "INSERT INTO teams (org_id, name) VALUES (%(o)s, %(n)s) RETURNING id",
            {"o": str(owner.org_id), "n": name},
        )[0]["id"]
        _sql(
            "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s, %(u)s, %(o)s)",
            {"t": str(team), "u": str(person.user_id), "o": str(owner.org_id)},
        )


def _clash_world() -> tuple[Tenant, Tenant, uuid.UUID, uuid.UUID, uuid.UUID]:
    maya = named(seed_tenant(), "maya.patel@acme.com")
    diego = named(add_user(maya), "diego.ruiz@acme.com")
    _teams(maya, (maya, "Product"), (diego, "Sales"))
    m1 = meeting_with(maya, maya, title="Pricing")
    m2 = meeting_with(maya, diego, title="Sales offsite")
    mine = decision(maya, m1, "Drop the free tier", maya.user_id)
    theirs = decision(maya, m2, "Keep the free tier for startups", diego.user_id)
    _sql(
        "UPDATE decisions SET quote = 'we keep free for startups' WHERE id = %(d)s",
        {"d": str(theirs)},
    )
    conflict = record(
        maya,
        m1,
        maya.user_id,
        {
            "alert": {
                **alert("conflict", mine),
                "confidence": 0.86,
                "surfaced": True,
                "rationale": "One drops the free tier, the other keeps it.",
                "related": [{"kind": "decision", "id": str(theirs), "label": "Keep the free"}],
            }
        },
        f"proactive:conflict:{mine}",
    )
    assert apply(conflict) is True
    sid = _sql("SELECT id FROM decision_settlements WHERE decision_a_id = %(d)s", {"d": str(mine)})
    return maya, diego, mine, theirs, uuid.UUID(str(sid[0]["id"]))


def test_a_cross_team_clash_opens_one_settle_thread_for_both_owners(client):
    maya, diego, mine, theirs, sid = _clash_world()
    sent = notifications(f"settle:{sid}:opened")
    assert {n["user_id"] for n in sent} == {str(maya.user_id), str(diego.user_id)}
    assert all(n["link"] == f"/work/settle/{sid}" for n in sent)
    # Maya was not in the sales offsite: her notification does not quote Diego's decision
    to_maya = next(n for n in sent if n["user_id"] == str(maya.user_id))
    assert "startups" not in to_maya["body"]
    sign_in(maya)
    s = client.get(f"/api/v1/settlements/{sid}").json()
    assert s["status"] == "open" and s["mine"]["decision_id"] == str(mine)
    assert s["theirs"]["visible"] is False and s["theirs"]["content"] is None
    assert s["theirs"]["team"] == "Sales" and s["theirs"]["owner"]["name"] == "Diego Ruiz"
    assert s["rationale"].startswith("Mytheus thinks")  # cannot see both sides
    br = client.get(f"/api/v1/decisions/{mine}/blast-radius").json()
    clash = br["rings"][1]["items"][0]
    assert clash["kind"] == "decision" and clash["effect"] == "contradicts"
    assert clash["redacted"] is True and clash["settlement_id"] == str(sid)
    assert clash["href"] == f"/work/settle/{sid}"
    assert br["summary"] == "Clashes with 1 decision from Sales"
    outsider = add_user(maya)
    sign_in(outsider)
    assert client.get(f"/api/v1/settlements/{sid}").status_code == 404
    assert client.get("/api/v1/settlements").json()["items"] == []


def test_share_message_and_keep_both(client):
    maya, diego, _mine, _theirs, sid = _clash_world()
    sign_in(diego)
    shared = client.post(f"/api/v1/settlements/{sid}/share").json()
    assert shared["mine"]["shared"] is True
    msg = client.post(f"/api/v1/settlements/{sid}/messages", json={"body": "Startups only?"})
    assert msg.status_code == 200 and msg.json()["messages"][0]["mine"] is True
    sign_in(maya)
    s = client.get(f"/api/v1/settlements/{sid}").json()
    assert s["theirs"]["visible"] is True
    assert s["theirs"]["quote"] == "we keep free for startups"
    assert s["rationale"] == "One drops the free tier, the other keeps it."
    assert s["messages"][0]["author"]["name"] == "Diego Ruiz" and s["messages"][0]["mine"] is False
    kept = client.post(f"/api/v1/settlements/{sid}/keep-both", json={"note": "Startups differ"})
    assert kept.status_code == 200 and kept.json()["status"] == "kept_both"
    assert (
        client.post(f"/api/v1/settlements/{sid}/keep-both", json={"note": "again"}).status_code
        == 200
    )
    late = client.post(
        f"/api/v1/settlements/{sid}/supersede", json={"winner_decision_id": str(_mine)}
    )
    assert late.status_code == 409


def test_supersede_is_a_proposal_for_the_winner_and_accept_writes_the_lineage(client):
    maya, diego, mine, theirs, sid = _clash_world()
    sign_in(maya)
    proposed = client.post(
        f"/api/v1/settlements/{sid}/supersede", json={"winner_decision_id": str(mine)}
    ).json()
    assert proposed["status"] == "open" and proposed["proposal"]["by_me"] is True
    assert client.post(f"/api/v1/settlements/{sid}/accept").status_code == 409  # not her own
    sign_in(diego)
    t = task(
        maya,
        _sql("SELECT meeting_id FROM decisions WHERE id = %(d)s", {"d": str(theirs)})[0][
            "meeting_id"
        ],
        diego.user_id,
        "Ship the free plan page",
    )
    view = client.get(f"/api/v1/settlements/{sid}").json()
    assert view["proposal"]["by_me"] is False
    done = client.post(f"/api/v1/settlements/{sid}/accept")
    assert done.status_code == 200, done.text
    assert done.json()["status"] == "superseded"
    assert done.json()["resolution"]["winner_decision_id"] == str(mine)
    row = _sql("SELECT supersedes FROM decisions WHERE id = %(d)s", {"d": str(mine)})
    assert str(row[0]["supersedes"]) == str(theirs)
    events = _sql(
        "SELECT payload FROM outbox WHERE topic = 'decisions' AND idempotency_key = %(k)s",
        {"k": f"decision:{mine}:supersedes:{theirs}"},
    )
    assert len(events) == 1 and events[0]["payload"]["supersedes"] == str(theirs)
    reversals = _sql("SELECT kind FROM decision_reversals WHERE action_id = %(a)s", {"a": str(t)})
    assert [r["kind"] for r in reversals] == ["superseded"]


def test_the_losing_owner_gives_way_at_once(client):
    maya, _diego, mine, theirs, sid = _clash_world()
    sign_in(maya)
    res = client.post(
        f"/api/v1/settlements/{sid}/supersede", json={"winner_decision_id": str(theirs)}
    )
    assert res.status_code == 200 and res.json()["status"] == "superseded"
    row = _sql("SELECT supersedes FROM decisions WHERE id = %(d)s", {"d": str(theirs)})
    assert str(row[0]["supersedes"]) == str(mine)


def test_same_team_clash_is_kept_without_a_thread_and_meet_books(client):
    maya = named(seed_tenant(), "maya.patel@acme.com")
    m = meeting_with(maya, maya)
    a = decision(maya, m, "A", maya.user_id)
    b = decision(maya, m, "B", maya.user_id)
    rec = record(
        maya,
        m,
        maya.user_id,
        {
            "alert": {
                **alert("conflict", a),
                "related": [{"kind": "decision", "id": str(b), "label": "B"}],
            }
        },
        f"proactive:conflict:{a}",
    )
    assert apply(rec) is True
    assert (
        _sql("SELECT cross_team FROM decision_clashes WHERE decision_id = %(d)s", {"d": str(a)})[0][
            "cross_team"
        ]
        is False
    )
    assert (
        _sql("SELECT 1 FROM decision_settlements WHERE decision_a_id = %(d)s", {"d": str(a)}) == []
    )
    maya2, diego, _mine, _theirs, sid = _clash_world()
    sign_in(diego)
    slot = client.get(f"/api/v1/settlements/{sid}/meeting-slot").json()
    assert {p["name"] for p in slot["people"]} == {"Maya Patel", "Diego Ruiz"}
    booked = client.post(f"/api/v1/settlements/{sid}/book", json={})
    assert booked.status_code == 201, booked.text
    again = client.post(f"/api/v1/settlements/{sid}/book", json={})
    assert again.status_code == 201 and again.json()["meeting_id"] == booked.json()["meeting_id"]
    assert client.get(f"/api/v1/settlements/{sid}").json()["status"] == "meeting_booked"


# --- internal routes --------------------------------------------------------------------------


def test_why_source_and_open_assumptions_for_the_agent_runtime(client):
    owner, raj, m, d = world()
    other = meeting_with(owner, owner)
    task(owner, other, raj.user_id, "Draft the plan")
    res = client.get(
        f"/internal/decisions/{d}/why-source",
        headers=headers("agent-runtime-service", owner.org_id),
    )
    assert res.status_code == 200, res.text
    src = res.json()
    assert src["decision"]["content"] == "Go with vendor A" and src["already_read"] is False
    assert [a["content"] for a in src["candidate_actions"]] == ["Draft the plan"]
    assert {p["name"] for p in src["participants"]} == {"Maya Patel", "Raj Mehta"}
    assert client.get(f"/internal/decisions/{d}/why-source").status_code in (401, 403)
    assert (
        client.get(
            f"/internal/decisions/{d}/why-source",
            headers=headers("agent-runtime-service", seed_tenant().org_id),
        ).status_code
        == 404
    )
    _price_assumption(owner, m, d)
    open_ = client.get(
        "/internal/assumptions/open", headers=headers("agent-runtime-service", owner.org_id)
    ).json()
    assert {a["statement"] for a in open_["items"]} >= {"The vendor stays under 40k a year"}
    excluded = client.get(
        f"/internal/assumptions/open?exclude_meeting_id={m}",
        headers=headers("agent-runtime-service", owner.org_id),
    ).json()
    assert all(a["meeting_id"] != str(m) for a in excluded["items"])
    assert (
        client.get(
            f"/internal/decisions/{d}/why-source",
            headers=headers("agent-runtime-service", owner.org_id),
        ).json()["already_read"]
        is True
    )


def test_watch_rules():
    item = {"watch_date": date(2026, 3, 31), "value": 40000.0, "comparator": "lt", "unit": "USD"}
    late = Issue(
        key="SSO-142",
        status="In Progress",
        status_category="indeterminate",
        due_date=date(2026, 4, 14),
    )
    assert jira_break(item, late, date(2026, 2, 1)) == (
        "SSO-142 is now due 14 Apr 2026, after 31 Mar 2026.",
        "jira:SSO-142:due:2026-04-14",
    )
    on_time = Issue(
        key="SSO-142",
        status="In Progress",
        status_category="indeterminate",
        due_date=date(2026, 3, 1),
    )
    assert jira_break(item, on_time, date(2026, 2, 1)) is None
    assert jira_break(item, on_time, date(2026, 4, 2)) is not None  # still open past the date
    done = Issue(key="SSO-142", status="Done", status_category="done")
    assert jira_break(item, done, date(2026, 5, 1)) is None
    linear_open = Issue(
        key="ENG-7", provider="linear", status_category="new", due_date=date(2026, 3, 1)
    )
    linear_found = jira_break(item, linear_open, date(2026, 4, 2))
    assert linear_found is not None and linear_found[1].startswith("linear:ENG-7:")
    assert deal_break(item, Deal(id="9", name="Acme", amount=39000)) is None
    broke = deal_break(item, Deal(id="9", name="Acme renewal", amount=46000))
    assert broke == ("Acme renewal is now 46,000 USD in HubSpot.", "hubspot:9:amount:46000.0")
    assert deal_break(item, Deal(id="9", closed_lost=True))[1] == "hubspot:9:lost"  # type: ignore[index]


def test_watch_list_and_results_break_on_evidence_and_record_sources(client):
    owner, raj, m, d = world()
    apply(why_record(owner, m, d, owner.user_id))
    sso = _sql(
        "SELECT id FROM decision_assumptions WHERE decision_id = %(d)s AND kind = 'dependency'",
        {"d": str(d)},
    )[0]["id"]
    _sql(
        "UPDATE decision_assumptions SET watch_deal_id = '77' "
        "WHERE decision_id = %(d)s AND kind = 'price'",
        {"d": str(d)},
    )
    h = headers("workflow-service", owner.org_id)
    watch = client.get("/internal/assumptions/watch", headers=h).json()
    keys = {(i["jira_key"], i["deal_id"]) for i in watch["items"]}
    assert ("SSO-142", None) in keys and (None, "77") in keys
    body = {
        "as_of": "2026-02-01",
        "sources": {"jira": "connected", "linear": "not_connected", "hubspot": "connected"},
        "issues": [
            {
                "key": "SSO-142",
                "summary": "SAML SSO",
                "status": "In Progress",
                "status_category": "indeterminate",
                "due_date": "2026-04-14",
                "url": "https://acme.atlassian.net/browse/SSO-142",
            }
        ],
        "deals": [{"id": "77", "name": "Acme renewal", "amount": 39000}],
        "domain_deals": [],
    }
    applied = client.post("/internal/assumptions/watch-results", json=body, headers=h).json()
    assert applied == {"checked": 2, "broken": 1, "deals_linked": 0}
    again = client.post("/internal/assumptions/watch-results", json=body, headers=h).json()
    assert again["broken"] == 0  # the SSO reason is already broken: nothing twice
    ev = _sql(
        "SELECT source, source_ref, source_url, quote, rationale FROM assumption_evidence "
        "WHERE assumption_id = %(a)s",
        {"a": str(sso)},
    )
    assert ev[0]["source"] == "jira" and ev[0]["source_ref"] == "SSO-142"
    assert ev[0]["quote"] == "SSO-142: SAML SSO (In Progress, due 14 Apr 2026)"
    sign_in(owner)
    why_body = client.get(f"/api/v1/decisions/{d}/why").json()
    assert (
        why_body["sources"]["jira"] == "connected"
        and why_body["sources"]["linear"] == "not_connected"
    )
    broken = next(a for a in why_body["assumptions"] if a["status"] == "broken")
    assert broken["open_break"]["redacted"] is False  # Jira evidence is not a meeting
    assert broken["open_break"]["source_url"] == "https://acme.atlassian.net/browse/SSO-142"


def test_a_linear_issue_breaks_the_reason_with_linear_as_the_source(client):
    owner, raj, m, d = world()
    apply(why_record(owner, m, d, owner.user_id))
    sso = _sql(
        "SELECT id FROM decision_assumptions WHERE decision_id = %(d)s AND kind = 'dependency'",
        {"d": str(d)},
    )[0]["id"]
    h = headers("workflow-service", owner.org_id)
    issue = {
        "key": "SSO-142",
        "provider": "linear",
        "summary": "SAML SSO",
        "status": "In Progress",
        "status_category": "indeterminate",
        "due_date": "2026-04-14",
        "url": "https://linear.app/acme/issue/SSO-142",
    }
    # Linear issue, but Linear reported not connected: no break.
    off = {
        "as_of": "2026-02-01",
        "sources": {"jira": "not_connected", "linear": "not_connected", "hubspot": "not_connected"},
        "issues": [issue],
        "deals": [],
        "domain_deals": [],
    }
    assert (
        client.post("/internal/assumptions/watch-results", json=off, headers=h).json()["broken"]
        == 0
    )
    on = {
        **off,
        "sources": {"jira": "not_connected", "linear": "connected", "hubspot": "not_connected"},
    }
    assert (
        client.post("/internal/assumptions/watch-results", json=on, headers=h).json()["broken"] == 1
    )
    ev = _sql(
        "SELECT source, source_ref, source_url FROM assumption_evidence "
        "WHERE assumption_id = %(a)s",
        {"a": str(sso)},
    )
    assert ev[0]["source"] == "linear" and ev[0]["source_ref"] == "SSO-142"
    assert ev[0]["source_url"] == "https://linear.app/acme/issue/SSO-142"


def test_integration_service_answers_not_connected(migrated_db):
    from services.integration_service.watch_sources import SourcesRequest, watch_sources

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    try:
        org = seed_tenant().org_id
        out = asyncio.run(
            watch_sources(
                org, SourcesRequest(jira_keys=["SSO-1"], deal_ids=["7"], domains=["acme.io"])
            )
        )
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        db_mod.get_engine.cache_clear()
        db_mod.get_sessionmaker.cache_clear()
    assert out.jira.connected is False and out.linear.connected is False
    assert out.hubspot.connected is False and out.hubspot.deals == []


# --- the schedule's activity ------------------------------------------------------------------


def test_watch_activity_wires_core_and_integration():
    import httpx

    from services.workflow_service.assumption_activities import AssumptionWatchActivities
    from services.workflow_service.workflows.assumption_watch import (
        ASSUMPTION_WATCH_CRON,
        AssumptionWatchWorkflow,
    )

    calls: list[tuple[str, str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if request.url.path == "/internal/assumptions/watch":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"jira_key": "SSO-142", "deal_id": None},
                        {"jira_key": None, "deal_id": "77"},
                    ],
                    "deal_domains": [{"decision_id": str(uuid.uuid4()), "domains": ["acme.io"]}],
                },
            )
        if request.url.path == "/internal/watch/sources":
            return httpx.Response(
                200,
                json={
                    "jira": {"connected": True, "issues": [{"key": "SSO-142"}]},
                    "linear": {"connected": False},
                    "hubspot": {"connected": False, "deals": [], "domain_deals": []},
                },
            )
        return httpx.Response(200, json={"checked": 2, "broken": 1, "deals_linked": 0})

    acts = AssumptionWatchActivities(
        core_url="http://core",
        core_secret=WF,
        integration_url="http://integ",
        integration_secret=WF,
        transport=httpx.MockTransport(handler),
    )
    result = asyncio.run(acts.watch_tenant(str(uuid.uuid4()), date(2026, 11, 11)))
    assert result == {"checked": 2, "broken": 1, "deals_linked": 0}
    assert calls[1][2] == {"jira_keys": ["SSO-142"], "deal_ids": ["77"], "domains": ["acme.io"]}
    assert calls[2][2]["sources"] == {
        "jira": "connected",
        "linear": "not_connected",
        "hubspot": "not_connected",
    }
    assert calls[2][2]["issues"] == [{"key": "SSO-142"}] and calls[2][2]["as_of"] == "2026-11-11"
    assert ASSUMPTION_WATCH_CRON == "15 */6 * * *" and AssumptionWatchWorkflow
