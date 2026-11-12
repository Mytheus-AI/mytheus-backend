"""Decisions as bets ("Beyond the meeting", Layer 3), against a real Postgres.

- the owner, host or confirmer adds and edits a bet; another participant may read it but not
  change it (403); someone outside the meeting or another tenant gets a 404;
- DecisionAgent's after-meeting proposals become `proposed` bets owned by the confirmer, once,
  with a private note; accept, dismiss and restore (the Undo);
- the daily check: due bets, a connected metric suggests (never resolves), "not connected"
  evidence without one, later decisions and meetings found as evidence and withheld from
  people who were not in them, the owner asked once;
- resolve, reopen, re-resolve with the lesson;
- the company page: aggregates only, a team only with 3+ resolved bets, failing assumptions
  grouped, confidential meetings counted but never quoted; hints for a similar decision;
- the status history trigger; the workflow activity and the CRM metric reader.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import date, timedelta
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import bets
from services.core_api.routes import decision_bets, internal_bets, internal_meetings
from services.workflow_service.bet_activities import BetActivities
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
from tests.test_worklogs import apply, result

WF = "w" * 40
NIL = uuid.UUID(int=0)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={WF}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-bets-test")
    app.include_router(decision_bets.router)
    app.include_router(internal_bets.router)
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


def wf(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": WF,
        "X-Mytheus-Org": str(org_id),
    }


def set_email(t: Tenant, email: str) -> None:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})


def host_meeting(
    host: Tenant, *others: Tenant, title: str = "Billing review", confidential: bool = False
) -> uuid.UUID:
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = %(t)s, confidential = %(c)s, status = 'ended' "
        "WHERE id = %(m)s",
        {"t": title, "c": confidential, "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    for o in others:
        add_participant(m, host.org_id, o.user_id)
    return m


def decision(
    t: Tenant,
    m: uuid.UUID,
    content: str,
    *,
    status: str = "confirmed",
    by: uuid.UUID | None = None,
    supersedes: uuid.UUID | None = None,
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status, confirmed_by, "
        "supersedes) VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(b)s, %(sup)s) RETURNING id",
        {
            "o": str(t.org_id),
            "m": str(m),
            "c": content,
            "s": status,
            "b": str(by) if by else None,
            "sup": str(supersedes) if supersedes else None,
        },
    )
    return uuid.UUID(str(row[0]["id"]))


def bet_body(**over: Any) -> dict[str, Any]:
    body = {
        "statement": "Annual billing cuts monthly churn by 3 points by March",
        "kind": "metric",
        "metric": "Monthly churn",
        "direction": "decrease",
        "magnitude": 3,
        "unit": "points",
        "baseline": 7.5,
        "check_date": (date.today() + timedelta(days=90)).isoformat(),
        "source": "meetings",
        "category": "pricing",
        "confidence_pct": 70,
        "assumption": "Customers on monthly plans accept a discount for annual",
    }
    body.update(over)
    return body


def world() -> tuple[Tenant, Tenant, Tenant, uuid.UUID, uuid.UUID]:
    host = seed_tenant()
    set_email(host, "maya.patel@example.com")
    member = add_user(host)
    set_email(member, "raj.mehta@example.com")
    outsider = add_user(host)
    m = host_meeting(host, member)
    d = decision(host, m, "Move every plan to annual billing", by=host.user_id)
    return host, member, outsider, m, d


def notes(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


# --- shapes and maths ---------------------------------------------------------------------------


def test_suggest_outcome_reads_the_direction_and_size():
    s = bets.suggest_outcome
    assert (
        s(direction="decrease", magnitude=3, unit="points", baseline=7.5, observed=4.0)[0] == "hit"
    )
    assert (
        s(direction="decrease", magnitude=3, unit="points", baseline=7.5, observed=6.0)[0]
        == "partly"
    )
    assert (
        s(direction="decrease", magnitude=3, unit="points", baseline=7.5, observed=8.0)[0]
        == "missed"
    )
    assert s(direction="increase", magnitude=None, unit=None, baseline=10, observed=12)[0] == "hit"
    assert (
        s(direction="reach", magnitude=100, unit="deals", baseline=None, observed=85)[0] == "partly"
    )
    assert s(direction="hold", magnitude=None, unit="%", baseline=50, observed=51)[0] == "hit"
    outcome, why = s(direction="increase", magnitude=5, unit="%", baseline=None, observed=3)
    assert outcome is None and "no baseline" in why


def test_bet_body_validation():
    with pytest.raises(ValueError):
        bets.BetUpsert(**bet_body(kind="event"))  # an event cannot "decrease"
    with pytest.raises(ValueError):
        bets.BetUpsert(**bet_body(direction="reach", magnitude=None))
    with pytest.raises(ValueError):
        bets.BetUpsert(**bet_body(metric_key="hubspot.deals_won"))  # needs source crm
    ok = bets.BetUpsert(**bet_body(kind="event", direction="happen"))
    assert ok.magnitude is None and ok.unit is None


def test_keywords_and_similarity():
    assert bets.keywords("Annual billing will cut churn by 3 points") == [
        "annual",
        "billing",
        "churn",
    ]
    assert bets.tsquery(["churn", "billing"]) == "churn | billing"
    assert bets.similar("Customers accept annual billing", "customers will accept annual billing")


# --- the person-facing API ----------------------------------------------------------------------


def test_host_adds_a_bet_and_others_read_it(client):
    host, member, outsider, _m, d = world()
    sign_in(host)
    empty = client.get(f"/api/v1/decisions/{d}/bet").json()
    assert empty["bet"] is None and empty["can_edit"] is True
    assert {o["name"] for o in empty["owner_candidates"]} == {"Maya Patel", "Raj Mehta"}

    saved = client.put(f"/api/v1/decisions/{d}/bet", json=bet_body())
    assert saved.status_code == 200, saved.text
    bet = saved.json()["bet"]
    assert bet["status"] == "open" and bet["proposed_by"] == "person"
    assert bet["owner_name"] == "Maya Patel" and bet["magnitude"] == 3.0

    sign_in(member)
    view = client.get(f"/api/v1/decisions/{d}/bet").json()
    assert view["bet"]["statement"].startswith("Annual billing") and view["can_edit"] is False
    assert client.put(f"/api/v1/decisions/{d}/bet", json=bet_body()).status_code == 403
    assert (
        client.post(f"/api/v1/decisions/{d}/bet/resolve", json={"outcome": "hit"}).status_code
        == 403
    )

    sign_in(outsider)
    assert client.get(f"/api/v1/decisions/{d}/bet").status_code == 404
    other = seed_tenant()
    sign_in(other)
    assert client.get(f"/api/v1/decisions/{d}/bet").status_code == 404


def test_owner_can_hand_the_bet_to_a_participant_only(client):
    host, member, outsider, _m, d = world()
    sign_in(host)
    bad = client.put(
        f"/api/v1/decisions/{d}/bet", json=bet_body(owner_user_id=str(outsider.user_id))
    )
    assert bad.status_code == 422
    ok = client.put(f"/api/v1/decisions/{d}/bet", json=bet_body(owner_user_id=str(member.user_id)))
    assert ok.json()["bet"]["owner_name"] == "Raj Mehta"
    sign_in(member)
    assert client.get(f"/api/v1/decisions/{d}/bet").json()["can_edit"] is True
    mine = client.get("/api/v1/bets/mine").json()["items"]
    assert [i["bet"]["decision_id"] for i in mine] == [str(d)]
    assert mine[0]["needs_you"] is False


def test_resolve_reopen_and_resolve_again(client):
    host, _member, _o, _m, d = world()
    sign_in(host)
    client.put(f"/api/v1/decisions/{d}/bet", json=bet_body())
    res = client.post(
        f"/api/v1/decisions/{d}/bet/resolve",
        json={"outcome": "partly", "assumption_held": False, "lesson": "Annual takes two quarters"},
    )
    assert res.status_code == 200, res.text
    bet = res.json()["bet"]
    assert bet["status"] == "resolved" and bet["outcome"] == "partly"
    assert bet["resolved_by_name"] == "Maya Patel"
    assert client.put(f"/api/v1/decisions/{d}/bet", json=bet_body()).status_code == 409
    reopened = client.post(f"/api/v1/decisions/{d}/bet/reopen").json()["bet"]
    assert reopened["status"] == "open" and reopened["outcome"] is None
    again = client.post(f"/api/v1/decisions/{d}/bet/resolve", json={"outcome": "hit"}).json()["bet"]
    assert again["outcome"] == "hit"
    audits = _sql(
        "SELECT action FROM audit_logs WHERE target_type = 'decision_bet' AND org_id = %(o)s",
        {"o": str(host.org_id)},
    )
    assert {a["action"] for a in audits} >= {"decision_bet.saved", "decision_bet.resolved"}


# --- DecisionAgent's proposals ------------------------------------------------------------------


def proposal(host: Tenant, m: uuid.UUID, d: uuid.UUID, **over: Any) -> dict[str, Any]:
    exp = {
        "decision_id": str(d),
        "statement": "Annual billing should cut churn by 3 points by March",
        "kind": "metric",
        "metric": "Monthly churn",
        "direction": "decrease",
        "magnitude": 3,
        "unit": "points",
        "check_date": "2027-03-31",
        "source": "metrics",
        "category": "pricing",
        "assumption": "Monthly customers take the annual discount",
        "quote": "this should cut churn by 3 points by March",
        "confidence": 0.82,
    }
    exp.update(over)
    value = result(
        host,
        m,
        1.0,
        user_id=NIL,
        window="bets:g1",
        agent="DecisionAgent",
        kind="decision_candidate",
        key=f"agentresult:{m}:bets:g1:DecisionAgent",
    )
    value["window_id"] = "bets:g1"
    value["payload_json"] = json.dumps(
        {"prompt_version": "decision_bet@v1", "generation": "g1", "expectations": [exp]}
    )
    return value


async def test_agent_proposal_lands_once_and_people_decide(client):
    host, member, _o, m, d = world()
    _sql(
        "UPDATE decisions SET confirmed_by = %(u)s WHERE id = %(d)s",
        {"u": str(member.user_id), "d": str(d)},
    )
    assert await apply(proposal(host, m, d)) is True
    rows = _sql("SELECT * FROM decision_bets WHERE decision_id = %(d)s", {"d": str(d)})
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "proposed" and row["proposed_by"] == "agent"
    assert (
        str(row["owner_user_id"]) == str(member.user_id)
        and row["prompt_version"] == "decision_bet@v1"
    )
    sent = notes(f"decision-bet:{row['id']}:proposed")
    assert len(sent) == 1 and sent[0]["user_id"] == str(member.user_id)
    assert sent[0]["link"].endswith(f"/decisions/{d}#bet")
    # A replay (new idempotency key, same decision) never adds a second bet.
    again = proposal(host, m, d)
    again["header"]["idempotency_key"] += ":replay"
    assert await apply(again) is False

    sign_in(member)
    view = client.get(f"/api/v1/decisions/{d}/bet").json()
    assert view["bet"]["status"] == "proposed" and view["bet"]["quote"].startswith("this should")
    dismissed = client.post(f"/api/v1/decisions/{d}/bet/dismiss").json()["bet"]
    assert dismissed["status"] == "dismissed"
    restored = client.post(f"/api/v1/decisions/{d}/bet/restore").json()["bet"]
    assert restored["status"] == "proposed"
    accepted = client.post(f"/api/v1/decisions/{d}/bet/accept").json()["bet"]
    assert accepted["status"] == "open"
    assert client.post(f"/api/v1/decisions/{d}/bet/accept").json()["bet"]["status"] == "open"


async def test_proposals_for_another_meetings_decision_or_bad_shapes_are_ignored(client):
    host, _member, _o, m, d = world()
    other_m = host_meeting(host)
    other_d = decision(host, other_m, "Something else")
    assert await apply(proposal(host, m, other_d)) is False
    bad = proposal(host, m, d, direction="sideways")
    bad["header"]["idempotency_key"] += ":bad"
    assert await apply(bad) is False
    assert (
        _sql(
            "SELECT 1 FROM decision_bets WHERE decision_id IN (%(a)s, %(b)s)",
            {"a": str(d), "b": str(other_d)},
        )
        == []
    )


# --- the check -----------------------------------------------------------------------------------


def _make_due(d: uuid.UUID) -> None:
    _sql(
        "UPDATE decision_bets SET check_date = current_date - 1 WHERE decision_id = %(d)s",
        {"d": str(d)},
    )


def test_check_suggests_from_a_connected_metric_and_asks_the_owner_once(client):
    host, member, _o, m, d = world()
    a = task(host, m, member.user_id, "Ship the annual plan page")
    sign_in(host)
    client.put(
        f"/api/v1/decisions/{d}/bet",
        json=bet_body(
            source="crm",
            metric_key="hubspot.deals_won",
            metric="Deals won",
            direction="increase",
            magnitude=10,
            unit="deals",
            baseline=None,
        ),
    )
    _make_due(d)
    today = date.today().isoformat()
    due = client.get(f"/internal/bets/due?as_of={today}", headers=wf(host.org_id)).json()["items"]
    [item] = [i for i in due if i["decision_id"] == str(d)]
    assert item["metric_key"] == "hubspot.deals_won" and item["action_ids"] == [str(a)]
    body = {
        "as_of": today,
        "metric": {
            "connected": True,
            "provider": "hubspot",
            "label": "Deals won",
            "unit": "deals",
            "value": 32,
            "baseline_value": 20,
        },
        "jira": {
            "connected": True,
            "issues": [
                {
                    "action_id": str(a),
                    "issue_key": "WEB-7",
                    "issue_url": "https://x/WEB-7",
                    "open": False,
                }
            ],
        },
    }
    res = client.post(f"/internal/bets/{item['bet_id']}/check", json=body, headers=wf(host.org_id))
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["status"] == "due" and out["suggested_outcome"] == "hit" and out["notified"] is True
    again = client.post(
        f"/internal/bets/{item['bet_id']}/check", json=body, headers=wf(host.org_id)
    ).json()
    assert again["evidence_added"] == 0 and again["notified"] is False
    assert len(notes(f"decision-bet:{item['bet_id']}:due")) == 1

    view = client.get(f"/api/v1/decisions/{d}/bet").json()["bet"]
    assert view["status"] == "due" and view["outcome"] is None  # suggested, never resolved
    assert view["suggested_outcome"] == "hit" and view["baseline"] == 20.0
    kinds = [e["kind"] for e in view["evidence"]]
    assert kinds[0] == "metric" and "jira" in kinds
    assert any("1 of 1 Jira issues" in e["title"] for e in view["evidence"])
    sign_in(member)
    mine = client.get("/api/v1/bets/mine").json()["items"]
    assert mine == [] or all(i["bet"]["owner_user_id"] != str(member.user_id) for i in mine)


def test_check_without_a_connection_says_so_and_finds_later_meetings(client):
    host, member, outsider, m, d = world()
    sign_in(host)
    client.put(f"/api/v1/decisions/{d}/bet", json=bet_body(source="metrics"))
    _sql(
        "UPDATE decisions SET created_at = now() - interval '30 days' WHERE id = %(d)s",
        {"d": str(d)},
    )
    # A later decision about churn, in a meeting the member was NOT in.
    later_m = host_meeting(host, outsider, title="Churn deep dive")
    decision(host, later_m, "Offer a churn-save discount to monthly customers")
    # The decision that replaced this one, in a meeting the member was in.
    m3 = host_meeting(host, member, title="Billing follow-up")
    decision(host, m3, "Keep monthly billing for the starter plan", supersedes=d)
    _make_due(d)
    bet_id = _sql("SELECT id FROM decision_bets WHERE decision_id = %(d)s", {"d": str(d)})[0]["id"]
    res = client.post(
        f"/internal/bets/{bet_id}/check",
        json={"as_of": date.today().isoformat(), "metric": {"connected": False}, "jira": None},
        headers=wf(host.org_id),
    )
    assert res.status_code == 200 and res.json()["suggested_outcome"] is None
    view = client.get(f"/api/v1/decisions/{d}/bet").json()["bet"]
    titles = [e["title"] for e in view["evidence"]]
    assert any("not connected" in t for t in titles)
    assert any(t.startswith("Replaced by a later decision") for t in titles)
    assert any("churn-save" in t for t in titles)
    sign_in(member)
    member_view = client.get(f"/api/v1/decisions/{d}/bet").json()["bet"]
    withheld = [e for e in member_view["evidence"] if not e["visible"]]
    assert withheld and all("churn-save" not in e["title"] for e in member_view["evidence"])


def test_a_proposed_bet_is_never_checked(client):
    host, _member, _o, m, d = world()
    _sql(
        "INSERT INTO decision_bets (org_id, decision_id, meeting_id, status, statement, metric, "
        "direction, check_date, proposed_by) VALUES (%(o)s, %(d)s, %(m)s, 'proposed', 'x', 'y', "
        "'increase', current_date - 1, 'agent')",
        {"o": str(host.org_id), "d": str(d), "m": str(m)},
    )
    due = client.get(f"/internal/bets/due?as_of={date.today()}", headers=wf(host.org_id)).json()
    assert all(i["decision_id"] != str(d) for i in due["items"])


# --- learning ---------------------------------------------------------------------------------


def _resolved(
    host: Tenant,
    m: uuid.UUID,
    outcome: str,
    *,
    category: str = "pricing",
    team: uuid.UUID | None = None,
    p: int | None = 70,
    assumption: str | None = None,
    held: bool | None = None,
    lesson: str | None = None,
) -> uuid.UUID:
    d = decision(host, m, f"Decision {uuid.uuid4().hex[:6]}")
    _sql(
        "INSERT INTO decision_bets (org_id, decision_id, meeting_id, owner_user_id, team_id, "
        "status, statement, metric, direction, check_date, category, confidence_pct, assumption, "
        "assumption_held, lesson, outcome, resolved_at, resolved_by) VALUES (%(o)s, %(d)s, %(m)s, "
        "%(u)s, %(t)s, 'resolved', 'Raise annual conversions', 'Conversions', 'increase', "
        "current_date, %(c)s, %(p)s, %(a)s, %(h)s, %(l)s, %(out)s, now(), %(u)s)",
        {
            "o": str(host.org_id),
            "d": str(d),
            "m": str(m),
            "u": str(host.user_id),
            "t": str(team) if team else None,
            "c": category,
            "p": p,
            "a": assumption,
            "h": held,
            "l": lesson,
            "out": outcome,
        },
    )
    return d


def _team(t: Tenant, name: str) -> uuid.UUID:
    row = _sql(
        "INSERT INTO teams (org_id, name) VALUES (%(o)s, %(n)s) RETURNING id",
        {"o": str(t.org_id), "n": name},
    )
    return uuid.UUID(str(row[0]["id"]))


def test_calibration_is_aggregate_and_hides_small_teams(client):
    host = seed_tenant()
    m = host_meeting(host)
    secret = host_meeting(host, title="Board", confidential=True)
    growth, ops = _team(host, "Growth"), _team(host, "Ops")
    for out in ("hit", "hit", "missed"):
        _resolved(
            host,
            m,
            out,
            team=growth,
            p=80,
            assumption="Customers accept the annual discount",
            held=out != "missed",
        )
    _resolved(
        host,
        m,
        "partly",
        category="hiring",
        team=ops,
        p=55,
        assumption="customers will accept the annual discount",
        held=False,
        lesson="Price changes need a full quarter to show",
    )
    _resolved(
        host,
        secret,
        "missed",
        category="finance",
        assumption="Board secret plan",
        held=False,
        lesson="Never shown",
    )
    sign_in(host)
    cal = client.get("/api/v1/bets/calibration").json()
    assert cal["totals"]["resolved"] == 5 and cal["totals"]["hit"] == 2
    assert [t["label"] for t in cal["by_team"]] == ["Growth"] and cal["teams_below_threshold"] == 1
    pricing = next(c for c in cal["by_category"] if c["key"] == "pricing")
    assert pricing["resolved"] == 3 and pricing["hit_rate"] == round(2 / 3, 3)
    assert cal["buckets"] and cal["totals"]["brier"] is not None
    [failing] = cal["failing_assumptions"]
    assert failing["times"] == 2  # the two phrasings are one assumption
    raw = json.dumps(cal)
    assert "Board secret plan" not in raw and "Never shown" not in raw
    assert "Maya" not in raw and str(host.user_id) not in raw  # no person anywhere
    assert [lesson["lesson"] for lesson in cal["lessons"]] == [
        "Price changes need a full quarter to show"
    ]


def test_hints_offer_lessons_from_similar_calls(client):
    host = seed_tenant()
    m = host_meeting(host)
    _resolved(host, m, "missed", lesson="Annual discounts cannibalise expansion revenue")
    sign_in(host)
    h = client.get(
        "/api/v1/bets/hints",
        params={"q": "Annual discount for monthly plans", "category": "pricing"},
    ).json()
    assert h["record"]["resolved"] == 1 and h["record"]["missed"] == 1
    assert h["lessons"][0]["lesson"].startswith("Annual discounts")
    d = decision(host, m, "Offer an annual discount on the team plan")
    view = client.get(f"/api/v1/decisions/{d}/bet").json()
    assert view["hints"]["lessons"], view["hints"]


# --- status history -------------------------------------------------------------------------------


def test_status_history_records_every_change():
    host = seed_tenant()
    m = seed_meeting(host.org_id)
    d = decision(host, m, "Ship it", status="proposed")
    _sql("UPDATE decisions SET status = 'confirmed' WHERE id = %(d)s", {"d": str(d)})
    _sql("UPDATE decisions SET content = 'Ship it now' WHERE id = %(d)s", {"d": str(d)})
    rows = _sql(
        "SELECT from_status, to_status FROM record_status_history WHERE record_id = %(d)s "
        "ORDER BY changed_at, id",
        {"d": str(d)},
    )
    assert [(r["from_status"], r["to_status"]) for r in rows] == [
        (None, "proposed"),
        ("proposed", "confirmed"),
    ]


async def test_status_history_is_append_only_for_the_app(migrated_db):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn:
            with pytest.raises(Exception):  # noqa: B017 - permission denied
                await conn.execute(text("DELETE FROM record_status_history"))
    finally:
        await engine.dispose()


# --- the workflow activity and the CRM reader --------------------------------------------------


def test_activity_reads_the_crm_and_jira_then_posts_the_check():
    calls: list[tuple[str, Any]] = []
    bet_id, action = str(uuid.uuid4()), str(uuid.uuid4())

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.url.path, body))
        if request.url.path == "/internal/bets/due":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "bet_id": bet_id,
                            "decision_id": str(uuid.uuid4()),
                            "source": "crm",
                            "metric_key": "hubspot.deals_won",
                            "decided_on": "2026-08-01",
                            "check_date": "2026-11-01",
                            "action_ids": [action],
                        }
                    ]
                },
            )
        if request.url.path == "/internal/crm/metric":
            return httpx.Response(
                200,
                json={
                    "connected": True,
                    "value": 12,
                    "baseline_value": 9,
                    "label": "Deals won",
                    "unit": "deals",
                    "partial": False,
                },
            )
        if request.url.path == "/internal/jira/issue-status":
            return httpx.Response(200, json={"connected": False, "issues": []})
        return httpx.Response(
            200, json={"status": "due", "notified": True, "suggested_outcome": "hit"}
        )

    acts = BetActivities(
        core_url="http://core",
        core_secret="s" * 40,
        integration_url="http://integ",
        integration_secret="s" * 40,
        transport=httpx.MockTransport(handler),
    )
    out = asyncio.run(acts.bets_for_tenant(str(uuid.uuid4()), date(2026, 11, 11)))
    assert out == {"due": 1, "checked": 1, "failed": 0, "notified": 1, "suggested": 1}
    assert calls[1] == (
        "/internal/crm/metric",
        {"metric_key": "hubspot.deals_won", "start": "2026-08-01", "end": "2026-11-01"},
    )
    check = calls[-1]
    assert check[0] == f"/internal/bets/{bet_id}/check"
    assert check[1]["metric"]["value"] == 12 and check[1]["jira"] == {
        "connected": False,
        "provider": None,
        "issues": [],
    }


def test_crm_reader_counts_and_sums_with_pagination():
    from services.integration_service import bet_metrics
    from services.integration_service.clients import HubSpotApi

    pages = iter(
        [
            {
                "results": [
                    {"properties": {"amount": "1000"}},
                    {"properties": {"amount": "250.5"}},
                ],
                "paging": {"next": {"after": "2"}},
            },
            {"results": [{"properties": {"amount": None}}]},
        ]
    )
    seen: list[Any] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        if body["properties"] == ["hs_object_id"]:
            return httpx.Response(200, json={"total": 7, "results": []})
        return httpx.Response(200, json=next(pages))

    class C:
        hubspot = HubSpotApi(transport=httpx.MockTransport(handler))

    jan, end = date(2026, 1, 1), date(2026, 1, 31)
    read = bet_metrics.read_value
    count, _ = asyncio.run(read(C(), "tok", "hubspot.deals_won", jan, end))  # type: ignore[arg-type]
    total, partial = asyncio.run(read(C(), "tok", "hubspot.revenue_won", jan, end))  # type: ignore[arg-type]
    assert count == 7 and total == 1250.5 and partial is False
    won = [f["propertyName"] for f in seen[0]["filterGroups"][0]["filters"]]
    assert won == ["closedate", "hs_is_closed_won"]


async def test_crm_reader_says_not_connected_without_hubspot(migrated_db):
    from services.integration_service import bet_metrics

    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    t = seed_tenant()
    out = await bet_metrics.crm_metric(
        t.org_id,
        bet_metrics.MetricRequest(
            metric_key="hubspot.new_contacts", start=date(2026, 1, 1), end=date(2026, 2, 1)
        ),
    )
    assert out.connected is False and out.label == "New contacts"
