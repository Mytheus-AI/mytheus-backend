"""The company time machine ("Beyond the meeting", Layer 3), against a real Postgres.

A pricing story told over four meetings (dates set explicitly through the status history):

- June 10, "Pricing review": decide "Price the starter plan at 19 dollars" (quote, rationale),
  promise "Draft the pricing page" due June 20, done June 18 (kept);
  promise "Email the resellers" due June 25, never done (broken).
- July 5, "Budget": decide "Hire two engineers" (not pricing).
- September 2, "Pricing reset", a meeting the member was NOT in: "Price the starter plan at 29
  dollars" replaces the June decision.
- October 1, "Pricing follow-up": "Offer annual pricing at 290 dollars".

The member sees June 10, July 5 and October 1; the September decision only as "a decision
from a meeting you were not in".
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime, timedelta

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import mcp
from services.core_api import time_machine as tm
from services.core_api.routes import time_machine
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
from tests.test_decision_bets import decision, set_email
from tests.test_my_tasks import add_user, task

Y = 2026


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-time-machine-test")
    app.include_router(time_machine.router)
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


def at(month: int, day: int, hour: int = 10) -> datetime:
    return datetime(Y, month, day, hour, tzinfo=UTC)


def meeting_on(t: Tenant, when: datetime, title: str, *people: Tenant) -> uuid.UUID:
    m = seed_meeting(t.org_id)
    _sql(
        "UPDATE meetings SET title = %(t)s, scheduled_start = %(w)s, created_at = %(w)s "
        "WHERE id = %(m)s",
        {"t": title, "w": when, "m": str(m)},
    )
    for p in people:
        add_participant(m, t.org_id, p.user_id)
    return m


def confirmed_on(
    record_id: uuid.UUID, kind: str, when: datetime, status: str = "confirmed"
) -> None:
    table = "decisions" if kind == "decision" else "action_items"
    _sql(
        f"UPDATE {table} SET created_at = %(w)s WHERE id = %(i)s", {"w": when, "i": str(record_id)}
    )  # noqa: S608
    _sql(
        "UPDATE record_status_history SET changed_at = %(w)s WHERE record_id = %(i)s",
        {"w": when, "i": str(record_id)},
    )
    if status != "confirmed":
        _sql(
            "INSERT INTO record_status_history (org_id, record_kind, record_id, from_status, "
            "to_status, changed_at) SELECT org_id, %(k)s, id, 'confirmed', %(s)s, %(w)s "
            f"FROM {table} WHERE id = %(i)s",  # noqa: S608
            {"k": kind, "s": status, "w": when, "i": str(record_id)},
        )


def story() -> dict[str, object]:
    host = seed_tenant()
    set_email(host, "maya.patel@example.com")
    member = add_user(host)
    set_email(member, "raj.mehta@example.com")
    june = meeting_on(host, at(6, 10), "Pricing review", host, member)
    july = meeting_on(host, at(7, 5), "Budget", host, member)
    sept = meeting_on(host, at(9, 2), "Pricing reset", host)
    octo = meeting_on(host, at(10, 1), "Pricing follow-up", host, member)
    d1 = decision(host, june, "Price the starter plan at 19 dollars", by=host.user_id)
    _sql(
        "UPDATE decisions SET rationale = 'Undercut the two closest rivals', "
        "quote = 'Let us go with nineteen for starter', quote_speaker_id = %(u)s WHERE id = %(d)s",
        {"u": str(host.user_id), "d": str(d1)},
    )
    confirmed_on(d1, "decision", at(6, 10))
    d2 = decision(host, july, "Hire two engineers")
    confirmed_on(d2, "decision", at(7, 5))
    d3 = decision(host, sept, "Price the starter plan at 29 dollars", supersedes=d1)
    confirmed_on(d3, "decision", at(9, 2))
    d4 = decision(host, octo, "Offer annual pricing at 290 dollars")
    confirmed_on(d4, "decision", at(10, 1))
    page = task(host, june, member.user_id, "Draft the pricing page", due=f"{Y}-06-20")
    confirmed_on(page, "action", at(6, 10))
    _sql(
        "INSERT INTO record_status_history (org_id, record_kind, record_id, from_status, "
        "to_status, changed_at) VALUES (%(o)s, 'action', %(i)s, 'confirmed', 'done', %(w)s)",
        {"o": str(host.org_id), "i": str(page), "w": at(6, 18)},
    )
    _sql("UPDATE action_items SET status = 'done' WHERE id = %(i)s", {"i": str(page)})
    # (the trigger's own 'done' row is dated now, after the story: harmless)
    resellers = task(host, june, host.user_id, "Email the resellers", due=f"{Y}-06-25")
    confirmed_on(resellers, "action", at(6, 10))
    return {
        "host": host,
        "member": member,
        "d1": d1,
        "d2": d2,
        "d3": d3,
        "d4": d4,
        "june": june,
        "sept": sept,
        "octo": octo,
        "page": page,
        "resellers": resellers,
    }


def test_topic_stems():
    assert tm.topic_stems("How did our pricing strategy change?") == ["pric"]
    assert tm.matches(["pric"], "Raise the price by 8%")
    assert not tm.matches(["pric"], "Hire two engineers")


def test_snapshot_before_and_after_the_reversal(client):
    s = story()
    sign_in(s["member"])  # type: ignore[arg-type]
    july = client.get("/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-07-31"}).json()
    contents = [d["content"] for d in july["decisions"]]
    assert contents == ["Hire two engineers", "Price the starter plan at 19 dollars"]
    starter = july["decisions"][1]
    assert starter["quote"] == "Let us go with nineteen for starter"
    assert starter["quote_speaker_name"] == "Maya Patel"
    assert starter["rationale"] == "Undercut the two closest rivals"
    assert starter["meeting"]["title"] == "Pricing review"
    assert [p["content"] for p in july["promises"]] == ["Email the resellers"]
    assert july["promises"][0]["overdue"] is True
    june15 = client.get("/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-06-15"}).json()
    assert {p["content"] for p in june15["promises"]} == {
        "Draft the pricing page",
        "Email the resellers",
    }

    october = client.get(
        "/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-10-15", "topic": "pricing"}
    ).json()
    assert [d["content"] for d in october["decisions"]] == ["Offer annual pricing at 290 dollars"]
    [replaced] = october["replaced"]
    assert replaced["content"] == "Price the starter plan at 19 dollars"
    assert replaced["replaced_by"]["visible"] is False  # the September meeting was not theirs
    assert replaced["replaced_by"].get("content") is None
    assert october["counts"]["replaced"] == 1
    early = client.get("/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-01-01"}).json()
    assert early["decisions"] == [] and early["earliest"] == f"{Y}-06-10"


def test_the_host_sees_what_replaced_it(client):
    s = story()
    sign_in(s["host"])  # type: ignore[arg-type]
    october = client.get("/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-10-15"}).json()
    [replaced] = october["replaced"]
    assert replaced["replaced_by"]["content"] == "Price the starter plan at 29 dollars"
    assert replaced["replaced_by"]["meeting"]["title"] == "Pricing reset"


def test_diff_names_the_changes_and_the_meetings(client):
    s = story()
    sign_in(s["host"])  # type: ignore[arg-type]
    d = client.get(
        "/api/v1/time-machine/diff", params={"from": f"{Y}-06-01", "to": f"{Y}-10-31"}
    ).json()
    assert d["from"] == f"{Y}-06-01" and d["to"] == f"{Y}-10-31"
    assert d["counts"]["new_decisions"] == 4 and d["counts"]["reversed"] == 1
    [rev] = d["reversed"]
    assert rev["old"]["content"].endswith("19 dollars") and rev["new"]["content"].endswith(
        "29 dollars"
    )
    assert rev["caused_by"]["title"] == "Pricing reset"
    assert [p["content"] for p in d["promises_kept"]] == ["Draft the pricing page"]
    assert d["promises_kept"][0]["on_time"] is True
    assert [p["content"] for p in d["promises_broken"]] == ["Email the resellers"]
    titles = [m["meeting"]["title"] for m in d["meetings"]]
    assert titles[0] == "Pricing review" and "Pricing reset" in titles

    pricing = client.get(
        "/api/v1/time-machine/diff",
        params={"from": f"{Y}-08-01", "to": f"{Y}-10-31", "topic": "pricing"},
    ).json()
    assert [x["content"] for x in pricing["new_decisions"]] == [
        "Price the starter plan at 29 dollars",
        "Offer annual pricing at 290 dollars",
    ]
    assert pricing["counts"]["promises_broken"] == 0


def test_timeline_cites_meeting_and_quote_and_withholds_what_it_must(client):
    s = story()
    sign_in(s["member"])  # type: ignore[arg-type]
    t = client.get(
        "/api/v1/time-machine/timeline",
        params={"topic": "pricing", "from": f"{Y}-06-01", "to": f"{Y}-10-31"},
    ).json()
    contents = [step["decision"].get("content") for step in t["steps"]]
    assert contents == [
        "Price the starter plan at 19 dollars",
        "Offer annual pricing at 290 dollars",
    ]
    first = t["steps"][0]
    assert first["quote"] == "Let us go with nineteen for starter"
    assert first["decision"]["meeting"]["title"] == "Pricing review"
    assert first["in_force_now"] is False
    sign_in(s["host"])  # type: ignore[arg-type]
    full = client.get("/api/v1/time-machine/timeline", params={"topic": "pricing"}).json()
    kinds = [(step["kind"], step["decision"]["content"]) for step in full["steps"]]
    assert ("replaced", "Price the starter plan at 29 dollars") in kinds
    rep = next(step for step in full["steps"] if step["kind"] == "replaced")
    assert rep["replaces"]["content"] == "Price the starter plan at 19 dollars"


def test_guards(client):
    s = story()
    sign_in(s["member"])  # type: ignore[arg-type]
    future = (date.today() + timedelta(days=3)).isoformat()
    assert client.get("/api/v1/time-machine/snapshot", params={"as_of": future}).status_code == 422
    assert (
        client.get("/api/v1/time-machine/timeline", params={"topic": "the and"}).status_code == 422
    )
    other = seed_tenant()
    sign_in(other)
    empty = client.get("/api/v1/time-machine/snapshot", params={"as_of": f"{Y}-10-15"}).json()
    assert empty["decisions"] == [] and empty["counts"]["in_force"] == 0


def test_mcp_markdown_for_company_as_of():
    snap = {
        "as_of": "2026-07-31",
        "topic": "pricing",
        "counts": {
            "in_force": 1,
            "replaced": 0,
            "promises_open": 1,
            "promises_overdue": 1,
            "goals_active": 0,
        },
        "decisions": [
            {
                "id": "d1",
                "content": "Price starter at 19",
                "meeting": {"title": "Pricing review"},
                "decided_at": "2026-06-10T10:00:00Z",
                "rationale": "Undercut rivals",
                "quote": "Let us go with nineteen",
                "quote_speaker_name": "Maya Patel",
                "bet": None,
            }
        ],
        "replaced": [],
        "promises": [
            {
                "content": "Email the resellers",
                "owner_name": "Maya Patel",
                "due": "2026-06-25",
                "overdue": True,
            }
        ],
        "goals": [],
    }
    text = mcp.as_of_markdown(snap)
    assert text.startswith("# The company as of 2026-07-31 (topic: pricing)")
    assert 'Said: Maya Patel: "Let us go with nineteen"' in text
    assert "Email the resellers (Maya Patel, due 2026-06-25) (overdue)" in text
    assert "company_as_of" in {t["name"] for t in mcp.TOOLS}
