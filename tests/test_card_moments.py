"""Polish program, "Make the meeting calm and fast" (core-api side): card moments, agent updates
and the in-meeting ledger.

Consumer (`handle_agent_result`):
- every card row (decision, action, suggestion, note) and every promise stores its moment
  fields; a quote speaker from another tenant is nulled (gap 31);
- a candidate with `updates_candidate_id` updates the existing proposed card in place (no
  second row, the first moment kept, an assignee never cleared), creates it when it was never
  stored, and leaves a confirmed, dismissed or person-edited card alone.

Routes:
- the candidate, suggestion, worklog and commitment reads expose the moment fields;
- PATCH marks a card edited (`edited_at`), so no later agent update overwrites it;
- confirming an action closes the assignee's open promise of the same moment (and only that);
- confirm / edit / dismiss of a card and ask / dismiss of a suggestion write the meeting ledger
  the agents read (`nexus:{org_id}:meeting-ledger:{meeting_id}`), and a Redis outage never
  fails the request.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import fakeredis
import pytest

from services.core_api import meeting_ledger
from services.core_api.routes import candidates, commitments, suggestions, worklogs
from tests.conftest import APP_URL
from tests.test_candidates import acand, candidate_result, dcand, participant_meeting, rows
from tests.test_chat_api import Tenant, _create_test_app, _sql, seed_meeting, seed_tenant, sign_in
from tests.test_suggestions import seed_user
from tests.test_worklogs import apply, result

MOMENT = str(uuid.uuid4())


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    from starlette.testclient import TestClient

    from libs.shared import auth as auth_mod
    from libs.shared import db as db_mod
    from tests.test_chat_api import PRINCIPAL

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-card-moments-test")
    for module in (candidates, suggestions, commitments, worklogs):
        app.include_router(module.router)
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


class _SyncFakeRedis:
    """fakeredis's async client bound to no event loop: the TestClient runs its own."""

    def __init__(self) -> None:
        self.server = fakeredis.FakeServer()
        self.hashes: dict[str, dict[str, Any]] = {}

    async def hset(self, key: str, field: str, value: str) -> None:
        self.hashes.setdefault(key, {})[field] = json.loads(value)

    async def expire(self, key: str, ttl: int) -> None:
        assert ttl == 24 * 3600


@pytest.fixture
def ledger(monkeypatch) -> _SyncFakeRedis:
    fake = _SyncFakeRedis()
    monkeypatch.setattr(meeting_ledger, "get_redis", lambda: fake)
    return fake


def moment(**extra: Any) -> dict[str, Any]:
    return {
        "moment_id": MOMENT,
        "why": "Priya took it on",
        "quote": "I'll send the pricing deck by Friday",
        "speaker_id": None,
        "speaker_label": "Priya",
        "start_ms": 754_200,
        "end_ms": 757_900,
        **extra,
    }


def one(table: str, row_id: str) -> dict[str, Any]:
    (row,) = _sql(f"SELECT * FROM {table} WHERE id = %(id)s", {"id": row_id})
    return row


# --- consumer -------------------------------------------------------------------------------


async def test_every_card_and_promise_stores_its_moment(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    d = dcand(0.9, **moment(speaker_id=str(t.user_id)))
    a = acand(0.9, **moment())
    assert await apply(candidate_result(t, m, [d])) is True
    assert await apply(candidate_result(t, m, [a], agent="ActionAgent", window="w1")) is True
    row = one("decisions", d["id"])
    assert str(row["moment_id"]) == MOMENT and row["why"] == "Priya took it on"
    assert row["quote"] == "I'll send the pricing deck by Friday"
    assert str(row["quote_speaker_id"]) == str(t.user_id)
    assert (row["quote_start_ms"], row["quote_end_ms"]) == (754_200, 757_900)
    assert str(one("action_items", a["id"])["moment_id"]) == MOMENT

    note = result(t, m, 0.9, window="w2")
    note["payload_json"] = json.dumps(
        {
            "surfaced": True,
            **moment(why="You made a promise here"),
            "commitments": [{"content": "Send the deck", "due_hint": "Fri", "moment_id": MOMENT}],
        }
    )
    assert await apply(note) is True
    (log,) = _sql("SELECT * FROM worklogs WHERE meeting_id = %(m)s", {"m": str(m)})
    assert str(log["moment_id"]) == MOMENT and log["why"] == "You made a promise here"
    (promise,) = _sql("SELECT * FROM commitments WHERE meeting_id = %(m)s", {"m": str(m)})
    assert str(promise["moment_id"]) == MOMENT

    sugg = result(t, m, 0.9, window="w3", agent="QuestionAgent", kind="suggestion")
    sid = str(uuid.uuid4())
    sugg["payload_json"] = json.dumps(
        {
            "suggestions": [
                {"id": sid, "question": "Who signs?", "priority": 1, "confidence": 0.9, **moment()}
            ]
        }
    )
    assert await apply(sugg) is True
    assert str(one("agent_suggestions", sid)["moment_id"]) == MOMENT


async def test_a_quote_speaker_from_another_tenant_is_nulled(migrated_db):
    t, other = seed_tenant(), seed_tenant()
    m = seed_meeting(t.org_id)
    d = dcand(0.9, **moment(speaker_id=str(other.user_id), moment_id="not-a-uuid"))
    await apply(candidate_result(t, m, [d]))
    row = one("decisions", d["id"])
    assert row["quote_speaker_id"] is None and row["moment_id"] is None


async def test_an_update_changes_the_card_in_place_and_keeps_its_moment(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    first = acand(0.8, assignee_id=str(t.user_id), **moment())
    await apply(candidate_result(t, m, [first], agent="ActionAgent", window="w0"))
    update = {
        **acand(0.92, assignee_id=None, due_date="2026-12-09"),
        "id": first["id"],
        "updates_candidate_id": first["id"],
        "content": "Send the pricing deck to Acme by 9 December",
        **moment(moment_id=str(uuid.uuid4()), why="Refines an earlier card", start_ms=900_000),
    }
    assert await apply(candidate_result(t, m, [update], agent="ActionAgent", window="w1"))
    (row,) = rows("action_items", m)
    assert row["content"] == "Send the pricing deck to Acme by 9 December"
    assert round(row["confidence"], 2) == 0.92
    assert str(row["assignee_id"]) == str(t.user_id)  # an update never clears the owner
    assert str(row["due_date"]) == "2026-12-09"
    assert str(row["moment_id"]) == MOMENT  # the card's first moment stays
    assert row["why"] == "Refines an earlier card" and row["quote_start_ms"] == 900_000


async def test_an_update_never_touches_a_resolved_or_edited_card(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    cards = [dcand(0.9) for _ in range(3)]
    await apply(candidate_result(t, m, cards))
    _sql("UPDATE decisions SET status = 'dismissed' WHERE id = %(id)s", {"id": cards[0]["id"]})
    _sql(
        "UPDATE decisions SET status = 'confirmed', confirmed_by = %(u)s WHERE id = %(id)s",
        {"id": cards[1]["id"], "u": str(t.user_id)},
    )
    _sql("UPDATE decisions SET edited_at = now() WHERE id = %(id)s", {"id": cards[2]["id"]})
    updates = [
        {**dcand(0.95), "id": c["id"], "updates_candidate_id": c["id"], "content": "Changed"}
        for c in cards
    ]
    assert await apply(candidate_result(t, m, updates, window="w1")) is False
    assert all(r["content"] != "Changed" for r in rows("decisions", m))
    assert len(rows("decisions", m)) == 3


async def test_an_update_of_a_never_stored_card_creates_it(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    target = str(uuid.uuid4())
    upd = {**dcand(0.9), "id": target, "updates_candidate_id": target}
    assert await apply(candidate_result(t, m, [upd])) is True
    assert [str(r["id"]) for r in rows("decisions", m)] == [target]


# --- routes ---------------------------------------------------------------------------------


def _seed_promise(t: Tenant, m: uuid.UUID, moment_id: str, user: uuid.UUID) -> str:
    (row,) = _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, source_window_id, "
        "moment_id) VALUES (%(o)s, %(u)s, %(m)s, 'Send the deck', %(w)s, %(mo)s) RETURNING id",
        {"o": str(t.org_id), "u": str(user), "m": str(m), "mo": moment_id, "w": moment_id},
    )
    return str(row["id"])


def test_reads_expose_the_moment_and_an_edit_marks_the_card(client, migrated_db, ledger):
    import asyncio

    t = seed_tenant()
    m = participant_meeting(t)
    d = dcand(0.9, **moment(speaker_id=str(t.user_id)))
    asyncio.run(apply(candidate_result(t, m, [d])))
    sign_in(t)
    body = client.get(f"/api/v1/meetings/{m}/candidates").json()
    (got,) = body["decisions"]
    assert got["moment_id"] == MOMENT and got["why"] == "Priya took it on"
    assert got["quote"] and got["quote_speaker_id"] == str(t.user_id)
    assert (got["quote_start_ms"], got["quote_end_ms"]) == (754_200, 757_900)
    state = client.get(f"/api/v1/meetings/{m}/candidates/{d['id']}").json()
    assert state["moment_id"] == MOMENT and state["quote_start_ms"] == 754_200

    r = client.patch(f"/api/v1/decisions/{d['id']}", json={"content": "Ship on the 9th"})
    assert r.status_code == 200
    assert one("decisions", d["id"])["edited_at"] is not None
    key = f"nexus:{t.org_id}:meeting-ledger:{m}"
    assert ledger.hashes[key][f"s:{d['id']}"] == {
        "status": "edited",
        "text": "Ship on the 9th",
        "kind": "decision",
    }
    assert client.post(f"/api/v1/decisions/{d['id']}/dismiss").status_code == 200
    assert ledger.hashes[key][f"s:{d['id']}"]["status"] == "dismissed"


def test_confirming_an_action_closes_the_assignees_promise_of_that_moment(
    client, migrated_db, ledger
):
    import asyncio

    t = seed_tenant()
    m = participant_meeting(t)
    colleague = seed_user(t)
    a = acand(0.9, assignee_id=str(t.user_id), **moment())
    asyncio.run(apply(candidate_result(t, m, [a], agent="ActionAgent")))
    mine = _seed_promise(t, m, MOMENT, t.user_id)
    other_moment = _seed_promise(t, m, str(uuid.uuid4()), t.user_id)
    theirs = _seed_promise(t, m, MOMENT, colleague.user_id)
    sign_in(t)
    assert client.post(f"/api/v1/actions/{a['id']}/confirm").status_code == 200
    closed = one("commitments", mine)
    assert closed["status"] == "done" and closed["done_at"] is not None
    assert str(closed["action_item_id"]) == a["id"]
    assert one("commitments", other_moment)["status"] == "open"
    assert one("commitments", theirs)["status"] == "open"
    key = f"nexus:{t.org_id}:meeting-ledger:{m}"
    assert ledger.hashes[key][f"s:{a['id']}"]["status"] == "confirmed"

    listed = client.get("/api/v1/commitments").json()["items"]
    by_id = {i["id"]: i for i in listed}
    assert by_id[mine]["action_item_id"] == a["id"] and by_id[mine]["moment_id"] == MOMENT


def test_a_suggestion_dismissal_reaches_the_ledger_and_redis_failing_is_harmless(
    client, migrated_db, monkeypatch
):
    import asyncio

    t = seed_tenant()
    m = participant_meeting(t)
    sid = str(uuid.uuid4())
    value = result(t, m, 0.9, agent="QuestionAgent", kind="suggestion")
    value["payload_json"] = json.dumps(
        {
            "suggestions": [
                {
                    "id": sid,
                    "question": "Who signs QA?",
                    "priority": 1,
                    "confidence": 0.9,
                    **moment(),
                }
            ]
        }
    )
    asyncio.run(apply(value))
    fake = _SyncFakeRedis()
    monkeypatch.setattr(meeting_ledger, "get_redis", lambda: fake)
    sign_in(t)
    got = client.get(f"/api/v1/suggestions?meeting_id={m}").json()["open"]
    assert got[0]["moment_id"] == MOMENT and got[0]["why"] == "Priya took it on"
    assert client.post(f"/api/v1/suggestions/{sid}/dismiss").status_code == 200
    key = f"nexus:{t.org_id}:meeting-ledger:{m}"
    assert fake.hashes[key][f"q:{t.user_id}:{sid}"] == {
        "status": "dismissed",
        "text": "Who signs QA?",
    }

    class Down:
        async def hset(self, *_a: Any) -> None:
            raise ConnectionError("redis down")

    monkeypatch.setattr(meeting_ledger, "get_redis", lambda: Down())
    d = dcand(0.9)
    asyncio.run(apply(candidate_result(t, m, [d])))
    assert client.post(f"/api/v1/decisions/{d['id']}/confirm").status_code == 200


def test_notes_expose_their_moment(client, migrated_db, ledger):
    import asyncio

    t = seed_tenant()
    m = participant_meeting(t)
    note = result(t, m, 0.9)
    note["payload_json"] = json.dumps({"surfaced": True, **moment(why="You own billing")})
    asyncio.run(apply(note))
    sign_in(t)
    r = client.get(f"/api/v1/meetings/{m}/worklogs")
    assert r.status_code == 200, r.text
    (item,) = r.json()["items"]
    assert item["moment_id"] == MOMENT and item["why"] == "You own billing"
