"""Decision / action candidates: Plan 7-9 section 3.6 and the Phase 8 Part A contract (section 2).

Consumer (`handle_agent_result`): candidates become `proposed` rows keyed by their own ids and
write ZERO outbox rows; redelivery writes nothing more; below-silent writes nothing; a foreign
assignee is nulled; bad candidate shapes are skipped; a non-JSON payload is poison.

Routes: GET candidates serves only proposed, above-surface rows (403 for a non-participant, 404
cross-tenant); confirm writes the row and exactly one outbox row whose payload carries the Avro
field names; re-confirm adds nothing; dismiss emits nothing; the 409 status rules; supersedes;
another tenant's row is 404 on every route. RLS isolation for the three tables. And the
producer grep: only `services/core_api/routes/` enqueues to `decisions` / `actions`.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from services.core_api.consumer import PoisonMessage
from services.core_api.routes import candidates
from tests.conftest import APP_URL
from tests.test_chat_api import (
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_worklogs import apply, result

DECISION_FIELDS = {
    "decision_id",
    "meeting_id",
    "content",
    "rationale",
    "confidence",
    "status",
    "confirmed_by",
    "supersedes",
}
ACTION_FIELDS = {
    "action_id",
    "meeting_id",
    "content",
    "assignee_id",
    "due_date",
    "confidence",
    "status",
}


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
    app = _create_test_app("core-api-candidates-test")
    app.include_router(candidates.router)
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


def candidate_result(
    t: Tenant,
    meeting_id: uuid.UUID,
    cands: list[dict[str, Any]],
    *,
    agent: str = "DecisionAgent",
    window: str = "w0",
) -> dict[str, Any]:
    kind = "decision_candidate" if agent == "DecisionAgent" else "action_candidate"
    value = result(t, meeting_id, 0.9, window=window, agent=agent, kind=kind)
    value["payload_json"] = json.dumps({"candidates": cands, "prompt": "x@v1"})
    return value


def dcand(confidence: float = 0.86, **extra: Any) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "content": f"Ship the beta at {confidence}",
        "confidence": confidence,
        "surfaced": confidence > 0.7,
        "rationale": "QA signed off",
        "stakeholder_ids": [],
        **extra,
    }


def acand(confidence: float = 0.86, **extra: Any) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "content": f"Write the release notes at {confidence}",
        "confidence": confidence,
        "surfaced": confidence > 0.7,
        "assignee_id": None,
        "due_date": "2026-12-02",
        "depends_on": [],
        **extra,
    }


def rows(table: str, meeting_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        f"SELECT * FROM {table} WHERE meeting_id = %(m)s ORDER BY created_at",
        {"m": str(meeting_id)},
    )


def outbox_for(org_id: uuid.UUID, topic: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT topic, idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
        "AND topic = %(t)s ORDER BY id",
        {"o": str(org_id), "t": topic},
    )


def all_outbox(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql("SELECT topic FROM outbox WHERE org_id = %(o)s", {"o": str(org_id)})


def seed_decision(
    t: Tenant, meeting_id: uuid.UUID, *, confidence: float = 0.9, status: str = "proposed"
) -> uuid.UUID:
    did = uuid.uuid4()
    _sql(
        "INSERT INTO decisions (id, org_id, meeting_id, content, rationale, confidence, status, "
        "confirmed_by) VALUES (%(id)s, %(o)s, %(m)s, 'Decide', NULL, %(c)s, %(s)s, %(by)s)",
        {
            "id": str(did),
            "o": str(t.org_id),
            "m": str(meeting_id),
            "c": confidence,
            "s": status,
            "by": str(t.user_id) if status == "confirmed" else None,
        },
    )
    return did


def seed_action(
    t: Tenant,
    meeting_id: uuid.UUID,
    *,
    confidence: float = 0.9,
    due: date | None = date(2026, 12, 2),
    assignee: uuid.UUID | None = None,
) -> uuid.UUID:
    aid = uuid.uuid4()
    _sql(
        "INSERT INTO action_items (id, org_id, meeting_id, content, assignee_id, due_date, "
        "confidence) VALUES (%(id)s, %(o)s, %(m)s, 'Do it', %(a)s, %(d)s, %(c)s)",
        {
            "id": str(aid),
            "o": str(t.org_id),
            "m": str(meeting_id),
            "a": str(assignee) if assignee else None,
            "d": due,
            "c": confidence,
        },
    )
    return aid


def participant_meeting(t: Tenant) -> uuid.UUID:
    m = seed_meeting(t.org_id)
    add_participant(m, t.org_id, t.user_id)
    return m


# --- consumer ------------------------------------------------------------------------------


async def test_decision_candidates_become_proposed_rows_and_no_outbox(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    high, silent = dcand(0.86), dcand(0.55, rationale=None)
    assert await apply(candidate_result(t, m, [high, silent])) is True
    got = {str(r["id"]): r for r in rows("decisions", m)}
    assert set(got) == {high["id"], silent["id"]}
    assert {r["status"] for r in got.values()} == {"proposed"}
    assert got[high["id"]]["rationale"] == "QA signed off"
    assert got[silent["id"]]["rationale"] is None
    assert got[high["id"]]["confirmed_by"] is None
    assert all_outbox(t.org_id) == []


async def test_action_candidates_become_proposed_rows_and_no_outbox(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    c = acand(0.9, assignee_id=str(t.user_id))
    assert await apply(candidate_result(t, m, [c], agent="ActionAgent")) is True
    [row] = rows("action_items", m)
    assert str(row["id"]) == c["id"]
    assert row["status"] == "proposed"
    assert row["assignee_id"] == str(t.user_id)
    assert row["due_date"] == date(2026, 12, 2)
    assert all_outbox(t.org_id) == []


async def test_a_redelivered_candidate_result_writes_nothing_more(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = candidate_result(t, m, [dcand(0.9)])
    assert await apply(value) is True
    assert await apply(value) is False
    # a replayed window under a NEW idempotency key: the candidate id is the dedupe authority
    replay = json.loads(json.dumps(value))
    replay["header"]["idempotency_key"] = f"replay:{uuid.uuid4()}"
    assert await apply(replay) is False
    assert len(rows("decisions", m)) == 1
    assert all_outbox(t.org_id) == []


async def test_a_below_silent_candidate_writes_nothing(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    assert await apply(candidate_result(t, m, [dcand(0.39)])) is False
    assert await apply(candidate_result(t, m, [acand(0.2)], agent="ActionAgent")) is False
    assert rows("decisions", m) == [] and rows("action_items", m) == []


async def test_tenant_overrides_decide_the_silent_band(migrated_db):
    t = seed_tenant()
    _sql(
        "INSERT INTO tenant_settings (org_id, agent_confidence_overrides) VALUES (%(o)s, %(j)s) "
        "ON CONFLICT (org_id) DO UPDATE "
        "SET agent_confidence_overrides = EXCLUDED.agent_confidence_overrides",
        {"o": str(t.org_id), "j": json.dumps({"DecisionAgent": {"surface": 0.9, "silent": 0.6}})},
    )
    m = seed_meeting(t.org_id)
    await apply(candidate_result(t, m, [dcand(0.5), dcand(0.65)]))
    assert [round(r["confidence"], 2) for r in rows("decisions", m)] == [0.65]


async def test_a_foreign_assignee_is_nulled(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    cands = [
        acand(0.9, assignee_id=str(b.user_id)),
        acand(0.8, assignee_id="Participant 2", due_date="not a date"),
    ]
    await apply(candidate_result(a, m, cands, agent="ActionAgent"))
    got = rows("action_items", m)
    assert len(got) == 2
    assert {r["assignee_id"] for r in got} == {None}
    assert {r["due_date"] for r in got} == {date(2026, 12, 2), None}


async def test_bad_candidate_shapes_are_skipped_not_poison(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    good = dcand(0.9)
    bad = [
        "not an object",
        {**dcand(0.9), "id": "nope"},
        {**dcand(0.9), "content": "  "},
        {**dcand(0.9), "confidence": "high"},
        {**dcand(0.9), "confidence": 1.5},
        {**dcand(0.9), "confidence": True},
    ]
    assert await apply(candidate_result(t, m, [*bad, good])) is True
    assert [str(r["id"]) for r in rows("decisions", m)] == [good["id"]]
    no_list = candidate_result(t, m, [], window="w9")
    no_list["payload_json"] = json.dumps({"candidates": "x"})
    assert await apply(no_list) is False


async def test_a_non_json_payload_is_poison(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = candidate_result(t, m, [dcand(0.9)])
    value["payload_json"] = "{not json"
    with pytest.raises(PoisonMessage):
        await apply(value)
    assert rows("decisions", m) == []


async def test_a_candidate_for_another_tenants_meeting_is_poison(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    theirs = seed_meeting(b.org_id)
    with pytest.raises(PoisonMessage):
        await apply(candidate_result(a, theirs, [dcand(0.9)]))
    assert rows("decisions", theirs) == []


# --- GET candidates -----------------------------------------------------------------------


def test_get_candidates_returns_only_proposed_surfaced_rows(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    d1 = seed_decision(t, m, confidence=0.8)
    seed_decision(t, m, confidence=0.6)  # silent band: stored, not served
    seed_decision(t, m, confidence=0.95, status="dismissed")
    seed_decision(t, m, confidence=0.95, status="confirmed")
    a1 = seed_action(t, m, confidence=0.9)
    seed_action(t, m, confidence=0.5)
    other = seed_meeting(t.org_id)
    seed_decision(t, other, confidence=0.9)

    sign_in(t)
    resp = client.get(f"/api/v1/meetings/{m}/candidates")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [d["id"] for d in body["decisions"]] == [str(d1)]
    assert [a["id"] for a in body["actions"]] == [str(a1)]
    assert body["actions"][0]["due_date"] == "2026-12-02"
    assert body["decisions"][0]["status"] == "proposed"


def test_get_candidates_is_403_for_a_non_participant_and_404_cross_tenant(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    not_mine = seed_meeting(a.org_id)
    sign_in(a)
    resp = client.get(f"/api/v1/meetings/{not_mine}/candidates")
    assert resp.status_code == 403
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"
    theirs = participant_meeting(b)
    assert client.get(f"/api/v1/meetings/{theirs}/candidates").status_code == 404


# --- confirm / dismiss / edit -------------------------------------------------------------


def test_confirm_decision_writes_the_row_and_exactly_one_outbox_row(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    d = seed_decision(t, m, confidence=0.86)
    sign_in(t)
    resp = client.post(f"/api/v1/decisions/{d}/confirm")
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "confirmed"
    assert resp.json()["confirmed_by"] == str(t.user_id)
    [row] = _sql("SELECT status, confirmed_by FROM decisions WHERE id = %(d)s", {"d": str(d)})
    assert row == {"status": "confirmed", "confirmed_by": str(t.user_id)}

    [event] = outbox_for(t.org_id, "decisions")
    assert event["idempotency_key"] == f"decision:{d}:confirmed"
    payload = event["payload"]
    assert set(payload) == DECISION_FIELDS | {"header"}
    assert payload["decision_id"] == str(d)
    assert payload["meeting_id"] == str(m)
    assert payload["rationale"] == ""  # Avro `rationale` is a non-null string
    assert payload["status"] == "confirmed"
    assert payload["confirmed_by"] == str(t.user_id)
    assert payload["supersedes"] is None
    assert payload["confidence"] == pytest.approx(0.86, abs=1e-6)
    assert payload["header"]["org_id"] == str(t.org_id)
    assert payload["header"]["producer"] == "core-api"
    assert payload["header"]["idempotency_key"] == f"decision:{d}:confirmed"
    audit_rows = _sql(
        "SELECT action FROM audit_logs WHERE org_id = %(o)s AND target_id = %(d)s",
        {"o": str(t.org_id), "d": str(d)},
    )
    assert [r["action"] for r in audit_rows] == ["decision.confirmed"]

    again = client.post(f"/api/v1/decisions/{d}/confirm")
    assert again.status_code == 200
    assert again.json()["status"] == "confirmed"
    assert len(outbox_for(t.org_id, "decisions")) == 1


def test_confirm_action_writes_the_row_and_exactly_one_outbox_row(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    a = seed_action(t, m, confidence=0.9, assignee=t.user_id, due=date(2026, 12, 2))
    sign_in(t)
    resp = client.post(f"/api/v1/actions/{a}/confirm")
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "confirmed"
    [event] = outbox_for(t.org_id, "actions")
    assert event["idempotency_key"] == f"action:{a}:confirmed"
    payload = event["payload"]
    assert set(payload) == ACTION_FIELDS | {"header"}
    assert payload["action_id"] == str(a)
    assert payload["assignee_id"] == str(t.user_id)
    assert payload["due_date"] == (date(2026, 12, 2) - date(1970, 1, 1)).days
    assert payload["status"] == "confirmed"
    assert client.post(f"/api/v1/actions/{a}/confirm").status_code == 200
    assert len(outbox_for(t.org_id, "actions")) == 1

    b = seed_action(t, m, due=None)
    assert client.post(f"/api/v1/actions/{b}/confirm").status_code == 200
    nulls = [e["payload"] for e in outbox_for(t.org_id, "actions")][1]
    assert nulls["due_date"] is None and nulls["assignee_id"] is None


def test_confirm_rolls_back_the_row_and_the_outbox_together(client, migrated_db, monkeypatch):
    t = seed_tenant()
    m = participant_meeting(t)
    d = seed_decision(t, m)

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("audit write failed")

    monkeypatch.setattr(candidates, "audit", boom)
    sign_in(t)
    try:
        resp = client.post(f"/api/v1/decisions/{d}/confirm")
        assert resp.status_code >= 500
    except RuntimeError:
        pass
    assert _sql("SELECT status FROM decisions WHERE id = %(d)s", {"d": str(d)}) == [
        {"status": "proposed"}
    ]
    assert outbox_for(t.org_id, "decisions") == []


def test_dismiss_emits_nothing_and_leaves_no_confirmed_row(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    d, a = seed_decision(t, m), seed_action(t, m)
    sign_in(t)
    for path in (f"/api/v1/decisions/{d}/dismiss", f"/api/v1/actions/{a}/dismiss"):
        resp = client.post(path)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "dismissed"
        assert client.post(path).status_code == 200  # idempotent
    assert all_outbox(t.org_id) == []
    assert (
        _sql(
            "SELECT 1 FROM decisions WHERE meeting_id = %(m)s AND status = 'confirmed' "
            "UNION ALL SELECT 1 FROM action_items "
            "WHERE meeting_id = %(m)s AND status = 'confirmed'",
            {"m": str(m)},
        )
        == []
    )
    # confirm after dismiss is a conflict, and still emits nothing
    for path in (f"/api/v1/decisions/{d}/confirm", f"/api/v1/actions/{a}/confirm"):
        resp = client.post(path)
        assert resp.status_code == 409
        assert resp.json()["code"] == "VERSION_CONFLICT"
    assert all_outbox(t.org_id) == []


def test_edit_before_confirm_and_409_after(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    d, a = seed_decision(t, m), seed_action(t, m)
    sign_in(t)
    resp = client.patch(f"/api/v1/decisions/{d}", json={"content": "Ship on 9 Dec"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["content"] == "Ship on 9 Dec" and resp.json()["status"] == "proposed"
    resp = client.patch(
        f"/api/v1/actions/{a}",
        json={"content": "Draft notes", "assignee_id": str(t.user_id), "due_date": None},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["assignee_id"] == str(t.user_id) and resp.json()["due_date"] is None
    assert client.patch(f"/api/v1/decisions/{d}", json={}).status_code == 422
    assert client.patch(f"/api/v1/decisions/{d}", json={"content": None}).status_code == 422
    assert client.patch(f"/api/v1/decisions/{d}", json={"org_id": "x"}).status_code == 422

    assert client.post(f"/api/v1/decisions/{d}/confirm").status_code == 200
    assert client.post(f"/api/v1/actions/{a}/confirm").status_code == 200
    # the event carries the edited text
    assert outbox_for(t.org_id, "decisions")[0]["payload"]["content"] == "Ship on 9 Dec"
    assert outbox_for(t.org_id, "actions")[0]["payload"]["content"] == "Draft notes"
    resp = client.patch(f"/api/v1/decisions/{d}", json={"content": "late edit"})
    assert resp.status_code == 409 and resp.json()["code"] == "VERSION_CONFLICT"
    assert client.patch(f"/api/v1/actions/{a}", json={"content": "late"}).status_code == 409
    assert client.post(f"/api/v1/decisions/{d}/dismiss").status_code == 409
    assert client.post(f"/api/v1/actions/{a}/dismiss").status_code == 409


def test_an_action_cannot_be_assigned_to_another_tenants_user(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = participant_meeting(a)
    act = seed_action(a, m)
    sign_in(a)
    resp = client.patch(f"/api/v1/actions/{act}", json={"assignee_id": str(b.user_id)})
    assert resp.status_code == 404
    assert _sql("SELECT assignee_id FROM action_items WHERE id = %(i)s", {"i": str(act)}) == [
        {"assignee_id": None}
    ]


def test_supersedes_is_stored_and_in_the_payload(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    old = seed_decision(t, m, status="confirmed")
    new = seed_decision(t, m)
    sign_in(t)
    resp = client.post(f"/api/v1/decisions/{new}/confirm", json={"supersedes": str(old)})
    assert resp.status_code == 200, resp.text
    assert resp.json()["supersedes"] == str(old)
    assert _sql("SELECT supersedes FROM decisions WHERE id = %(d)s", {"d": str(new)}) == [
        {"supersedes": str(old)}
    ]
    [event] = outbox_for(t.org_id, "decisions")
    assert event["payload"]["supersedes"] == str(old)


def test_supersedes_must_be_a_visible_confirmed_other_decision(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = participant_meeting(a)
    d = seed_decision(a, m)
    proposed = seed_decision(a, m)
    foreign = seed_decision(b, seed_meeting(b.org_id), status="confirmed")
    sign_in(a)
    url = f"/api/v1/decisions/{d}/confirm"
    assert client.post(url, json={"supersedes": str(foreign)}).status_code == 404
    assert client.post(url, json={"supersedes": str(uuid.uuid4())}).status_code == 404
    assert client.post(url, json={"supersedes": str(proposed)}).status_code == 422
    assert client.post(url, json={"supersedes": str(d)}).status_code == 422
    assert client.post(url, json={"bogus": 1}).status_code == 422
    assert all_outbox(a.org_id) == []
    assert _sql("SELECT status FROM decisions WHERE id = %(d)s", {"d": str(d)}) == [
        {"status": "proposed"}
    ]


def test_a_non_participant_cannot_act_on_a_candidate(client, migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)  # caller is NOT a participant
    d, a = seed_decision(t, m), seed_action(t, m)
    sign_in(t)
    for method, path, body in (
        ("patch", f"/api/v1/decisions/{d}", {"content": "x"}),
        ("post", f"/api/v1/decisions/{d}/confirm", None),
        ("post", f"/api/v1/decisions/{d}/dismiss", None),
        ("patch", f"/api/v1/actions/{a}", {"content": "x"}),
        ("post", f"/api/v1/actions/{a}/confirm", None),
        ("post", f"/api/v1/actions/{a}/dismiss", None),
    ):
        resp = getattr(client, method)(path, json=body) if body else getattr(client, method)(path)
        assert resp.status_code == 403, (path, resp.text)
        assert resp.json()["code"] == "NOT_A_PARTICIPANT"
    assert all_outbox(t.org_id) == []


def test_another_tenants_candidate_is_404_on_every_route(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    theirs = participant_meeting(b)
    d, act = seed_decision(b, theirs), seed_action(b, theirs)
    sign_in(a)
    for method, path, body in (
        ("patch", f"/api/v1/decisions/{d}", {"content": "x"}),
        ("post", f"/api/v1/decisions/{d}/confirm", None),
        ("post", f"/api/v1/decisions/{d}/dismiss", None),
        ("patch", f"/api/v1/actions/{act}", {"content": "x"}),
        ("post", f"/api/v1/actions/{act}/confirm", None),
        ("post", f"/api/v1/actions/{act}/dismiss", None),
        ("get", f"/api/v1/meetings/{theirs}/candidates", None),
    ):
        resp = getattr(client, method)(path, json=body) if body else getattr(client, method)(path)
        assert resp.status_code == 404, (path, resp.text)
    assert all_outbox(b.org_id) == []
    assert _sql("SELECT status FROM decisions WHERE id = %(d)s", {"d": str(d)}) == [
        {"status": "proposed"}
    ]


# --- RLS ----------------------------------------------------------------------------------


async def test_rls_hides_another_tenants_candidates_and_suggestions(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    seed_decision(a, m)
    seed_action(a, m)
    _sql(
        "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, question, priority) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'Ask about QA?', 2)",
        {"o": str(a.org_id), "u": str(a.user_id), "m": str(m)},
    )
    engine = create_async_engine(APP_URL)
    try:
        for org in (a.org_id, b.org_id, None):
            async with engine.connect() as conn, conn.begin():
                if org is not None:
                    await conn.execute(
                        text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org)}
                    )
                expected = 1 if org == a.org_id else 0
                for table in ("decisions", "action_items", "agent_suggestions"):
                    n = (
                        await conn.execute(
                            text(f"SELECT count(*) FROM {table} WHERE meeting_id = :m"),
                            {"m": str(m)},
                        )
                    ).scalar()
                    assert n == expected, (table, org)
        # tenant B cannot write a row into tenant A either (WITH CHECK)
        async with engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(b.org_id)}
            )
            with pytest.raises(Exception, match="row-level security"):
                await conn.execute(
                    text(
                        "INSERT INTO decisions (org_id, meeting_id, content, confidence) "
                        "VALUES (:o, :m, 'x', 0.9)"
                    ),
                    {"o": str(a.org_id), "m": str(m)},
                )
    finally:
        await engine.dispose()


# --- the producer grep --------------------------------------------------------------------

_REPO = Path(__file__).resolve().parents[1]
_TOPIC_LITERAL = re.compile(r"""topic\s*=\s*["'](decisions|actions)["']""")


def test_only_the_confirm_routes_enqueue_decisions_or_actions() -> None:
    """Rule zero: nothing reaches `decisions` / `actions` until a user presses Confirm.

    Every `EventEnvelope(topic="decisions"|"actions")` in the shipped code must live in
    `services/core_api/routes/`, and the only raw `INSERT INTO outbox` is the shared helper.
    """
    producers: set[str] = set()
    raw_outbox: set[str] = set()
    for root in ("services", "libs", "scripts", "migrations"):
        for path in (_REPO / root).rglob("*.py"):
            rel = path.relative_to(_REPO).as_posix()
            source = path.read_text(encoding="utf-8")
            if _TOPIC_LITERAL.search(source):
                producers.add(rel)
            if re.search(r"INSERT\s+INTO\s+outbox", source, re.IGNORECASE):
                raw_outbox.add(rel)
    assert producers == {"services/core_api/routes/candidates.py"}, producers
    assert all(p.startswith("services/core_api/routes/") for p in producers)
    assert raw_outbox == {"libs/shared/outbox.py"}, raw_outbox
    consumer = (_REPO / "services/core_api/consumer.py").read_text(encoding="utf-8")
    assert "enqueue" not in consumer and "EventEnvelope" not in consumer


def test_an_undecodable_record_is_poison_not_a_crash():
    """A record the Avro reader can never decode (a non-UUID in a `uuid` field) is dead-lettered.
    Before, the ValueError escaped the loop, the consumer died, and every restart re-read it."""
    from services.core_api.consumer import decode

    def bad(_raw: object, _ctx: object) -> object:
        raise ValueError("badly formed hexadecimal UUID string")

    with pytest.raises(PoisonMessage):
        decode(bad, b"\x00", None)

    class RegistryDown(Exception):
        pass

    def transient(_raw: object, _ctx: object) -> object:
        raise RegistryDown("schema registry unreachable")

    with pytest.raises(RegistryDown):
        decode(transient, b"\x00", None)


# --- the shared candidate ledger (polish program, Phase 0) ---------------------------------


def test_include_resolved_adds_confirmed_rows_never_dismissed(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    proposed = seed_decision(t, m, confidence=0.8)
    confirmed = seed_decision(t, m, confidence=0.5, status="confirmed")  # any confidence
    seed_decision(t, m, confidence=0.95, status="dismissed")
    done = seed_action(t, m, confidence=0.3)

    sign_in(t)
    assert client.post(f"/api/v1/actions/{done}/confirm").status_code == 200
    plain = client.get(f"/api/v1/meetings/{m}/candidates").json()
    assert [d["id"] for d in plain["decisions"]] == [str(proposed)]
    assert plain["actions"] == []

    body = client.get(f"/api/v1/meetings/{m}/candidates?include_resolved=true").json()
    assert {d["id"]: d["status"] for d in body["decisions"]} == {
        str(proposed): "proposed",
        str(confirmed): "confirmed",
    }
    assert [(a["id"], a["status"]) for a in body["actions"]] == [(str(done), "confirmed")]


def test_get_one_candidate_in_any_status_for_the_gateway(client, migrated_db):
    t = seed_tenant()
    m = participant_meeting(t)
    d, a = seed_decision(t, m), seed_action(t, m)
    sign_in(t)

    resp = client.get(f"/api/v1/meetings/{m}/candidates/{d}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["kind"] == "decision"
    assert resp.json()["status"] == "proposed"

    assert client.post(f"/api/v1/actions/{a}/confirm").status_code == 200
    body = client.get(f"/api/v1/meetings/{m}/candidates/{a}").json()
    assert body["kind"] == "action"
    assert body["status"] == "confirmed"
    assert body["confirmed_by"] == str(t.user_id)
    assert body["due_date"] == "2026-12-02"

    # Another meeting's id under this meeting, an unknown id: 404.
    other = participant_meeting(t)
    assert client.get(f"/api/v1/meetings/{other}/candidates/{d}").status_code == 404
    assert client.get(f"/api/v1/meetings/{m}/candidates/{uuid.uuid4()}").status_code == 404


def test_get_one_candidate_is_403_for_a_non_participant_and_404_cross_tenant(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    not_mine = seed_meeting(a.org_id)
    d = seed_decision(a, not_mine)
    sign_in(a)
    resp = client.get(f"/api/v1/meetings/{not_mine}/candidates/{d}")
    assert resp.status_code == 403
    theirs = participant_meeting(b)
    theirs_d = seed_decision(b, theirs)
    sign_in(a)
    assert client.get(f"/api/v1/meetings/{theirs}/candidates/{theirs_d}").status_code == 404
