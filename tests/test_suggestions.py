"""QuestionAgent suggestions: Plan 7-9 section 3.10, mytheus-docs ADR-0034.

Consumer: a `suggestion` result becomes `open` rows for the result's `user_id` under the
suggestions' own ids, with priority, confidence, rationale and citations; below the tenant's
silent threshold nothing is written; a redelivery or a replay under a new key writes nothing
more; bad suggestions are skipped; a non-JSON payload is poison; nothing goes to the outbox.

Routes: GET serves the caller's open, surfaced suggestions for the meeting plus their saved ones
from every meeting (Save-for-later persists into the next meeting); another participant of the
same meeting never sees them; a non-participant is 403; another tenant is 404. ask / dismiss /
save follow the ADR-0034 D7 transitions (409 otherwise), write one audit row each, and are
owner-only (another user, same tenant or not, gets 404). RLS isolation for the table.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from services.core_api.consumer import PoisonMessage
from services.core_api.routes import suggestions
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
    app = _create_test_app("core-api-suggestions-test")
    app.include_router(suggestions.router)
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


def seed_user(t: Tenant, tier: str = "member") -> Tenant:
    """A second user in the same tenant."""
    uid, clerk_user = uuid.uuid4(), f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'v@example.com')",
        {"u": str(uid), "o": str(t.org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(t.org_id), "u": str(uid), "t": tier},
    )
    return Tenant(t.org_id, t.clerk_org, uid, clerk_user)


def meeting_with(*people: Tenant) -> uuid.UUID:
    m = seed_meeting(people[0].org_id)
    for p in people:
        add_participant(m, p.org_id, p.user_id)
    return m


def scand(confidence: float = 0.86, **extra: Any) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "question": f"What is the budget ceiling at {confidence}?",
        "priority": 1,
        "confidence": confidence,
        "surfaced": confidence > 0.7,
        "rationale": "You own vendor spend.",
        "citations": [],
        **extra,
    }


def suggestion_result(
    t: Tenant, meeting_id: uuid.UUID, items: list[dict[str, Any]], *, window: str = "w0"
) -> dict[str, Any]:
    value = result(t, meeting_id, 0.9, window=window, agent="QuestionAgent", kind="suggestion")
    value["payload_json"] = json.dumps({"suggestions": items, "prompt": "question_agent@v1"})
    return value


def set_overrides(t: Tenant, overrides: dict[str, Any]) -> None:
    _sql(
        "INSERT INTO tenant_settings (org_id, agent_confidence_overrides) VALUES (%(o)s, %(j)s) "
        "ON CONFLICT (org_id) DO UPDATE "
        "SET agent_confidence_overrides = EXCLUDED.agent_confidence_overrides",
        {"o": str(t.org_id), "j": json.dumps(overrides)},
    )


def stored(meeting_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM agent_suggestions WHERE meeting_id = %(m)s ORDER BY created_at, id",
        {"m": str(meeting_id)},
    )


def seed_suggestion(
    t: Tenant,
    meeting_id: uuid.UUID,
    *,
    confidence: float | None = 0.9,
    status: str = "open",
    question: str = "Who signs the contract?",
) -> uuid.UUID:
    sid = uuid.uuid4()
    _sql(
        "INSERT INTO agent_suggestions (id, org_id, user_id, meeting_id, question, priority, "
        "status, confidence) VALUES (%(id)s, %(o)s, %(u)s, %(m)s, %(q)s, 2, %(s)s, %(c)s)",
        {
            "id": str(sid),
            "o": str(t.org_id),
            "u": str(t.user_id),
            "m": str(meeting_id),
            "q": question,
            "s": status,
            "c": confidence,
        },
    )
    return sid


def audits(target: uuid.UUID) -> list[str]:
    return [
        r["action"]
        for r in _sql(
            "SELECT action FROM audit_logs WHERE target_id = %(t)s ORDER BY occurred_at",
            {"t": str(target)},
        )
    ]


def status_of(sid: uuid.UUID) -> str:
    return str(
        _sql("SELECT status FROM agent_suggestions WHERE id = %(s)s", {"s": str(sid)})[0]["status"]
    )


# --- consumer ------------------------------------------------------------------------------


async def test_suggestions_become_open_rows_for_their_owner(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    cite = {"kind": "transcript", "chunk_id": "c1", "meeting_id": str(m), "start_ms": 1200}
    high = scand(0.86, citations=[cite, {"kind": "web", "url": "x"}])
    silent = scand(0.55, priority=3, rationale=None)
    assert await apply(suggestion_result(t, m, [high, silent])) is True
    got = {str(r["id"]): r for r in stored(m)}
    assert set(got) == {high["id"], silent["id"]}
    h, s = got[high["id"]], got[silent["id"]]
    assert {h["status"], s["status"]} == {"open"}
    assert h["user_id"] == s["user_id"] == str(t.user_id)
    assert (h["priority"], s["priority"]) == (1, 3)
    assert round(h["confidence"], 2) == 0.86 and h["rationale"] == "You own vendor spend."
    assert h["citations"] == [cite]  # an unknown citation kind is dropped
    assert s["rationale"] is None and s["citations"] == []
    assert _sql("SELECT 1 FROM outbox WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []


async def test_below_silent_and_malformed_suggestions_are_not_written(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    bad = [
        scand(0.39),
        scand(0.9, priority=4),
        scand(0.9, priority="1"),
        scand(0.9, priority=True),
        scand(0.9, id="not-a-uuid"),
        scand(0.9, question="   "),
        scand(1.5),
        "nope",
    ]
    assert await apply(suggestion_result(t, m, bad)) is False
    assert stored(m) == []


async def test_a_redelivery_or_replay_writes_nothing_more(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = suggestion_result(t, m, [scand(0.9)])
    assert await apply(value) is True
    assert await apply(value) is False
    replay = json.loads(json.dumps(value))
    replay["header"]["idempotency_key"] = f"replay:{uuid.uuid4()}"
    assert await apply(replay) is False  # the suggestion id is the authority
    assert len(stored(m)) == 1


async def test_the_tenant_silent_override_applies(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    set_overrides(t, {"QuestionAgent": {"silent": 0.6}})
    assert await apply(suggestion_result(t, m, [scand(0.55), scand(0.65)])) is True
    assert [round(r["confidence"], 2) for r in stored(m)] == [0.65]


async def test_a_non_json_payload_is_poison(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = suggestion_result(t, m, [])
    value["payload_json"] = "{not json"
    with pytest.raises(PoisonMessage):
        await apply(value)


async def test_a_user_from_another_tenant_is_poison(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    value = suggestion_result(a, m, [scand(0.9)])
    value["user_id"] = str(b.user_id)
    with pytest.raises(PoisonMessage):
        await apply(value)
    assert stored(m) == []


# --- read ----------------------------------------------------------------------------------


def test_get_serves_my_open_surfaced_ones_and_my_saved_ones(client, migrated_db):
    me = seed_tenant()
    earlier, now = meeting_with(me), meeting_with(me)
    shown = seed_suggestion(me, now, confidence=0.9, question="Shown?")
    seed_suggestion(me, now, confidence=0.6, question="Silent?")
    seed_suggestion(me, now, confidence=None, question="No confidence?")
    seed_suggestion(me, now, confidence=0.95, status="asked", question="Asked?")
    seed_suggestion(me, earlier, confidence=0.9, question="Other meeting, open?")
    saved = seed_suggestion(me, earlier, confidence=0.9, status="saved", question="Saved?")
    sign_in(me)
    resp = client.get(f"/api/v1/suggestions?meeting_id={now}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [s["id"] for s in body["open"]] == [str(shown)]
    assert [s["id"] for s in body["saved"]] == [str(saved)]
    assert body["saved"][0]["meeting_id"] == str(earlier)
    assert set(body["open"][0]) == {
        "id",
        "meeting_id",
        "question",
        "priority",
        "status",
        "confidence",
        "rationale",
        "citations",
        "created_at",
    }


def test_the_tenant_surface_override_applies(client, migrated_db):
    me = seed_tenant()
    m = meeting_with(me)
    set_overrides(me, {"QuestionAgent": {"surface": 0.5}})
    s = seed_suggestion(me, m, confidence=0.6)
    sign_in(me)
    assert [x["id"] for x in client.get(f"/api/v1/suggestions?meeting_id={m}").json()["open"]] == [
        str(s)
    ]


def test_another_participant_never_sees_my_suggestions(client, migrated_db):
    me = seed_tenant()
    other = seed_user(me)
    m = meeting_with(me, other)
    mine = seed_suggestion(me, m)
    seed_suggestion(me, m, status="saved")
    sign_in(other)
    body = client.get(f"/api/v1/suggestions?meeting_id={m}").json()
    assert body == {"open": [], "saved": []}
    for action in ("ask", "dismiss", "save"):
        resp = client.post(f"/api/v1/suggestions/{mine}/{action}")
        assert resp.status_code == 404, (action, resp.text)
    assert status_of(mine) == "open" and audits(mine) == []


def test_a_non_participant_is_403_and_another_tenant_is_404(client, migrated_db):
    me = seed_tenant()
    outsider = seed_user(me)
    m = meeting_with(me)
    sign_in(outsider)
    resp = client.get(f"/api/v1/suggestions?meeting_id={m}")
    assert resp.status_code == 403 and resp.json()["code"] == "NOT_A_PARTICIPANT"

    stranger = seed_tenant()
    s = seed_suggestion(me, m)
    sign_in(stranger)
    assert client.get(f"/api/v1/suggestions?meeting_id={m}").status_code == 404
    for action in ("ask", "dismiss", "save"):
        assert client.post(f"/api/v1/suggestions/{s}/{action}").status_code == 404
    assert client.post(f"/api/v1/suggestions/{uuid.uuid4()}/ask").status_code == 404
    assert status_of(s) == "open"


# --- save persists into the next meeting ----------------------------------------------------


async def test_save_then_the_card_shows_up_in_my_next_meeting(client, migrated_db):
    me = seed_tenant()
    first = meeting_with(me)
    c = scand(0.9)
    assert await apply(suggestion_result(me, first, [c])) is True
    sign_in(me)
    assert [
        s["id"] for s in client.get(f"/api/v1/suggestions?meeting_id={first}").json()["open"]
    ] == [c["id"]]
    resp = client.post(f"/api/v1/suggestions/{c['id']}/save")
    assert resp.status_code == 200 and resp.json()["status"] == "saved"

    second = meeting_with(me)  # a new meeting with the same user
    body = client.get(f"/api/v1/suggestions?meeting_id={second}").json()
    assert body["open"] == []
    assert [s["id"] for s in body["saved"]] == [c["id"]]
    # and it can be asked there
    assert client.post(f"/api/v1/suggestions/{c['id']}/ask").json()["status"] == "asked"
    assert client.get(f"/api/v1/suggestions?meeting_id={second}").json()["saved"] == []
    assert audits(uuid.UUID(c["id"])) == ["suggestion.saved", "suggestion.asked"]


# --- transitions ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "start,action,expected",
    [
        ("open", "ask", "asked"),
        ("open", "dismiss", "dismissed"),
        ("open", "save", "saved"),
        ("saved", "ask", "asked"),
        ("saved", "dismiss", "dismissed"),
    ],
)
def test_legal_transitions_write_one_audit_row(client, migrated_db, start, action, expected):
    me = seed_tenant()
    s = seed_suggestion(me, meeting_with(me), status=start)
    sign_in(me)
    resp = client.post(f"/api/v1/suggestions/{s}/{action}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == expected
    assert status_of(s) == expected
    assert audits(s) == [f"suggestion.{expected}"]


@pytest.mark.parametrize(
    "start,action",
    [
        ("asked", "dismiss"),
        ("asked", "save"),
        ("dismissed", "ask"),
        ("dismissed", "save"),
    ],
)
def test_illegal_transitions_are_409(client, migrated_db, start, action):
    me = seed_tenant()
    s = seed_suggestion(me, meeting_with(me), status=start)
    sign_in(me)
    resp = client.post(f"/api/v1/suggestions/{s}/{action}")
    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "VERSION_CONFLICT"
    assert status_of(s) == start and audits(s) == []


@pytest.mark.parametrize(
    "start,action", [("asked", "ask"), ("saved", "save"), ("dismissed", "dismiss")]
)
def test_repeating_the_current_status_is_200_and_writes_nothing(client, migrated_db, start, action):
    me = seed_tenant()
    s = seed_suggestion(me, meeting_with(me), status=start)
    sign_in(me)
    resp = client.post(f"/api/v1/suggestions/{s}/{action}")
    assert resp.status_code == 200 and resp.json()["status"] == start
    assert audits(s) == []


def test_saved_cannot_go_back_to_open_by_any_route(client, migrated_db):
    me = seed_tenant()
    s = seed_suggestion(me, meeting_with(me), status="saved")
    sign_in(me)
    assert client.post(f"/api/v1/suggestions/{s}/open").status_code in (404, 405)


# --- RLS ----------------------------------------------------------------------------------


async def test_rls_isolates_agent_suggestions(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    seed_suggestion(a, m)
    engine = create_async_engine(APP_URL)
    try:
        for org, expected in ((a.org_id, 1), (b.org_id, 0), (None, 0)):
            async with engine.connect() as conn, conn.begin():
                if org is not None:
                    await conn.execute(
                        text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org)}
                    )
                n = (
                    await conn.execute(
                        text("SELECT count(*) FROM agent_suggestions WHERE meeting_id = :m"),
                        {"m": str(m)},
                    )
                ).scalar()
                assert n == expected, org
        async with engine.connect() as conn:
            async with conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(b.org_id)}
                )
                with pytest.raises(Exception, match="row-level security"):
                    await conn.execute(
                        text(
                            "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, "
                            "question, priority) VALUES (:o, :u, :m, 'x?', 1)"
                        ),
                        {"o": str(a.org_id), "u": str(a.user_id), "m": str(m)},
                    )
    finally:
        await engine.dispose()
