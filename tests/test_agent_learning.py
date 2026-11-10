"""Agents that learn (polish program wave 2): feedback records, calibration, guidance, memory.

- every real confirm, dismiss, edit, ask, save and note rating is one `agent_feedback` row plus
  one `agent-feedback` outbox row whose payload matches the AgentFeedback Avro field set; a
  repeat that changes nothing records nothing; a rolled-back confirm records nothing;
- calibration: bounded, minimum sample size, decayed, quantised; the workspace band moves the
  decision/action reload and `GET /internal/agent-policy` together (one threshold source); the
  per-person band moves only that person's suggestions;
- guidance: workspace examples for the meeting agents never come from a confidential meeting;
  personal examples are only the person's own;
- memory: the `agent-feedback` consumer learns topics and recurring questions (dedupe on
  redelivery), the owner can read, pin, forget, mute, pause and delete it, nobody else can, the
  internal compact read and the brief use it.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis
import fastavro
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.bff import meeting_screens
from services.core_api import learning
from services.core_api import person_memory as pm
from services.core_api.routes import (
    candidates,
    collaborations,
    internal_learning,
    internal_meetings,
    internal_policy,
    me_memory,
    suggestions,
    worklogs,
)
from tests.conftest import APP_URL
from tests.test_agent_collaborations import seed_fill
from tests.test_candidates import seed_action, seed_decision
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
from tests.test_notifications import _HEADER_SCHEMA
from tests.test_suggestions import seed_suggestion, seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

AGENT_SECRET = "a" * 40
FEEDBACK_FIELDS = {
    "header",
    "feedback_id",
    "user_id",
    "meeting_id",
    "agent",
    "kind",
    "candidate_id",
    "action",
    "confidence",
    "content",
    "edit_json",
}

# Verbatim from mytheus-events schemas/agent-feedback/v1.avsc (docs trimmed): every payload the
# outbox stores must serialize against it, or the relay dead-letters it.
_UUID = {"type": "string", "logicalType": "uuid"}
_FEEDBACK_SCHEMA = fastavro.parse_schema(
    {
        "type": "record",
        "name": "AgentFeedback",
        "namespace": "io.nexus.events",
        "fields": [
            {"name": "header", "type": _HEADER_SCHEMA},
            {"name": "feedback_id", "type": _UUID},
            {"name": "user_id", "type": _UUID},
            {"name": "meeting_id", "type": ["null", _UUID], "default": None},
            {"name": "agent", "type": "string"},
            {"name": "kind", "type": "string"},
            {"name": "candidate_id", "type": _UUID},
            {"name": "action", "type": "string"},
            {"name": "confidence", "type": ["null", "float"], "default": None},
            {"name": "content", "type": ["null", "string"], "default": None},
            {"name": "edit_json", "type": ["null", "string"], "default": None},
        ],
    }
)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"agent-runtime-service={AGENT_SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-learning-test")
    for module in (
        candidates,
        suggestions,
        collaborations,
        worklogs,
        me_memory,
        internal_policy,
        internal_learning,
        meeting_screens,
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


def internal(org_id: uuid.UUID) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": AGENT_SECRET,
        "X-Mytheus-Org": str(org_id),
    }


def meeting_with(t: Tenant, *people: Tenant) -> uuid.UUID:
    m = seed_meeting(t.org_id)
    add_participant(m, t.org_id, t.user_id)
    for p in people:
        add_participant(m, t.org_id, p.user_id)
    return m


def feedback_rows(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT user_id, meeting_id, agent, kind, candidate_id, action, confidence, content, "
        "edit_diff FROM agent_feedback WHERE org_id = %(o)s ORDER BY created_at, id",
        {"o": str(org_id)},
    )


def feedback_events(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
        "AND topic = 'agent-feedback' ORDER BY id",
        {"o": str(org_id)},
    )


def seed_feedback(
    t: Tenant,
    agent: str,
    action: str,
    n: int,
    *,
    user: uuid.UUID | None = None,
    kind: str = "decision",
    meeting_id: uuid.UUID | None = None,
    content: str | None = "Ship it",
    age_days: float = 0.0,
) -> None:
    for _ in range(n):
        _sql(
            "INSERT INTO agent_feedback (org_id, user_id, meeting_id, agent, kind, candidate_id, "
            "action, content, created_at) VALUES (%(o)s, %(u)s, %(m)s, %(a)s, %(k)s, "
            "gen_random_uuid(), %(act)s, %(c)s, now() - make_interval(secs => %(age)s))",
            {
                "o": str(t.org_id),
                "u": str(user or t.user_id),
                "m": str(meeting_id) if meeting_id else None,
                "a": agent,
                "k": kind,
                "act": action,
                "c": content,
                "age": age_days * 86400,
            },
        )


async def recalibrate(t: Tenant, agent: str, user: uuid.UUID | None = None) -> None:
    from libs.shared.tenancy import set_rls

    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, t.org_id)  # type: ignore[arg-type]
            await learning.recalibrate(conn, t.org_id, agent, user or t.user_id)  # type: ignore[arg-type]
    finally:
        await engine.dispose()


# --- pure ---------------------------------------------------------------------------------------


def test_calibration_is_bounded_needs_samples_and_is_quantised():
    cs = learning.calibrated_surface
    assert cs(0.7, 5, 5, 10, 20) is None  # too few items
    assert cs(0.7, 0, 0, 30, 20) is None
    high = cs(0.7, 0.0, 100.0, 100, 20)  # everything dismissed
    low = cs(0.7, 100.0, 0.0, 100, 20)  # everything confirmed
    assert high is not None and low is not None
    assert 0.7 < high <= learning.SURFACE_CEIL and learning.SURFACE_FLOOR <= low < 0.7
    assert abs(high / learning.STEP - round(high / learning.STEP)) < 1e-6
    # at the target dismiss share nothing moves
    assert cs(0.7, 65.0, 35.0, 100, 20) == 0.7
    # an admin's stricter base is the starting point, and stays reachable
    assert cs(0.9, 0.0, 100.0, 100, 20) == 0.9
    assert cs(0.9, 100.0, 0.0, 100, 20) == pytest.approx(0.8)


def test_redaction_and_edit_diff():
    assert learning.redact("mail  bob@acme.com or +1 (555) 123-4567 now") == (
        "mail [REDACTED_EMAIL] or [REDACTED_PHONE] now"
    )
    assert learning.redact("x" * 400) is not None and len(learning.redact("x" * 400)) == 280
    assert learning.redact("   ") is None
    assert learning.edit_diff({"content": "a", "due_date": None}, {"content": "b"}) == {
        "content": {"before": "a", "after": "b"}
    }
    assert learning.edit_diff({"content": "a"}, {"content": "a"}) == {}


def test_topics_come_from_the_vocabulary_first_then_key_phrases():
    vocab = [("Project Falcon", ["Falcon"]), ("billing", [])]
    assert pm.extract_topics("Move the falcon launch to May", vocab) == ["Project Falcon"]
    phrases = pm.extract_topics("Migrate the payments ledger to Postgres by Friday", [])
    assert phrases[0] == "payments ledger" and len(phrases) == pm.MAX_PHRASES_PER_ITEM
    assert all(p.lower() not in {"the", "friday"} for p in phrases)
    now = datetime.now(UTC)
    topics = pm.merge_topics([], ["Pricing"], 1.0, [], now)
    topics = pm.merge_topics(topics, ["pricing", "Hiring"], 1.0, ["hiring"], now)
    assert [(t["label"], t["mentions"]) for t in topics] == [("Pricing", 2)]
    later = pm.merge_topics(topics, [], 0.0, [], now + timedelta(days=30))
    assert later[0]["weight"] == pytest.approx(1.0, abs=0.01)  # one half-life: 2.0 -> 1.0
    assert pm.merge_topics(topics, ["Pricing"], -5.0, [], now) == []


def test_recurring_questions_merge_near_duplicates():
    now = datetime.now(UTC)
    qs = pm.merge_question([], "Who signs the vendor contract?", now)
    qs = pm.merge_question(qs, "who signs the vendor contract", now)
    qs = pm.merge_question(qs, "What is the hiring budget?", now)
    ranked = pm.ranked_questions(qs, 5)
    assert [(q["text"].lower().rstrip("?"), q["count"]) for q in ranked][0] == (
        "who signs the vendor contract",
        2,
    )


# --- feedback records ---------------------------------------------------------------------------


def test_confirm_dismiss_and_edit_record_feedback_with_the_avro_fields(client):
    t = seed_tenant()
    m = meeting_with(t)
    d, a = seed_decision(t, m), seed_action(t, m)
    sign_in(t)
    assert client.patch(f"/api/v1/decisions/{d}", json={"content": "Ship on May 2"}).is_success
    assert client.patch(f"/api/v1/decisions/{d}", json={"content": "Ship on May 2"}).is_success
    assert client.post(f"/api/v1/decisions/{d}/confirm").is_success
    assert client.post(f"/api/v1/decisions/{d}/confirm").is_success  # repeat: nothing
    assert client.post(f"/api/v1/actions/{a}/dismiss").is_success
    rows = feedback_rows(t.org_id)
    assert [(r["agent"], r["kind"], r["action"]) for r in rows] == [
        ("DecisionAgent", "decision", "edit"),
        ("DecisionAgent", "decision", "confirm"),
        ("ActionAgent", "action", "dismiss"),
    ]
    assert rows[0]["edit_diff"] == {"content": {"before": "Decide", "after": "Ship on May 2"}}
    assert rows[1]["content"] == "Ship on May 2" and str(rows[1]["meeting_id"]) == str(m)
    events = feedback_events(t.org_id)
    assert len(events) == 3
    for ev in events:
        payload = ev["payload"]
        assert set(payload) == FEEDBACK_FIELDS
        assert ev["idempotency_key"] == f"feedback:{payload['feedback_id']}"
        assert payload["user_id"] == str(t.user_id)
        fastavro.validation.validate(payload, _FEEDBACK_SCHEMA, raise_errors=True)
    assert json.loads(events[0]["payload"]["edit_json"])["content"]["after"] == "Ship on May 2"


def test_a_rolled_back_confirm_records_no_feedback(client, monkeypatch):
    t = seed_tenant()
    m = meeting_with(t)
    d = seed_decision(t, m)

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("audit down")

    monkeypatch.setattr(candidates, "audit", boom)
    sign_in(t)
    try:
        assert client.post(f"/api/v1/decisions/{d}/confirm").status_code >= 500
    except RuntimeError:
        pass
    assert feedback_rows(t.org_id) == [] and feedback_events(t.org_id) == []


def test_suggestion_fill_and_note_feedback(client):
    t = seed_tenant()
    m = meeting_with(t)
    s1 = seed_suggestion(t, m, question="Who owns the pricing page?")
    s2 = seed_suggestion(t, m)
    fill = seed_fill(t, m, content="Pricing moved to Q3.")
    note = _sql(
        "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, relevance_score, "
        "confidence, created_by_agent, surfaced) VALUES (%(o)s, %(m)s, %(u)s, 'w', "
        "'- You agreed to draft the pricing FAQ', 0.8, 0.9, true, true) RETURNING id",
        {"o": str(t.org_id), "m": str(m), "u": str(t.user_id)},
    )[0]["id"]
    sign_in(t)
    assert client.post(f"/api/v1/suggestions/{s1}/save").is_success
    assert client.post(f"/api/v1/suggestions/{s1}/ask").is_success
    assert client.post(f"/api/v1/suggestions/{s2}/dismiss").is_success
    assert client.post(f"/api/v1/context-fills/{fill}/dismiss").is_success
    assert client.post(f"/api/v1/context-fills/{fill}/dismiss").is_success  # repeat
    r = client.post(f"/api/v1/worklogs/{note}/feedback", json={"useful": True})
    assert r.status_code == 200 and r.json()["useful"] is True
    assert client.post(f"/api/v1/worklogs/{note}/feedback", json={"useful": True}).is_success
    assert client.post(f"/api/v1/worklogs/{note}/feedback", json={"useful": False}).is_success
    assert [(r["agent"], r["kind"], r["action"]) for r in feedback_rows(t.org_id)] == [
        ("QuestionAgent", "suggestion", "save"),
        ("QuestionAgent", "suggestion", "ask"),
        ("QuestionAgent", "suggestion", "dismiss"),
        ("ContextGapAgent", "context_fill", "dismiss"),
        ("PersonalListenerAgent", "note", "confirm"),
        ("PersonalListenerAgent", "note", "dismiss"),
    ]
    # someone else's note is a 404, and so is another tenant's
    other = seed_user(t)
    sign_in(other)
    assert client.post(f"/api/v1/worklogs/{note}/feedback", json={"useful": True}).status_code == (
        404
    )


# --- calibration: one threshold source ----------------------------------------------------------


async def test_workspace_calibration_moves_the_reload_and_the_policy_together(client):
    t = seed_tenant()
    m = meeting_with(t)
    seed_feedback(t, "DecisionAgent", "dismiss", 30)
    await recalibrate(t, "DecisionAgent")
    body = client.get("/internal/agent-policy", headers=internal(t.org_id)).json()
    surface = body["agent_confidence_overrides"]["DecisionAgent"]["surface"]
    assert 0.7 < surface <= learning.SURFACE_CEIL
    assert body["calibration"]["DecisionAgent"]["samples"] == 30
    assert body["calibration"]["DecisionAgent"]["dismiss_rate"] == 1.0
    assert body["user_overrides"] == {}  # decisions are one card for everyone
    below = seed_decision(t, m, confidence=round(surface - 0.01, 3))
    above = seed_decision(t, m, confidence=round(surface + 0.01, 3))
    sign_in(t)
    served = {d["id"] for d in client.get(f"/api/v1/meetings/{m}/candidates").json()["decisions"]}
    assert served == {str(above)} and str(below) not in served


async def test_an_admin_override_is_the_base_calibration_moves_from(client):
    t = seed_tenant()
    _sql(
        "INSERT INTO tenant_settings (org_id, agent_confidence_overrides) VALUES (%(o)s, "
        '\'{"ActionAgent": {"surface": 0.8, "silent": 0.5}}\'::jsonb)',
        {"o": str(t.org_id)},
    )
    seed_feedback(t, "ActionAgent", "confirm", 40, kind="action")
    await recalibrate(t, "ActionAgent")
    band = client.get("/internal/agent-policy", headers=internal(t.org_id)).json()[
        "agent_confidence_overrides"
    ]["ActionAgent"]
    assert learning.SURFACE_FLOOR <= band["surface"] < 0.8
    assert band["silent"] == 0.5


async def test_a_persons_calibration_moves_only_their_suggestions(client):
    t = seed_tenant()
    bob = seed_user(t)
    m = meeting_with(t, bob)
    seed_feedback(t, "QuestionAgent", "dismiss", 12, kind="suggestion")
    await recalibrate(t, "QuestionAgent")
    body = client.get("/internal/agent-policy", headers=internal(t.org_id)).json()
    mine = body["user_overrides"][str(t.user_id)]["QuestionAgent"]["surface"]
    assert mine > 0.7 and str(bob.user_id) not in body["user_overrides"]
    seed_suggestion(t, m, confidence=0.72, question="Mine at 0.72")
    bob_s = uuid.uuid4()
    _sql(
        "INSERT INTO agent_suggestions (id, org_id, user_id, meeting_id, question, priority, "
        "status, confidence) VALUES (%(id)s, %(o)s, %(u)s, %(m)s, 'Bob at 0.72', 2, 'open', 0.72)",
        {"id": str(bob_s), "o": str(t.org_id), "u": str(bob.user_id), "m": str(m)},
    )
    sign_in(t)
    assert client.get(f"/api/v1/suggestions?meeting_id={m}").json()["open"] == []
    sign_in(bob)
    assert [
        s["question"] for s in client.get(f"/api/v1/suggestions?meeting_id={m}").json()["open"]
    ] == ["Bob at 0.72"]


async def test_old_feedback_decays_and_falls_out_of_the_window(client):
    t = seed_tenant()
    seed_feedback(t, "DecisionAgent", "dismiss", 30, age_days=120)  # outside the window
    await recalibrate(t, "DecisionAgent")
    body = client.get("/internal/agent-policy", headers=internal(t.org_id)).json()
    assert body["agent_confidence_overrides"].get(
        "DecisionAgent", {"surface": 0.7, "silent": 0.4}
    ) == {
        "surface": 0.7,
        "silent": 0.4,
    }


# --- guidance -----------------------------------------------------------------------------------


def test_guidance_scopes_and_the_confidential_rule(client):
    t = seed_tenant()
    bob = seed_user(t)
    open_m, secret_m = meeting_with(t), meeting_with(t)
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(secret_m)})
    seed_feedback(t, "DecisionAgent", "confirm", 1, meeting_id=open_m, content="Adopt Postgres")
    seed_feedback(t, "DecisionAgent", "dismiss", 1, meeting_id=secret_m, content="Acquire Acme")
    seed_feedback(t, "DecisionAgent", "dismiss", 1, meeting_id=open_m, content="Lunch at noon")
    seed_feedback(
        t, "QuestionAgent", "ask", 1, kind="suggestion", meeting_id=open_m, content="Mine?"
    )
    seed_feedback(
        t,
        "QuestionAgent",
        "dismiss",
        1,
        kind="suggestion",
        user=bob.user_id,
        meeting_id=open_m,
        content="Bob's?",
    )
    body = client.get("/internal/agent-guidance", headers=internal(t.org_id)).json()["agents"]
    assert set(body) == {"DecisionAgent"}
    texts = {(e["action"], e["text"]) for e in body["DecisionAgent"]["examples"]}
    assert texts == {("confirm", "Adopt Postgres"), ("dismiss", "Lunch at noon")}
    assert body["DecisionAgent"]["scope"] == "workspace"
    mine = client.get(
        f"/internal/agent-guidance?user_id={t.user_id}", headers=internal(t.org_id)
    ).json()["agents"]
    assert mine["QuestionAgent"] == {
        "scope": "person",
        "examples": [{"action": "confirm", "text": "Mine?", "before": None}],
    }
    other = seed_tenant()
    assert client.get("/internal/agent-guidance", headers=internal(other.org_id)).json() == {
        "agents": {}
    }
    assert client.get("/internal/agent-guidance").status_code == 401


def test_edits_become_before_after_examples(client):
    t = seed_tenant()
    m = meeting_with(t)
    d = seed_decision(t, m)
    sign_in(t)
    client.patch(f"/api/v1/decisions/{d}", json={"content": "Ship the beta on May 2"})
    body = client.get("/internal/agent-guidance", headers=internal(t.org_id)).json()["agents"]
    assert {"action": "edit", "text": "Ship the beta on May 2", "before": "Decide"} in body[
        "DecisionAgent"
    ]["examples"]


# --- memory -------------------------------------------------------------------------------------


def event(t: Tenant, **payload: Any) -> dict[str, Any]:
    fid = str(uuid.uuid4())
    return {
        "header": {"org_id": str(t.org_id), "idempotency_key": f"feedback:{fid}"},
        "feedback_id": fid,
        "user_id": str(t.user_id),
        "meeting_id": None,
        "agent": "DecisionAgent",
        "kind": "decision",
        "candidate_id": str(uuid.uuid4()),
        "action": "confirm",
        "confidence": 0.9,
        "content": "Move Project Falcon to the new billing stack",
        "edit_json": None,
        **payload,
    }


async def consume(value: dict[str, Any], redis: Any | None = None) -> bool:
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            return await pm.handle_agent_feedback(
                conn, redis or fakeredis.aioredis.FakeRedis(), value  # type: ignore[arg-type]
            )
    finally:
        await engine.dispose()


async def test_the_consumer_learns_topics_and_questions_once(client):
    t = seed_tenant()
    _sql(
        "INSERT INTO org_glossary_terms (org_id, term, aliases) VALUES "
        "(%(o)s, 'Project Falcon', ARRAY['Falcon'])",
        {"o": str(t.org_id)},
    )
    first = event(t)
    redis = fakeredis.aioredis.FakeRedis()
    assert await consume(first, redis) is True
    assert await consume(first, redis) is False  # redelivery
    assert await consume(first) is False  # the inbox, after the Redis key is gone
    asked = "Who signs off the Falcon budget?"
    for _ in range(2):
        await consume(
            event(t, agent="QuestionAgent", kind="suggestion", action="ask", content=asked)
        )
    sign_in(t)
    body = client.get("/api/v1/me/memory").json()
    assert body["topics"][0]["label"] == "Project Falcon"
    assert body["topics"][0]["mentions"] == 3
    assert body["recurring_questions"][0] == {
        "text": asked,
        "count": 2,
        "last_at": body["recurring_questions"][0]["last_at"],
    }
    assert {c["agent"] for c in body["calibration"]} == learning.USER_SCOPED_AGENTS


async def test_a_confirmed_action_teaches_its_assignee(client):
    t = seed_tenant()
    bob = seed_user(t)
    m = meeting_with(t, bob)
    a = seed_action(t, m)
    _sql(
        "UPDATE action_items SET assignee_id = %(b)s WHERE id = %(a)s",
        {"b": str(bob.user_id), "a": str(a)},
    )
    await consume(
        event(
            t,
            kind="action",
            agent="ActionAgent",
            candidate_id=str(a),
            content="Draft the SOC2 report",
        )
    )
    bob_mem = _sql(
        "SELECT topics FROM person_memory WHERE user_id = %(u)s", {"u": str(bob.user_id)}
    )
    assert bob_mem and bob_mem[0]["topics"]


async def test_memory_is_owner_only_editable_and_forgettable(client):
    t = seed_tenant()
    bob = seed_user(t)
    m = meeting_with(t, bob)
    await consume(event(t, content="Renegotiate the Stripe contract"))
    sign_in(bob)
    assert client.get("/api/v1/me/memory").json()["topics"] == []  # Bob sees only his own
    sign_in(t)
    learned = [x["label"] for x in client.get("/api/v1/me/memory").json()["topics"]]
    assert learned
    body = client.patch(
        "/api/v1/me/memory", json={"forget_topics": learned, "pin_topics": ["Hiring plan"]}
    ).json()
    assert [x["label"] for x in body["topics"]] == ["Hiring plan"]
    assert body["topics"][0]["pinned"] is True and body["muted_topics"] == learned
    await consume(event(t, content="Renegotiate the Stripe contract"))  # muted: not relearned
    relearned = [x["label"] for x in client.get("/api/v1/me/memory").json()["topics"]]
    assert relearned == ["Hiring plan"]
    assert client.patch("/api/v1/me/memory", json={}).status_code == 422
    # learning off: nothing new is learned and the agents get nothing
    assert (
        client.patch("/api/v1/me/memory", json={"learning_enabled": False}).json()[
            "learning_enabled"
        ]
        is False
    )
    assert await consume(event(t, content="Quarterly roadmap review")) is False
    compact = client.get(f"/internal/person-memory/{t.user_id}", headers=internal(t.org_id)).json()
    assert compact["learning_enabled"] is False and compact["topics"] == []
    # forget everything: the row goes and the text of the feedback is scrubbed
    seed_feedback(t, "DecisionAgent", "confirm", 1, meeting_id=m, content="Secret text")
    assert client.delete("/api/v1/me/memory").status_code == 204
    assert _sql("SELECT 1 FROM person_memory WHERE user_id = %(u)s", {"u": str(t.user_id)}) == []
    assert (
        _sql(
            "SELECT 1 FROM agent_feedback WHERE user_id = %(u)s AND content IS NOT NULL",
            {"u": str(t.user_id)},
        )
        == []
    )
    assert client.get("/api/v1/me/memory").json()["learning_enabled"] is True


async def test_compact_memory_for_the_agents_and_the_brief(client):
    t = seed_tenant()
    m = meeting_with(t)
    a = seed_action(t, m)
    _sql(
        "UPDATE action_items SET status = 'confirmed', assignee_id = %(u)s WHERE id = %(a)s",
        {"u": str(t.user_id), "a": str(a)},
    )
    _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, due_hint, "
        "source_window_id) VALUES (%(o)s, %(u)s, %(m)s, 'Send the deck', 'Friday', 'w1')",
        {"o": str(t.org_id), "u": str(t.user_id), "m": str(m)},
    )
    await consume(event(t, content="Adopt the new onboarding flow"))
    compact = client.get(f"/internal/person-memory/{t.user_id}", headers=internal(t.org_id)).json()
    assert compact["learning_enabled"] is True and compact["topics"]
    assert compact["open_actions"] == [{"content": "Do it", "due": "2026-12-02"}]
    assert compact["open_promises"] == [{"content": "Send the deck", "due": "Friday"}]
    other = seed_tenant()
    assert (
        client.get(
            f"/internal/person-memory/{t.user_id}", headers=internal(other.org_id)
        ).status_code
        == 404
    )
    sign_in(t)
    brief = client.get(f"/bff/meetings/{m}/brief").json()
    assert brief["memory"]["open_actions_total"] == 1
    assert brief["memory"]["open_promises"][0]["content"] == "Send the deck"
    assert brief["memory"]["topics"]


def test_rls_isolates_the_three_tables(client):
    t, other = seed_tenant(), seed_tenant()
    seed_feedback(t, "DecisionAgent", "confirm", 1)
    _sql(
        "INSERT INTO person_memory (org_id, user_id) VALUES (%(o)s, %(u)s)",
        {"o": str(t.org_id), "u": str(t.user_id)},
    )
    _sql(
        "INSERT INTO agent_calibration (org_id, agent, samples) VALUES (%(o)s, 'DecisionAgent', 1)",
        {"o": str(t.org_id)},
    )
    import psycopg2

    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn, conn.cursor() as cur:
            for table in ("agent_feedback", "person_memory", "agent_calibration"):
                cur.execute("SELECT set_config('app.current_org', %s, true)", (str(other.org_id),))
                cur.execute(f"SELECT count(*) FROM {table} WHERE org_id = %s", (str(t.org_id),))
                assert cur.fetchone()[0] == 0, table
                cur.execute("SELECT set_config('app.current_org', '', true)")
                cur.execute(f"SELECT count(*) FROM {table}")
                assert cur.fetchone()[0] == 0, table
    finally:
        conn.close()


def test_the_gateway_routes_my_memory_to_core_api():
    from services.api_gateway.edge import route_for

    assert route_for("/api/v1/me/memory") == "core-api"
    assert route_for("/api/v1/meetings/x") == "core-api"
    assert route_for("/internal/person-memory/x") is None  # never routed publicly
