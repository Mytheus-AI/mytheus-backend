"""ContextGapAgent collaborations and context fills in core-api (P10 contract section 6).

Consumer: one `agent_collaborations` row per collaboration entry, idempotent (a redelivery and a
replay under a new key write nothing more), the fill fields only on the producing row, the band
re-derived from the tenant's overrides (below silent: fill fields dropped, row kept), one audit
row per entry with the right action, the nil meeting tolerated for a responder-side security
entry, an entry naming another requester skipped, a non-JSON payload poison, and `kind` read
the same whether the decoder hands over a str or an enum-like symbol.

Routes: the collaboration log for participants and admins (404 for anyone else, any tenant),
newest first, paginated, never carrying fill content; the caller's own surfaced non-dismissed
fills; dismiss requester-only (404 otherwise), idempotent, audited once. RLS isolation.

Internal: related attendees ranked by shared ended meetings then recency, excluding this
meeting's participants and the user, behind the agent-runtime-service caller token.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from libs.shared.tenancy import set_rls
from services.core_api.consumer import PoisonMessage
from services.core_api.routes import collaborations, internal_agents, internal_meetings
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
from tests.test_suggestions import meeting_with, seed_user, set_overrides
from tests.test_worklogs import apply, result

pytestmark = pytest.mark.usefixtures("migrated_db")

AGENT_SECRET = "a" * 40
RAG_SECRET = "r" * 40
NIL = "00000000-0000-0000-0000-000000000000"


def _swap_db() -> str | None:
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    return previous


def _restore_db(previous: str | None) -> None:
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = _swap_db()
    app = _create_test_app("core-api-collaborations-test")
    app.include_router(collaborations.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    _restore_db(previous)


@pytest.fixture(scope="module")
def internal(migrated_db):
    previous = _swap_db()
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},rag-service={RAG_SECRET}"
    )
    app = create_app("core-api-related-test")
    app.include_router(internal_agents.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    _restore_db(previous)


# --- builders --------------------------------------------------------------------------------


def entry(
    requester: uuid.UUID,
    *,
    hop: str = "peer",
    res: str = "answered",
    responder: str | None = None,
    success: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "correlation_id": str(uuid.uuid4()) if hop != "rag" else None,
        "hop": hop,
        "requester": str(requester),
        "responder": responder or ("rag" if hop == "rag" else f"user:{uuid.uuid4()}"),
        "query": "what did we decide about the vendor contract",
        "query_class": "decision_lineage",
        "target_scope": "same_team" if hop != "rag" else None,
        "result": res,
        "success": success,
        "latency_ms": 412,
        **extra,
    }


def fill_for(e: dict[str, Any], confidence: float = 0.86, **extra: Any) -> dict[str, Any]:
    return {
        "id": e["id"],
        "content": "The team chose Acme last Tuesday, pending legal review.",
        "confidence": confidence,
        "surfaced": confidence > 0.7,
        "rationale": "Bob's agent had the decision from the prior meeting.",
        "hop": e["hop"],
        "responder": e["responder"],
        "citations": [
            {"kind": "decision", "decision_id": str(uuid.uuid4())},
            {"kind": "web", "url": "dropped"},
        ],
        **extra,
    }


def fill_result(
    t: Tenant,
    meeting_id: uuid.UUID | str,
    entries: list[dict[str, Any]],
    fill: dict[str, Any] | None = None,
    *,
    user_id: uuid.UUID | None = None,
    window: str = "w0",
    kind: Any = "context_fill",
) -> dict[str, Any]:
    value = result(
        t,
        meeting_id,  # type: ignore[arg-type]
        (fill or {}).get("confidence", 0.0),
        user_id=user_id,
        window=window,
        agent="ContextGapAgent",
    )
    value["kind"] = kind
    value["content"] = (fill or {}).get("content", "")
    value["payload_json"] = json.dumps(
        {
            "gap": {"query": "vendor contract decision", "query_class": "decision_lineage"},
            "collaborations": entries,
            "fill": fill,
            "prompt": "context_gap_agent@v1",
            "model_tier": "extract",
        }
    )
    return value


def rows(meeting_id: uuid.UUID | str) -> dict[str, dict[str, Any]]:
    return {
        str(r["id"]): r
        for r in _sql(
            "SELECT * FROM agent_collaborations WHERE meeting_id = %(m)s", {"m": str(meeting_id)}
        )
    }


def entry_audits(org_id: uuid.UUID) -> list[tuple[str, str, str]]:
    return sorted(
        (str(r["target_id"]), r["action"], r["actor_type"])
        for r in _sql(
            "SELECT target_id, action, actor_type FROM audit_logs WHERE org_id = %(o)s "
            "AND target_type = 'agent_collaboration' AND action LIKE 'a2a.request.%%'",
            {"o": str(org_id)},
        )
    )


def seed_fill(
    t: Tenant,
    meeting_id: uuid.UUID,
    *,
    surfaced: bool = True,
    dismissed: bool = False,
    content: str | None = "Context.",
    responder: str = "rag",
) -> uuid.UUID:
    cid = uuid.uuid4()
    _sql(
        "INSERT INTO agent_collaborations (id, org_id, meeting_id, requester, responder, hop, "
        "query, query_class, result, success, latency_ms, fill_content, confidence, surfaced, "
        "rationale, dismissed_at) VALUES (%(i)s, %(o)s, %(m)s, %(u)s, %(r)s, 'rag', 'q', "
        "'meeting_context', 'rag_hit', true, 30, %(c)s, 0.9, %(s)s, 'why', "
        "CASE WHEN %(d)s THEN now() END)",
        {
            "i": str(cid),
            "o": str(t.org_id),
            "m": str(meeting_id),
            "u": str(t.user_id),
            "r": responder,
            "c": content,
            "s": surfaced,
            "d": dismissed,
        },
    )
    return cid


# --- consumer --------------------------------------------------------------------------------


async def test_one_row_per_entry_with_the_fill_only_on_the_producing_row():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    denied = entry(t.user_id, res="authz_denied", success=False)
    answered = entry(t.user_id)
    rag = entry(t.user_id, hop="rag", res="rag_hit")
    fill = fill_for(answered)
    assert await apply(fill_result(t, m, [denied, answered, rag], fill)) is True

    got = rows(m)
    assert set(got) == {denied["id"], answered["id"], rag["id"]}
    a = got[answered["id"]]
    assert a["fill_content"] == fill["content"]
    assert round(a["confidence"], 2) == 0.86 and a["surfaced"] is True
    assert a["rationale"] == fill["rationale"]
    assert [c["kind"] for c in a["citations"]] == ["decision"]  # unknown kind dropped
    assert a["requester"] == str(t.user_id) and a["responder"] == answered["responder"]
    assert a["correlation_id"] == answered["correlation_id"]
    assert a["window_id"] == f"{m}:w0" and a["latency_ms"] == 412
    for other in (denied["id"], rag["id"]):
        r = got[other]
        assert r["fill_content"] is None and r["confidence"] is None
        assert r["surfaced"] is False and r["citations"] == []
    assert got[denied["id"]]["success"] is False and got[denied["id"]]["result"] == "authz_denied"
    assert got[rag["id"]]["target_scope"] is None and got[rag["id"]]["responder"] == "rag"
    assert _sql("SELECT 1 FROM outbox WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []


async def test_redelivery_and_replay_write_no_duplicate_rows_or_audits():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = fill_result(t, m, [entry(t.user_id), entry(t.user_id, res="deadline_miss")])
    assert await apply(value) is True
    assert await apply(value) is False  # same idempotency key
    replay = json.loads(json.dumps(value))
    replay["header"]["idempotency_key"] = f"replay:{uuid.uuid4()}"
    assert await apply(replay) is False  # the entry ids are the authority
    assert len(rows(m)) == 2
    assert len(entry_audits(t.org_id)) == 2


async def test_one_audit_row_per_entry_with_the_action_for_its_result():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    by_result = {
        res: entry(t.user_id, res=res, hop="rag" if res.startswith("rag") else "peer")
        for res in (
            "answered",
            "no_answer",
            "deadline_miss",
            "rag_hit",
            "rag_miss",
            "authz_denied",
            "org_spoof",
            "foreign_org",
        )
    }
    assert await apply(fill_result(t, m, list(by_result.values()))) is True
    expected = {
        "answered": "a2a.request.allowed",
        "no_answer": "a2a.request.allowed",
        "deadline_miss": "a2a.request.allowed",
        "rag_hit": "a2a.request.allowed",
        "rag_miss": "a2a.request.allowed",
        "authz_denied": "a2a.request.denied",
        "org_spoof": "a2a.request.blocked",
        "foreign_org": "a2a.request.blocked",
    }
    assert entry_audits(t.org_id) == sorted(
        (e["id"], expected[res], "agent") for res, e in by_result.items()
    )


async def test_the_band_is_re_derived_from_the_tenant_thresholds():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    mid_band, low = entry(t.user_id), entry(t.user_id)
    assert await apply(fill_result(t, m, [mid_band], fill_for(mid_band, 0.55), window="w1"))
    assert await apply(fill_result(t, m, [low], fill_for(low, 0.35, surfaced=True), window="w2"))
    got = rows(m)
    assert got[mid_band["id"]]["fill_content"] and got[mid_band["id"]]["surfaced"] is False
    # below silent: the row is kept, the fill is not written (payload's surfaced ignored)
    assert got[low["id"]]["fill_content"] is None and got[low["id"]]["surfaced"] is False

    t2 = seed_tenant()
    m2 = seed_meeting(t2.org_id)
    set_overrides(t2, {"ContextGapAgent": {"surface": 0.5, "silent": 0.3}})
    e1, e2 = entry(t2.user_id), entry(t2.user_id)
    assert await apply(fill_result(t2, m2, [e1], fill_for(e1, 0.55), window="w1"))
    assert await apply(fill_result(t2, m2, [e2], fill_for(e2, 0.35), window="w2"))
    got2 = rows(m2)
    assert got2[e1["id"]]["surfaced"] is True
    assert got2[e2["id"]]["fill_content"] and got2[e2["id"]]["surfaced"] is False


async def test_a_fill_pointing_at_no_entry_writes_no_fill():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    e = entry(t.user_id)
    stray = fill_for(entry(t.user_id))
    assert await apply(fill_result(t, m, [e], stray)) is True
    assert rows(m)[e["id"]]["fill_content"] is None


async def test_bad_entries_are_skipped_and_the_rest_applied():
    t = seed_tenant()
    other = seed_user(t)
    m = seed_meeting(t.org_id)
    good = entry(t.user_id)
    bad = [
        entry(other.user_id),  # names someone other than the result's user
        entry(t.user_id, id="nope"),
        entry(t.user_id, hop="carrier_pigeon"),
        entry(t.user_id, res="maybe"),
        entry(t.user_id, success="yes"),
        entry(t.user_id, latency_ms=-1),
        entry(t.user_id, latency_ms=True),
        entry(t.user_id, query="   "),
        {**entry(t.user_id), "responder": ""},
        "not an entry",
    ]
    assert await apply(fill_result(t, m, [*bad, good])) is True
    assert set(rows(m)) == {good["id"]}


async def test_a_security_entry_with_the_nil_meeting_is_logged():
    t = seed_tenant()
    spoof = entry(t.user_id, res="org_spoof", success=False, hop="peer")
    assert await apply(fill_result(t, NIL, [spoof])) is True
    assert set(rows(NIL)) >= {spoof["id"]}
    assert rows(NIL)[spoof["id"]]["org_id"] == str(t.org_id)


async def test_another_tenants_meeting_or_user_is_poison():
    a, b = seed_tenant(), seed_tenant()
    m_b = seed_meeting(b.org_id)
    with pytest.raises(PoisonMessage):
        await apply(fill_result(a, m_b, [entry(a.user_id)]))
    m_a = seed_meeting(a.org_id)
    with pytest.raises(PoisonMessage):
        await apply(fill_result(a, m_a, [entry(b.user_id)], user_id=b.user_id))
    assert rows(m_a) == {} and rows(m_b) == {}


async def test_a_non_json_payload_is_poison():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = fill_result(t, m, [])
    value["payload_json"] = "{nope"
    with pytest.raises(PoisonMessage):
        await apply(value)
    value["payload_json"] = None
    value["header"]["idempotency_key"] = f"k:{uuid.uuid4()}"
    with pytest.raises(PoisonMessage):
        await apply(value)


class _Symbol:
    """What an Avro enum looks like if a decoder hands over an object, not a str."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return self.name


async def test_kind_is_read_the_same_as_a_str_or_an_enum_symbol():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    e = entry(t.user_id)
    assert await apply(fill_result(t, m, [e], kind=_Symbol("context_fill"))) is True
    assert set(rows(m)) == {e["id"]}


# --- RLS -------------------------------------------------------------------------------------


async def test_rls_isolates_agent_collaborations_and_fails_closed():
    a, b = seed_tenant(), seed_tenant()
    seed_fill(a, seed_meeting(a.org_id))
    seed_fill(b, seed_meeting(b.org_id))
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {
                r[0] for r in await conn.execute(text("SELECT org_id FROM agent_collaborations"))
            }
            assert orgs == {a.org_id}
        async with engine.connect() as conn, conn.begin():
            n = (await conn.execute(text("SELECT count(*) FROM agent_collaborations"))).scalar()
            assert n == 0
    finally:
        await engine.dispose()


# --- GET /meetings/{id}/collaborations -------------------------------------------------------


async def test_the_log_is_for_participants_and_admins_and_404_for_anyone_else(client):
    me = seed_tenant()
    bob = seed_user(me)
    _sql("UPDATE users SET email = 'bob@example.com' WHERE id = %(u)s", {"u": str(bob.user_id)})
    m = meeting_with(me, bob)
    peer = entry(me.user_id, responder=f"user:{bob.user_id}")
    rag = entry(me.user_id, hop="rag", res="rag_miss", success=False)
    assert await apply(fill_result(me, m, [peer, rag], fill_for(peer)))

    sign_in(bob)
    resp = client.get(f"/api/v1/meetings/{m}/collaborations")
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert {i["id"] for i in items} == {peer["id"], rag["id"]}
    by_id = {i["id"]: i for i in items}
    assert by_id[peer["id"]]["responder_label"] == "bob@example.com"
    assert by_id[peer["id"]]["requester_id"] == str(me.user_id)
    assert by_id[peer["id"]]["requester_name"] == "u@example.com"
    assert by_id[rag["id"]]["responder_label"] == "Knowledge base"
    assert set(items[0]) == {
        "id",
        "created_at",
        "hop",
        "requester_id",
        "requester_name",
        "responder",
        "responder_label",
        "query_class",
        "target_scope",
        "result",
        "success",
        "latency_ms",
    }  # never fill_content or citations

    admin = seed_user(me, "admin")  # not a participant, but an admin
    sign_in(admin)
    assert len(client.get(f"/api/v1/meetings/{m}/collaborations").json()["items"]) == 2

    outsider = seed_user(me)  # a member who is not a participant
    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{m}/collaborations").status_code == 404

    stranger = seed_tenant("admin")
    sign_in(stranger)
    assert client.get(f"/api/v1/meetings/{m}/collaborations").status_code == 404


async def test_a_related_meeting_responder_is_named_only_to_the_requester_and_admins(client):
    me = seed_tenant()
    bob = seed_user(me)  # in this meeting
    carol = seed_user(me)  # met me recently; NOT in this meeting
    _sql("UPDATE users SET email = 'carol@example.com' WHERE id = %(u)s", {"u": str(carol.user_id)})
    m = meeting_with(me, bob)
    related = entry(me.user_id, hop="related_meeting", responder=f"user:{carol.user_id}")
    peer = entry(me.user_id, responder=f"user:{bob.user_id}")
    assert await apply(fill_result(me, m, [related, peer]))

    def seen_by(who: Tenant) -> dict[str, dict[str, Any]]:
        sign_in(who)
        resp = client.get(f"/api/v1/meetings/{m}/collaborations")
        assert resp.status_code == 200, resp.text
        return {i["id"]: i for i in resp.json()["items"]}

    for who in (me, seed_user(me, "admin"), seed_user(me, "owner")):
        rows_ = seen_by(who)
        assert rows_[related["id"]]["responder"] == f"user:{carol.user_id}"
        assert rows_[related["id"]]["responder_label"] == "carol@example.com"

    as_bob = seen_by(bob)  # another participant
    assert as_bob[related["id"]]["responder"] == "user"
    assert as_bob[related["id"]]["responder_label"] == "A colleague"
    assert str(carol.user_id) not in json.dumps(as_bob)
    assert "carol@example.com" not in json.dumps(as_bob)
    # the peer hop names a participant, so it stays visible to every participant
    assert as_bob[peer["id"]]["responder"] == f"user:{bob.user_id}"


async def test_the_log_is_newest_first_and_paginates(client):
    me = seed_tenant()
    m = meeting_with(me)
    ids = []
    for n in range(5):
        e = entry(me.user_id)
        assert await apply(fill_result(me, m, [e], window=f"w{n}"))
        ids.append(e["id"])
    sign_in(me)
    first = client.get(f"/api/v1/meetings/{m}/collaborations?limit=2").json()
    second = client.get(
        f"/api/v1/meetings/{m}/collaborations?limit=2&cursor={first['next_cursor']}"
    ).json()
    third = client.get(
        f"/api/v1/meetings/{m}/collaborations?limit=2&cursor={second['next_cursor']}"
    ).json()
    seen = [i["id"] for page in (first, second, third) for i in page["items"]]
    assert seen == list(reversed(ids))
    assert third["next_cursor"] is None
    resp = client.get(f"/api/v1/meetings/{m}/collaborations?cursor=garbage")
    assert resp.status_code == 400


# --- context fills ---------------------------------------------------------------------------


async def test_context_fills_are_my_surfaced_non_dismissed_ones_only(client):
    me = seed_tenant()
    bob = seed_user(me)
    m = meeting_with(me, bob)
    shown = seed_fill(me, m, responder=f"user:{bob.user_id}")
    seed_fill(me, m, surfaced=False)
    seed_fill(me, m, dismissed=True)
    seed_fill(me, m, content=None)  # a collaboration with no fill
    seed_fill(bob, m)  # Bob's own fill
    seed_fill(me, meeting_with(me))  # another meeting
    sign_in(me)
    resp = client.get(f"/api/v1/meetings/{m}/context-fills")
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [i["id"] for i in items] == [str(shown)]
    assert items[0]["responder_label"] == "v@example.com"  # Bob, from seed_user
    assert set(items[0]) == {
        "id",
        "content",
        "confidence",
        "rationale",
        "hop",
        "responder",
        "responder_label",
        "citations",
        "created_at",
    }

    outsider = seed_user(me)
    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{m}/context-fills").status_code == 404
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/context-fills").status_code == 404


async def test_a_consumed_fill_reaches_its_requester_and_nobody_else(client):
    me = seed_tenant()
    bob = seed_user(me)
    m = meeting_with(me, bob)
    e = entry(me.user_id, responder=f"user:{bob.user_id}")
    assert await apply(fill_result(me, m, [e], fill_for(e)))
    sign_in(me)
    assert [i["id"] for i in client.get(f"/api/v1/meetings/{m}/context-fills").json()["items"]] == [
        e["id"]
    ]
    sign_in(bob)
    assert client.get(f"/api/v1/meetings/{m}/context-fills").json()["items"] == []


def test_dismiss_is_requester_only_idempotent_and_audited_once(client):
    me = seed_tenant()
    bob = seed_user(me)
    m = meeting_with(me, bob)
    fid = seed_fill(me, m)
    no_fill = seed_fill(me, m, content=None)

    sign_in(bob)  # same meeting, not the requester
    assert client.post(f"/api/v1/context-fills/{fid}/dismiss").status_code == 404
    sign_in(seed_tenant())
    assert client.post(f"/api/v1/context-fills/{fid}/dismiss").status_code == 404

    sign_in(me)
    assert client.post(f"/api/v1/context-fills/{no_fill}/dismiss").status_code == 404
    first = client.post(f"/api/v1/context-fills/{fid}/dismiss")
    assert first.status_code == 200, first.text
    second = client.post(f"/api/v1/context-fills/{fid}/dismiss")
    assert second.status_code == 200
    assert first.json()["dismissed_at"] == second.json()["dismissed_at"]
    assert client.get(f"/api/v1/meetings/{m}/context-fills").json()["items"] == []
    audits = _sql(
        "SELECT action, actor_id FROM audit_logs WHERE target_id = %(t)s", {"t": str(fid)}
    )
    assert audits == [{"action": "context_fill.dismissed", "actor_id": str(me.user_id)}]


# --- internal related attendees --------------------------------------------------------------


def _ended(t: Tenant, *people: uuid.UUID, ago_days: int) -> uuid.UUID:
    m = seed_meeting(t.org_id)
    for p in people:
        add_participant(m, t.org_id, p)
    _sql(
        "UPDATE meetings SET status = 'ended', updated_at = now() - make_interval(days => %(d)s) "
        "WHERE id = %(m)s",
        {"m": str(m), "d": ago_days},
    )
    return m


def _headers(org_id: uuid.UUID, service: str = "agent-runtime-service", secret: str = AGENT_SECRET):
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def test_related_attendees_are_ranked_by_shared_meetings_then_recency(internal):
    p = seed_tenant()
    a, b, c, d, here = (seed_user(p).user_id for _ in range(5))
    _ended(p, p.user_id, a, b, ago_days=1)
    _ended(p, p.user_id, a, c, ago_days=2)
    _ended(p, p.user_id, a, here, ago_days=3)
    _ended(p, p.user_id, c, ago_days=30)
    _ended(p, a, b, c, d, ago_days=1)  # P not there: does not count
    live = seed_meeting(p.org_id)  # not ended: does not count
    add_participant(live, p.org_id, p.user_id)
    add_participant(live, p.org_id, d)
    current = meeting_with(p, Tenant(p.org_id, p.clerk_org, here, ""))

    resp = internal.get(
        f"/internal/meetings/{current}/related-attendees",
        params={"user_id": str(p.user_id)},
        headers=_headers(p.org_id),
    )
    assert resp.status_code == 200, resp.text
    # a: 3 shared; c: 2 (newest 2 days ago); b: 1 (1 day ago); here: excluded (in this meeting)
    assert resp.json() == {"user_ids": [str(a), str(c), str(b)]}


def test_related_attendees_caps_at_five_and_only_the_last_ten_meetings(internal):
    p = seed_tenant()
    current = meeting_with(p)
    old = seed_user(p).user_id
    _ended(p, p.user_id, old, ago_days=100)  # the 11th most recent: out of range
    others = [seed_user(p).user_id for _ in range(10)]
    for n, u in enumerate(others):
        _ended(p, p.user_id, u, ago_days=n + 1)
    resp = internal.get(
        f"/internal/meetings/{current}/related-attendees",
        params={"user_id": str(p.user_id)},
        headers=_headers(p.org_id),
    )
    assert resp.json()["user_ids"] == [str(u) for u in others[:5]]


def test_related_attendees_requires_the_agent_runtime_token(internal):
    p = seed_tenant()
    current = meeting_with(p)
    url = f"/internal/meetings/{current}/related-attendees"
    q = {"user_id": str(p.user_id)}
    assert internal.get(url, params=q).status_code == 401
    assert internal.get(url, params=q, headers=_headers(p.org_id, secret="x" * 40)).status_code == (
        401
    )
    rag = _headers(p.org_id, service="rag-service", secret=RAG_SECRET)
    assert internal.get(url, params=q, headers=rag).status_code == 401  # not an allowed caller
    assert internal.get(url, params=q, headers=_headers(p.org_id)).status_code == 200
    # another tenant's meeting or user is 404
    other = seed_tenant()
    assert internal.get(url, params=q, headers=_headers(other.org_id)).status_code == 404
    elsewhere = {"user_id": str(other.user_id)}
    assert internal.get(url, params=elsewhere, headers=_headers(p.org_id)).status_code == 404
