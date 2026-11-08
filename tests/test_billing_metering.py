"""billing-metering-service: Plan 7-9 section 3.1 acceptance checks.

1. An `agent_token` message is metered exactly once, even on redelivery and even when the
   Redis dedupe cache has evicted (the UNIQUE constraint is the backstop).
2. 80% and 95% set their flags once and emit notifications; `state` stays `ok`.
3. Free goes straight `ok -> blocked` at its cap; Pro goes `throttled` at 100% and `blocked`
   at 2x (ADR-0029 D1). Each crossing writes a `billing-events` outbox row.
4. A failed transaction un-marks the dedupe key, so the redelivery is applied.
5. Only `agent_token` is accepted on the topic; anything else is poison.
6. RLS isolation on `usage_events` and `agent_budgets`.
7. Derived `meeting_minute` / `transcription_second` are written once, and the rollup fills
   `usage_aggregates` with the per-tier split.
8. `GET /usage` is admin-only; `GET /plans` lists the catalog.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis
import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.billing_metering_service import budgets, consumer, metering, routes
from tests.conftest import APP_URL, BOOTSTRAP_URL

NOW = datetime.now(UTC).replace(microsecond=0)
PERIOD = budgets.period_of(NOW)


# --- helpers -------------------------------------------------------------------------------


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id: uuid.UUID, clerk_org: str, user_id: uuid.UUID, clerk_user: str):
        self.org_id, self.clerk_org, self.user_id, self.clerk_user = (
            org_id,
            clerk_org,
            user_id,
            clerk_user,
        )


def seed_tenant(tier: str = "admin", plan: str | None = None) -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org, clerk_user = f"org_{uuid.uuid4().hex[:16]}", f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(s)s, %(c)s)",
        {"id": str(org_id), "s": f"bill-{org_id}", "c": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'a@x.io')",
        {"u": str(user_id), "o": str(org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(org_id), "u": str(user_id), "t": tier},
    )
    if plan:
        _sql(
            "INSERT INTO subscriptions (org_id, plan_id) "
            "SELECT %(o)s, id FROM plans WHERE code = %(p)s",
            {"o": str(org_id), "p": plan},
        )
    return Tenant(org_id, clerk_org, user_id, clerk_user)


def usage_value(org_id: uuid.UUID, tokens: int, *, key: str | None = None, **over: Any) -> dict:
    micros = int(NOW.timestamp() * 1_000_000)
    value = {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": key or f"usage:{org_id}:agent_token:{micros}:{uuid.uuid4()}",
            "org_id": str(org_id),
            "schema_version": 1,
            "producer": "agent-runtime-service",
            "occurred_at": micros,
            "correlation_id": None,
            "causation_id": None,
        },
        "user_id": None,
        "event_type": "agent_token",
        "quantity": float(tokens),
        "source_id": str(uuid.uuid4()),
        "model_tier": "sonnet",
    }
    value.update(over)
    return value


@pytest.fixture
async def env(migrated_db):
    engine = create_async_engine(APP_URL)
    redis = fakeredis.aioredis.FakeRedis()
    yield async_sessionmaker(engine, expire_on_commit=False), redis
    await engine.dispose()


def budget_row(org_id: uuid.UUID) -> dict[str, Any]:
    return _sql(
        "SELECT * FROM agent_budgets WHERE org_id = %(o)s AND period = %(p)s",
        {"o": str(org_id), "p": PERIOD},
    )[0]


def outbox(org_id: uuid.UUID, topic: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s AND topic = %(t)s "
        "ORDER BY id",
        {"o": str(org_id), "t": topic},
    )


# --- pure functions ------------------------------------------------------------------------


def test_budget_limits_are_per_tenant_and_pro_has_a_throttle_band():
    assert budgets.budget_limits("free", 200_000) == (200_000, 200_000)
    assert budgets.budget_limits("pro", 4_000_000) == (4_000_000, 8_000_000)
    assert budgets.budget_limits("enterprise", 0) == (0, 0)


def test_state_machine():
    assert budgets.next_state(10, 100, 200) == "ok"
    assert budgets.next_state(100, 100, 200) == "throttled"
    assert budgets.next_state(200, 100, 200) == "blocked"
    assert budgets.next_state(100, 100, 100) == "blocked"  # Free: no throttle band


def test_period_end_rolls_the_year():
    assert budgets.period_end("2026-12") == datetime(2027, 1, 1, tzinfo=UTC)


def test_enforcement_mode_defaults_to_alert_only_and_refuses_typos(monkeypatch):
    monkeypatch.delenv("BUDGET_ENFORCEMENT_MODE", raising=False)
    assert budgets.enforcement_mode() == "alert_only"
    monkeypatch.setenv("BUDGET_ENFORCEMENT_MODE", "enforced")
    with pytest.raises(ValueError):
        budgets.enforcement_mode()


# --- consumer ------------------------------------------------------------------------------


async def test_a_message_is_metered_once_and_mirrored(env):
    sm, redis = env
    t = seed_tenant()
    value = usage_value(t.org_id, 1_000)

    assert await consumer.process(sm, redis, value) == "applied"
    assert await consumer.process(sm, redis, value) == "duplicate"

    # The cache evicted: the UNIQUE constraint still refuses a second charge.
    await redis.flushall()
    assert await consumer.process(sm, redis, value) == "duplicate"

    rows = _sql(
        "SELECT quantity, model_tier FROM usage_events WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )
    assert (
        len(rows) == 1 and int(rows[0]["quantity"]) == 1_000 and rows[0]["model_tier"] == "sonnet"
    )
    b = budget_row(t.org_id)
    assert (b["tokens_consumed"], b["token_quota"], b["hard_cap"], b["state"]) == (
        1_000,
        200_000,
        200_000,
        "ok",
    )
    # Free subscription was provisioned lazily.
    assert _sql("SELECT 1 FROM subscriptions WHERE org_id = %(o)s", {"o": str(t.org_id)})

    await consumer.process(sm, redis, usage_value(t.org_id, 500))
    mirror = await redis.hgetall(budgets.mirror_key(t.org_id, PERIOD))
    assert mirror[b"tokens_consumed"] == b"1500"
    assert mirror[b"state"] == b"ok" and mirror[b"enforcement"] == b"alert_only"
    assert await redis.ttl(budgets.mirror_key(t.org_id, PERIOD)) > 0


async def test_occurred_at_as_a_decoded_datetime_is_accepted(env):
    """The confluent AvroDeserializer returns `timestamp-micros` as a datetime, not an int.
    Found by the end-to-end run through real Redpanda: every real message went to the DLQ."""
    sm, redis = env
    t = seed_tenant()
    value = usage_value(t.org_id, 77)
    value["header"]["occurred_at"] = NOW
    assert await consumer.process(sm, redis, value) == "applied"
    assert budget_row(t.org_id)["tokens_consumed"] == 77


async def test_alerts_are_flags_that_fire_once_each(env):
    sm, redis = env
    t = seed_tenant()
    await consumer.process(sm, redis, usage_value(t.org_id, 170_000))  # 85%
    b = budget_row(t.org_id)
    assert b["alert_80_sent"] and not b["alert_95_sent"] and b["state"] == "ok"

    await consumer.process(sm, redis, usage_value(t.org_id, 5_000))  # 87.5%: nothing new
    await consumer.process(sm, redis, usage_value(t.org_id, 20_000))  # 97.5%
    b = budget_row(t.org_id)
    assert b["alert_95_sent"] and b["state"] == "ok"

    notes = outbox(t.org_id, "notifications")
    assert [n["payload"]["title"] for n in notes] == ["AI budget 80% used", "AI budget 95% used"]
    assert all(n["payload"]["user_id"] == str(t.user_id) for n in notes)
    assert outbox(t.org_id, "billing-events") == []


async def test_one_big_charge_fires_both_alerts(env):
    sm, redis = env
    t = seed_tenant()
    await consumer.process(sm, redis, usage_value(t.org_id, 195_000))
    b = budget_row(t.org_id)
    assert b["alert_80_sent"] and b["alert_95_sent"]
    assert len(outbox(t.org_id, "notifications")) == 2


async def test_free_goes_straight_to_blocked_at_its_cap(env):
    sm, redis = env
    t = seed_tenant()
    await consumer.process(sm, redis, usage_value(t.org_id, 200_000))
    assert budget_row(t.org_id)["state"] == "blocked"
    events = outbox(t.org_id, "billing-events")
    assert len(events) == 1
    detail = json.loads(events[0]["payload"]["detail_json"])
    assert events[0]["payload"]["event_type"] == "quota_exceeded" and detail["state"] == "blocked"
    titles = [n["payload"]["title"] for n in outbox(t.org_id, "notifications")]
    assert "AI budget exhausted" in titles

    # Further usage is still recorded (durable facts are never shed) but emits nothing new.
    await consumer.process(sm, redis, usage_value(t.org_id, 10))
    assert budget_row(t.org_id)["tokens_consumed"] == 200_010
    assert len(outbox(t.org_id, "billing-events")) == 1


async def test_pro_throttles_at_quota_and_blocks_at_twice_quota(env):
    sm, redis = env
    t = seed_tenant(plan="pro")
    await consumer.process(sm, redis, usage_value(t.org_id, 4_000_000))
    b = budget_row(t.org_id)
    assert (b["token_quota"], b["hard_cap"], b["state"]) == (4_000_000, 8_000_000, "throttled")
    await consumer.process(sm, redis, usage_value(t.org_id, 4_000_000))
    assert budget_row(t.org_id)["state"] == "blocked"
    states = [
        json.loads(e["payload"]["detail_json"])["state"] for e in outbox(t.org_id, "billing-events")
    ]
    assert states == ["throttled", "blocked"]


async def test_a_failed_transaction_unmarks_the_message_for_redelivery(env, monkeypatch):
    sm, redis = env
    t = seed_tenant()
    value = usage_value(t.org_id, 300)

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("database went away")

    monkeypatch.setattr(consumer, "apply_tokens", boom)
    with pytest.raises(RuntimeError):
        await consumer.process(sm, redis, value)
    assert not await redis.exists(
        consumer.dedupe_key(str(t.org_id), value["header"]["idempotency_key"])
    )
    assert _sql("SELECT 1 FROM usage_events WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []

    monkeypatch.undo()
    assert await consumer.process(sm, redis, value) == "applied"
    assert budget_row(t.org_id)["tokens_consumed"] == 300


@pytest.mark.parametrize(
    "override",
    [
        {"event_type": "meeting_minute"},
        {"quantity": -5.0},
        {"model_tier": "gpt"},
        {"user_id": "not-a-uuid"},
    ],
)
async def test_poison_messages_are_refused(env, override):
    sm, redis = env
    t = seed_tenant()
    with pytest.raises(consumer.PoisonMessage):
        await consumer.process(sm, redis, usage_value(t.org_id, 1, **override))


async def test_a_header_without_a_tenant_is_poison(env):
    sm, redis = env
    value = usage_value(uuid.uuid4(), 1)
    value["header"]["org_id"] = "acme"
    with pytest.raises(consumer.PoisonMessage):
        await consumer.process(sm, redis, value)


async def test_rls_isolates_usage_and_budgets(env):
    sm, redis = env
    a, b = seed_tenant(), seed_tenant()
    await consumer.process(sm, redis, usage_value(a.org_id, 42))
    async with sm() as session, session.begin():
        await session.execute(
            text("SELECT set_config('app.current_org', :o, true)"), {"o": str(b.org_id)}
        )
        seen = (
            await session.execute(
                text(
                    "SELECT (SELECT count(*) FROM usage_events WHERE org_id = :a) + "
                    "(SELECT count(*) FROM agent_budgets WHERE org_id = :a)"
                ),
                {"a": str(a.org_id)},
            )
        ).scalar_one()
    assert seen == 0
    async with sm() as session, session.begin():  # no tenant set: fail closed
        assert (await session.execute(text("SELECT count(*) FROM agent_budgets"))).scalar_one() == 0


# --- derived dimensions + rollup -----------------------------------------------------------


def seed_ended_meeting(t: Tenant) -> uuid.UUID:
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title, status) VALUES (%(m)s, %(o)s, 'M', 'ended')",
        {"m": str(meeting_id), "o": str(t.org_id)},
    )
    # Two speakers, overlapping: span 0..150s (3 minutes rounded up), speech 90 + 70 = 160s.
    for speaker, start, end in (("a", 0, 90_000), ("b", 80_000, 150_000)):
        _sql(
            "INSERT INTO transcript_chunks (org_id, meeting_id, speaker_id, start_ms, end_ms, "
            "text, confidence) VALUES (%(o)s, %(m)s, %(s)s, %(st)s, %(en)s, 'hi', 0.9)",
            {"o": str(t.org_id), "m": str(meeting_id), "s": speaker, "st": start, "en": end},
        )
    return meeting_id


async def test_meeting_usage_is_derived_once_and_rolled_up_with_the_tier_split(env):
    sm, redis = env
    t = seed_tenant()
    seed_ended_meeting(t)
    await consumer.process(sm, redis, usage_value(t.org_id, 700, model_tier="haiku"))
    await consumer.process(sm, redis, usage_value(t.org_id, 300, model_tier="sonnet"))

    since = NOW - timedelta(days=1)
    for expected in (2, 0):  # second run inserts nothing
        async with sm() as session, session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(t.org_id)}
            )
            assert await metering.derive_meeting_usage(session, t.org_id, since) == expected
            await metering.rollup_period(session, t.org_id, PERIOD)

    agg = {
        r["dimension"]: r
        for r in _sql(
            "SELECT dimension, quantity, tier_breakdown FROM usage_aggregates "
            "WHERE org_id = %(o)s AND billing_period = %(p)s",
            {"o": str(t.org_id), "p": PERIOD},
        )
    }
    assert float(agg["meeting_minute"]["quantity"]) == 3
    assert float(agg["transcription_second"]["quantity"]) == 160
    assert float(agg["agent_token"]["quantity"]) == 1_000
    assert agg["agent_token"]["tier_breakdown"] == {"haiku": 700, "sonnet": 300}
    quota = _sql(
        "SELECT quota, consumed, state FROM quotas WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )[0]
    assert (int(quota["quota"]), int(quota["consumed"]), quota["state"]) == (300, 3, "ok")


# --- API -----------------------------------------------------------------------------------


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = create_app("billing-test")
    app.include_router(routes.router)
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


def sign_in(t: Tenant) -> None:
    PRINCIPAL.clerk_org, PRINCIPAL.clerk_user = t.clerk_org, t.clerk_user


def test_usage_is_admin_only_and_shows_the_live_budget(client):
    admin = seed_tenant("admin")
    sign_in(admin)
    r = client.get("/usage")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["period"] == PERIOD and body["plan"]["code"] == "free"
    assert body["budget"]["token_quota"] == 200_000 and body["budget"]["state"] == "ok"
    assert body["budget"]["enforcement"] == "alert_only"

    sign_in(seed_tenant("member"))
    assert client.get("/usage").status_code == 403


def test_plans_lists_the_catalog(client):
    sign_in(seed_tenant("member"))
    r = client.get("/plans")
    assert r.status_code == 200
    assert [p["code"] for p in r.json()] == ["free", "pro", "enterprise"]
