"""`services/core_api/consumer.py`'s `handle_presence_event`: Task 5, Plan 3-4 section 3.3
step 5 ("Consume presence-events for analytics; read model only, no domain write").

Exercises the testable core directly (no live Kafka/Schema Registry needed - see the module
docstring's split rationale), against a real Postgres (`app.current_org` / RLS re-assertion,
the inbox dedupe) and `fakeredis` (the Redis-side dedupe check), matching this repo's own
testing convention (`tests/test_idempotency.py` uses the same fakeredis pattern).

Covers: a fresh message is recorded once; a redelivered message (same idempotency_key) is a
no-op; and the KNOWN CONTRACT GAP documented in `consumer.py` - a header `org_id` that is not
a real UUID (exactly what `realtime-gateway`'s current producer sends, per the task-5 report)
is routed to `PoisonMessage` rather than silently accepted under an unresolvable tenant scope.
"""

from __future__ import annotations

import uuid

import fakeredis.aioredis
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from services.core_api.consumer import PoisonMessage, handle_presence_event
from tests.conftest import APP_URL, seed_org


def _envelope(org_id: str, idempotency_key: str, **payload) -> dict:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": idempotency_key,
            "org_id": org_id,
            "schema_version": 1,
            "producer": "realtime-gateway",
            "occurred_at": 0,
            "correlation_id": None,
            "causation_id": None,
        },
        **payload,
    }


async def test_a_fresh_message_is_recorded_via_the_inbox(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="pe")
    await su.dispose()

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    key = str(uuid.uuid4())
    value = _envelope(
        str(org_id), key, meeting_id=str(uuid.uuid4()), user_id=str(uuid.uuid4()), event_type="join"
    )

    try:
        async with app_engine.connect() as conn, conn.begin():
            applied = await handle_presence_event(conn, redis, value)
        assert applied is True

        async with app_engine.connect() as conn, conn.begin():
            from sqlalchemy import text

            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_id)}
            )
            rows = (
                await conn.execute(
                    text(
                        "SELECT consumer_group FROM inbox WHERE org_id = :o "
                        "AND idempotency_key = :k"
                    ),
                    {"o": str(org_id), "k": key},
                )
            ).all()
        assert len(rows) == 1
        assert rows[0][0] == "core-api"
    finally:
        await app_engine.dispose()


async def test_a_redelivered_message_is_a_dedupe_no_op(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="pe2")
    await su.dispose()

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    key = str(uuid.uuid4())
    value = _envelope(
        str(org_id),
        key,
        meeting_id=str(uuid.uuid4()),
        user_id=str(uuid.uuid4()),
        event_type="leave",
    )

    try:
        async with app_engine.connect() as conn, conn.begin():
            first = await handle_presence_event(conn, redis, value)
        async with app_engine.connect() as conn, conn.begin():
            second = await handle_presence_event(conn, redis, value)
        assert first is True
        assert second is False, "a redelivered idempotency_key must be a no-op"
    finally:
        await app_engine.dispose()


async def test_a_non_uuid_org_id_header_is_a_poison_message():
    """The KNOWN CONTRACT GAP: realtime-gateway's current producer puts the raw Clerk org
    claim (a string like "org_abc123", never a UUID) into header.org_id. This must fail
    closed as a poison message, not silently `SET app.current_org` to a value that names no
    real tenant."""
    redis = fakeredis.aioredis.FakeRedis()
    value = _envelope(
        "org_2abcNotAUuid",
        str(uuid.uuid4()),
        meeting_id=str(uuid.uuid4()),
        user_id="user_alsoNotAUuid",
        event_type="join",
    )

    with pytest.raises(PoisonMessage):
        await handle_presence_event(None, redis, value)  # session unused before the raise


async def test_a_missing_header_field_is_a_poison_message():
    redis = fakeredis.aioredis.FakeRedis()
    value = {"header": {"org_id": None, "idempotency_key": None}, "event_type": "join"}

    with pytest.raises(PoisonMessage):
        await handle_presence_event(None, redis, value)
