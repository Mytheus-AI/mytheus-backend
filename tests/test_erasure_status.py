"""Erasure status per store: admin-ops' `admin-ops-service.erasure-status` consumer and the
`status`/`stores` fields of `GET /admin/tenant/erasures`.

Real Postgres as `mytheus_app`, fakeredis. Covered:
- a new request is `in_progress`, with `core_api` done and the four remote stores pending;
- each `erasure_completed` appends one `data_erasure.store_completed` audit row and the list
  shows that store done with its counts; the request is `completed` once all four reported;
- redelivery (same key, or a cold cache) is a no-op; other admin events are ignored;
- poison (no header, a non-UUID, an erasure id of another tenant) raises and writes nothing,
  and `consume_one` routes it to `admin-events.dlq`;
- a failed transaction releases the dedupe key so the redelivery applies;
- rows recorded before wave 3 read as completed with core-api's share only.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import fakeredis.aioredis
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.admin_ops_service import erasure_consumer
from services.admin_ops_service.erasure_consumer import (
    PoisonMessage,
    consume_one,
    process,
)
from services.admin_ops_service.erasure_status import REMOTE_STORES
from tests.conftest import APP_URL
from tests.test_admin_ops_api import _sql, audit_rows, new_tenant
from tests.test_data_erasure import _meeting, client  # noqa: F401 - the shared fixture

pytestmark = pytest.mark.usefixtures("migrated_db")

SERVICES = {
    "transcript_chunks": "transcription-service",
    "vectors": "embedding-service",
    "search_index": "search-service",
    "knowledge_graph": "knowledge-graph-service",
}


def completed_event(
    org_id: uuid.UUID, erasure_id: str, store: str, counts: dict[str, int] | None = None
) -> dict[str, Any]:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": f"erasure_completed:{erasure_id}:{store}",
            "org_id": str(org_id),
            "schema_version": 1,
            "producer": SERVICES.get(store, "x-service"),
            "occurred_at": 0,
            "correlation_id": erasure_id,
            "causation_id": None,
        },
        "event_type": "erasure_completed",
        "actor_id": str(uuid.uuid4()),
        "target_type": "meeting",
        "target_id": str(uuid.uuid4()),
        "detail_json": json.dumps(
            {
                "erasure_id": erasure_id,
                "store": store,
                "service": SERVICES.get(store, "x-service"),
                "counts": counts if counts is not None else {"rows": 3},
            }
        ),
    }


async def apply(value: Any, redis: Any) -> str:
    engine = create_async_engine(APP_URL)
    try:
        return await process(async_sessionmaker(engine, expire_on_commit=False), redis, value)
    finally:
        await engine.dispose()


def _request_meeting_erasure(client: Any) -> tuple[Any, str]:  # noqa: F811
    t = new_tenant()
    t.add_user("admin").sign_in()
    meeting = _meeting(t.org_id, title="Budget")
    res = client.post(
        "/admin/tenant/erasures",
        json={"kind": "meeting", "target_id": meeting, "confirm": "Budget"},
    )
    assert res.status_code == 200, res.text
    return t, res.json()["id"]


def _stores(client: Any, erasure_id: str) -> tuple[str, dict[str, dict[str, Any]]]:  # noqa: F811
    items = client.get("/admin/tenant/erasures").json()["items"]
    item = next(i for i in items if i["id"] == erasure_id)
    return item["status"], {s["store"]: s for s in item["stores"]}


async def test_a_request_is_in_progress_until_every_store_reports(client):  # noqa: F811
    t, erasure_id = _request_meeting_erasure(client)
    status, stores = _stores(client, erasure_id)
    assert status == "in_progress"
    assert list(stores) == ["core_api", *REMOTE_STORES]
    assert stores["core_api"]["status"] == "completed"
    assert stores["core_api"]["completed_at"] is not None
    assert all(stores[s]["status"] == "pending" for s in REMOTE_STORES)

    redis = fakeredis.aioredis.FakeRedis()
    assert (
        await apply(completed_event(t.org_id, erasure_id, "vectors", {"points": 12}), redis)
        == "applied"
    )
    status, stores = _stores(client, erasure_id)
    assert status == "in_progress"
    assert stores["vectors"]["status"] == "completed"
    assert stores["vectors"]["counts"] == {"points": 12}
    rows = audit_rows(t.org_id, "data_erasure.store_completed")
    assert len(rows) == 1
    assert rows[0]["actor_type"] == "system"
    assert rows[0]["target_type"] == "data_erasure"
    assert rows[0]["target_id"] == erasure_id
    assert rows[0]["metadata"]["store"] == "vectors"
    assert rows[0]["metadata"]["service"] == "embedding-service"

    for store in ("transcript_chunks", "search_index", "knowledge_graph"):
        assert await apply(completed_event(t.org_id, erasure_id, store), redis) == "applied"
    status, stores = _stores(client, erasure_id)
    assert status == "completed"
    assert all(s["status"] == "completed" for s in stores.values())


async def test_redelivery_is_a_no_op_and_other_events_are_ignored(client):  # noqa: F811
    t, erasure_id = _request_meeting_erasure(client)
    event = completed_event(t.org_id, erasure_id, "search_index")
    redis = fakeredis.aioredis.FakeRedis()
    assert await apply(event, redis) == "applied"
    assert await apply(event, redis) == "duplicate"
    # A cold cache: the inbox row is the authority.
    assert await apply(event, fakeredis.aioredis.FakeRedis()) == "duplicate"
    assert len(audit_rows(t.org_id, "data_erasure.store_completed")) == 1

    other = dict(event, event_type="erasure_requested")
    assert await apply(other, fakeredis.aioredis.FakeRedis()) == "ignored"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda e: e.update(header={}),
        lambda e: e["header"].update(org_id="not-a-uuid"),
        lambda e: e.update(detail_json="{"),
        lambda e: e.update(detail_json=json.dumps({"erasure_id": "x", "store": "vectors"})),
        lambda e: e.update(
            detail_json=json.dumps(
                {"erasure_id": str(uuid.uuid4()), "store": "Bad Store!", "service": "s"}
            )
        ),
    ],
)
async def test_malformed_events_are_poison(client, mutate):  # noqa: F811
    t, erasure_id = _request_meeting_erasure(client)
    event = completed_event(t.org_id, erasure_id, "vectors")
    mutate(event)
    with pytest.raises(PoisonMessage):
        await apply(event, fakeredis.aioredis.FakeRedis())
    assert audit_rows(t.org_id, "data_erasure.store_completed") == []


async def test_an_erasure_of_another_tenant_is_poison_and_releases_its_key(client):  # noqa: F811
    a, erasure_a = _request_meeting_erasure(client)
    b, _ = _request_meeting_erasure(client)
    redis = fakeredis.aioredis.FakeRedis()
    # Tenant B's header naming tenant A's erasure: invisible under B's RLS.
    with pytest.raises(PoisonMessage):
        await apply(completed_event(b.org_id, erasure_a, "vectors"), redis)
    assert audit_rows(b.org_id, "data_erasure.store_completed") == []
    assert audit_rows(a.org_id, "data_erasure.store_completed") == []
    assert await redis.keys("*") == []


class _Msg:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def value(self) -> bytes:
        return self._raw

    def key(self) -> bytes:
        return b"k"


class _Dlq:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def produce(self, **kw: Any) -> None:
        self.sent.append(kw)

    def flush(self, timeout: float) -> int:
        return 0


async def test_consume_one_routes_poison_to_the_dlq_and_applies_good_records(client):  # noqa: F811
    t, erasure_id = _request_meeting_erasure(client)
    good = completed_event(t.org_id, erasure_id, "knowledge_graph")
    engine = create_async_engine(APP_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    dlq = _Dlq()
    redis = fakeredis.aioredis.FakeRedis()
    try:
        outcome = await consume_one(
            _Msg(json.dumps(good).encode()),
            decode_value=lambda raw: json.loads(raw or b""),
            session_factory=factory,
            redis=redis,
            dlq_producer=dlq,
        )
        assert outcome == "applied"
        outcome = await consume_one(
            _Msg(b"\x00garbage"),
            decode_value=lambda raw: json.loads(raw or b""),
            session_factory=factory,
            redis=redis,
            dlq_producer=dlq,
        )
        assert outcome == "dlq"
        assert [m["topic"] for m in dlq.sent] == ["admin-events.dlq"]
        assert json.loads(dlq.sent[0]["value"])["group"] == erasure_consumer.CONSUMER_GROUP
    finally:
        await engine.dispose()


def test_rows_before_wave_3_read_as_completed(client):  # noqa: F811
    t = new_tenant()
    t.add_user("admin").sign_in()
    legacy_id = str(uuid.uuid4())
    _sql(
        "INSERT INTO audit_logs (org_id, actor_type, action, target_type, target_id, metadata) "
        "VALUES (%(o)s, 'user', 'data_erasure.completed', 'data_erasure', %(t)s, "
        "CAST(%(m)s AS jsonb))",
        {
            "o": str(t.org_id),
            "t": legacy_id,
            "m": json.dumps(
                {
                    "kind": "meeting",
                    "status": "completed",
                    "target_id": str(uuid.uuid4()),
                    "erased": {"notes": 2},
                    "label": "Old",
                    "at": datetime.now(UTC).isoformat(),
                }
            ),
        },
    )
    status, stores = _stores(client, legacy_id)
    assert status == "completed"
    assert list(stores) == ["core_api"]
    assert stores["core_api"]["counts"] == {"notes": 2}
