"""Outbox unique-dedupe (Guard 2) + full-value payload shape, and inbox composite-PK dedupe."""

from __future__ import annotations

import json
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared import inbox as inbox_lib
from libs.shared import outbox as outbox_lib
from libs.shared.events import EventEnvelope


async def test_outbox_enqueue_full_value_and_idempotent(migrated_db):
    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org = str(uuid.uuid4())
    ik = f"decision:{uuid.uuid4()}:confirmed"
    env = EventEnvelope(
        topic="decisions",
        payload={
            "decision_id": str(uuid.uuid4()),
            "meeting_id": str(uuid.uuid4()),
            "content": "c",
            "rationale": "r",
            "confidence": 0.9,
            "status": "confirmed",
            "confirmed_by": str(uuid.uuid4()),
        },
        org_id=org,
        producer="core-api",
        idempotency_key=ik,
    )

    async with session_factory() as s:
        async with s.begin():
            id1 = await outbox_lib.enqueue(s, env)
        async with s.begin():
            id2 = await outbox_lib.enqueue(s, env)  # duplicate (topic, idempotency_key)
    assert id1 == id2, "double-enqueue of the same fact must not create a second row"

    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT partition_key, schema_subject, payload FROM outbox WHERE id = :id"
                    ),
                    {"id": id1},
                )
            )
            .mappings()
            .first()
        )
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert row["partition_key"] == org
    assert row["schema_subject"] == "decisions-value"
    assert payload["header"]["org_id"] == org  # full Avro value: header + fields
    assert payload["header"]["idempotency_key"] == ik
    assert payload["status"] == "confirmed"


async def test_inbox_composite_pk_dedupe(migrated_db):
    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org, ik, cg = str(uuid.uuid4()), str(uuid.uuid4()), "knowledge-graph-service"

    async with session_factory() as s:
        async with s.begin():
            await inbox_lib.record_consumed(s, cg, org, ik)
        async with s.begin():
            await inbox_lib.record_consumed(s, cg, org, ik)  # replay -> no-op

    async with engine.begin() as conn:
        count = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM inbox "
                    "WHERE consumer_group = :c AND org_id = :o AND idempotency_key = :k"
                ),
                {"c": cg, "o": org, "k": ik},
            )
        ).scalar_one()
    assert count == 1
