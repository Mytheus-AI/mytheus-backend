"""Kafka consumer for notification_service.

Consumes: notifications, agent-results (priority alerts).
Produces: notification-delivery-events.

Each message: re-assert org_id from header → SET app.current_org → dedupe → deliver.
"""
from __future__ import annotations

import asyncio
import os

from aiokafka import AIOKafkaConsumer

from libs.shared.inbox import is_duplicate, record_consumed

_BROKERS = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
_TOPICS = ["notifications", "agent-results"]
_GROUP = "notification-service"


async def run_consumer() -> None:
    consumer = AIOKafkaConsumer(
        *_TOPICS,
        bootstrap_servers=_BROKERS,
        group_id=_GROUP,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await _handle(msg)
            await consumer.commit()
    finally:
        await consumer.stop()


async def _handle(msg) -> None:  # type: ignore[type-arg]
    import json

    headers = {k: v.decode() for k, v in msg.headers}
    org_id = headers.get("org_id", "")
    idempotency_key = headers.get("idempotency_key", "")
    topic = msg.topic

    # TODO: get redis + session from app state
    # if await is_duplicate(redis, org_id, topic, idempotency_key):
    #     return
    # await record_consumed(session, org_id, topic, idempotency_key)

    payload = json.loads(msg.value)
    # TODO: route to email / Slack / in-app channel based on payload.channel


if __name__ == "__main__":
    asyncio.run(run_consumer())
