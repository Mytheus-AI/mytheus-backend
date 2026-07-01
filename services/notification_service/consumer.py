"""Kafka consumer for notification_service.

Consumes: notifications, agent-results (priority alerts).
Produces: notification-delivery-events.

Each message: re-assert org_id from header → SET app.current_org → dedupe → deliver.
"""

from __future__ import annotations

import asyncio
import os

from aiokafka import AIOKafkaConsumer

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


async def _handle(msg: object) -> None:
    # TODO(P1): re-assert org_id from msg.headers -> SET app.current_org ->
    #   is_duplicate/record_consumed (inbox + Redis dedupe) ->
    #   route json.loads(msg.value) to email / Slack / in-app based on payload.channel.
    _ = msg  # placeholder until the consumer is implemented


if __name__ == "__main__":
    asyncio.run(run_consumer())
