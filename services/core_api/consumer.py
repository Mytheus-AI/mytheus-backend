"""Kafka consumer for core-api.

Consumes: `agent-results` (Plan 7-9 section 3.3 step 3: `PersonalListenerAgent` notes into
`worklogs`; section 3.6 step 3: decision/action candidates into `decisions`/`action_items` as
`proposed`, with no event; see `handle_agent_result`) and presence-events (analytics read
model only; no domain write - Plan 3-4 section 3.3 step 5). Presence itself stays owned by
`realtime-gateway`/Redis; this consumer never writes `meeting_participants` or any table
`core-api` treats as domain source of truth from this path.

Split the same way `libs/shared/relay.py` splits `relay_once` (testable core) from
`services/relay/main.py`'s `run()` (the confluent-kafka/asyncio harness): `handle_presence_event`
takes an already-decoded message and is exercised directly in tests against a real Postgres +
fakeredis, with no live broker or Schema Registry required. `run_consumer()` is the thin,
mostly-untested wiring around it.

Contract with `realtime-gateway`'s producer: CLOSED, both sides. `PresenceEvent`'s envelope
header, and this topic's `user_id` payload field, are documented as UUID-typed
(`mytheus-events/schemas/presence-events/v1.avsc`, `_canon/03-event-taxonomy.md`'s envelope
table: "org_id ... Tenant root; RLS/cell scoping anchor"), and `realtime-gateway`'s
`presenceEventsProducer.ts` now honours that: it resolves `internalOrgId`/`internalUserId` via
this service's own `GET /api/v1/meetings/:id/participants/:clerkUserId` (reading
`ParticipantCheck.org_id`, added in `ca9a8db`, and `Participant.user_id`, which Task 6 had
already been discarding until it started reading it) before ever producing, and additionally
validates `isUuid(orgId) && isUuid(userId)` immediately before `send()`. Verified end to end by
Task 6 against a real `core-api` process, real Postgres, real RS256 verification. A Clerk-native
id can no longer reach this topic.

History, for context only - NOT a current condition: for a window during this band, the
producer passed the raw JWT `org_id`/`sub` claims straight through unresolved, which would have
made every real message fail the parse below and land in `presence-events.dlq`. That window is
closed; nothing here is presently poison-messaging on this account.

`handle_presence_event`'s strict `uuid.UUID(header["org_id"])` parse and DLQ-on-failure below
are NOT a workaround for the now-closed gap and must not be relaxed as if they were. They are
the standing "add an event consumer" recipe from this repo's `CLAUDE.md` (re-assert `org_id`
from the header before any write), and now serve as defence against a FUTURE regression on the
producer side - this or another producer sending a malformed header again - not as a permanent
crutch for a bug that no longer exists.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import date
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.tenancy import set_rls
from services.core_api.confidence import SILENT_FROM as _SILENT_FROM
from services.core_api.confidence import SURFACE_ABOVE as _SURFACE_ABOVE
from services.core_api.confidence import thresholds
from services.core_api.visibility import is_visible

logger = logging.getLogger(__name__)

TOPIC = "presence-events"
AGENT_RESULTS = "agent-results"
TOPICS = [TOPIC, AGENT_RESULTS]
CONSUMER_GROUP = "core-api"

# doc 08 section 6, the uniform confidence policy (see `services/core_api/confidence.py`).
# Re-exported under their historical names.
SURFACE_ABOVE = _SURFACE_ABOVE
SILENT_FROM = _SILENT_FROM
_thresholds = thresholds

# Phase 8 Part A contract section 2: the two candidate kinds that get a `proposed` row.
_CANDIDATE_TABLES = {
    ("DecisionAgent", "decision_candidate"): "decisions",
    ("ActionAgent", "action_candidate"): "action_items",
}


class PoisonMessage(Exception):
    """The message cannot be safely applied (schema mismatch, header that names no real
    tenant). The caller routes the raw message to `{TOPIC}.dlq` and commits past it - never
    re-raised into the poll loop, per the "never block the partition" consumer recipe."""


async def handle_presence_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one already-Avro-decoded `presence-events` message.

    Returns True if it was newly recorded, False if it was a dedupe no-op. Raises
    `PoisonMessage` if the header does not carry a resolvable tenant - expected not to happen
    against `realtime-gateway`'s current producer (see the module docstring), but kept as the
    standing defence the "add an event consumer" recipe calls for regardless.
    """
    header = value.get("header") or {}
    raw_org_id = header.get("org_id")
    idempotency_key = header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"presence-events message missing header org_id/idempotency_key: {value!r}"
        )

    try:
        org_id = uuid.UUID(str(raw_org_id))
    except ValueError as exc:
        raise PoisonMessage(
            f"presence-events header.org_id is not a UUID ({raw_org_id!r}); the producer "
            "contract requires a resolved internal UUID here - see the module docstring"
        ) from exc

    if await is_duplicate(redis, str(org_id), TOPIC, idempotency_key):
        return False

    await set_rls(session, org_id)

    # Read-model only, per the plan text: no write to meeting_participants or any table this
    # service treats as domain source of truth. A durable analytics sink (its own table) is a
    # data-model decision this task does not have the authority to make unilaterally (CLAUDE.md
    # naming authority: a new table needs `_canon/02-data-model.md` plus an ADR first) - logged
    # structurally instead, and flagged as a concrete follow-up in the task-5 report.
    logger.info(
        "presence-events: %s org=%s meeting=%s user=%s",
        value.get("event_type"),
        org_id,
        value.get("meeting_id"),
        value.get("user_id"),
        extra={
            "org_id": str(org_id),
            "meeting_id": value.get("meeting_id"),
            "user_id": value.get("user_id"),
            "event_type": value.get("event_type"),
        },
    )
    try:
        await record_consumed(session, CONSUMER_GROUP, str(org_id), idempotency_key)
    except IntegrityError as exc:
        # `inbox.org_id` references `organizations`: a tenant that does not exist can never
        # apply. Raised as IntegrityError it crashed the loop and blocked the partition on
        # every restart; as poison it is dead-lettered (same fix as agent-results, 97f2709).
        raise PoisonMessage(f"presence-events names unknown tenant {org_id}") from exc
    return True


def _uuid(raw: Any, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(raw))
    except ValueError as exc:
        raise PoisonMessage(f"agent-results {what} is not a UUID: {raw!r}") from exc


async def handle_agent_result(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one decoded `agent-results` message. True if a `worklogs` row was written.

    `PersonalListenerAgent` notes go to `worklogs` (P7). `DecisionAgent`/`decision_candidate`
    and `ActionAgent`/`action_candidate` go to `decisions` / `action_items` as `proposed` rows
    (P8 Part A; True if any candidate row was new), never to the outbox. Other kinds
    (suggestions) are recorded in the inbox and otherwise ignored until their band.

    `surfaced` is decided HERE, not trusted from the payload (ADR-0030 D3): `core-api` can read
    the tenant's `agent_confidence_overrides`, the AI tier cannot. Below `silent` nothing is
    written. Every id in the message is checked against this tenant under RLS before the
    insert, because a foreign key resolves rows the writing transaction cannot see (gap 31):
    a meeting or user from another tenant is poison, never a row.
    """
    header = value.get("header") or {}
    raw_org_id, idempotency_key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"agent-results message missing header org_id/idempotency_key: {value!r}"
        )
    org_id = _uuid(raw_org_id, "header.org_id")
    if await is_duplicate(redis, str(org_id), AGENT_RESULTS, idempotency_key):
        return False
    try:
        return await _apply_agent_result(session, org_id, idempotency_key, value)
    except BaseException:
        # the transaction will roll back: forget the dedupe key so the redelivery is applied
        await redis.delete(f"nexus:{org_id}:dedupe:{AGENT_RESULTS}:{idempotency_key}")
        raise


async def _apply_agent_result(
    session: AsyncSession, org_id: uuid.UUID, idempotency_key: str, value: dict[str, Any]
) -> bool:
    await set_rls(session, org_id)
    # Claim the inbox row FIRST, in this transaction. `record_consumed` is ON CONFLICT DO
    # NOTHING after the effect, which only dedupes the inbox row itself: once the Redis key
    # has expired, a redelivery would write a second note. Claiming first makes the inbox the
    # authority; a rollback releases the claim together with the note.
    try:
        claimed = (
            await session.execute(
                text(
                    "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                    "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, idempotency_key) "
                    "DO NOTHING RETURNING 1"
                ),
                {"g": CONSUMER_GROUP, "o": str(org_id), "k": idempotency_key},
            )
        ).first()
    except IntegrityError as exc:
        # `inbox.org_id` references `organizations`: a tenant that does not exist (deleted, or
        # never real) can never apply, so it is poison. Raised as IntegrityError it would crash
        # the loop and block the partition on every restart. Found by the 2026-11-08 run.
        raise PoisonMessage(f"agent-results names unknown tenant {org_id}") from exc
    if claimed is None:
        return False
    agent, kind = str(value.get("agent")), str(value.get("kind"))
    candidate_table = _CANDIDATE_TABLES.get((agent, kind))
    if candidate_table is None and (agent != "PersonalListenerAgent" or kind != "note"):
        return False

    meeting_id = _uuid(value.get("meeting_id"), "meeting_id")
    user_id = _uuid(value.get("user_id"), "user_id")
    chunk_ids = [str(_uuid(c, "source_chunk_ids[]")) for c in value.get("source_chunk_ids") or []]
    visible = (
        await session.execute(
            text(
                "SELECT (SELECT count(*) FROM meetings WHERE id = :m AND deleted_at IS NULL), "
                "(SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL), "
                "(SELECT agent_confidence_overrides FROM tenant_settings)"
            ),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).one()
    if not visible[0] or not visible[1]:
        raise PoisonMessage(
            f"agent-results names a meeting or user not in tenant {org_id}: "
            f"meeting={meeting_id} user={user_id}"
        )
    surface, silent = _thresholds(visible[2], agent)
    if candidate_table is not None:
        return await _persist_candidates(
            session, org_id, meeting_id, candidate_table, silent, value.get("payload_json")
        )
    confidence = float(value.get("confidence") or 0.0)
    content = str(value.get("content") or "").strip()
    written = False
    if content and confidence >= silent:
        await session.execute(
            text(
                "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, "
                "relevance_score, confidence, created_by_agent, source_chunk_ids, surfaced) "
                "VALUES (:o, :m, :u, :w, :c, :r, :conf, true, CAST(:chunks AS uuid[]), :s)"
            ),
            {
                "o": str(org_id),
                "m": str(meeting_id),
                "u": str(user_id),
                "w": str(value.get("window_id") or ""),
                "c": content,
                "r": max(0.0, min(1.0, float(value.get("relevance_score") or 0.0))),
                "conf": max(0.0, min(1.0, confidence)),
                "chunks": chunk_ids,
                "s": confidence > surface,
            },
        )
        written = True
    return written


def _candidate_confidence(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    c = float(raw)
    return c if 0.0 <= c <= 1.0 else None


def _optional_uuid(raw: Any) -> uuid.UUID | None:
    if not isinstance(raw, str):
        return None
    try:
        return uuid.UUID(raw)
    except ValueError:
        return None


def _optional_date(raw: Any) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


async def _persist_candidates(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    table: str,
    silent: float,
    payload_json: Any,
) -> bool:
    """Insert each decision/action candidate as `status = 'proposed'`. True if any row is new.

    Phase 8 Part A contract section 2. NO outbox row and NO topic event, ever: a candidate is
    unconfirmed until a participant presses Confirm (`routes/candidates.py`, the only producer
    of `decisions` / `actions`). The candidate's own deterministic `id` is the primary key, so
    a replayed window that slipped past the inbox is `ON CONFLICT (id) DO NOTHING`.

    A payload that is not JSON is poison (the producer is broken); a candidate with a bad
    shape is skipped and the rest are applied. The band is re-derived from the tenant's
    thresholds here, never trusted from the payload's `surfaced`.
    """
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError as exc:
        raise PoisonMessage("agent-results payload_json is not JSON") from exc
    if payload is None:
        raise PoisonMessage("agent-results candidate result has no payload_json string")
    candidates = payload.get("candidates") if isinstance(payload, dict) else None
    if not isinstance(candidates, list):
        return False
    written = False
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        cand_id = _optional_uuid(cand.get("id"))
        content = cand.get("content")
        confidence = _candidate_confidence(cand.get("confidence"))
        if cand_id is None or not isinstance(content, str) or confidence is None:
            continue
        content = content.strip()
        if not content or confidence < silent:
            continue
        params: dict[str, Any] = {
            "id": str(cand_id),
            "o": str(org_id),
            "m": str(meeting_id),
            "c": content,
            "conf": confidence,
        }
        if table == "decisions":
            rationale = cand.get("rationale")
            params["r"] = (rationale.strip() or None) if isinstance(rationale, str) else None
            sql = (
                "INSERT INTO decisions (id, org_id, meeting_id, content, rationale, confidence, "
                "status) VALUES (:id, :o, :m, :c, :r, :conf, 'proposed') "
                "ON CONFLICT (id) DO NOTHING RETURNING 1"
            )
        else:
            # A foreign key resolves another tenant's user with RLS bypassed (gap 31): an
            # assignee this tenant cannot see is dropped, never stored.
            assignee = _optional_uuid(cand.get("assignee_id"))
            if assignee is not None and not await is_visible(session, "users", assignee):
                assignee = None
            params["a"] = str(assignee) if assignee else None
            params["d"] = _optional_date(cand.get("due_date"))
            sql = (
                "INSERT INTO action_items (id, org_id, meeting_id, content, assignee_id, "
                "due_date, confidence, status) VALUES (:id, :o, :m, :c, :a, :d, :conf, "
                "'proposed') ON CONFLICT (id) DO NOTHING RETURNING 1"
            )
        if (await session.execute(text(sql), params)).first() is not None:
            written = True
    return written


def decode(deserializer: Any, raw_value: bytes | None, ctx: Any) -> Any:
    """Decode one record, turning a record that can never decode into `PoisonMessage`.

    A malformed value (a non-UUID string in a `uuid` field, a truncated or foreign payload) raises
    `ValueError`/`TypeError`/`EOFError`/`IndexError` from the Avro reader on every attempt. Left
    uncaught it killed the process, and every restart re-read the same offset, so one bad record
    stopped this consumer for every tenant. Transient failures (the Schema Registry unreachable)
    are other exception types and still propagate, so the record is retried, not dead-lettered.
    """
    try:
        return deserializer(raw_value, ctx)
    except (ValueError, TypeError, EOFError, IndexError) as exc:
        raise PoisonMessage(f"undecodable record: {exc}") from exc


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("core-api-consumer")
    serve_metrics()  # no-op unless METRICS_PORT is set
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")
    redis: Redis[bytes] = redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    dlq_producer = KafkaProducer(
        {"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True}
    )
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
    session_factory = get_sessionmaker()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("core-api consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            topic_name = msg.topic() or TOPIC
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, topic_name, _ts if _ts_type else None)
            handler = handle_agent_result if topic_name == AGENT_RESULTS else handle_presence_event
            try:
                value = decode(
                    deserializer, raw_value, SerializationContext(topic_name, MessageField.VALUE)
                )
                async with session_factory() as session, session.begin():
                    applied = await handler(session, redis, value)
                record_processed(CONSUMER_GROUP, topic_name, "applied" if applied else "duplicate")
            except PoisonMessage as exc:
                logger.error("%s: poison message routed to DLQ: %s", topic_name, exc)
                dlq_producer.produce(
                    topic=f"{topic_name}.dlq",
                    key=msg.key(),
                    value=json.dumps(
                        {"error": str(exc), "raw": raw_value.hex() if raw_value else None}
                    ).encode(),
                )
                dlq_producer.flush(10)
                record_dlq(CONSUMER_GROUP, topic_name, "POISON")
                record_processed(CONSUMER_GROUP, topic_name, "dlq")
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
