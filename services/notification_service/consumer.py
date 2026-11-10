"""Kafka consumer for notification-service (P11 contract section 3; plan 10-13 section 3.8).

    python -m services.notification_service.consumer      # consumer group `notification-service`

Consumes:
- `agent-results`: ONLY `ProactiveAgent` / `alert` results whose `alert.surfaced` is true.
  Every other agent result (notes, candidates, suggestions, context fills, unsurfaced alerts)
  is ignored before any database work; core-api and realtime-gateway handle those.
- `notifications`: cross-channel notification intents (billing's budget alerts today; the
  workflow-service follow-ups and P12 briefs later).

Produces `notification-delivery-events` (through the outbox), one receipt per channel attempt.

Per message, in one transaction:
1. Re-assert `org_id` from the event header (poison if missing or not a UUID), fast-path dedupe
   on `nexus:{org_id}:dedupe:{topic}:{idempotency_key}`, `SET app.current_org`, then claim the
   envelope's inbox row FIRST (the inbox, not Redis, is the authority once the key expires).
2. Check the recipient (and, for an alert, its meeting) exist in this tenant under RLS. A
   foreign key alone would accept another tenant's id (gap 31), so a miss is poison.
3. Insert the `notification_log` row, `ON CONFLICT (id) DO NOTHING`. `id` is the alert's own
   deterministic `alert.id`, or uuid5(`notifications:{source_event_id}:{user_id}`), so a replay
   under a fresh envelope key still writes nothing new.
4. For each channel the recipient's preferences allow (`enabled` and `priority <= max_priority`),
   claim the canon key `notif:{source_event_id}:{channel}:{user_id}` (inbox plus Redis, both
   namespaced under `notification-delivery-events`), make the attempt, and enqueue its receipt
   with that key as its idempotency key. A claim already held means that channel was attempted
   before (a second `notifications` event for the same source and user, say) and is skipped.
   - in_app: the row is the delivery (realtime-gateway delivers live alerts itself); `sent`.
   - email: the SMTP adapter (`email.py`), opt-in per user (off by default). Unconfigured ->
     skipped (`smtp_not_configured`) in the same transaction. Configured -> queued as a
     `PendingEmail` and sent only AFTER the transaction commits (see below).
   - slack (P12): with a Slack sender configured (`slack.py`), queued as a `PendingSlack` and
     sent after commit like email: the bot token is fetched from integration-service per send,
     then users.lookupByEmail -> conversations.open -> chat.postMessage. Not connected ->
     `skipped:slack_not_connected`. No sender configured -> skipped in the transaction, as in P11.

Receipt status. `NotificationDeliveryStatus` in mytheus-events is `sent | failed | bounced`
and P11 may not change a schema, so a skipped attempt is recorded as `status: failed` with
`detail: "skipped:<reason>"`. Consumers tell "skipped" from a real failure by that prefix.

Email is sent after commit. The transaction claims the email channel's key and commits; only
then does `send_pending` send the mail and write its receipt in a second small transaction. A
redelivery finds the claim and never sends again. The trade: a crash between the commit and
the send loses that one email (and its receipt) instead of duplicating it, and a database
failure after a successful send loses the receipt. An urgent alert is still in the recipient's
in-app inbox either way.

Split like `services/core_api/consumer.py`: `handle_message` and `send_pending` are the tested
core (real Postgres, fakeredis, a fake email sender), `run_consumer()` the thin confluent-kafka
harness with the DLQ.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.inbox import is_duplicate
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.notification_service.email import EmailSender, email_sender_from_env, render_body
from services.notification_service.prefs import CHANNELS, Channel, load_prefs
from services.notification_service.slack import SlackSender, slack_sender_from_env

logger = logging.getLogger(__name__)

AGENT_RESULTS = "agent-results"
NOTIFICATIONS = "notifications"
DELIVERY_TOPIC = "notification-delivery-events"
TOPICS = [AGENT_RESULTS, NOTIFICATIONS]
CONSUMER_GROUP = "notification-service"
PRODUCER = "notification-service"

# uuid5 namespace for `notifications` rows (an alert brings its own id).
NOTIFICATION_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://mytheus.ai/ns/notification-service")
# ADR-0039 C7 adds `already_decided` and `topic_watch` (same alert shape, same delivery).
ALERT_TYPES = frozenset(
    {
        "conflict",
        "workload",
        "dependency",
        "missing_stakeholder",
        "already_decided",
        "topic_watch",
    }
)
MAX_TITLE = 120
MAX_BODY = 400
MAX_NOTIFICATION_TITLE = 200
MAX_NOTIFICATION_BODY = 2000
SKIPPED_SMTP = "skipped:smtp_not_configured"
SKIPPED_SLACK = "skipped:slack_not_connected"


class PoisonMessage(Exception):
    """The message can never apply. The harness routes it to `{topic}.dlq` and commits past it."""


@dataclass(frozen=True)
class _Notification:
    id: uuid.UUID
    source_event_id: str
    user_id: uuid.UUID
    meeting_id: uuid.UUID | None
    source: str
    alert_type: str | None
    priority: int
    title: str
    body: str
    link: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class PendingEmail:
    """An email whose channel claim is in the transaction; sent by `send_pending` after commit."""

    org_id: uuid.UUID
    notification_id: uuid.UUID
    key: str
    to: str
    subject: str
    body: str
    correlation_id: str | None
    causation_id: str | None


@dataclass(frozen=True)
class PendingSlack:
    """A Slack DM whose channel claim is in the transaction; sent by `send_pending` after commit.
    Carries no token: `send_pending` asks integration-service for one per send."""

    org_id: uuid.UUID
    notification_id: uuid.UUID
    key: str
    to_email: str
    title: str
    body: str
    link: str | None
    correlation_id: str | None
    causation_id: str | None


def _uuid(raw: Any, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(raw))
    except ValueError as exc:
        raise PoisonMessage(f"{what} is not a UUID: {raw!r}") from exc


def _safe_link(raw: Any) -> str | None:
    """A relative app path only. Anything else (an absolute or protocol-relative URL) is
    dropped: the link is rendered in emails and the UI, and must never point off-site."""
    if isinstance(raw, str) and raw.startswith("/") and not raw.startswith("//"):
        return raw[:500]
    return None


def _text(raw: Any, limit: int, what: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise PoisonMessage(f"{what} is missing or empty")
    return raw.strip()[:limit]


def _priority(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise PoisonMessage(f"priority is not an int: {raw!r}")
    return max(1, min(3, raw))


def _enum_str(raw: Any) -> str:
    """An Avro enum may be decoded as a str or as an object with a `name`."""
    return str(getattr(raw, "name", raw))


def parse_alert(value: dict[str, Any]) -> _Notification | None:
    """A surfaced ProactiveAgent alert, or None for anything this service does not deliver."""
    if (_enum_str(value.get("agent")), _enum_str(value.get("kind"))) != ("ProactiveAgent", "alert"):
        return None
    raw = value.get("payload_json")
    try:
        payload = json.loads(raw) if isinstance(raw, str) else None
    except ValueError as exc:
        raise PoisonMessage("agent-results payload_json is not JSON") from exc
    alert = payload.get("alert") if isinstance(payload, dict) else None
    if not isinstance(alert, dict):
        raise PoisonMessage("ProactiveAgent alert result carries no alert object")
    if alert.get("surfaced") is not True:
        return None
    alert_type = alert.get("alert_type")
    if alert_type not in ALERT_TYPES:
        raise PoisonMessage(f"unknown alert_type {alert_type!r}")
    alert_id = _uuid(alert.get("id"), "alert.id")
    return _Notification(
        id=alert_id,
        source_event_id=str(alert_id),
        user_id=_uuid(value.get("user_id"), "user_id"),
        meeting_id=_uuid(value.get("meeting_id"), "meeting_id"),
        source="agent_alert",
        alert_type=str(alert_type),
        priority=_priority(alert.get("priority")),
        title=_text(alert.get("title"), MAX_TITLE, "alert.title"),
        body=_text(alert.get("body"), MAX_BODY, "alert.body"),
        link=_safe_link(alert.get("link")),
        payload=alert,
    )


def parse_notification(value: dict[str, Any]) -> _Notification:
    source_event_id = value.get("source_event_id")
    if not isinstance(source_event_id, str) or not source_event_id.strip():
        raise PoisonMessage("notifications message has no source_event_id")
    user_id = _uuid(value.get("user_id"), "user_id")
    link = _safe_link(value.get("link"))
    return _Notification(
        id=uuid.uuid5(NOTIFICATION_NS, f"notifications:{source_event_id}:{user_id}"),
        source_event_id=source_event_id,
        user_id=user_id,
        meeting_id=None,
        source="notification",
        alert_type=None,
        priority=_priority(value.get("priority")),
        title=_text(value.get("title"), MAX_NOTIFICATION_TITLE, "title"),
        body=_text(value.get("body"), MAX_NOTIFICATION_BODY, "body"),
        link=link,
        payload={
            "source_event_id": source_event_id,
            "requested_channel": _enum_str(value.get("channel")),
            "producer": (value.get("header") or {}).get("producer"),
        },
    )


async def handle_message(
    session: AsyncSession,
    redis: Redis[bytes],
    topic: str,
    value: dict[str, Any],
    *,
    email: EmailSender | None = None,
    pending: list[PendingEmail | PendingSlack] | None = None,
    slack: SlackSender | None = None,
) -> bool:
    """Apply one decoded message. True if a `notification_log` row was newly written.

    With an `email` (or `slack`) sender, an allowed email (or slack) channel is appended to
    `pending` (required then) instead of being sent; the caller passes `pending` to
    `send_pending` after the commit.
    """
    if (email is not None or slack is not None) and pending is None:
        raise ValueError("a sender needs a pending list: external sends happen after commit")
    if topic == AGENT_RESULTS:
        note = parse_alert(value)
        if note is None:
            return False
    elif topic == NOTIFICATIONS:
        note = parse_notification(value)
    else:
        raise PoisonMessage(f"unexpected topic {topic!r}")

    header = value.get("header") or {}
    raw_org, idempotency_key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not idempotency_key:
        raise PoisonMessage(f"{topic} message missing header org_id/idempotency_key")
    org_id = _uuid(raw_org, "header.org_id")
    if await is_duplicate(redis, str(org_id), topic, str(idempotency_key)):
        return False
    claimed_keys: list[str] = [f"nexus:{org_id}:dedupe:{topic}:{idempotency_key}"]
    try:
        return await _apply(
            session,
            redis,
            org_id,
            str(idempotency_key),
            header,
            note,
            pending if email is not None else None,
            claimed_keys,
            pending if slack is not None else None,
        )
    except BaseException:
        # The transaction rolls back: release every dedupe key so the redelivery applies.
        await redis.delete(*claimed_keys)
        raise


async def _claim_inbox(session: AsyncSession, org_id: uuid.UUID, key: str) -> bool:
    try:
        row = (
            await session.execute(
                text(
                    "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                    "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, idempotency_key) "
                    "DO NOTHING RETURNING 1"
                ),
                {"g": CONSUMER_GROUP, "o": str(org_id), "k": key},
            )
        ).first()
    except IntegrityError as exc:
        # `inbox.org_id` references `organizations`: an unknown tenant can never apply.
        raise PoisonMessage(f"message names unknown tenant {org_id}") from exc
    return row is not None


async def _apply(
    session: AsyncSession,
    redis: Redis[bytes],
    org_id: uuid.UUID,
    idempotency_key: str,
    header: dict[str, Any],
    note: _Notification,
    pending: list[PendingEmail | PendingSlack] | None,
    claimed_keys: list[str],
    pending_slack: list[PendingEmail | PendingSlack] | None = None,
) -> bool:
    await set_rls(session, org_id)
    if not await _claim_inbox(session, org_id, idempotency_key):
        return False
    known = (
        await session.execute(
            text(
                "SELECT (SELECT email FROM users WHERE id = :u AND deleted_at IS NULL), "
                "(SELECT count(*) FROM meetings WHERE id = :m AND deleted_at IS NULL)"
            ),
            {"u": str(note.user_id), "m": str(note.meeting_id or uuid.UUID(int=0))},
        )
    ).one()
    user_email, meeting_known = known[0], known[1]
    if user_email is None or (note.meeting_id is not None and not meeting_known):
        raise PoisonMessage(
            f"message names a user or meeting not in tenant {org_id}: "
            f"user={note.user_id} meeting={note.meeting_id}"
        )
    inserted = (
        await session.execute(
            text(
                "INSERT INTO notification_log (id, org_id, user_id, meeting_id, source, "
                "alert_type, priority, title, body, link, payload) VALUES (:id, :o, :u, :m, "
                ":src, :at, :p, :t, :b, :l, CAST(:payload AS jsonb)) "
                "ON CONFLICT (id) DO NOTHING RETURNING 1"
            ),
            {
                "id": str(note.id),
                "o": str(org_id),
                "u": str(note.user_id),
                "m": str(note.meeting_id) if note.meeting_id else None,
                "src": note.source,
                "at": note.alert_type,
                "p": note.priority,
                "t": note.title,
                "b": note.body,
                "l": note.link,
                "payload": json.dumps(note.payload),
            },
        )
    ).first() is not None

    prefs = await load_prefs(session, note.user_id)
    for channel in CHANNELS:
        if not prefs[channel].allows(note.priority):
            continue
        key = f"notif:{note.source_event_id}:{channel}:{note.user_id}"
        # Namespaced under the receipt topic: producers already use this exact string as the
        # `notifications` envelope key (billing's budget alerts do), which this message has
        # just claimed above, so an un-namespaced claim would always look like a repeat.
        if await is_duplicate(redis, str(org_id), DELIVERY_TOPIC, key):
            continue
        claimed_keys.append(f"nexus:{org_id}:dedupe:{DELIVERY_TOPIC}:{key}")
        if not await _claim_inbox(session, org_id, f"{DELIVERY_TOPIC}:{key}"):
            continue
        if channel == "email" and pending is not None:
            pending.append(
                PendingEmail(
                    org_id=org_id,
                    notification_id=note.id,
                    key=key,
                    to=user_email,
                    subject=note.title,
                    body=render_body(note.body, note.link),
                    correlation_id=header.get("correlation_id"),
                    causation_id=header.get("event_id"),
                )
            )
            continue
        if channel == "slack" and pending_slack is not None:
            pending_slack.append(
                PendingSlack(
                    org_id=org_id,
                    notification_id=note.id,
                    key=key,
                    to_email=user_email,
                    title=note.title,
                    body=note.body,
                    link=note.link,
                    correlation_id=header.get("correlation_id"),
                    causation_id=header.get("event_id"),
                )
            )
            continue
        status, detail = _immediate(channel)
        await _receipt(
            session,
            org_id,
            note.id,
            key,
            channel,
            status,
            detail,
            header.get("correlation_id"),
            header.get("event_id"),
        )
    return inserted


def _immediate(channel: Channel) -> tuple[str, str | None]:
    """Channels settled inside the transaction -> (NotificationDeliveryStatus symbol, detail)."""
    if channel == "in_app":
        return "sent", None
    if channel == "slack":
        return "failed", SKIPPED_SLACK
    return "failed", SKIPPED_SMTP  # email with no SMTP configured


async def _receipt(
    session: AsyncSession,
    org_id: uuid.UUID,
    notification_id: uuid.UUID,
    key: str,
    channel: str,
    status: str,
    detail: str | None,
    correlation_id: str | None,
    causation_id: str | None,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic=DELIVERY_TOPIC,
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=key,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "notification_id": str(notification_id),
                "channel": channel,
                "status": status,
                "detail": detail,
            },
        ),
    )


async def send_pending(
    session_factory: Callable[[], Any],
    pending: list[PendingEmail | PendingSlack],
    email: EmailSender | None,
    slack: SlackSender | None = None,
) -> None:
    """Send emails and Slack DMs whose claims have committed, then record each receipt in its
    own short transaction. A mail server or Slack problem is a `failed` receipt, never an
    exception."""
    for item in pending:
        channel = "slack" if isinstance(item, PendingSlack) else "email"
        try:
            if isinstance(item, PendingSlack):
                if slack is None:
                    raise RuntimeError("no Slack sender")
                status, detail = await slack.send(
                    org_id=item.org_id,
                    email=item.to_email,
                    title=item.title,
                    body=item.body,
                    link=item.link,
                )
            else:
                if email is None:
                    raise RuntimeError("no email sender")
                await email.send(to=item.to, subject=item.subject, body=item.body)
                status, detail = "sent", None
        except Exception as exc:
            logger.warning(
                "%s delivery failed for notification %s: %s",
                channel,
                item.notification_id,
                type(exc).__name__,
            )
            prefix = "slack_error" if channel == "slack" else "smtp_error"
            status, detail = "failed", f"{prefix}:{type(exc).__name__}"
        async with session_factory() as session, session.begin():
            await set_rls(session, item.org_id)
            await _receipt(
                session,
                item.org_id,
                item.notification_id,
                item.key,
                channel,
                status,
                detail,
                item.correlation_id,
                item.causation_id,
            )


def decode(deserializer: Any, raw_value: bytes | None, ctx: Any) -> Any:
    """Decode one record; a record that can never decode becomes `PoisonMessage`."""
    try:
        return deserializer(raw_value, ctx)
    except (ValueError, TypeError, EOFError, IndexError) as exc:
        raise PoisonMessage(f"undecodable record: {exc}") from exc


async def consume_one(
    msg: Any,
    *,
    decode_value: Callable[[bytes | None, str], Any],
    session_factory: Callable[[], Any],
    redis: Redis[bytes],
    dlq_producer: Any,
    email: EmailSender | None,
    slack: SlackSender | None = None,
) -> str:
    """Decode and apply one record in its own transaction; route poison to `{topic}.dlq`.

    Returns "applied", "duplicate" or "dlq". The caller commits the offset after this returns,
    whatever the outcome, so one bad record never blocks the partition. Any other exception
    (database or Schema Registry down) propagates and the record is retried.
    """
    raw_value = msg.value()
    topic_name = msg.topic() or NOTIFICATIONS
    pending: list[PendingEmail | PendingSlack] = []
    try:
        value = decode_value(raw_value, topic_name)
        async with session_factory() as session, session.begin():
            applied = await handle_message(
                session, redis, topic_name, value, email=email, pending=pending, slack=slack
            )
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
        return "dlq"
    if pending:
        await send_pending(session_factory, pending, email, slack)  # only after the commit
    outcome = "applied" if applied else "duplicate"
    record_processed(CONSUMER_GROUP, topic_name, outcome)
    return outcome


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("notification-service-consumer")
    serve_metrics()  # no-op unless METRICS_PORT is set
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")
    redis: Redis[bytes] = redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )
    email = email_sender_from_env()
    if email is None:
        logger.info("SMTP_URL/SMTP_FROM unset: email attempts are recorded as skipped")
    slack = slack_sender_from_env()
    if slack is None:
        logger.info("no integration-service caller secret: Slack attempts are recorded as skipped")

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
                logger.error("notification-service consumer error: %s", msg.error())
                continue
            topic_name = msg.topic() or NOTIFICATIONS
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, topic_name, _ts if _ts_type else None)
            await consume_one(
                msg,
                decode_value=lambda raw, t: decode(
                    deserializer, raw, SerializationContext(t, MessageField.VALUE)
                ),
                session_factory=session_factory,
                redis=redis,
                dlq_producer=dlq_producer,
                email=email,
                slack=slack,
            )
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
