"""notification-service (P11 contract section 3; plan 10-13 section 3.8), against a real Postgres.

Consumer (`handle_message` / `consume_one`): only surfaced `ProactiveAgent` alerts become
`notification_log` rows; a `notifications` event becomes a row; redelivery and replay are no-ops;
channel preferences decide the attempts (email opt-in and off by default, skipped without SMTP,
sent with a fake SMTP adapter only after the commit, Slack skipped until P12); one
`notification-delivery-events` receipt per attempt through the outbox, valid against the
mytheus-events schema; poison raises and `consume_one` routes it to `{topic}.dlq`.

Routes: the caller's own rows only (other users and other tenants never), dismissed hidden by
default, priority-then-time with `meeting_id`, cursor pagination; PATCH owner-only (404 for anyone
else), idempotent, one audit row per real change; preferences defaults, upsert, in_app forced on.
RLS isolation for both tables, failing closed with no `app.current_org`.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import fakeredis.aioredis
import fastavro
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.tenancy import set_rls
from services.notification_service import routes
from services.notification_service.consumer import (
    NOTIFICATION_NS,
    PendingEmail,
    PoisonMessage,
    consume_one,
    handle_message,
    send_pending,
)
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

# Verbatim from mytheus-events schemas/_envelope/EventEnvelopeHeader.avsc and
# schemas/notification-delivery-events/v1.avsc (docs trimmed). The receipt must serialize
# against exactly this, or the relay dead-letters it.
_HEADER_SCHEMA = {
    "type": "record",
    "name": "EventEnvelopeHeader",
    "namespace": "io.nexus.events",
    "fields": [
        {"name": "event_id", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "idempotency_key", "type": "string"},
        {"name": "org_id", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "schema_version", "type": "int", "default": 1},
        {"name": "producer", "type": "string"},
        {"name": "occurred_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "correlation_id", "type": ["null", "string"], "default": None},
        {"name": "causation_id", "type": ["null", "string"], "default": None},
    ],
}
_DELIVERY_SCHEMA = fastavro.parse_schema(
    {
        "type": "record",
        "name": "NotificationDeliveryEvent",
        "namespace": "io.nexus.events",
        "fields": [
            {"name": "header", "type": _HEADER_SCHEMA},
            {"name": "notification_id", "type": "string"},
            {"name": "channel", "type": "string"},
            {
                "name": "status",
                "type": {
                    "type": "enum",
                    "name": "NotificationDeliveryStatus",
                    "symbols": ["sent", "failed", "bounced"],
                },
            },
            {"name": "detail", "type": ["null", "string"], "default": None},
        ],
    }
)


# --- builders --------------------------------------------------------------------------------


def _header(t: Tenant, key: str, producer: str = "agent-runtime-service") -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "idempotency_key": key,
        "org_id": str(t.org_id),
        "schema_version": 1,
        "producer": producer,
        "occurred_at": 0,
        "correlation_id": None,
        "causation_id": None,
    }


def alert_obj(
    *,
    alert_type: str = "conflict",
    priority: int = 1,
    surfaced: bool = True,
    confidence: float = 0.86,
    link: str | None = "/decisions/x",
    **extra: Any,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "priority": priority,
        "title": "This decision contradicts one from last week",
        "body": "Ship on Friday conflicts with the freeze agreed on Monday.",
        "confidence": confidence,
        "surfaced": surfaced,
        "rationale": "Both decisions name the same release window.",
        "subject": {"kind": "decision", "id": str(uuid.uuid4())},
        "related": [{"kind": "decision", "id": str(uuid.uuid4()), "label": "Code freeze"}],
        "link": link,
        **extra,
    }


def alert_result(
    t: Tenant,
    meeting_id: uuid.UUID,
    alert: dict[str, Any],
    *,
    recipient: uuid.UUID | None = None,
    agent: str = "ProactiveAgent",
    kind: str = "alert",
) -> dict[str, Any]:
    uid = recipient or t.user_id
    window = f"proactive:{alert.get('alert_type')}:{alert.get('subject', {}).get('id')}"
    return {
        "header": _header(t, f"agentresult:{meeting_id}:{window}:{uid}:{agent}"),
        "meeting_id": str(meeting_id),
        "window_id": window,
        "user_id": str(uid),
        "agent": agent,
        "kind": kind,
        "relevance_score": alert.get("confidence", 0.0),
        "confidence": alert.get("confidence", 0.0),
        "content": alert.get("title", ""),
        "payload_json": json.dumps(
            {"alert": alert, "prompt": "proactive_conflict@v1", "model_tier": "sonnet"}
        ),
        "source_chunk_ids": [],
    }


def notification_value(
    t: Tenant,
    *,
    user_id: uuid.UUID | None = None,
    source_event_id: str | None = None,
    channel: str = "in_app",
    priority: int = 2,
) -> dict[str, Any]:
    uid = user_id or t.user_id
    sid = source_event_id or f"budget:{t.org_id}:2026-11:budget_80"
    return {
        "header": _header(t, f"notif:{sid}:{channel}:{uid}", "billing-metering-service"),
        "source_event_id": sid,
        "channel": channel,
        "user_id": str(uid),
        "priority": priority,
        "title": "80% of this month's AI budget used",
        "body": "Agents keep running.",
        "link": "/settings/usage",
    }


class FakeEmail:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[dict[str, str]] = []
        self.fail = fail

    async def send(self, *, to: str, subject: str, body: str) -> None:
        if self.fail:
            raise ConnectionRefusedError("mail server down")
        self.sent.append({"to": to, "subject": subject, "body": body})


async def apply(
    topic: str, value: dict[str, Any], redis: Any | None = None, email: Any | None = None
) -> bool:
    """One message the way `consume_one` runs it: the transaction, then emails after commit."""
    engine = create_async_engine(APP_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    pending: list[PendingEmail] = []
    try:
        async with factory() as session, session.begin():
            applied = await handle_message(
                session,
                redis or fakeredis.aioredis.FakeRedis(),
                topic,
                value,
                email=email,
                pending=pending,
            )
        if email is not None and pending:
            await send_pending(factory, pending, email)
        return applied
    finally:
        await engine.dispose()


def log_rows(user_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM notification_log WHERE user_id = %(u)s ORDER BY created_at, id",
        {"u": str(user_id)},
    )


def receipts(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
        "AND topic = 'notification-delivery-events' ORDER BY id",
        {"o": str(org_id)},
    )


def by_channel(org_id: uuid.UUID) -> dict[str, tuple[str, str | None]]:
    return {
        r["payload"]["channel"]: (r["payload"]["status"], r["payload"]["detail"])
        for r in receipts(org_id)
    }


def set_pref(t: Tenant, channel: str, enabled: bool, max_priority: int) -> None:
    _sql(
        "INSERT INTO notification_preferences (org_id, user_id, channel, enabled, max_priority) "
        "VALUES (%(o)s, %(u)s, %(c)s, %(e)s, %(p)s) ON CONFLICT (org_id, user_id, channel) "
        "DO UPDATE SET enabled = EXCLUDED.enabled, max_priority = EXCLUDED.max_priority",
        {"o": str(t.org_id), "u": str(t.user_id), "c": channel, "e": enabled, "p": max_priority},
    )


# --- consumer: what becomes a row ------------------------------------------------------------


async def test_a_surfaced_proactive_alert_becomes_one_row_for_its_recipient():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    alert = alert_obj(alert_type="workload", priority=2)
    assert await apply("agent-results", alert_result(t, m, alert)) is True
    [row] = log_rows(t.user_id)
    assert str(row["id"]) == alert["id"] and row["org_id"] == str(t.org_id)
    assert row["meeting_id"] == str(m) and row["source"] == "agent_alert"
    assert (row["alert_type"], row["priority"], row["status"]) == ("workload", 2, "unread")
    assert row["title"] == alert["title"] and row["body"] == alert["body"]
    assert row["link"] == "/decisions/x"
    assert row["payload"] == alert  # the full alert object, for the UI


async def test_only_surfaced_proactive_alerts_become_rows():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    unsurfaced = alert_obj(surfaced=False, confidence=0.55)
    assert await apply("agent-results", alert_result(t, m, unsurfaced)) is False
    suggestion = alert_result(t, m, alert_obj(), agent="QuestionAgent", kind="suggestion")
    assert await apply("agent-results", suggestion) is False
    note = alert_result(t, m, alert_obj(), agent="PersonalListenerAgent", kind="note")
    assert await apply("agent-results", note) is False
    assert log_rows(t.user_id) == [] and receipts(t.org_id) == []
    # ignored results never touch the inbox either
    assert _sql("SELECT 1 FROM inbox WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []


async def test_an_off_site_link_is_dropped():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    for link in ("https://evil.example/x", "//evil.example/x"):
        assert await apply("agent-results", alert_result(t, m, alert_obj(link=link)))
    assert [r["link"] for r in log_rows(t.user_id)] == [None, None]


async def test_a_notifications_event_becomes_a_row_with_an_in_app_receipt():
    t = seed_tenant()
    value = notification_value(t, priority=2)
    assert await apply("notifications", value) is True
    [row] = log_rows(t.user_id)
    expected_id = uuid.uuid5(
        NOTIFICATION_NS, f"notifications:{value['source_event_id']}:{t.user_id}"
    )
    assert row["id"] == str(expected_id)
    assert (row["source"], row["alert_type"], row["meeting_id"]) == ("notification", None, None)
    assert row["priority"] == 2 and row["link"] == "/settings/usage"
    assert row["payload"]["source_event_id"] == value["source_event_id"]
    # email is opt-in and this user never opted in, so only in_app is attempted
    assert by_channel(t.org_id) == {"in_app": ("sent", None)}


# --- consumer: idempotency -------------------------------------------------------------------


async def test_redelivery_and_replay_are_no_ops():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    set_pref(t, "email", True, 1)
    redis = fakeredis.aioredis.FakeRedis()
    email = FakeEmail()
    value = alert_result(t, m, alert_obj(priority=1))
    assert await apply("agent-results", value, redis, email) is True
    assert await apply("agent-results", value, redis, email) is False  # Redis fast path
    assert await apply("agent-results", value, None, email) is False  # inbox, cache evicted
    replay = json.loads(json.dumps(value))
    replay["header"]["idempotency_key"] = f"replay:{uuid.uuid4()}"
    assert await apply("agent-results", replay, None, email) is False  # alert.id is the row key
    assert len(log_rows(t.user_id)) == 1
    assert len(receipts(t.org_id)) == 2  # in_app + email, once each
    assert len(email.sent) == 1


async def test_a_second_intent_for_the_same_source_attempts_no_channel_twice():
    t = seed_tenant()
    set_pref(t, "email", True, 1)
    first = notification_value(t, channel="in_app", priority=1)
    again = notification_value(t, channel="email", priority=1)  # same source, other channel
    email = FakeEmail()
    assert await apply("notifications", first, email=email) is True
    assert await apply("notifications", again, email=email) is False
    assert len(log_rows(t.user_id)) == 1 and len(email.sent) == 1
    assert sorted(r["idempotency_key"] for r in receipts(t.org_id)) == sorted(
        f"notif:{first['source_event_id']}:{c}:{t.user_id}" for c in ("email", "in_app")
    )


# --- consumer: channel preferences -----------------------------------------------------------


async def test_email_is_off_by_default_even_for_priority_1():
    t = seed_tenant()
    email = FakeEmail()
    assert await apply(
        "agent-results", alert_result(t, seed_meeting(t.org_id), alert_obj()), email=email
    )
    assert by_channel(t.org_id) == {"in_app": ("sent", None)} and email.sent == []


async def test_email_is_sent_only_after_the_commit():
    t = seed_tenant()
    set_pref(t, "email", True, 1)
    email = FakeEmail()
    engine = create_async_engine(APP_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    pending: list[PendingEmail] = []
    value = alert_result(t, seed_meeting(t.org_id), alert_obj(priority=1))
    try:
        with pytest.raises(RuntimeError):
            async with factory() as session, session.begin():
                await handle_message(
                    session,
                    fakeredis.aioredis.FakeRedis(),
                    "agent-results",
                    value,
                    email=email,
                    pending=pending,
                )
                assert len(pending) == 1 and email.sent == []  # queued, not sent
                raise RuntimeError("the commit never happens")
    finally:
        await engine.dispose()
    assert email.sent == [] and log_rows(t.user_id) == []  # rolled back: nothing went out
    assert await apply("agent-results", value, email=email) is True  # redelivery applies
    assert len(email.sent) == 1 and by_channel(t.org_id)["email"] == ("sent", None)


async def test_email_is_skipped_without_smtp_and_sent_with_an_adapter():
    t = seed_tenant()
    set_pref(t, "email", True, 1)
    m = seed_meeting(t.org_id)
    assert await apply("agent-results", alert_result(t, m, alert_obj(priority=1)), email=None)
    assert by_channel(t.org_id) == {
        "in_app": ("sent", None),
        "email": ("failed", "skipped:smtp_not_configured"),
    }

    u = seed_tenant()
    set_pref(u, "email", True, 1)
    _sql("UPDATE users SET email = 'priya@example.com' WHERE id = %(u)s", {"u": str(u.user_id)})
    m2 = seed_meeting(u.org_id)
    email = FakeEmail()
    alert = alert_obj(priority=1, link="/decisions/abc")
    assert await apply("agent-results", alert_result(u, m2, alert), email=email)
    assert by_channel(u.org_id) == {"in_app": ("sent", None), "email": ("sent", None)}
    [sent] = email.sent
    assert sent["to"] == "priya@example.com" and sent["subject"] == alert["title"]
    assert alert["body"] in sent["body"]


async def test_preferences_decide_which_channels_are_attempted():
    # email off: in_app only, even for priority 1
    t = seed_tenant()
    set_pref(t, "email", False, 3)
    email = FakeEmail()
    assert await apply("notifications", notification_value(t, priority=1), email=email)
    assert by_channel(t.org_id) == {"in_app": ("sent", None)} and email.sent == []

    # email for everything, slack on: slack is recorded as skipped until P12
    u = seed_tenant()
    set_pref(u, "email", True, 3)
    set_pref(u, "slack", True, 3)
    assert await apply("notifications", notification_value(u, priority=3), email=email)
    assert by_channel(u.org_id) == {
        "in_app": ("sent", None),
        "email": ("sent", None),
        "slack": ("failed", "skipped:slack_not_connected"),
    }

    # a stored in_app opt-out is ignored: the row is the inbox
    w = seed_tenant()
    set_pref(w, "in_app", False, 1)
    assert await apply("notifications", notification_value(w, priority=3))
    assert len(log_rows(w.user_id)) == 1 and "in_app" in by_channel(w.org_id)


async def test_a_mail_server_failure_is_a_failed_receipt_not_a_crash():
    t = seed_tenant()
    set_pref(t, "email", True, 1)
    assert await apply("notifications", notification_value(t, priority=1), email=FakeEmail(True))
    assert by_channel(t.org_id)["email"] == ("failed", "smtp_error:ConnectionRefusedError")


async def test_one_receipt_per_attempt_matching_the_delivery_event_schema():
    t = seed_tenant()
    set_pref(t, "slack", True, 3)
    set_pref(t, "email", True, 1)
    m = seed_meeting(t.org_id)
    value = alert_result(t, m, alert_obj(priority=1))
    assert await apply("agent-results", value, email=FakeEmail())
    rows = receipts(t.org_id)
    assert len(rows) == 3
    alert_id = json.loads(value["payload_json"])["alert"]["id"]
    for r in rows:
        payload = r["payload"]
        fastavro.validate(payload, _DELIVERY_SCHEMA, raise_errors=True)
        assert payload["notification_id"] == alert_id
        assert payload["header"]["producer"] == "notification-service"
        assert payload["header"]["causation_id"] == value["header"]["event_id"]
        assert r["idempotency_key"] == f"notif:{alert_id}:{payload['channel']}:{t.user_id}"


# --- consumer: poison ------------------------------------------------------------------------


async def test_poison_messages_raise():
    a, b = seed_tenant(), seed_tenant()
    m_a, m_b = seed_meeting(a.org_id), seed_meeting(b.org_id)
    cases = []
    no_header = alert_result(a, m_a, alert_obj())
    no_header["header"]["org_id"] = None
    cases.append(no_header)
    bad_json = alert_result(a, m_a, alert_obj())
    bad_json["payload_json"] = "{nope"
    cases.append(bad_json)
    cases.append(alert_result(a, m_a, alert_obj(alert_type="gossip")))
    cases.append(alert_result(a, m_a, alert_obj(id="not-a-uuid")))
    cases.append(alert_result(a, m_a, alert_obj(title="  ")))
    cases.append(alert_result(a, m_b, alert_obj()))  # another tenant's meeting
    cases.append(alert_result(a, m_a, alert_obj(), recipient=b.user_id))  # another tenant's user
    unknown_org = alert_result(a, m_a, alert_obj())
    unknown_org["header"]["org_id"] = str(uuid.uuid4())
    cases.append(unknown_org)
    for value in cases:
        with pytest.raises(PoisonMessage):
            await apply("agent-results", value)
    with pytest.raises(PoisonMessage):
        await apply("notifications", notification_value(a, user_id=b.user_id))
    assert log_rows(a.user_id) == [] and log_rows(b.user_id) == []


async def test_a_failed_apply_releases_its_dedupe_keys():
    t, other = seed_tenant(), seed_tenant()
    redis = fakeredis.aioredis.FakeRedis()
    value = notification_value(t, user_id=other.user_id)
    with pytest.raises(PoisonMessage):
        await apply("notifications", value, redis)
    assert await redis.keys("*") == []


class _Msg:
    def __init__(self, topic: str, raw: bytes) -> None:
        self._topic, self._raw = topic, raw

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._raw

    def key(self) -> bytes:
        return b"k"


class _Producer:
    def __init__(self) -> None:
        self.produced: list[dict[str, Any]] = []

    def produce(self, **kw: Any) -> None:
        self.produced.append(kw)

    def flush(self, timeout: float) -> int:
        return 0


async def test_consume_one_routes_poison_to_the_dlq_and_applies_good_records():
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    engine = create_async_engine(APP_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    redis = fakeredis.aioredis.FakeRedis()
    dlq = _Producer()
    good = alert_result(t, m, alert_obj())
    bad = alert_result(t, m, alert_obj())
    bad["payload_json"] = "{nope"

    def decode(raw: bytes | None, _topic: str) -> Any:
        if raw == b"undecodable":
            raise PoisonMessage("undecodable record")
        return json.loads(raw or b"")

    try:
        kw: dict[str, Any] = {
            "decode_value": decode,
            "session_factory": factory,
            "redis": redis,
            "dlq_producer": dlq,
            "email": None,
        }
        assert await consume_one(_Msg("agent-results", b"undecodable"), **kw) == "dlq"
        assert await consume_one(_Msg("agent-results", json.dumps(bad).encode()), **kw) == "dlq"
        raw_good = json.dumps(good).encode()
        assert await consume_one(_Msg("agent-results", raw_good), **kw) == "applied"
        assert await consume_one(_Msg("agent-results", raw_good), **kw) == "duplicate"
    finally:
        await engine.dispose()
    assert [p["topic"] for p in dlq.produced] == ["agent-results.dlq", "agent-results.dlq"]
    assert json.loads(dlq.produced[1]["value"])["raw"] == json.dumps(bad).encode().hex()
    assert len(log_rows(t.user_id)) == 1


# --- RLS -------------------------------------------------------------------------------------


def seed_row(
    t: Tenant,
    *,
    meeting_id: uuid.UUID | None = None,
    priority: int = 2,
    status: str = "unread",
    created: str = "now()",
    title: str = "T",
) -> uuid.UUID:
    nid = uuid.uuid4()
    _sql(
        "INSERT INTO notification_log (id, org_id, user_id, meeting_id, source, alert_type, "
        "priority, title, body, payload, status, created_at) VALUES (%(i)s, %(o)s, %(u)s, "
        f"%(m)s, 'agent_alert', 'conflict', %(p)s, %(t)s, 'B', '{{}}', %(s)s, {created})",
        {
            "i": str(nid),
            "o": str(t.org_id),
            "u": str(t.user_id),
            "m": str(meeting_id) if meeting_id else None,
            "p": priority,
            "t": title,
            "s": status,
        },
    )
    return nid


@pytest.mark.parametrize("table", ["notification_log", "notification_preferences"])
async def test_rls_isolates_and_fails_closed(table):
    a, b = seed_tenant(), seed_tenant()
    for t in (a, b):
        seed_row(t)
        set_pref(t, "email", True, 2)
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {r[0] for r in await conn.execute(text(f"SELECT org_id FROM {table}"))}
            assert orgs == {a.org_id}
        async with engine.connect() as conn, conn.begin():
            assert (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar() == 0
    finally:
        await engine.dispose()


# --- routes ----------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("notification-service-test")
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


def _ids(resp: Any) -> list[str]:
    assert resp.status_code == 200, resp.text
    return [i["id"] for i in resp.json()["items"]]


def test_list_is_my_own_rows_newest_first_without_dismissed(client):
    me = seed_tenant()
    colleague = seed_user(me)
    stranger = seed_tenant()
    old = seed_row(me, created="now() - interval '2 hours'")
    new = seed_row(me, created="now() - interval '1 hour'")
    gone = seed_row(me, status="dismissed")
    seed_row(colleague)
    seed_row(stranger)
    sign_in(me)
    assert _ids(client.get("/notifications")) == [str(new), str(old)]
    assert _ids(client.get("/notifications", params={"status": "dismissed"})) == [str(gone)]
    item = client.get("/notifications").json()["items"][0]
    assert set(item) == {
        "id",
        "meeting_id",
        "source",
        "alert_type",
        "priority",
        "title",
        "body",
        "link",
        "payload",
        "status",
        "created_at",
        "read_at",
        "dismissed_at",
    }


def test_meeting_view_is_priority_first_then_newest_and_paginates(client):
    me = seed_tenant()
    m, other_meeting = seed_meeting(me.org_id), seed_meeting(me.org_id)
    blue_old = seed_row(me, meeting_id=m, priority=3, created="now() - interval '3 hours'")
    blue_new = seed_row(me, meeting_id=m, priority=3, created="now() - interval '2 hours'")
    red = seed_row(me, meeting_id=m, priority=1, created="now() - interval '10 hours'")
    yellow = seed_row(me, meeting_id=m, priority=2, created="now() - interval '1 hour'")
    seed_row(me, meeting_id=other_meeting, priority=1)
    sign_in(me)
    expected = [str(red), str(yellow), str(blue_new), str(blue_old)]
    assert _ids(client.get("/notifications", params={"meeting_id": str(m)})) == expected
    got: list[str] = []
    params: dict[str, Any] = {"meeting_id": str(m), "limit": 1}
    while True:
        resp = client.get("/notifications", params=params)
        got += _ids(resp)
        cursor = resp.json()["next_cursor"]
        if not cursor:
            break
        params["cursor"] = cursor
    assert got == expected
    assert client.get("/notifications", params={"cursor": "garbage"}).status_code == 400


def audits(target: uuid.UUID) -> list[str]:
    return [
        r["action"]
        for r in _sql(
            "SELECT action FROM audit_logs WHERE target_id = %(t)s ORDER BY occurred_at",
            {"t": str(target)},
        )
    ]


def test_patch_is_owner_only_idempotent_and_audited_once_per_change(client):
    me = seed_tenant()
    colleague = seed_user(me)
    stranger = seed_tenant()
    mine = seed_row(me)
    theirs = seed_row(colleague)
    foreign = seed_row(stranger)
    sign_in(me)
    url = f"/notifications/{mine}"
    first = client.patch(url, json={"status": "read"})
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "read" and first.json()["read_at"]
    again = client.patch(url, json={"status": "read"})
    assert again.status_code == 200 and again.json()["read_at"] == first.json()["read_at"]
    dismissed = client.patch(url, json={"status": "dismissed"})
    assert dismissed.json()["status"] == "dismissed" and dismissed.json()["dismissed_at"]
    assert client.patch(url, json={"status": "read"}).json()["status"] == "dismissed"
    assert client.patch(url, json={"status": "dismissed"}).status_code == 200
    assert audits(mine) == ["notification.read", "notification.dismissed"]
    assert client.get("/notifications").json()["items"] == []  # dismissed never returns

    for nid in (theirs, foreign, uuid.uuid4()):
        assert client.patch(f"/notifications/{nid}", json={"status": "read"}).status_code == 404
    assert audits(theirs) == [] and audits(foreign) == []
    assert client.patch(url, json={"status": "unread"}).status_code == 422


def test_preferences_default_upsert_and_in_app_forced(client):
    me = seed_tenant()
    sign_in(me)
    resp = client.get("/notifications/preferences")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "items": [
            {"channel": "in_app", "enabled": True, "max_priority": 3},
            {"channel": "email", "enabled": False, "max_priority": 1},
            {"channel": "slack", "enabled": False, "max_priority": 1},
        ],
        # No email provider in the test environment (Phase 4: the screen says so).
        "email_configured": False,
        "email_provider": None,
    }
    put = client.put(
        "/notifications/preferences",
        json={
            "items": [
                {"channel": "email", "enabled": True, "max_priority": 3},
                {"channel": "in_app", "enabled": False, "max_priority": 1},
            ]
        },
    )
    assert put.status_code == 200, put.text
    want = {
        "items": [
            {"channel": "in_app", "enabled": True, "max_priority": 3},
            {"channel": "email", "enabled": True, "max_priority": 3},
            {"channel": "slack", "enabled": False, "max_priority": 1},
        ],
        "email_configured": False,
        "email_provider": None,
    }
    assert put.json() == want
    assert client.get("/notifications/preferences").json() == want
    dup = {"items": [{"channel": "email", "enabled": True, "max_priority": 1}] * 2}
    assert client.put("/notifications/preferences", json=dup).status_code == 422
    bad = {"items": [{"channel": "email", "enabled": True, "max_priority": 4}]}
    assert client.put("/notifications/preferences", json=bad).status_code == 422

    # another user's preferences are untouched and unseen
    other = seed_user(me)
    sign_in(other)
    assert client.get("/notifications/preferences").json()["items"][1]["max_priority"] == 1
