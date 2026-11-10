"""P12 sync paths: Jira outbound and inbound, Google Calendar -> meetings, core-api's internal
routes, and real Slack delivery in notification-service.

Real Postgres (as `mytheus_app`), fakeredis for dedupe, fakes for every provider.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis.aioredis
import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from libs.shared.tenancy import set_rls
from services.core_api.integration_events import handle_integration_event
from services.core_api.routes import internal_integrations, internal_meetings
from services.integration_service import config, store, worker
from services.integration_service.clients import TokenGrant
from services.integration_service.connectors import calendar
from services.integration_service.consumer import handle_action
from services.notification_service.consumer import handle_message, send_pending
from services.notification_service.slack import SlackDelivery
from tests import _integration_fakes as fx
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, add_participant, seed_meeting, seed_tenant
from tests.test_proactive_internal import seed_action
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")


@pytest.fixture(autouse=True)
def _env_and_engine():
    previous_env = fx.install_env()
    previous_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    yield
    fx.restore_env(previous_env)
    if previous_db is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous_db
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
async def factory():
    engine = create_async_engine(APP_URL)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()
    await db_mod.get_engine().dispose()


async def connect(
    factory: Any, t: Tenant, provider: str, account: str, *, expires_in: int | None = 3600
) -> store.Connection:
    """A connection with real encrypted tokens, as the OAuth callback would leave it."""
    access, refresh = fx.SECRET_TOKENS[provider]
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        conn = await store.upsert_connection(
            session,
            org_id=t.org_id,
            provider=provider,
            external_account_id=account,
            scopes=["x"],
            installed_by=t.user_id,
        )
        await store.store_tokens(
            session, conn, TokenGrant(access, refresh, expires_in, ["x"], account)
        )
        if provider == config.JIRA:
            await store.put_cursor(
                session,
                t.org_id,
                conn.id,
                "jira_site",
                {
                    "cloud_id": account,
                    "site_url": "https://nexus.atlassian.net",
                    "project_key": "NEX",
                    "webhook_ids": [101],
                },
                datetime.now(UTC) + timedelta(days=30),
            )
    return conn


def header(t: Tenant, key: str, producer: str = "core-api") -> dict[str, Any]:
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


def action_value(t: Tenant, action_id: uuid.UUID, meeting_id: uuid.UUID, **extra: Any) -> dict:
    return {
        "header": header(t, f"action:{action_id}:confirmed"),
        "action_id": str(action_id),
        "meeting_id": str(meeting_id),
        "content": "Send the revised budget to finance",
        "assignee_id": None,
        "due_date": 20404,  # 2025-11-12, Avro date
        "confidence": 0.9,
        "status": "confirmed",
        **extra,
    }


async def push(factory: Any, value: dict[str, Any], clients: Any, redis: Any = None) -> str:
    async with factory() as session, session.begin():
        return await handle_action(
            session, redis or fakeredis.aioredis.FakeRedis(), value, clients=clients
        )


# --- Jira outbound ---------------------------------------------------------------------------


async def test_a_confirmed_action_creates_exactly_one_jira_issue(factory):
    t = seed_tenant("owner")
    assignee = seed_user(t)
    _sql(
        "UPDATE users SET email = 'assignee@example.com' WHERE id = %(u)s",
        {"u": str(assignee.user_id)},
    )
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    meeting = seed_meeting(t.org_id)
    action = uuid.uuid4()
    value = action_value(t, action, meeting, assignee_id=str(assignee.user_id))

    assert await push(factory, value, clients) == "created"
    [fields] = clients.jira.created
    assert fields["summary"] == "Send the revised budget to finance"
    assert fields["labels"] == [f"nexus:{action}"]
    assert fields["duedate"] == "2025-11-12"
    assert fields["assignee"] == {"accountId": "acc-1"}
    assert fields["project"] == {"key": "NEX"}
    link = json.dumps(fields["description"])
    assert f"https://app.mytheus.test/meetings/{meeting}" in link
    [mapped] = _sql(
        "SELECT external_id, external_url, last_synced_status FROM integration_id_map "
        "WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert mapped == {
        "external_id": "NEX-1",
        "external_url": "https://nexus.atlassian.net/browse/NEX-1",
        "last_synced_status": "confirmed",
    }

    # Redelivery: same envelope, new consumer process (no Redis key) -> inbox says duplicate.
    assert await push(factory, value, clients) == "duplicate"
    # A replay under a fresh envelope key reaches the handler, which finds the id map row.
    value2 = {**value, "header": header(t, f"replay:{uuid.uuid4()}")}
    assert await push(factory, value2, clients) == "exists"
    assert len(clients.jira.created) == 1


async def test_the_label_search_fallback_links_instead_of_duplicating(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    # The crash window: the issue exists in Jira, the id map row never committed.
    clients.jira.issues.append(
        {
            "id": "1",
            "key": "NEX-77",
            "fields": {"labels": [f"nexus:{action}"], "status": {"statusCategory": {"key": "new"}}},
        }
    )
    assert await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients) == "linked"
    assert clients.jira.created == []
    [mapped] = _sql(
        "SELECT external_id FROM integration_id_map WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )
    assert mapped["external_id"] == "NEX-77"


async def test_no_jira_connection_means_no_issue(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    value = action_value(t, uuid.uuid4(), seed_meeting(t.org_id))
    assert await push(factory, value, clients) == "not_connected"
    assert clients.jira.created == []


async def test_an_expiring_token_is_refreshed_and_versioned(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    conn = await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}", expires_in=10)
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        tokens = await store.access_token(session, conn, clients.oauth["jira"])
    assert tokens.access_token == "refreshed-jira-ACCESS-secret"
    assert tokens.token_version == 2
    [row] = _sql(
        "SELECT token_version FROM oauth_tokens WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )
    assert row["token_version"] == 2


# --- Jira inbound ----------------------------------------------------------------------------


def jira_delivery(
    t: Tenant, key: str, category: str, action: uuid.UUID, clients: Any = None
) -> uuid.UUID:
    """A verified `jira:issue_updated` inbox row. With `clients`, Jira's own copy of the issue
    is moved to the same status (the worker re-reads it and trusts only that read)."""
    if clients is not None:
        clients.jira.set_status(key, category)
    rid = uuid.uuid4()
    payload = {
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "key": key,
            "fields": {
                "labels": [f"nexus:{action}"],
                "status": {"statusCategory": {"key": category}},
            },
        },
    }
    _sql(
        "INSERT INTO integration_webhook_inbox (id, org_id, provider, delivery_id, event_type, "
        "payload) VALUES (%(i)s, %(o)s, 'jira', %(d)s, 'jira:issue_updated', %(p)s::jsonb)",
        {"i": str(rid), "o": str(t.org_id), "d": str(rid), "p": json.dumps(payload)},
    )
    return rid


async def test_inbound_done_calls_core_once_and_the_echo_is_suppressed(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients)
    core = fx.FakeCore()

    first = jira_delivery(t, "NEX-1", "done", action, clients)
    assert await worker.process_row(t.org_id, first, clients=clients, core=core) == "processed"
    assert [(c[1], c[2], c[3]) for c in core.status_calls] == [(action, "done", "NEX-1")]
    [mapped] = _sql(
        "SELECT last_synced_status FROM integration_id_map WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert mapped["last_synced_status"] == "done"

    # A second delivery with the same status (Jira echoing, or a field edit) is suppressed.
    echo = jira_delivery(t, "NEX-1", "done", action, clients)
    assert await worker.process_row(t.org_id, echo, clients=clients, core=core) == "skipped"
    # "In progress" maps to confirmed, which differs from done: reopened in NEXUS.
    reopened = jira_delivery(t, "NEX-1", "indeterminate", action, clients)
    assert await worker.process_row(t.org_id, reopened, clients=clients, core=core) == "processed"
    assert [c[2] for c in core.status_calls] == ["done", "confirmed"]
    # The initial "To Do" state equals the confirmed watermark: suppressed.
    todo = jira_delivery(t, "NEX-1", "new", action, clients)
    assert await worker.process_row(t.org_id, todo, clients=clients, core=core) == "skipped"
    assert len(core.status_calls) == 2
    statuses = _sql(
        "SELECT status FROM integration_webhook_inbox WHERE org_id = %(o)s ORDER BY received_at",
        {"o": str(t.org_id)},
    )
    assert [s["status"] for s in statuses] == ["processed", "skipped", "processed", "skipped"]


async def test_the_worker_drains_pending_rows_per_tenant(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients)
    core = fx.FakeCore()
    jira_delivery(t, "NEX-1", "done", action, clients)
    jira_delivery(t, "NEX-1", "done", action, clients)
    summary = await worker.drain_once(clients=clients, core=core, org_ids=[t.org_id])
    assert summary == {"processed": 1, "skipped": 1}
    assert len(core.status_calls) == 1
    assert await worker.drain_once(clients=clients, core=core, org_ids=[t.org_id]) == {}


async def test_the_status_comes_from_jira_not_from_the_webhook_body(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients)
    core = fx.FakeCore()
    # The body claims Done; Jira itself still has the issue in To Do.
    lying = jira_delivery(t, "NEX-1", "done", action)
    assert await worker.process_row(t.org_id, lying, clients=clients, core=core) == "skipped"
    assert core.status_calls == [] and clients.jira.reads == ["NEX-1"]


async def test_a_nexus_label_on_another_issue_cannot_flip_the_action(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients)
    # Someone copies the label onto an unrelated issue and closes it.
    clients.jira.issues.append(
        {
            "id": "2",
            "key": "NEX-2",
            "fields": {
                "labels": [f"nexus:{action}"],
                "status": {"statusCategory": {"key": "done"}},
            },
        }
    )
    core = fx.FakeCore()
    rid = jira_delivery(t, "NEX-2", "done", action)
    assert await worker.process_row(t.org_id, rid, clients=clients, core=core) == "skipped"
    assert core.status_calls == []
    [mapped] = _sql(
        "SELECT external_id, last_synced_status FROM integration_id_map WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert mapped == {"external_id": "NEX-1", "last_synced_status": "confirmed"}


async def test_an_issue_this_tenant_did_not_create_is_ignored(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    core = fx.FakeCore()
    stray = jira_delivery(t, "OTHER-9", "done", uuid.uuid4())
    assert await worker.process_row(t.org_id, stray, clients=clients, core=core) == "skipped"
    assert core.status_calls == []


async def test_a_failing_row_is_retried_then_parked(factory):
    t = seed_tenant("owner")
    clients = fx.fake_clients()
    await connect(factory, t, "jira", f"cloud-{uuid.uuid4().hex[:6]}")
    action = uuid.uuid4()
    await push(factory, action_value(t, action, seed_meeting(t.org_id)), clients)

    class DownCore(fx.FakeCore):
        async def external_status(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            from services.integration_service.core_client import CoreApiError

            raise CoreApiError(503, "HTTP 503")

    rid = jira_delivery(t, "NEX-1", "done", action, clients)
    for _ in range(worker.MAX_ATTEMPTS - 1):
        assert (
            await worker.process_row(t.org_id, rid, clients=clients, core=DownCore()) == "pending"
        )
    assert await worker.process_row(t.org_id, rid, clients=clients, core=DownCore()) == "failed"
    [row] = _sql(
        "SELECT attempts, last_error FROM integration_webhook_inbox WHERE id = %(i)s",
        {"i": str(rid)},
    )
    assert row["attempts"] == worker.MAX_ATTEMPTS and "CoreApiError" in row["last_error"]


# --- Google Calendar -> meetings -------------------------------------------------------------


def calendar_event(event_id: str, attendee_emails: list[str], **extra: Any) -> dict[str, Any]:
    return {
        "id": event_id,
        "status": "confirmed",
        "summary": "Quarterly planning",
        "updated": "2026-11-10T09:00:00.000Z",
        "start": {"dateTime": "2026-11-12T10:00:00Z"},
        "end": {"dateTime": "2026-11-12T11:00:00Z"},
        "organizer": {"email": attendee_emails[0]},
        "attendees": [{"email": e} for e in attendee_emails],
        **extra,
    }


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put(self, key: str, data: bytes, content_type: str | None) -> None:
        self.objects[key] = data


async def test_a_calendar_sync_emits_the_event_and_its_attachment(factory):
    t = seed_tenant("owner")
    alice = seed_user(t)
    _sql("UPDATE users SET email = 'Alice@Example.com' WHERE id = %(u)s", {"u": str(alice.user_id)})
    clients = fx.fake_clients()
    conn = await connect(factory, t, "google_calendar", f"o-{uuid.uuid4().hex[:6]}@example.com")
    event_id = f"evt{uuid.uuid4().hex[:8]}"
    clients.calendar.events = [
        calendar_event(
            event_id,
            ["alice@example.com", "guest@outside.com"],
            attachments=[
                {"fileId": "f1", "title": "Plan.pdf", "mimeType": "application/pdf"},
                {"fileId": "f2", "title": "Theirs.pdf", "mimeType": "application/pdf"},
            ],
        ),
        {"id": "gone", "status": "cancelled"},
    ]
    clients.calendar.owners = {"f1": ["alice@example.com"], "f2": ["guest@outside.com"]}
    blobs = MemoryStore()
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        processed = await calendar.process(session, t.org_id, {}, "initial_sync", clients, blobs)
    assert processed == "processed"
    meeting_id = calendar.meeting_id_for(t.org_id, event_id)
    key = f"{t.org_id}/{meeting_id}/f1-Plan.pdf"
    assert blobs.objects == {key: b"%PDF-1.4 attachment bytes"}
    rows = _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s "
        "AND topic = 'integration-events' ORDER BY id",
        {"o": str(t.org_id)},
    )
    kinds = {r["payload"]["event_type"]: r["payload"] for r in rows}
    assert set(kinds) == {"calendar_event_upserted", "attachment_to_ingest"}
    ingest = json.loads(kinds["attachment_to_ingest"]["payload_json"])
    assert ingest == {"meeting_id": str(meeting_id), "object_key": key, "filename": "Plan.pdf"}
    upsert = json.loads(kinds["calendar_event_upserted"]["payload_json"])
    assert upsert["meeting_id"] == str(meeting_id)
    assert upsert["attendees"] == [
        {"email": "alice@example.com", "user_id": str(alice.user_id)},
        {"email": "guest@outside.com", "user_id": None},
    ]
    assert kinds["calendar_event_upserted"]["external_id"] == event_id
    # The guest's file is listed on the meeting but never downloaded or ingested.
    assert [(a["file_id"], a["object_key"]) for a in upsert["attachments"]] == [
        ("f1", key),
        ("f2", None),
    ]
    [cursor] = _sql(
        "SELECT cursor FROM integration_cursors WHERE connection_id = %(c)s AND kind = 'gcal_sync'",
        {"c": str(conn.id)},
    )
    assert cursor["cursor"] == {"sync_token": "sync-1"}

    # The next push syncs from the stored token, and the same event version emits nothing new.
    _sql(
        "INSERT INTO integration_cursors (org_id, connection_id, kind, cursor) VALUES "
        "(%(o)s, %(c)s, 'gcal_channel', '{\"channel_id\": \"ch-1\"}'::jsonb)",
        {"o": str(t.org_id), "c": str(conn.id)},
    )
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        stale = await calendar.process(
            session, t.org_id, {"channel_id": "old"}, "exists", clients, blobs
        )
        assert stale == "skipped"
        await calendar.process(session, t.org_id, {"channel_id": "ch-1"}, "exists", clients, blobs)
    assert clients.calendar.list_calls == [None, "sync-1"]
    assert clients.calendar.downloads == ["f1"]
    again = _sql(
        "SELECT count(*) AS n FROM outbox WHERE org_id = %(o)s AND topic = 'integration-events'",
        {"o": str(t.org_id)},
    )
    assert again[0]["n"] == 2


async def test_an_event_organized_outside_the_tenant_is_not_ingested(factory):
    t = seed_tenant("owner")
    alice = seed_user(t)
    _sql("UPDATE users SET email = 'alice@example.com' WHERE id = %(u)s", {"u": str(alice.user_id)})
    clients = fx.fake_clients()
    await connect(factory, t, "google_calendar", f"o-{uuid.uuid4().hex[:6]}@example.com")
    clients.calendar.events = [
        calendar_event(
            "evt-outsider",
            ["stranger@outside.com", "alice@example.com"],
            attachments=[{"fileId": "f9", "title": "x.pdf", "mimeType": "application/pdf"}],
        )
    ]
    clients.calendar.owners = {"f9": ["alice@example.com"]}
    async with factory() as session, session.begin():
        await set_rls(session, t.org_id)
        await calendar.process(session, t.org_id, {}, "initial_sync", clients, MemoryStore())
    assert (
        _sql(
            "SELECT 1 FROM outbox WHERE org_id = %(o)s AND topic = 'integration-events'",
            {"o": str(t.org_id)},
        )
        == []
    )
    assert clients.calendar.downloads == []


def upserted_value(
    t: Tenant, event_id: str, payload: dict[str, Any], key: str | None = None
) -> dict[str, Any]:
    return {
        "header": header(t, key or f"gcal:{event_id}:{uuid.uuid4()}", "integration-service"),
        "provider": "google_calendar",
        "event_type": "calendar_event_upserted",
        "external_id": event_id,
        "payload_json": json.dumps(payload),
    }


async def test_core_creates_and_patches_one_meeting_per_calendar_event(factory):
    t = seed_tenant("owner")
    bob = seed_user(t)
    _sql("UPDATE users SET email = 'Bob@Example.com' WHERE id = %(u)s", {"u": str(bob.user_id)})
    _sql("UPDATE users SET email = 'owner@example.com' WHERE id = %(u)s", {"u": str(t.user_id)})
    event_id = f"evt{uuid.uuid4().hex[:8]}"
    meeting_id = calendar.meeting_id_for(t.org_id, event_id)
    payload = {
        "meeting_id": str(meeting_id),
        "title": "Quarterly planning",
        "starts_at": "2026-11-12T10:00:00Z",
        "ends_at": "2026-11-12T11:00:00Z",
        "attendees": [
            {"email": "bob@example.com", "user_id": None},
            {"email": "stranger@outside.com", "user_id": str(uuid.uuid4())},  # hint ignored
        ],
        "organizer_email": "owner@example.com",
        "attachments": [],
    }
    redis = fakeredis.aioredis.FakeRedis()
    async with factory() as session, session.begin():
        assert await handle_integration_event(session, redis, upserted_value(t, event_id, payload))
    [meeting] = _sql(
        "SELECT id, title, scheduled_start, created_by, calendar_event_id FROM meetings "
        "WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )
    assert meeting["id"] == str(meeting_id) and meeting["created_by"] == str(t.user_id)
    assert meeting["calendar_event_id"] == event_id
    parts = _sql(
        "SELECT user_id, role_in_meeting FROM meeting_participants WHERE meeting_id = %(m)s "
        "ORDER BY role_in_meeting",
        {"m": str(meeting_id)},
    )
    assert {(p["user_id"], p["role_in_meeting"]) for p in parts} == {
        (str(bob.user_id), "attendee"),
        (str(t.user_id), "host"),
    }

    # Redelivery of the same envelope: a no-op.
    value = upserted_value(t, event_id, payload, key="fixed-key")
    async with factory() as session, session.begin():
        await handle_integration_event(session, redis, value)
    async with factory() as session, session.begin():
        assert not await handle_integration_event(session, redis, value)

    # A new version of the event patches the same meeting.
    moved = {
        **payload,
        "title": "Quarterly planning (moved)",
        "starts_at": "2026-11-13T10:00:00Z",
        "ends_at": "2026-11-13T11:00:00Z",
    }
    async with factory() as session, session.begin():
        assert await handle_integration_event(session, redis, upserted_value(t, event_id, moved))
    rows = _sql(
        "SELECT title, scheduled_start FROM meetings WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )
    assert len(rows) == 1 and rows[0]["title"] == "Quarterly planning (moved)"
    assert rows[0]["scheduled_start"].day == 13
    events = _sql(
        "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = 'meeting-events' ORDER BY id",
        {"o": str(t.org_id)},
    )
    assert [e["payload"]["event_type"] for e in events] == ["created", "updated"]
    assert (
        len(
            _sql(
                "SELECT 1 FROM meeting_participants WHERE meeting_id = %(m)s",
                {"m": str(meeting_id)},
            )
        )
        == 2
    )


async def test_core_refuses_an_event_whose_organizer_is_not_a_tenant_user(factory):
    t = seed_tenant("owner")
    event_id = f"evt{uuid.uuid4().hex[:8]}"
    payload = {
        "meeting_id": str(calendar.meeting_id_for(t.org_id, event_id)),
        "title": "Not ours",
        "starts_at": "2026-11-12T10:00:00Z",
        "ends_at": "2026-11-12T11:00:00Z",
        "attendees": [{"email": "u@example.com", "user_id": str(t.user_id)}],
        "organizer_email": "stranger@outside.com",
        "attachments": [],
    }
    async with factory() as session, session.begin():
        changed = await handle_integration_event(
            session, fakeredis.aioredis.FakeRedis(), upserted_value(t, event_id, payload)
        )
    assert changed is False
    assert _sql("SELECT 1 FROM meetings WHERE org_id = %(o)s", {"o": str(t.org_id)}) == []


async def test_core_ignores_other_integration_events(factory):
    t = seed_tenant("owner")
    value = {
        "header": header(t, "x", "integration-service"),
        "provider": "google_calendar",
        "event_type": "attachment_to_ingest",
        "external_id": "f1",
        "payload_json": "{}",
    }
    async with factory() as session, session.begin():
        assert not await handle_integration_event(session, fakeredis.aioredis.FakeRedis(), value)


# --- core-api internal routes ----------------------------------------------------------------


@pytest.fixture
def core_client():
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"integration-service={fx.CORE_SECRET_INTEG},workflow-service={'w' * 40}"
    )
    app = create_app("core-api-p12-test")
    app.include_router(internal_integrations.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)


def _core_headers(t: Tenant, service: str = "integration-service") -> dict[str, str]:
    secret = fx.CORE_SECRET_INTEG if service == "integration-service" else "w" * 40
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(t.org_id),
    }


def test_external_status_is_idempotent_audited_and_emits_nothing(core_client):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    action = seed_action(t, meeting, t.user_id)
    url = f"/internal/actions/{action}/external-status"
    body = {"status": "done", "source": "jira", "external_key": "NEX-1"}
    r = core_client.post(url, json=body, headers=_core_headers(t))
    assert r.status_code == 200 and r.json()["changed"] is True
    again = core_client.post(url, json=body, headers=_core_headers(t))
    assert again.json() == {"action_id": str(action), "status": "done", "changed": False}
    back = core_client.post(url, json={**body, "status": "confirmed"}, headers=_core_headers(t))
    assert back.json()["changed"] is True
    [row] = _sql("SELECT status FROM action_items WHERE id = %(a)s", {"a": str(action)})
    assert row["status"] == "confirmed"
    audits = _sql(
        "SELECT metadata FROM audit_logs WHERE org_id = %(o)s AND action = 'action.status_synced' "
        "ORDER BY occurred_at",
        {"o": str(t.org_id)},
    )
    assert [(a["metadata"]["from"], a["metadata"]["to"]) for a in audits] == [
        ("confirmed", "done"),
        ("done", "confirmed"),
    ]
    assert audits[0]["metadata"]["external_key"] == "NEX-1"
    assert (
        _sql(
            "SELECT 1 FROM outbox WHERE org_id = %(o)s AND topic = 'actions'", {"o": str(t.org_id)}
        )
        == []
    )


def test_external_status_404_across_tenants_409_for_proposed_and_caller_bound(core_client):
    t, other = seed_tenant("owner"), seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    action = seed_action(t, meeting, t.user_id)
    proposed = seed_action(t, meeting, t.user_id, "proposed")
    body = {"status": "done", "source": "jira", "external_key": "NEX-1"}
    r = core_client.post(
        f"/internal/actions/{action}/external-status", json=body, headers=_core_headers(other)
    )
    assert r.status_code == 404
    r = core_client.post(
        f"/internal/actions/{proposed}/external-status", json=body, headers=_core_headers(t)
    )
    assert r.status_code == 409
    r = core_client.post(
        f"/internal/actions/{action}/external-status",
        json=body,
        headers=_core_headers(t, "workflow-service"),
    )
    assert r.status_code == 401


def test_action_status_for_the_follow_up_workflow(core_client):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    _sql(
        "UPDATE meetings SET created_by = %(u)s WHERE id = %(m)s",
        {"u": str(t.user_id), "m": str(meeting)},
    )
    action = seed_action(t, meeting, t.user_id)
    r = core_client.get(
        f"/internal/actions/{action}/status", headers=_core_headers(t, "workflow-service")
    )
    assert r.status_code == 200
    assert r.json() == {
        "status": "confirmed",
        "assignee_id": str(t.user_id),
        "meeting_id": str(meeting),
        "organizer_id": str(t.user_id),
        "due_date": None,
        "content": "Do it",
    }
    assert (
        core_client.get(f"/internal/actions/{action}/status", headers=_core_headers(t)).status_code
        == 401
    )


def test_slack_digest_has_only_confirmed_items(core_client):
    t = seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    add_participant(meeting, t.org_id, t.user_id)
    seed_action(t, meeting, t.user_id)
    seed_action(t, meeting, t.user_id, "proposed")
    r = core_client.get(f"/internal/meetings/{meeting}/slack-digest", headers=_core_headers(t))
    body = r.json()
    assert len(body["actions"]) == 1 and body["actions"][0]["status"] == "confirmed"
    assert body["participants"][0]["open_actions"] == 1


# --- notification-service: real Slack delivery ----------------------------------------------


class SlackWire:
    """Plays both integration-service's token route and Slack's Web API."""

    def __init__(self, connected: bool = True) -> None:
        self.connected = connected
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/internal/slack/token":
            assert request.headers["x-mytheus-service"] == "notification-service"
            if not self.connected:
                return httpx.Response(404, json={"code": "NOT_FOUND"})
            return httpx.Response(
                200, json={"access_token": "xoxb-LIVE-SECRET", "expires_in": None, "team_id": "T"}
            )
        assert request.headers["authorization"] == "Bearer xoxb-LIVE-SECRET"
        if path.endswith("users.lookupByEmail"):
            return httpx.Response(200, json={"ok": True, "user": {"id": "U1"}})
        if path.endswith("conversations.open"):
            return httpx.Response(200, json={"ok": True, "channel": {"id": "D1"}})
        if path.endswith("chat.postMessage"):
            return httpx.Response(200, json={"ok": True, "ts": "1.2"})
        return httpx.Response(404)


def _slack_sender(wire: SlackWire) -> SlackDelivery:
    return SlackDelivery(
        integration_url="http://integration-service:8000",
        secret=fx.CALLER_SECRET_NOTIF,
        app_base_url="https://app.mytheus.test",
        transport=httpx.MockTransport(wire.handler),
        slack_api="https://slack.test/api",
    )


async def _deliver(factory: Any, t: Tenant, sender: SlackDelivery) -> None:
    value = {
        "header": header(t, f"notif:slack-prep-brief:{uuid.uuid4()}", "integration-service"),
        "source_event_id": f"slack-prep-brief:{uuid.uuid4()}",
        "channel": "slack",
        "user_id": str(t.user_id),
        "priority": 1,
        "title": "Prep brief: Roadmap",
        "body": "*Roadmap* starts soon.",
        "link": "/meetings/abc",
    }
    pending: list[Any] = []
    async with factory() as session, session.begin():
        await handle_message(
            session,
            fakeredis.aioredis.FakeRedis(),
            "notifications",
            value,
            pending=pending,
            slack=sender,
        )
    await send_pending(factory, pending, None, sender)


def _slack_receipt(t: Tenant) -> tuple[str, str | None]:
    rows = _sql(
        "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = "
        "'notification-delivery-events'",
        {"o": str(t.org_id)},
    )
    [slack] = [r["payload"] for r in rows if r["payload"]["channel"] == "slack"]
    return slack["status"], slack["detail"]


def _enable_slack(t: Tenant) -> None:
    _sql(
        "INSERT INTO notification_preferences (org_id, user_id, channel, enabled, max_priority) "
        "VALUES (%(o)s, %(u)s, 'slack', true, 3)",
        {"o": str(t.org_id), "u": str(t.user_id)},
    )


async def test_slack_delivery_uses_the_fetched_token_and_never_stores_it(factory):
    t = seed_tenant("owner")
    _enable_slack(t)
    wire = SlackWire()
    await _deliver(factory, t, _slack_sender(wire))
    assert _slack_receipt(t) == ("sent", None)
    paths = [r.url.path for r in wire.requests]
    assert paths == [
        "/internal/slack/token",
        "/api/users.lookupByEmail",
        "/api/conversations.open",
        "/api/chat.postMessage",
    ]
    posted = json.loads(wire.requests[-1].content)
    assert posted["channel"] == "D1"
    assert posted["blocks"][0]["text"]["text"] == "Prep brief: Roadmap"
    assert posted["blocks"][-1]["elements"][0]["url"] == "https://app.mytheus.test/meetings/abc"
    # The token is nowhere in the database.
    for table, col in (("outbox", "payload::text"), ("notification_log", "payload::text")):
        hits = _sql(
            f"SELECT 1 FROM {table} WHERE org_id = %(o)s AND {col} LIKE '%%xoxb-LIVE%%'",  # noqa: S608
            {"o": str(t.org_id)},
        )
        assert hits == [], table


async def test_slack_not_connected_is_recorded_as_skipped(factory):
    t = seed_tenant("owner")
    _enable_slack(t)
    await _deliver(factory, t, _slack_sender(SlackWire(connected=False)))
    assert _slack_receipt(t) == ("failed", "skipped:slack_not_connected")
