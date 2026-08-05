"""`POST /livekit/webhook`: Task 4, Plan 3-4 section 3.1 step 5.

Same structural pattern as `tests/test_meeting_token_route.py`: one module-scoped `TestClient`,
`FakeTransport` standing in for LiveKit (no account exists for this project), injected via
`app.dependency_overrides[routes.get_transport]` - `webhook.py` imports and reuses that same
`get_transport` function object, so the override applies to both routers on the one app.

The webhook event objects below are `SimpleNamespace` stand-ins, not real
`livekit.protocol.webhook.WebhookEvent` protobufs: importing the real type here would violate
ADR-0003 Decision 2 / R23 (`tests/test_livekit_import_boundary.py` - the LiveKit SDK may be
imported ONLY by `services/meeting_service/transport.py`). Their attribute names
(`event`, `room.name`, `room.metadata`, `participant.sid`, `egress_info.egress_id`) match the
real protobuf shape exactly (confirmed against the installed `livekit-api` package), which is
what `services/meeting_service/webhook.py` duck-types against via `getattr`.

Acceptance checks covered here (Task 4 brief):
  1. signed `egress_ended` -> `recording_ref` persisted with the `{org_id}/{meeting_id}/` key
     form, exactly one `meeting-room-events` outbox row.
  2. the SAME webhook redelivered is a no-op - unchanged row and outbox counts.
  3. an invalid signature is rejected and writes nothing.
  4. room metadata carrying no resolvable `org_id` fails closed and writes nothing (four
     sub-cases: absent, malformed JSON, missing org_id key, org_id names no real org).
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.meeting_service import routes, webhook
from services.meeting_service.rooms import ensure_meeting_room, sync_livekit_metadata
from services.meeting_service.transport import FakeTransport
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()
FAKE = FakeTransport()


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("meeting-service-test")
    app.include_router(routes.router)
    app.include_router(webhook.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    app.dependency_overrides[routes.get_transport] = lambda: FAKE

    with TestClient(app) as c:
        yield c

    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def seed_org(slug_prefix: str = "wh") -> uuid.UUID:
    org_id = uuid.uuid4()
    _sql(
        "INSERT INTO organizations (id, name, slug) VALUES (%(id)s, 'Org', %(slug)s)",
        {"id": str(org_id), "slug": f"{slug_prefix}-{org_id}"},
    )
    return org_id


def seed_meeting_room(org_id: uuid.UUID, *, room: str | None = None) -> tuple[uuid.UUID, str]:
    meeting_id = uuid.uuid4()
    room = room or f"meeting-{meeting_id}"
    _sql(
        "INSERT INTO meetings (id, org_id, title) VALUES (%(id)s, %(org)s, 'M')",
        {"id": str(meeting_id), "org": str(org_id)},
    )
    _sql(
        "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room) "
        "VALUES (%(org)s, %(meeting)s, %(room)s)",
        {"org": str(org_id), "meeting": str(meeting_id), "room": room},
    )
    return meeting_id, room


def _room(name: str, org_id: uuid.UUID | None, *, metadata: str | None = "__default__"):
    if metadata == "__default__":
        metadata = json.dumps({"org_id": str(org_id)}) if org_id is not None else None
    return SimpleNamespace(name=name, metadata=metadata)


def _participant(sid: str | None) -> SimpleNamespace | None:
    return SimpleNamespace(sid=sid) if sid is not None else None


def _egress_info(egress_id: str | None) -> SimpleNamespace | None:
    return SimpleNamespace(egress_id=egress_id) if egress_id is not None else None


def _evt(
    event: str,
    *,
    room,
    participant=None,
    egress_info=None,
) -> SimpleNamespace:
    return SimpleNamespace(event=event, room=room, participant=participant, egress_info=egress_info)


def outbox_rows(idempotency_key: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM outbox WHERE topic = 'meeting-room-events' AND idempotency_key = %(k)s",
        {"k": idempotency_key},
    )


def meeting_room_row(org_id: uuid.UUID, meeting_id: uuid.UUID) -> dict[str, Any]:
    rows = _sql(
        "SELECT * FROM meeting_rooms WHERE org_id = %(o)s AND meeting_id = %(m)s",
        {"o": str(org_id), "m": str(meeting_id)},
    )
    assert len(rows) == 1
    return rows[0]


@pytest.fixture(autouse=True)
def _fresh_fake():
    FAKE.calls.clear()
    FAKE.room_metadata.clear()
    FAKE.verify_webhook_result = None
    FAKE.verify_webhook_error = None


# --- acceptance check 1: signed egress_ended persists recording_ref + one outbox row ---------


def test_signed_egress_ended_persists_recording_ref_and_emits_one_outbox_row(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt(
        "egress_ended",
        room=_room(room, org_id),
        egress_info=_egress_info("EG_abc123"),
    )
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 200, resp.text

    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] == f"{org_id}/{meeting_id}/EG_abc123"
    assert row["egress_id"] == "EG_abc123"

    dedupe_key = f"{room}:egress_ended:-"
    rows = outbox_rows(dedupe_key)
    assert len(rows) == 1
    payload = rows[0]["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["meeting_id"] == str(meeting_id)
    assert payload["event_type"] == "recording_ready"
    assert payload["room"] == room
    assert payload["header"]["org_id"] == str(org_id)


# --- room-metadata follow-up: the real round trip, not two independent assumptions ------------


async def test_room_established_via_ensure_room_then_webhook_resolves_the_correct_tenant(client):
    """Closes the gap flagged after the first landing of this task: establish a room for REAL
    through `rooms.ensure_meeting_room` (a real DB upsert, not a hand-seeded row), run
    `rooms.sync_livekit_metadata` the same way `routes.py`/`consumer.py` do (out of band, after
    the upsert's own transaction has closed - fix round 1), then build the webhook event's
    `room.metadata` from EXACTLY what `FakeTransport.ensure_room` recorded, not a value this
    test re-derives independently. That makes this a genuine round trip: if `ensure_room` ever
    wrote the wrong org_id (e.g. a Clerk id instead of the internal uuid), this test would
    still pass unless it reads the SAME dict `ensure_room` wrote, which is exactly what it does.
    """
    org_id = seed_org()
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title) VALUES (%(id)s, %(org)s, 'M')",
        {"id": str(meeting_id), "org": str(org_id)},
    )

    engine = create_async_engine(APP_URL)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_id)}
            )
            result = await ensure_meeting_room(conn, org_id, meeting_id)
        assert result.needs_livekit_sync is True

        await sync_livekit_metadata(session_factory, FAKE, org_id=org_id, room=result.room)
    finally:
        await engine.dispose()

    room = result.room
    assert (
        room in FAKE.room_metadata
    ), "sync_livekit_metadata must have called transport.ensure_room"

    evt = _evt(
        "egress_ended",
        room=_room(room, None, metadata=FAKE.room_metadata[room]),
        egress_info=_egress_info("EG_roundtrip"),
    )
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 200, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] == f"{org_id}/{meeting_id}/EG_roundtrip"


# --- acceptance check 2: redelivery is a no-op -------------------------------------------------


def test_redelivered_egress_ended_is_a_no_op(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt("egress_ended", room=_room(room, org_id), egress_info=_egress_info("EG_dup"))
    FAKE.verify_webhook_result = evt

    first = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})
    second = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert first.status_code == 200
    assert second.status_code == 200

    dedupe_key = f"{room}:egress_ended:-"
    assert len(outbox_rows(dedupe_key)) == 1, "redelivery must not create a second outbox row"

    rows = _sql(
        "SELECT count(*) AS n FROM meeting_rooms WHERE org_id = %(o)s AND meeting_id = %(m)s",
        {"o": str(org_id), "m": str(meeting_id)},
    )
    assert rows[0]["n"] == 1, "redelivery must not create a second meeting_rooms row"


# --- acceptance check 3: invalid signature is rejected, writes nothing -------------------------


def test_invalid_signature_is_rejected_and_writes_nothing(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    FAKE.verify_webhook_error = ValueError("bad signature")

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "bad-sig"})

    assert resp.status_code == 401, resp.text
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert resp.json()["code"] == "AUTH_REQUIRED"

    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None
    assert len(outbox_rows(f"{room}:egress_ended:-")) == 0


# --- acceptance check 4: unresolvable org_id fails closed, writes nothing ----------------------


def test_absent_room_metadata_fails_closed(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt(
        "egress_ended", room=_room(room, None, metadata=None), egress_info=_egress_info("EG_x")
    )
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == "MALFORMED_BODY"
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None


def test_malformed_json_room_metadata_fails_closed(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt(
        "egress_ended",
        room=_room(room, None, metadata="{not json"),
        egress_info=_egress_info("EG_x"),
    )
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 400, resp.text
    assert resp.json()["code"] == "MALFORMED_BODY"
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None


def test_room_metadata_missing_org_id_key_fails_closed(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt(
        "egress_ended",
        room=_room(room, None, metadata=json.dumps({"role": "host"})),
        egress_info=_egress_info("EG_x"),
    )
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 400, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None


def test_room_metadata_naming_an_unknown_org_fails_closed(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    nonexistent_org = uuid.uuid4()
    evt = _evt("egress_ended", room=_room(room, nonexistent_org), egress_info=_egress_info("EG_x"))
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 400, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None
    assert outbox_rows(f"{room}:egress_ended:-") == []


def test_room_metadata_naming_a_soft_deleted_org_fails_closed(client):
    other_org = seed_org()
    _sql("UPDATE organizations SET deleted_at = now() WHERE id = %(o)s", {"o": str(other_org)})
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt("egress_ended", room=_room(room, other_org), egress_info=_egress_info("EG_x"))
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 400, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None


# --- participant_joined / participant_left ------------------------------------------------------


def test_participant_joined_emits_joined_and_activates_a_pending_room(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt("participant_joined", room=_room(room, org_id), participant=_participant("PA_1"))
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 200, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["status"] == "active"

    dedupe_key = f"{room}:participant_joined:PA_1"
    rows = outbox_rows(dedupe_key)
    assert len(rows) == 1
    payload = rows[0]["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["event_type"] == "joined"
    assert payload["participant_id"] == "PA_1"


def test_participant_left_emits_left_without_reactivating_status(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    _sql(
        "UPDATE meeting_rooms SET status = 'active' WHERE org_id = %(o)s AND meeting_id = %(m)s",
        {"o": str(org_id), "m": str(meeting_id)},
    )
    evt = _evt("participant_left", room=_room(room, org_id), participant=_participant("PA_2"))
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 200, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["status"] == "active"

    dedupe_key = f"{room}:participant_left:PA_2"
    rows = outbox_rows(dedupe_key)
    assert len(rows) == 1


# --- unhandled event types are a recognized 200 no-op -------------------------------------------


def test_unhandled_event_type_is_a_200_no_op(client):
    org_id = seed_org()
    meeting_id, room = seed_meeting_room(org_id)
    evt = _evt("room_started", room=_room(room, org_id))
    FAKE.verify_webhook_result = evt

    resp = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})

    assert resp.status_code == 200, resp.text
    row = meeting_room_row(org_id, meeting_id)
    assert row["recording_ref"] is None
    assert row["status"] == "pending"
