"""Phone push: `push_devices` routes and the consumer's Expo mirror of new in-app rows.

Real Postgres as `mytheus_app`, fakeredis, the Expo Push API faked with an httpx MockTransport.
Covered: register is the caller's own and idempotent, a token moves to whoever signs in last,
delete is owner-only; `push_configured` follows `PUSH_PROVIDER`; RLS isolates and fails
closed; a new notification is pushed once after commit (with its link), a redelivery pushes
nothing, `DeviceNotRegistered` deletes the token, and an Expo outage never fails the message.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import fakeredis.aioredis
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.tenancy import set_rls
from services.notification_service import push as push_mod
from services.notification_service.consumer import handle_message, send_pending
from services.notification_service.push import ExpoPushSender, PendingPush
from tests.conftest import APP_URL
from tests.test_chat_api import PRINCIPAL, Tenant, _create_test_app, _sql, seed_tenant, sign_in
from tests.test_notifications import notification_value
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")


def token() -> str:
    return f"ExponentPushToken[{uuid.uuid4().hex[:22]}]"


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("notification-push-test")
    app.include_router(push_mod.router)
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


def devices(t: Tenant) -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, user_id, token, platform FROM push_devices WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )


def test_register_is_idempotent_and_listed_without_the_token(client, monkeypatch):
    monkeypatch.delenv("PUSH_PROVIDER", raising=False)
    me = seed_tenant()
    sign_in(me)
    tok = token()
    first = client.post("/notifications/push-devices", json={"token": tok, "platform": "ios"})
    assert first.status_code == 200, first.text
    assert first.json()["push_configured"] is False
    again = client.post("/notifications/push-devices", json={"token": tok, "platform": "ios"})
    assert again.json()["id"] == first.json()["id"]
    assert len(devices(me)) == 1
    listed = client.get("/notifications/push-devices").json()
    assert listed["push_configured"] is False
    assert [i["id"] for i in listed["items"]] == [first.json()["id"]]
    assert "token" not in listed["items"][0]

    monkeypatch.setenv("PUSH_PROVIDER", "expo")
    assert client.get("/notifications/push-devices").json()["push_configured"] is True


@pytest.mark.parametrize("bad", ["", "abc", "ExponentPushToken[]", "ExponentPushToken[x y]"])
def test_only_expo_tokens_are_accepted(client, bad):
    sign_in(seed_tenant())
    res = client.post("/notifications/push-devices", json={"token": bad, "platform": "ios"})
    assert res.status_code in (400, 422)
    res = client.post("/notifications/push-devices", json={"token": token(), "platform": "web"})
    assert res.status_code in (400, 422)


def test_a_token_moves_to_whoever_signs_in_last_and_delete_is_owner_only(client):
    me = seed_tenant()
    colleague = seed_user(me)
    tok = token()
    sign_in(me)
    mine = client.post("/notifications/push-devices", json={"token": tok, "platform": "android"})
    sign_in(colleague)
    theirs = client.post("/notifications/push-devices", json={"token": tok, "platform": "android"})
    assert theirs.json()["id"] == mine.json()["id"]
    assert [str(r["user_id"]) for r in devices(me)] == [str(colleague.user_id)]

    sign_in(me)
    assert client.delete(f"/notifications/push-devices/{mine.json()['id']}").status_code == 404
    sign_in(colleague)
    assert client.delete(f"/notifications/push-devices/{mine.json()['id']}").status_code == 204
    assert devices(me) == []
    assert client.delete(f"/notifications/push-devices/{mine.json()['id']}").status_code == 404


def test_another_tenants_device_is_invisible(client):
    a, b = seed_tenant(), seed_tenant()
    sign_in(a)
    dev = client.post("/notifications/push-devices", json={"token": token(), "platform": "ios"})
    sign_in(b)
    assert client.get("/notifications/push-devices").json()["items"] == []
    assert client.delete(f"/notifications/push-devices/{dev.json()['id']}").status_code == 404


async def test_rls_isolates_and_fails_closed():
    a, b = seed_tenant(), seed_tenant()
    for t in (a, b):
        _sql(
            "INSERT INTO push_devices (org_id, user_id, token, platform) "
            "VALUES (%(o)s, %(u)s, %(t)s, 'ios')",
            {"o": str(t.org_id), "u": str(t.user_id), "t": token()},
        )
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {r[0] for r in await conn.execute(text("SELECT org_id FROM push_devices"))}
            assert orgs == {a.org_id}
        async with engine.connect() as conn, conn.begin():
            assert (await conn.execute(text("SELECT count(*) FROM push_devices"))).scalar() == 0
    finally:
        await engine.dispose()


class FakeExpo:
    def __init__(self, errors: dict[str, str] | None = None, down: bool = False) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        self.errors = errors or {}
        self.down = down

    def handler(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            return httpx.Response(503, json={"errors": [{"code": "INTERNAL"}]})
        body = json.loads(request.content)
        self.calls.append(body)
        data = []
        for m in body:
            if m["to"] in self.errors:
                data.append(
                    {"status": "error", "message": "x", "details": {"error": self.errors[m["to"]]}}
                )
            else:
                data.append({"status": "ok", "id": str(uuid.uuid4())})
        return httpx.Response(200, json={"data": data})

    def sender(self) -> ExpoPushSender:
        return ExpoPushSender(transport=httpx.MockTransport(self.handler))


def seed_device(t: Tenant, tok: str) -> None:
    _sql(
        "INSERT INTO push_devices (org_id, user_id, token, platform) "
        "VALUES (%(o)s, %(u)s, %(t)s, 'ios')",
        {"o": str(t.org_id), "u": str(t.user_id), "t": tok},
    )


async def run(value: dict[str, Any], sender: Any, redis: Any) -> bool:
    engine = create_async_engine(APP_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    pending: list[Any] = []
    try:
        async with factory() as session, session.begin():
            applied = await handle_message(
                session, redis, "notifications", value, pending=pending, push=sender
            )
        await send_pending(factory, pending, None, None, sender)
        return applied
    finally:
        await engine.dispose()


async def test_a_new_notification_is_pushed_once_with_its_link():
    me = seed_tenant()
    tok = token()
    seed_device(me, tok)
    expo = FakeExpo()
    value = notification_value(me, source_event_id=f"push:{uuid.uuid4()}")
    redis = fakeredis.aioredis.FakeRedis()
    assert await run(value, expo.sender(), redis) is True
    assert len(expo.calls) == 1
    msg = expo.calls[0][0]
    assert msg["to"] == tok
    assert msg["title"] == value["title"]
    assert msg["data"]["link"] == "/settings/usage"
    assert uuid.UUID(msg["data"]["notification_id"])
    # Redelivered (cold cache too): no new row, so no second push.
    assert await run(value, expo.sender(), fakeredis.aioredis.FakeRedis()) is False
    assert len(expo.calls) == 1


async def test_device_not_registered_deletes_the_token_and_an_outage_is_harmless():
    me = seed_tenant()
    dead, alive = token(), token()
    seed_device(me, dead)
    seed_device(me, alive)
    expo = FakeExpo(errors={dead: "DeviceNotRegistered"})
    await run(
        notification_value(me, source_event_id=f"push:{uuid.uuid4()}"),
        expo.sender(),
        fakeredis.aioredis.FakeRedis(),
    )
    assert [r["token"] for r in devices(me)] == [alive]

    down = FakeExpo(down=True)
    applied = await run(
        notification_value(me, source_event_id=f"push:{uuid.uuid4()}"),
        down.sender(),
        fakeredis.aioredis.FakeRedis(),
    )
    assert applied is True
    assert [r["token"] for r in devices(me)] == [alive]


async def test_no_devices_means_no_call():
    me = seed_tenant()
    expo = FakeExpo()
    await run(
        notification_value(me, source_event_id=f"push:{uuid.uuid4()}"),
        expo.sender(),
        fakeredis.aioredis.FakeRedis(),
    )
    assert expo.calls == []


def test_pending_push_is_a_frozen_value():
    item = PendingPush(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "t", "b", None, 2)
    with pytest.raises(AttributeError):
        item.title = "x"  # type: ignore[misc]


def test_sender_is_off_unless_push_provider_is_expo(monkeypatch):
    monkeypatch.delenv("PUSH_PROVIDER", raising=False)
    assert push_mod.push_sender_from_env() is None
    monkeypatch.setenv("PUSH_PROVIDER", "apns")
    assert push_mod.push_sender_from_env() is None
    monkeypatch.setenv("PUSH_PROVIDER", "expo")
    assert isinstance(push_mod.push_sender_from_env(), ExpoPushSender)
