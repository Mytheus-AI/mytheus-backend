"""Working profile and agent persona: ADR-0038 D1-D4 in identity-tenant-service.

- `GET /api/v1/users/me`: the caller's own `UserProfile`, with the six new fields and their
  defaults.
- `PATCH /users/{id}`: `listen_for` / `ignore_topics` trimmed, de-duplicated, limited (15 x
  100), same authorization as `role`; `agent_*` self only (403 even for an admin); an empty
  string stores NULL; every validation failure is a 422 naming the field.
- `GET /users` items carry `agent_name` / `agent_avatar_seed`.
- `get_user_context` (REST `/context/{id}` and gRPC `GetUserContext`) carries `listen_for`,
  `ignore_topics`, `agent_name`, `agent_personality`, `agent_instructions` (proto fields
  12-16), and a PATCH evicts the cached context so the next read reflects it.

Reuses `tests/test_user_context.py`'s seeding and its signed-in `PRINCIPAL`.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.identity_tenant_service import grpc_server as grpc_server_mod
from tests.conftest import APP_URL
from tests.test_user_context import _seed_user_row, _sql, seed_tenant, sign_in
from tests.test_user_context import client as _client  # noqa: F401 - the module fixture

client = _client


def _loc(resp: Any) -> list[list[str]]:
    assert resp.status_code == 422, resp.text
    return [e["loc"] for e in resp.json()["errors"]]


def test_get_me_returns_own_profile_with_persona_defaults(client):
    t = seed_tenant(role="Backend Engineer")
    sign_in(t)
    resp = client.get("/users/me")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == str(t.user_id) and body["role"] == "Backend Engineer"
    assert body["listen_for"] == [] and body["ignore_topics"] == []
    assert body["agent_name"] is None and body["agent_avatar_seed"] is None
    assert body["agent_personality"] == "balanced"
    assert body["agent_instructions"] is None


def test_patch_profile_and_persona_round_trip_with_trimming_and_dedupe(client):
    t = seed_tenant()
    sign_in(t)
    resp = client.patch(
        f"/users/{t.user_id}",
        json={
            "listen_for": ["  billing ", "Billing", "SLA breaches"],
            "ignore_topics": ["lunch"],
            "agent_name": "  Echo ",
            "agent_avatar_seed": "abc123",
            "agent_personality": "concise",
            "agent_instructions": "Always note numbers.",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["listen_for"] == ["billing", "SLA breaches"]
    assert body["ignore_topics"] == ["lunch"]
    assert body["agent_name"] == "Echo"
    assert body["agent_avatar_seed"] == "abc123"
    assert body["agent_personality"] == "concise"
    assert body["agent_instructions"] == "Always note numbers."
    assert client.get("/users/me").json() == body

    # Empty strings clear to NULL; an empty list clears a topic list.
    resp = client.patch(
        f"/users/{t.user_id}",
        json={
            "agent_name": "",
            "agent_avatar_seed": "",
            "agent_instructions": "  ",
            "listen_for": [],
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["agent_name"], body["agent_avatar_seed"], body["agent_instructions"]) == (
        None,
        None,
        None,
    )
    assert body["listen_for"] == [] and body["agent_personality"] == "concise"
    row = _sql(
        "SELECT agent_name, agent_avatar_seed, agent_instructions FROM users WHERE id = %(u)s",
        {"u": str(t.user_id)},
    )[0]
    assert row == {"agent_name": None, "agent_avatar_seed": None, "agent_instructions": None}


@pytest.mark.parametrize(
    "body,field",
    [
        ({"listen_for": [f"t{i}" for i in range(16)]}, "listen_for"),
        ({"listen_for": ["x" * 101]}, "listen_for"),
        ({"ignore_topics": ["   "]}, "ignore_topics"),
        ({"ignore_topics": None}, "ignore_topics"),
        ({"agent_name": "x" * 41}, "agent_name"),
        ({"agent_avatar_seed": "ABC123"}, "agent_avatar_seed"),
        ({"agent_avatar_seed": "abc"}, "agent_avatar_seed"),
        ({"agent_personality": "sarcastic"}, "agent_personality"),
        ({"agent_personality": None}, "agent_personality"),
        ({"agent_instructions": "x" * 501}, "agent_instructions"),
    ],
)
def test_validation_failures_are_422_naming_the_field(client, body, field):
    t = seed_tenant()
    sign_in(t)
    locs = _loc(client.patch(f"/users/{t.user_id}", json=body))
    assert any(loc[:2] == ["body", field] for loc in locs), locs


def test_fifteen_distinct_topics_after_dedupe_are_accepted(client):
    t = seed_tenant()
    sign_in(t)
    topics = [f"topic {i}" for i in range(15)] + ["TOPIC 0", " topic 1 "]
    resp = client.patch(f"/users/{t.user_id}", json={"listen_for": topics})
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["listen_for"]) == 15


def test_agent_fields_are_self_only_even_for_an_admin(client):
    admin = seed_tenant(tier="admin")
    member = _seed_user_row(admin.org_id, "member")
    sign_in(admin)

    for field, value in (
        ("agent_name", "Echo"),
        ("agent_avatar_seed", "abcd"),
        ("agent_personality", "direct"),
        ("agent_instructions", "x"),
    ):
        resp = client.patch(f"/users/{member.user_id}", json={field: value})
        assert resp.status_code == 403, (field, resp.text)

    # The profile fields keep their rule: an admin may set someone's listen-for list.
    resp = client.patch(f"/users/{member.user_id}", json={"listen_for": ["billing"]})
    assert resp.status_code == 200, resp.text
    row = _sql(
        "SELECT agent_name, listen_for FROM users WHERE id = %(u)s", {"u": str(member.user_id)}
    )[0]
    assert row["agent_name"] is None and row["listen_for"] == ["billing"]


def test_member_list_carries_agent_name_and_seed(client):
    t = seed_tenant()
    sign_in(t)
    client.patch(f"/users/{t.user_id}", json={"agent_name": "Echo", "agent_avatar_seed": "seed42"})
    items = {i["id"]: i for i in client.get("/users").json()["items"]}
    assert items[str(t.user_id)]["agent_name"] == "Echo"
    assert items[str(t.user_id)]["agent_avatar_seed"] == "seed42"


async def test_user_context_carries_profile_and_persona_and_is_evicted_on_patch(client):
    import grpc

    import identity_pb2
    import identity_pb2_grpc

    t = seed_tenant(role="engineer")
    sign_in(t)
    before = client.get(f"/context/{t.user_id}").json()  # warms the cache
    assert before["listen_for"] == [] and before["agent_personality"] == "balanced"
    assert "agent_avatar_seed" not in before

    client.patch(
        f"/users/{t.user_id}",
        json={
            "listen_for": ["billing"],
            "ignore_topics": ["lunch"],
            "agent_name": "Echo",
            "agent_personality": "friendly",
            "agent_instructions": "Note numbers.",
        },
    )
    rest = client.get(f"/context/{t.user_id}").json()
    assert rest["listen_for"] == ["billing"] and rest["ignore_topics"] == ["lunch"]
    assert rest["agent_name"] == "Echo"
    assert rest["agent_personality"] == "friendly"
    assert rest["agent_instructions"] == "Note numbers."

    engine = create_async_engine(APP_URL)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        grpc_server_mod.IdentityService(async_sessionmaker(engine, expire_on_commit=False)), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            resp = await identity_pb2_grpc.IdentityServiceStub(channel).GetUserContext(
                identity_pb2.GetUserContextRequest(user_id=str(t.user_id)),
                metadata=(("org-id", str(t.org_id)),),
            )
    finally:
        await server.stop(None)
        await engine.dispose()
    assert list(resp.listen_for) == ["billing"]
    assert list(resp.ignore_topics) == ["lunch"]
    assert resp.agent_name == "Echo"
    assert resp.agent_personality == "friendly"
    assert resp.agent_instructions == "Note numbers."
    fields = {f.name: f.number for f in identity_pb2.UserContext.DESCRIPTOR.fields}
    assert {k: fields[k] for k in list(fields)[-5:]} == {
        "listen_for": 12,
        "ignore_topics": 13,
        "agent_name": 14,
        "agent_personality": 15,
        "agent_instructions": 16,
    }


def test_a_context_cached_before_the_new_fields_still_validates():
    from services.identity_tenant_service.schemas import UserContext

    old = {
        "user_id": "00000000-0000-0000-0000-000000000001",
        "org_id": "00000000-0000-0000-0000-000000000002",
        "role": None,
        "skills": [],
        "bio": None,
        "avatar_url": None,
        "team": [],
        "department": None,
        "recent_meetings": [],
        "topics": [],
    }
    ctx = UserContext(**old)
    assert ctx.listen_for == [] and ctx.agent_personality == "balanced"


# --- the person's real name (users.display_name) ---------------------------------------------


def test_display_name_round_trips_on_me_and_the_member_list(client):
    t = seed_tenant()
    sign_in(t)
    assert client.get("/users/me").json()["display_name"] is None

    resp = client.patch(f"/users/{t.user_id}", json={"display_name": "  Samar   Rao "})
    assert resp.status_code == 200, resp.text
    assert resp.json()["display_name"] == "Samar Rao"
    assert client.get("/users/me").json()["display_name"] == "Samar Rao"
    items = {i["id"]: i for i in client.get("/users").json()["items"]}
    assert items[str(t.user_id)]["display_name"] == "Samar Rao"

    # Blank clears to NULL; more than 80 characters is a 422 naming the field.
    assert (
        client.patch(f"/users/{t.user_id}", json={"display_name": "  "}).json()["display_name"]
        is None
    )
    assert ["body", "display_name"] in _loc(
        client.patch(f"/users/{t.user_id}", json={"display_name": "x" * 81})
    )


def test_display_name_is_self_only_even_for_an_admin(client):
    admin = seed_tenant(tier="admin")
    member = _seed_user_row(admin.org_id, "member")
    sign_in(admin)
    resp = client.patch(f"/users/{member.user_id}", json={"display_name": "Someone Else"})
    assert resp.status_code == 403, resp.text
    row = _sql("SELECT display_name FROM users WHERE id = %(u)s", {"u": str(member.user_id)})[0]
    assert row["display_name"] is None


def test_person_name_prefers_the_stored_name_then_the_email():
    from services.core_api.records import person_name

    assert person_name("  Samar  Rao ", "nnm23ad071@nmamit.in") == "Samar Rao"
    assert person_name(None, "nnm23ad071@nmamit.in") == "Nnm23ad071"
    assert person_name("   ", "maya.patel@acme.com") == "Maya Patel"
    assert person_name(None, None) is None
