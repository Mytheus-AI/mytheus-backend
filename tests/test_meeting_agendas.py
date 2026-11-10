"""Linked meetings, hosts and agendas: ADR-0038 D5-D8 in core-api.

Routes, over HTTP as `mytheus_app` (real RLS, real transactions; only `verify_token` stubbed):
- create: host defaults to the creator; `host_user_id` must be the creator or a listed
  participant (422 naming it); the creator is always a participant (`attendee` when not host);
  `previous_meeting_id` must be another meeting of this tenant (422 naming it); agenda owners
  must be participants; a 422 writes nothing.
- get: `host_user_id`, `previous_meeting_id`, `previous_meeting` and the ordered agenda.
- PATCH: host change (exactly one host afterwards; new host must be a participant) and the
  follow-up link, host or creator only (403), 404 for another tenant's meeting.
- PUT agenda: replace semantics (kept ids keep status, new ones pending, missing ones deleted),
  owners must be participants, foreign item ids 422, host or creator only, 409 once ended.
- PATCH agenda item: host sets status with `status_source = 'host'`.
- carry-forward: confirmed decisions, confirmed actions, the caller's own saved questions.
- internal `GET /internal/meetings/{id}/agenda` behind the agent-runtime-service caller token.

Consumer (`handle_agent_result`): MeetingAgent `meeting_update` applies each `updates` entry
(only this meeting's items, host wins, no covered -> in_progress, `status_source = 'agent'`,
`agent_note`), ignores the `nudge`, and is idempotent; unknown agent classes / kinds are
tolerated by both agent-results consumers (core-api and notification-service).

RLS isolation and fail-closed for `meeting_agenda_items`.
"""

from __future__ import annotations

import io
import json
import os
import uuid
from typing import Any

import fastavro
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.tenancy import set_rls
from services.core_api.routes import internal_agents, internal_meetings, meetings
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
from tests.test_worklogs import apply, result

AGENT_SECRET = "m" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"agent-runtime-service={AGENT_SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-agenda-test")
    app.include_router(meetings.router)
    app.include_router(internal_agents.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _loc(resp: Any) -> list[list[str]]:
    assert resp.status_code == 422, resp.text
    return [e["loc"] for e in resp.json().get("errors", [])]


def create(client: TestClient, **body: Any) -> dict[str, Any]:
    resp = client.post("/api/v1/meetings", json={"title": "Weekly", **body})
    assert resp.status_code == 201, resp.text
    return resp.json()


def roles(meeting_id: str) -> dict[str, str | None]:
    return {
        str(r["user_id"]): r["role_in_meeting"]
        for r in _sql(
            "SELECT user_id, role_in_meeting FROM meeting_participants WHERE meeting_id = %(m)s",
            {"m": meeting_id},
        )
    }


def agenda_rows(meeting_id: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT * FROM meeting_agenda_items WHERE meeting_id = %(m)s ORDER BY position",
        {"m": meeting_id},
    )


# --- create / get ----------------------------------------------------------------------------


def test_creator_is_host_by_default_and_meeting_carries_host_and_link(client):
    owner = seed_tenant()
    sign_in(owner)
    first = create(client)
    assert first["host_user_id"] == str(owner.user_id)
    assert first["previous_meeting_id"] is None
    assert first["previous_meeting"] is None and first["agenda"] == []

    second = create(client, previous_meeting_id=first["id"])
    assert second["previous_meeting_id"] == first["id"]
    got = client.get(f"/api/v1/meetings/{second['id']}").json()
    assert got["previous_meeting"] == {
        "id": first["id"],
        "title": "Weekly",
        "scheduled_start": None,
    }
    listed = {m["id"]: m for m in client.get("/api/v1/meetings").json()["items"]}
    assert listed[second["id"]]["host_user_id"] == str(owner.user_id)
    assert listed[second["id"]]["previous_meeting_id"] == first["id"]


def test_named_host_must_be_listed_and_creator_becomes_attendee(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    stranger = seed_user(owner)
    sign_in(owner)

    assert _loc(
        client.post(
            "/api/v1/meetings",
            json={"title": "x", "host_user_id": str(stranger.user_id)},
        )
    ) == [["body", "host_user_id"]]

    m = create(client, participant_ids=[str(bob.user_id)], host_user_id=str(bob.user_id))
    assert m["host_user_id"] == str(bob.user_id)
    assert roles(m["id"]) == {str(owner.user_id): "attendee", str(bob.user_id): "host"}


def test_previous_meeting_must_be_in_this_tenant(client):
    other = seed_tenant()
    foreign_meeting = seed_meeting(other.org_id)
    owner = seed_tenant()
    sign_in(owner)
    before = _sql("SELECT count(*) AS n FROM meetings WHERE org_id = %(o)s", {"o": owner.org_id})
    for prev in (foreign_meeting, uuid.uuid4()):
        assert _loc(
            client.post("/api/v1/meetings", json={"title": "x", "previous_meeting_id": str(prev)})
        ) == [["body", "previous_meeting_id"]]
    after = _sql("SELECT count(*) AS n FROM meetings WHERE org_id = %(o)s", {"o": owner.org_id})
    assert before == after, "a rejected create must write nothing"


def test_agenda_on_create_is_ordered_and_owners_must_be_participants(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    outsider = seed_user(owner)
    sign_in(owner)

    resp = client.post(
        "/api/v1/meetings",
        json={
            "title": "x",
            "participant_ids": [str(bob.user_id)],
            "agenda": [
                {"title": "Intro"},
                {"title": "Budget", "owner_user_id": str(outsider.user_id)},
            ],
        },
    )
    assert _loc(resp) == [["body", "agenda", "1", "owner_user_id"]]

    m = create(
        client,
        participant_ids=[str(bob.user_id)],
        agenda=[
            {"title": "  Intro  ", "duration_minutes": 5},
            {"title": "Budget", "owner_user_id": str(bob.user_id)},
        ],
    )
    assert [(a["position"], a["title"], a["status"]) for a in m["agenda"]] == [
        (0, "Intro", "pending"),
        (1, "Budget", "pending"),
    ]
    assert m["agenda"][1]["owner_user_id"] == str(bob.user_id)
    assert m["agenda"][0]["status_source"] is None and m["agenda"][0]["agent_note"] is None


def test_agenda_limits_are_validated(client):
    sign_in(seed_tenant())
    too_many = [{"title": f"Item {i}"} for i in range(21)]
    assert _loc(client.post("/api/v1/meetings", json={"title": "x", "agenda": too_many})) == [
        ["body", "agenda"]
    ]
    assert ["body", "agenda", "0", "duration_minutes"] in _loc(
        client.post(
            "/api/v1/meetings",
            json={"title": "x", "agenda": [{"title": "a", "duration_minutes": 241}]},
        )
    )
    assert ["body", "agenda", "0", "title"] in _loc(
        client.post("/api/v1/meetings", json={"title": "x", "agenda": [{"title": "x" * 201}]})
    )


# --- PATCH: host and follow-up link ------------------------------------------------------------


def test_host_change_keeps_exactly_one_host(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    carol = seed_user(owner)
    sign_in(owner)
    m = create(client, participant_ids=[str(bob.user_id)])

    assert _loc(
        client.patch(f"/api/v1/meetings/{m['id']}", json={"host_user_id": str(carol.user_id)})
    ) == [["body", "host_user_id"]]
    assert _loc(client.patch(f"/api/v1/meetings/{m['id']}", json={"host_user_id": None})) == [
        ["body", "host_user_id"]
    ]

    resp = client.patch(f"/api/v1/meetings/{m['id']}", json={"host_user_id": str(bob.user_id)})
    assert resp.status_code == 200, resp.text
    assert resp.json()["host_user_id"] == str(bob.user_id)
    assert roles(m["id"]) == {str(owner.user_id): "attendee", str(bob.user_id): "host"}

    # The creator, no longer host, may still hand it on; the new host may too.
    sign_in(bob)
    resp = client.patch(f"/api/v1/meetings/{m['id']}", json={"host_user_id": str(owner.user_id)})
    assert resp.status_code == 200, resp.text
    assert list(roles(m["id"]).values()).count("host") == 1


def test_only_host_or_creator_may_change_host_or_link(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    earlier = create(client)
    m = create(client, participant_ids=[str(bob.user_id)])

    sign_in(bob)
    for body in ({"host_user_id": str(bob.user_id)}, {"previous_meeting_id": earlier["id"]}):
        resp = client.patch(f"/api/v1/meetings/{m['id']}", json=body)
        assert resp.status_code == 403, resp.text
    # Ordinary fields keep their existing rule.
    assert client.patch(f"/api/v1/meetings/{m['id']}", json={"title": "Renamed"}).status_code == 200

    sign_in(owner)
    resp = client.patch(f"/api/v1/meetings/{m['id']}", json={"previous_meeting_id": earlier["id"]})
    assert resp.status_code == 200 and resp.json()["previous_meeting"]["id"] == earlier["id"]
    assert _loc(
        client.patch(f"/api/v1/meetings/{m['id']}", json={"previous_meeting_id": m["id"]})
    ) == [["body", "previous_meeting_id"]]
    resp = client.patch(f"/api/v1/meetings/{m['id']}", json={"previous_meeting_id": None})
    assert resp.status_code == 200 and resp.json()["previous_meeting"] is None


def test_another_tenants_meeting_is_404_on_every_new_route(client):
    other = seed_tenant()
    sign_in(other)
    theirs = create(client, agenda=[{"title": "Secret"}])
    item = theirs["agenda"][0]["id"]

    sign_in(seed_tenant())
    mid = theirs["id"]
    assert client.get(f"/api/v1/meetings/{mid}").status_code == 404
    assert (
        client.patch(
            f"/api/v1/meetings/{mid}", json={"host_user_id": str(other.user_id)}
        ).status_code
        == 404
    )
    assert client.put(f"/api/v1/meetings/{mid}/agenda", json={"items": []}).status_code == 404
    assert (
        client.patch(
            f"/api/v1/meetings/{mid}/agenda/{item}", json={"status": "covered"}
        ).status_code
        == 404
    )
    assert client.get(f"/api/v1/meetings/{mid}/carry-forward").status_code == 404
    assert len(agenda_rows(mid)) == 1


# --- agenda replace / item status ------------------------------------------------------------


def test_agenda_put_replaces_keeping_status_of_kept_items(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    m = create(
        client,
        participant_ids=[str(bob.user_id)],
        agenda=[{"title": "A"}, {"title": "B"}, {"title": "C"}],
    )
    a, b, c = (i["id"] for i in m["agenda"])
    _sql(
        "UPDATE meeting_agenda_items SET status = 'covered', status_source = 'agent', "
        "agent_note = 'done' WHERE id = %(i)s",
        {"i": b},
    )

    resp = client.put(
        f"/api/v1/meetings/{m['id']}/agenda",
        json={
            "items": [
                {"id": b, "title": "B renamed", "owner_user_id": str(bob.user_id)},
                {"title": "D", "duration_minutes": 10},
                {"id": a, "title": "A"},
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    items = resp.json()
    assert [(i["position"], i["title"], i["status"]) for i in items] == [
        (0, "B renamed", "covered"),
        (1, "D", "pending"),
        (2, "A", "pending"),
    ]
    assert items[0]["id"] == b and items[0]["agent_note"] == "done"
    assert items[0]["owner_user_id"] == str(bob.user_id)
    assert items[2]["id"] == a
    assert c not in {r["id"] for r in agenda_rows(m["id"])}

    assert client.put(f"/api/v1/meetings/{m['id']}/agenda", json={"items": []}).json() == []
    assert agenda_rows(m["id"]) == []


def test_agenda_put_rejects_foreign_ids_duplicates_and_non_participant_owners(client):
    owner = seed_tenant()
    outsider = seed_user(owner)
    sign_in(owner)
    other = create(client, agenda=[{"title": "Elsewhere"}])
    m = create(client, agenda=[{"title": "Mine"}])
    mine = m["agenda"][0]["id"]
    url = f"/api/v1/meetings/{m['id']}/agenda"

    assert _loc(
        client.put(url, json={"items": [{"id": other["agenda"][0]["id"], "title": "x"}]})
    ) == [["body", "items", "0", "id"]]
    assert _loc(
        client.put(url, json={"items": [{"id": mine, "title": "x"}, {"id": mine, "title": "y"}]})
    ) == [["body", "items", "1", "id"]]
    assert _loc(
        client.put(url, json={"items": [{"title": "x", "owner_user_id": str(outsider.user_id)}]})
    ) == [["body", "items", "0", "owner_user_id"]]
    assert [r["title"] for r in agenda_rows(m["id"])] == ["Mine"]


def test_agenda_edits_are_host_or_creator_only_and_closed_once_ended(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    m = create(client, participant_ids=[str(bob.user_id)], agenda=[{"title": "A"}])
    item = m["agenda"][0]["id"]

    sign_in(bob)
    assert client.put(f"/api/v1/meetings/{m['id']}/agenda", json={"items": []}).status_code == 403
    assert (
        client.patch(
            f"/api/v1/meetings/{m['id']}/agenda/{item}", json={"status": "covered"}
        ).status_code
        == 403
    )

    sign_in(owner)
    resp = client.patch(f"/api/v1/meetings/{m['id']}/agenda/{item}", json={"status": "skipped"})
    assert resp.status_code == 200, resp.text
    assert (resp.json()["status"], resp.json()["status_source"]) == ("skipped", "host")
    missing = client.patch(
        f"/api/v1/meetings/{m['id']}/agenda/{uuid.uuid4()}", json={"status": "covered"}
    )
    assert missing.status_code == 404
    assert _loc(
        client.patch(f"/api/v1/meetings/{m['id']}/agenda/{item}", json={"status": "done"})
    ) == [["body", "status"]]

    assert client.patch(f"/api/v1/meetings/{m['id']}", json={"status": "ended"}).status_code == 200
    resp = client.put(f"/api/v1/meetings/{m['id']}/agenda", json={"items": []})
    assert resp.status_code == 409, resp.text
    assert len(agenda_rows(m["id"])) == 1


# --- carry-forward -----------------------------------------------------------------------------


def _decision(org: uuid.UUID, meeting: str, content: str, status: str) -> None:
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s)",
        {"o": str(org), "m": meeting, "c": content, "s": status},
    )


def _action(org: uuid.UUID, meeting: str, content: str, status: str, assignee: Any = None) -> None:
    _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, confidence, status, assignee_id, "
        "due_date) VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(a)s, '2026-12-01')",
        {
            "o": str(org),
            "m": meeting,
            "c": content,
            "s": status,
            "a": str(assignee) if assignee else None,
        },
    )


def _saved(org: uuid.UUID, user: uuid.UUID, meeting: str, question: str, status: str) -> None:
    _sql(
        "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, question, priority, status) "
        "VALUES (%(o)s, %(u)s, %(m)s, %(q)s, 2, %(s)s)",
        {"o": str(org), "u": str(user), "m": meeting, "q": question, "s": status},
    )


def test_carry_forward_lists_what_the_previous_meeting_left_open(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    earlier = create(client, participant_ids=[str(bob.user_id)])
    e = earlier["id"]
    _decision(owner.org_id, e, "Ship in Q1", "confirmed")
    _decision(owner.org_id, e, "Maybe hire", "proposed")
    _action(owner.org_id, e, "Draft the plan", "confirmed", bob.user_id)
    _action(owner.org_id, e, "Already done", "done")
    _action(owner.org_id, e, "Just a candidate", "proposed")
    _saved(owner.org_id, owner.user_id, e, "Who owns pricing?", "saved")
    _saved(owner.org_id, owner.user_id, e, "Open one", "open")
    _saved(owner.org_id, bob.user_id, e, "Bob's question", "saved")

    m = create(client, previous_meeting_id=e)
    body = client.get(f"/api/v1/meetings/{m['id']}/carry-forward").json()
    assert body["previous_meeting"]["id"] == e
    assert [d["content"] for d in body["decisions"]] == ["Ship in Q1"]
    assert [(a["title"], a["owner_user_id"], a["due_date"]) for a in body["open_actions"]] == [
        ("Draft the plan", str(bob.user_id), "2026-12-01")
    ]
    assert [q["question"] for q in body["saved_questions"]] == ["Who owns pricing?"]

    sign_in(bob)
    body = client.get(f"/api/v1/meetings/{m['id']}/carry-forward").json()
    assert [q["question"] for q in body["saved_questions"]] == ["Bob's question"]

    unlinked = client.get(f"/api/v1/meetings/{earlier['id']}/carry-forward").json()
    assert unlinked == {
        "previous_meeting": None,
        "decisions": [],
        "open_actions": [],
        "saved_questions": [],
    }


# --- internal route ----------------------------------------------------------------------------


def _headers(org_id: uuid.UUID, secret: str = AGENT_SECRET) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "agent-runtime-service",
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def test_internal_agenda_for_the_meeting_agent(client):
    owner = seed_tenant()
    sign_in(owner)
    earlier = create(client)
    _decision(owner.org_id, earlier["id"], "Ship in Q1", "confirmed")
    _action(owner.org_id, earlier["id"], "Draft the plan", "confirmed")
    m = create(
        client,
        previous_meeting_id=earlier["id"],
        scheduled_start="2026-11-10T10:00:00Z",
        scheduled_end="2026-11-10T11:00:00Z",
        agenda=[{"title": "A", "duration_minutes": 10}, {"title": "B"}],
    )
    url = f"/internal/meetings/{m['id']}/agenda"

    resp = client.get(url, headers=_headers(owner.org_id))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["host_user_id"] == str(owner.user_id)
    assert body["previous_meeting_id"] == earlier["id"]
    assert body["scheduled_start"].startswith("2026-11-10T10:00:00")
    assert body["scheduled_end"].startswith("2026-11-10T11:00:00")
    assert [
        (i["position"], i["title"], i["duration_minutes"], i["status"]) for i in body["items"]
    ] == [
        (0, "A", 10, "pending"),
        (1, "B", None, "pending"),
    ]
    assert set(body["items"][0]) == {
        "id",
        "position",
        "title",
        "duration_minutes",
        "status",
        "status_source",
        "owner_user_id",  # ADR-0039 C5
        "goal_id",  # ADR-0039 B3
    }
    assert body["carry_forward"] == {
        "decisions": ["Ship in Q1"],
        "open_actions": ["Draft the plan"],
    }

    assert client.get(url, headers=_headers(owner.org_id, "x" * 40)).status_code in (401, 403)
    assert client.get(url, headers=_headers(seed_tenant().org_id)).status_code == 404
    assert client.get(url).status_code in (401, 403)


# --- consumer: MeetingAgent meeting_update -----------------------------------------------------


def meeting_update(
    t: Tenant,
    meeting_id: Any,
    updates: list[dict[str, Any]],
    *,
    window: str = "w0",
    nudge: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = result(
        t,
        uuid.UUID(str(meeting_id)),
        0.0,
        window=window,
        agent="MeetingAgent",
        kind="meeting_update",
    )
    value["content"] = ""
    value["payload_json"] = json.dumps({"updates": updates, "nudge": nudge, "surfaced": True})
    return value


def _status(item_id: str) -> tuple[str, str | None, str | None]:
    row = _sql(
        "SELECT status, status_source, agent_note FROM meeting_agenda_items WHERE id = %(i)s",
        {"i": item_id},
    )[0]
    return row["status"], row["status_source"], row["agent_note"]


async def test_meeting_update_applies_the_agent_rules(client):
    owner = seed_tenant()
    sign_in(owner)
    m = create(client, agenda=[{"title": "A"}, {"title": "B"}, {"title": "C"}])
    other = create(client, agenda=[{"title": "Elsewhere"}])
    a, b, c = (i["id"] for i in m["agenda"])
    foreign_item = other["agenda"][0]["id"]
    client.patch(f"/api/v1/meetings/{m['id']}/agenda/{c}", json={"status": "skipped"})

    value = meeting_update(
        owner,
        m["id"],
        [
            {"item_id": a, "status": "covered", "note": "Agreed on scope"},
            {"item_id": b, "status": "in_progress", "note": "x" * 400},
            {"item_id": c, "status": "covered", "note": "agent thinks so"},  # host wins
            {"item_id": foreign_item, "status": "covered", "note": "other meeting"},
            {"item_id": a, "status": "in_progress"},  # second entry for the same item: ignored
            {"item_id": "not-a-uuid", "status": "covered"},
            {"item_id": b, "status": "skipped"},
        ],
        nudge={
            "id": str(uuid.uuid4()),
            "type": "off_agenda",
            "item_id": None,
            "content": "Off topic",
        },
    )
    assert await apply(value) is True
    assert _status(a) == ("covered", "agent", "Agreed on scope")
    assert _status(b) == ("in_progress", "agent", "x" * 300)
    assert _status(c) == ("skipped", "host", None)
    assert _status(foreign_item) == ("pending", None, None)

    # Redelivery of the same record: no-op.
    assert await apply(value) is False

    # A later window can never move a covered item back to in progress, but can move b on.
    later = meeting_update(
        owner,
        m["id"],
        [{"item_id": a, "status": "in_progress"}, {"item_id": b, "status": "covered"}],
        window="w1",
    )
    assert await apply(later) is True
    assert _status(a) == ("covered", "agent", "Agreed on scope")
    assert _status(b) == ("covered", "agent", None)

    # A replay under a new envelope key (inbox row lost) changes nothing either.
    replay = meeting_update(owner, m["id"], [{"item_id": b, "status": "covered"}], window="w2")
    assert await apply(replay) is False

    # A nudge-only record writes nothing and is not poison.
    nudge_only = meeting_update(
        owner,
        m["id"],
        [],
        window="w3",
        nudge={"id": str(uuid.uuid4()), "type": "over_time", "item_id": b, "content": "Over"},
    )
    assert await apply(nudge_only) is False


async def test_unknown_agent_classes_and_kinds_are_tolerated_not_poison(client):
    owner = seed_tenant()
    m = seed_meeting(owner.org_id)
    for agent, kind in (
        ("MeetingAgent", "agenda_update"),
        ("MeetingAgent", "meeting_nudge"),
        ("SomeFutureAgent", "some_future_kind"),
    ):
        assert await apply(result(owner, m, 0.9, agent=agent, kind=kind, window=kind)) is False


async def test_notification_service_ignores_meeting_agent_records(client):
    from services.notification_service.consumer import AGENT_RESULTS, parse_alert

    owner = seed_tenant()
    m = seed_meeting(owner.org_id)
    value = meeting_update(owner, m, [{"item_id": str(uuid.uuid4()), "status": "covered"}])
    assert AGENT_RESULTS == "agent-results"
    assert parse_alert(value) is None
    value["kind"] = "some_future_kind"
    assert parse_alert(value) is None


def test_writer_schema_decoding_hands_new_enum_symbols_over_as_strings():
    """Every consumer here builds `AvroDeserializer(SchemaRegistryClient(...))` with NO reader
    schema, so records decode with the writer's schema and a symbol the consumer's code has
    never seen arrives as a plain string. Shown with fastavro, the library both sides use: a
    record written with `MeetingAgent` / `meeting_update` decodes without a reader schema."""
    writer = fastavro.parse_schema(
        {
            "type": "record",
            "name": "AgentResult",
            "fields": [
                {
                    "name": "agent",
                    "type": {
                        "type": "enum",
                        "name": "AgentClass",
                        "symbols": ["PersonalListenerAgent", "MeetingAgent"],
                    },
                },
                {
                    "name": "kind",
                    "type": {
                        "type": "enum",
                        "name": "AgentResultKind",
                        "symbols": ["note", "meeting_update"],
                    },
                },
            ],
        }
    )
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, writer, {"agent": "MeetingAgent", "kind": "meeting_update"})
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, writer)
    assert decoded == {"agent": "MeetingAgent", "kind": "meeting_update"}


# --- RLS -------------------------------------------------------------------------------------


async def test_meeting_agenda_items_rls_isolates_and_fails_closed(client):
    a, b = seed_tenant(), seed_tenant()
    for t in (a, b):
        sign_in(t)
        create(client, agenda=[{"title": "Item"}])
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {
                r[0] for r in await conn.execute(text("SELECT org_id FROM meeting_agenda_items"))
            }
            assert orgs == {a.org_id}
            # WITH CHECK: no row can be written for another tenant.
            with pytest.raises(DBAPIError, match="row-level security"):
                async with conn.begin_nested():
                    await conn.execute(
                        text(
                            "INSERT INTO meeting_agenda_items (org_id, meeting_id, position, "
                            "title) SELECT :o, meeting_id, 9, 'x' FROM meeting_agenda_items "
                            "LIMIT 1"
                        ),
                        {"o": str(b.org_id)},
                    )
        async with engine.connect() as conn, conn.begin():
            count = (await conn.execute(text("SELECT count(*) FROM meeting_agenda_items"))).scalar()
            assert count == 0
    finally:
        await engine.dispose()
