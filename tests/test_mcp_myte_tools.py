"""Ask Myte's four bff MCP tools (ADR-0047 D6), end to end through the MCP client.

Real Postgres, the real bff app, the official MCP Python client, a personal access token (the
`apps` fixture of `test_mcp_server.py`), and once more with a Clerk session (verify_token
overridden, the same resolution chain every session route uses):

- `list_my_schedule`: the caller's upcoming and live meetings only (not a meeting they are not
  in, not another workspace's, not an ended one, not one past the window), with host, attendee
  count, the attendance choice (a manual choice, and a rule previewed without writing) and live;
- `find_people`: workspace members by name or email with role, team and department, never
  another workspace's people;
- `list_my_inbox`: the caller's unread notifications (not read ones, not a colleague's) and the
  detections waiting for them, newest first;
- `get_my_myte`: the persona, listening settings and the notes kept in the last 7 days.

The pure checks at the bottom (the catalogue and schemas, argument clamping, matching) need no
database.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from libs.shared import auth as auth_mod
from libs.shared.authz import RequestContext
from services.bff import mcp as mcp_mod
from services.bff import myte_tools
from tests.test_api_tokens import mint
from tests.test_chat_api import Tenant, _sql, add_participant, seed_tenant
from tests.test_mcp_server import apps, session_for  # noqa: F401 - the shared fixture
from tests.test_my_tasks import add_user

NOW = datetime.now(UTC)


def _meeting(
    org: uuid.UUID,
    title: str,
    host: Tenant,
    *,
    start: datetime,
    status: str = "scheduled",
    minutes: int = 30,
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, scheduled_end, created_by) "
        "VALUES (%(o)s, %(t)s, %(s)s, %(a)s, %(b)s, %(h)s) RETURNING id",
        {
            "o": str(org),
            "t": title,
            "s": status,
            "a": start,
            "b": start + timedelta(minutes=minutes),
            "h": str(host.user_id),
        },
    )
    m = row[0]["id"]
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(org)},
    )
    return uuid.UUID(str(m))


def _name(t: Tenant, name: str, email: str) -> None:
    _sql(
        "UPDATE users SET display_name = %(n)s, email = %(e)s WHERE id = %(u)s",
        {"n": name, "e": email, "u": str(t.user_id)},
    )


def _seed() -> dict[str, Any]:
    me = seed_tenant()
    _name(me, "Sam Rivera", "sam@example.com")
    priya = add_user(me)
    _name(priya, "Priya Sharma", "priya@example.com")
    _sql("UPDATE users SET role = 'Staff engineer' WHERE id = %(u)s", {"u": str(priya.user_id)})
    dept, team = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO departments (id, org_id, name) VALUES (%(d)s, %(o)s, 'Engineering')",
        {"d": str(dept), "o": str(me.org_id)},
    )
    _sql(
        "INSERT INTO teams (id, org_id, name, department_id) VALUES (%(t)s, %(o)s, 'Platform', "
        "%(d)s)",
        {"t": str(team), "o": str(me.org_id), "d": str(dept)},
    )
    _sql(
        "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s, %(u)s, %(o)s)",
        {"t": str(team), "u": str(priya.user_id), "o": str(me.org_id)},
    )

    # Schedule: one I host (Priya invited, I chose to send my Myte), one Priya hosts that my
    # rule skips, one live now, plus three that must not show.
    hosted = _meeting(me.org_id, "Atlas pricing", me, start=NOW + timedelta(days=1))
    add_participant(hosted, me.org_id, priya.user_id)
    _sql(
        "UPDATE meeting_participants SET attendance_choice = 'agent_listens', "
        "attendance_choice_source = 'manual' WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(hosted), "u": str(me.user_id)},
    )
    standup = _meeting(me.org_id, "Daily standup", priya, start=NOW + timedelta(days=2))
    add_participant(standup, me.org_id, me.user_id)
    _sql(
        "INSERT INTO attendance_rules (org_id, user_id, choice, title_contains) "
        "VALUES (%(o)s, %(u)s, 'skip', 'standup')",
        {"o": str(me.org_id), "u": str(me.user_id)},
    )
    live = _meeting(
        me.org_id, "Incident review", me, start=NOW - timedelta(minutes=20), status="live"
    )
    _meeting(me.org_id, "Not mine", priya, start=NOW + timedelta(days=1))
    _meeting(me.org_id, "Too far", me, start=NOW + timedelta(days=12))
    _meeting(me.org_id, "Over", me, start=NOW - timedelta(days=1), status="ended")

    # Inbox: an unread and a read notification for me, an unread one for Priya, a detection.
    def note(user: Tenant, title: str, status: str, created: datetime) -> uuid.UUID:
        nid = uuid.uuid4()
        _sql(
            "INSERT INTO notification_log (id, org_id, user_id, meeting_id, source, alert_type, "
            "priority, title, body, payload, status, created_at) VALUES (%(i)s, %(o)s, %(u)s, "
            "%(m)s, 'notification', NULL, 2, %(t)s, 'Body', '{}', %(s)s, %(c)s)",
            {
                "i": str(nid),
                "o": str(me.org_id),
                "u": str(user.user_id),
                "m": str(hosted),
                "t": title,
                "s": status,
                "c": created,
            },
        )
        return nid

    unread = note(me, "Priya asked about Atlas", "unread", NOW - timedelta(hours=2))
    note(me, "Old news", "read", NOW - timedelta(hours=1))
    note(priya, "Priya's own", "unread", NOW)
    detection = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, 'Ship Atlas in March', 0.95, 'proposed') RETURNING id",
        {"o": str(me.org_id), "m": str(hosted)},
    )[0]["id"]

    # My Myte: a persona, a listening profile, one note this week and one older.
    _sql(
        "UPDATE users SET agent_name = 'Pip', agent_personality = 'friendly', "
        "agent_instructions = 'Keep it short', listen_for = ARRAY['pricing'], "
        "ignore_topics = ARRAY['lunch'] WHERE id = %(u)s",
        {"u": str(me.user_id)},
    )
    for created in (NOW - timedelta(days=1), NOW - timedelta(days=20)):
        _sql(
            "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, surfaced, "
            "created_at) VALUES (%(o)s, %(m)s, %(u)s, %(w)s, '- a note', true, %(c)s)",
            {
                "o": str(me.org_id),
                "m": str(hosted),
                "u": str(me.user_id),
                "w": f"w-{uuid.uuid4()}",
                "c": created,
            },
        )

    other = seed_tenant()
    _name(other, "Priya Elsewhere", "priya@other.example")
    _meeting(other.org_id, "Foreign", other, start=NOW + timedelta(days=1))
    return {
        "me": me,
        "priya": priya,
        "hosted": hosted,
        "standup": standup,
        "live": live,
        "unread": unread,
        "detection": detection,
    }


async def test_the_four_tools_as_the_token_owner(apps):  # noqa: F811
    s = _seed()
    me = s["me"]
    async with session_for(apps, mint(me, scope="read")) as session:
        listed = {t.name: t for t in (await session.list_tools()).tools}
        assert {"list_my_schedule", "find_people", "list_my_inbox", "get_my_myte"} <= set(listed)
        assert listed["get_my_myte"].annotations.read_only_hint is True

        schedule = await session.call_tool("list_my_schedule", {"days": 7})
        assert not schedule.is_error
        items = schedule.structured_content["items"]
        by_id = {i["id"]: i for i in items}
        assert set(by_id) == {str(s["hosted"]), str(s["standup"]), str(s["live"])}
        assert items[0]["id"] == str(s["live"]) and items[0]["live"] is True
        hosted = by_id[str(s["hosted"])]
        assert hosted["attendance"] == "agent"
        assert hosted["attendee_count"] == 2 and hosted["host_name"] == "Sam Rivera"
        assert hosted["title"] == "Atlas pricing" and hosted["end"] > hosted["start"]
        standup = by_id[str(s["standup"])]
        assert standup["attendance"] == "skip" and standup["host_name"] == "Priya Sharma"
        assert by_id[str(s["live"])]["attendance"] == "attend"
        written = _sql(
            "SELECT attendance_choice FROM meeting_participants WHERE meeting_id = %(m)s "
            "AND user_id = %(u)s",
            {"m": str(s["standup"]), "u": str(me.user_id)},
        )
        assert written == [{"attendance_choice": None}], "the rule is previewed, never written"

        people = await session.call_tool("find_people", {"query": "priya"})
        assert not people.is_error
        assert people.structured_content["items"] == [
            {
                "id": str(s["priya"].user_id),
                "name": "Priya Sharma",
                "email": "priya@example.com",
                "role": "Staff engineer",
                "team": "Platform",
                "teams": ["Platform"],
                "department": "Engineering",
                "agent_name": None,
            }
        ]
        by_email = await session.call_tool("find_people", {"query": "sam@example", "limit": 3})
        assert [p["name"] for p in by_email.structured_content["items"]] == ["Sam Rivera"]
        nobody = await session.call_tool("find_people", {"query": "elsewhere"})
        assert nobody.structured_content["items"] == []

        inbox = await session.call_tool("list_my_inbox", {})
        assert not inbox.is_error
        got = inbox.structured_content["items"]
        assert [(i["kind"], i["id"]) for i in got] == [
            ("detection", str(s["detection"])),
            ("notification", str(s["unread"])),
        ]
        assert got[0]["detection_kind"] == "decision" and got[0]["body"] == "Ship Atlas in March"
        assert got[1]["title"] == "Priya asked about Atlas"
        assert got[1]["meeting_id"] == str(s["hosted"])

        myte = await session.call_tool("get_my_myte", {})
        assert not myte.is_error
        body = myte.structured_content
        assert body["name"] == "Pip" and body["name_is_default"] is False
        assert body["personality"] == "friendly" and body["instructions"] == "Keep it short"
        assert body["listening"]["listen_for"] == ["pricing"]
        assert body["listening"]["ignore_topics"] == ["lunch"]
        assert body["listening"]["attendance_rules"] == 1
        assert body["listening"]["time_zone"] == "UTC"
        assert body["notes_last_7_days"] == 1
        assert "Pip" in myte.content[0].text


async def test_the_tools_work_with_a_clerk_session(apps):  # noqa: F811
    s = _seed()
    me = s["me"]
    apps.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": me.clerk_user,
        "org_id": me.clerk_org,
    }
    try:
        async with session_for(apps, "clerk-session-jwt") as session:
            myte = await session.call_tool("get_my_myte", {})
            assert not myte.is_error, myte.content
            assert myte.structured_content["name"] == "Pip"
            schedule = await session.call_tool("list_my_schedule", {"days": 1})
            ids = {i["id"] for i in schedule.structured_content["items"]}
            assert ids == {str(s["hosted"]), str(s["live"])}
    finally:
        apps.dependency_overrides.pop(auth_mod.verify_token, None)


async def test_a_default_myte_name_and_an_empty_week(apps):  # noqa: F811
    me = seed_tenant()
    _name(me, "Ana Lopez", "ana@example.com")
    async with session_for(apps, mint(me, scope="read")) as session:
        myte = await session.call_tool("get_my_myte", {})
        assert myte.structured_content["name"] == "Ana's Myte"
        assert myte.structured_content["name_is_default"] is True
        assert myte.structured_content["notes_last_7_days"] == 0
        week = await session.call_tool("list_my_schedule", {})
        assert week.structured_content == {"days": 7, "items": []}
        inbox = await session.call_tool("list_my_inbox", {"limit": 20})
        assert inbox.structured_content == {"items": []}
        bad = await session.call_tool("find_people", {"query": "   "})
        assert bad.is_error


def _ctx(user_id: uuid.UUID | None) -> RequestContext:
    return RequestContext(
        org_id=uuid.uuid4(), user_id=user_id, clerk_user_id="user_x", rbac_tier="member", ip=None
    )


# --- pure: no database --------------------------------------------------------------------------


def test_catalogue_and_schemas():
    tools = {t["name"]: t for t in myte_tools.TOOLS}
    assert set(tools) == {"list_my_schedule", "find_people", "list_my_inbox", "get_my_myte"}
    for t in tools.values():
        assert t["annotations"]["readOnlyHint"] is True
        assert t["inputSchema"]["type"] == "object"
        assert t["inputSchema"]["additionalProperties"] is False
    assert tools["list_my_schedule"]["inputSchema"]["properties"]["days"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": 14,
        "default": 7,
    }
    assert tools["find_people"]["inputSchema"]["required"] == ["query"]
    assert tools["find_people"]["inputSchema"]["properties"]["limit"]["maximum"] == 10
    assert tools["list_my_inbox"]["inputSchema"]["properties"]["limit"]["maximum"] == 20
    assert tools["get_my_myte"]["inputSchema"]["properties"] == {}
    # Registered on the server, before the two write tools, and never shadowing a name.
    names = [t["name"] for t in mcp_mod.TOOLS]
    assert len(names) == len(set(names))
    assert set(tools) <= set(names) and names[-2:] == ["add_promise", "create_action"]


async def test_tools_list_over_json_rpc_includes_them():
    ctx = _ctx(uuid.uuid4())
    out = await mcp_mod.handle(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, ctx, mcp_mod.CoreClient("")
    )
    names = {t["name"] for t in out["result"]["tools"]}
    assert {"list_my_schedule", "find_people", "list_my_inbox", "get_my_myte"} <= names


async def test_a_bad_argument_is_a_tool_error_not_a_crash():
    ctx = _ctx(uuid.uuid4())
    out = await mcp_mod.handle(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "list_my_schedule", "arguments": {"days": "lots"}},
        },
        ctx,
        mcp_mod.CoreClient(""),
    )
    assert out["result"]["isError"] is True
    assert "days" in out["result"]["content"][0]["text"]


@pytest.mark.parametrize(
    ("args", "expected"),
    [({}, 7), ({"days": 0}, 1), ({"days": 99}, 14), ({"days": "3"}, 3), ({"days": None}, 7)],
)
def test_days_is_clamped(args, expected):
    assert myte_tools._int_arg("list_my_schedule", args) == expected


def test_a_session_without_a_user_is_refused():
    with pytest.raises(myte_tools.MyteToolError):
        myte_tools._me(_ctx(None))


def test_matching_is_every_word_case_insensitive():
    assert myte_tools._matches("priya sha", "Priya Sharma", "p@example.com")
    assert myte_tools._matches("EXAMPLE", None, "p@example.com")
    assert not myte_tools._matches("priya jones", "Priya Sharma", "p@example.com")
