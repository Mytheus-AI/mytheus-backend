"""The four read-only MCP tools Ask Myte added (ADR-0047 D6), also served to Claude and IDEs.

- `list_my_schedule` `{days: 1..14 = 7}`: the caller's upcoming meetings.
- `find_people` `{query, limit: 1..10 = 5}`: workspace members by name or email.
- `list_my_inbox` `{limit: 1..20 = 10}`: unread notifications and detections waiting for review.
- `get_my_myte` `{}`: the caller's Myte persona, its listening settings, and how many notes it
  kept in the last 7 days.

How they read, and why it differs from the core-api tools. The other tools call core-api over
HTTP with the caller's bearer. These four read surfaces a personal access token may not reach
(`/users`, `/notifications`, `/api/v1/listening`: `libs/shared/pat.py` keeps them off the token
surface on purpose), so an HTTP hop would work for a Clerk session and fail for every token. They
read in-process instead, the way bff's screens do (`dashboard.py`, `people.py`), and still AS THE
CALLER:

- the session is bound to the caller's `org_id` (`set_rls`, transaction-local), so RLS scopes
  every row to their workspace exactly as on the public routes;
- the transaction is `READ ONLY`, so bff stays a non-writer by construction (a write anywhere
  below would fail, not land);
- each tool reuses the owning route or its read model rather than restating it: identity's
  `list_members` and `read_my_profile`, notification-service's `list_notifications`, core-api's
  `listening` read model (`meeting_facts`, `load_rules`, `load_preferences`) and the dashboard's
  `load_needs_review`, and filters on the caller's own `user_id` the same way those do.

So a tool sees exactly what the person sees in the app, whichever credential they came with.

Every item has a stable `id` and the fields Ask Myte cites (ADR-0047 D5): a title, the times, and
the meeting it belongs to where there is one.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ProblemDetail
from libs.shared.tenancy import set_rls
from services.bff.dashboard import load_needs_review
from services.core_api import listening
from services.core_api.records import person_name
from services.identity_tenant_service.routes.users import list_members, read_my_profile
from services.notification_service.routes import list_notifications

_INT_LIMITS = {
    "list_my_schedule": ("days", 1, 14, 7),
    "find_people": ("limit", 1, 10, 5),
    "list_my_inbox": ("limit", 1, 20, 10),
}
_CHOICE = {"attend": "attend", "agent_listens": "agent", "skip": "skip"}
_RECENT_NOTES_DAYS = 7

TOOLS: list[dict[str, Any]] = [
    {
        "name": "list_my_schedule",
        "title": "List my schedule",
        "description": (
            "Your upcoming meetings for the next `days` days (and any that are live now), soonest "
            "first: id, title, start, end, host, attendee count, your attendance choice (attend, "
            "agent: your Myte listens for you, skip) and whether it is live."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "minimum": 1, "maximum": 14, "default": 7},
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "find_people",
        "title": "Find people",
        "description": (
            "Find people in your workspace by name or email, with their role, teams and "
            "department."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "minLength": 1, "maxLength": 120},
                "limit": {"type": "integer", "minimum": 1, "maximum": 10, "default": 5},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "list_my_inbox",
        "title": "List my inbox",
        "description": (
            "What is waiting for you, newest first: unread notifications, and decisions or tasks "
            "your Myte detected in your meetings that wait for you to confirm or dismiss."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 10},
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "get_my_myte",
        "title": "Get my Myte",
        "description": (
            "Your Myte: its name, personality and instructions, what it listens for and ignores, "
            "your interruption and digest settings, and how many notes it kept for you in the "
            "last 7 days."
        ),
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
]
NAMES = frozenset(t["name"] for t in TOOLS)


class MyteToolError(Exception):
    """Raised for a failure the model should see; `mcp.call_tool` turns it into `isError`."""


@asynccontextmanager
async def read_session(ctx: RequestContext) -> AsyncIterator[AsyncSession]:
    """A READ ONLY transaction with `app.current_org` bound to the caller's tenant."""
    async with get_sessionmaker()() as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await set_rls(session, ctx.org_id)
            yield session


def _int_arg(name: str, args: dict[str, Any]) -> int:
    key, lo, hi, default = _INT_LIMITS[name]
    raw = args.get(key, default)
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, int | float | str):
        raise MyteToolError(f"`{key}` must be a whole number from {lo} to {hi}.")
    try:
        value = int(raw)
    except ValueError:
        raise MyteToolError(f"`{key}` must be a whole number from {lo} to {hi}.") from None
    return max(lo, min(value, hi))


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise MyteToolError("This account has no Mytheus profile in this workspace.")
    return ctx.user_id


async def call(name: str, args: dict[str, Any], ctx: RequestContext) -> tuple[str, Any]:
    """Run one of the four tools -> (text for the model, structuredContent)."""
    if name == "list_my_schedule":
        return await list_my_schedule(ctx, _int_arg(name, args))
    if name == "find_people":
        query = " ".join(str(args.get("query") or "").split())
        if not query:
            raise MyteToolError("`query` is required.")
        return await find_people(ctx, query[:120], _int_arg(name, args))
    if name == "list_my_inbox":
        return await list_my_inbox(ctx, _int_arg(name, args))
    if name == "get_my_myte":
        return await get_my_myte(ctx)
    raise MyteToolError(f"Unknown tool {name!r}.")


# --- list_my_schedule ---------------------------------------------------------------------------


async def list_my_schedule(ctx: RequestContext, days: int) -> tuple[str, dict[str, Any]]:
    me = _me(ctx)
    now = datetime.now(UTC)
    async with read_session(ctx) as session:
        # A live meeting may have started hours ago: look back a day, keep what is live or not
        # yet over.
        facts = await listening.meeting_facts(
            session,
            user_id=me,
            start=now - timedelta(days=1),
            end=now + timedelta(days=days),
            open_only=True,
        )
        facts = [m for m in facts if m["status"] == "live" or _not_over(m, now)]
        rules = await listening.load_rules(session, me)
        extra = await _hosts_and_counts(session, [m["id"] for m in facts])
    items = []
    for m in facts:
        choice, _source = listening.effective(m)
        if m.get("attendance_choice_source") != "manual":
            # The page applies rules when it is opened; read-only here, so preview the rule
            # the same way `listening.apply_rules` would materialise it.
            rule = listening.first_match(rules, m)
            if rule is not None:
                choice = rule.choice
        host_name, count = extra.get(m["id"], (None, 0))
        items.append(
            {
                "id": str(m["id"]),
                "title": m["title"] or "Untitled meeting",
                "start": _iso(m["scheduled_start"]),
                "end": _iso(m["scheduled_end"]),
                "host_name": host_name,
                "attendee_count": count,
                "attendance": _CHOICE.get(choice, "attend"),
                "live": m["status"] == "live",
                "hosting": bool(m["hosting"]),
            }
        )
    lines = [
        f"- {i['title']}: {i['start'] or 'no time set'}"
        + (" (live now)" if i["live"] else "")
        + (f", host {i['host_name']}" if i["host_name"] else "")
        + f", {i['attendee_count']} invited, you: {i['attendance']} `{i['id']}`"
        for i in items
    ]
    text_ = "\n".join(lines) or f"No meetings in the next {days} day(s)."
    return text_, {"days": days, "items": items}


def _not_over(m: dict[str, Any], now: datetime) -> bool:
    start, end = m["scheduled_start"], m["scheduled_end"]
    if end is not None:
        return bool(end > now)
    return start is None or bool(start >= now - timedelta(hours=1))


async def _hosts_and_counts(
    session: AsyncSession, meeting_ids: list[uuid.UUID]
) -> dict[uuid.UUID, tuple[str | None, int]]:
    if not meeting_ids:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT m.id, "
                    "(SELECT count(*) FROM meeting_participants p WHERE p.meeting_id = m.id) "
                    "  AS attendee_count, "
                    "h.display_name, h.email FROM meetings m "
                    "LEFT JOIN LATERAL (SELECT u.display_name, u.email FROM users u "
                    "  LEFT JOIN meeting_participants hp ON hp.user_id = u.id "
                    "   AND hp.meeting_id = m.id AND hp.role_in_meeting = 'host' "
                    "  WHERE u.deleted_at IS NULL "
                    "  AND (hp.user_id IS NOT NULL OR u.id = m.created_by) "
                    "  ORDER BY (hp.user_id IS NOT NULL) DESC LIMIT 1) h ON true "
                    "WHERE m.id = ANY(CAST(:ids AS uuid[]))"
                ),
                {"ids": [str(i) for i in meeting_ids]},
            )
        )
        .mappings()
        .all()
    )
    return {
        r["id"]: (person_name(r["display_name"], r["email"]), int(r["attendee_count"] or 0))
        for r in rows
    }


# --- find_people --------------------------------------------------------------------------------


def _matches(query: str, *fields: str | None) -> bool:
    hay = " ".join(f.casefold() for f in fields if f)
    return all(word in hay for word in query.casefold().split())


async def find_people(ctx: RequestContext, query: str, limit: int) -> tuple[str, dict[str, Any]]:
    async with read_session(ctx) as session:
        members = (await list_members(ctx, session)).items
        found = [
            m
            for m in members
            if _matches(query, m.display_name, m.email, person_name(m.display_name, m.email))
        ][:limit]
        teams = await _teams_of(session, [m.id for m in found])
    items: list[dict[str, Any]] = []
    for m in found:
        mine = teams.get(m.id, [])
        items.append(
            {
                "id": str(m.id),
                "name": person_name(m.display_name, m.email) or m.email,
                "email": m.email,
                "role": m.role,
                "team": mine[0]["team"] if mine else None,
                "teams": [str(t["team"]) for t in mine],
                "department": next((t["department"] for t in mine if t["department"]), None),
                "agent_name": m.agent_name,
            }
        )
    lines = [
        f"- {i['name']} <{i['email']}>"
        + (f", {i['role']}" if i["role"] else "")
        + (", team " + ", ".join(i["teams"]) if i["teams"] else "")
        + (f", {i['department']}" if i["department"] else "")
        + f" `{i['id']}`"
        for i in items
    ]
    return "\n".join(lines) or f"Nobody in your workspace matches {query!r}.", {
        "query": query,
        "items": items,
    }


async def _teams_of(
    session: AsyncSession, user_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[dict[str, str | None]]]:
    if not user_ids:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT tm.user_id, t.name AS team, d.name AS department FROM team_members tm "
                    "JOIN teams t ON t.id = tm.team_id AND t.deleted_at IS NULL "
                    "LEFT JOIN departments d ON d.id = t.department_id AND d.deleted_at IS NULL "
                    "WHERE tm.user_id = ANY(CAST(:ids AS uuid[])) ORDER BY t.name, d.name"
                ),
                {"ids": [str(u) for u in user_ids]},
            )
        )
        .mappings()
        .all()
    )
    out: dict[uuid.UUID, list[dict[str, str | None]]] = {}
    for r in rows:
        out.setdefault(r["user_id"], []).append({"team": r["team"], "department": r["department"]})
    return out


# --- list_my_inbox ------------------------------------------------------------------------------


async def list_my_inbox(ctx: RequestContext, limit: int) -> tuple[str, dict[str, Any]]:
    me = _me(ctx)
    async with read_session(ctx) as session:
        page = await list_notifications(
            ctx, session, meeting_id=None, status="unread", limit=limit, cursor=None
        )
        review = await load_needs_review(session, str(me), limit)
    items: list[dict[str, Any]] = [
        {
            "id": str(n.id),
            "kind": "notification",
            "title": n.title,
            "body": n.body,
            "created_at": _iso(n.created_at),
            "meeting_id": str(n.meeting_id) if n.meeting_id else None,
            "priority": n.priority,
            "link": n.link,
        }
        for n in page.items
    ]
    items += [
        {
            "id": str(r.id),
            "kind": "detection",
            "detection_kind": r.kind,
            "title": ("Decision to review" if r.kind == "decision" else "Task to review")
            + (f" from {r.meeting_title}" if r.meeting_title else ""),
            "body": r.content,
            "created_at": _iso(r.created_at),
            "meeting_id": str(r.meeting_id),
            "meeting_title": r.meeting_title,
        }
        for r in review
    ]
    items.sort(key=_created, reverse=True)
    items = items[:limit]
    lines = [
        f"- [{i['kind'] if i['kind'] == 'notification' else i['detection_kind'] + ' to review'}] "
        f"{i['title'] if i['kind'] == 'notification' else i['body']}"
        + (f": {i['body']}" if i["kind"] == "notification" and i["body"] else "")
        + f" ({str(i['created_at'])[:10]}) `{i['id']}`"
        for i in items
    ]
    return "\n".join(lines) or "Nothing is waiting for you.", {"items": items}


def _created(item: dict[str, Any]) -> datetime:
    raw = item.get("created_at")
    return datetime.fromisoformat(raw) if raw else datetime.min.replace(tzinfo=UTC)


# --- get_my_myte --------------------------------------------------------------------------------


async def get_my_myte(ctx: RequestContext) -> tuple[str, dict[str, Any]]:
    me = _me(ctx)
    async with read_session(ctx) as session:
        try:
            profile = await read_my_profile(ctx, session)
        except ProblemDetail as exc:  # the route's 404: no users row in this workspace
            raise MyteToolError("This account has no Mytheus profile in this workspace.") from exc
        prefs = await listening.load_preferences(session, me)
        rules = await listening.load_rules(session, me)
        notes = int(
            (
                await session.execute(
                    text(
                        "SELECT count(*) FROM worklogs WHERE user_id = :u AND surfaced "
                        "AND deleted_at IS NULL AND created_at >= now() - make_interval(days => :d)"
                    ),
                    {"u": str(me), "d": _RECENT_NOTES_DAYS},
                )
            ).scalar()
            or 0
        )
    person = person_name(profile.display_name, profile.email) or "you"
    first = person.split()[0] if person.split() else "you"
    custom = " ".join((profile.agent_name or "").split())
    name = custom or f"{first}'s Myte"
    body = {
        "name": name,
        "name_is_default": not custom,
        "avatar_seed": profile.agent_avatar_seed,
        "personality": profile.agent_personality,
        "instructions": profile.agent_instructions,
        "listening": {
            "listen_for": list(profile.listen_for),
            "ignore_topics": list(profile.ignore_topics),
            "interruptions_enabled": prefs.interruptions_enabled,
            "max_interruptions_per_hour": prefs.max_per_hour,
            "quiet_hours": (
                {"start": prefs.quiet_start, "end": prefs.quiet_end}
                if prefs.quiet_start and prefs.quiet_end
                else None
            ),
            "focus_blocks": len(prefs.focus_blocks),
            "never_during_focus": prefs.never_during_focus,
            "digest_enabled": prefs.digest_enabled,
            "digest_hour": prefs.digest_hour,
            "time_zone": prefs.time_zone,
            "attendance_rules": len(rules),
        },
        "notes_last_7_days": notes,
    }
    lines = [
        f"Your Myte is {name} ({profile.agent_personality}).",
        f"Instructions: {profile.agent_instructions}" if profile.agent_instructions else None,
        (
            f"Listens for: {', '.join(profile.listen_for)}."
            if profile.listen_for
            else "Listens for: nothing in particular."
        ),
        f"Ignores: {', '.join(profile.ignore_topics)}." if profile.ignore_topics else None,
        "Interruptions: "
        + ("on" if prefs.interruptions_enabled else "off")
        + f", evening digest {'on' if prefs.digest_enabled else 'off'}"
        + f", {len(rules)} attendance rule(s), time zone {prefs.time_zone}.",
        f"Notes kept in the last {_RECENT_NOTES_DAYS} days: {notes}.",
    ]
    return "\n".join(line for line in lines if line), body
