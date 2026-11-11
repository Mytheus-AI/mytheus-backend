"""`get_user_context()` - the source of record for a person's role, skills, team, department,
recent meetings and topics. Plan 5-6 section 3.3.

Cached at `nexus:{org_id}:user_ctx:{user_id}`, TTL 300s (canon). THE CACHE IS AN OPTIMIZATION,
NEVER A DEPENDENCY: a miss, a slow Redis, an unreachable Redis, or a corrupt cached value must
all degrade latency and nothing else - `_compute` from Postgres is always the correct fallback,
and every Redis call below is wrapped so a cache failure can never become a request failure.
This is the property Plan 1-2's verification flagged as the one most likely to be built subtly
wrong, because the happy path looks identical either way (see `tests/test_user_context.py`'s
`test_...unreachable_redis...` for the proof).

`teams`/`team_members`/`departments` and `meetings`/`meeting_participants` are owned by
`core-api`, not by this service (Plan 5-6 Task 1's reconciliation; recorded in
`_canon/24-glossary-and-naming.md` section 10). This module only ever READS them, direct SQL,
same Postgres instance, same established pattern `services/meeting_service/routes.py` already
uses to read core-api's `meetings`/`meeting_participants` and `libs/shared/org_resolver.py`
uses (from every service) to read identity's `organizations`. It never writes them, and adds no
route that would.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key
from services.identity_tenant_service.taxonomy.loader import topics_for as _topics_for

logger = logging.getLogger(__name__)

CTX_TTL = 300  # 5 minutes (canon, Plan 5-6 section 3.3)

_SELECT_USER = text(
    "SELECT id, role, skills, bio, avatar_url, listen_for, ignore_topics, agent_name, "
    "agent_personality, agent_instructions FROM users WHERE id = :uid AND deleted_at IS NULL"
)

_SELECT_TEAMS = text(
    "SELECT t.id, t.name, t.department_id, t.description FROM teams t "
    "JOIN team_members tm ON tm.team_id = t.id "
    "WHERE tm.user_id = :uid AND t.deleted_at IS NULL "
    "ORDER BY t.name"
)

# A user's "department" is derived transitively through their teams (users carry no
# department_id of their own in the data model). If a user sits on teams under more than one
# department, the first alphabetically is returned - a deliberate, documented simplification
# for a field the plan's own pseudocode treats as singular; there is no canon tiebreak rule for
# the multi-department case.
_SELECT_DEPARTMENT = text(
    "SELECT DISTINCT d.id, d.name, d.description FROM departments d "
    "JOIN teams t ON t.department_id = d.id "
    "JOIN team_members tm ON tm.team_id = t.id "
    "WHERE tm.user_id = :uid AND d.deleted_at IS NULL "
    "ORDER BY d.name LIMIT 1"
)

_SELECT_RECENT_MEETINGS = text(
    "SELECT m.id, m.title, m.status, m.scheduled_start, m.scheduled_end FROM meetings m "
    "JOIN meeting_participants mp ON mp.meeting_id = m.id "
    "WHERE mp.user_id = :uid AND m.deleted_at IS NULL "
    "ORDER BY COALESCE(m.scheduled_start, m.created_at) DESC "
    "LIMIT :limit"
)

_RECENT_MEETINGS_LIMIT = 10

# --- role -> topics -----------------------------------------------------------------------
#
# `_topics_for` (imported above as `taxonomy.loader.topics_for`) resolves against the process's
# pinned taxonomy version (`ROLE_TOPICS_TAXONOMY_VERSION`, default "v1") - Plan 5-6 section 3.2.
# This used to be a small inline fallback dict (see git history) because 3.2 had not landed when
# `get_user_context` was first built; that fallback happened to carry the same five roles' same
# topic lists as the real `role_topics.v1.yaml`, so no caller-visible topic values changed when
# it was swapped for the real taxonomy - only the source of truth did.
#
# CACHE STALENESS ON A TAXONOMY VERSION BUMP: a version change does not reach an already-cached
# `user_ctx` entry until its 300s TTL expires. This is accepted, not fixed - the cache key and
# payload are deliberately unchanged. Full reasoning in `taxonomy/loader.py`'s module docstring.


async def get_user_context(
    session: AsyncSession, org_id: uuid.UUID, user_id: uuid.UUID
) -> dict[str, Any] | None:
    """Resolve `(org_id, user_id)` to the user's context. Cache-then-compute.

    Returns `None` if the user does not exist in this tenant. `session` must already have
    `app.current_org` set for `org_id` (the RLS anchor - same contract as every other
    tenant-scoped query in this codebase), so a cross-tenant `user_id` is invisible to
    `_compute` and this returns `None` exactly as it would for an unknown id: existence is
    tenant-confidential, and the caller (REST 404, gRPC NOT_FOUND) must not be able to tell
    the two apart.

    Every Redis operation here is caught narrowly around the Redis call only, deliberately
    with a broad `except Exception` (unlike the fail-CLOSED broad-except ban documented in
    `grpc_server.py` for infrastructure failure - that ban is about not letting a genuine data
    failure look like success; this is the opposite case: Redis is not the system of record,
    so ANY failure shape it can produce - connection refused, timeout, a wrong-typed reply, a
    library bug - must fail OPEN to Postgres, never fail the request).
    """
    key = redis_key(org_id, "user_ctx", str(user_id))

    cached: bytes | None = None
    try:
        cached = await get_redis().get(key)
    except Exception as exc:  # noqa: BLE001 - deliberate: see docstring
        logger.warning("user_ctx cache read failed for %s: %s", key, exc)

    if cached:
        try:
            parsed: dict[str, Any] = json.loads(cached)
            return parsed
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            # A corrupt or unexpected cached value. Never trust it, never raise on it - treat
            # exactly like a miss.
            logger.warning("user_ctx cache entry corrupt for %s: %s", key, exc)

    ctx = await _compute(session, org_id, user_id)
    if ctx is None:
        return None

    try:
        await get_redis().set(key, json.dumps(ctx), ex=CTX_TTL)
    except Exception as exc:  # noqa: BLE001 - deliberate: see docstring
        logger.warning("user_ctx cache write failed for %s: %s", key, exc)

    return ctx


async def _compute(
    session: AsyncSession, org_id: uuid.UUID, user_id: uuid.UUID
) -> dict[str, Any] | None:
    user_row = (await session.execute(_SELECT_USER, {"uid": str(user_id)})).mappings().first()
    if user_row is None:
        return None

    team_rows = (await session.execute(_SELECT_TEAMS, {"uid": str(user_id)})).mappings().all()
    dept_row = (await session.execute(_SELECT_DEPARTMENT, {"uid": str(user_id)})).mappings().first()
    meeting_rows = (
        (
            await session.execute(
                _SELECT_RECENT_MEETINGS, {"uid": str(user_id), "limit": _RECENT_MEETINGS_LIMIT}
            )
        )
        .mappings()
        .all()
    )

    return {
        "user_id": str(user_id),
        "org_id": str(org_id),
        "role": user_row["role"],
        "skills": list(user_row["skills"] or []),
        "bio": user_row["bio"],
        "avatar_url": user_row["avatar_url"],
        "team": [
            {
                "id": str(t["id"]),
                "name": t["name"],
                "department_id": str(t["department_id"]) if t["department_id"] else None,
                "description": t["description"],
            }
            for t in team_rows
        ],
        "department": (
            {
                "id": str(dept_row["id"]),
                "name": dept_row["name"],
                "description": dept_row["description"],
            }
            if dept_row is not None
            else None
        ),
        "recent_meetings": [
            {
                "id": str(m["id"]),
                "title": m["title"],
                "status": m["status"],
                "scheduled_start": (
                    m["scheduled_start"].isoformat() if m["scheduled_start"] else None
                ),
                "scheduled_end": m["scheduled_end"].isoformat() if m["scheduled_end"] else None,
            }
            for m in meeting_rows
        ],
        "topics": _topics_for(user_row["role"]),
        # ADR-0038 D4: what PersonalListenerAgent / QuestionAgent listen for and how they word
        # their notes. The avatar seed is deliberately absent: no agent needs it.
        "listen_for": list(user_row["listen_for"] or []),
        "ignore_topics": list(user_row["ignore_topics"] or []),
        "agent_name": user_row["agent_name"],
        "agent_personality": user_row["agent_personality"] or "balanced",
        "agent_instructions": user_row["agent_instructions"],
    }


async def invalidate_user_context(org_id: uuid.UUID, user_id: uuid.UUID) -> None:
    """DEL the cache entry. Call this from every write path that changes what
    `get_user_context` returns for `(org_id, user_id)`.

    Wired today from `PATCH /users/{user_id}` (role/skills/bio all invalidate; simplest correct
    rule is "any profile write invalidates" rather than enumerating which fields feed the
    cached shape). The plan also calls for invalidation "on `team_members` changes", but as of
    this task NO route in this codebase writes `team_members` at all (`core-api`'s
    `org_hierarchy.py` only lists/creates teams and departments - there is no add/remove-member
    endpoint yet). There is therefore nothing to wire it into; this function is exported and
    ready for whichever change adds that write path to call, in `core-api`, since
    `team_members` is core-api-owned and this service must not become a second writer of it.
    Until then a stale team membership self-heals at the 5-minute TTL, same as any other cache
    miss window.

    Best-effort by design, same reasoning as the cache reads/writes in `get_user_context`: an
    invalidation that fails must not fail the write it is attached to. A stale entry still
    expires on its own at the TTL either way.
    """
    key = redis_key(org_id, "user_ctx", str(user_id))
    try:
        await get_redis().delete(key)
    except Exception as exc:  # noqa: BLE001 - deliberate: see get_user_context's docstring
        logger.warning("user_ctx cache invalidation failed for %s: %s", key, exc)


def cache_expires_at_ms() -> int:
    """Best-effort hint for gRPC's `cache_expires_at` (unix ms), approximated as "now + the
    full TTL" from the moment of the call. Not authoritative (a cache HIT could be anywhere
    within its TTL window, not freshly set), and no acceptance check depends on precision here
    - it exists so the field the proto already reserves is not left permanently zero."""
    return int((time.time() + CTX_TTL) * 1000)
