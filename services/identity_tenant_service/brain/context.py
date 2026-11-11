"""`get_org_context()`: the company brain as services read it (ADR-0039 A9).

Cached at `nexus:{org_id}:org_ctx`, TTL 300 s, evicted by every Part A write
(`invalidate_org_context`). Same contract as `context.service.get_user_context`: the cache is an
optimization, never a dependency, so every Redis failure falls through to Postgres and a corrupt
entry is a miss. `session` must already have `app.current_org` set for `org_id`.

An org with no brain data is not an error: every field is empty. That is the ADR's "opt-in by
data" rule, and it lets a caller treat a fresh tenant and a configured one the same way.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key

logger = logging.getLogger(__name__)

ORG_CTX_TTL = 300

_SELECT_PROFILE = text(
    "SELECT description, industry, products, customers FROM org_profiles WHERE org_id = :o"
)
_SELECT_GLOSSARY = text(
    "SELECT term, kind, definition, aliases FROM org_glossary_terms ORDER BY lower(term), id"
)
_SELECT_OWNERS = text("SELECT topic, owner_user_id FROM topic_owners ORDER BY lower(topic), id")
_SELECT_GOALS = text(
    "SELECT id, title, period FROM org_goals WHERE status = 'active' "
    "ORDER BY position, created_at, id"
)
_SELECT_NEVER_RECORD = text("SELECT never_record_topics FROM tenant_settings WHERE org_id = :o")
_SELECT_WATCHES = text(
    "SELECT user_id, topic FROM topic_watches ORDER BY user_id, lower(topic), id"
)

# People structure: departments and teams with what they do, so every agent knows how the
# company is organised (whose role a note is for, which team owns what a meeting is discussing).
# Read from core-api's tables, the same direct read `context/service.py` already makes; never
# written here. Member user ids are NOT carried: an agent gets a person's own team ids from
# GetUserContext, and the org-wide picture needs only the head count.
_SELECT_DEPARTMENTS = text(
    "SELECT id, name, description, parent_id FROM departments "
    "WHERE deleted_at IS NULL ORDER BY lower(name), id"
)
_SELECT_TEAMS = text(
    "SELECT t.id, t.name, t.description, t.department_id, "
    "(SELECT count(*) FROM team_members tm WHERE tm.team_id = t.id) AS member_count "
    "FROM teams t WHERE t.deleted_at IS NULL ORDER BY lower(t.name), t.id"
)


def org_ctx_key(org_id: uuid.UUID) -> str:
    return redis_key(org_id, "org_ctx")


async def compute_org_context(session: AsyncSession, org_id: uuid.UUID) -> dict[str, Any]:
    o = {"o": str(org_id)}
    profile = (await session.execute(_SELECT_PROFILE, o)).mappings().first()
    glossary = (await session.execute(_SELECT_GLOSSARY)).mappings().all()
    owners = (await session.execute(_SELECT_OWNERS)).mappings().all()
    goals = (await session.execute(_SELECT_GOALS)).mappings().all()
    settings = (await session.execute(_SELECT_NEVER_RECORD, o)).mappings().first()
    watches = (await session.execute(_SELECT_WATCHES)).mappings().all()
    departments = (await session.execute(_SELECT_DEPARTMENTS)).mappings().all()
    teams = (await session.execute(_SELECT_TEAMS)).mappings().all()
    live_departments = {str(d["id"]) for d in departments}
    return {
        "description": profile["description"] if profile else "",
        "industry": profile["industry"] if profile else "",
        "products": list(profile["products"]) if profile else [],
        "customers": list(profile["customers"]) if profile else [],
        "glossary": [
            {
                "term": g["term"],
                "kind": g["kind"],
                "definition": g["definition"] or "",
                "aliases": list(g["aliases"] or []),
            }
            for g in glossary
        ],
        "topic_owners": [
            {"topic": t["topic"], "owner_user_id": str(t["owner_user_id"])} for t in owners
        ],
        "goals": [
            {"id": str(g["id"]), "title": g["title"], "period": g["period"] or ""} for g in goals
        ],
        "never_record_topics": list(settings["never_record_topics"]) if settings else [],
        "topic_watches": [{"user_id": str(w["user_id"]), "topic": w["topic"]} for w in watches],
        "departments": [
            {
                "id": str(d["id"]),
                "name": d["name"],
                "description": d["description"] or "",
                # a parent that was deleted reads as top level, like the People page shows it
                "parent_id": (
                    str(d["parent_id"])
                    if d["parent_id"] and str(d["parent_id"]) in live_departments
                    else ""
                ),
            }
            for d in departments
        ],
        "teams": [
            {
                "id": str(t["id"]),
                "name": t["name"],
                "description": t["description"] or "",
                "department_id": (
                    str(t["department_id"])
                    if t["department_id"] and str(t["department_id"]) in live_departments
                    else ""
                ),
                "member_count": int(t["member_count"] or 0),
            }
            for t in teams
        ],
    }


async def get_org_context(session: AsyncSession, org_id: uuid.UUID) -> dict[str, Any]:
    """Cache-then-compute. Never raises for a Redis problem; raises for a Postgres one."""
    key = org_ctx_key(org_id)
    cached: bytes | None = None
    try:
        cached = await get_redis().get(key)
    except Exception as exc:  # noqa: BLE001 - a cache failure must fall open to Postgres
        logger.warning("org_ctx cache read failed for %s: %s", key, exc)
    if cached:
        try:
            parsed: dict[str, Any] = json.loads(cached)
            return parsed
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            logger.warning("org_ctx cache entry corrupt for %s: %s", key, exc)

    ctx = await compute_org_context(session, org_id)
    try:
        await get_redis().set(key, json.dumps(ctx), ex=ORG_CTX_TTL)
    except Exception as exc:  # noqa: BLE001 - see above
        logger.warning("org_ctx cache write failed for %s: %s", key, exc)
    return ctx


async def invalidate_org_context(org_id: uuid.UUID) -> None:
    """DEL the cached context. Called after the commit of every Part A write.

    After, not before: evicting inside the transaction would let a concurrent reader re-cache
    the pre-commit state. Best-effort like `invalidate_user_context`: the write has already
    committed, and a stale entry expires at the 5-minute TTL regardless.
    """
    key = org_ctx_key(org_id)
    try:
        await get_redis().delete(key)
    except Exception as exc:  # noqa: BLE001 - see get_org_context
        logger.warning("org_ctx cache invalidation failed for %s: %s", key, exc)
