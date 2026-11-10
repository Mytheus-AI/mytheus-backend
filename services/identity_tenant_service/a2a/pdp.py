"""The A2A policy decision point (ADR-0013; P10 contract section 3).

`authorize()` is the whole policy. Deny by default, explicit deny wins:

- no live row for (grantee_role, query_class, target_scope) in the tenant -> `deny`;
- any live row with `effect = 'deny'` -> `deny`;
- otherwise, a live `effect = 'allow'` row -> `allow`.

An unknown value in any field is `deny` before the database is asked: the table CHECKs the same
sets, so such a request can never match a row, and saying so early keeps the PDP from issuing a
query shaped by garbage input. The tenant is bound with `set_rls` and the query also names
`org_id`, so a missing or wrong `app.current_org` matches nothing (fail closed).

Revocation and grant changes invalidate the PEP's decision cache
(`nexus:{org_id}:a2a:grant:{grantee_role}:{query_class}:{target_scope}`, 60 s TTL, written by
agent-runtime-service) with `invalidate_decision_cache`: SCAN + DEL over
`nexus:{org_id}:a2a:grant:*`, never a `KEYS`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Literal
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import redis_key
from libs.shared.tenancy import set_rls

logger = logging.getLogger(__name__)

GRANTEE_ROLES = ("owner", "admin", "member")
QUERY_CLASSES = ("meeting_context", "decision_lineage", "action_status")
TARGET_SCOPES = ("self", "same_team", "department", "org")
EFFECTS = ("allow", "deny")

# Narrowest first. Adopting the template at a scope grants that scope and every narrower one.
SCOPE_ORDER = ("self", "same_team", "department", "org")

Decision = Literal["allow", "deny"]

_SELECT_EFFECTS = text(
    "SELECT effect FROM agent_authz_scopes WHERE org_id = :o AND grantee_role = :r "
    "AND query_class = :q AND target_scope = :s AND deleted_at IS NULL"
)


def decide(effects: Iterable[str]) -> Decision:
    """The pure rule: allow only when an allow is present and no deny is."""
    found = set(effects)
    return "allow" if "allow" in found and "deny" not in found else "deny"


def valid_tuple(grantee_role: str, query_class: str, target_scope: str) -> bool:
    return (
        grantee_role in GRANTEE_ROLES
        and query_class in QUERY_CLASSES
        and target_scope in TARGET_SCOPES
    )


async def authorize(
    session: AsyncSession,
    org_id: UUID,
    grantee_role: str,
    query_class: str,
    target_scope: str,
) -> Decision:
    """`allow` or `deny` for one A2A request shape. Raises only on a database failure, which
    the gRPC handler reports as UNAVAILABLE so the PEP denies without caching the answer."""
    if not valid_tuple(grantee_role, query_class, target_scope):
        return "deny"
    await set_rls(session, org_id)
    rows = await session.execute(
        _SELECT_EFFECTS,
        {"o": str(org_id), "r": grantee_role, "q": query_class, "s": target_scope},
    )
    return decide(str(r[0]) for r in rows)


def cache_pattern(org_id: UUID) -> str:
    return redis_key(org_id, "a2a", "grant", "*")


async def invalidate_decision_cache(redis: Redis[bytes], org_id: UUID) -> int:
    """Delete every cached decision for this tenant. Returns how many keys were removed."""
    removed = 0
    batch: list[bytes] = []
    async for key in redis.scan_iter(match=cache_pattern(org_id), count=500):
        batch.append(key)
        if len(batch) >= 500:
            removed += int(await redis.delete(*batch))
            batch = []
    if batch:
        removed += int(await redis.delete(*batch))
    return removed
