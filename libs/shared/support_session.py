"""Support sessions: Mytheus staff acting as a workspace member (ADR-0046 D4).

Clerk issues the session (an actor token), and its session JWT carries an `act` claim naming
the staff member's Clerk user. That claim is not trusted on its own. `tenant_context` calls
`resolve` for every token that has one, and the request is refused unless the workspace holds an
open `support_sessions` row for exactly that staff member and that member, unexpired and not
ended, whose `impersonate` grant is still active. So ending the session, revoking the grant or
reaching `expires_at` cuts access on the next request, however long Clerk keeps the session.

The resolved session lives in a context variable for the rest of the request, which is how the
shared `audit()` helper tags every row written during it (`actor_type = 'system'`, plus
`metadata.support_session`) without any route having to remember.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.errors import ErrorCode, ProblemDetail


@dataclass(frozen=True)
class SupportSession:
    id: UUID
    staff_user_id: UUID

    def audit_tag(self) -> dict[str, str]:
        return {"id": str(self.id), "staff_user_id": str(self.staff_user_id)}


_current: ContextVar[SupportSession | None] = ContextVar("support_session", default=None)


def current() -> SupportSession | None:
    return _current.get()


def set_current(session: SupportSession | None) -> None:
    _current.set(session)


def actor_from(claims: dict[str, Any]) -> str | None:
    """The acting staff member's Clerk user id, when the token is an impersonation session."""
    act = claims.get("act")
    if not isinstance(act, dict):
        return None
    sub = act.get("sub")
    return sub if isinstance(sub, str) and sub else None


# Run with `app.current_org` already bound to the token's workspace (RLS), so only that
# workspace's sessions and grants are visible.
_OPEN_SESSION = text(
    "SELECT s.id, s.staff_user_id FROM support_sessions s "
    "JOIN support_grants g ON g.id = s.grant_id "
    "WHERE s.staff_clerk_user_id = :actor AND s.user_id = :user "
    "AND s.ended_at IS NULL AND s.expires_at > now() "
    "AND g.scope = 'impersonate' AND g.revoked_at IS NULL AND g.expires_at > now() "
    "ORDER BY s.started_at DESC LIMIT 1"
)


def ended() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "This support session has ended")


async def resolve(session: AsyncSession, actor: str, user_id: UUID | None) -> SupportSession:
    """The open support session for this actor and member, or a 401."""
    if user_id is None:
        raise ended()
    row = (
        (await session.execute(_OPEN_SESSION, {"actor": actor, "user": str(user_id)}))
        .mappings()
        .first()
    )
    if row is None:
        raise ended()
    return SupportSession(id=row["id"], staff_user_id=row["staff_user_id"])
