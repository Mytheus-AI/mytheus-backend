"""User profile: role/skills/bio. Plan 5-6 Task 1 (3.1) step 3, the one route that section
genuinely adds here.

`identity-tenant-service` owns `users` (canon `_canon/02-data-model.md` section 2.1 and
`_canon/01-service-catalog.md`'s service map: `users` (role, skills[], bio, avatar_url)), so a
route that writes those columns belongs in this service and nowhere else. This module does NOT
add the plan's `GET /orgs/{org_id}/departments` / `/teams` / `/teams/{team_id}/members` reads -
see the Task 1 report for why: canon's own service catalog (and `mytheus-backend/CLAUDE.md`)
assigns the `/teams` and `/departments` prefixes, and the CRUD behind them, to `core-api`
(`services/core_api/routes/org_hierarchy.py` already serves the list reads), and the plan's own
`{org_id}` path shape would violate the "`org_id` never in a path" invariant that
`org_hierarchy.py`'s docstring already explains for the sibling `/teams` route. Adding a second
read surface for the same tables under a different prefix is not what "add what is genuinely
missing" calls for.

Two structural rules on this route, both mirroring `core_api/routes/meetings.py`:

- `org_id` is never a path or query parameter; it comes from `RequestContext`.
- A user in another tenant is invisible under RLS, so a bad `user_id` here is a 404, not a 403 -
  existence is tenant-confidential, same as every other resource route in this program.

Authorization is a judgment call the plan text does not specify (it only names the route and the
editable fields): a caller may PATCH their own profile at `member` tier and above; PATCHing
SOMEONE ELSE's profile requires `admin` or above, the same threshold `org_hierarchy.py` uses for
writes to the org-hierarchy tables one tier up. This is deliberately looser than requiring admin
for every call (a member cannot self-serve `bio`/`skills` updates otherwise) and deliberately
stricter than allowing any member to edit anyone (making `role`, in particular, self-service for
everyone in the tenant).
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_USER_PROFILE_UPDATED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.identity_tenant_service.context.service import invalidate_user_context
from services.identity_tenant_service.schemas import (
    Member,
    MemberList,
    UserProfile,
    UserProfileUpdate,
)

router = APIRouter(tags=["users"])

_USER_COLUMNS = (
    "id, email, display_name, role, skills, bio, avatar_url, listen_for, ignore_topics, "
    "agent_name, "
    "agent_avatar_seed, agent_personality, agent_instructions, created_at, updated_at"
)


def _row_to_profile(row: Any) -> UserProfile:
    return UserProfile(**dict(row))


async def _fetch_user(session: AsyncSession, user_id: uuid.UUID) -> Any:
    """Fetch or 404. No `org_id` predicate: RLS already scopes it (see `_fetch_meeting` in
    `core_api/routes/meetings.py` for the same reasoning, verbatim)."""
    row = (
        (
            await session.execute(
                text(f"SELECT {_USER_COLUMNS} FROM users WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


# Bounded rather than paged: a workspace in this product is a team, and the invite picker and
# the workspace page want everyone at once. A tenant past this size needs paging, not a raise.
_MEMBER_LIMIT = 500


@router.get("/users", response_model=MemberList)
async def list_members(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MemberList:
    """authz: member. Everyone in the caller's tenant, with their access tier. RLS scopes both
    tables to the tenant; there is no `org_id` predicate to forget."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT u.id, u.clerk_user_id, u.email, u.display_name, u.avatar_url, u.role, "
                    "g.tier, "
                    "u.agent_name, u.agent_avatar_seed "
                    "FROM users u LEFT JOIN rbac_grants g ON g.user_id = u.id "
                    "WHERE u.deleted_at IS NULL ORDER BY lower(u.email), u.id LIMIT :n"
                ),
                {"n": _MEMBER_LIMIT},
            )
        )
        .mappings()
        .all()
    )
    return MemberList(items=[Member(**dict(r)) for r in rows])


@router.get("/users/me", response_model=UserProfile)
async def read_my_profile(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> UserProfile:
    """authz: member. The caller's own profile, working profile and agent persona (ADR-0038
    D3). A principal with no `users` row in this tenant is a 404, like any unknown user."""
    if ctx.user_id is None:
        raise not_found()
    return _row_to_profile(await _fetch_user(session, ctx.user_id))


@router.patch("/users/{user_id}", response_model=UserProfile)
async def update_user_profile(
    user_id: uuid.UUID,
    body: UserProfileUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> UserProfile:
    # Existence FIRST, and RLS-scoped: a cross-tenant id is invisible here and 404s below
    # before the tier check ever runs, so a low-tier caller cannot use this route's 403 to
    # learn that an id exists in another tenant. Only once the row is known to be in the
    # caller's OWN tenant does "may this caller edit it" become the relevant question.
    await _fetch_user(session, user_id)

    if ctx.user_id != user_id and not ctx.satisfies("admin"):
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "Editing another user's profile requires the admin role or above",
        )

    # ADR-0038 D3: the persona is the person's own. Not even an admin may rename or re-voice
    # someone else's agent, so this check is not relaxed by tier.
    if body.agent_fields_set() and ctx.user_id != user_id:
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "Only the person themselves can change their agent",
        )

    # A person's name is theirs to set (the Clerk sync writes it too); an admin cannot rename
    # someone, for the same reason they cannot rename their agent.
    if "display_name" in body.model_fields_set and ctx.user_id != user_id:
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "Only the person themselves can change their name",
        )

    fields = body.model_dump(exclude_unset=True)
    set_clauses = ", ".join(f"{col} = :{col}" for col in fields)
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE users SET {set_clauses}, updated_at = now() "
                    f"WHERE id = :id RETURNING {_USER_COLUMNS}"
                ),
                {**fields, "id": str(user_id)},
            )
        )
        .mappings()
        .one()
    )

    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_USER_PROFILE_UPDATED,
        target_type="user",
        target_id=user_id,
        metadata={"fields": sorted(fields)},
        ip=ctx.ip,
    )

    # get_user_context() caches role/skills/bio/avatar_url (and, since ADR-0038, the working
    # profile and persona) for this user; any of them changing
    # makes the cached entry stale. Evicting here (rather than only relying on the 5-minute
    # TTL) is what "PATCH /users/{id} changing role evicts the key" (Plan 5-6 section 3.3
    # acceptance check) requires. Best-effort: see invalidate_user_context's docstring for why
    # a failure here must not fail this write.
    await invalidate_user_context(ctx.org_id, user_id)

    return _row_to_profile(row)
