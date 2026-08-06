"""The person-profile composite. Plan 5-6 section 3.5.

Wraps `get_user_context()` (identity-tenant-service, Plan 5-6 section 3.3) rather than
re-deriving its shape: that function already assembles role, skills, team, department, recent
meetings and topics, cached at `nexus:{org_id}:user_ctx:{user_id}` for 300s. Calling it
in-process is the same cross-service-read precedent this codebase already uses throughout
(`context/service.py` reading core-api's `teams`/`meetings` directly; `bff/dashboard.py`
reading `meetings` directly; `provisioning.py`'s seeder calling core-api's `create_team`/
`create_meeting` in-process) - `bff` and `identity-tenant-service` are two modules of one
deployable today (`mytheus-backend/CLAUDE.md`: "one deployable, every service a Python
module"). When the modules are later split onto separate Kubernetes services, this call
becomes a call to the already-existing `GetUserContext` gRPC method or `GET /context/{id}`
REST route - the same extraction seam every other in-process cross-service read in this repo
already carries.

The one field this endpoint adds beyond `get_user_context()`'s own shape is `email`: identity
data the cached context deliberately excludes (see `context/service.py`'s module docstring,
which also excludes `rbac_tier` for a different reason - "cache the mapping, never the
verdict", ADR-0010 D2). `email` carries no such reason to stay uncached; it is included here
because a person-profile screen needs one to render.

No arm/degrade wrapping here, unlike `dashboard.py` and `org_chart.py`. Those aggregate
several genuinely independent read models where a partial screen is still useful. This
endpoint is one resource behind one already-degrading call (`get_user_context` itself never
lets a Redis failure become a request failure - it falls open to Postgres); a further failure
here is a real Postgres failure with nothing partial to show, so it is left to propagate to
the shared unhandled-exception handler (`libs/shared/errors.py`), which already returns a
clean RFC 7807 500 without leaking internals. `None` from `get_user_context` is a different,
expected case - "this user does not exist in your tenant" - and is a plain 404, never a 500.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.identity_tenant_service.context.service import get_user_context
from services.identity_tenant_service.schemas import DepartmentRef, MeetingRef, TeamRef

router = APIRouter(prefix="/bff", tags=["bff"])

_SELECT_EMAIL = text("SELECT email FROM users WHERE id = :uid AND deleted_at IS NULL")


class PersonProfile(BaseModel):
    """`GET /bff/people/{user_id}` response - `get_user_context()`'s shape plus `email`."""

    user_id: uuid.UUID
    org_id: uuid.UUID
    email: str
    role: str | None
    skills: list[str]
    bio: str | None
    avatar_url: str | None
    team: list[TeamRef]
    department: DepartmentRef | None
    recent_meetings: list[MeetingRef]
    topics: list[str]


@router.get("/people/{user_id}", response_model=PersonProfile)
async def person_profile(
    user_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> dict[str, object]:
    """A `user_id` in another tenant and a genuinely unknown one are indistinguishable here,
    both 404 - the same contract `GET /context/{user_id}` already gives, since this wraps that
    same resolver (existence is tenant-confidential, `libs/shared/authz.py::not_found`)."""
    context = await get_user_context(session, ctx.org_id, user_id)
    if context is None:
        raise not_found()

    email_row = (await session.execute(_SELECT_EMAIL, {"uid": str(user_id)})).mappings().first()
    # get_user_context already confirmed this row is visible in this tenant, in this same
    # transaction/session - a missing email row here would mean the two queries disagree about
    # whether the user exists, which is a bug worth a loud failure, not a silent 404.
    assert (
        email_row is not None
    ), f"user {user_id} resolved by get_user_context but has no row for the email lookup"

    return {**context, "email": email_row["email"]}
