"""`GET /context/{user_id}` - the REST surface for `get_user_context()`. Plan 5-6 section 3.3
step 2.

`org_id` resolves from the request context (Clerk JWT via `tenant_session`), never the path -
`user_id` in the path names WHICH user's context to read, not which tenant to read it from. A
`user_id` in another tenant is invisible under RLS, so it 404s here exactly like every other
resource route in this program (existence is tenant-confidential).

Authorization: `member` tier or above, no self-vs-other restriction. Unlike `PATCH
/users/{user_id}` (a write, gated tighter), this is a read of ordinary team-visible profile
information - the same "any member may view" threshold `core_api/routes/org_hierarchy.py`
already uses for `GET /teams`/`GET /departments`. The plan does not specify authorization for
this route; this mirrors the closest precedent in the codebase rather than inventing a new
threshold.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.identity_tenant_service.context.service import get_user_context
from services.identity_tenant_service.schemas import UserContext

router = APIRouter(tags=["context"])


@router.get("/context/{user_id}", response_model=UserContext)
async def read_user_context(
    user_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> dict[str, object]:
    result = await get_user_context(session, ctx.org_id, user_id)
    if result is None:
        raise not_found()
    return result
