"""`GET /taxonomy/role-topics` - the REST surface for the role -> topics taxonomy. Plan 5-6
section 3.2 step 2.

Not tenant-scoped data (see `schemas.RoleTopicsResponse`): the taxonomy is the same for every
org, so this route needs no `org_id` and returns none. It still sits behind `require_tier`
like every other route in this service (CLAUDE.md: "an endpoint with no declared auth fails
review") - a caller must be an authenticated member of some org, matching the read threshold
`GET /context/{user_id}` and `core_api/routes/org_hierarchy.py`'s `GET /teams`/`GET
/departments` already use for "any member may read" resources.

`version` is optional and defaults to the process's pinned `active_version()` (env
`ROLE_TOPICS_TAXONOMY_VERSION`) when omitted, so a caller who does not care which version is
active gets the one `get_user_context()` itself is using. An unknown version 404s - no silent
fallback to another version, per the plan's own acceptance check.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from libs.shared.authz import RequestContext, require_tier
from libs.shared.errors import ErrorCode, ProblemDetail
from services.identity_tenant_service.schemas import RoleTopicsResponse
from services.identity_tenant_service.taxonomy.loader import TaxonomyVersionNotFound, load_taxonomy

router = APIRouter(tags=["taxonomy"])


@router.get("/taxonomy/role-topics", response_model=RoleTopicsResponse)
async def read_role_topics(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    version: Annotated[str | None, Query()] = None,
) -> RoleTopicsResponse:
    del ctx  # authorization only - the response carries no tenant data
    try:
        taxonomy = load_taxonomy(version)
    except TaxonomyVersionNotFound as exc:
        raise ProblemDetail(
            404, ErrorCode.NOT_FOUND, f"Unknown taxonomy version: {version!r}"
        ) from exc
    return RoleTopicsResponse(version=taxonomy.version, roles=taxonomy.roles)
