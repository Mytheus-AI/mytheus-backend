"""Meeting types (ADR-0039 B1): `GET /api/v1/meeting-templates` for members; `POST`, `PATCH`,
`DELETE /api/v1/meeting-templates/{id}` for the org roles `owner` and `admin` (company-wide
data). At most 50 per org (422). A meeting type in another tenant is a 404 (RLS hides it).

Deleting a type never touches the meetings made from it: `meetings.template_id` is
`ON DELETE SET NULL`.
"""

from __future__ import annotations

import json
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_MEETING_TEMPLATE_CREATED,
    ACTION_MEETING_TEMPLATE_DELETED,
    ACTION_MEETING_TEMPLATE_UPDATED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import (
    MAX_TEMPLATES,
    MeetingTemplate,
    MeetingTemplateCreate,
    MeetingTemplateList,
    MeetingTemplateUpdate,
)

router = APIRouter(prefix="/api/v1", tags=["meeting-templates"])

_COLUMNS = (
    "id, name, description, default_duration_minutes, agenda, confidential, created_by, "
    "created_at, updated_at"
)


def _template(row: Any) -> MeetingTemplate:
    return MeetingTemplate(**dict(row))


@router.get("/meeting-templates", response_model=MeetingTemplateList)
async def list_templates(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingTemplateList:
    rows = (
        (
            await session.execute(
                text(f"SELECT {_COLUMNS} FROM meeting_templates ORDER BY lower(name), id")
            )
        )
        .mappings()
        .all()
    )
    return MeetingTemplateList(items=[_template(r) for r in rows])


@router.post(
    "/meeting-templates", status_code=status.HTTP_201_CREATED, response_model=MeetingTemplate
)
async def create_template(
    body: MeetingTemplateCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingTemplate:
    # Serialize concurrent creates per tenant so two cannot both pass the cap check.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('meeting_templates:' || :o))"),
        {"o": str(ctx.org_id)},
    )
    count = (await session.execute(text("SELECT count(*) FROM meeting_templates"))).scalar_one()
    if count >= MAX_TEMPLATES:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"At most {MAX_TEMPLATES} meeting types per organization",
            extra={
                "errors": [
                    {
                        "loc": ["body", "name"],
                        "msg": f"at most {MAX_TEMPLATES} meeting types per organization",
                        "type": "value_error",
                    }
                ]
            },
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meeting_templates (org_id, name, description, "
                    "default_duration_minutes, agenda, confidential, created_by) "
                    "VALUES (:o, :n, :d, :dur, CAST(:a AS jsonb), :c, :by) "
                    f"RETURNING {_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "n": body.name,
                    "d": body.description,
                    "dur": body.default_duration_minutes,
                    "a": json.dumps([i.model_dump() for i in body.agenda]),
                    "c": body.confidential,
                    "by": str(ctx.user_id) if ctx.user_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_TEMPLATE_CREATED,
        target_type="meeting_template",
        target_id=row["id"],
        metadata={"name": body.name},
        ip=ctx.ip,
    )
    response.headers["Location"] = f"/api/v1/meeting-templates/{row['id']}"
    return _template(row)


@router.patch("/meeting-templates/{template_id}", response_model=MeetingTemplate)
async def update_template(
    template_id: uuid.UUID,
    body: MeetingTemplateUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingTemplate:
    fields = body.model_dump(exclude_unset=True)
    params: dict[str, Any] = {"id": str(template_id)}
    sets: list[str] = []
    for name, value in fields.items():
        if name == "agenda":
            sets.append("agenda = CAST(:agenda AS jsonb)")
            params["agenda"] = json.dumps(value)
        else:
            sets.append(f"{name} = :{name}")
            params[name] = value
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE meeting_templates SET {', '.join(sets)}, updated_at = now() "
                    f"WHERE id = :id RETURNING {_COLUMNS}"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_TEMPLATE_UPDATED,
        target_type="meeting_template",
        target_id=template_id,
        metadata={"changed": sorted(fields)},
        ip=ctx.ip,
    )
    return _template(row)


@router.delete("/meeting-templates/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_template(
    template_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    deleted = (
        await session.execute(
            text("DELETE FROM meeting_templates WHERE id = :id RETURNING name"),
            {"id": str(template_id)},
        )
    ).first()
    if deleted is None:
        raise not_found()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_TEMPLATE_DELETED,
        target_type="meeting_template",
        target_id=template_id,
        metadata={"name": deleted[0]},
        ip=ctx.ip,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
