"""User context endpoint (source of record for role/skills/team).

GET /context/{user_id} — heavily read by the AI tier via gRPC GetUserContext.
Result cached at nexus:{org_id}:user_ctx:{user_id} (5-min TTL).
"""
from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["context"])


@router.get("/{user_id}")
async def get_user_context(user_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
