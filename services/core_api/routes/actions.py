from typing import Any
from fastapi import APIRouter, Depends, Header
from libs.shared.auth import verify_token

router = APIRouter(tags=["actions"])


@router.get("")
async def list_actions(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/{action_id}")
async def get_action(action_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/{action_id}/confirm")
async def confirm_action(
    action_id: str,
    claims: dict = Depends(verify_token),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Any:
    """Confirm a candidate action item. Emits to actions topic via outbox."""
    raise NotImplementedError
