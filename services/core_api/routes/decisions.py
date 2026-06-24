from typing import Any
from fastapi import APIRouter, Depends, Header
from libs.shared.auth import verify_token

router = APIRouter(tags=["decisions"])


@router.get("")
async def list_decisions(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/{decision_id}")
async def get_decision(decision_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/{decision_id}/confirm")
async def confirm_decision(
    decision_id: str,
    claims: dict = Depends(verify_token),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Any:
    """Confirm a candidate decision surfaced by agents.

    This is the ONLY path that emits to the decisions topic via the transactional
    outbox. knowledge-graph-service creates the PRODUCED edge on receipt.
    """
    raise NotImplementedError
