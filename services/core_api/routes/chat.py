from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["chat"])


@router.get("/meetings/{meeting_id}/messages")
async def list_messages(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/meetings/{meeting_id}/messages")
async def post_message(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
