from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["oauth"])

PROVIDERS = ("google_calendar", "slack", "jira", "linear")


@router.get("/{provider}/connect")
async def start_oauth(provider: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/{provider}/callback")
async def oauth_callback(provider: str, code: str, state: str) -> Any:
    raise NotImplementedError


@router.delete("/{provider}/disconnect")
async def disconnect(provider: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
