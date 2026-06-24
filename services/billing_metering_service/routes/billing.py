from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["billing"])


@router.get("/subscription")
async def get_subscription(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/subscription")
async def create_subscription(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/invoices")
async def list_invoices(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/plans")
async def list_plans() -> Any:
    raise NotImplementedError
