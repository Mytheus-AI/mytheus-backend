"""Supabase JWT verification and org_id extraction.

Supabase issues HS256 JWTs signed with SUPABASE_JWT_SECRET. The `sub` claim is
the Supabase user UUID (= public.users.id). The `org_id` custom claim is injected
by the custom_access_token_hook Postgres function (see migrations/sql/custom_claims_hook.sql).

Flow:
  api-gateway  →  pre-validate JWT (nginx lua or envoy ext_authz)
  service tier →  re-verify here (gateway is a DDoS shield, not a trust delegation)
"""
from __future__ import annotations

import os
from typing import Any
from uuid import UUID

import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer = HTTPBearer(auto_error=False)

_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")
_ALGORITHM = "HS256"
_AUDIENCE = "authenticated"


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> dict[str, Any]:
    """Verify the Supabase JWT and return its claims dict.

    Raises 401 if missing, expired, or invalid. The returned dict always
    contains ``sub`` (user UUID str) and ``org_id`` (str, from custom claim).
    """
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    try:
        payload: dict[str, Any] = jwt.decode(
            credentials.credentials,
            _JWT_SECRET,
            algorithms=[_ALGORITHM],
            audience=_AUDIENCE,
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    if "org_id" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing org_id claim — ensure custom_access_token_hook is configured",
        )
    return payload


def user_id_from(claims: dict[str, Any]) -> UUID:
    return UUID(claims["sub"])


def org_id_from(claims: dict[str, Any]) -> UUID:
    return UUID(claims["org_id"])
