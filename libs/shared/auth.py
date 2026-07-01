"""Clerk JWT verification (JWKS) + org_id extraction. Reverts the Supabase Auth drift.

Clerk issues RS256 session JWTs; org_id is injected via Clerk session claims (JWT template).
api-gateway pre-validates against cached JWKS; the service tier RE-VERIFIES here (the gateway is a
DDoS shield, not a trust delegation). The JWKS client + issuer are read lazily from the
environment so importing this module never requires Clerk env to be set.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from uuid import UUID

import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient

_bearer = HTTPBearer(auto_error=False)


@lru_cache(maxsize=1)
def _jwks_client() -> PyJWKClient:
    # https://<subdomain>.clerk.accounts.dev/.well-known/jwks.json — caches + honors key rotation.
    return PyJWKClient(os.environ["CLERK_JWKS_URL"], cache_keys=True)


def _issuer() -> str:
    return os.environ["CLERK_ISSUER"]  # https://<subdomain>.clerk.accounts.dev


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),  # noqa: B008
) -> dict[str, Any]:
    if not credentials:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bearer token")
    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(credentials.credentials).key
        payload: dict[str, Any] = jwt.decode(
            credentials.credentials,
            signing_key,
            algorithms=["RS256"],
            issuer=_issuer(),
            options={"require": ["exp", "iat", "iss"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token expired") from None
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc
    if "org_id" not in payload:  # Clerk session claim; fail closed
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token missing org_id claim")
    return payload


def user_id_from(claims: dict[str, Any]) -> str:
    return str(claims["sub"])  # Clerk user id (maps to users.clerk_user_id)


def org_id_from(claims: dict[str, Any]) -> UUID:
    return UUID(claims["org_id"])
