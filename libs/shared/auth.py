"""Clerk JWT verification (JWKS) plus tenant-claim extraction.

Clerk issues RS256 session JWTs carrying its NATIVE organization id, an opaque `org_...`
string. That string is not our `org_id` and must never be treated as one: it is resolved to
the tenant root through `organizations.clerk_org_id` by `libs/shared/org_resolver.py`, and
that resolution IS the authorization checkpoint (ADR-0010 D2), not overhead on the way to one.

`api-gateway` pre-validates against cached JWKS; the service tier RE-VERIFIES here, because
the gateway is a DDoS shield and not a trust delegation. The JWKS client and issuer are read
lazily from the environment so importing this module never requires Clerk env to be set.

Four defects from the Plan 0 version, all in gap 8 of the band record:

1. `org_id_from` called `UUID(claims["org_id"])` on an opaque Clerk string. Every real token
   raised `ValueError` out of an un-handled path and became a 500, so the resolved
   tenant-claim decision had no working implementation at all.
2. The claim name was hardcoded instead of read from `CLERK_JWT_ORG_CLAIM`, so the Clerk JWT
   template could not be pointed at a custom claim without a code change.
3. `nbf` was not required, so a token minted for the future was accepted early.
4. A JWKS fetch failure became a 500. `PyJWKClientError` does not derive from
   `InvalidTokenError`, so it escaped the `except` that was meant to catch it. An identity
   provider being unreachable is a dependency outage (503, retryable), not a malformed
   request and not a bug in this service.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import jwt
from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient, PyJWKClientError

from libs.shared.errors import ErrorCode, ProblemDetail

_bearer = HTTPBearer(auto_error=False)

# Clerk's default session claim holding the organization id. Overridable because the JWT
# template is customer-configurable; the VALUE is always Clerk's opaque org id either way.
DEFAULT_ORG_CLAIM = "org_id"


@lru_cache(maxsize=1)
def _jwks_client() -> PyJWKClient:
    # https://<subdomain>.clerk.accounts.dev/.well-known/jwks.json - caches + honors rotation.
    return PyJWKClient(os.environ["CLERK_JWKS_URL"], cache_keys=True)


def _issuer() -> str:
    return os.environ["CLERK_ISSUER"]  # https://<subdomain>.clerk.accounts.dev


def org_claim_name() -> str:
    return os.environ.get("CLERK_JWT_ORG_CLAIM", DEFAULT_ORG_CLAIM)


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),  # noqa: B008
) -> dict[str, Any]:
    """Verify a Clerk session token and return its claims. Fails closed on every branch."""
    if not credentials:
        raise ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Missing bearer token")

    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(credentials.credentials).key
    except PyJWKClientError as exc:
        # The IdP is unreachable or served an unusable key set. This is NOT the caller's
        # fault and NOT a 500: a 503 tells the client to retry and keeps the outage
        # attributable to the dependency rather than to this service.
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Identity provider unavailable"
        ) from exc

    try:
        payload: dict[str, Any] = jwt.decode(
            credentials.credentials,
            signing_key,
            algorithms=["RS256"],  # never "none", never HS*: the key here is a PUBLIC key
            issuer=_issuer(),
            # nbf is required, not merely honored when present. Without it a token minted
            # with a future nbf is simply accepted early, which silently widens the window
            # an issued-but-not-yet-valid token is good for.
            options={"require": ["exp", "iat", "nbf", "iss", "sub"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise ProblemDetail(401, ErrorCode.TOKEN_EXPIRED, "Token expired") from exc
    except jwt.InvalidTokenError as exc:
        # Deliberately does not echo `str(exc)`: the message varies by failure mode and hands
        # an attacker a free oracle for which check failed.
        raise ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Invalid token") from exc

    claim = org_claim_name()
    value = payload.get(claim)
    if not isinstance(value, str) or not value:
        # Claim-absent is an explicit 401 and never a 500 (ADR-0010 D2). A token with no
        # organization is a personal-account session: authenticated, but with no tenant to
        # act in, so there is nothing it may reach.
        raise ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Token carries no organization")
    return payload


def user_id_from(claims: dict[str, Any]) -> str:
    return str(claims["sub"])  # Clerk user id (maps to users.clerk_user_id)


def clerk_org_id_from(claims: dict[str, Any]) -> str:
    """The token's raw Clerk organization id.

    OPAQUE. Do not parse it, do not cast it to UUID, do not use it as a Redis namespace or a
    partition key. It is only ever an input to `org_resolver.resolve_org_id`, whose output is
    the `org_id` everything else in the system is keyed on.
    """
    return str(claims[org_claim_name()])
