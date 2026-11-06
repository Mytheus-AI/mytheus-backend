"""Service-to-service authentication for the internal mint route.

The callers of `/pii-tokens/mint` are the redaction boundaries (ADR-0020 D2), not users:
`ingestion-service` runs off the bus with no user JWT at all. So this route cannot reuse the
Clerk chain in `libs/shared/authz.py`; it authenticates the calling SERVICE and takes `org_id`
from the `X-Mytheus-Org` header, which is how canon says `org_id` rides a REST hop.

INTERIM MECHANISM, recorded as such in ADR-0024: a per-caller shared secret from
`MYTHEUS_PII_VAULT_CALLER_TOKENS` (`service=secret,service=secret`). Per caller rather than one
shared secret so a leaked credential is attributable and revocable alone. The production
answer is mesh identity (mTLS / SPIFFE) once the Kubernetes substrate exists; this dependency
is the single place that changes.

Fails closed everywhere: unset config, unknown service, wrong secret, missing or malformed
org header, unknown or inactive org - each refuses, and none reveals which check failed
beyond 401-vs-404.
"""

from __future__ import annotations

import hmac
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from fastapi import Request

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id

SERVICE_HEADER = "x-mytheus-service"
SERVICE_TOKEN_HEADER = "x-mytheus-service-token"
ORG_HEADER = "x-mytheus-org"

# ADR-0020 D2: the redaction boundaries, and nobody else.
ALLOWED_CALLERS = frozenset(
    {"transcription-service", "ingestion-service", "agent-runtime-service", "rag-service"}
)


@dataclass(frozen=True)
class InternalCaller:
    service: str
    org_id: uuid.UUID


def parse_caller_tokens(raw: str) -> dict[str, bytes]:
    """Parse `service=secret,...`. Unknown service names are a config error, not ignored."""
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError("MYTHEUS_PII_VAULT_CALLER_TOKENS entries must be service=secret")
        if name not in ALLOWED_CALLERS:
            raise ValueError(f"{name!r} is not an allowed pii-vault caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


def _unauthenticated() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")


def authenticate_service(headers: Mapping[str, str], tokens: Mapping[str, bytes]) -> str:
    service = headers.get(SERVICE_HEADER, "")
    presented = headers.get(SERVICE_TOKEN_HEADER, "").encode("utf-8")
    expected = tokens.get(service)
    # Compare against a dummy on a miss so timing does not reveal which services exist.
    if not hmac.compare_digest(presented, expected or b"\x00" * 32) or expected is None:
        raise _unauthenticated()
    return service


async def internal_caller(request: Request) -> InternalCaller:
    """FastAPI dependency: authenticated boundary service + active tenant, or refuse."""
    try:
        tokens = parse_caller_tokens(os.environ.get("MYTHEUS_PII_VAULT_CALLER_TOKENS", ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "pii-vault caller config invalid"
        ) from exc
    if not tokens:
        raise _unauthenticated()
    service = authenticate_service(request.headers, tokens)
    try:
        org_id = uuid.UUID(request.headers.get(ORG_HEADER, ""))
    except ValueError as exc:
        raise _unauthenticated() from exc
    async with get_sessionmaker()() as session, session.begin():
        resolved = await resolve_org_by_id(session, org_id)
    if resolved is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return InternalCaller(service=service, org_id=resolved)
