"""Service identity for internal-only routes, where there is no user to authenticate.

WHY THIS EXISTS, AND WHY IT IS NOT THE JWT PATTERN

Every REST surface in this program re-verifies a Clerk session JWT itself and takes `org_id`
from the verified claim - `libs/shared/auth.py` here, and its faithful reimplementation in
`mytheus-intelligence/libs/shared/auth.py`. That pattern is correct and is not replaced by
anything in this module. It has one hard precondition: a user is present.

The PII vault's mint path has no user. `transcription-service` mints placeholders while
proxying live meeting audio, from a Redpanda consumer and a Deepgram callback, with no request
and no session token anywhere in scope. The same is true of `POST /ner`. Passing a user's token
down the audio path was considered and is worse than this: it would mean holding a
participant's credential for the duration of a meeting and using it for writes they never
made, which corrupts the audit trail's meaning as well as its contents.

So this is machine-to-machine authentication, and it is deliberately the smallest thing that
is honestly one: a pre-shared secret, compared in constant time, required, with no default.

WHAT IT IS AND IS NOT WORTH

It authenticates the CALLER as "a service in this deployment". It does NOT authorize a
tenant: the caller names its own `org_id` in the request body, and this module cannot check
that claim against anything. A leaked internal secret is therefore a cross-tenant capability
on the routes that use it. That is stated plainly rather than buried, and it is why:

  - it guards only internal routes that are not exposed through `api-gateway`;
  - every `resolve` is audited unconditionally (`audit_logs`), so a disclosure made with a
    stolen secret is at least *recorded* against the tenant it touched; and
  - `mint` writes nothing a caller can read back - minting is idempotent by construction and
    returns only opaque tokens, so the reachable damage is bounded well below `resolve`'s.

The real answer is workload identity: mTLS with SPIFFE/SPIRE, or short-lived signed service
tokens issued per deployment, where a compromised credential expires and is attributable to
one workload. That needs a service mesh the program does not have on a single Docker Compose
node (ADR-0016), so it is named here as the successor rather than pretended into existence.
When it lands, this module is the one seam that changes.

OPERATIONALLY

`INTERNAL_SERVICE_TOKEN` must be set on every service that calls or serves an internal route,
and it has no default. An authentication mechanism with a default is an authentication
mechanism that is off in exactly the deployment nobody checked.
"""

from __future__ import annotations

import hmac
import os

from fastapi import Header

from libs.shared.errors import ErrorCode, ProblemDetail

ENV_VAR = "INTERNAL_SERVICE_TOKEN"
HEADER = "X-Internal-Service-Token"

# The minimum that is not trivially guessable. A short shared secret is a password, and this
# one is never rotated by a human typing it, so there is no usability argument for allowing it.
_MIN_TOKEN_LEN = 32


def configured_token() -> str:
    """The expected secret. Raises if unset or too short, at call time rather than import time.

    Deliberately not `lru_cache`d: a cached miss would survive a fixed configuration for the
    life of the process, and tests set this per case.
    """
    token = os.environ.get(ENV_VAR, "").strip()
    if not token:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "This deployment has no internal service credential configured",
        )
    if len(token) < _MIN_TOKEN_LEN:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            f"{ENV_VAR} must be at least {_MIN_TOKEN_LEN} characters",
        )
    return token


async def require_internal_service(
    x_internal_service_token: str = Header(default=""),
) -> None:
    """Route dependency for an internal-only endpoint.

    `hmac.compare_digest` rather than `==`: string equality short-circuits on the first
    differing byte, which leaks the shared secret to anyone who can time enough requests. That
    is a real attack against a long-lived credential, and the fix costs nothing.
    """
    expected = configured_token()
    if not hmac.compare_digest(x_internal_service_token, expected):
        raise ProblemDetail(
            401,
            ErrorCode.AUTH_REQUIRED,
            "This endpoint is internal and requires a valid service credential",
        )
