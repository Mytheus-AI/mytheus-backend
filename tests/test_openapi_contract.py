"""The emitted OpenAPI documents are the API contract, so they get asserted like one.

Three gates cover this surface and they cover different things, which is why all three exist:

- `python -m scripts.emit_openapi --check` (CI): the committed spec equals what the code
  produces. Catches "changed a route, forgot to regenerate".
- `spectral lint` (CI): the document obeys the 13 house rules. Catches "added a route that
  takes org_id in the path".
- `oasdiff breaking` (CI): the change is not breaking for existing clients. Catches "deleted
  a path", "made an optional request field required".

The tests here cover what none of those three can, and one thing one of them was wrongly
believed to cover.
"""

from __future__ import annotations

import pytest

from libs.shared.errors import ErrorCode
from libs.shared.openapi import finalize

# Every ErrorCode value ever published. ADD to this list when you add a code; never remove.
#
# This exists because the claim it enforces was documented and untrue. `.spectral.yaml` said
# "Values may be ADDED; a published value is never removed or repurposed. oasdiff enforces the
# removal half in CI." It does not: `ErrorCode` is reachable only from a RESPONSE schema, and
# narrowing a response enum is not a breaking change by oasdiff's definition - the server
# simply stops sending a value clients already handle. Verified by deleting `NOT_FOUND` from a
# copy of `core-api.yaml` and running `oasdiff breaking`, which reported the two paths deleted
# in the same probe and said nothing about the enum.
#
# Removing a code IS breaking here, for a reason specific to this program rather than to
# OpenAPI: both clients branch on `code`, and `mytheus-mobile`'s `ApiError` carries it as a
# typed field. A removed code is a branch that silently stops being reachable.
PUBLISHED_ERROR_CODES = frozenset(
    {
        "MALFORMED_BODY",
        "INVALID_CURSOR",
        "AUTH_REQUIRED",
        "TOKEN_EXPIRED",
        "FORBIDDEN_ROLE",
        "CROSS_TENANT_DENIED",
        "NOT_FOUND",
        "IDEMPOTENCY_KEY_REUSED",
        "VERSION_CONFLICT",
        "VALIDATION_FAILED",
        "RATE_LIMITED",
        "QUOTA_EXCEEDED",
        "INTERNAL",
        "DEPENDENCY_UNAVAILABLE",
        "NOT_IMPLEMENTED",
        "NOT_A_PARTICIPANT",
        "CONSENT_REQUIRED",
    }
)


def _raw_document() -> dict:
    """A minimal FastAPI-shaped document: one authenticated route, one public one."""
    return {
        "openapi": "3.1.0",
        "info": {"title": "probe", "version": "1.0.0"},
        "components": {
            "securitySchemes": {"HTTPBearer": {"type": "http", "scheme": "bearer"}},
            "schemas": {},
        },
        "paths": {
            "/api/v1/things": {
                "post": {
                    "operationId": "create_thing",
                    "security": [{"HTTPBearer": []}],
                    "responses": {
                        "200": {"description": "ok"},
                        "422": {
                            "description": "validation",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        },
                    },
                }
            },
            "/clerk/webhook": {
                "post": {
                    "operationId": "clerk_webhook",
                    "responses": {"200": {"description": "ok"}},
                }
            },
            "/healthz": {
                "get": {"operationId": "healthz", "responses": {"200": {"description": "ok"}}}
            },
        },
    }


def test_no_published_error_code_is_ever_removed() -> None:
    """The frozen half of the enum contract, enforced where oasdiff cannot enforce it."""
    current = {code.value for code in ErrorCode}
    removed = PUBLISHED_ERROR_CODES - current
    assert removed == set(), (
        f"error codes removed from libs.shared.errors.ErrorCode: {sorted(removed)}. "
        "Both clients branch on `code`; a removed value is a branch that silently stops "
        "being reachable. Add codes, never remove them."
    )


def test_the_spec_enum_is_the_python_enum() -> None:
    """The whole point of emitting rather than hand-writing: one source, so no sync step."""
    schema = finalize(_raw_document())
    assert schema["components"]["schemas"]["ErrorCode"]["enum"] == [c.value for c in ErrorCode]


def test_document_declares_security_once_and_reuses_fastapis_scheme() -> None:
    """No second scheme invented alongside FastAPI's own.

    The first version of `_install_security` added a `bearerAuth` scheme next to the
    `HTTPBearer` one FastAPI emits from the `Depends` chain, so the document advertised two
    different ways to authenticate the same request.
    """
    schema = finalize(_raw_document())
    schemes = schema["components"]["securitySchemes"]
    assert list(schemes) == ["HTTPBearer"]
    assert schema["security"] == [{"HTTPBearer": []}]


def test_authenticated_operations_inherit_the_default_and_document_401() -> None:
    schema = finalize(_raw_document())
    operation = schema["paths"]["/api/v1/things"]["post"]
    assert "security" not in operation, "redundant per-operation copy of the document default"
    assert "401" in operation["responses"]
    assert "500" in operation["responses"]


@pytest.mark.parametrize("path", ["/clerk/webhook", "/healthz"])
def test_public_operations_opt_out_explicitly_and_document_no_401(path: str) -> None:
    """`security: []` is the visible exception; a 401 there would be a documented fiction.

    A webhook is not unauthenticated - it verifies a Svix or LiveKit signature over the raw
    body before parsing it. What it does not do is accept a bearer token, so promising a 401
    would describe a response it never sends.
    """
    schema = finalize(_raw_document())
    operation = schema["paths"][path]["post" if "webhook" in path else "get"]
    assert operation["security"] == []
    assert "401" not in operation["responses"]


def test_every_documented_error_is_problem_json() -> None:
    """FastAPI documents 422 as application/json; the service returns problem+json.

    Left alone, the single error FastAPI does document is the one a generated client parses
    wrongly.
    """
    schema = finalize(_raw_document())
    for methods in schema["paths"].values():
        for operation in methods.values():
            for status, response in operation["responses"].items():
                if status[0] in "45":
                    assert list(response["content"]) == [
                        "application/problem+json"
                    ], f"{status} is not problem+json"


def test_mutating_routes_document_the_idempotency_header_and_webhooks_do_not() -> None:
    """The header the middleware reads, on the methods it reads it for.

    Webhooks are excluded and that is the contract: they dedupe on the provider's own event id
    through the inbox, because Clerk and LiveKit will not attach a header we invented.
    """
    schema = finalize(_raw_document())
    names = [p["name"] for p in schema["paths"]["/api/v1/things"]["post"]["parameters"]]
    assert "Idempotency-Key" in names
    assert "parameters" not in schema["paths"]["/clerk/webhook"]["post"]


def test_finalize_is_idempotent() -> None:
    """Running it twice is the same as running it once.

    `install()` caches, so this should never happen in the app - but the emitter calls
    `app.openapi()` more than once per service, and a post-process that appended a duplicate
    `Idempotency-Key` parameter on every call would produce an invalid document only on the
    second run, which is the kind of bug that shows up once in CI and never locally.
    """
    once = finalize(_raw_document())
    twice = finalize(finalize(_raw_document()))
    assert once == twice
