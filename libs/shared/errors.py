"""RFC 7807 problem+json error model, applied to EVERY error a service can emit.

The wire contract is frozen in `mytheus-docs/design/openapi/api-gateway.yaml`: a Problem
carries `type`, `title`, `status` and `code` (plus optional `detail`, `instance`,
`trace_id`), and `code` is drawn from the ErrorCode enum and nothing else. `code` is the
stable machine-readable value - never change one once published; add, never repurpose.

Why the handlers below are broader than they look. Plan 0 registered handlers for
`ProblemDetail` and `404` only, and neither emitted a `code` field. Everything else - and
that includes every `HTTPException` raised by `libs/shared/auth.verify_token`, so the whole
authentication surface - fell through to FastAPI's default `{"detail": ...}` JSON. Clients
would therefore have had to parse two different error shapes and could not branch on `code`
at all, which is exactly what the frozen enum exists to prevent.

Cross-tenant access returns 404 (existence is tenant-confidential), never 403.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

_TYPE_BASE = "https://api.mytheus.ai/errors"


class ErrorCode(StrEnum):
    """The frozen program-wide enum. Mirrors ErrorCode in design/openapi/api-gateway.yaml.

    Every service emits from this list and no other. Keep the two in sync: the Spectral gate
    (section 3.11) lints the specs, and a value here with no counterpart there is a bug.
    """

    MALFORMED_BODY = "MALFORMED_BODY"
    INVALID_CURSOR = "INVALID_CURSOR"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    FORBIDDEN_ROLE = "FORBIDDEN_ROLE"
    CROSS_TENANT_DENIED = "CROSS_TENANT_DENIED"
    NOT_FOUND = "NOT_FOUND"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    INTERNAL = "INTERNAL"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"


# Fallback mapping for bare HTTPExceptions raised by application code and by FastAPI itself.
# Anything not listed resolves by status class in `_code_for_status`.
_STATUS_TO_CODE: dict[int, ErrorCode] = {
    400: ErrorCode.MALFORMED_BODY,
    401: ErrorCode.AUTH_REQUIRED,
    403: ErrorCode.FORBIDDEN_ROLE,
    404: ErrorCode.NOT_FOUND,
    409: ErrorCode.VERSION_CONFLICT,
    422: ErrorCode.VALIDATION_FAILED,
    429: ErrorCode.RATE_LIMITED,
    500: ErrorCode.INTERNAL,
    501: ErrorCode.NOT_IMPLEMENTED,
    503: ErrorCode.DEPENDENCY_UNAVAILABLE,
}


def _code_for_status(status: int) -> ErrorCode:
    mapped = _STATUS_TO_CODE.get(status)
    if mapped is not None:
        return mapped
    return ErrorCode.INTERNAL if status >= 500 else ErrorCode.MALFORMED_BODY


class ProblemDetail(Exception):
    """Raise this for any error whose `code` the caller is meant to branch on."""

    def __init__(
        self,
        status: int,
        code: ErrorCode | str,
        detail: str,
        *,
        instance: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.status = status
        self.code = ErrorCode(code) if not isinstance(code, ErrorCode) else code
        self.detail = detail
        self.instance = instance
        self.extra = extra or {}


class NotImplementedProblem(ProblemDetail):
    """501 for a route that exists in the contract but is deliberately not built yet.

    Used by the deferred SCIM surface. `raise NotImplementedError` is an unhandled
    exception and therefore a 500, which tells a caller "we are broken" when the truth is
    "we do not offer this yet" - a different thing, and one an IdP integrator must be able
    to distinguish.
    """

    def __init__(self, detail: str = "Not implemented", *, instance: str | None = None) -> None:
        super().__init__(501, ErrorCode.NOT_IMPLEMENTED, detail, instance=instance)


def problem_response(
    status: int,
    code: ErrorCode,
    detail: str | None = None,
    *,
    instance: str | None = None,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"{_TYPE_BASE}/{code.value}",
        "title": code.value.replace("_", " ").title(),
        "status": status,
        "code": code.value,
    }
    if detail:
        body["detail"] = detail
    if instance:
        body["instance"] = instance
    if extra:
        body.update(extra)
    return JSONResponse(status_code=status, media_type="application/problem+json", content=body)


def add_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ProblemDetail)
    async def problem_handler(request: Request, exc: ProblemDetail) -> JSONResponse:
        return problem_response(
            exc.status, exc.code, exc.detail, instance=exc.instance, extra=exc.extra
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        # Catches HTTPException from application code (notably auth.verify_token) AND the
        # 404/405 Starlette raises for unrouted paths. Registered on the Starlette class,
        # not fastapi.HTTPException, because the latter is a subclass and would miss those.
        detail = exc.detail if isinstance(exc.detail, str) else None
        return problem_response(exc.status_code, _code_for_status(exc.status_code), detail)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        # `errors` is additive to RFC 7807, which explicitly permits extension members.
        return problem_response(
            422,
            ErrorCode.VALIDATION_FAILED,
            "Request validation failed",
            extra={"errors": _safe_errors(exc)},
        )

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        # Log the real cause server-side; return nothing about it. An unhandled exception
        # message can carry a query fragment, a row value, or a connection string, and this
        # response crosses a tenant boundary.
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return problem_response(500, ErrorCode.INTERNAL, "Internal server error")


def _safe_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Field-level validation detail, without echoing the offending input back."""
    out: list[dict[str, Any]] = []
    for err in exc.errors():
        out.append(
            {
                "loc": [str(p) for p in err.get("loc", ())],
                "msg": str(err.get("msg", "")),
                "type": str(err.get("type", "")),
            }
        )
    return out
