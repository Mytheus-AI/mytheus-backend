"""Make the emitted OpenAPI document describe what a caller actually experiences.

FastAPI derives a spec from the Python signatures it can see. That spec is true as far as it
goes and materially incomplete, because three parts of this service's contract are implemented
somewhere a type annotation cannot reach:

1. **Errors.** `libs/shared/errors.add_exception_handlers` converts every `ProblemDetail`,
   every `HTTPException`, every `RequestValidationError` and every uncaught `Exception` into
   RFC 7807 `application/problem+json` carrying a `code` from the frozen `ErrorCode` enum.
   FastAPI knows none of that: it documents `422` as `application/json` with its own
   `HTTPValidationError`, and documents no other error at all. A client generated from the raw
   document cannot parse a single error this service emits.
2. **Idempotency.** `IdempotencyMiddleware` reads the `Idempotency-Key` header on mutating
   requests. It is middleware, so no route declares the header and the raw document has no
   parameter for it. A generated client would have no way to send the one header the write
   contract is built on.
3. **Authentication.** The bearer token is required by a `Depends` chain, which FastAPI records
   as a dependency rather than as a `securityScheme`. The raw document declares no security at
   all, so every route reads as anonymous.

So the emitter post-processes. Everything added here is a statement about behaviour that
already exists in this process - it documents the app, it does not decorate it. The one rule
to keep when editing: never add something to the document that the code does not do. A spec
that overstates is worse than one that understates, because the understatement is visible at
the call site and the overstatement is not.

The `ErrorCode` enum is copied out of the Python enum rather than written here. `errors.py`
carried the instruction "Keep the two in sync: a value here with no counterpart there is a
bug" for as long as the specs were hand-written. Sourcing the enum from the code deletes the
instruction instead of restating it.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from libs.shared.errors import ErrorCode
from libs.shared.public_routes import is_public_path

_PROBLEM_MEDIA_TYPE = "application/problem+json"
_PROBLEM_REF = {"$ref": "#/components/schemas/Problem"}
_MUTATING_METHODS = ("post", "put", "patch")

# Documented on every operation that can reach the handler that raises it. Deliberately short:
# an operation lists the errors a caller must branch on, not every status the stack can emit.
# 500 is on every operation because the catch-all handler is; 401 only where a token is
# required, which is what `authed-operations-document-401` in the Spectral ruleset checks.
_UNIVERSAL_ERRORS: dict[str, str] = {
    "500": "Unhandled server error. Carries a `trace_id` and no internals.",
}
_AUTHENTICATED_ERRORS: dict[str, str] = {
    "401": "No bearer token, or a token that is expired, malformed or fails JWKS verification.",
}
_SERVICE_CREDENTIAL_401 = (
    "Missing or invalid credentials for this operation's own security scheme (not a bearer token)."
)


def _problem_response(description: str) -> dict[str, Any]:
    return {"description": description, "content": {_PROBLEM_MEDIA_TYPE: {"schema": _PROBLEM_REF}}}


def _components(schema: dict[str, Any]) -> dict[str, Any]:
    components: dict[str, Any] = schema.setdefault("components", {})
    return components


def _install_security(schema: dict[str, Any]) -> str | None:
    """Promote FastAPI's own bearer scheme to a document-level default. Returns its name.

    FastAPI already emits a `securityScheme` and a per-operation `security` block wherever a
    route depends on `HTTPBearer`, because `HTTPBearer` is a `SecurityBase`. So the scheme is
    NOT invented here - inventing a second one alongside it was the first thing this function
    did wrong, and it produced a document advertising two different ways to authenticate the
    same request. What is added is the document-level default and a description FastAPI has no
    way to know.

    Document-level `security` rather than per-operation is the load-bearing choice. It makes a
    route authenticated by DEFAULT, so a new one is covered without anybody remembering, and
    opting out becomes an explicit empty `security: []` on that operation - a visible, greppable,
    reviewable act. The inverse (nothing at the top, opt IN per route) means every new route is
    anonymous until someone notices, and nobody notices.
    """
    schemes = _components(schema).get("securitySchemes") or {}
    name = next(
        (key for key, value in schemes.items() if value.get("scheme", "").lower() == "bearer"),
        None,
    )
    if name is None:
        return None

    schemes[name]["bearerFormat"] = "JWT"
    schemes[name]["description"] = (
        "Clerk-issued RS256 session token, verified against cached JWKS at `api-gateway` and "
        "re-verified in the service tier. `org_id` is derived from the token and is never "
        "accepted from a path, query, body or client header. The public API (meetings, "
        "decisions, actions, commitments, search, ask, MCP) also accepts a personal access "
        "token, `mytheus_pat_...`, created under `/api/v1/api-tokens`: it acts as its owner, "
        "`read` tokens may only read, and each token is rate limited at the gateway."
    )
    schema["security"] = [{name: []}]
    return str(name)


def _install_error_schemas(schema: dict[str, Any]) -> None:
    schemas = _components(schema).setdefault("schemas", {})
    schemas["ErrorCode"] = {
        "type": "string",
        "description": (
            "The frozen program-wide error enum. Clients branch on this and never on `detail`, "
            "which is human-readable and free to change. Values are added, never removed or "
            "repurposed. Emitted verbatim from `libs.shared.errors.ErrorCode`, so this list "
            "cannot drift from the code; the no-removal half is asserted by "
            "`test_no_published_error_code_is_ever_removed`, because `oasdiff` does not treat "
            "narrowing a response enum as a breaking change."
        ),
        "enum": [code.value for code in ErrorCode],
    }
    schemas["Problem"] = {
        "type": "object",
        "description": (
            "RFC 7807 problem detail. Every 4xx and 5xx this service emits has this shape, "
            "including the ones FastAPI would otherwise return in its own format: "
            "`libs/shared/errors.py` registers handlers for `ProblemDetail`, `HTTPException`, "
            "`RequestValidationError` and bare `Exception`."
        ),
        "required": ["type", "title", "status", "code"],
        "properties": {
            "type": {"type": "string", "format": "uri", "description": "Stable error type URI."},
            "title": {"type": "string", "description": "Short human-readable summary."},
            "status": {"type": "integer", "description": "Repeats the HTTP status code."},
            "code": {"allOf": [{"$ref": "#/components/schemas/ErrorCode"}]},
            "detail": {"type": "string", "description": "Human-readable specifics. Never parsed."},
            "instance": {"type": "string", "description": "URI of the failing request."},
            "trace_id": {"type": "string", "description": "Correlates with the trace and logs."},
        },
    }


def _rewrite_error_responses(operation: dict[str, Any]) -> None:
    """Point every documented error at `Problem`, in `problem+json`.

    FastAPI documents `422` as `application/json` carrying its own `HTTPValidationError`. That
    is not what the service returns: the `RequestValidationError` handler converts it to
    `problem+json` with `code: VALIDATION_FAILED`. Left alone, the one error FastAPI does
    document would be the one a generated client parses wrongly.
    """
    for status, response in operation.get("responses", {}).items():
        if not status[:1].isdigit() or status[0] not in "45":
            continue
        response["content"] = {_PROBLEM_MEDIA_TYPE: {"schema": _PROBLEM_REF}}


def _install_idempotency_header(path: str, method: str, operation: dict[str, Any]) -> None:
    """Document the header the middleware reads, on the methods it reads it for.

    Webhooks are excluded and that exclusion is the contract, not an oversight: a webhook
    dedupes on the provider's own event id through the inbox, because the sender is Clerk or
    LiveKit or Stripe and will not attach a header we invented.
    """
    if method not in _MUTATING_METHODS or is_public_path(path):
        return
    parameters = operation.setdefault("parameters", [])
    if any(p.get("name") == "Idempotency-Key" for p in parameters):
        return
    parameters.append(
        {
            "name": "Idempotency-Key",
            "in": "header",
            "required": False,
            "schema": {"type": "string"},
            "description": (
                "Client-generated key making this write safe to retry. A replay with the same "
                "key replays the original response; a replay with a different body is "
                "`409 IDEMPOTENCY_KEY_REUSED`. Scoped per tenant and per route."
            ),
        }
    )


def _simplify_operation_ids(app: FastAPI) -> None:
    """Name each operation after its handler function, when that name is unambiguous.

    `operationId` becomes the method name in every generated client, and FastAPI's default is
    `{name}_{mangled_path}_{method}` - `create_meeting_api_v1_meetings_post` for a function
    called `create_meeting`. The handler's own name is what a caller would have written.

    Read from `app.routes` rather than trimmed off the generated string. The first attempt did
    the latter, assuming the method suffix was the only decoration, and produced
    `post_chat_api_v1_chat`: the path is mangled into the MIDDLE, not just the end. Reading the
    route reads the value instead of guessing at the format.

    A duplicated function name leaves FastAPI's long form on every route sharing it. Two
    operations colliding on one client method is worse than one ugly method name, and picking a
    winner arbitrarily would make which one wins depend on import order.
    """
    from fastapi.routing import APIRoute

    def _walk(items: Any) -> list[APIRoute]:
        """Flatten nested routers.

        `app.include_router(...)` does not splice the child's routes into `app.routes` in this
        FastAPI version: it appends a single `_IncludedRouter` holding them. A flat scan finds
        only `/healthz` and silently leaves every real route with its default operationId,
        which is precisely what the first version of this did - it looked like the uniqueness
        check was rejecting every name, when it had simply never seen them.
        """
        collected: list[APIRoute] = []
        for item in items:
            if isinstance(item, APIRoute):
                if item.include_in_schema:
                    collected.append(item)
                continue
            # FastAPI 0.138's `_IncludedRouter` holds its child on `original_router` and has no
            # `.routes` of its own; older layouts nest under `.routes`. Try both rather than
            # pinning to one, so a FastAPI bump degrades to "operationIds stay long" instead of
            # to a crash.
            nested = getattr(item, "original_router", None) or item
            child = getattr(nested, "routes", None)
            if child:
                collected.extend(_walk(child))
        return collected

    routes = _walk(app.routes)
    counts: dict[str, int] = {}
    for route in routes:
        counts[route.name] = counts.get(route.name, 0) + 1
    for route in routes:
        if counts[route.name] == 1:
            route.operation_id = route.name


def finalize(schema: dict[str, Any]) -> dict[str, Any]:
    """Apply every post-process to a raw FastAPI document. Pure, so it is directly testable."""
    scheme = _install_security(schema)
    _install_error_schemas(schema)

    for path, methods in schema.get("paths", {}).items():
        public = is_public_path(path)
        for method, operation in methods.items():
            if method == "parameters" or not isinstance(operation, dict):
                continue

            # A public route is authenticated by something other than a bearer token, or by
            # nothing. Either way the empty list is the OpenAPI way to say "the document-level
            # requirement does not apply here", and it is visible in review.
            #
            # An authenticated operation has its per-operation block REMOVED rather than kept.
            # It repeats the document default verbatim, and the repetition is not free: a
            # reviewer reading a route with no `security` line has to know the default exists,
            # whereas a route with an explicit `security: []` is unmissable. Dropping the
            # redundant copy is what makes the exception visible.
            if public:
                operation["security"] = []
            elif scheme and operation.get("security") == [{scheme: []}]:
                operation.pop("security")

            _rewrite_error_responses(operation)
            _install_idempotency_header(path, method, operation)

            responses = operation.setdefault("responses", {})
            documented = dict(_UNIVERSAL_ERRORS)
            if not public:
                documented.update(_AUTHENTICATED_ERRORS)
                # An operation that still carries its own `security` block here declares a
                # non-bearer scheme (e.g. the pii-vault's internal service secret), so the
                # bearer-specific 401 text would overstate.
                if operation.get("security"):
                    documented["401"] = _SERVICE_CREDENTIAL_401
            for status, description in documented.items():
                responses.setdefault(status, _problem_response(description))

    return schema


def install(app: FastAPI, service_name: str) -> None:
    """Replace `app.openapi` so `/openapi.json` and the emitter produce the same document.

    One code path, deliberately. If the emitter post-processed and the live `/openapi.json` did
    not, the committed spec and the running service would describe different APIs while both
    looked correct, which is the drift this whole exercise removes.
    """

    def _openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        _simplify_operation_ids(app)
        app.openapi_schema = finalize(
            get_openapi(
                title=service_name,
                version=app.version,
                description=app.description or None,
                routes=app.routes,
            )
        )
        return app.openapi_schema

    app.openapi = _openapi  # type: ignore[method-assign]


__all__: list[str] = ["finalize", "install"]
