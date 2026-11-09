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


# --- Response models must carry a real item type -----------------------------------------
#
# These exist because the emitted document is what a generated client is built from, and a
# response model typed `list[dict[str, Any]]` emits as an unconstrained object map: the client
# gets `Record<string, unknown>[]`, which is WEAKER than the hand-written types the generated
# client exists to replace. Nothing else in the gate stack can see that. `--check` only asks
# whether the spec matches the code, `spectral` has no rule about item types, and `oasdiff`
# calls loosening a response schema non-breaking. So the loosening is invisible to all three
# and only shows up as a worse frontend, months later.


def _bff_dashboard_schemas() -> dict:
    from services.bff.main import app

    return app.openapi()["components"]["schemas"]


def test_the_meetings_arm_declares_its_item_type() -> None:
    """`MeetingsArm.items` refs a named schema, never a free-form object.

    Guard against the shape this file's header describes: `items` typed as a dict map. The
    assertion is on the property (an item type exists and is a `$ref`), not on the spelling of
    one schema name, so renaming `DashboardMeeting` does not silently disarm it.
    """
    items = _bff_dashboard_schemas()["MeetingsArm"]["properties"]["items"]
    assert items["type"] == "array"
    assert "$ref" in items["items"], (
        "MeetingsArm.items has no item type. A client generated from this document would "
        f"receive an untyped array: {items['items']!r}"
    )


def test_the_meetings_arm_item_type_is_fully_specified() -> None:
    """The referenced schema has real fields, not `additionalProperties`.

    A `$ref` alone is not enough: a `$ref` to a schema that is itself an open object map is
    the same defect one level down.
    """
    schemas = _bff_dashboard_schemas()
    ref = schemas["MeetingsArm"]["properties"]["items"]["items"]["$ref"]
    target = schemas[ref.rsplit("/", 1)[-1]]

    assert target.get("additionalProperties") is not True, f"{ref} is an open object map"
    assert set(target["properties"]) == {
        "id",
        "title",
        "status",
        "scheduled_start",
        "scheduled_end",
        "livekit_room",
    }
    # The status union, not a bare string. A client switches on these four values.
    assert target["properties"]["status"]["enum"] == ["scheduled", "live", "ended", "cancelled"]


def test_every_arm_marks_items_required() -> None:
    """The server always sends `items`, so the schema has to say so.

    With a Pydantic default the field is optional in the schema and a generated client types
    it `items?:`, forcing every caller to handle an absence the server never produces. That is
    a contract that understates itself, and callers write dead null-handling against it.
    """
    schemas = _bff_dashboard_schemas()
    for arm in ("MeetingsArm", "UnbuiltArm"):
        assert "items" in schemas[arm]["required"], arm
        assert "status" in schemas[arm]["required"], arm


def test_the_unbuilt_arms_do_not_invent_an_item_type() -> None:
    """The other direction, and it matters as much as the first.

    `action_items` and `decisions` have no read model until Plan 8. Emitting a fabricated
    record type for them would ship an invention to every generated client as though it were
    the contract, so their item type stays the open object map it is today. `worklogs` has a
    real read model since Plan 7 (the caller's surfaced notes), so it is typed.
    """
    schemas = _bff_dashboard_schemas()
    unbuilt_items = schemas["UnbuiltArm"]["properties"]["items"]["items"]
    assert (
        "$ref" not in unbuilt_items
    ), f"an item type was invented for the unbuilt arms: {unbuilt_items!r}"

    dashboard = schemas["Dashboard"]["properties"]
    for arm in ("action_items", "decisions"):
        assert dashboard[arm]["$ref"].endswith("/UnbuiltArm"), arm
    assert dashboard["upcoming_meetings"]["$ref"].endswith("/MeetingsArm")
    assert dashboard["worklogs"]["$ref"].endswith("/WorklogsArm")
    assert schemas["WorklogsArm"]["properties"]["items"]["items"]["$ref"].endswith("/DashboardNote")


def test_the_dashboard_and_core_api_serialize_the_same_column_identically() -> None:
    """`meetings.scheduled_start` must look the same whichever endpoint returned it.

    Found while typing the dashboard arm: `bff` hand-built its rows and called `.isoformat()`,
    emitting `+00:00`, while `core-api` returned a Pydantic model and emitted `Z`, for the same
    column of the same table. Both are valid RFC 3339 and both parse, so nothing was broken -
    which is why it survived: a client that normalises on one spelling is simply wrong against
    the other endpoint, and only on UTC values.

    Asserted as agreement between the two models rather than against a literal, so the pair
    stays consistent even if the serializer's spelling changes under both at once.
    """
    import datetime
    import json
    import uuid

    from services.bff.dashboard import DashboardMeeting
    from services.core_api.schemas import Meeting

    when = datetime.datetime(2026, 8, 31, 10, 0, 0, 123456, tzinfo=datetime.UTC)
    meeting_id = uuid.uuid4()

    arm_row = json.loads(
        DashboardMeeting(
            id=meeting_id,
            title="Standup",
            status="scheduled",
            scheduled_start=when,
            scheduled_end=None,
            livekit_room=None,
        ).model_dump_json()
    )
    resource = json.loads(
        Meeting(
            id=meeting_id,
            title="Standup",
            status="scheduled",
            scheduled_start=when,
            scheduled_end=None,
            livekit_room=None,
            created_by=None,
            created_at=when,
            updated_at=when,
        ).model_dump_json()
    )

    for field in ("id", "title", "status", "scheduled_start", "scheduled_end", "livekit_room"):
        assert arm_row[field] == resource[field], (
            f"{field} differs between /bff/dashboard and /api/v1/meetings for the same row: "
            f"{arm_row[field]!r} vs {resource[field]!r}"
        )
