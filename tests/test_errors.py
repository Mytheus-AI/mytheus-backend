"""RFC 7807 problem+json is universal, and every body carries the frozen `code` enum.

The frozen contract is design/openapi/api-gateway.yaml in mytheus-docs: Problem requires
`type`, `title`, `status` and `code`, and `code` comes from ErrorCode and nothing else.

Plan 0 satisfied none of that. errors.py registered handlers for ProblemDetail and 404
only, so every other HTTPException fell through to FastAPI's default `{"detail": ...}`
shape with media type application/json; no handler emitted a `code` field at all; and there
was no enum in code for anything to be checked against.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.testclient import TestClient

from libs.shared.errors import ErrorCode, ProblemDetail, add_exception_handlers


class _Body(BaseModel):
    n: int


@pytest.fixture()
def client() -> TestClient:
    app = FastAPI()
    add_exception_handlers(app)

    @app.get("/problem")
    async def problem() -> None:
        raise ProblemDetail(409, ErrorCode.IDEMPOTENCY_KEY_REUSED, "same key, different body")

    @app.get("/http-exception")
    async def http_exception() -> None:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("a secret internal detail that must not reach the client")

    @app.post("/validated")
    async def validated(body: _Body) -> dict[str, int]:
        return {"n": body.n}

    return TestClient(app, raise_server_exceptions=False)


def _assert_problem_shape(resp, status: int, code: str) -> dict:
    assert resp.status_code == status
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    for field in ("type", "title", "status", "code"):
        assert field in body, f"Problem is missing required field {field!r}"
    assert body["status"] == status
    assert body["code"] == code
    assert body["code"] in {c.value for c in ErrorCode}, "code must come from the frozen enum"
    return body


def test_problem_detail_carries_the_code_field(client):
    _assert_problem_shape(client.get("/problem"), 409, "IDEMPOTENCY_KEY_REUSED")


def test_plain_http_exception_is_also_problem_json(client):
    """The gap that mattered: verify_token raises bare HTTPExceptions on every auth path."""
    body = _assert_problem_shape(client.get("/http-exception"), 401, "AUTH_REQUIRED")
    assert body["detail"] == "Missing bearer token"


def test_unhandled_exception_is_problem_json_and_leaks_nothing(client):
    body = _assert_problem_shape(client.get("/boom"), 500, "INTERNAL")
    assert "secret internal detail" not in str(body)


def test_validation_error_is_problem_json(client):
    body = _assert_problem_shape(
        client.post("/validated", json={"n": "not-an-int"}), 422, "VALIDATION_FAILED"
    )
    assert "errors" in body, "validation problems should say which fields failed"


def test_404_is_problem_json(client):
    _assert_problem_shape(client.get("/no-such-route"), 404, "NOT_FOUND")


def test_scim_returns_501_not_500():
    """Resolved decision: SCIM is deferred, so /scim/v2/* returns 501 with a 7807 body.

    Plan 0 raised NotImplementedError, which is an unhandled exception and therefore a 500:
    it reads as "we are broken", not "we do not offer this yet".
    """
    from services.identity_tenant_service.routes import scim

    app = FastAPI()
    add_exception_handlers(app)
    # The router carries a real auth dependency; override it, this test is about the status.
    app.dependency_overrides[scim.verify_token] = lambda: {"sub": "u"}
    app.include_router(scim.router, prefix="/scim/v2")
    client = TestClient(app, raise_server_exceptions=False)

    for method, path in (
        ("get", "/scim/v2/Users"),
        ("post", "/scim/v2/Users"),
        ("put", "/scim/v2/Users/u1"),
        ("delete", "/scim/v2/Users/u1"),
        ("get", "/scim/v2/Groups"),
    ):
        resp = getattr(client, method)(path)
        _assert_problem_shape(resp, 501, "NOT_IMPLEMENTED")
