"""Acceptance check 7 (Task 3 brief): `org_id` appears in no request-body model, anywhere in
`meeting-service` - proven structurally, not by review.

`org_id` on the token-mint route comes only from `RequestContext` (the Clerk-verified token
via `libs.shared.authz`); the route itself takes no request body (`meeting_id` is a path
param). This walks every Pydantic `BaseModel` subclass defined under `services/
meeting_service/` and asserts none of them declares an `org_id` field - a static, exceptionless
check, in the same spirit as `tests/test_livekit_import_boundary.py`'s import-boundary grep,
rather than a check that only happens to pass because no request model exists yet.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
_SERVICE_DIR = REPO_ROOT / "services" / "meeting_service"


def _pydantic_model_field_names() -> dict[str, set[str]]:
    """class name -> set of annotated field names, for every `class X(BaseModel...)` under
    `services/meeting_service/`. AST-based so it needs no import (and therefore no DB/env)."""
    out: dict[str, set[str]] = {}
    for path in sorted(_SERVICE_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            bases = {b.id for b in node.bases if isinstance(b, ast.Name)}
            if "BaseModel" not in bases:
                continue
            fields = {
                stmt.target.id
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
            }
            # `as_posix()`, not `str()`: `Path.__str__` uses the OS separator, so on Windows
            # the key came out `services\meeting_service\schemas.py::TokenResponse` and the
            # forward-slash assertion below failed on a model that was in fact found.
            rel = path.relative_to(REPO_ROOT).as_posix()
            out[f"{rel}::{node.name}"] = fields
    return out


def test_no_pydantic_model_in_meeting_service_declares_an_org_id_field():
    models = _pydantic_model_field_names()
    assert models, "expected at least one BaseModel in services/meeting_service (schemas.py)"

    offenders = {name: fields for name, fields in models.items() if "org_id" in fields}
    assert offenders == {}, (
        "org_id must never appear in a request/response model in meeting-service - it is "
        f"derived from the verified token only; found it declared on: {offenders}"
    )


def test_the_scan_actually_finds_the_response_model():
    """Without this, an empty or misnamed schemas module would make the check above
    vacuously pass - the same false-green shape Task 2's import-boundary test guarded
    against."""
    models = _pydantic_model_field_names()
    assert "services/meeting_service/schemas.py::TokenResponse" in models
