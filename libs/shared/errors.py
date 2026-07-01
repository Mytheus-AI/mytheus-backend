"""RFC 7807 problem+json error model.

All API errors use this shape. The `code` field is a stable machine-readable
enum — never change it once published. The `detail` field is human-readable.
Cross-tenant access returns 404 (existence is tenant-confidential), not 403.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ProblemDetail(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        detail: str,
        *,
        instance: str | None = None,
    ) -> None:
        self.status = status
        self.code = code
        self.detail = detail
        self.instance = instance


def add_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ProblemDetail)
    async def problem_handler(request: Request, exc: ProblemDetail) -> JSONResponse:
        body: dict[str, Any] = {
            "type": f"https://api.mytheus.ai/errors/{exc.code}",
            "title": exc.code.replace("_", " ").title(),
            "status": exc.status,
            "detail": exc.detail,
        }
        if exc.instance:
            body["instance"] = exc.instance
        return JSONResponse(
            status_code=exc.status,
            media_type="application/problem+json",
            content=body,
        )

    @app.exception_handler(404)
    async def not_found_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            media_type="application/problem+json",
            content={"type": "about:blank", "title": "Not Found", "status": 404},
        )
