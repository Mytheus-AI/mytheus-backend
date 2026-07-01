"""Shift-left authz gate: every FastAPI route must declare auth or be explicitly public.

Canonical reference copy (lives in mytheus-platform); the runnable copy ships in each Python code
repo so its CI can `uv run python scripts/check_authz.py`.

A route PASSES if any of:
  * its router/app was created with a `dependencies=[Depends(...)/Security(...)]` guard, OR
  * the route decorator itself carries `dependencies=[Depends(...)/Security(...)]`, OR
  * a parameter default is `Depends(...)` / `Security(...)` (an auth/authorization dependency), OR
  * its path matches the PUBLIC allowlist (health probes, signature-verified webhooks, oauth
    callbacks, openapi/docs), OR
  * the route carries a `# authz: public` comment marker on/above the decorator.

Otherwise the PR fails. This is a presence check, not a proof: it exists so a new endpoint cannot
silently ship with no declared auth (11-security-architecture.md §3.2). Exit 1 on any violation.
"""

from __future__ import annotations

import ast
import pathlib
import sys

PUBLIC_PREFIXES = ("/healthz", "/health", "/openapi", "/docs", "/redoc")
PUBLIC_CONTAINS = ("/webhook", "/oauth", "/clerk/", "/stripe/", "/livekit/", "/callback")
ROUTE_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "websocket"}
AUTH_CALLABLES = {"Depends", "Security"}
ROUTER_FACTORIES = {"APIRouter", "FastAPI"}


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)


def _deps_list_has_auth(node: ast.expr | None) -> bool:
    """True if a `dependencies=[...]` value contains a Depends()/Security() call."""
    if not isinstance(node, ast.List):
        return False
    return any(isinstance(el, ast.Call) and _call_name(el) in AUTH_CALLABLES for el in node.elts)


def _kwarg(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def guarded_routers(tree: ast.Module) -> set[str]:
    """Names bound to an APIRouter/FastAPI(...) created with an auth `dependencies=` guard."""
    guarded: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        value = node.value
        if (
            isinstance(value, ast.Call)
            and _call_name(value) in ROUTER_FACTORIES
            and _deps_list_has_auth(_kwarg(value, "dependencies"))
        ):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    guarded.add(target.id)
    return guarded


def route_decorator(dec: ast.expr) -> tuple[str, str] | None:
    """Return (router_var, path) for a route decorator (path "" if dynamic); None if not a route."""
    if (
        isinstance(dec, ast.Call)
        and isinstance(dec.func, ast.Attribute)
        and dec.func.attr in ROUTE_METHODS
        and isinstance(dec.func.value, ast.Name)
    ):
        router_var = dec.func.value.id
        arg0 = dec.args[0] if dec.args else None
        if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
            return router_var, arg0.value
        return router_var, ""
    return None


def has_auth_dependency(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    defaults = list(fn.args.defaults) + [d for d in fn.args.kw_defaults if d is not None]
    return any(isinstance(d, ast.Call) and _call_name(d) in AUTH_CALLABLES for d in defaults)


def is_public_path(path: str) -> bool:
    prefixed = any(path.startswith(p) for p in PUBLIC_PREFIXES)
    return prefixed or any(c in path for c in PUBLIC_CONTAINS)


def has_public_marker(fn: ast.FunctionDef | ast.AsyncFunctionDef, lines: list[str]) -> bool:
    start = min([d.lineno for d in fn.decorator_list] + [fn.lineno])
    window = range(start - 1, fn.lineno)
    return any("authz: public" in lines[i] for i in window if 0 <= i < len(lines))


def main() -> int:
    root = pathlib.Path("services")
    if not root.exists():
        print("no services/ directory; nothing to check")
        return 0

    violations: list[str] = []
    for py in sorted(root.rglob("*.py")):
        src = py.read_text(encoding="utf-8")
        lines = src.splitlines()
        tree = ast.parse(src, filename=str(py))
        guarded = guarded_routers(tree)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for dec in node.decorator_list:
                parsed = route_decorator(dec)
                if parsed is None:
                    continue
                router_var, path = parsed
                dec_deps = isinstance(dec, ast.Call) and _deps_list_has_auth(
                    _kwarg(dec, "dependencies")
                )
                if (
                    router_var in guarded
                    or dec_deps
                    or has_auth_dependency(node)
                    or is_public_path(path)
                    or has_public_marker(node, lines)
                ):
                    continue
                violations.append(
                    f"{py}:{node.lineno} route {path or '<dynamic>'} ({node.name}) "
                    "has no declared auth and is not public"
                )

    if violations:
        print("AUTHZ CHECK FAILED - routes missing a declared auth dependency:")
        for v in violations:
            print("  -", v)
        print(
            "Fix: add Depends(verify_token)/Security(...) (route or router level), or mark the "
            "route public (allowlist path or a `# authz: public` comment)."
        )
        return 1

    print("authz check passed: every FastAPI route declares auth or is explicitly public")
    return 0


if __name__ == "__main__":
    sys.exit(main())
