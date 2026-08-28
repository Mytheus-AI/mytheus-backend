"""Emit an OpenAPI 3.1 document per HTTP service, from the running app.

    python -m scripts.emit_openapi           # write design/openapi/<service>.yaml
    python -m scripts.emit_openapi --check   # exit 1 if any committed file is stale

WHY THIS EXISTS

`mytheus-docs/design/openapi/` holds five specs hand-written during Plan 1-2's design phase,
before a single route existed. They were accurate the day they were written and have been
drifting since: `bff.yaml` still describes `Dashboard` as flat arrays plus a separate `arms`
object, while `dashboard.py` has returned an inline `{status, items}` arm per field for
months. Plan 1-2 refused to generate a TypeScript client from them for exactly that reason -
generating from a drifted spec does not prevent drift, it ships it to every caller.

A spec emitted from the app cannot drift, because there is nothing for it to drift from. The
`--check` mode is what turns that from a nice property into a guarantee: CI regenerates and
fails if the committed file differs, so changing a route without regenerating is a red build
rather than a spec that quietly stops being true.

WHAT IS EMITTED, AND WHAT IS NOT

Auto-discovered rather than listed: every `services/*/main.py` exposing an `app` with at least
one route of its own. A list would have to be maintained, and the evidence that it would not be
is already here - the hand-written set covers `admin-ops-service`, which has no routes at all,
and omits `meeting-service`, which has two. Discovery cannot make that mistake.

`api-gateway` is skipped along with the route-less services, and that is the right answer even
though it is the actual public surface. It proxies: its routes live in `edge.ROUTE_MAP`, not in
its own decorators, so FastAPI sees only `/healthz`. Emitting that produced a document with
zero paths and no security scheme, which failed the ruleset for a completely accurate reason -
there is no contract there to describe. The gateway's real contract is the union of the
documents below, which nothing here can compute; see `design/openapi/README.md`.
"""

from __future__ import annotations

import argparse
import importlib
import pathlib
import sys
from typing import Any

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SERVICES_DIR = REPO_ROOT / "services"
OUTPUT_DIR = REPO_ROOT / "design" / "openapi"

# Mounted on every app by `create_app`, so their presence does not make a service an HTTP
# surface worth publishing a contract for.
_BUILTIN_PATHS = {"/healthz", "/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}

_HEADER = (
    "# GENERATED FILE - DO NOT EDIT.\n"
    "#\n"
    "# Emitted from the FastAPI app by `python -m scripts.emit_openapi` (`make openapi`).\n"
    "# Edit the routes and Pydantic models, then regenerate. CI runs `--check` and fails the\n"
    "# build if this file and the code disagree, so a hand edit here is reverted by the next\n"
    "# regeneration and blocks the build until then.\n"
)


def _discover() -> list[Any]:
    """Import every `services/<pkg>/main.py` that exposes an `app`, in a stable order."""
    found: list[Any] = []
    for main_py in sorted(SERVICES_DIR.glob("*/main.py")):
        package = main_py.parent.name
        module = importlib.import_module(f"services.{package}.main")
        app = getattr(module, "app", None)
        if app is None:
            continue
        found.append(app)
    return found


def _own_paths(schema: dict[str, Any]) -> set[str]:
    return set(schema.get("paths", {})) - _BUILTIN_PATHS


def _render(app: Any) -> str:
    schema = app.openapi()
    # sort_keys so the output is byte-stable across runs and Python versions: `--check`
    # compares bytes, and a dict-ordering difference would read as a drift that is not one.
    body = yaml.safe_dump(schema, sort_keys=True, allow_unicode=True, width=100)
    return _HEADER + body


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write. Exit 1 if any committed spec differs from what the code produces.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stale: list[str] = []
    written: list[str] = []
    skipped: list[str] = []

    for app in _discover():
        service = app.title
        schema = app.openapi()
        if not _own_paths(schema):
            skipped.append(service)
            continue

        target = OUTPUT_DIR / f"{service}.yaml"
        rendered = _render(app)

        if args.check:
            current = target.read_text(encoding="utf-8") if target.exists() else ""
            if current != rendered:
                stale.append(
                    f"{target.relative_to(REPO_ROOT)} ({'missing' if not current else 'stale'})"
                )
            continue

        target.write_text(rendered, encoding="utf-8", newline="\n")
        written.append(f"{target.relative_to(REPO_ROOT)}  ({len(_own_paths(schema))} paths)")

    if skipped:
        print(f"skipped (no routes of their own): {', '.join(sorted(skipped))}")

    if args.check:
        if stale:
            print("OpenAPI specs are out of date with the code:", file=sys.stderr)
            for entry in stale:
                print(f"  - {entry}", file=sys.stderr)
            print("\nRun `make openapi` and commit the result.", file=sys.stderr)
            return 1
        print("OpenAPI specs match the code.")
        return 0

    for entry in written:
        print(f"wrote {entry}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
