"""R23: the LiveKit SDK is imported in exactly one file (Task 2, Deviation 2).

ADR-0003 section "Decision 2" and canon's meeting-service ownership row both say the
adapter is the only caller of the LiveKit SDK: `core-api`, `transcription-service`, and the
agent tier reach LiveKit only through `MeetingTransport`, or through `meeting-room-events`.
That is what makes the eventual self-host swap (Tech Decision 2.7) a one-interface
reimplementation instead of a grep-and-pray migration, so it is enforced here rather than
left as a code-review convention.

This test is a deliverable of Task 2 (not an optional extra): a plain source grep, no import
machinery, so it also catches a `TYPE_CHECKING`-only or commented-out import attempt, and it
runs with no services or containers up.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# The one file allowed to import the LiveKit SDK. Matches ADR-0003 verbatim: "the transport
# adapter — the ONLY place the LiveKit SDK is imported".
_ALLOWED_IMPORTER = REPO_ROOT / "services" / "meeting_service" / "transport.py"

# `import livekit`, `from livekit import ...`, `from livekit.xyz import ...` - not
# `livekit_api` or `livekit-api` (the PyPI/package name, which appears legitimately in
# pyproject.toml/uv.lock and is a different string).
_LIVEKIT_IMPORT_RE = re.compile(r"^\s*(?:import\s+livekit\b|from\s+livekit\b)", re.MULTILINE)

# Directories that are not this codebase's own source: virtualenv, git, caches, build output.
# Checked against the path RELATIVE to REPO_ROOT, not the absolute path: this worktree's own
# checkout lives under a `.worktrees/` directory (see CLAUDE.md's worktree layout), so an
# absolute-path check would exclude every file in the repo, including the ones this test
# exists to scan.
_EXCLUDED_DIR_PARTS = {".venv", ".git", "__pycache__", ".mypy_cache", ".ruff_cache", ".worktrees"}


def _python_files() -> list[Path]:
    return [
        p
        for p in REPO_ROOT.rglob("*.py")
        if not _EXCLUDED_DIR_PARTS.intersection(p.relative_to(REPO_ROOT).parts)
    ]


def test_livekit_sdk_is_imported_only_by_the_transport_adapter():
    offenders = {
        str(path.relative_to(REPO_ROOT)): match.group(0).strip()
        for path in _python_files()
        if path != _ALLOWED_IMPORTER
        for match in [_LIVEKIT_IMPORT_RE.search(path.read_text())]
        if match
    }
    assert offenders == {}, (
        "the LiveKit SDK must be imported ONLY by "
        f"{_ALLOWED_IMPORTER.relative_to(REPO_ROOT)} (ADR-0003 Decision 2 / R23); "
        f"found it imported elsewhere: {offenders}"
    )


def test_the_allowed_importer_actually_imports_livekit():
    """The other half: the rule is exceptionless, not "nobody happens to import it yet".

    Without this, deleting the `from livekit import api` line in the adapter (leaving it a
    dead stub) would make the boundary test above vacuously pass.
    """
    text = _ALLOWED_IMPORTER.read_text()
    assert _LIVEKIT_IMPORT_RE.search(text), (
        f"{_ALLOWED_IMPORTER.relative_to(REPO_ROOT)} must import the livekit SDK; "
        "an empty adapter would make the import-boundary test above meaningless"
    )
