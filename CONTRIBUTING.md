# Contributing to mytheus-backend

## Naming comes from canon — never invent
Service, table, topic, store, bucket, Redis-key, and AGE-label names are frozen in
`mytheus-docs/_canon/` (`01-service-catalog.md`, `02-data-model.md`, `03-event-taxonomy.md`) and
published in `24-glossary-and-naming.md`. Use them verbatim. A rename goes **Glossary + ADR first**
in `mytheus-docs`, then propagates here — never the reverse.

**Canon location (recorded decision):** canon authority lives in `mytheus-docs`. For MVP the
contract-binding repos consume it as a **sibling checkout** — clone all Mytheus repos under one
parent directory. Revisit git-submodule / vendoring post-W36.

## Every PR includes an `org_id` authz review
Tenant isolation is fail-closed at every layer — `org_id` on every hop (JWT claim, REST header,
gRPC metadata, `SET app.current_org`, `nexus:{org_id}:` Redis prefix). Every PR review explicitly
verifies that new query / producer / consumer paths carry `org_id` and set `app.current_org`
transaction-locally, and that new FastAPI routes declare auth (the `scripts/check_authz.py` gate).
Shift-left security from day one (`11-security-architecture.md` §3.2,
`16-deployment-and-infrastructure.md` §5.2).

## Standards
- **Conventional Commits** (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`).
- **No `Co-Authored-By` footer** in commits (per this repo's CLAUDE.md).
- **Trunk-based**: short-lived branches, frequent PRs, mandatory review.
- CI green before merge: `make ci` (ruff + black + mypy + pytest) plus the authz check and a
  Docker image build. Install hooks once: `uv run pre-commit install`.
- A breaking REST change needs `/api/v2` + a deprecation window; a breaking topic or table change
  needs an ADR (and a canon edit) first. Alembic migrations are expand → deploy → contract.
