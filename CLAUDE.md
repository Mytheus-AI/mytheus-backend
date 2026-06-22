# CLAUDE.md

> Working guide for AI coding agents in `reprise-backend`, the FastAPI CRUD/control-plane and async-orchestration tier of Reprise AI. Read this before editing.

For the full design, see `system.md`. For frozen names, see the canon in `reprise-docs` (`_canon/01-service-catalog.md`, `_canon/02-data-model.md`, `_canon/03-event-taxonomy.md`).

## 🎯 Purpose

`reprise-backend` is the **CRUD control plane + async orchestration** tier of **Reprise AI** (internal architecture codename: NEXUS), an intelligent meeting-orchestration multi-tenant SaaS.

It owns the relational heart, identity/tenancy, billing/metering, integrations, meeting-room control, notifications, and Temporal workflows. It is **not** the AI/inference tier (`reprise-intelligence`) or the real-time-streaming tier (`reprise-realtime`). It scales on HTTP req/s (CRUD, p95 < 300 ms) and Temporal queue depth.

## 🧱 Stack & Layout

**Stack & versions**

- Python 3.11+, FastAPI, Pydantic v2
- SQLAlchemy + Alembic; Supabase Postgres (RLS, `org_id`)
- Redis / Upstash (namespaced `nexus:{org_id}:…`)
- Redpanda via `aiokafka`; Avro + Schema Registry (BACKWARD)
- Temporal (Temporal Python SDK), Stripe SDK, Clerk, gRPC (internal mesh)
- Tooling: `uv`/Poetry, `ruff`, `black`, `mypy`, `pytest` + `testcontainers`

**Repo layout**

Monorepo: `services/<name>/`, shared code in `libs/shared/`, gRPC in `proto/`, Alembic in `migrations/`.

**Service modules in this repo**

- `api-gateway`
- `bff`
- `identity-tenant-service`
- `core-api`
- `meeting-service`
- `integration-service`
- `billing-metering-service`
- `admin-ops-service`
- `notification-service`
- `workflow-service`

Each service is a bounded context with one owner. Cross-context access is REST/gRPC (read) or events (async projection), **never** a cross-service table write. The repo runs as a modular monolith early (Railway/Render) and splits into these services on Kubernetes later: the boundaries are the extraction seams, so do not blur them.

## 📐 Conventions

- Add the failing test first (TDD); use `testcontainers` for anything touching Postgres/Redis/Redpanda.
- Type everything; keep wire models in Pydantic v2.
- Use RFC 7807 `application/problem+json` errors with a stable `code` enum.
- Accept `Idempotency-Key` on non-idempotent mutating POSTs.
- Return the canonical persisted resource from writes (never a partial echo).
- Verify provider webhook signatures, dedupe on the provider `event_id`, return `2xx` fast, and do the work on the event plane.
- Log structured JSON with `org_id` + `trace_id`.
- Set `app.current_org` transaction-locally before any tenant-scoped query.

## 🚦 Cross-Cutting Rules (non-negotiable)

These are not style preferences; they are invariants. Violating one is a correctness bug.

| Rule | What it means |
| --- | --- |
| **`org_id` on every hop** | Derived from the Clerk JWT at `api-gateway`; propagated as a REST header and gRPC metadata; set on every Postgres session via `SET app.current_org` (transaction-local: `set_config('app.current_org', :org_id, true)`); and prefixed onto every Redis key as `nexus:{org_id}:…`. `org_id` is never a path or query parameter. Cross-tenant access returns `404`, never `403`. |
| **One writer per store** | Each store has exactly one owning service (see `system.md` §4 / `_canon/02-data-model.md`). A new write path requires a data-model change first: edit canon, then the Alembic migration, then code. Never write another service's tables. |
| **At-least-once + idempotency (outbox / inbox / DLQ)** | Produce through a transactional **outbox** (same DB transaction as the state change); consume through an **inbox** + Redis dedupe (`nexus:{org_id}:dedupe:{topic}:{idempotency_key}`, 24h TTL); poison messages go to a **per-topic DLQ**. Consume handlers must be idempotent. |
| **No silent commits** | `decisions`/`actions` are emitted only on user confirmation via `core-api`'s outbox. Agents surface candidates via `agent-results`; they never write Postgres and never produce `decisions`/`actions`. No silent graph writes. |
| **Deny-by-default agent authz** | Agent-to-agent authorization and any cross-tenant request fails closed and is audited. |
| **Stateless services + durable stores only** | No session affinity, no in-process authoritative cache. State lives in Postgres, Redis, object storage, Temporal, or the JWT. |

## 📛 Naming Authority

Service names, table names, and topic names are **frozen** and come from the canon in `reprise-docs`:

- `_canon/01-service-catalog.md` - service names + sync-API ownership
- `_canon/02-data-model.md` - table/store names + tenant keys
- `_canon/03-event-taxonomy.md` - topic names + producer/consumer + schema

**Never invent or rename a service, table, or topic silently.** A rename goes through the **Glossary** plus an **ADR** first, then propagates from canon outward.

Keep these identifiers verbatim, always: `nexus_knowledge`, `transcript_chunks`, the `nexus:{org_id}:` Redis prefix, `app.current_org`, `get_user_context`. The product name in prose is **Reprise AI**.

## ✅ Do / ❌ Don't

**✅ Do**

- Add the failing test first (TDD); use `testcontainers` for anything touching Postgres/Redis/Redpanda.
- Set `app.current_org` transaction-locally before any tenant-scoped query.
- Return the canonical persisted resource from writes (never a partial echo).
- Use RFC 7807 `application/problem+json` errors with a stable `code` enum.
- Accept `Idempotency-Key` on non-idempotent mutating POSTs.
- Verify provider webhook signatures, dedupe on the provider `event_id`, return `2xx` fast, and do the work on the event plane.
- Type everything; keep wire models in Pydantic v2; log structured JSON with `org_id` + `trace_id`.

**❌ Don't**

- Don't write another service's tables, or add a write path without a canon/data-model change.
- Don't put `org_id` in a path/query, or trust the gateway's JWT pre-validation as the only check (services re-verify).
- Don't emit `decisions`/`actions` without confirmation; don't let an agent write Postgres.
- Don't set `app.current_org` non-transaction-locally (it leaks across pooled connections).
- Don't return or log OAuth tokens or secrets.
- Don't introduce a new service/table/topic name without an ADR + Glossary entry.
- Don't block a synchronous REST call on an LLM or cross-service fan-out: commit an intent, return a pollable/subscribable resource.

## 🛠️ Commands

```bash
uv sync                                     # install (or: poetry install)
docker compose up -d                        # local Postgres + Redis + Redpanda + Temporal
uv run alembic upgrade head                 # apply migrations
uv run uvicorn services.core_api.main:app --reload --port 8000
uv run python -m services.workflow_service.worker   # Temporal workers

uv run pytest                               # tests (testcontainers-backed)
uv run ruff check . && uv run black --check . && uv run mypy .

uv run alembic revision --autogenerate -m "feat: add <table>"   # new migration (after canon edit)
```

## 🔀 Pull Requests

- **Conventional Commits** (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`).
- **Commits must NOT include a `Co-Authored-By` footer.**
- Trunk-based, short-lived branches, mandatory PR review.
- CI must pass: ruff + black + mypy + pytest + Spectral lint + `oasdiff` breaking-change gate.
- Every endpoint declares its auth requirement in the OpenAPI spec (no declared auth = failed review).
- A breaking REST change needs a major-version bump (`/api/v2`); breaking topic/table changes need an ADR.
