# CLAUDE.md

`reprise-backend` is the FastAPI CRUD/control-plane plus async-orchestration tier of Reprise AI (internal architecture codename: NEXUS). It is a modular monolith today that splits into the ten services below on Kubernetes later; the single rule that makes that split safe is: every store has exactly one writer, and `org_id` rides every hop (REST header, gRPC metadata, `SET app.current_org`, and the `nexus:{org_id}:` Redis prefix).

For the deep design, read `system.md`. For frozen names, read the canon in `reprise-docs`: `_canon/01-service-catalog.md` (services + API ownership), `_canon/02-data-model.md` (tables + tenant keys), `_canon/03-event-taxonomy.md` (topics + producers/consumers + schema). Where this file disagrees with canon, canon wins.

## Repo at a glance

This repo is the synchronous CRUD/control plane plus the async/orchestration tier. It does NOT contain the AI/inference tier (`reprise-intelligence`: LangGraph agents, embeddings, RAG, knowledge graph) or the real-time WebSocket tier (`reprise-realtime`: realtime-gateway, transcription-service). Those scale on different signals and bill against different cost centers, which is why the system is cut here.

Ten bounded contexts live here, by scaling family:

- Edge (CRUD family): `api-gateway`, `bff`.
- CRUD / control plane: `identity-tenant-service`, `core-api`, `meeting-service`, `integration-service`, `billing-metering-service`, `admin-ops-service`.
- Async / orchestration: `notification-service` (event-driven worker), `workflow-service` (Temporal workers).

Early stage: one deployable on Railway/Render, every service a Python module. Production: each module peels off into its own service on Kubernetes (EKS/GKE) with per-service HPA/KEDA autoscalers. The bounded-context boundaries below are the extraction seams. Do not blur them: cross-context access is REST/gRPC (read) or events (async), never a cross-service table write.

## Architecture orientation

Edge path. All external traffic enters Cloudflare (CDN, WAF, global L7 rate limit), then the single ingress `api-gateway` (TLS, per-tenant rate limit, Clerk JWT pre-validation against cached JWKS, `org_id` extraction from the `org` claim, routing). The gateway FAILS CLOSED: if `org_id` cannot be resolved, no request reaches a tenant-scoped store. From there `bff` aggregates screen-shaped reads (the only caller of `/bff/*`), or the gateway routes straight to a service. The WebSocket leg bypasses `bff` and rides the gateway directly to `reprise-realtime`. Every internal hop propagates `org_id` as a REST header and gRPC metadata; the service tier RE-VERIFIES the JWT (the gateway is a DDoS shield, not a trust delegation).

Sync surface. REST is the external, resource-oriented contract (`/api/v1/...`, OpenAPI 3.1, spec-first). gRPC is internal/mesh-only for tight-latency calls, the canonical one being `identity-tenant-service.GetUserContext(user_id)` which carries `org_id` in metadata and is read heavily by the agent tier. `org_id` is never a path or query parameter; cross-tenant access returns `404`, never `403` (existence is tenant-confidential).

Async backbone. Redpanda (Kafka API), Avro + Schema Registry, one subject per topic (`{topic}-value`), BACKWARD compatibility, every topic partitioned by `org_id`. Delivery is at-least-once with idempotency: transactional outbox on produce, inbox + Redis dedupe on consume, per-topic DLQ for poison messages. Consumers re-assert `org_id` from the event header before any write so RLS applies identically on the async path.

Confirmation rule. `core-api` is the sole producer of confirmed `decisions`/`actions`. Agents (in `reprise-intelligence`) only detect candidates and surface them via `agent-results`; they never write Postgres and never produce `decisions`/`actions`. A user confirms via `core-api` (`POST /decisions/:id/confirm`, `POST /actions/:id/confirm`); only then does `core-api`'s outbox emit the event that `knowledge-graph-service` consumes to create the graph edge. There are no silent graph writes.

## Service map

| Service | Responsibility | Owns (tables / stores) | Produces | Consumes | API prefix |
|---|---|---|---|---|---|
| `api-gateway` | Single public ingress: TLS, WAF, rate limit, JWT pre-validate, `org_id` extract, route. No business logic. | none (stateless, config-only) | none | none | front of every prefix; owns none |
| `bff` | Backend-for-frontend: screen-shaped read aggregation, fan-out, short read cache. No domain ownership. | none (short `nexus:{org_id}:…` Redis read cache) | none | none | `/bff/*` |
| `identity-tenant-service` | Identity and tenancy: org/tenant provisioning, RBAC, SSO/SAML/SCIM, Clerk user-sync, agent-authz grants, user-context source of record. | `organizations` (id = `org_id`), `users`, `tenant_settings`, `data_residency`, RBAC/SSO/SCIM/agent-authz state | `tenant-events`, `notifications` | `user-sync-events` (Clerk inbox) | REST `/orgs`, `/rbac`, `/scim/v2`, `/context`, `/clerk/webhook`; gRPC `GetUserContext` |
| `core-api` | Relational heart and domain CRUD; outbox for confirmed `decisions`/`actions`. | `meetings`, `meeting_participants`, `meeting_speakers`, `worklogs`, `decisions`, `action_items`, `agent_suggestions`, `agent_collaborations`, `chat_messages`, `teams`, `team_members`, `departments` | `meeting-events`; `decisions`/`actions` (on confirm only) | `agent-results`, `decisions`, `actions` | `/meetings`, `/teams`, `/departments`, `/worklogs`, `/decisions`, `/actions`, `/suggestions`, `/chat` |
| `meeting-service` | LiveKit transport adapter: room token mint, webhooks, server-side audio hook to transcription. | `meeting_rooms`; `nexus-recordings` bucket | `meeting-room-events` | `meeting-events` | `/api/meetings/:id/token`, `/livekit/webhook` |
| `integration-service` | External integrations: Google Calendar, Slack, Jira (default)/Linear (alt). Guardrail R8. | `integration_connections`, `oauth_tokens` (envelope-encrypted) | `integration-events`, `actions` (status sync) | `actions`, `meeting-events`, `notifications` | `/oauth/{provider}`, `/webhooks/{provider}` |
| `billing-metering-service` | SaaS monetization: Stripe billing/seats/dunning/tax, usage metering, quota enforcement (R4). | `plans` (global, no `org_id`), `subscriptions`, `usage_events`, `usage_aggregates`, `quotas`, `invoices`, `agent_budgets`; `nexus-invoices` bucket | `billing-events`, `notifications` | `usage-events`, `tenant-events` | `/billing/*`, `/usage`, `/plans`, `/stripe/webhook` |
| `admin-ops-service` | Tenant-admin and internal-ops consoles; compliance audit trail. | feature flags, ops audit log, `audit_logs` (append-only), impersonation grants; `nexus-audit-archive` | `admin-events` | `tenant-events`, `billing-events`, `graph-events` | `/admin/tenant/*`, `/admin/ops/*` |
| `notification-service` | Unified delivery (in-app, email, Slack). Sole authorized external-channel fan-out for `notifications`. | `notification_log`, delivery receipts, channel prefs; Redis priority queue `nexus:{org_id}:queue:agent:{user_id}` | `notification-delivery-events` | `notifications`, `agent-results` | `/notifications` |
| `workflow-service` | Temporal durable orchestration: meeting lifecycle, action follow-up, RTBF saga, dunning saga. | none durable (Temporal owns workflow state); may write `nexus-exports`/`nexus-audit-archive` | `notifications` | `billing-events`, `tenant-events`, `meeting-events`, `actions`, `admin-events` | internal `/workflows/*` (not public) |

Note: `teams`/`team_members`/`departments` CRUD is assigned to `core-api`; identity holds the identity-and-tenancy surface. `transcript_chunks` is owned by `transcription-service` in `reprise-realtime` and is read-only here (served via FTS).

## Where things live

```
reprise-backend/
├── services/
│   ├── api-gateway/            # ingress config + thin control-plane glue (no data)
│   ├── bff/                    # FastAPI screen aggregation (/bff/*)
│   ├── identity-tenant-service/
│   ├── core-api/
│   ├── meeting-service/
│   ├── integration-service/
│   ├── billing-metering-service/
│   ├── admin-ops-service/
│   ├── notification-service/
│   └── workflow-service/       # Temporal workers (run via `python -m ...worker`)
├── libs/
│   └── shared/                 # tenancy (org_id ctx, RLS session helper), outbox/inbox,
│                               # idempotency, event envelopes, auth, telemetry, errors
├── proto/                      # gRPC service + message defs (GetUserContext, …)
├── migrations/                 # Alembic (one history; per-service schemas)
├── docker-compose.yml          # local Postgres + Redis + Redpanda + Temporal dev
└── pyproject.toml              # uv/Poetry workspace
```

Each service is a Python package under `services/`. Anything cross-cutting (the `app.current_org` session helper, outbox/inbox base classes, the event envelope, idempotency-key handling, RFC 7807 error model) belongs in `libs/shared/` so all services use one implementation; do not fork these per service.

## Invariants you must not break

| Rule | Why | If you break it |
|---|---|---|
| `org_id` on every hop | Tenant isolation is fail-closed at every layer: header + gRPC metadata carry it, `SET app.current_org` scopes RLS, the `nexus:{org_id}:` prefix scopes Redis. | Cross-tenant data leak. The most severe class of bug in this product. |
| One writer per store | Bounded contexts split cleanly only if no table has two writers. A new write path is a data-model decision, not a code decision. | Two services race the same rows; the future service split re-cuts a domain. Fix canon first. |
| At-least-once + idempotency on every async edge | Redpanda redelivers. Outbox makes the state change and the emit atomic; inbox/dedupe makes redelivery a no-op; DLQ keeps one poison message off the partition. | Lost events, phantom events, double-applied side effects (e.g. a graph edge created twice), or a stalled partition. |
| No silent commits | `decisions`/`actions` represent confirmed user intent; the knowledge graph trusts them. | The graph gains edges the user never confirmed. Only `core-api`'s outbox emits these, only on confirm. |
| Deny-by-default agent authz | Agent-to-agent and cross-tenant requests must fail closed and be audited (`agent_collaborations`). | An agent acts across a tenant boundary it was never granted. |
| Stateless services + durable stores only | Horizontal scale requires no session affinity and no in-process authoritative cache. State lives in Postgres, Redis, object storage, Temporal, or the JWT. | A pod restart or scale-out drops authoritative state; load balancing breaks. |

## Naming authority

Service, table, and topic names are frozen in the canon in `reprise-docs`:

- `_canon/01-service-catalog.md` - service names and sync-API ownership.
- `_canon/02-data-model.md` - table/store names and tenant keys.
- `_canon/03-event-taxonomy.md` - topic names, producers/consumers, schema.

Never invent or rename a service, table, or topic silently. A rename goes through the Glossary plus an ADR first, then propagates from canon outward. Keep these identifiers verbatim, always: `org_id`, `app.current_org`, the `nexus:{org_id}:` Redis prefix, `nexus_knowledge`, `transcript_chunks`, `get_user_context`. The product name in prose is Reprise AI.

## Common tasks (recipes)

Add a REST endpoint to a service.
1. Confirm the path prefix belongs to this service (`_canon/01-service-catalog.md`); no two services share a prefix.
2. Define Pydantic v2 request/response models; the response is the canonical persisted resource, never a partial echo.
3. Write the route under the service package; depend on the shared tenancy helper so `app.current_org` is set transaction-locally before any tenant-scoped query.
4. Mutating POSTs accept `Idempotency-Key`. Errors are RFC 7807 `application/problem+json` with a stable `code` enum.
5. Declare the auth/RBAC requirement in the OpenAPI spec (an endpoint with no declared auth fails review). Non-breaking additions ship in `/api/v1`; breaking changes need `/api/v2` + deprecation window.
6. Add the failing test first (RLS isolation + happy path).

Add a new event producer (with outbox).
1. Confirm the topic name and that this service is an allowed producer in `_canon/03-event-taxonomy.md`; align the Avro schema with `reprise-events` (BACKWARD compatible).
2. In the same DB transaction as the state change, write an outbox row (using the `libs/shared` outbox). Never publish to Redpanda directly from request code.
3. The relay publishes the outbox row; partition key is `org_id`. Include `org_id` and an `idempotency_key` in the envelope.
4. Test that the state change and the outbox row commit atomically (and roll back together).

Add a new event consumer (with inbox/dedupe + DLQ).
1. Confirm this service is an allowed consumer of the topic in `_canon/03-event-taxonomy.md`.
2. Re-assert `org_id` from the event header and `SET app.current_org` before any write.
3. Dedupe on `idempotency_key` via the inbox table plus `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL); write the handler to be idempotent regardless.
4. Route poison messages (schema mismatch, repeated failure) to the per-topic DLQ; never block the partition.
5. Test redelivery is a no-op and that a poison message lands in the DLQ.

Add a table + Alembic migration (with RLS policy).
1. Edit `_canon/02-data-model.md` FIRST (a new write path is a data-model change). One owning service only.
2. Add `org_id UUID` referencing `organizations(id)` to every tenant-scoped row. `plans` is the deliberate exception (global, no `org_id`).
3. `uv run alembic revision --autogenerate -m "feat: add <table>"`; in the migration enable RLS with the canonical policy:
   ```sql
   ALTER TABLE <t> ENABLE ROW LEVEL SECURITY;
   ALTER TABLE <t> FORCE ROW LEVEL SECURITY;
   CREATE POLICY tenant_isolation ON <t>
     USING (org_id = current_setting('app.current_org')::uuid)
     WITH CHECK (org_id = current_setting('app.current_org')::uuid);
   ```
4. Migrations are forward-only and reviewed. Add an RLS-isolation test (a query under tenant A must not see tenant B's rows).

Add a gRPC method.
1. Define the message/service in `proto/`; regenerate stubs. Keep changes BACKWARD compatible.
2. `org_id` travels in gRPC metadata, not in the message body; the handler sets `app.current_org` from it.
3. gRPC stays mesh-internal (e.g. `GetUserContext`); it is never routed publicly.

Add a Temporal workflow/activity.
1. Add it under `services/workflow-service/`; Temporal owns workflow state, so keep workers stateless.
2. Workflows are driven by consumed events or task-queue signals; activities drive CRUD over REST against `core-api`/`integration-service` (do not cross-write their tables).
3. Use durable timers for delays (e.g. action reminders at day 1/3/7), not cron. Key tenant-scoped sagas (e.g. RTBF) by `org_id`/`user_id`.
4. Register the workflow/activity on the worker (`python -m services.workflow_service.worker`).

## Testing

- Runner: `pytest`, TDD (failing test first).
- Backing services: `testcontainers` spins up real Postgres, Redis, and Redpanda per session, so integration tests hit real engines, not mocks.
- What to test, minimum: RLS isolation (tenant A cannot read tenant B; a query with no `app.current_org` set returns nothing, i.e. fail-closed); outbox/inbox idempotency (state + outbox commit atomically; redelivery is a no-op); contract tests against the `reprise-events` Avro schemas (round-trip + BACKWARD compatibility); the RFC 7807 error model.
- Quality gates: `ruff` + `black --check` + `mypy` all green; a failing lint or type-check fails the PR.

```bash
uv run pytest                          # full suite
uv run pytest services/core_api -q     # one service
uv run pytest -m "not slow"            # skip testcontainers-heavy tests
```

## Local development

```bash
uv sync                                     # install deps (or: poetry install)
cp .env.example .env                        # fill from the table below
docker compose up -d                        # Postgres + Redis + Redpanda + Temporal dev
uv run alembic upgrade head                 # apply migrations

uv run uvicorn services.core_api.main:app --reload --port 8000   # run one service
uv run python -m services.workflow_service.worker                # Temporal workers

uv run pytest                                                    # test
uv run ruff check . && uv run black --check . && uv run mypy .   # lint + typecheck
uv run alembic revision --autogenerate -m "feat: add <table>"    # new migration (AFTER canon edit)
```

Required env vars (see `.env.example` for the full list): `DATABASE_URL` (pooled Supabase Postgres; `app.current_org` set per transaction), `REDIS_URL`, `REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL`, `CLERK_SECRET_KEY` / `CLERK_JWKS_URL` / `CLERK_WEBHOOK_SECRET`, `STRIPE_SECRET_KEY` / `STRIPE_WEBHOOK_SECRET`, `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_TASK_QUEUE`, `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` / `LIVEKIT_URL`, `OTEL_EXPORTER_OTLP_ENDPOINT` / `SENTRY_DSN`.

## Pitfalls

- Forgetting `SET app.current_org`, or setting it non-transaction-locally. With a pooled connection (PgBouncer/Supabase pooler) a non-local GUC leaks one tenant's `app.current_org` into another tenant's reuse of the connection. Always `set_config('app.current_org', :org_id, true)` (the `true` is transaction-local). A missing setting must fail closed (no rows), never fall open.
- Cross-writing another service's table because it is "right there" in the shared Postgres. Read it via that service's API or via events; never write it.
- Producing an event without the outbox (publishing to Redpanda from request code). It breaks atomicity: you get lost or phantom events on a crash or rollback.
- Consuming without dedupe. At-least-once means you WILL be redelivered; a non-idempotent handler double-applies the side effect.
- Breaking a topic schema's BACKWARD compatibility. Existing consumers cannot read new messages; coordinate via `reprise-events` and never remove/retype a field in place.
- Writing the knowledge graph (or emitting `decisions`/`actions`) without confirmation. Only `core-api`'s outbox emits these, and only on user confirm.
- Putting `org_id` in a path/query, or returning `403` for cross-tenant access (use `404`). Logging or returning OAuth tokens/secrets (they are envelope-encrypted and never leave the service).
- Blocking a synchronous REST call on an LLM or a cross-service fan-out. Commit an intent and return a pollable/subscribable resource instead.

## Working with other repos

- Depends on `reprise-events` for the Avro schemas, topic taxonomy, and Schema Registry contracts. Every producer/consumer here is bound by those.
- Serves `reprise-ui` and `reprise-mobile` over REST through `bff` (`reprise-ui` is the only caller of `/bff/*`).
- Feeds `reprise-realtime` (realtime-gateway, transcription-service) and `reprise-intelligence` (agent-runtime, RAG, embeddings, knowledge-graph) via Redpanda. Those tiers own `transcript_chunks`, vectors, and the AGE `nexus_knowledge` graph; this repo reads them but never writes them.
- Canon and ADRs live in `reprise-docs`; infra/manifests live in `reprise-platform`.

## Commit and PR conventions

- Conventional Commits (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`).
- Commits in this project must NOT include a `Co-Authored-By` footer.
- Trunk-based development, short-lived branches, mandatory PR review.
- CI must pass: ruff + black + mypy + pytest (testcontainers) + Spectral lint + `oasdiff` breaking-change gate against `main`.
- Every endpoint declares its auth requirement in the OpenAPI spec (no declared auth = failed review). A breaking REST change needs a major-version bump (`/api/v2`); a breaking topic or table change needs an ADR (and a canon edit) first.
