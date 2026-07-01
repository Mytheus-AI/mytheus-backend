# Mytheus - Backend / Control Plane (`mytheus-backend`)

> The CRUD control plane and async orchestration tier for **Mytheus** - an intelligent meeting-orchestration multi-tenant SaaS (internal architecture codename: NEXUS).

## What this is

`mytheus-backend` is the **synchronous control plane plus asynchronous orchestration** tier of Mytheus. It owns the relational heart (meetings, decisions, actions, worklogs), identity and tenancy, billing and metering, integrations, real-time room control, notifications, and the durable Temporal workflows that stitch them together. It is the request/response and event-consuming half of the platform; the AI/inference and real-time-streaming tiers live in sibling repositories (see [Related repos](#related-repos)).

It is **not** the AI tier (no LangGraph agents, no embeddings, no GPU inference) and **not** the WebSocket fan-out tier - those scale on different signals and bill against different cost centers. This repo scales on **HTTP req/s** (CRUD services, p95 < 300 ms SLO) and **Temporal task-queue depth** (the async tier).

Early on, these services run as a **modular monolith on Railway/Render** - every service below is a module in one deployable. As load grows, the seams below are the extraction points: the monolith **splits into the listed services on Kubernetes (EKS/GKE)** with per-service autoscalers. The bounded-context boundaries are drawn so the split never re-cuts a domain.

Architecture source of truth: the canon docs in the `mytheus-docs` repo. The most relevant are `03-container-architecture.md`, `04-service-catalog.md`, `19-api-architecture.md`, `06-multi-tenancy.md`, and `07-event-architecture.md`.

## Services in this repo

| Service | Responsibility | Scales on |
|---|---|---|
| `api-gateway` | Single ingress (Envoy/Nginx behind Cloudflare): TLS termination, WAF passthrough, global rate limit, Clerk JWT pre-validation, `org_id` extraction, routing to `bff`/services. No business logic, no data. | req/s + concurrent connections |
| `bff` | Backend-for-frontend (FastAPI): screen-shaped `/bff/*` endpoints, read aggregation/denormalization, fan-out to services, short Redis read cache. No domain ownership. | UI req/s |
| `identity-tenant-service` | Users, organizations, tenants, RBAC, `tenant_settings`, `data_residency`; Clerk webhook user-sync (outbox + reconciliation); SSO/SAML/SCIM; agent-authz grants. Produces `tenant-events`; exposes gRPC `GetUserContext`. | auth + webhook req/s |
| `core-api` | The relational heart: `meetings`, `decisions`, `action_items`, `worklogs`, `agent_suggestions`, `chat_messages`, `teams`. Consumes `agent-results`/`decisions`/`actions`; produces `meeting-events` and confirmed `decisions`/`actions`. | req/s |
| `meeting-service` | `meeting_rooms`; LiveKit token mint, room API, webhooks; server-side audio hook to the transcription tier. Produces `meeting-room-events`. | concurrent rooms |
| `integration-service` | `integration_connections`/`oauth_tokens`; Google Calendar / Slack / Jira / Linear OAuth + sync (guardrail R8). Produces `integration-events`; syncs `actions` back. | webhook + sync req/s |
| `billing-metering-service` | `plans`, `subscriptions`, `usage_events`, `usage_aggregates`, `quotas`, `invoices`, `agent_budgets`; Stripe billing/seats/dunning/tax. Consumes `usage-events`; produces `billing-events`. | metering ingest + req/s |
| `admin-ops-service` | Feature flags, `audit_logs`, internal ops console, impersonation grants. Produces `admin-events`. | low traffic |
| `notification-service` | `notification_log`, delivery receipts, channel prefs; sole authorized external-channel fan-out. Consumes `notifications`/`agent-results`; produces `notification-delivery-events`. | `notifications` throughput |
| `workflow-service` | Temporal workers: `MeetingLifecycleWorkflow`, `ActionFollowUpWorkflow`, RTBF + dunning sagas. | Temporal task-queue depth |

## Tech stack

- **Language / runtime:** Python 3.11+
- **Web framework:** FastAPI; Pydantic v2 for all request/response models
- **ORM / migrations:** SQLAlchemy + Alembic
- **Database:** PostgreSQL (docker-compose for dev; self-hosted on the VPS for prod, or a managed pooled Postgres). Row-Level Security, `org_id` tenant key; Apache AGE `nexus_knowledge` co-located but owned by the AI tier
- **Cache / pub-sub:** Redis / Upstash (namespaced `nexus:{org_id}:…`)
- **Event bus:** Redpanda (Kafka API) via `aiokafka`; Avro + Schema Registry, BACKWARD compat
- **Durable workflows:** Temporal (Temporal Python SDK), Temporal Cloud task queues
- **Billing:** Stripe SDK
- **Auth:** Clerk (JWT verify at the gateway, webhook user-sync at identity)
- **Internal mesh:** gRPC for tight-latency service-to-service calls (e.g. `GetUserContext`)
- **Meeting transport:** LiveKit (token mint, room API, webhooks) behind a transport adapter

## Repo structure

A `uv`/Poetry-managed monorepo. Each service is a Python package under `services/`; shared code is a single `libs/shared` package; proto and migrations are top-level so they are owned across services.

```
mytheus-backend/
├── services/
│   ├── api-gateway/            # Envoy/Nginx config + thin control-plane glue
│   ├── bff/                    # FastAPI screen aggregation (/bff/*)
│   ├── identity-tenant-service/
│   ├── core-api/
│   ├── meeting-service/
│   ├── integration-service/
│   ├── billing-metering-service/
│   ├── admin-ops-service/
│   ├── notification-service/
│   └── workflow-service/       # Temporal workers
├── libs/
│   └── shared/                 # tenancy (org_id ctx, RLS session), outbox/inbox,
│                               # idempotency, event envelopes, auth, telemetry, errors
├── proto/                      # gRPC service + message definitions (GetUserContext, …)
├── migrations/                 # Alembic migrations (one history; per-service schemas)
├── docker-compose.yml          # local Postgres + Redis + Redpanda (+ Temporal dev)
├── pyproject.toml              # uv/Poetry workspace
└── README.md
```

## Quick start

**Prerequisites**

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) (preferred) or Poetry
- Docker + Docker Compose (local Postgres + Redis + Redpanda, and a Temporal dev server)

**Install**

```bash
# clone, then from the repo root
uv sync                       # or: poetry install
cp .env.example .env          # fill in the values from "Environment variables"
```

**Run the local backing services**

```bash
docker compose up -d          # Postgres, Redis, Redpanda, Temporal dev
uv run alembic upgrade head   # apply migrations
```

**Run a service (modular-monolith dev mode)**

```bash
uv run uvicorn services.core_api.main:app --reload --port 8000
# each service is its own FastAPI app (services.<svc>.main:app); run the ones you need
# Temporal workers:
uv run python -m services.workflow_service.worker
# Transactional-outbox relay (worker process):
uv run python -m services.relay.main
```

**Test**

```bash
uv run pytest                 # see Testing below
```

## Environment variables

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (pooled). RLS `app.current_org` is set per transaction. |
| `REDIS_URL` | Redis / Upstash URL for namespaced cache, dedupe, rate-limit, presence. |
| `REDPANDA_BROKERS` | Comma-separated Redpanda (Kafka API) bootstrap brokers. |
| `SCHEMA_REGISTRY_URL` | Avro Schema Registry endpoint. |
| `CLERK_SECRET_KEY` / `CLERK_JWKS_URL` / `CLERK_WEBHOOK_SECRET` | Clerk auth, JWKS for JWT verify, Svix webhook signature secret. |
| `STRIPE_SECRET_KEY` / `STRIPE_WEBHOOK_SECRET` | Stripe billing and signed webhook verification. |
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_TASK_QUEUE` | Temporal Cloud (or dev server) connection and queue. |
| `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` / `LIVEKIT_URL` | LiveKit token mint, room API, webhook verification. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `SENTRY_DSN` | OpenTelemetry export and Sentry error reporting. |

See `.env.example` for the full, current list.

## Testing

- **Runner:** `pytest`.
- **Backing services:** [`testcontainers`](https://testcontainers.com/) spins up ephemeral Postgres, Redis, and Redpanda per test session, so integration tests run against real engines (RLS policies, outbox/inbox, Avro round-trips) rather than mocks.
- **Style:** TDD - write the failing test first. Tests cover tenant isolation (RLS fail-closed), idempotent event consumption, and the typed error model.

```bash
uv run pytest                          # full suite
uv run pytest services/core_api -q     # one service
uv run pytest -m "not slow"            # skip testcontainers-heavy tests
```

## Related repos

- `mytheus-docs` - the frozen canon and architecture source of truth (`_canon/`, the numbered docs).
- `mytheus-ui` - Next.js 14 web app (the only caller of `/bff/*`).
- `mytheus-mobile` - mobile client.
- `mytheus-realtime` - real-time tier: `realtime-gateway` (Socket.io fan-out) + `transcription-service` (Deepgram proxy).
- `mytheus-intelligence` - AI/inference tier: `agent-runtime-service`, `rag-service`, `embedding-service`, `reranker-service`, `knowledge-graph-service`, `search-service`, `ingestion-service`.
- `mytheus-events` - shared Avro schemas, topic taxonomy, and Schema Registry contracts.
- `mytheus-platform` - Terraform IaC, Kubernetes manifests, CI/CD, and deployment config.

## License

Proprietary © Mytheus. All rights reserved.
