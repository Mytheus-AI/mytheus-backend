# `reprise-backend` - System Design

Deep design reference for the **Reprise AI** control plane and async orchestration tier (internal architecture codename: NEXUS). This document is derived from, and subordinate to, the frozen canon in the `reprise-docs` repo. Where this doc and canon ever disagree, canon wins. Canon docs referenced by filename below (verbatim): `03-container-architecture.md`, `04-service-catalog.md`, `19-api-architecture.md`, `05-data-architecture.md`, `06-multi-tenancy.md`, `07-event-architecture.md`, `18-saas-business-architecture.md`, and the naming authorities `_canon/01-service-catalog.md`, `_canon/02-data-model.md`, `_canon/03-event-taxonomy.md`.

---

## 1. Purpose & bounded contexts

`reprise-backend` is the **synchronous CRUD/control plane plus asynchronous orchestration** half of Reprise AI. It owns durable relational state, identity and tenancy, the SaaS business plane (billing/metering), integrations, meeting-room control, notifications, and the Temporal workflows that coordinate long-running, cross-service processes.

It explicitly does **not** contain the AI/inference tier (LangGraph agents, embeddings, reranker, RAG, knowledge graph, search, ingestion - those live in `reprise-intelligence`) nor the real-time-streaming tier (`realtime-gateway`, `transcription-service` - `reprise-realtime`). Those tiers scale on different signals (GPU/tokens/queue depth; concurrent WebSocket connections) and bill against different cost centers, which is exactly why the system is cut where it is (`03-container-architecture.md` §8).

The ten bounded contexts in this repo, by scaling family:

- **Edge (CRUD family):** `api-gateway`, `bff`.
- **CRUD / control plane:** `identity-tenant-service`, `core-api`, `meeting-service`, `integration-service`, `billing-metering-service`, `admin-ops-service`.
- **Async / orchestration:** `notification-service` (event-driven worker), `workflow-service` (Temporal workers).

Two invariants bind every context and are not re-derived per service (`04-service-catalog.md` §1):

1. **Tenant isolation at every layer** - `org_id` injected from the Clerk-verified JWT, RLS as the fail-closed backstop.
2. **At-least-once event delivery with idempotency** - transactional outbox on produce, inbox + Redis dedupe on consume, per-topic DLQ.

---

## 2. Per-service breakdown

For each service: responsibility, owned tables/stores, events produced, events consumed, and the synchronous API it exposes (REST or gRPC). Table ownership is the container-level projection of `03-container-architecture.md` §5 and `04-service-catalog.md` §3.

### 2.1 `api-gateway` (edge)

- **Responsibility.** Single public ingress: TLS termination (with Cloudflare), WAF passthrough, global + per-tenant rate limiting, Clerk JWT pre-validation against cached JWKS, `org_id` extraction from the `org` claim, routing to `bff` or directly to a service / the WebSocket plane. Holds no business logic and owns no data.
- **Owned tables/stores.** None (stateless, config-only).
- **Events produced / consumed.** None - off the event bus entirely.
- **Sync API.** Owns no path prefix of its own; it is the front of every prefix in `_canon/01-service-catalog.md` §6. Re-verification of the JWT still happens at the service tier - the gateway's pre-validation is a DDoS shield and an optimization, not a trust delegation.

### 2.2 `bff` (edge)

- **Responsibility.** Backend-for-frontend: aggregates/denormalizes reads so the web app makes one call per screen, fans out to services over REST/gRPC, shapes payloads, applies a short read-through Redis cache. No domain ownership.
- **Owned tables/stores.** None (short `nexus:{org_id}:…` Redis read cache only).
- **Events produced / consumed.** None.
- **Sync API (REST).** `GET/POST /bff/*` (screen-shaped), the **only** caller of those endpoints, consumed only by `reprise-ui`. Downstream reads: `identity-tenant-service`, `core-api`, `integration-service`, `billing-metering-service`, `admin-ops-service`, plus the AI-tier `rag-service`/`search-service`.

### 2.3 `identity-tenant-service` (CRUD)

- **Responsibility.** Identity & tenancy control plane: Clerk webhook user-sync (outbox + reconciliation, Clerk as source of truth), org/tenant provisioning & lifecycle, RBAC, SSO/SAML/SCIM, agent-to-agent authorization grants (deny-by-default), and the `get_user_context(user_id)` source-of-record for role/skills/team.
- **Owned tables/stores.** `organizations` (tenant root, `id` = `org_id`), `users`, `teams`, `team_members`, `departments`, `tenant_settings`, `data_residency`, plus RBAC roles/grants, SSO config, SCIM state, agent-authz scopes. Redis: `nexus:{org_id}:user_ctx:{user_id}` (5-min TTL). (Org-hierarchy tables `teams`/`team_members`/`departments` are reassigned to `core-api` for CRUD per `03-container-architecture.md` §5; identity owns the identity-and-tenancy surface.)
- **Events produced.** `tenant-events` (provisioned/suspended/deleted), `notifications`.
- **Events consumed.** `user-sync-events` (Clerk inbox, sourced from the Clerk webhook - a managed-plane edge, not a Reprise-produced topic).
- **Sync API.** REST: `POST /clerk/webhook`, `GET/POST /orgs`, `/rbac/*`, `/scim/v2/*`, `GET /context/{user_id}`. **gRPC:** `GetUserContext(user_id)` - carries `org_id` in metadata; read heavily by the agent tier.

### 2.4 `core-api` (CRUD)

- **Responsibility.** The relational heart and domain CRUD: meetings, participants/speakers, worklogs, decisions/actions read+write models, agent suggestions, agent-collaboration logs, chat persistence, teams/departments read paths. Holds the **outbox** that emits confirmed `decisions`/`actions`.
- **Owned tables/stores.** `meetings`, `meeting_participants`, `meeting_speakers`, `worklogs`, `decisions`, `action_items`, `agent_suggestions`, `agent_collaborations`, `chat_messages`, `teams`, `team_members`, `departments`. Does **not** own `transcript_chunks` (owned by `transcription-service` in `reprise-realtime`); serves its read view via FTS.
- **Events produced.** `meeting-events` (created/updated/ended); and via transactional outbox, `decisions`/`actions` **on confirmation only** - never speculatively.
- **Events consumed.** `agent-results`, `decisions`, `actions` (to update read models, idempotently).
- **Sync API (REST).** `GET/POST/PATCH /meetings`, `/teams`, `/departments`, `/worklogs`, `/decisions`, `/actions`, `/chat`; action endpoints `POST /decisions/:id/confirm`, `POST /actions/:id/confirm`. `GET /meetings/:id/transcript` is served here via FTS over `transcription-service`-owned data.

### 2.5 `meeting-service` (CRUD / real-time-adjacent)

- **Responsibility.** LiveKit transport adapter: mints participant-verified room tokens, receives LiveKit webhooks, bridges the server-side audio hook to `transcription-service`. SDK abstracted behind the adapter so the pick stays swappable.
- **Owned tables/stores.** `meeting_rooms` (LiveKit room ↔ meeting map, egress/recording refs), Postgres RLS by `org_id`. Object storage: `nexus-recordings` (`{org_id}/{meeting_id}/`).
- **Events produced.** `meeting-room-events` (joined/left/recording-ready). The raw-audio handoff to `transcription-service` is a **server-side LiveKit hook, not a topic**.
- **Events consumed.** `meeting-events` (to open/close rooms).
- **Sync API (REST).** `POST /api/meetings/:id/token` (p95 < 250 ms), `POST /livekit/webhook`.

### 2.6 `integration-service` (CRUD)

- **Responsibility.** External integrations: Google Calendar OAuth (auto-create meetings, match participants by email, route attachments to ingestion), Slack bot OAuth, Jira (default) / Linear (alt) bidirectional action-item sync + status webhooks. **Guardrail R8: Calendar + Slack + one of Jira/Linear only.**
- **Owned tables/stores.** `integration_connections` (provider, `external_account_id`, status, scopes, `installed_by`) and `oauth_tokens` (envelope-encrypted at rest, per-tenant KMS data key; never returned by API), plus external↔Reprise id maps and webhook cursors. Postgres RLS by `org_id`. (Canon §3 prose names the map `integration_links`; the frozen data-model table names are `integration_connections` + `oauth_tokens`, used here.)
- **Events produced.** `integration-events` (event→meeting, attachment-to-ingest, external status → action update) and `actions` (status sync back).
- **Events consumed.** `actions` (push Reprise action → external ticket), `meeting-events`, `notifications`.
- **Sync API (REST).** OAuth callbacks `/oauth/{provider}`, provider webhooks `/webhooks/{provider}`.

### 2.7 `billing-metering-service` (CRUD)

- **Responsibility.** SaaS monetization: Stripe billing/subscriptions/seats/dunning/tax, usage metering (`usage-events` → aggregation → quota enforcement), plans/tiers (Free/Pro/Enterprise), per-tenant token budgets & hard quotas (enforces R4), billing read models.
- **Owned tables/stores.** `plans` (global, no `org_id`), `subscriptions`, `usage_events` (append-only, daily range-partition), `usage_aggregates`, `quotas`, `invoices` (immutable once finalized, 7-year retention), `agent_budgets` (per tenant+month; `ok`/`throttled`/`blocked`). Redis hot mirror: `nexus:{org_id}:budget:{period}`. Object storage: `nexus-invoices`.
- **Events produced.** `billing-events` (quota-exceeded, dunning, plan-changed), `notifications`.
- **Events consumed.** `usage-events`, `tenant-events`.
- **Sync API (REST).** `GET/POST /billing/*`, `POST /stripe/webhook`, `GET /usage`.

### 2.8 `admin-ops-service` (CRUD)

- **Responsibility.** Two consoles: tenant-admin (org-scoped self-serve: members, plan, integrations, export/erasure requests) and internal ops (cross-tenant support, feature flags, tenant suspend/migrate, consumer-lag & cost dashboards, audit-log access).
- **Owned tables/stores.** Feature flags, ops audit log, support/impersonation grants, and the compliance `audit_logs` trail (append-only, no UPDATE/DELETE, tamper-evident, monthly range-partition, 1y hot → `nexus-audit-archive`).
- **Events produced.** `admin-events` (flag changed, tenant suspended, erasure requested).
- **Events consumed.** `tenant-events`, `billing-events`, `graph-events` (ops views).
- **Sync API (REST).** `GET/POST /admin/tenant/*`, `/admin/ops/*` (RBAC-gated, fully audited).

### 2.9 `notification-service` (async worker)

- **Responsibility.** Unified delivery fabric: in-app (via `realtime-gateway`), email, Slack DM/channel. Templating, per-user priority queue (urgent alerts pre-empt notes), dedupe, delivery receipts. **Sole consumer authorized to fan `notifications` to external channels.**
- **Owned tables/stores.** `notification_log`, delivery receipts, user channel prefs (Postgres RLS by `org_id`). Redis priority queue `nexus:{org_id}:queue:agent:{user_id}`.
- **Events produced.** `notification-delivery-events`.
- **Events consumed.** `notifications`, `agent-results` (priority alerts).
- **Sync API (REST).** `GET /notifications`, `PATCH /notifications/:id` (read/dismiss) - the durable read side of the realtime alert stream.

### 2.10 `workflow-service` (async / Temporal)

- **Responsibility.** Durable orchestration on Temporal: `MeetingLifecycleWorkflow`, `ActionFollowUpWorkflow` (reminders day 1/3/7, manager escalation via Slack DM at day 10), RTBF (right-to-be-forgotten) deletion saga across all stores, and dunning sagas. Temporal owns workflow state; the workers here are stateless.
- **Owned tables/stores.** None durable outside Temporal (which owns workflow state). May write `nexus-exports` / `nexus-audit-archive` object buckets.
- **Events produced.** `notifications`.
- **Events consumed.** `billing-events`, `tenant-events`, `meeting-events`, `actions`, `admin-events` (drives suspend/migrate/RTBF).
- **Sync API.** Drives CRUD over REST (`core-api`, `integration-service`); internal `/workflows/*` admin surface (not public). Temporal task queues are the primary inbound.

---

## 3. The edge path: Cloudflare → api-gateway → bff/services

All external traffic enters through Cloudflare and is funneled into a single ingress, `api-gateway`. Nothing reaches an internal service except through `api-gateway` (HTTP/WSS) or the Redpanda bus (`03-container-architecture.md` §6).

```
User ──HTTPS+Clerk JWT──▶ Cloudflare (CDN, WAF, global rate limit)
                              │
                              ▼
                         api-gateway ──JWT verify──▶ Clerk (cached JWKS)
                              │  extract org_id + role, pre-validate (FAIL CLOSED)
                              ▼
                            bff ──REST/gRPC (org_id propagated)──▶ CRUD/AI service
                                                                      │
                                              SET app.current_org = :org_id; query
                                                                      ▼
                                                          PostgreSQL (RLS filters by org_id)
```

Edge responsibilities, in order:

1. **Cloudflare** - cached static/RSC assets, WAF rules, global L7 rate limiting before any origin compute.
2. **`api-gateway`** - single ingress: TLS, WAF passthrough, global + per-tenant rate limit, Clerk JWT pre-validation, **`org_id` extraction**, routing. **Fails closed** if `org_id` cannot be resolved - no request reaches a tenant-scoped store without a resolved tenant.
3. **`bff`** - screen aggregation; the only caller of `/bff/*`; owns no data.
4. **`web-app` ↔ `realtime-gateway`** - the WebSocket leg bypasses `bff` and rides through `api-gateway` directly to the real-time tier (in `reprise-realtime`). This is the only long-lived bidirectional path.

`org_id` is **never** a path or query parameter on tenant-scoped resources; it is always derived from the token, so a client cannot request another tenant's data by guessing an id (`19-api-architecture.md` §2.1). A cross-tenant access attempt returns `404`, not `403` - existence itself is tenant-confidential.

---

## 4. Data ownership rule - one writer per store

Every store has exactly one **owning service** that writes it; everyone else reads via that service's API or via events - never cross-writes. This is the exhaustive ownership projection of `03-container-architecture.md` §5. Tables owned **in this repo**:

| Owning service | Tables / stores it writes |
|---|---|
| `identity-tenant-service` | `organizations`, `users`, `teams`*, `team_members`*, `departments`*, `tenant_settings`, `data_residency`, RBAC/SSO/SCIM/agent-authz state |
| `core-api` | `meetings`, `meeting_participants`, `meeting_speakers`, `worklogs`, `decisions`, `action_items`, `agent_suggestions`, `agent_collaborations`, `chat_messages`, `teams`, `team_members`, `departments` |
| `meeting-service` | `meeting_rooms`; `nexus-recordings` object bucket |
| `integration-service` | `integration_connections`, `oauth_tokens` |
| `billing-metering-service` | `plans`, `subscriptions`, `usage_events`, `usage_aggregates`, `quotas`, `invoices`, `agent_budgets`; `nexus-invoices` object bucket |
| `admin-ops-service` | feature flags, ops audit log, `audit_logs`, impersonation grants; `nexus-audit-archive` |
| `notification-service` | `notification_log`, delivery receipts, channel prefs |

\* Org-hierarchy CRUD (`teams`/`team_members`/`departments`) is assigned to `core-api` per `03-container-architecture.md` §5; identity holds the identity-and-tenancy surface. Stores owned by the AI/real-time tiers (`transcript_chunks` → `transcription-service`; `document_chunks`/`document_elements` → `ingestion-service`; Qdrant vectors → `embedding-service`; AGE `nexus_knowledge` → `knowledge-graph-service`) are read-only from this repo's perspective. **Any new write path requires a `_canon/02-data-model.md` edit first.**

---

## 5. Multi-tenancy

The tenant key is **`org_id UUID`**, references `organizations(id)`, present on every tenant-scoped row, cache key, event record, and object prefix (`06-multi-tenancy.md` §1). Model: **pooled + PostgreSQL RLS** for the Free/Pro long tail; **cell-based isolation** (dedicated Postgres instance/cluster) for Enterprise - and RLS is **on even inside a cell** (defense-in-depth).

The canonical RLS policy (frozen in `_canon/02-data-model.md` §7):

```sql
ALTER TABLE <tenant_table> ENABLE ROW LEVEL SECURITY;
ALTER TABLE <tenant_table> FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON <tenant_table>
  USING (org_id = current_setting('app.current_org')::uuid)
  WITH CHECK (org_id = current_setting('app.current_org')::uuid);
```

The session GUC is set **per request, transaction-local**, from the injected context:

```sql
SELECT set_config('app.current_org', :org_id_from_jwt, true);  -- true = transaction-local
```

Transaction-local is mandatory: with a pooled connection (PgBouncer/Supabase pooler) a non-local GUC could leak one tenant's `app.current_org` into another tenant's reuse of the connection. This is the single most important RLS operational rule at SaaS scale.

**Cell routing for enterprise.** `identity-tenant-service` resolves `org_id` → posture + cell; `data_residency.db_cell` pins the cell and `region`/`qdrant_region`/`object_bucket_region`, immutable after `locked_at`. Enterprise traffic is routed to the dedicated cell; pooled traffic to the shared estate.

---

## 6. Event delivery contract

The async backbone is **Redpanda** (Kafka API), **Avro + Schema Registry**, one subject per topic (`{topic}-value`), **BACKWARD** compatibility, every topic **partitioned by tenant** (`org_id` partition key). Delivery is **at-least-once with idempotency** (`07-event-architecture.md`, `_canon/03-event-taxonomy.md`):

- **Transactional outbox.** A producer writes an outbox row in the **same DB transaction** as its state change; a relay publishes it to Redpanda. The state change and the intent-to-emit commit atomically - no lost events, no phantom events.
- **Inbox dedupe.** A consumer dedupes on `idempotency_key` via an **inbox** table plus the Redis cache key `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL). A redelivered message is recognized and skipped; consume handlers are written to be idempotent regardless.
- **Per-topic DLQ.** Poison messages (schema mismatch, repeated handler failure) land in a per-topic dead-letter queue for inspection and replay; they never block the partition.
- **Consumer re-asserts `org_id`** from the event header before any write, so RLS scope applies identically on the async path.

This contract is the event-plane twin of the synchronous `Idempotency-Key` contract (`19-api-architecture.md` §8): a retried `POST /decisions/:id/confirm` produces the `decisions` event exactly once, so the graph edge is never double-created.

### 6.1 Canonical topics produced/consumed here

`transcript-chunks` is **not** produced or consumed in this repo (it is owned by the real-time/AI tiers). The topics on this repo's edges:

| Topic | Produced by (here) | Consumed by (here) |
|---|---|---|
| `tenant-events` | `identity-tenant-service` | `billing-metering-service`, `admin-ops-service`, `workflow-service` |
| `meeting-events` | `core-api` | `meeting-service`, `workflow-service` |
| `meeting-room-events` | `meeting-service` | (consumed by `transcription-service` in `reprise-realtime`) |
| `integration-events` | `integration-service` | (consumed by `ingestion-service` in `reprise-intelligence`) |
| `usage-events` | (produced by the AI tier) | `billing-metering-service` |
| `billing-events` | `billing-metering-service` | `workflow-service`, `admin-ops-service` |
| `notifications` | `identity-tenant-service`, `billing-metering-service`, `integration-service`, `workflow-service` (+ agent tier) | `notification-service` |
| `notification-delivery-events` | `notification-service` | (ops views) |
| `decisions` | `core-api` (on confirm) | `core-api` (read models), `knowledge-graph-service`, `integration-service` |
| `actions` | `core-api` (on confirm), `integration-service` (status sync) | `core-api`, `integration-service`, `workflow-service`, `knowledge-graph-service` |
| `agent-results` | (produced by the agent tier) | `core-api`, `notification-service` |
| `admin-events` | `admin-ops-service` | `workflow-service` |

---

## 7. Confirmation rule - no silent graph writes

`agent-runtime-service` (in `reprise-intelligence`) *detects* candidate decisions and action items and surfaces them via `agent-results` to the sidebar. It **never** writes Postgres directly and **never** produces the `decisions`/`actions` topics. The user confirms via `core-api` (`POST /decisions/:id/confirm`, `POST /actions/:id/confirm`); only then does **`core-api`'s transactional outbox** emit the confirmed `decisions`/`actions` event that `knowledge-graph-service` consumes to create the AGE `PRODUCED` / `ASSIGNED_TO` edge. There are **no silent graph writes** - edges exist only behind a confirmed row (`03-container-architecture.md` §4.2, §9).

---

## 8. Sync API ownership & path prefixes

The synchronous REST surface is **resource-oriented and uniform**, organized by owning service per `_canon/01-service-catalog.md` §6 / `19-api-architecture.md` §5. **No two services share a path prefix.**

- **Versioning.** URI-path major version: `/api/v1/...`. A breaking change (remove/rename a field, tighten a type, change an error shape or default) requires a major bump; `/api/v2` runs in parallel through a published deprecation window (min 6 months for Pro/Enterprise). Non-breaking additions ship within `v1`. The resolved version is echoed in `X-NEXUS-API-Version`.
- **OpenAPI.** Spec-first. Each service publishes an **OpenAPI 3.1** document; FastAPI's generated schema is reconciled against it in CI. Spectral lints the conventions (snake_case fields, RFC 7807 errors with a stable `code` enum, cursor pagination, `Idempotency-Key` on mutating POSTs, `org_id`-never-in-path, declared auth). `oasdiff` fails any breaking change without a major bump. The TS client and Pydantic models are generated from the spec, so client/server drift is structurally impossible.
- **Internal-only (never routed publicly):** the AI/real-time tiers' `/embed`, `/rerank`, `/graph/cypher`, `/agents/*`, and Temporal `/workflows/*`. The gRPC `GetUserContext` is mesh-internal.

Owning-service prefixes (this repo): `/users`, `/orgs`, `/rbac`, `/scim/v2`, `/context` → `identity-tenant-service`; `/meetings`, `/teams`, `/departments`, `/worklogs`, `/decisions`, `/actions`, `/suggestions`, `/chat` → `core-api`; `/api/meetings/:id/token`, `/livekit/webhook` → `meeting-service`; `/oauth/{provider}`, `/webhooks/{provider}` → `integration-service`; `/billing/*`, `/usage`, `/plans`, `/stripe/webhook` → `billing-metering-service`; `/admin/*` → `admin-ops-service`; `/notifications` → `notification-service`; `/bff/*` → `bff`.

---

## 9. Billing/metering pipeline

`usage-events` (token, meeting-minute, transcription-second, storage-GB) flow from the producing tiers into `billing-metering-service`, which:

1. **Aggregates** them idempotently (on `idempotency_key`, append-only, no double-bill) into `usage_aggregates`.
2. **Enforces quotas** against `agent_budgets` per (tenant, period): `ok` → `throttled` → `blocked`, deny-by-default at the hard cap, alerts at 80%/95%. A Redis hot mirror `nexus:{org_id}:budget:{period}` serves fast quota checks even when nightly aggregation lags.
3. **Drives Stripe** for subscriptions/seats/dunning/tax; the `POST /stripe/webhook` handler verifies the signature, dedupes on the provider `event_id`, updates `subscriptions`/`invoices`, and emits `billing-events` that drive the dunning saga in `workflow-service`.

A breached budget emits a `429 QUOTA_EXCEEDED` on the API path and a `billing-events` quota-exceeded on the bus; agents stop spending. See `18-saas-business-architecture.md`.

---

## 10. Temporal workflows

`workflow-service` hosts the durable orchestration; Temporal owns workflow state so the workers are stateless and scale on **task-queue depth**:

- **`MeetingLifecycleWorkflow`** - driven by `meeting-events`; coordinates room open/close, post-meeting processing, and cleanup.
- **`ActionFollowUpWorkflow`** - driven by confirmed `actions`; reminders at day 1/3/7, manager escalation via Slack DM at day 10 (durable timers, no cron).
- **RTBF saga** - triggered by `admin-events` (erasure requested); deletes a tenant/user across all six stores in a single workflow keyed by `org_id`/`user_id`.
- **Dunning saga** - driven by `billing-events`; staged payment-retry and downgrade.

---

## 11. Scaling profile

- **CRUD / control plane** (`api-gateway`, `bff`, `identity-tenant-service`, `core-api`, `meeting-service`, `integration-service`, `billing-metering-service`, `admin-ops-service`, `notification-service`): autoscale on **HTTP req/s + p95 latency**, cost driven by CPU + Postgres connections. SLO: **REST p95 < 300 ms, p99 < 800 ms** (token-mint p95 < 250 ms).
- **Async / orchestration** (`workflow-service`): autoscale on **Temporal task-queue depth**, cost driven by worker CPU.

Every service is **horizontally stateless** - no session affinity, no in-process authoritative cache. Session state lives in the Clerk JWT; ephemeral caching lives in namespaced Redis. Early stage runs as a modular monolith on Railway/Render; production runs on Kubernetes (EKS/GKE) with per-service HPA/KEDA signals, peeled off in the canon extraction order.

---

## 12. Security

- **JWT verify at the gateway** against cached Clerk JWKS; the service tier **re-verifies** (the gateway is a shield, not a trust delegation). JWKS unreachable → fail **closed** for protected routes.
- **RBAC** org/role-scoped on every endpoint; the security requirement is declared in the OpenAPI spec and reviewed in every PR (an endpoint with no declared auth fails review).
- **Deny-by-default agent-to-agent authz.** Agent-authz grant lookups fail closed; cross-tenant agent requests are refused and audited (`agent_collaborations`).
- **Secrets handling.** OAuth tokens are **envelope-encrypted at rest** (per-tenant KMS data key), never logged, never returned by API. Provider webhooks are **signature-authenticated** (Svix/Clerk, Stripe-Signature, LiveKit HMAC, Google channel token, Jira/Linear HMAC) before any work; verify → dedupe → normalize → return `2xx` fast.
- **Tenant confidentiality.** Cross-tenant access returns `404`, never `403`; `org_id` is never path/query-derived.
- **Audit.** `audit_logs` are append-only and tamper-evident; an operation that requires audit fails closed if the audit write fails.

---

## 13. Dev practices

- **Typing.** Full Python type hints; Pydantic v2 models for all wire contracts; `mypy` in CI.
- **Formatting / linting.** `ruff` + `black` (or `ruff format`); a failing lint fails the PR.
- **Testing.** `pytest` with **TDD** - failing test first. `testcontainers` for real Postgres/Redis/Redpanda in integration tests (RLS fail-closed, outbox/inbox, Avro round-trips).
- **Migrations.** Alembic; a new write path requires a data-model change (canon) before the migration. Migrations are forward-only and reviewed.
- **Commits.** Conventional Commits (`feat:`, `fix:`, `chore:`, …). **No `Co-Authored-By` footer** in this project.
- **Branching.** Trunk-based with short-lived branches and mandatory PR review.

---

## 14. CI/CD

GitHub Actions pipeline:

1. **Lint** - `ruff` + `black --check`.
2. **Type-check** - `mypy`.
3. **Test** - `pytest` with `testcontainers`.
4. **Spec governance** - Spectral lint + `oasdiff` breaking-change gate against `main`; client/model generation from OpenAPI.
5. **Build** - Docker image per service.
6. **Deploy** - to Railway/Render (early) or Kubernetes EKS/GKE (production) via the `reprise-platform` Terraform/manifests.

---

## 15. Observability

- **Tracing.** OpenTelemetry; the W3C `trace_id` is propagated from `api-gateway` through every hop, surfaced in error envelopes, and pivots to logs/Sentry.
- **Metrics & dashboards.** Prometheus + Grafana - req/s, p95/p99 latency, error rates, **consumer lag** per topic, Temporal queue depth, per-tenant quota state.
- **Errors.** Sentry, keyed by `trace_id`.
- **Logs.** Structured **JSON** logs (always include `org_id`, `trace_id`, service, route).
- **Alerts.** Consumer-lag alerts per topic (lag SLO breaches), quota/budget alerts (80%/95%/hard cap), p95 SLO breaches, DLQ depth.
