# Session Summary

## Session 1 — Auth migration + initial scaffold

### 1. Replaced Clerk Auth with Supabase Auth

**Why this decision was taken:**
The project already uses Supabase Postgres. Using Supabase Auth eliminates a separate third-party vendor dependency (Clerk), reduces the credential surface, and allows auth + database to share a single Supabase project. The existing Clerk decision was recorded in ADR-0010 (`mytheus-docs/adr/0010-auth-clerk-enterprise-sso.md`); this change supersedes that ADR for the identity provider only — the surrounding architecture (JWT-in-bearer, org_id in every hop, RBAC tiers, SCIM stubs) is preserved.

**Key changes from Clerk → Supabase Auth:**

| Concern | Clerk (old) | Supabase Auth (new) |
|---|---|---|
| JWT signing | RS256, Clerk JWKS | HS256, `SUPABASE_JWT_SECRET` |
| `org_id` in JWT | Clerk `org` claim (automatic) | Custom claim via PostgreSQL hook function |
| User sync | `POST /clerk/webhook` (Svix HMAC) | Postgres trigger on `auth.users` (primary) + DB webhook (secondary) |
| User reconciliation | Temporal job → Clerk API | Temporal job → Supabase Admin API |
| SSO/SAML/SCIM | Clerk enterprise SSO | Supabase SSO (Pro+ plan) — route stubs added |
| Env vars removed | `CLERK_SECRET_KEY`, `CLERK_JWKS_URL`, `CLERK_WEBHOOK_SECRET` | — |
| Env vars added | — | `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_JWT_SECRET`, `SUPABASE_WEBHOOK_SECRET` |

**Supabase custom JWT claims hook (`migrations/sql/custom_claims_hook.sql`):**
Supabase does not automatically put `org_id` in the JWT. A PostgreSQL function `public.custom_access_token_hook` reads `public.users.org_id` for the signing user and injects it as a custom JWT claim on every token issue/refresh. This preserves the `org_id`-from-JWT pattern used everywhere (api-gateway extraction, service-tier re-verification, `SET app.current_org`, Redis namespacing).

**To activate:** Run the SQL, then in Supabase Dashboard → Auth → Hooks → "Customize Access Token (JWT) Claims" → select `public.custom_access_token_hook`.

**User sync trigger (`migrations/sql/user_sync_trigger.sql`):**
A `AFTER INSERT OR UPDATE OR DELETE ON auth.users` trigger syncs rows into `public.users` immediately and transactionally. This replaces the reliability problem that drove Clerk's inbox/reconciliation design: because the trigger fires in the same Postgres transaction as the auth change, it cannot be dropped. The secondary Supabase DB webhook and hourly Temporal reconciliation job remain as belt-and-suspenders.

**`public.users.id` = `auth.users.id`** — the Supabase user UUID is used directly as the primary key; no separate `supabase_user_id` column is needed (unlike Clerk's `clerk_user_id` field).

**JWT verification in `libs/shared/auth.py`:**
Uses `PyJWT` with `HS256` and `SUPABASE_JWT_SECRET`. The audience is `"authenticated"` (Supabase default). Raises `401` if the token is missing, expired, or lacks the `org_id` custom claim.

---

## Session 2 — Scaffold cleanup (removed overcomplication)

### What was over-created in session 1 and why it was trimmed

In session 1, the scaffold went too far: it created a `raise NotImplementedError` stub for every endpoint in all 10 services (30+ route files), plus some files that duplicate ownership from sibling repos. The user correctly identified this as overcomplication — reading the sibling repo READMEs confirmed the problem.

**Files deleted (33 files):**

- **All route stubs** across bff, core_api, meeting_service, integration_service, billing_metering_service, admin_ops_service, notification_service — every one was `raise NotImplementedError`. Routes should be added when features are implemented, not pre-declared.
- `migrations/sql/rls_policies.sql` — the RLS template comment is documentation (it's in CLAUDE.md already); the outbox/inbox DDL belongs in an Alembic migration generated from SQLAlchemy models, not a standalone SQL file in a skeleton with no models yet.

**Why `libs/shared/events.py` was kept (not deleted):**
CLAUDE.md explicitly states that event envelopes belong in `libs/shared/`. The `mytheus-events` repo owns Avro schemas and Schema Registry subjects; `libs/shared/events.py` is the Python wrapper/envelope class that this service uses to enqueue outbox rows. These are distinct concerns.

**Why `conftest.py` was kept:**
CLAUDE.md specifies testcontainers as the backing-service strategy for tests. The root conftest.py implements exactly that pattern. It's minimal (25 lines) and is the correct foundation for future tests.

**Why `libs/shared/outbox.py` and `inbox.py` were kept:**
CLAUDE.md explicitly says `outbox/inbox base classes` belong in `libs/shared/`. These are not the Avro schemas (those are in mytheus-events); they are the Python helpers that write outbox rows in a transaction and dedupe inbox messages.

### Final structure after cleanup

```
mytheus-backend/
├── pyproject.toml
├── docker-compose.yml
├── .env.example
├── Makefile
├── conftest.py                          # testcontainers fixtures (Postgres + Redis)
├── libs/
│   └── shared/                          # 8 cross-cutting modules (CLAUDE.md: do not fork these)
│       ├── auth.py                      # Supabase JWT verify + org_id/user_id extraction
│       ├── tenancy.py                   # org_id context var + set_rls() (transaction-local GUC)
│       ├── outbox.py                    # transactional outbox enqueue helper
│       ├── inbox.py                     # at-least-once dedupe (Redis + inbox table)
│       ├── events.py                    # EventEnvelope dataclass (org_id partition key)
│       ├── idempotency.py               # Idempotency-Key middleware for mutating POSTs
│       ├── errors.py                    # RFC 7807 ProblemDetail exception + handlers
│       └── telemetry.py                 # OpenTelemetry setup (OTLP exporter, FastAPI)
├── services/
│   ├── api_gateway/main.py              # FastAPI app, no routes yet (JWT middleware + routing TODO)
│   ├── bff/main.py                      # FastAPI app, no routes yet
│   ├── identity_tenant_service/
│   │   ├── main.py                      # FastAPI app (auth + scim wired)
│   │   └── routes/
│   │       ├── auth.py                  # POST /supabase/webhook (Supabase user-sync)
│   │       └── scim.py                  # SCIM 2.0 stubs /scim/v2/* (Enterprise, not implemented)
│   ├── core_api/main.py
│   ├── meeting_service/main.py
│   ├── integration_service/main.py
│   ├── billing_metering_service/main.py
│   ├── admin_ops_service/main.py
│   ├── notification_service/
│   │   ├── main.py                      # FastAPI app (REST read surface)
│   │   └── consumer.py                  # AIOKafka consumer for notifications + agent-results
│   └── workflow_service/
│       ├── worker.py                    # Temporal worker entry point
│       └── workflows/
│           ├── meeting_lifecycle.py     # MeetingLifecycleWorkflow
│           ├── action_followup.py       # ActionFollowUpWorkflow (day 1/3/7/10 timers)
│           ├── rtbf.py                  # RTBF deletion saga (6 stores)
│           └── dunning.py              # Dunning saga
├── proto/
│   └── identity.proto                   # GetUserContext gRPC (identity-tenant-service)
└── migrations/
    ├── alembic.ini
    ├── env.py
    ├── versions/                         # empty; first migration: uv run alembic revision --autogenerate
    └── sql/
        ├── custom_claims_hook.sql        # Supabase custom JWT org_id claim hook
        └── user_sync_trigger.sql         # auth.users → public.users trigger
```

28 meaningful Python files (vs 60+ before cleanup).

**Notes on libs/shared file count:** The lean skeleton originally described "~6 files" but CLAUDE.md explicitly names event envelopes and idempotency-key handling as cross-cutting concerns that belong here. The 8-file count is correct:
- `events.py` — `outbox.py` imports `EventEnvelope` from it; these are coupled. The `mytheus-events` repo owns Avro schemas; this is the Python envelope wrapper.
- `idempotency.py` — working middleware (not a stub); CLAUDE.md mandates Idempotency-Key on every mutating POST.

**Note on `api_gateway/`:** Was previously only a `nginx.conf` (infra config that belongs in `mytheus-platform`). Replaced with a proper Python package stub (`main.py` + `__init__.py`) matching the other 9 service packages. The nginx.conf was deleted.

---

## What still needs to be done (next sessions)

1. **SQLAlchemy models** — Add ORM models per service, update `migrations/env.py` imports, run first Alembic migration (this is also where the outbox/inbox DDL lands, with RLS policies per the template in CLAUDE.md).
2. **Route implementations** — Add `routes/` packages and implement real DB queries using `set_rls()` + SQLAlchemy. Start with `identity_tenant_service` (orgs, rbac, context) then `core_api` (meetings, decisions, actions).
3. **Outbox relay** — A background task or Temporal activity that reads pending `outbox` rows and publishes to Redpanda. Wire into `workflow_service` or as a FastAPI background task.
4. **Kafka consumers** — Wire `notification_service/consumer.py` (get redis/session from app state); add consumers for `core_api` (agent-results, decisions, actions) and other services.
5. **gRPC server** — Implement `GetUserContext` in `identity_tenant_service`; generate stubs with `make proto`.
6. **Temporal reconciliation** — Scheduled workflow calling Supabase Admin API (`GET /auth/v1/admin/users`) to diff and reconcile `public.users` hourly.
7. **Supabase hook activation** — Run `migrations/sql/custom_claims_hook.sql` against the Supabase project and enable in Dashboard (Auth → Hooks → "Customize Access Token (JWT) Claims" → select `public.custom_access_token_hook`).
8. **Tests** — RLS isolation tests, outbox/inbox idempotency tests, Avro round-trip tests against `mytheus-events` schemas.
9. **Update sibling repos** — `mytheus-ui` and `mytheus-mobile` still reference Clerk (`@clerk/nextjs`, `@clerk/clerk-expo`). They need updating to `@supabase/ssr` and Supabase Mobile Auth respectively once this backend auth migration is confirmed working.

## Key invariants (never break these)

- `org_id` on every hop — JWT claim, REST header, gRPC metadata, `SET app.current_org` GUC, `nexus:{org_id}:` Redis prefix.
- `set_config('app.current_org', :org_id, true)` — the `true` (transaction-local) is MANDATORY. Non-local GUC leaks across pooled connections.
- Cross-tenant access returns `404`, never `403` (existence is tenant-confidential).
- One writer per store — no service cross-writes another service's tables.
- `decisions`/`actions` events ONLY from `core_api` outbox, ONLY on user-confirmed `POST /decisions/:id/confirm` or `/actions/:id/confirm`.
- OAuth tokens are envelope-encrypted at rest; never returned by API.
- `SUPABASE_SERVICE_ROLE_KEY` — admin key, stays server-side only.

---

## Session 3 — Plan 0 scaffold (event contracts, data layer, relay, gRPC) + revert Supabase Auth → Clerk

This session executed **Plan 0** (`mytheus-docs/plan/plan-0-scaffold-and-contracts.md`), Phases 0B and 0C for this repo. The single most important reversal: **identity is Clerk again** (the resolved program decision `IDENTITY = Clerk` in `00-PROJECT-UNDERSTANDING.md §11`), undoing the Session-1 Supabase-Auth drift documented above.

### Why the Supabase→Clerk revert

The Session-1 change (Clerk → Supabase Auth) contradicted the frozen program decision and ADR-0010 (`0010-auth-clerk-enterprise-sso.md`), which keep **Clerk** as the identity provider. Supabase Auth was reverted; **Supabase-managed Postgres was NOT the thing reverted** — only Auth. (Deployment note: this program now self-hosts Postgres/Redis/Redpanda on an existing VPS rather than Railway/Render, so `DATABASE_URL` points at the VPS/self-hosted Postgres in prod and the docker-compose Postgres in dev.)

### Exactly what changed (auth)

- **`libs/shared/auth.py`** — HS256 `SUPABASE_JWT_SECRET` verifier replaced with **Clerk RS256 JWKS** verification via `PyJWKClient` (cached, rotation-honored). `sub` is the Clerk user id (maps to `users.clerk_user_id`), `org_id` is a Clerk session claim; missing `org_id` fails closed (401). JWKS client + issuer read **lazily** so importing the module never requires Clerk env.
- **`services/identity_tenant_service/routes/auth.py`** — Supabase DB-webhook (`POST /supabase/webhook`, `X-Webhook-Secret` HMAC) replaced with **`POST /clerk/webhook`** verifying the **Svix** signature (`svix` dependency). Handler stub returns `{ok: true}`; P1 will inbox-dedupe on the Clerk `event_id`, upsert `users`, and outbox `user-sync-events`.
- **`.env.example`** — removed the `SUPABASE_*` auth block; re-added `CLERK_SECRET_KEY` / `CLERK_PUBLISHABLE_KEY` / `CLERK_JWKS_URL` / `CLERK_ISSUER` / `CLERK_WEBHOOK_SECRET`.
- **Deleted** `migrations/sql/custom_claims_hook.sql` and `migrations/sql/user_sync_trigger.sql` (the Supabase `custom_access_token_hook` and the `auth.users → public.users` trigger). Clerk injects `org_id` at the edge; user-sync is the Svix webhook + reconciliation, not a Postgres trigger.
- README/CLAUDE prose reconciled; a CI grep guard asserts no `supabase_jwt` / `custom_access_token_hook` / `/supabase/webhook` references remain.

### Data + eventing (Phase 0B)

- **`libs/shared/db.py`** — SQLAlchemy 2.0 `Base` (with a stable naming convention so autogenerate is deterministic) + `TenantMixin` (`org_id`) + `TimestampMixin`.
- **`libs/shared/infra_models.py`** — `Outbox` (BIGSERIAL id, `partition_key`, `schema_subject`, `outbox_idem_uq` unique, partial `outbox_unpublished_idx`) and `Inbox` (composite PK `consumer_group, org_id, idempotency_key`). The Session-1 lib drift was reconciled: `outbox.enqueue` now writes the full canonical column set (payload = the full Avro value `{header, ...}`), and `inbox.record_consumed` writes the composite-PK shape.
- **Models** — `Organization` (id IS org_id), `User` (`clerk_user_id` unique) full; `teams`/`team_members`/`departments`/`tenant_settings`/`data_residency` and `meetings`/`meeting_participants` as P2 stubs.
- **Migration `0001`** (autogenerated, then hand-edited for RLS) — creates all 11 tables, `CREATE EXTENSION pgcrypto`, and **RLS `ENABLE` + `FORCE` + `tenant_isolation`** on every tenant table. `organizations` is keyed on its own `id`; all others on `org_id`. Policies use `current_setting('app.current_org', true)` so an unset GUC yields zero rows (fail-closed), never an error. `alembic check` reports no model/migration drift. **Deviation from the plan's "two migrations": one autogenerated migration is used** so model↔migration names never drift; the split into infra/identity was a separability nicety, not load-bearing.
- **Idempotency** — `libs/shared/idempotency.py` converted to a Starlette `IdempotencyMiddleware`; `libs/shared/app_factory.create_app(service_name)` mounts it (+ RFC7807 handlers, OTel, `/healthz`) and every one of the 9 HTTP service `main.py` now uses it.
- **Relay** — `libs/shared/relay.py` (`relay_once`, `FOR UPDATE SKIP LOCKED`, flush-then-mark = at-least-once) + `services/relay/main.py` (confluent-kafka Avro producer against `{topic}-value`, `acks=all`, idempotent). Run: `make run-relay`.
- **gRPC** — `make proto` regenerates `identity_pb2*.py`; `services/identity_tenant_service/grpc_server.py` implements `GetUserContext` (org_id from **metadata**, missing → `UNAUTHENTICATED`, sets `app.current_org`, returns a typed UserContext). Run: `make run-identity-grpc`. Note: the `proto` Make target is now `.PHONY` (it collided with the `proto/` directory).

### Toolchain notes

- Pinned to a `uv`-managed **Python 3.12** venv (host python is 3.14). Added deps: `confluent-kafka[avro,schemaregistry]` (relay), `svix` (Clerk webhook), `psycopg2-binary` (Alembic sync engine), `fakeredis` (dev, Redis-free unit tests).
- Alembic is invoked with `-c migrations/alembic.ini` (config lives under `migrations/`); added the missing `migrations/script.py.mako` template.

### Verification (Phase 0B/0C green)

`make ci` is green end-to-end: **ruff + black** clean (59 files), **mypy strict** clean (49 source files), **11 pytest** pass — RLS isolation, outbox/inbox idempotency, idempotency-replay, relay real-bus round-trip, gRPC fail-closed, and the Clerk auth suite. Tests run against isolated dev engines (`mytheus-pg-test` on `:5433`, `mytheus-redis-test` on `:6380`) so they never disturb other local projects that hold `:5432`/`:6379`.

Two small typing decisions (documented so they are not "mysterious ignores"):
- **`mypy exclude` adds `tests/` + root `conftest.py`.** Test bodies are not strict-typed (pytest fixtures/monkeypatch are dynamically typed); production code under `libs/` + `services/` remains fully `strict`. `identity_pb2*` and `migrations/` were already excluded.
- **`grpc_server.py`** carries one `# type: ignore[misc]` on `class IdentityService(identity_pb2_grpc.IdentityServiceServicer)` — strict mode forbids subclassing `Any`, and the generated servicer base is `Any` (protobuf descriptor magic mypy cannot see). This is the standard, localized escape hatch for generated gRPC stubs.
- **`idempotency.py`** casts `call_next(...)`'s result to `StreamingResponse` (Starlette's `BaseHTTPMiddleware` always returns one at runtime) so `.body_iterator` is typed, and normalizes `str`/`bytes` chunks before caching.
