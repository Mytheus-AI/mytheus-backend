# Session Summary

## What was done

### 1. Replaced Clerk Auth with Supabase Auth

**Why this decision was taken:**
The project already uses Supabase Postgres. Using Supabase Auth eliminates a separate third-party vendor dependency (Clerk), reduces the credential surface, and allows auth + database to share a single Supabase project. The existing Clerk decision was recorded in ADR-0010 (`reprise-docs/adr/0010-auth-clerk-enterprise-sso.md`); this change supersedes that ADR for the identity provider only — the surrounding architecture (JWT-in-bearer, org_id in every hop, RBAC tiers, SCIM stubs) is preserved.

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

### 2. Created the project skeleton

**Why this structure:**
The structure follows exactly what is specified in:
- `reprise-backend/CLAUDE.md` (run commands, `services.core_api.main:app` module paths)
- `reprise-docs/_canon/01-service-catalog.md` (canonical service names, API path prefixes)
- `reprise-docs/_canon/02-data-model.md` (table ownership, tenant key patterns)
- `reprise-docs/_canon/03-event-taxonomy.md` (topic names, producer/consumer mapping)

**What was created:**

```
reprise-backend/
├── pyproject.toml              # uv workspace, all deps declared
├── docker-compose.yml          # Postgres (supabase/postgres), Redis, Redpanda, Temporal
├── .env.example                # Supabase + all other env vars documented
├── Makefile                    # dev shortcuts (make dev, make test, make run-core-api, etc.)
├── conftest.py                 # pytest fixtures (testcontainers Postgres + Redis)
├── libs/
│   └── shared/
│       ├── auth.py             # Supabase JWT verify + org_id/user_id extraction
│       ├── tenancy.py          # org_id context var + set_rls() (transaction-local GUC)
│       ├── outbox.py           # transactional outbox enqueue helper
│       ├── inbox.py            # at-least-once dedupe (Redis + inbox table)
│       ├── events.py           # EventEnvelope dataclass (org_id partition key)
│       ├── idempotency.py      # Idempotency-Key middleware for mutating POSTs
│       ├── errors.py           # RFC 7807 ProblemDetail exception + handlers
│       └── telemetry.py        # OpenTelemetry setup (OTLP exporter, FastAPI instrumentation)
├── services/
│   ├── api_gateway/
│   │   └── nginx.conf          # upstream routing, JWT pre-validation placeholder
│   ├── bff/                    # FastAPI BFF — /bff/* screen-shaped reads
│   ├── identity_tenant_service/ # Supabase webhook, orgs, rbac, scim stubs, context
│   ├── core_api/               # meetings, decisions, actions, worklogs, chat, teams, depts
│   ├── meeting_service/        # LiveKit token mint, webhook receiver
│   ├── integration_service/    # OAuth callbacks, provider webhook receivers
│   ├── billing_metering_service/ # Stripe billing, usage, quota
│   ├── admin_ops_service/      # tenant admin + internal ops consoles
│   ├── notification_service/   # REST + Kafka consumer
│   └── workflow_service/       # Temporal worker + 4 workflow stubs
├── proto/
│   └── identity.proto          # GetUserContext gRPC definition
└── migrations/
    ├── alembic.ini
    ├── env.py
    ├── versions/               # empty; first migration: uv run alembic revision --autogenerate
    └── sql/
        ├── custom_claims_hook.sql  # Supabase custom JWT org_id claim hook
        ├── user_sync_trigger.sql   # auth.users → public.users trigger
        └── rls_policies.sql        # canonical RLS template + outbox/inbox tables
```

**Service module naming:** All directories use underscores (`core_api`, `identity_tenant_service`) so they are valid Python package names and match the run commands in CLAUDE.md (`services.core_api.main:app`).

**Invariants preserved from CLAUDE.md:**
- Every route uses `Depends(verify_token)` → `org_id` from JWT, never from path/query.
- Every mutating POST has an `Idempotency-Key` header parameter.
- `confirm_decision` / `confirm_action` are the ONLY routes that will emit to `decisions`/`actions` topics (via outbox — not yet wired, marked TODO).
- `set_rls()` always uses `transaction_local=True` — enforced in the shared helper.
- Cross-tenant access returns `404` (implementation detail for when routes are filled in).
- SCIM routes stubbed, not dropped — ready for Supabase SSO/SAML (Pro+ plan) activation.

---

## What still needs to be done (next sessions)

1. **SQLAlchemy models** — Add ORM models per service, update `migrations/env.py` imports, run first Alembic migration.
2. **Route implementations** — Fill in `raise NotImplementedError` bodies with real DB queries using `set_rls()` + SQLAlchemy.
3. **Outbox relay** — A background task or Temporal activity that reads pending `outbox` rows and publishes to Redpanda.
4. **Kafka consumers** — Wire `notification_service/consumer.py` and add consumers for `core_api` (agent-results, decisions, actions) and other services.
5. **gRPC server** — Implement `GetUserContext` in `identity_tenant_service` and generate stubs with `make proto`.
6. **Temporal reconciliation** — Add Temporal scheduled workflow that calls Supabase Admin API (`GET /auth/v1/admin/users`) to diff and reconcile `public.users` hourly.
7. **Supabase hook activation** — Run `migrations/sql/custom_claims_hook.sql` against the Supabase project and enable the hook in the Dashboard (Auth → Hooks).
8. **Tests** — Write RLS isolation tests (tenant A cannot read tenant B; no `app.current_org` → zero rows), outbox/inbox idempotency tests, Avro round-trip tests.
