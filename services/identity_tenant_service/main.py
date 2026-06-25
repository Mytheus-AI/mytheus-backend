"""Identity & tenancy service.

Owns: organizations, users, RBAC grants, SSO config, agent-authz scopes.
Auth provider: Supabase Auth (replaces Clerk).
  - JWT verification: SUPABASE_JWT_SECRET (HS256), org_id from custom claim.
  - User sync: Postgres trigger on auth.users → public.users (immediate);
    Temporal reconciliation job pulls Supabase Admin API hourly (dropped-event safety net).
  - SSO/SAML: stubbed; enable via Supabase SSO settings per tenant (Pro+ plan).
  - SCIM: stubbed endpoints at /scim/v2/*.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.identity_tenant_service.routes import auth, scim

app = FastAPI(title="identity-tenant-service", version="1.0.0")
setup_telemetry(app, service_name="identity-tenant-service")
add_exception_handlers(app)

app.include_router(auth.router)            # POST /supabase/webhook
app.include_router(scim.router, prefix="/scim/v2")  # SCIM 2.0 stubs (Enterprise)

# TODO: add routers for /orgs /rbac /context
