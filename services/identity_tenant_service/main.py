"""identity-tenant-service — identity & tenancy.

Owns: organizations, users, teams/departments, RBAC grants, SSO/SAML/SCIM, tenant_settings,
data_residency. Auth provider: Clerk (JWKS verify + Svix user-sync webhook). Hosts
get_user_context (gRPC GetUserContext, org_id in metadata).
"""

from libs.shared.app_factory import create_app
from services.identity_tenant_service.routes import (
    auth,
    context,
    pii_tokens,
    scim,
    taxonomy,
    tenant_settings,
    users,
)

app = create_app("identity-tenant-service")
app.include_router(auth.router)  # POST /clerk/webhook (Svix-verified)
app.include_router(scim.router, prefix="/scim/v2")  # SCIM 2.0 stubs (Enterprise)
app.include_router(users.router)  # PATCH /users/{user_id} (role/skills/bio)
app.include_router(context.router)  # GET /context/{user_id} (get_user_context, cached)
app.include_router(taxonomy.router)  # GET /taxonomy/role-topics (role -> topics, versioned)
app.include_router(tenant_settings.router)  # GET/PATCH /tenant-settings (pii_redaction_enabled)

# Mounted, not include_router'd, so the PII vault's mint/resolve surface stays out of this
# service's emitted OpenAPI document (ADR-0022 gates that document with `spectral lint`, and an
# internal route in the public contract is how a client SDK eventually grows a method for it).
# Internal-only: not exposed through `api-gateway`, authenticated as a workload rather than a
# user (`libs/shared/internal_auth.py`), and called by the three redaction boundaries per
# ADR-0020 D2.
app.mount("/internal", pii_tokens.create_pii_tokens_app())

# TODO: add routers for /orgs /rbac
