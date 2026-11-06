"""identity-tenant-service — identity & tenancy.

Owns: organizations, users, teams/departments, RBAC grants, SSO/SAML/SCIM, tenant_settings,
data_residency. Auth provider: Clerk (JWKS verify + Svix user-sync webhook). Hosts
get_user_context (gRPC GetUserContext, org_id in metadata).
"""

from libs.shared.app_factory import create_app
from services.identity_tenant_service.pii_vault import routes as pii_vault_routes
from services.identity_tenant_service.routes import (
    auth,
    context,
    orgs,
    rbac,
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
app.include_router(orgs.router)  # GET /orgs (own tenant), POST /orgs (platform-operator provision)
app.include_router(rbac.router)  # GET /rbac/grants, PUT/DELETE /rbac/grants/{user_id}
# POST /pii-tokens/mint (INTERNAL, per-caller service secret; never routed by api-gateway) and
# POST /pii-tokens/resolve (Clerk JWT, `admin` tier, audited).
app.include_router(pii_vault_routes.router)
