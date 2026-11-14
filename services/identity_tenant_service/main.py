"""identity-tenant-service — identity & tenancy.

Owns: organizations, users, teams/departments, RBAC grants, SSO/SAML/SCIM, tenant_settings,
data_residency. Auth provider: Clerk (JWKS verify + Svix user-sync webhook). Hosts
get_user_context (gRPC GetUserContext, org_id in metadata).
"""

from libs.shared.app_factory import create_app
from services.identity_tenant_service.a2a import routes as a2a_routes
from services.identity_tenant_service.brain import routes as brain_routes
from services.identity_tenant_service.branding import routes as branding_routes
from services.identity_tenant_service.oauth import app_routes as oauth_app_routes
from services.identity_tenant_service.oauth import public_routes as oauth_public_routes
from services.identity_tenant_service.pii_vault import routes as pii_vault_routes
from services.identity_tenant_service.routes import (
    api_tokens,
    auth,
    context,
    internal_status,
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
# GET/POST /api/v1/agent-authz-scopes, DELETE /api/v1/agent-authz-scopes/{id},
# POST /api/v1/agent-authz-scopes/template (A2A grants, ADR-0013, owner/admin only).
app.include_router(a2a_routes.router)
# POST /internal/tenant-status (INTERNAL, caller admin-ops-service; suspend/reinstate, P12).
app.include_router(internal_status.router)
# The company brain (ADR-0039 Part A): /api/v1/org/{profile,glossary,topic-owners,goals,
# ground-rules,readiness,welcome} and /api/v1/users/me/topic-watches.
app.include_router(brain_routes.router)
# The workspace logo (ADR-0047): GET/PUT/DELETE /api/v1/org/logo, GET /api/v1/org/logo/image.
app.include_router(branding_routes.router)
app.include_router(api_tokens.router)  # /api/v1/api-tokens (personal access tokens, Phase 4)
# OAuth for the MCP server (Claude custom connectors): the authorization server. Public protocol
# endpoints `/.well-known/oauth-authorization-server`, `/oauth/{register,authorize,token,revoke}`;
# signed-in `/api/v1/oauth/consent` (the consent page) and `/api/v1/oauth/grants` (Connected apps).
app.include_router(oauth_public_routes.router)
app.include_router(oauth_app_routes.router)
