"""identity-tenant-service — identity & tenancy.

Owns: organizations, users, teams/departments, RBAC grants, SSO/SAML/SCIM, tenant_settings,
data_residency. Auth provider: Clerk (JWKS verify + Svix user-sync webhook). Hosts
get_user_context (gRPC GetUserContext, org_id in metadata).
"""

from libs.shared.app_factory import create_app
from services.identity_tenant_service.routes import auth, context, scim, users

app = create_app("identity-tenant-service")
app.include_router(auth.router)  # POST /clerk/webhook (Svix-verified)
app.include_router(scim.router, prefix="/scim/v2")  # SCIM 2.0 stubs (Enterprise)
app.include_router(users.router)  # PATCH /users/{user_id} (role/skills/bio)
app.include_router(context.router)  # GET /context/{user_id} (get_user_context, cached)

# TODO: add routers for /orgs /rbac
