"""api-gateway — single public ingress: TLS, WAF, per-tenant rate limit, Clerk JWT pre-validate,
org_id extraction, routing. Stateless, no data ownership. FAILS CLOSED: if org_id cannot be
resolved from the JWT, no request reaches a tenant-scoped store.
"""

from libs.shared.app_factory import create_app

app = create_app("api-gateway")

# TODO: add JWT pre-validation middleware, rate limiting, reverse-proxy routing (Plan 1-2)
