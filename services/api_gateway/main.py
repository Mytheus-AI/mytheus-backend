"""api-gateway - the single public ingress.

TLS, WAF and the global rate limit sit in Cloudflare in front of this. What this process owns
is Clerk JWT pre-validation, `org_id` resolution, and routing. Stateless, no data ownership.

FAILS CLOSED: if `org_id` cannot be resolved from the token, the request is refused here and
no tenant-scoped store is touched.

The per-tenant rate limit is deliberately still a stub. The full posture belongs with the WAF
work in `11-security-architecture.md` section 5, and a token-bucket in a single process would
be wrong the moment there are two replicas.
"""

from libs.shared.app_factory import create_app
from services.api_gateway import proxy
from services.api_gateway.edge import edge_auth

app = create_app("api-gateway")
app.middleware("http")(edge_auth)

# Forwarding, mounted last so `/healthz` and the docs routes this process serves itself are
# matched before the catch-all. The services now run as separate containers, so there is
# something to proxy to; see `proxy.py` for what the hop preserves and why.
proxy.install(app)
