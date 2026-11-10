"""admin-ops-service — tenant-admin (/admin/tenant/*) and internal-ops (/admin/ops/*) consoles.

audit_logs is append-only (no UPDATE/DELETE). An operation requiring audit fails closed if the
audit write fails. Owns `feature_flags`, `ops_audit_log` and `support_grants` (P12) and produces
`admin-events` through the outbox.
"""

from libs.shared.app_factory import create_app
from services.admin_ops_service import ops_routes, tenant_routes

app = create_app("admin-ops-service")
app.include_router(tenant_routes.router)  # /admin/tenant/* (owner/admin, audited)
app.include_router(ops_routes.router)  # /admin/ops/* (Mytheus staff only, ops_audit_log)
