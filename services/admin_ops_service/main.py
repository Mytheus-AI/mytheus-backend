"""Admin & ops service.

Two consoles:
  - Tenant admin (/admin/tenant/*): org-scoped self-serve (members, plan, integrations, export).
  - Internal ops (/admin/ops/*): cross-tenant support, feature flags, audit-log access.

audit_logs is append-only (no UPDATE/DELETE). An operation that requires audit
fails closed if the audit write fails.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.admin_ops_service.routes import tenant_admin, ops

app = FastAPI(title="admin-ops-service", version="1.0.0")
setup_telemetry(app, service_name="admin-ops-service")
add_exception_handlers(app)

app.include_router(tenant_admin.router, prefix="/admin/tenant")
app.include_router(ops.router, prefix="/admin/ops")
