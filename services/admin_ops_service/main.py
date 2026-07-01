"""admin-ops-service — tenant-admin (/admin/tenant/*) and internal-ops (/admin/ops/*) consoles.

audit_logs is append-only (no UPDATE/DELETE). An operation requiring audit fails closed if the
audit write fails.
"""

from libs.shared.app_factory import create_app

app = create_app("admin-ops-service")

# TODO: add routers for /admin/tenant/* and /admin/ops/*
