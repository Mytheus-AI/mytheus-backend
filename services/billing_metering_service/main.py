"""Billing & metering service.

Owns: plans, subscriptions, usage_events, usage_aggregates, quotas, invoices, agent_budgets.
Redis hot mirror: nexus:{org_id}:budget:{period} for fast quota checks.
Enforces R4: token budgets and hard quotas (ok → throttled → blocked).
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
app = FastAPI(title="billing-metering-service", version="1.0.0")
setup_telemetry(app, service_name="billing-metering-service")
add_exception_handlers(app)

# TODO: add routers for /billing /usage /plans /stripe/webhook
