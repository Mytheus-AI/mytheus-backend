"""Billing & metering service.

Owns: plans, subscriptions, usage_events, usage_aggregates, quotas, invoices, agent_budgets.
Redis hot mirror: nexus:{org_id}:budget:{period} for fast quota checks.
Enforces R4: token budgets and hard quotas (ok → throttled → blocked).
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.billing_metering_service.routes import billing, usage, stripe_webhook

app = FastAPI(title="billing-metering-service", version="1.0.0")
setup_telemetry(app, service_name="billing-metering-service")
add_exception_handlers(app)

app.include_router(billing.router, prefix="/billing")
app.include_router(usage.router, prefix="/usage")
app.include_router(stripe_webhook.router)
