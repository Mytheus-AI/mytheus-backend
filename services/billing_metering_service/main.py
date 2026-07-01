"""billing-metering-service — Stripe billing, usage metering, quota enforcement (R4).

Owns: plans, subscriptions, usage_events, usage_aggregates, quotas, invoices, agent_budgets.
Budget states: ok -> throttled -> blocked. Quota enforcement never sheds durable facts.
"""

from libs.shared.app_factory import create_app

app = create_app("billing-metering-service")

# TODO: add routers for /billing /usage /plans /stripe/webhook
