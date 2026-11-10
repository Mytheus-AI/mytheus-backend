"""billing-metering-service - usage metering, per-tenant token budgets, quota enforcement (R4).

Owns: plans, subscriptions, usage_events, usage_aggregates, quotas, invoices, agent_budgets.
Budget states: ok -> throttled -> blocked. Quota enforcement never sheds durable facts.

Processes:
- this app: `GET /usage`, `GET /plans`, and `POST /billing/internal/budget` (the agent tier's
  budget read-through, service-authenticated), and `GET /internal/plan-features` (the
  tenant's plan features for ProactiveAgent, service-authenticated, P11)
- `python -m services.billing_metering_service.consumer`: the `usage-events` consumer
- `python -m services.billing_metering_service.jobs`: the nightly derive + rollup

Stripe checkout, the `/stripe/webhook` and dunning land in their own band (Plan 7-9 step 8).
"""

from libs.shared.app_factory import create_app
from services.billing_metering_service import internal, routes

app = create_app("billing-metering-service")
app.include_router(routes.router)
app.include_router(internal.router)
