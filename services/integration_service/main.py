"""integration-service - external integrations (Google Calendar, Slack, Jira). Guardrail R8:
Calendar + Slack + exactly one PM tool (Jira; Linear is not built).

    uvicorn services.integration_service.main:app                 # the API (this module)
    python -m services.integration_service.worker                 # webhook inbox + renewals
    python -m services.integration_service.consumer               # `actions` -> Jira (group
                                                                  # `integration-service`)

OAuth tokens are envelope-encrypted at rest (ADR-0024) and never returned by a public route;
logs are redacted at the logger (`redaction.py`).
"""

from libs.shared.app_factory import create_app
from services.integration_service import internal, routes, webhooks
from services.integration_service.redaction import install_redaction

install_redaction()

app = create_app("integration-service")
app.include_router(routes.router)
app.include_router(webhooks.router)
app.include_router(internal.router)
