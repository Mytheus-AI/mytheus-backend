"""Integration service.

Guardrail R8: Calendar + Slack + one of Jira/Linear only.
OAuth tokens are envelope-encrypted at rest (per-tenant KMS data key); never returned by API.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.integration_service.routes import oauth, webhooks

app = FastAPI(title="integration-service", version="1.0.0")
setup_telemetry(app, service_name="integration-service")
add_exception_handlers(app)

app.include_router(oauth.router, prefix="/oauth")
app.include_router(webhooks.router, prefix="/webhooks")
