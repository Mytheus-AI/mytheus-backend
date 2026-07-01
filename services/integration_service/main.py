"""integration-service — external integrations (Google Calendar, Slack, Jira/Linear).

Guardrail R8: Calendar + Slack + exactly one PM tool. OAuth tokens are envelope-encrypted at
rest and never returned by API.
"""

from libs.shared.app_factory import create_app

app = create_app("integration-service")

# TODO: add routers for /oauth/{provider} and /webhooks/{provider}
