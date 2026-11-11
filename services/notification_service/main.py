"""notification-service - unified delivery fabric (in-app / email / Slack).

Owns `notification_log` and `notification_preferences`. Sole consumer authorized to fan
`notifications` out to external channels.

Processes:
- this app: `GET /notifications`, `PATCH /notifications/{id}`, `GET`/`PUT
  /notifications/preferences` (the caller's own rows; `routes.py`)
- `python -m services.notification_service.consumer`: the `agent-results` (ProactiveAgent
  alerts) + `notifications` consumer, group `notification-service`, which writes
  `notification_log`, delivers per channel preference and emits `notification-delivery-events`
  receipts through the outbox
"""

from libs.shared.app_factory import create_app
from services.notification_service import internal, routes, user_email

app = create_app("notification-service")
app.include_router(routes.router)
# `POST /internal/email/send` (core-api caller token): follow-up email to non-user guests.
app.include_router(internal.router)
app.include_router(user_email.router)
