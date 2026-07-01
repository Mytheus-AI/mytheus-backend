"""notification-service — unified delivery fabric (in-app / email / Slack).

Async worker consumes notifications + agent-results; REST API is the durable read/dismiss side.
Sole consumer authorized to fan notifications to external channels.
"""

from libs.shared.app_factory import create_app

app = create_app("notification-service")

# TODO: add router for /notifications (read/dismiss REST surface)
