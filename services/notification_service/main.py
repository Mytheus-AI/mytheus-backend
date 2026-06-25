"""Notification service — unified delivery fabric.

Async worker: consumes notifications + agent-results topics.
REST API: read/dismiss notifications (the durable read side of the realtime alert stream).
Sole consumer authorized to fan notifications to external channels (email, Slack DM).
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
app = FastAPI(title="notification-service", version="1.0.0")
setup_telemetry(app, service_name="notification-service")
add_exception_handlers(app)

# TODO: add router for /notifications (read/dismiss REST surface)
