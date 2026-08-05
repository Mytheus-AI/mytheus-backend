"""meeting-service — LiveKit transport adapter: room-token mint, webhooks, server-side audio hook.

Owns: meeting_rooms, nexus-recordings object bucket. The SDK is behind a transport adapter so
the provider is swappable.
"""

from libs.shared.app_factory import create_app
from services.meeting_service.routes import router as token_router
from services.meeting_service.webhook import router as webhook_router

app = create_app("meeting-service")
app.include_router(token_router)  # POST /api/meetings/:id/token
app.include_router(webhook_router)  # POST /livekit/webhook

# The `meeting-events` consumer (services/meeting_service/consumer.py) runs as its own worker
# process (`python -m services.meeting_service.consumer`), not inside this FastAPI app - same
# convention as `services/core_api/consumer.py`.
