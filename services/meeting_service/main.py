"""meeting-service — LiveKit transport adapter: room-token mint, webhooks, server-side audio hook.

Owns: meeting_rooms, nexus-recordings object bucket. The SDK is behind a transport adapter so
the provider is swappable.
"""

from libs.shared.app_factory import create_app
from services.meeting_service.routes import router as token_router

app = create_app("meeting-service")
app.include_router(token_router)  # POST /api/meetings/:id/token

# TODO: add the /livekit/webhook router (Task 4)
