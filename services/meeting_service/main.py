"""meeting-service — LiveKit transport adapter: room-token mint, webhooks, server-side audio hook.

Owns: meeting_rooms, nexus-recordings object bucket. The SDK is behind a transport adapter so
the provider is swappable.
"""

from libs.shared.app_factory import create_app

app = create_app("meeting-service")

# TODO: add routers for /api/meetings/:id/token and /livekit/webhook
