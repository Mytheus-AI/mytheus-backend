"""Pydantic v2 wire models for meeting-service.

`org_id` appears in no request model, same rule as `core-api`'s (`services/core_api/schemas.
py`). It is derived from the verified token via `RequestContext`, never accepted from a
caller. The token-mint route in fact takes no request body at all — `meeting_id` comes from
the path and everything else from the tenant context — so this module only needs a response
model today; it exists as its own file so a request model added later (the webhook route's
inbound payload is verified/parsed differently and does not belong here) has a home that
does not also import `libs.shared.authz` or FastAPI routing concerns.
"""

from __future__ import annotations

from pydantic import BaseModel


class TokenResponse(BaseModel):
    token: str
    room: str
    livekit_url: str
