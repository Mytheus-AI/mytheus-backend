"""Pydantic v2 wire models for identity-tenant-service.

Mirrors `services/core_api/schemas.py`'s conventions: a mutating response returns the canonical
persisted resource, never a partial echo, and `org_id` appears in no request or response model
here (it is derived from the verified token, and the response body never needs to restate it -
matching `Meeting`/`Team`/`Department` in core-api's schemas).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator


class UserProfileUpdate(BaseModel):
    """`PATCH /users/{user_id}` body. `avatar_url` is deliberately absent: it is Clerk-managed
    (set from `image_url` by the `/clerk/webhook` sync in `routes/auth.py`), so a second write
    path for it here would race the webhook rather than complement it. `role` here is the
    DOMAIN role (job title/function) on `users`, never `rbac_grants.tier` - see `libs/shared/
    authz.py`'s module docstring for why the two must never be conflated.
    """

    model_config = ConfigDict(extra="forbid")

    role: Annotated[str, Field(min_length=1, max_length=200)] | None = None
    skills: list[Annotated[str, Field(min_length=1, max_length=100)]] | None = None
    bio: Annotated[str, Field(max_length=2000)] | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> UserProfileUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        return self


class UserProfile(BaseModel):
    id: uuid.UUID
    email: str
    role: str | None
    skills: list[str]
    bio: str | None
    avatar_url: str | None
    created_at: datetime
    updated_at: datetime
