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


class TeamRef(BaseModel):
    """A team membership as it appears inside `UserContext.team`. Not the full `Team`
    resource `core-api` serves at `GET /api/v1/teams` - just enough to identify and label it."""

    id: uuid.UUID
    name: str
    department_id: uuid.UUID | None


class DepartmentRef(BaseModel):
    id: uuid.UUID
    name: str


class MeetingRef(BaseModel):
    """A recent meeting as it appears inside `UserContext.recent_meetings` - same
    "just enough to identify and label it" scope as `TeamRef`."""

    id: uuid.UUID
    title: str | None
    status: str
    scheduled_start: datetime | None
    scheduled_end: datetime | None


class RoleTopicsResponse(BaseModel):
    """`GET /taxonomy/role-topics` response - the full role->topics map for one pinned
    taxonomy version (Plan 5-6 section 3.2). Not tenant-scoped: the taxonomy is identical for
    every org, so unlike every other resource in this service the response carries no `org_id`
    and reading it needs no `app.current_org` - it is versioned config, not tenant data."""

    version: str
    roles: dict[str, list[str]]


class UserContext(BaseModel):
    """`GET /context/{user_id}` response - the canonical `get_user_context()` shape (Plan 5-6
    section 3.3), cached at `nexus:{org_id}:user_ctx:{user_id}`. Field-identical to the dict
    `context.service.get_user_context` returns and to what is stored in the cache, so this
    model is also what validates a value read back from Redis.
    """

    user_id: uuid.UUID
    org_id: uuid.UUID
    role: str | None
    skills: list[str]
    bio: str | None
    avatar_url: str | None
    team: list[TeamRef]
    department: DepartmentRef | None
    recent_meetings: list[MeetingRef]
    topics: list[str]
