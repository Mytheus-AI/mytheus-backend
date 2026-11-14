"""Pydantic v2 wire models for identity-tenant-service.

Mirrors `services/core_api/schemas.py`'s conventions: a mutating response returns the canonical
persisted resource, never a partial echo, and `org_id` appears in no request or response model
here (it is derived from the verified token, and the response body never needs to restate it -
matching `Meeting`/`Team`/`Department` in core-api's schemas).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ADR-0038 D1/D2 limits. `listen_for` / `ignore_topics`: at most 15 entries of 1 to 100 chars
# each, trimmed and de-duplicated (case-insensitively, first spelling kept).
MAX_TOPIC_ITEMS = 15
MAX_TOPIC_CHARS = 100
MAX_AGENT_NAME = 40
MAX_AGENT_INSTRUCTIONS = 500
# `users.display_name`: the person's real name, at most 80 characters (a CHECK enforces it).
MAX_DISPLAY_NAME = 80
# The UI's `isDoodleSeed`.
AGENT_AVATAR_SEED_PATTERN = r"^[a-z0-9]{4,32}$"

AgentPersonality = Literal["balanced", "concise", "detailed", "direct", "friendly"]

_AGENT_FIELDS = frozenset(
    {"agent_name", "agent_avatar_seed", "agent_personality", "agent_instructions"}
)


def _topic_list(raw: list[str] | None) -> list[str] | None:
    if raw is None:
        raise ValueError("cannot be null; send [] to clear")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        value = item.strip()
        if not value:
            raise ValueError("entries must not be empty")
        if len(value) > MAX_TOPIC_CHARS:
            raise ValueError(f"entries must be at most {MAX_TOPIC_CHARS} characters")
        if value.casefold() in seen:
            continue
        seen.add(value.casefold())
        out.append(value)
    if len(out) > MAX_TOPIC_ITEMS:
        raise ValueError(f"at most {MAX_TOPIC_ITEMS} entries")
    return out


def _blank_to_none(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def clerk_display_name(first: object, last: object) -> str | None:
    """Clerk's `first_name` + `last_name` as one name: whitespace collapsed, at most
    `MAX_DISPLAY_NAME` characters, None when both are empty. What the webhook stores."""
    parts = [str(p) for p in (first, last) if isinstance(p, str)]
    joined = " ".join(" ".join(parts).split())
    return joined[:MAX_DISPLAY_NAME].rstrip() or None


class UserProfileUpdate(BaseModel):
    """`PATCH /users/{user_id}` body. `avatar_url` is deliberately absent: it is Clerk-managed
    (set from `image_url` by the `/clerk/webhook` sync in `routes/auth.py`), so a second write
    path for it here would race the webhook rather than complement it. `role` here is the
    DOMAIN role (job title/function) on `users`, never `rbac_grants.tier` - see `libs/shared/
    authz.py`'s module docstring for why the two must never be conflated.

    ADR-0038 adds the working profile (`listen_for`, `ignore_topics`: profile fields, same
    authorization as `role`) and the agent persona (`agent_*`: self only, see the route). An
    empty string for `agent_name` / `agent_avatar_seed` / `agent_instructions` stores NULL.
    Every failure is a 422 whose `errors[].loc` names the field.
    """

    model_config = ConfigDict(extra="forbid")

    role: Annotated[str, Field(min_length=1, max_length=200)] | None = None
    # ADR-0038 D1 describes `skills` as "at most 30" and `bio` as "up to 1000". Those bounds are
    # NOT tightened here: both are narrower than what this route already accepted, which the
    # oasdiff gate rejects (`request-property-max-items-set`, `-max-length-decreased`) and which
    # would 422 an existing client. Onboarding enforces the tighter bounds in the UI.
    skills: list[Annotated[str, Field(min_length=1, max_length=100)]] | None = None
    bio: Annotated[str, Field(max_length=2000)] | None = None
    listen_for: list[str] | None = None
    ignore_topics: list[str] | None = None
    agent_name: str | None = None
    agent_avatar_seed: str | None = None
    agent_personality: AgentPersonality | None = None
    agent_instructions: str | None = None
    # The person's real name. Self only (see the route); an empty string stores NULL.
    display_name: str | None = None

    @field_validator("listen_for", "ignore_topics")
    @classmethod
    def _topics(cls, v: list[str] | None) -> list[str] | None:
        return _topic_list(v)

    @field_validator("agent_name")
    @classmethod
    def _agent_name(cls, v: str | None) -> str | None:
        value = _blank_to_none(v)
        if value is not None and len(value) > MAX_AGENT_NAME:
            raise ValueError(f"must be at most {MAX_AGENT_NAME} characters")
        return value

    @field_validator("display_name")
    @classmethod
    def _display_name(cls, v: str | None) -> str | None:
        value = " ".join(v.split()) if v is not None else None
        value = value or None
        if value is not None and len(value) > MAX_DISPLAY_NAME:
            raise ValueError(f"must be at most {MAX_DISPLAY_NAME} characters")
        return value

    @field_validator("agent_avatar_seed")
    @classmethod
    def _agent_avatar_seed(cls, v: str | None) -> str | None:
        value = _blank_to_none(v)
        if value is not None and not re.fullmatch(AGENT_AVATAR_SEED_PATTERN, value):
            raise ValueError("must be 4 to 32 lowercase letters or digits")
        return value

    @field_validator("agent_personality")
    @classmethod
    def _agent_personality(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("cannot be null")
        return v

    @field_validator("agent_instructions")
    @classmethod
    def _agent_instructions(cls, v: str | None) -> str | None:
        value = _blank_to_none(v)
        if value is not None and len(value) > MAX_AGENT_INSTRUCTIONS:
            raise ValueError(f"must be at most {MAX_AGENT_INSTRUCTIONS} characters")
        return value

    @model_validator(mode="after")
    def _at_least_one_field(self) -> UserProfileUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        return self

    def agent_fields_set(self) -> bool:
        """True if the body touches the persona, which only the person themselves may edit."""
        return bool(self.model_fields_set & _AGENT_FIELDS)


class UserProfile(BaseModel):
    id: uuid.UUID
    email: str
    # The person's real name; null until known (clients fall back to their own label).
    display_name: str | None = None
    role: str | None
    skills: list[str]
    bio: str | None
    avatar_url: str | None
    listen_for: list[str]
    ignore_topics: list[str]
    agent_name: str | None
    agent_avatar_seed: str | None
    agent_personality: AgentPersonality
    agent_instructions: str | None
    created_at: datetime
    updated_at: datetime


class Member(BaseModel):
    """One person in the caller's tenant: who to show in a members list or an invite picker.
    `display_name` is the person's real name as synced from Clerk (or set by the person); null
    until known, in which case clients join on `clerk_user_id` or fall back to the email.
    `agent_name` / `agent_avatar_seed` let a meeting room label everyone's agent (ADR-0038 D3);
    null means the UI's default ("{first name}'s agent", a seed derived from the user id)."""

    id: uuid.UUID
    clerk_user_id: str | None
    email: str
    avatar_url: str | None
    role: str | None
    tier: str | None
    display_name: str | None = None
    agent_name: str | None = None
    agent_avatar_seed: str | None = None


class MemberList(BaseModel):
    items: list[Member]


class TeamRef(BaseModel):
    """A team membership as it appears inside `UserContext.team`. Not the full `Team`
    resource `core-api` serves at `GET /api/v1/teams` - just enough to identify and label it."""

    id: uuid.UUID
    name: str
    department_id: uuid.UUID | None
    # People structure: what the team does. Defaulted so a context cached before the field
    # existed (up to the 300 s TTL after a deploy) still validates.
    description: str | None = None


class DepartmentRef(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None = None


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
    # ADR-0038 D4. Defaulted so a context cached before these fields existed (up to the 300 s
    # TTL after a deploy) still validates instead of failing the read.
    listen_for: list[str] = Field(default_factory=list)
    ignore_topics: list[str] = Field(default_factory=list)
    agent_name: str | None = None
    agent_personality: str = "balanced"
    agent_instructions: str | None = None


# --- /orgs -------------------------------------------------------------------------------

# Clerk's opaque identifiers. Validated for SHAPE only, never parsed: the prefix check exists so
# a caller who pastes an internal `org_id` UUID where Clerk's id belongs gets a 422 instead of
# a tenant that can never be resolved (`libs/shared/org_resolver.py` looks requests up by this
# exact column).
_CLERK_ORG_ID = r"^org_[A-Za-z0-9]{1,64}$"
_CLERK_USER_ID = r"^user_[A-Za-z0-9]{1,64}$"


class OrgOwner(BaseModel):
    """The tenant's first owner. Becomes a `users` mirror row plus an `owner` `rbac_grants`
    row in the provisioning transaction; Clerk's later `user.*` webhook converges the mirror."""

    model_config = ConfigDict(extra="forbid")

    clerk_user_id: Annotated[str, Field(pattern=_CLERK_USER_ID)]
    email: Annotated[str, Field(min_length=3, max_length=320, pattern=r"^[^@\s]+@[^@\s]+$")]


class OrgCreate(BaseModel):
    """`POST /orgs` body. There is no `org_id` field: the id is minted server-side."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=200)]
    slug: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")]
    clerk_org_id: Annotated[str, Field(pattern=_CLERK_ORG_ID)]
    owner: OrgOwner


class Organization(BaseModel):
    """The tenant root, restricted to the columns `mytheus_app` may read at all (migration
    `a1c4e7f20b18` grants SELECT on `id, clerk_org_id, name, slug, status, deleted_at` only)."""

    id: uuid.UUID
    name: str
    slug: str
    status: str
    clerk_org_id: str | None
    # ADR-0048, additive: the workspace logo, from `tenant_settings`. `logo_url` is the
    # versioned, gateway-relative `GET /api/v1/org/logo/image?v={n}` (bearer required), or
    # null when the workspace has no logo.
    logo_url: str | None = None
    logo_updated_at: datetime | None = None


class ProvisionedOrganization(Organization):
    owner_user_id: uuid.UUID


# --- /rbac -------------------------------------------------------------------------------

RbacTier = Literal["owner", "admin", "member"]


class RbacGrantUpdate(BaseModel):
    """`PUT /rbac/grants/{user_id}` body: set this user's tier, creating the grant if absent."""

    model_config = ConfigDict(extra="forbid")

    tier: RbacTier


class RbacGrant(BaseModel):
    user_id: uuid.UUID
    tier: RbacTier
    granted_by: uuid.UUID | None
    created_at: datetime


class RbacGrantPage(BaseModel):
    """Keyset page over `rbac_grants`, ordered by `user_id`. `next_cursor` is opaque."""

    items: list[RbacGrant]
    next_cursor: str | None = None
