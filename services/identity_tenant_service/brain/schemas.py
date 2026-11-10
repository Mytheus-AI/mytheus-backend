"""Wire models for the company brain (ADR-0039 Part A).

Every limit the ADR fixes is enforced here, so a violation is a 422 whose `errors[].loc` names
the field. Strings are trimmed; a blank optional string stores NULL. Lists are trimmed and
de-duplicated case-insensitively (first spelling kept) before they are counted, the same rule
`listen_for` uses (ADR-0038). The migration (`c1a2b3d4e5f6`) mirrors each limit as a CHECK.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_DESCRIPTION = 2000
MAX_INDUSTRY = 100
MAX_PRODUCTS = 30
MAX_CUSTOMERS = 50
MAX_ITEM = 80  # products, customers, aliases, topics, terms
MAX_DEFINITION = 300
MAX_ALIASES = 10
MAX_GLOSSARY_TERMS = 500
MAX_IMPORT_TERMS = 200
MAX_TOPIC_OWNERS = 200
MAX_GOAL_TITLE = 200
MAX_GOAL_DESCRIPTION = 1000
MAX_GOAL_PERIOD = 20
MAX_ACTIVE_GOALS = 30
MAX_NEVER_RECORD = 30
MAX_TOPIC_WATCHES = 20

GlossaryKind = Literal["product", "codename", "acronym", "customer", "person", "other"]
GoalStatus = Literal["active", "achieved", "dropped"]


def clean_list(raw: list[str] | None, *, max_items: int, max_len: int = MAX_ITEM) -> list[str]:
    if raw is None:
        raise ValueError("cannot be null; send [] to clear")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        value = item.strip()
        if not value:
            raise ValueError("entries must not be empty")
        if len(value) > max_len:
            raise ValueError(f"entries must be at most {max_len} characters")
        if value.casefold() in seen:
            continue
        seen.add(value.casefold())
        out.append(value)
    if len(out) > max_items:
        raise ValueError(f"at most {max_items} entries")
    return out


def required_text(raw: str | None, *, max_len: int) -> str:
    if raw is None:
        raise ValueError("cannot be null")
    value = raw.strip()
    if not value:
        raise ValueError("must not be empty")
    if len(value) > max_len:
        raise ValueError(f"must be at most {max_len} characters")
    return value


def optional_text(raw: str | None, *, max_len: int) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    if len(value) > max_len:
        raise ValueError(f"must be at most {max_len} characters")
    return value or None


def _at_least_one(model: BaseModel) -> None:
    if not model.model_fields_set:
        raise ValueError("PATCH requires at least one field")


# --- A1 company profile ----------------------------------------------------------------------


class OrgProfile(BaseModel):
    """`updated_by` / `updated_at` are null for an org that has never saved a profile."""

    description: str
    industry: str
    products: list[str]
    customers: list[str]
    updated_by: uuid.UUID | None
    updated_at: datetime | None


class OrgProfileUpdate(BaseModel):
    """`PUT /api/v1/org/profile`: replaces the whole profile. An omitted field is cleared."""

    model_config = ConfigDict(extra="forbid")

    description: str = ""
    industry: str = ""
    products: list[str] = Field(default_factory=list)
    customers: list[str] = Field(default_factory=list)

    @field_validator("description")
    @classmethod
    def _description(cls, v: str | None) -> str:
        return optional_text(v, max_len=MAX_DESCRIPTION) or ""

    @field_validator("industry")
    @classmethod
    def _industry(cls, v: str | None) -> str:
        return optional_text(v, max_len=MAX_INDUSTRY) or ""

    @field_validator("products")
    @classmethod
    def _products(cls, v: list[str] | None) -> list[str]:
        return clean_list(v, max_items=MAX_PRODUCTS)

    @field_validator("customers")
    @classmethod
    def _customers(cls, v: list[str] | None) -> list[str]:
        return clean_list(v, max_items=MAX_CUSTOMERS)


# --- A2 vocabulary ---------------------------------------------------------------------------


class GlossaryTerm(BaseModel):
    id: uuid.UUID
    term: str
    kind: GlossaryKind
    definition: str | None
    aliases: list[str]
    created_at: datetime
    updated_at: datetime


class GlossaryTermList(BaseModel):
    items: list[GlossaryTerm]


class GlossaryTermCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    term: str
    kind: GlossaryKind = "other"
    definition: str | None = None
    aliases: list[str] = Field(default_factory=list)

    @field_validator("term")
    @classmethod
    def _term(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_ITEM)

    @field_validator("definition")
    @classmethod
    def _definition(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_DEFINITION)

    @field_validator("aliases")
    @classmethod
    def _aliases(cls, v: list[str] | None) -> list[str]:
        return clean_list(v, max_items=MAX_ALIASES)


class GlossaryTermUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    term: str | None = None
    kind: GlossaryKind | None = None
    definition: str | None = None
    aliases: list[str] | None = None

    @field_validator("term")
    @classmethod
    def _term(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_ITEM)

    @field_validator("kind")
    @classmethod
    def _kind(cls, v: str | None) -> str:
        if v is None:
            raise ValueError("cannot be null")
        return v

    @field_validator("definition")
    @classmethod
    def _definition(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_DEFINITION)

    @field_validator("aliases")
    @classmethod
    def _aliases(cls, v: list[str] | None) -> list[str]:
        return clean_list(v, max_items=MAX_ALIASES)

    @model_validator(mode="after")
    def _non_empty(self) -> GlossaryTermUpdate:
        _at_least_one(self)
        return self


class GlossaryImport(BaseModel):
    """`POST /api/v1/org/glossary/import`: up to 200 terms, upserted by `lower(term)`. A term
    already in the glossary takes the imported spelling, kind, definition and aliases. When the
    same term appears twice in one import, the later entry wins."""

    model_config = ConfigDict(extra="forbid")

    terms: list[GlossaryTermCreate] = Field(min_length=1, max_length=MAX_IMPORT_TERMS)


class GlossaryImportResult(BaseModel):
    created: int
    updated: int
    items: list[GlossaryTerm]


# --- A3 who owns what ------------------------------------------------------------------------


class TopicOwner(BaseModel):
    id: uuid.UUID
    topic: str
    owner_user_id: uuid.UUID
    created_at: datetime


class TopicOwnerList(BaseModel):
    items: list[TopicOwner]


class TopicOwnerCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str
    owner_user_id: uuid.UUID

    @field_validator("topic")
    @classmethod
    def _topic(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_ITEM)


class TopicOwnerUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str | None = None
    owner_user_id: uuid.UUID | None = None

    @field_validator("topic")
    @classmethod
    def _topic(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_ITEM)

    @field_validator("owner_user_id")
    @classmethod
    def _owner(cls, v: uuid.UUID | None) -> uuid.UUID:
        if v is None:
            raise ValueError("cannot be null")
        return v

    @model_validator(mode="after")
    def _non_empty(self) -> TopicOwnerUpdate:
        _at_least_one(self)
        return self


# --- A4 goals --------------------------------------------------------------------------------


class OrgGoal(BaseModel):
    id: uuid.UUID
    title: str
    description: str | None
    period: str | None
    status: GoalStatus
    owner_user_id: uuid.UUID | None
    position: int
    created_at: datetime
    updated_at: datetime


class OrgGoalList(BaseModel):
    items: list[OrgGoal]


class OrgGoalCreate(BaseModel):
    """`position` defaults to after the last goal."""

    model_config = ConfigDict(extra="forbid")

    title: str
    description: str | None = None
    period: str | None = None
    status: GoalStatus = "active"
    owner_user_id: uuid.UUID | None = None
    position: int | None = Field(default=None, ge=0)

    @field_validator("title")
    @classmethod
    def _title(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_GOAL_TITLE)

    @field_validator("description")
    @classmethod
    def _description(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_GOAL_DESCRIPTION)

    @field_validator("period")
    @classmethod
    def _period(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_GOAL_PERIOD)


class OrgGoalUpdate(BaseModel):
    """Any subset. `description`, `period` and `owner_user_id` accept null to clear."""

    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    description: str | None = None
    period: str | None = None
    status: GoalStatus | None = None
    owner_user_id: uuid.UUID | None = None
    position: int | None = Field(default=None, ge=0)

    @field_validator("title")
    @classmethod
    def _title(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_GOAL_TITLE)

    @field_validator("description")
    @classmethod
    def _description(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_GOAL_DESCRIPTION)

    @field_validator("period")
    @classmethod
    def _period(cls, v: str | None) -> str | None:
        return optional_text(v, max_len=MAX_GOAL_PERIOD)

    @field_validator("status", "position")
    @classmethod
    def _not_null(cls, v: object) -> object:
        if v is None:
            raise ValueError("cannot be null")
        return v

    @model_validator(mode="after")
    def _non_empty(self) -> OrgGoalUpdate:
        _at_least_one(self)
        return self


# --- A5 ground rules -------------------------------------------------------------------------


class GroundRules(BaseModel):
    never_record_topics: list[str]


class GroundRulesUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    never_record_topics: list[str]

    @field_validator("never_record_topics")
    @classmethod
    def _topics(cls, v: list[str] | None) -> list[str]:
        return clean_list(v, max_items=MAX_NEVER_RECORD)


# --- A6 topic watches ------------------------------------------------------------------------


class TopicWatch(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    topic: str
    created_at: datetime


class TopicWatchList(BaseModel):
    items: list[TopicWatch]


class TopicWatchCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str

    @field_validator("topic")
    @classmethod
    def _topic(cls, v: str | None) -> str:
        return required_text(v, max_len=MAX_ITEM)


# --- A7 readiness ----------------------------------------------------------------------------

ReadinessKey = Literal["profile", "vocabulary", "owners", "goals", "ground_rules", "roles"]


class ReadinessCheck(BaseModel):
    key: ReadinessKey
    label: str
    done: bool
    detail: str


class Readiness(BaseModel):
    score: int = Field(ge=0, le=100)
    checks: list[ReadinessCheck]


# --- A8 welcome ------------------------------------------------------------------------------


class WelcomeOwner(BaseModel):
    topic: str
    owner_user_id: uuid.UUID


class TeamMate(BaseModel):
    user_id: uuid.UUID


class Welcome(BaseModel):
    profile: OrgProfile
    goals: list[OrgGoal]
    owners: list[WelcomeOwner]
    glossary: list[GlossaryTerm]
    team: list[TeamMate]
