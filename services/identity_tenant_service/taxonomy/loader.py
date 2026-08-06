"""The role -> topics taxonomy: loader, validator, and `topics_for()`. Plan 5-6 section 3.2.

WHAT "VERSION CONTROLLED" MEANS HERE, CONCRETELY (the plan says the taxonomy must be version
controlled; this is the decision, not left implicit):

1. Each version is its own git-tracked file, `role_topics.{version}.yaml`, reviewable in a PR
   like any other code change. A version is never edited in place once it exists - a change to
   what `engineer` maps to is a NEW file (`role_topics.v2.yaml`) plus a new
   `ROLE_TOPICS_TAXONOMY_VERSION` value, not a diff to `v1`'s file. This is what makes a cached
   or logged `topics_version` (see below) meaningful: it names an immutable, inspectable
   artifact, not a moving target.
2. The active version is `ROLE_TOPICS_TAXONOMY_VERSION` (env, default `"v1"`), pinned
   per-process the same way `REDIS_URL`/`DATABASE_URL` are - an operational config value, not
   a per-request or per-tenant choice. Changing it is a deploy, not a runtime toggle.
3. `GET /taxonomy/role-topics?version=v1` (routes/taxonomy.py) can always be asked for a
   version explicitly, so a caller need not guess which one is currently active, and an
   unknown version 404s rather than silently falling back to another one.

THE CACHE-STALENESS QUESTION THE BRIEF ASKED TO BE SETTLED, NOT LEFT IMPLICIT:

`get_user_context()` caches its result for 300s at `nexus:{org_id}:user_ctx:{user_id}`
(context/service.py). A taxonomy version bump does not reach an already-cached entry until
that entry's TTL expires, so for up to 5 minutes after a deploy some callers can see the OLD
version's topics for a user whose cache is still warm.

DECISION: this is acceptable, and the cache key/payload are deliberately left unchanged (no
`topics_version` segment added to the Redis key, no version field added to the cached JSON).
Reasons:

- A taxonomy version bump is a deploy-time event (point 2 above) - it requires a code/config
  change and a restart, exactly like any other change to what `_compute()` in
  `context/service.py` returns (e.g. a changed SQL query, a new field). No other change to
  `_compute()`'s output already gets its own cache-key segment or an explicit "you're reading a
  stale entry" signal, and there is no existing convention anywhere in this codebase for
  busting `nexus:*` caches on deploy. Singling out the taxonomy for that treatment would be new
  machinery for one input among several that already share the same bounded staleness.
- The staleness window is bounded and small (<= 300s) and self-heals with no operator action -
  the same property `context/service.py` already accepts, undisclosed-as-a-problem, for stale
  team membership (invalidate_user_context is unwired for team_members writes - no route
  writes that table yet) and for the invalidate-before-commit race on `PATCH /users/{id}`. This
  is the same class of tradeoff, not a new one.
- Embedding the version in the cache KEY would change `get_user_context()`'s behaviour beyond
  swapping the topics source - out of this task's scope, and it would break the already-green,
  already-verified tests in `tests/test_user_context.py` that construct
  `redis_key(org_id, "user_ctx", user_id)` directly (`test_ttl_on_key_is_at_most_300_seconds`,
  `test_redis_key_never_shared_between_two_orgs_for_the_same_user_id_value`).
- If a future change ever needs a ZERO-staleness cutover (e.g. a topic renamed for a legal
  reason and every cached context must reflect it immediately), the correct fix is a targeted,
  one-off bulk invalidation (SCAN `nexus:{org_id}:user_ctx:*` and DEL, or loop
  `invalidate_user_context` per user) run as part of that deploy - a rare operational action,
  not a cost every cache read should pay for an event that, by design, only a code change can
  trigger in the first place.

If this reasoning is ever revisited, the two concrete levers are: (a) fold `active_version()`
into the Redis key passed to `redis_key(...)` in `context/service.py`, or (b) store
`topics_version` inside the cached JSON and compare it against `active_version()` on read,
treating a mismatch as a miss. Either is a small, isolated change; neither is done here.
"""

from __future__ import annotations

import os
from functools import cache
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict

_TAXONOMY_DIR = Path(__file__).parent
_DEFAULT_TOPICS: list[str] = ["general"]


class RoleTopicsTaxonomy(BaseModel):
    """One pinned, Pydantic-validated version of the role -> topics map, as parsed from
    `role_topics.{version}.yaml`. `extra="forbid"` so a typo'd top-level YAML key (e.g.
    `verison:`) fails loudly at load time instead of being silently ignored."""

    model_config = ConfigDict(extra="forbid")

    version: str
    roles: dict[str, list[str]]


class TaxonomyVersionNotFound(Exception):
    """No `role_topics.{version}.yaml` exists for this version (or its own `version:` field
    disagrees with the filename it was loaded from - the same "no silent fallback" rule, just
    catching the case where the artifact itself is inconsistent rather than absent).

    Deliberately NOT raised for a version that exists but fails Pydantic validation - that is a
    different failure (the artifact is broken, not merely unregistered) and is left to surface
    as a `pydantic.ValidationError`, an internal error, not a 404. Conflating "this version does
    not exist" with "this version exists but is malformed" would misreport a deployment bug as
    a routine "try a different version" client error.
    """


def active_version() -> str:
    """The taxonomy version this process resolves `topics_for()` against, absent an explicit
    override. Read fresh from the environment on every call (unlike `_load` below, which is
    cached) so tests - and, in principle, an ops override - can change
    `ROLE_TOPICS_TAXONOMY_VERSION` without needing to clear a cache first."""
    return os.environ.get("ROLE_TOPICS_TAXONOMY_VERSION", "v1")


@cache
def _load(version: str) -> RoleTopicsTaxonomy:
    """Parse and validate `role_topics.{version}.yaml`. Cached per version: the artifact is
    immutable once published (see module docstring, point 1), so re-parsing it on every call
    would be pure overhead - the same reasoning `libs/shared/cache.get_redis` uses to justify
    caching something env cannot change mid-process."""
    path = _TAXONOMY_DIR / f"role_topics.{version}.yaml"
    if not path.is_file():
        raise TaxonomyVersionNotFound(version)
    raw = yaml.safe_load(path.read_text())
    taxonomy = RoleTopicsTaxonomy.model_validate(raw)
    if taxonomy.version != version:
        raise TaxonomyVersionNotFound(version)
    return taxonomy


def load_taxonomy(version: str | None = None) -> RoleTopicsTaxonomy:
    """The full map for one version - what `GET /taxonomy/role-topics` serves. Defaults to
    `active_version()` when `version` is omitted."""
    return _load(version or active_version())


def topics_for(role: str | None, *, version: str | None = None) -> list[str]:
    """Resolve a `users.role` value to its taxonomy topics. Deterministic: the same
    `(role, version)` pair always returns the same list, in the YAML's own declared order.

    An absent or unrecognized role resolves to `["general"]` - matching the taxonomy's own
    explicit `member: [general]` row rather than a separately-invented default, so an unknown
    role and the `member` role land on the same answer for the same reason (both mean "no
    specific subject-matter role").

    Returns a fresh list on every call (never the cached model's own list object) so a caller
    mutating the result cannot corrupt the `lru_cache`d taxonomy for every other caller.
    """
    taxonomy = load_taxonomy(version)
    if not role:
        return list(_DEFAULT_TOPICS)
    key = role.strip().lower().replace(" ", "_")
    return list(taxonomy.roles.get(key, _DEFAULT_TOPICS))
