"""Outbound calls to Clerk's Backend API. Currently one: publish a tenant's internal `org_id`.

Why this exists. `mytheus-intelligence` authorizes every AI-tier request from an
`internal_org_id` token claim carrying the ALREADY-RESOLVED internal `org_id` (a UUID), not
Clerk's opaque `org_...` string. It reads the claim rather than joining `public.organizations`
on purpose: that table belongs to this repo's bounded context, and reading it from the AI tier
would re-open the cross-context read that migration `9c2b7e4a1d03` closed for the async path.

Clerk populates that claim from the organization's `public_metadata`, so something has to put
the value there. Nothing did, so the claim was absent on every token and the AI tier failed
closed with 401 on every request - correctly, but permanently.

This is the writer. It is deliberately NOT called inside the provisioning transaction: it is a
network call to a third party, and holding a database transaction open across one is how a
slow dependency turns into connection-pool exhaustion.

Two configuration facts, neither of which this code can enforce:

  1. `CLERK_SECRET_KEY` must be a real key. Without it this is a logged no-op rather than an
     error, so local development and tests do not need Clerk credentials to provision a tenant.
  2. A Clerk JWT template must map `public_metadata.internal_org_id` into a claim named
     `internal_org_id` (or whatever `CLERK_JWT_INTERNAL_ORG_CLAIM` names in the AI tier). That
     is dashboard configuration. Writing the metadata without that mapping changes nothing
     about the token, so the AI tier keeps 401-ing and the cause is invisible from this side.
     Setup lives in deploy/README of `mytheus-platform`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

_API_BASE = "https://api.clerk.com/v1"
_TIMEOUT = httpx.Timeout(10.0)

# Values that look configured but are not. `.env.example` ships placeholders, and treating one
# as a live key produces a 401 from Clerk on every provision, which reads like an outage.
_PLACEHOLDERS = {"", "CHANGE_ME", "sk_test_CHANGE_ME", "placeholder", "TODO"}

# The metadata field the Clerk JWT template maps into the AI tier's claim. Named here rather
# than inlined so the string exists in exactly one place on the write side; the read side
# names it in mytheus-intelligence's `libs/shared/auth.py::DEFAULT_ORG_CLAIM`.
INTERNAL_ORG_ID_FIELD = "internal_org_id"


def _secret_key() -> str | None:
    key = os.environ.get("CLERK_SECRET_KEY", "").strip()
    return None if key in _PLACEHOLDERS else key


async def set_internal_org_id(clerk_org_id: str, org_id: UUID) -> bool:
    """Publish `org_id` to the Clerk organization's `public_metadata`.

    Returns True when Clerk accepted the write, False when it was skipped or failed. Never
    raises: a tenant that exists in the database with an unpublished claim is recoverable (the
    call is idempotent, so simply run it again), whereas failing provisioning outright because
    a third party was briefly unavailable would leave the caller with no tenant at all.

    Idempotent by construction - Clerk's metadata endpoint MERGES the object given, so writing
    the same value twice is a no-op and other fields on the organization are left untouched.
    """
    key = _secret_key()
    if key is None:
        logger.warning(
            "CLERK_SECRET_KEY is unset or a placeholder; not publishing internal_org_id for "
            "clerk_org_id=%s. AI-tier requests for this tenant will 401 until it is set and "
            "this is re-run.",
            clerk_org_id,
        )
        return False
    if not clerk_org_id:
        logger.warning("no clerk_org_id for org_id=%s; nothing to publish to", org_id)
        return False

    url = f"{_API_BASE}/organizations/{clerk_org_id}/metadata"
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.patch(
                url,
                headers={"Authorization": f"Bearer {key}"},
                json={"public_metadata": {INTERNAL_ORG_ID_FIELD: str(org_id)}},
            )
    except httpx.HTTPError:
        logger.exception(
            "publishing internal_org_id to Clerk failed for clerk_org_id=%s; the tenant is "
            "provisioned but AI-tier requests will 401 until this is re-run",
            clerk_org_id,
        )
        return False

    if response.status_code >= 400:
        # Body, not just status: Clerk returns a machine-readable reason (unknown org, bad
        # key), and without it every failure looks the same in the logs.
        logger.error(
            "Clerk rejected the internal_org_id write for clerk_org_id=%s: HTTP %s %s",
            clerk_org_id,
            response.status_code,
            response.text[:300],
        )
        return False

    logger.info("published internal_org_id=%s to clerk_org_id=%s", org_id, clerk_org_id)
    return True


# --- self-serve provisioning: ask Clerk, never the caller ------------------------------------


class ClerkUnavailable(Exception):
    """Clerk could not be asked (no key, network failure, 5xx). Callers answer 503."""


@dataclass(frozen=True)
class ClerkMembership:
    """One user's membership in one Clerk organization, as Clerk itself reports it."""

    role: str  # "org:admin" or "org:member"
    email: str
    avatar_url: str | None

    @property
    def is_admin(self) -> bool:
        return self.role == "org:admin"


@dataclass(frozen=True)
class ClerkOrgForAdmin:
    """What self-serve provisioning needs, read from Clerk rather than from the request."""

    name: str
    slug: str
    owner_email: str


async def _membership(
    client: httpx.AsyncClient, clerk_org_id: str, clerk_user_id: str
) -> ClerkMembership | None:
    members = await client.get(
        f"{_API_BASE}/organizations/{clerk_org_id}/memberships",
        params={"user_id": clerk_user_id, "limit": 1},
    )
    user = await client.get(f"{_API_BASE}/users/{clerk_user_id}")
    for response in (members, user):
        if response.status_code >= 500:
            raise ClerkUnavailable(f"Clerk answered HTTP {response.status_code}")
    if members.status_code >= 400 or user.status_code >= 400:
        return None
    row = next(
        (
            m
            for m in members.json().get("data") or []
            if (m.get("public_user_data") or {}).get("user_id") == clerk_user_id
        ),
        None,
    )
    if row is None:
        return None
    u = user.json()
    primary = u.get("primary_email_address_id")
    email = next(
        (e.get("email_address") for e in u.get("email_addresses") or [] if e.get("id") == primary),
        None,
    )
    if not email:
        return None
    return ClerkMembership(
        role=str(row.get("role") or ""), email=email, avatar_url=u.get("image_url")
    )


def _client(key: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=_TIMEOUT, headers={"Authorization": f"Bearer {key}"})


async def membership_of(clerk_org_id: str, clerk_user_id: str) -> ClerkMembership | None:
    """The caller's membership in the organization, or None when Clerk has none.

    Raises `ClerkUnavailable` when Clerk cannot be asked, so a caller never mistakes an outage
    for "not a member".
    """
    key = _secret_key()
    if key is None:
        raise ClerkUnavailable("CLERK_SECRET_KEY is unset or a placeholder")
    try:
        async with _client(key) as client:
            return await _membership(client, clerk_org_id, clerk_user_id)
    except httpx.HTTPError as exc:
        raise ClerkUnavailable("Clerk request failed") from exc


async def org_for_admin(clerk_org_id: str, clerk_user_id: str) -> ClerkOrgForAdmin | None:
    """The organization's name, slug and the caller's primary email, if the caller is an
    `org:admin` member of it. None when they are not (or the org or user does not exist).

    Authorization for self-serve provisioning rests on THIS answer, from Clerk's Backend API,
    not on the session token's role claim: a token proves who the caller is and which org is
    active, but whether they may create that org's tenant is checked at the source.
    """
    key = _secret_key()
    if key is None:
        raise ClerkUnavailable("CLERK_SECRET_KEY is unset or a placeholder")
    try:
        async with _client(key) as client:
            org = await client.get(f"{_API_BASE}/organizations/{clerk_org_id}")
            if org.status_code >= 500:
                raise ClerkUnavailable(f"Clerk answered HTTP {org.status_code}")
            if org.status_code >= 400:
                return None
            membership = await _membership(client, clerk_org_id, clerk_user_id)
    except httpx.HTTPError as exc:
        raise ClerkUnavailable("Clerk request failed") from exc

    o = org.json()
    if membership is None or not membership.is_admin or not o.get("name"):
        return None
    return ClerkOrgForAdmin(
        name=str(o["name"]), slug=str(o.get("slug") or ""), owner_email=membership.email
    )


@dataclass(frozen=True)
class ActorToken:
    """A Clerk impersonation (actor) token: a one-time sign-in link as `user_id`, with the
    staff member recorded as the actor in the resulting session's `act` claim."""

    id: str
    url: str


async def create_actor_token(
    clerk_user_id: str, actor_clerk_user_id: str, *, expires_in_seconds: int
) -> ActorToken:
    """Ask Clerk for an actor token (ADR-0046 D4). The link and the session it opens are both
    bounded by `expires_in_seconds`; the server-side `support_sessions` check is what actually
    enforces the time box. Raises `ClerkUnavailable` when Clerk cannot be asked or refuses."""
    key = _secret_key()
    if key is None:
        raise ClerkUnavailable("CLERK_SECRET_KEY is unset or a placeholder")
    body = {
        "user_id": clerk_user_id,
        "actor": {"sub": actor_clerk_user_id},
        "expires_in_seconds": expires_in_seconds,
        "session_max_duration_in_seconds": expires_in_seconds,
    }
    try:
        async with _client(key) as client:
            response = await client.post(f"{_API_BASE}/actor_tokens", json=body)
    except httpx.HTTPError as exc:
        raise ClerkUnavailable("Clerk request failed") from exc
    if response.status_code >= 300:
        raise ClerkUnavailable(f"Clerk refused the actor token (HTTP {response.status_code})")
    data = response.json()
    token_id, url = data.get("id"), data.get("url")
    if not isinstance(token_id, str) or not isinstance(url, str) or not url:
        raise ClerkUnavailable("Clerk returned an actor token without a sign-in link")
    return ActorToken(id=token_id, url=url)


async def revoke_actor_token(token_id: str) -> bool:
    """Revoke an unused actor token. Best effort: True when Clerk accepted it. A token that was
    already used cannot be revoked, which is fine, since the server-side session check ends
    access regardless."""
    key = _secret_key()
    if key is None:
        return False
    try:
        async with _client(key) as client:
            response = await client.post(f"{_API_BASE}/actor_tokens/{token_id}/revoke")
    except httpx.HTTPError:
        logger.warning("could not revoke actor token %s", token_id)
        return False
    return response.status_code < 300


@dataclass(frozen=True)
class ClerkUserSummary:
    """What the superadmin console needs to know about a Clerk account (ADR-0046 D1)."""

    id: str
    email: str | None
    name: str | None
    organization_count: int


async def _summary(client: httpx.AsyncClient, user: dict) -> ClerkUserSummary:  # type: ignore[type-arg]
    primary = user.get("primary_email_address_id")
    emails = user.get("email_addresses") or []
    email = next((e.get("email_address") for e in emails if e.get("id") == primary), None)
    if email is None and emails:
        email = emails[0].get("email_address")
    name = " ".join(p for p in (user.get("first_name"), user.get("last_name")) if p) or None
    memberships = await client.get(
        f"{_API_BASE}/users/{user['id']}/organization_memberships", params={"limit": 1}
    )
    if memberships.status_code >= 300:
        raise ClerkUnavailable(f"Clerk answered HTTP {memberships.status_code}")
    body = memberships.json()
    count = body.get("total_count") if isinstance(body, dict) else None
    if not isinstance(count, int):
        count = len(body.get("data", [])) if isinstance(body, dict) else len(body)
    return ClerkUserSummary(id=str(user["id"]), email=email, name=name, organization_count=count)


async def user_summary(clerk_user_id: str) -> ClerkUserSummary | None:
    """One Clerk account by id, with how many organizations it belongs to. None if unknown."""
    key = _secret_key()
    if key is None:
        raise ClerkUnavailable("CLERK_SECRET_KEY is unset or a placeholder")
    try:
        async with _client(key) as client:
            user = await client.get(f"{_API_BASE}/users/{clerk_user_id}")
            if user.status_code == 404:
                return None
            if user.status_code >= 300:
                raise ClerkUnavailable(f"Clerk answered HTTP {user.status_code}")
            return await _summary(client, user.json())
    except httpx.HTTPError as exc:
        raise ClerkUnavailable("Clerk request failed") from exc


async def users_by_email(email: str) -> list[ClerkUserSummary]:
    """Every Clerk account with this email address (normally one)."""
    key = _secret_key()
    if key is None:
        raise ClerkUnavailable("CLERK_SECRET_KEY is unset or a placeholder")
    try:
        async with _client(key) as client:
            found = await client.get(f"{_API_BASE}/users", params={"email_address": email})
            if found.status_code >= 300:
                raise ClerkUnavailable(f"Clerk answered HTTP {found.status_code}")
            users = found.json()
            return [await _summary(client, u) for u in (users if isinstance(users, list) else [])]
    except httpx.HTTPError as exc:
        raise ClerkUnavailable("Clerk request failed") from exc
