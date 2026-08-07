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
