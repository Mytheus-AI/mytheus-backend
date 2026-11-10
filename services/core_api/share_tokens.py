"""Share-link tokens and the one place a share link row is created.

A share link's token is `<payload>.<signature>`, both unpadded base64url:

- payload: the 16-byte tenant id followed by the 16-byte link id. Carrying the tenant is what
  lets the public read (which has no session) set `app.current_org` and then find the row
  under RLS like every other read, with no exception to tenant isolation.
- signature: the first 16 bytes of HMAC-SHA256 over the payload, keyed by
  `SHARE_LINK_SIGNING_KEY` (or, when that is unset, a key derived from `GATEWAY_SIGNING_KEY`
  for the purpose "share-links", so a deployment that already has the gateway key needs no
  new secret). A token that does not verify is refused before any database work, so guessing
  link ids costs nothing but CPU on our side.

The signature makes a token unforgeable; the row is the authority for everything that can
change after it was handed out: `revoked_at`, `expires_at`, and whether its creator may still
see what it shows. With neither key configured, creating a link is 503 and every public read
is 404: fail closed.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.errors import ErrorCode, ProblemDetail

KEY_ENV = "SHARE_LINK_SIGNING_KEY"
FALLBACK_ENV = "GATEWAY_SIGNING_KEY"
MIN_KEY = 32
SIG_BYTES = 16
DEFAULT_TTL_DAYS = 30
MAX_TTL_DAYS = 365
# The web app page that renders a share; it calls `GET /api/v1/public/shares/{token}`.
SHARE_PATH = "/s/"

ShareKind = Literal["meeting_recap", "decision_record"]


def _signing_key() -> bytes | None:
    own = os.environ.get(KEY_ENV, "").strip()
    if len(own) >= MIN_KEY:
        return own.encode("utf-8")
    gateway = os.environ.get(FALLBACK_ENV, "").strip()
    if len(gateway) >= MIN_KEY:
        return hmac.new(gateway.encode("utf-8"), b"mytheus/share-links/v1", "sha256").digest()
    return None


def configured() -> bool:
    return _signing_key() is not None


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def mint(org_id: uuid.UUID, link_id: uuid.UUID) -> str:
    key = _signing_key()
    if key is None:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Share links are not configured on this server"
        )
    payload = org_id.bytes + link_id.bytes
    sig = hmac.new(key, payload, hashlib.sha256).digest()[:SIG_BYTES]
    return f"{_b64(payload)}.{_b64(sig)}"


def verify(token: str) -> tuple[uuid.UUID, uuid.UUID] | None:
    """(org_id, link_id) for a token this server signed, else None. Never raises."""
    key = _signing_key()
    if key is None or len(token) > 200 or token.count(".") != 1:
        return None
    raw_payload, raw_sig = token.split(".")
    try:
        payload, sig = _unb64(raw_payload), _unb64(raw_sig)
    except (binascii.Error, ValueError):
        return None
    if len(payload) != 32 or len(sig) != SIG_BYTES:
        return None
    expected = hmac.new(key, payload, hashlib.sha256).digest()[:SIG_BYTES]
    if not hmac.compare_digest(expected, sig):
        return None
    return uuid.UUID(bytes=payload[:16]), uuid.UUID(bytes=payload[16:])


def app_base_url() -> str:
    return os.environ.get("APP_BASE_URL", "http://localhost:3000").rstrip("/")


def share_url(token: str) -> str:
    return f"{app_base_url()}{SHARE_PATH}{token}"


@dataclass(frozen=True)
class ShareLinkRow:
    id: uuid.UUID
    kind: str
    meeting_id: uuid.UUID
    decision_id: uuid.UUID | None
    created_by: uuid.UUID
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None
    view_count: int
    last_viewed_at: datetime | None

    @property
    def active(self) -> bool:
        return self.revoked_at is None and self.expires_at > datetime.now(UTC)


LINK_COLS = (
    "id, kind, meeting_id, decision_id, created_by, created_at, expires_at, revoked_at, "
    "view_count, last_viewed_at"
)


def row_from(mapping: Any) -> ShareLinkRow:
    return ShareLinkRow(**{k: mapping[k] for k in ShareLinkRow.__dataclass_fields__})


async def create_link(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    kind: ShareKind,
    meeting_id: uuid.UUID,
    decision_id: uuid.UUID | None,
    created_by: uuid.UUID,
    ttl_days: int = DEFAULT_TTL_DAYS,
) -> ShareLinkRow:
    """Insert one link. The CALLER has already checked the creator may share the record
    (`records.build_recap` / `records.build_decision_record` returned something)."""
    if not configured():
        mint(org_id, uuid.uuid4())  # raises the 503
    expires = datetime.now(UTC) + timedelta(days=max(1, min(ttl_days, MAX_TTL_DAYS)))
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO share_links (org_id, kind, meeting_id, decision_id, created_by, "
                    f"expires_at) VALUES (:o, :k, :m, :d, :u, :e) RETURNING {LINK_COLS}"
                ),
                {
                    "o": str(org_id),
                    "k": kind,
                    "m": str(meeting_id),
                    "d": str(decision_id) if decision_id else None,
                    "u": str(created_by),
                    "e": expires,
                },
            )
        )
        .mappings()
        .one()
    )
    return row_from(row)


async def active_recap_link(
    session: AsyncSession, meeting_id: uuid.UUID, created_by: uuid.UUID
) -> ShareLinkRow | None:
    """The newest live recap link this person made for this meeting, if any."""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {LINK_COLS} FROM share_links WHERE kind = 'meeting_recap' "
                    "AND meeting_id = :m AND created_by = :u AND revoked_at IS NULL "
                    "AND expires_at > now() ORDER BY created_at DESC LIMIT 1"
                ),
                {"m": str(meeting_id), "u": str(created_by)},
            )
        )
        .mappings()
        .first()
    )
    return row_from(row) if row else None
