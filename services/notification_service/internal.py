"""notification-service internal routes (caller tokens, see `internal_auth.py`).

- `GET /internal/email/status` (core-api): `{configured, provider}`, so core-api can tell the
  host before they send whether external guests can be emailed at all (the follow-up composer's
  "Email is not connected yet" state). Reads the environment only.
- `POST /internal/email/send` (core-api): send one email to up to 20 addresses that are not
  users of the tenant, e.g. the external guests a host listed on a meeting follow-up. Users are
  never mailed through here: they get the `notifications` event, whose delivery honours their
  channel preferences (ADR-0036: email is opt-in per user).

Idempotency. Each recipient is claimed in `email_deliveries` under
`{idempotency_key}:{recipient}` (`UNIQUE (org_id, idempotency_key)`) in one transaction; the
mail is sent only after that commit, and the row then moves to `sent` or `failed`. A replay
(the same key, a workflow retry, a host pressing Send again) finds the claim and reports the
recorded status with `replayed: true` instead of sending again. The trade, as for the
consumer's email: a crash between the commit and the send leaves that one recipient `pending`
and unsent rather than mailed twice.

Not configured. With no email provider (`email.email_sender_from_env()` is None) every
recipient is recorded `skipped` / `not_configured` and the answer says `configured: false`; the
caller shows the host that guests were not emailed. Never a 5xx.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.notification_service.email import (
    EmailError,
    EmailSender,
    email_sender_from_env,
    provider_name,
)
from services.notification_service.internal_auth import core_caller

logger = logging.getLogger(__name__)

router = APIRouter(tags=["notification-internal"])

MAX_RECIPIENTS = 20
EMAIL_RE = re.compile(
    r"^[^@\s<>,;\"']{1,64}@[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?\.[A-Za-z]{2,63}$"
)

DeliveryStatus = Literal["pending", "sent", "failed", "skipped"]


def normalize_email(raw: str) -> str:
    value = raw.strip().lower()
    if len(value) > 320 or not EMAIL_RE.match(value):
        raise ValueError(f"not an email address: {raw!r}")
    return value


class EmailSend(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["meeting_followup"]
    meeting_id: uuid.UUID | None = None
    idempotency_key: str = Field(min_length=1, max_length=200)
    recipients: list[str] = Field(min_length=1, max_length=MAX_RECIPIENTS)
    subject: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=20000)
    html: str | None = Field(default=None, max_length=200000)

    @field_validator("recipients")
    @classmethod
    def _emails(cls, value: list[str]) -> list[str]:
        out: list[str] = []
        for raw in value:
            email = normalize_email(raw)
            if email not in out:
                out.append(email)
        return out


class DeliveryResult(BaseModel):
    recipient: str
    status: DeliveryStatus
    detail: str | None = None
    replayed: bool = False


class EmailSendResult(BaseModel):
    configured: bool
    provider: Literal["resend", "smtp"] | None
    results: list[DeliveryResult]


_sender_override: EmailSender | None = None
_override_set = False


def get_sender() -> EmailSender | None:
    """The configured sender. Tests install a fake with `set_sender`."""
    if _override_set:
        return _sender_override
    return email_sender_from_env()


def set_sender(sender: EmailSender | None, *, active: bool = True) -> None:
    global _sender_override, _override_set
    _sender_override, _override_set = sender, active


class EmailStatus(BaseModel):
    configured: bool
    provider: Literal["resend", "smtp"] | None


@router.get("/internal/email/status", response_model=EmailStatus)
async def email_status(
    _org_id: Annotated[uuid.UUID, Depends(core_caller)],
) -> EmailStatus:
    """Whether this server has an email provider (no secret or address is revealed)."""
    sender = get_sender()
    return EmailStatus(configured=sender is not None, provider=provider_name(sender))


@router.post("/internal/email/send", response_model=EmailSendResult)
async def send_email(
    body: EmailSend,
    org_id: Annotated[uuid.UUID, Depends(core_caller)],
) -> EmailSendResult:
    sender = get_sender()
    provider = provider_name(sender)
    results: dict[str, DeliveryResult] = {}
    to_send: list[tuple[uuid.UUID, str]] = []
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if body.meeting_id is not None:
            visible = (
                await session.execute(
                    text("SELECT 1 FROM meetings WHERE id = :m AND deleted_at IS NULL"),
                    {"m": str(body.meeting_id)},
                )
            ).first()
            if visible is None:
                raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        for recipient in body.recipients:
            key = f"{body.idempotency_key}:{recipient}"
            status: DeliveryStatus = "pending" if sender else "skipped"
            detail: str | None = None if sender else "not_configured"
            claimed = (
                await session.execute(
                    text(
                        "INSERT INTO email_deliveries (org_id, idempotency_key, kind, meeting_id, "
                        "recipient, status, detail, provider) VALUES (:o, :k, :kind, :m, :r, :s, "
                        ":d, :p) ON CONFLICT (org_id, idempotency_key) DO NOTHING RETURNING id"
                    ),
                    {
                        "o": str(org_id),
                        "k": key,
                        "kind": body.kind,
                        "m": str(body.meeting_id) if body.meeting_id else None,
                        "r": recipient,
                        "s": status,
                        "d": detail,
                        "p": provider,
                    },
                )
            ).scalar()
            if claimed is None:
                prior = (
                    (
                        await session.execute(
                            text(
                                "SELECT status, detail FROM email_deliveries "
                                "WHERE idempotency_key = :k"
                            ),
                            {"k": key},
                        )
                    )
                    .mappings()
                    .one()
                )
                results[recipient] = DeliveryResult(
                    recipient=recipient,
                    status=prior["status"],
                    detail=prior["detail"],
                    replayed=True,
                )
                continue
            results[recipient] = DeliveryResult(recipient=recipient, status=status, detail=detail)
            if sender is not None:
                to_send.append((uuid.UUID(str(claimed)), recipient))

    for delivery_id, recipient in to_send:  # only after the claims committed
        assert sender is not None
        outcome: DeliveryStatus
        reason: str | None
        try:
            await sender.send(to=recipient, subject=body.subject, body=body.text, html=body.html)
            outcome, reason = "sent", None
        except EmailError as exc:
            outcome, reason = "failed", exc.reason
        except Exception as exc:  # a provider problem is a failed delivery, never a 5xx
            logger.warning("follow-up email failed: %s", type(exc).__name__)
            outcome, reason = "failed", f"send_error:{type(exc).__name__}"
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            await session.execute(
                text(
                    "UPDATE email_deliveries SET status = :s, detail = :d, updated_at = now() "
                    "WHERE id = :id"
                ),
                {"s": outcome, "d": reason, "id": str(delivery_id)},
            )
        results[recipient] = DeliveryResult(recipient=recipient, status=outcome, detail=reason)

    return EmailSendResult(
        configured=sender is not None,
        provider=provider,
        results=[results[r] for r in body.recipients],
    )
