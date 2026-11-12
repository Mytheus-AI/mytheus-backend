"""`POST /internal/email/invitations` (core-api): meeting invitations, updates and cancellations.

ADR-0043. A host sends a meeting to named people (`core-api` `routes/invitations.py`). Every
recipient is a user of the tenant, which core-api resolved under RLS; the host pressing Send is
the explicit request, so this is a transactional email to a person, not a notification (the
weekly digest's `/internal/email/user` is the same kind of exception to ADR-0036's opt-in rule).

Each message is already rendered by core-api for its recipient (their own time zone), so the
body here is `messages[]` (at most 50), one shared `attachment` (the meeting's `.ics`, sent as
`text/calendar; method=REQUEST` or `method=CANCEL`) and the kind. `reply_to` on each message is
the address of the person who sent it, so a reply reaches a human, not `noreply@`.

Idempotency and the not-configured answer are exactly `/internal/email/send`'s: each message is
claimed in `email_deliveries` under its own `idempotency_key` (core-api makes a fresh key per
send of a row, so a deliberate resend is a new delivery and a replayed call is not), the claims
commit, then the mails go out; a replay reports the recorded status with `replayed: true`.

Retries. A transient failure (the provider unreachable, HTTP 429 or 5xx) is retried up to
`SEND_ATTEMPTS` times with a short exponential backoff before the delivery is recorded `failed`;
a 4xx refusal is final at once. No provider: every message is `skipped` / `not_configured`.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.notification_service.email import (
    Attachment,
    EmailError,
    EmailSender,
    provider_name,
)
from services.notification_service.internal import (
    DeliveryResult,
    DeliveryStatus,
    EmailSendResult,
    get_sender,
    normalize_email,
)
from services.notification_service.internal_auth import core_caller

logger = logging.getLogger(__name__)

router = APIRouter(tags=["notification-internal"])

MAX_MESSAGES = 50
SEND_ATTEMPTS = 3
RETRY_BASE_S = 0.5

InvitationKind = Literal["meeting_invitation", "meeting_update", "meeting_cancel"]


class InvitationMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=200)
    recipient: str
    subject: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=20000)
    html: str | None = Field(default=None, max_length=200000)
    reply_to: str | None = None

    @field_validator("recipient")
    @classmethod
    def _recipient(cls, value: str) -> str:
        return normalize_email(value)

    @field_validator("reply_to")
    @classmethod
    def _reply_to(cls, value: str | None) -> str | None:
        return None if not value else normalize_email(value)


class InvitationAttachment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filename: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9._-]+$")
    content_type: Literal["text/calendar; method=REQUEST", "text/calendar; method=CANCEL"]
    content_base64: str = Field(min_length=1, max_length=200000)

    def decoded(self) -> Attachment:
        try:
            raw = base64.b64decode(self.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "attachment is not base64"
            ) from exc
        return Attachment(filename=self.filename, content_type=self.content_type, content=raw)


class InvitationSend(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: InvitationKind
    meeting_id: uuid.UUID
    messages: list[InvitationMessage] = Field(min_length=1, max_length=MAX_MESSAGES)
    attachment: InvitationAttachment | None = None


def _transient(reason: str) -> bool:
    if reason.startswith(("resend_unreachable", "send_error")):
        return True
    if reason.startswith("resend_http_"):
        code = reason.removeprefix("resend_http_")
        return code == "429" or code.startswith("5")
    return False


async def send_with_retries(
    sender: EmailSender,
    message: InvitationMessage,
    attachment: Attachment | None,
) -> tuple[DeliveryStatus, str | None]:
    """Send one message, retrying a transient failure. Never raises."""
    reason = "send_error"
    for attempt in range(SEND_ATTEMPTS):
        try:
            await sender.send(
                to=message.recipient,
                subject=message.subject,
                body=message.text,
                html=message.html,
                reply_to=message.reply_to,
                attachments=[attachment] if attachment else None,
            )
            return "sent", None
        except EmailError as exc:
            reason = exc.reason
        except Exception as exc:  # a provider problem is a failed delivery, never a 5xx
            logger.warning("invitation email failed: %s", type(exc).__name__)
            reason = f"send_error:{type(exc).__name__}"
        if not _transient(reason) or attempt == SEND_ATTEMPTS - 1:
            break
        await asyncio.sleep(RETRY_BASE_S * (2**attempt))
    return "failed", reason[:200]


@router.post("/internal/email/invitations", response_model=EmailSendResult)
async def send_invitations(
    body: InvitationSend,
    org_id: Annotated[uuid.UUID, Depends(core_caller)],
) -> EmailSendResult:
    """Send meeting invitations (or their update or cancellation). See the module doc."""
    sender = get_sender()
    provider = provider_name(sender)
    attachment = body.attachment.decoded() if body.attachment else None
    results: list[DeliveryResult] = []
    to_send: list[tuple[int, uuid.UUID, InvitationMessage]] = []
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        visible = (
            await session.execute(
                text("SELECT 1 FROM meetings WHERE id = :m AND deleted_at IS NULL"),
                {"m": str(body.meeting_id)},
            )
        ).first()
        if visible is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        for message in body.messages:
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
                        "k": message.idempotency_key,
                        "kind": body.kind,
                        "m": str(body.meeting_id),
                        "r": message.recipient,
                        "s": status,
                        "d": detail,
                        "p": provider,
                    },
                )
            ).scalar()
            if claimed is None:
                prior = (
                    await session.execute(
                        text(
                            "SELECT status, detail FROM email_deliveries "
                            "WHERE idempotency_key = :k"
                        ),
                        {"k": message.idempotency_key},
                    )
                ).one()
                results.append(
                    DeliveryResult(
                        recipient=message.recipient,
                        status=prior[0],
                        detail=prior[1],
                        replayed=True,
                    )
                )
                continue
            results.append(
                DeliveryResult(recipient=message.recipient, status=status, detail=detail)
            )
            if sender is not None:
                to_send.append((len(results) - 1, uuid.UUID(str(claimed)), message))

    for index, delivery_id, message in to_send:  # only after the claims committed
        assert sender is not None
        outcome, reason = await send_with_retries(sender, message, attachment)
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            await session.execute(
                text(
                    "UPDATE email_deliveries SET status = :s, detail = :d, updated_at = now() "
                    "WHERE id = :id"
                ),
                {"s": outcome, "d": reason, "id": str(delivery_id)},
            )
        results[index] = DeliveryResult(recipient=message.recipient, status=outcome, detail=reason)

    return EmailSendResult(configured=sender is not None, provider=provider, results=results)
