"""`POST /internal/email/user` (core-api): one email to one USER who opted into it.

Polish wave 3, the weekly "what changed for you" digest. `internal.py`'s `/internal/email/send`
mails only non-users (guests); users normally get email through the `notifications` event and
their channel preferences. The weekly digest is the exception the user asked for explicitly
(its own "Also send it by email" switch, `digest_preferences.email_enabled`), and a digest is
too long and too structured for the generic notification email, so core-api sends it here.

The recipient is a `user_id`, resolved to their address under RLS (never an address supplied by
the caller). Idempotency, the not-configured answer and the send-after-commit rule are exactly
`/internal/email/send`'s: claim `email_deliveries` under `{idempotency_key}:{user_id}`, commit,
then send; a replay reports the recorded status with `replayed: true`.
"""

from __future__ import annotations

import logging
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.notification_service.email import EmailError, provider_name
from services.notification_service.internal import (
    DeliveryResult,
    DeliveryStatus,
    EmailSendResult,
    get_sender,
)
from services.notification_service.internal_auth import core_caller

logger = logging.getLogger(__name__)

router = APIRouter(tags=["notification-internal"])


class UserEmailSend(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["weekly_digest"]
    user_id: uuid.UUID
    idempotency_key: str = Field(min_length=1, max_length=200)
    subject: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=20000)
    html: str | None = Field(default=None, max_length=200000)


@router.post("/internal/email/user", response_model=EmailSendResult)
async def send_user_email(
    body: UserEmailSend,
    org_id: Annotated[uuid.UUID, Depends(core_caller)],
) -> EmailSendResult:
    sender = get_sender()
    provider = provider_name(sender)
    key = f"{body.idempotency_key}:{body.user_id}"
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        email = (
            await session.execute(
                text("SELECT email FROM users WHERE id = :u AND deleted_at IS NULL"),
                {"u": str(body.user_id)},
            )
        ).scalar()
        if not email:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        status: DeliveryStatus = "pending" if sender else "skipped"
        detail: str | None = None if sender else "not_configured"
        claimed = (
            await session.execute(
                text(
                    "INSERT INTO email_deliveries (org_id, idempotency_key, kind, recipient, "
                    "status, detail, provider) VALUES (:o, :k, :kind, :r, :s, :d, :p) "
                    "ON CONFLICT (org_id, idempotency_key) DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "k": key,
                    "kind": body.kind,
                    "r": str(email)[:320],
                    "s": status,
                    "d": detail,
                    "p": provider,
                },
            )
        ).scalar()
        if claimed is None:
            prior = (
                await session.execute(
                    text("SELECT status, detail FROM email_deliveries WHERE idempotency_key = :k"),
                    {"k": key},
                )
            ).one()
            return EmailSendResult(
                configured=sender is not None,
                provider=provider,
                results=[
                    DeliveryResult(
                        recipient=str(email), status=prior[0], detail=prior[1], replayed=True
                    )
                ],
            )
    if sender is None:
        return EmailSendResult(
            configured=False,
            provider=None,
            results=[DeliveryResult(recipient=str(email), status=status, detail=detail)],
        )
    outcome: DeliveryStatus
    reason: str | None
    try:
        await sender.send(to=str(email), subject=body.subject, body=body.text, html=body.html)
        outcome, reason = "sent", None
    except EmailError as exc:
        outcome, reason = "failed", exc.reason
    except Exception as exc:  # a provider problem is a failed delivery, never a 5xx
        logger.warning("digest email failed: %s", type(exc).__name__)
        outcome, reason = "failed", f"send_error:{type(exc).__name__}"
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await session.execute(
            text(
                "UPDATE email_deliveries SET status = :s, detail = :d, updated_at = now() "
                "WHERE id = :id"
            ),
            {"s": outcome, "d": reason, "id": str(claimed)},
        )
    return EmailSendResult(
        configured=True,
        provider=provider,
        results=[DeliveryResult(recipient=str(email), status=outcome, detail=reason)],
    )
