"""Promises captured outside meetings (promise ledger): the internal door integration-service
uses after the person's agent read one of their own messages.

Caller `integration-service` (`MYTHEUS_CORE_CALLER_TOKENS`, the `internal_meetings.py` scheme).

- `POST /internal/promises/capture-check` {channel, author_email} -> {user_id, enabled}: is the
  author a live user of this workspace, and may their agent look at their `channel` messages
  (`promise_capture_preferences`, on unless they turned it off)? Asked BEFORE any model call, so
  nobody's message is sent for extraction unless its author is a user who allows it.
- `POST /internal/promises/captured` {channel, author_email, source_ref, source_link,
  occurred_at, prompt_version, promises: [...]}: writes each extracted promise as a `pending`
  `commitments` row of the author (they confirm or dismiss it in the ledger), with its source
  (link, exact quote, when). A recipient email of a live colleague becomes `recipient_user_id`
  (who to ask when the date moves); anything else stays a label. Replays write nothing twice:
  `uq_commitments_source` on (user, source_ref, md5(content)). One quiet in-app notification per
  message that produced new promises (`promise-found:{source_ref}`).

Email: there is no mail-reading integration in Mytheus yet, so nothing calls this with
`channel: email` today; the route accepts it so a future one only has to call it.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_integrations import integration_caller
from services.core_api.routes.promises import notify

router = APIRouter(tags=["core-internal"])

MAX_PER_MESSAGE = 5
MIN_CONFIDENCE = 0.7  # the canon gate: > 0.7 surfaces, the rest is dropped (no worklog here)
CaptureChannel = Literal["slack", "email"]


class CaptureCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: CaptureChannel
    author_email: str = Field(min_length=3, max_length=320)


class CaptureCheckResult(BaseModel):
    user_id: uuid.UUID | None
    enabled: bool


class CapturedPromise(BaseModel):
    model_config = ConfigDict(extra="ignore")

    content: str = Field(min_length=1, max_length=500)
    due_hint: str | None = Field(default=None, max_length=100)
    due_date: date | None = None
    recipient_label: str | None = Field(default=None, max_length=200)
    recipient_email: str | None = Field(default=None, max_length=320)
    quote: str | None = Field(default=None, max_length=1000)
    confidence: float = Field(ge=0, le=1)


class Captured(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: CaptureChannel
    author_email: str = Field(min_length=3, max_length=320)
    source_ref: str = Field(min_length=1, max_length=300)
    source_link: str | None = Field(default=None, max_length=2000)
    occurred_at: datetime | None = None
    prompt_version: str | None = Field(default=None, max_length=80)
    promises: list[CapturedPromise] = Field(max_length=20)


class CapturedResult(BaseModel):
    user_id: uuid.UUID | None
    written: int


async def _author(
    session: AsyncSession, channel: CaptureChannel, email: str
) -> tuple[uuid.UUID | None, bool]:
    row = (
        await session.execute(
            text(
                "SELECT u.id, COALESCE(p.slack_enabled, true), COALESCE(p.email_enabled, true) "
                "FROM users u LEFT JOIN promise_capture_preferences p ON p.user_id = u.id "
                "WHERE lower(u.email) = lower(:e) AND u.deleted_at IS NULL "
                "ORDER BY u.created_at LIMIT 1"
            ),
            {"e": email.strip()},
        )
    ).first()
    if row is None:
        return None, False
    return row[0], bool(row[1] if channel == "slack" else row[2])


@router.post("/internal/promises/capture-check", response_model=CaptureCheckResult)
async def capture_check(
    body: CaptureCheck,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> CaptureCheckResult:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        user_id, enabled = await _author(session, body.channel, body.author_email)
    return CaptureCheckResult(user_id=user_id, enabled=enabled)


def _clip(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())
    return (value if len(value) <= limit else value[: limit - 3].rstrip() + "...") or None


@router.post("/internal/promises/captured", response_model=CapturedResult)
async def captured(
    body: Captured,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> CapturedResult:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        user_id, enabled = await _author(session, body.channel, body.author_email)
        if user_id is None or not enabled:
            return CapturedResult(user_id=user_id, written=0)
        kept = [p for p in body.promises if p.confidence >= MIN_CONFIDENCE][:MAX_PER_MESSAGE]
        written: list[str] = []
        for promise in kept:
            recipient_id = None
            if promise.recipient_email:
                recipient_id = (
                    await session.execute(
                        text(
                            "SELECT id FROM users WHERE lower(email) = lower(:e) "
                            "AND deleted_at IS NULL AND id <> :me LIMIT 1"
                        ),
                        {"e": promise.recipient_email.strip(), "me": str(user_id)},
                    )
                ).scalar()
            content = _clip(promise.content, 500) or ""
            if not content:
                continue
            digest = hashlib.sha256(f"{body.source_ref}:{content}".encode()).hexdigest()[:24]
            row = (
                await session.execute(
                    text(
                        "INSERT INTO commitments (org_id, user_id, content, due_hint, due_at, "
                        "status, review_state, channel, source_window_id, recipient_user_id, "
                        "recipient_label, source_link, source_quote, source_ref, source_at, "
                        "confidence, created_by) VALUES (:o, :u, :c, :dh, :d, 'open', 'pending', "
                        ":ch, :w, :ru, :rl, :sl, :q, :ref, :at, :conf, NULL) "
                        "ON CONFLICT DO NOTHING RETURNING id"
                    ),
                    {
                        "o": str(org_id),
                        "u": str(user_id),
                        "c": content,
                        "dh": _clip(promise.due_hint, 100),
                        "d": promise.due_date,
                        "ch": body.channel,
                        "w": f"{body.channel}:{digest}",
                        "ru": str(recipient_id) if recipient_id else None,
                        "rl": None if recipient_id else _clip(promise.recipient_label, 200),
                        "sl": body.source_link,
                        "q": _clip(promise.quote, 1000),
                        "ref": body.source_ref,
                        "at": body.occurred_at,
                        "conf": promise.confidence,
                    },
                )
            ).first()
            if row is not None:
                written.append(content)
        if written:
            where = "Slack" if body.channel == "slack" else "your email"
            first = written[0] if len(written[0]) <= 100 else written[0][:97].rstrip() + "..."
            title = (
                f"Your agent found a promise in {where}: {first}"
                if len(written) == 1
                else f"Your agent found {len(written)} promises in {where}"
            )
            ref_key = hashlib.sha256(body.source_ref.encode()).hexdigest()[:24]
            await notify(
                session,
                org_id,
                user_id=user_id,
                source=f"promise-found:{body.channel}:{ref_key}",
                title=title,
                body="\n".join(f"- {c}" for c in written)
                + "\n\nConfirm what you meant to promise; dismiss the rest. Only you see these.",
                link="/work/promises?review=1",
                priority=3,
            )
    return CapturedResult(user_id=user_id, written=len(written))
