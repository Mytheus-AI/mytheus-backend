"""The delegated-answer log write, for the `agent-results` consumer (polish wave 3).

A `ContextGapAgent` / `context_fill` record whose `payload_json` carries a `delegated` object is
an answer an absent person's agent gave from what they pre-approved (or its refusal):

    {"id": uuid, "delegator_user_id": uuid, "statement_id": uuid | null,
     "statement_text": str | null, "question": str, "answer": str, "covered": bool,
     "asked_by": uuid | null}

The same answer travels once per live participant (every one gets the card), always with the
same `id`, so the row is written once: `ON CONFLICT (id) DO NOTHING`. The delegator must be a
participant of the meeting whose attendance is `agent`; a `statement_id` that is not one of
theirs for this meeting is dropped (the text snapshot stays). Anything malformed is ignored,
never poison: the collaboration rows in the same record must still apply.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

MAX_TEXT = 1000


def _uuid(raw: Any) -> uuid.UUID | None:
    if raw is None or raw == "":
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        return None


def _text(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value[:MAX_TEXT] if value else None


async def log_delegated_answer(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    window_id: str | None,
    payload: dict[str, Any],
) -> bool:
    """Write the log row for `payload["delegated"]`, if any. True if a row was new."""
    block = payload.get("delegated")
    if not isinstance(block, dict):
        return False
    entry_id = _uuid(block.get("id"))
    delegator = _uuid(block.get("delegator_user_id"))
    question, answer = _text(block.get("question")), _text(block.get("answer"))
    covered = block.get("covered")
    if entry_id is None or delegator is None or question is None or answer is None:
        return False
    if not isinstance(covered, bool):
        return False
    is_delegate = (
        await session.execute(
            text(
                "SELECT 1 FROM meeting_participants WHERE meeting_id = :m AND user_id = :u "
                "AND attendance = 'agent'"
            ),
            {"m": str(meeting_id), "u": str(delegator)},
        )
    ).first()
    if is_delegate is None:
        logger.warning("delegated answer for a non-delegate ignored meeting=%s", meeting_id)
        return False
    statement_id = _uuid(block.get("statement_id"))
    if statement_id is not None:
        owned = (
            await session.execute(
                text(
                    "SELECT 1 FROM delegated_answer_statements WHERE id = :s AND meeting_id = :m "
                    "AND user_id = :u"
                ),
                {"s": str(statement_id), "m": str(meeting_id), "u": str(delegator)},
            )
        ).first()
        if owned is None:
            statement_id = None
    asked_by = _uuid(block.get("asked_by"))
    if asked_by is not None:
        known = (
            await session.execute(
                text("SELECT 1 FROM users WHERE id = :u AND deleted_at IS NULL"),
                {"u": str(asked_by)},
            )
        ).first()
        if known is None:
            asked_by = None
    inserted = (
        await session.execute(
            text(
                "INSERT INTO delegated_answer_log (id, org_id, meeting_id, user_id, statement_id, "
                "statement_text, question, answer, covered, asked_by, window_id) VALUES (:id, :o, "
                ":m, :u, :s, :st, :q, :a, :c, :by, :w) ON CONFLICT (id) DO NOTHING RETURNING 1"
            ),
            {
                "id": str(entry_id),
                "o": str(org_id),
                "m": str(meeting_id),
                "u": str(delegator),
                "s": str(statement_id) if statement_id else None,
                "st": _text(block.get("statement_text")) if covered else None,
                "q": question,
                "a": answer,
                "c": covered,
                "by": str(asked_by) if asked_by else None,
                "w": window_id[:200] if window_id else None,
            },
        )
    ).first()
    return inserted is not None
