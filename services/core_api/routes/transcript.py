"""Transcript read model. Plan 3-4 section 3.8 step 2.

`GET /api/v1/meetings/:id/transcript` is the ordered replay of a meeting's finals, and is named
in [_canon/01-service-catalog.md] as core-api's. Two consumers need it and neither had one:

  - the transcript view, which replays chunks in order and jumps by timestamp; and
  - the live-caption strip, which MUST re-hydrate from here after a socket drop. The
    `realtime-gateway` deliberately buffers and replays nothing to a reconnecting client
    ("finals are re-hydrated from core-api's REST read model, never from this socket"), so
    without this endpoint a disconnect silently loses every caption spoken during it.

READ-ONLY, and that is a boundary not a convenience. `transcript_chunks` is owned and written
only by `transcription-service` in `mytheus-realtime`; `mytheus-backend/CLAUDE.md` states it is
"read-only here (served via FTS)". Nothing in this module writes, and no migration in this repo
touches that table.

WHY THERE IS NO SEARCH HERE, deliberately. The plan's section 3.8 step 2 describes this
endpoint as "FTS over `transcription-service`-owned `transcript_chunks`", and canon's service
catalog says "read via core-api FTS". Implementing that literally would mean adding a `tsv`
column and a GIN index to `transcript_chunks` - a schema change to another repo's table, from a
repo that does not own it, which is precisely the second-writer trap. `search-service` hit the
same instruction in Plan 5-6 and refused it for the same reason: its migration
`d415f023220e` says plainly that it "deliberately does NOT do that" and builds its own
`transcript_chunk_search` read model in its own schema instead. That decision stands, so search
belongs to `search-service`'s read model and this endpoint serves ordered replay only.
Filtering here without an index would also be a sequential scan over a partitioned table that
the capacity model sizes at ~360 finals per meeting and growing.

Ordered by `(start_ms, id)`, which is the same tuple the table's own index
`(meeting_id, start_ms)` is built on, so the keyset page is an index range scan rather than a
sort. `start_ms` alone is not unique - two speakers can begin on the same millisecond - so `id`
is the tiebreak that keeps a row from being skipped or repeated across pages.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import TranscriptChunk, TranscriptPage

router = APIRouter(prefix="/api/v1", tags=["transcript"])

DEFAULT_LIMIT = 100
# A meeting yields roughly 360 finals (capacity model), so 500 lets a client pull a whole
# meeting in one page when it wants to, without inviting an unbounded fetch.
MAX_LIMIT = 500

_COLUMNS = "id, meeting_id, speaker_id, start_ms, end_ms, text, is_final, confidence, redacted"


def _encode_cursor(start_ms: int, row_id: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(json.dumps([start_ms, str(row_id)]).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[int, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return int(raw[0]), uuid.UUID(raw[1])
    except (ValueError, TypeError, IndexError, binascii.Error) as exc:
        raise ProblemDetail(
            400, ErrorCode.INVALID_CURSOR, "cursor is not a valid transcript cursor"
        ) from exc


async def _assert_meeting_visible(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    """RLS-scoped existence check with no hand-written `org_id` predicate, so another tenant's
    meeting is a 404 and never a 403 - existence is tenant-confidential. Same shape as
    `routes.speakers._fetch_meeting_exists`.

    This runs BEFORE reading `transcript_chunks` on purpose. `transcript_chunks` carries no
    foreign key to `meetings` (declarative partitioning precludes one), so an unchecked read
    would happily return an empty page for a meeting in another tenant, which leaks the fact
    that the id is well-formed and simply empty rather than not yours.
    """
    row = (
        await session.execute(
            text("SELECT id FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if row is None:
        raise not_found()


@router.get("/meetings/{meeting_id}/transcript", response_model=TranscriptPage)
async def get_transcript(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
    from_ms: Annotated[int | None, Query(ge=0)] = None,
) -> TranscriptPage:
    """Finals only, in spoken order. No `org_id` parameter: RLS scopes it to the caller.

    `from_ms` is the plan's "timestamp jump via `(meeting_id, start_ms)`" (section 3.8 step 2):
    seek straight to the first final at or after a millisecond offset. It is a server-side seek
    rather than something the client does after loading everything, because "load the whole
    meeting and scroll" only holds while a meeting fits in one page. The capacity model says
    ~360 finals and `MAX_LIMIT` is 500, so today it usually would - but a long meeting is
    exactly the case where jumping matters most, and that is the case where the client-side
    shortcut fails. Both forms use the same `(meeting_id, start_ms)` index.

    `cursor` wins if both are given: a cursor is a position in a walk already in progress, and
    silently restarting that walk somewhere else would repeat or skip rows.
    """
    await _assert_meeting_visible(session, meeting_id)

    params: dict[str, Any] = {"meeting_id": str(meeting_id), "limit": limit + 1}
    # `is_final` is filtered even though `transcription-service` only ever persists finals. The
    # column exists in the table, so a reader that trusts the writer's discipline rather than
    # stating the predicate would silently start serving interims the day anything changes.
    where = ["meeting_id = :meeting_id", "is_final = true"]

    if cursor:
        start_ms, row_id = _decode_cursor(cursor)
        where.append("(start_ms, id) > (:cur_start, :cur_id)")
        params["cur_start"] = start_ms
        params["cur_id"] = str(row_id)
    elif from_ms is not None:
        where.append("start_ms >= :from_ms")
        params["from_ms"] = from_ms

    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLUMNS} FROM transcript_chunks WHERE {' AND '.join(where)} "
                    "ORDER BY start_ms ASC, id ASC LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )

    has_more = len(rows) > limit
    page = list(rows[:limit])
    return TranscriptPage(
        items=[TranscriptChunk(**dict(r)) for r in page],
        next_cursor=(
            _encode_cursor(page[-1]["start_ms"], page[-1]["id"]) if has_more and page else None
        ),
    )
