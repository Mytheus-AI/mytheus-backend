"""The company time machine: `GET /api/v1/time-machine/{snapshot,diff,timeline}`.

- `snapshot?as_of=YYYY-MM-DD[&topic=]`: the decision graph as it stood at the end of that day.
- `diff?from=&to=[&topic=]`: what changed between the two dates, and which meetings changed it.
- `timeline?topic=[&from=&to=]`: one topic's decisions in order, each citing meeting and quote.

Participant-scoped like the decision log: only what the caller was in (`time_machine.py`). No
tenant parameter (RLS). Dates in the future are refused; a date before the workspace existed
is simply empty.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import time_machine as tm

router = APIRouter(prefix="/api/v1/time-machine", tags=["time-machine"])

Topic = Annotated[str | None, Query(max_length=120)]


def _not_future(d: date) -> date:
    if d > datetime.now(UTC).date():
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "The time machine only goes back, not forward"
        )
    return d


async def _world(session: AsyncSession, ctx: RequestContext) -> tm.World:
    if ctx.user_id is None:
        raise not_found()
    return await tm.load(session, ctx.user_id)


@router.get("/snapshot", response_model=tm.Snapshot)
async def snapshot(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    as_of: Annotated[date, Query()],
    topic: Topic = None,
) -> tm.Snapshot:
    """The company's decisions, open promises and goals as they stood at the end of `as_of`."""
    return tm.snapshot(await _world(session, ctx), _not_future(as_of), (topic or "").strip())


@router.get("/diff", response_model=tm.Diff)
async def diff(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    from_date: Annotated[date, Query(alias="from")],
    to_date: Annotated[date, Query(alias="to")],
    topic: Topic = None,
) -> tm.Diff:
    """What changed between two dates: new and reversed decisions, promises made, kept and
    broken, bets resolved, goals that moved, and the meetings that caused each change."""
    return tm.diff(
        await _world(session, ctx),
        _not_future(from_date),
        _not_future(to_date),
        (topic or "").strip(),
    )


@router.get("/timeline", response_model=tm.Timeline)
async def timeline(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    topic: Annotated[str, Query(min_length=2, max_length=120)],
    from_date: Annotated[date | None, Query(alias="from")] = None,
    to_date: Annotated[date | None, Query(alias="to")] = None,
) -> tm.Timeline:
    """How one topic's decisions changed, step by step, each citing the meeting and the
    words that decided it."""
    if not tm.topic_stems(topic):
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "Name a topic, such as pricing or hiring"
        )
    return tm.timeline(await _world(session, ctx), topic.strip(), from_date, to_date)
