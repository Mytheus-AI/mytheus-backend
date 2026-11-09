"""The dashboard composite. Plan section 3.9.

`bff` is screen-shaped, not resource-shaped: one request per screen, so the browser is not
making six round trips over a mobile connection to paint one page. It owns no domain and
writes nothing.

Two properties matter more than the payload itself:

- **Every arm degrades independently.** A slow or failing arm returns its `status` and an
  empty list; it never fails the screen. A dashboard that 500s because the decisions panel is
  down is worse than one that renders four panels and an apology in the fifth.
- **The unbuilt arms are shaped, not absent.** `action_items` and `decisions` return `[]`
  with a settled status today (`worklogs` is real since Plan 7: the caller's own notes).
  The frontend renders its empty states against the real shape now, so the band that
  populates them is a data change rather than a UI change.

An arm that HAS data declares its item type, and that is load-bearing rather than tidiness.
This module is the source the OpenAPI document is emitted from (ADR-0022), so the models here
are the generated client's types. `items: list[dict[str, Any]]` emits as an unconstrained
object map, and a client generated from that gets `Record<string, unknown>[]` - strictly
weaker than the hand-written types the generated client exists to replace. So the typing has
to be right here, or generating the client makes the frontend worse rather than better.

The three arms with no read model behind them keep the open map on purpose; `UnbuiltArm` says
why, and why widening it was tried and reverted.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from services.core_api.schemas import MeetingStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["bff"])

# Per-arm budget. Deliberately well under any sane client timeout: the screen must come back
# with a degraded panel long before the browser gives up on the whole request.
ARM_TIMEOUT_SECONDS = 2.0
DEFAULT_UPCOMING = 10

ArmStatus = Literal["ok", "degraded", "empty"]


class DashboardMeeting(BaseModel):
    """The meetings-panel projection: six columns, not the whole `Meeting` resource.

    `status` is imported from `core-api`, which owns `meetings`, rather than restated here.
    A second copy of the four values would be a second place for them to drift, and this
    program has already found two copies of one thing diverging silently.
    """

    id: uuid.UUID
    title: str | None
    status: MeetingStatus
    scheduled_start: datetime | None
    scheduled_end: datetime | None
    livekit_room: str | None


class _ArmBase[ItemT](BaseModel):
    """One panel. `status` is part of the contract, not diagnostics.

    The client needs to tell "you have no meetings" from "we could not load your meetings",
    because those call for completely different empty states, and guessing from an empty list
    gets it wrong half the time.

    Generic in the item type, but never used parameterized on the wire: each panel is a named
    subclass below, so the emitted schema carries `MeetingsArm` rather than the `_ArmBase_...`
    name Pydantic derives for a parameterized generic. `items` has no default, so it is
    `required` in the schema and a generated client types it as present - which is what the
    server actually always sends.
    """

    status: ArmStatus
    items: list[ItemT]


class MeetingsArm(_ArmBase[DashboardMeeting]):
    pass


class DashboardNote(BaseModel):
    """One of the caller's own notes (a surfaced `worklogs` row, PersonalListenerAgent output)
    with the meeting it came from, for the dashboard's My notes panel."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    content: str
    created_at: datetime


class WorklogsArm(_ArmBase[DashboardNote]):
    pass


class UnbuiltArm(_ArmBase[dict[str, Any]]):
    """A panel whose read model does not exist yet, so its item type stays open.

    `action_items`, `worklogs` and `decisions` are shaped-but-empty until Plan 7-9 builds the
    read models behind them. Inventing an item shape now would freeze a wire contract that
    band has to either honour or break, on no evidence, and a generated client would carry
    that invention to every caller as though it were measured.

    The item type is therefore left exactly as it is on the wire today, an open object map.
    Widening it to `Any` was tried and reverted: `oasdiff breaking` reports `object` -> `any`
    as a `response-property-type-changed` on all three arms, and no client can observe the
    difference because these arrays are always empty. Loosening a live response type to make
    an always-empty array read better is not worth a breaking-change entry.

    The name is the signal: when a read model lands, this class is replaced by a typed arm,
    and that rename is a visible breaking change rather than a silent shape swap.
    """


class Dashboard(BaseModel):
    upcoming_meetings: MeetingsArm
    action_items: UnbuiltArm
    worklogs: WorklogsArm
    decisions: UnbuiltArm

    @property
    def degraded(self) -> bool:
        return any(
            arm.status == "degraded"
            for arm in (self.upcoming_meetings, self.action_items, self.worklogs, self.decisions)
        )


async def _arm[ArmT: _ArmBase[Any]](
    name: str, arm: type[ArmT], load: Callable[[], Awaitable[list[Any]]]
) -> ArmT:
    """Run one arm under a timeout, degrading rather than raising.

    Catches broadly on purpose. Anything an arm can raise - a timeout, a downstream 500, a
    serialization error - has the same correct handling at this layer: show the rest of the
    screen. Letting one arm's exception escape would make the composite strictly worse than
    the six round trips it replaced.

    The arm class is a parameter rather than a fixed type because each panel carries its own
    item type; `load` stays loosely typed because the arm model is what validates the items,
    so a loader that returns the wrong shape degrades its own panel instead of the screen.
    """
    try:
        items = await asyncio.wait_for(load(), timeout=ARM_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning("bff dashboard: arm %s timed out after %ss", name, ARM_TIMEOUT_SECONDS)
        return arm(status="degraded", items=[])
    except Exception:  # noqa: BLE001 - one arm must never take down the screen
        logger.exception("bff dashboard: arm %s failed", name)
        return arm(status="degraded", items=[])
    return arm(status="ok" if items else "empty", items=items)


@router.get("/dashboard", response_model=Dashboard)
async def dashboard(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=50)] = DEFAULT_UPCOMING,
) -> Dashboard:
    """One composite payload for the user dashboard, org-scoped by RLS."""

    async def upcoming_meetings() -> list[DashboardMeeting]:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT id, title, status, scheduled_start, scheduled_end, livekit_room "
                        "FROM meetings "
                        "WHERE deleted_at IS NULL AND status IN ('scheduled', 'live') "
                        # NULLS LAST so undated meetings sort after dated ones instead of
                        # monopolising the top of the panel, which is what ASC does by default.
                        "ORDER BY scheduled_start ASC NULLS LAST, created_at DESC "
                        "LIMIT :limit"
                    ),
                    {"limit": limit},
                )
            )
            .mappings()
            .all()
        )
        # Built as models rather than dicts so the columns are checked here, at the one place
        # that knows what the SELECT returned, instead of arriving at the client as whatever
        # the driver happened to hand back.
        return [
            DashboardMeeting(
                id=r["id"],
                title=r["title"],
                status=r["status"],
                scheduled_start=r["scheduled_start"],
                scheduled_end=r["scheduled_end"],
                livekit_room=r["livekit_room"],
            )
            for r in rows
        ]

    async def my_notes() -> list[DashboardNote]:
        """The caller's most recent surfaced notes across meetings. Their own only: a note is
        private to the person whose agent wrote it (ADR-0030), so `user_id` is the caller."""
        if ctx.user_id is None:
            return []
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT w.id, w.meeting_id, m.title AS meeting_title, w.content, "
                        "w.created_at FROM worklogs w JOIN meetings m ON m.id = w.meeting_id "
                        "WHERE w.user_id = :u AND w.surfaced AND w.deleted_at IS NULL "
                        "AND m.deleted_at IS NULL "
                        "ORDER BY w.created_at DESC, w.id DESC LIMIT :limit"
                    ),
                    {"u": str(ctx.user_id), "limit": limit},
                )
            )
            .mappings()
            .all()
        )
        return [DashboardNote(**dict(r)) for r in rows]

    async def not_yet_built() -> list[Any]:
        """Shaped and settled, not degraded.

        `empty` is the honest status: these read models genuinely have nothing in them yet, as
        opposed to having failed to load. Reporting `degraded` would make the client render a
        permanent error state for something that is working exactly as intended.
        """
        return []

    # Arms are gathered concurrently, so the screen costs the slowest arm rather than the sum.
    meetings_arm, actions_arm, worklogs_arm, decisions_arm = await asyncio.gather(
        _arm("upcoming_meetings", MeetingsArm, upcoming_meetings),
        _arm("action_items", UnbuiltArm, not_yet_built),
        _arm("worklogs", WorklogsArm, my_notes),
        _arm("decisions", UnbuiltArm, not_yet_built),
    )

    return Dashboard(
        upcoming_meetings=meetings_arm,
        action_items=actions_arm,
        worklogs=worklogs_arm,
        decisions=decisions_arm,
    )
