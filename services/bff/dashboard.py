"""The dashboard composite. Plan section 3.9.

`bff` is screen-shaped, not resource-shaped: one request per screen, so the browser is not
making six round trips over a mobile connection to paint one page. It owns no domain and
writes nothing.

Two properties matter more than the payload itself:

- **Every arm degrades independently.** A slow or failing arm returns its `status` and an
  empty list; it never fails the screen. A dashboard that 500s because the decisions panel is
  down is worse than one that renders four panels and an apology in the fifth.
- **Every arm is real.** `upcoming_meetings` and `worklogs` since Plan 7; `action_items`
  (the caller's open tasks), `decisions` (recent confirmed decisions from the caller's
  meetings), `recent_meetings` (the caller's ended meetings with what was confirmed in them)
  and `needs_review` (agent candidates in the caller's meetings still waiting for a person)
  since the real-data pass of 2026-11-10. Visibility follows `core-api`'s own rules: a task is
  its assignee's, a decision or candidate is its meeting participants'.

An arm that HAS data declares its item type, and that is load-bearing rather than tidiness.
This module is the source the OpenAPI document is emitted from (ADR-0022), so the models here
are the generated client's types. `items: list[dict[str, Any]]` emits as an unconstrained
object map, and a client generated from that gets `Record<string, unknown>[]` - strictly
weaker than the hand-written types the generated client exists to replace. So the typing has
to be right here, or generating the client makes the frontend worse rather than better.

`UnbuiltArm` was the open-map placeholder for the arms with no read model; it is gone now that
every arm has one, which is the visible rename its docstring promised.

The arms share one `AsyncSession`, and a session runs one statement at a time, so the loaders
take `_db` (an `asyncio.Lock`) around each query. The arms are still gathered, so each keeps its
own timeout and failure isolation; what they no longer do is race one connection.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from services.core_api.learning import band_for
from services.core_api.schemas import MeetingStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["bff"])

# Per-arm budget. Deliberately well under any sane client timeout: the screen must come back
# with a degraded panel long before the browser gives up on the whole request.
ARM_TIMEOUT_SECONDS = 2.0
DEFAULT_UPCOMING = 10
MAX_TASKS = 50
RECENT_DECISIONS = 5
RECENT_MEETINGS = 4
MAX_REVIEW = 10


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


class DashboardTask(BaseModel):
    """One of the caller's open (confirmed, not done) action items, for Your tasks."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    content: str
    due_date: date | None
    created_at: datetime


class TasksArm(_ArmBase[DashboardTask]):
    pass


class DashboardDecision(BaseModel):
    """A confirmed decision from one of the caller's meetings, for Recent decisions."""

    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    content: str
    rationale: str | None
    created_at: datetime


class DecisionsArm(_ArmBase[DashboardDecision]):
    pass


class DashboardRecentMeeting(BaseModel):
    """An ended meeting the caller was in, with what was confirmed in it."""

    id: uuid.UUID
    title: str | None
    scheduled_start: datetime | None
    scheduled_end: datetime | None
    created_at: datetime
    participant_count: int
    decision_count: int
    action_count: int


class RecentMeetingsArm(_ArmBase[DashboardRecentMeeting]):
    pass


class DashboardReviewItem(BaseModel):
    """An agent candidate (proposed, above the tenant's surface threshold) in one of the
    caller's meetings that nobody has confirmed or dismissed yet."""

    kind: Literal["decision", "action"]
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    content: str
    created_at: datetime


class NeedsReviewArm(_ArmBase[DashboardReviewItem]):
    pass


class Dashboard(BaseModel):
    upcoming_meetings: MeetingsArm
    action_items: TasksArm
    worklogs: WorklogsArm
    decisions: DecisionsArm
    recent_meetings: RecentMeetingsArm
    needs_review: NeedsReviewArm

    @property
    def degraded(self) -> bool:
        return any(
            arm.status == "degraded"
            for arm in (
                self.upcoming_meetings,
                self.action_items,
                self.worklogs,
                self.decisions,
                self.recent_meetings,
                self.needs_review,
            )
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
    db = asyncio.Lock()
    me = str(ctx.user_id) if ctx.user_id is not None else None

    async def rows(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        async with db:
            return [dict(r) for r in (await session.execute(text(sql), params)).mappings().all()]

    async def upcoming_meetings() -> list[DashboardMeeting]:
        found = await rows(
            "SELECT id, title, status, scheduled_start, scheduled_end, livekit_room "
            "FROM meetings "
            "WHERE deleted_at IS NULL AND status IN ('scheduled', 'live') "
            # NULLS LAST so undated meetings sort after dated ones instead of
            # monopolising the top of the panel, which is what ASC does by default.
            "ORDER BY scheduled_start ASC NULLS LAST, created_at DESC " "LIMIT :limit",
            {"limit": limit},
        )
        # Built as models rather than dicts so the columns are checked here, at the one place
        # that knows what the SELECT returned, instead of arriving at the client as whatever
        # the driver happened to hand back.
        return [DashboardMeeting(**r) for r in found]

    async def my_notes() -> list[DashboardNote]:
        """The caller's most recent surfaced notes across meetings. Their own only: a note is
        private to the person whose agent wrote it (ADR-0030), so `user_id` is the caller."""
        if me is None:
            return []
        found = await rows(
            "SELECT w.id, w.meeting_id, m.title AS meeting_title, w.content, "
            "w.created_at FROM worklogs w JOIN meetings m ON m.id = w.meeting_id "
            "WHERE w.user_id = :u AND w.surfaced AND w.deleted_at IS NULL "
            "AND m.deleted_at IS NULL "
            "ORDER BY w.created_at DESC, w.id DESC LIMIT :limit",
            {"u": me, "limit": limit},
        )
        return [DashboardNote(**r) for r in found]

    async def my_tasks() -> list[DashboardTask]:
        """The caller's open tasks, soonest due first, undated last: the same rows and order as
        `GET /api/v1/actions?assignee_id=me`, so the panel and the Tasks screen agree."""
        if me is None:
            return []
        found = await rows(
            "SELECT a.id, a.meeting_id, m.title AS meeting_title, a.content, a.due_date, "
            "a.created_at FROM action_items a JOIN meetings m ON m.id = a.meeting_id "
            "WHERE a.assignee_id = :u AND a.status = 'confirmed' "
            "AND a.deleted_at IS NULL AND m.deleted_at IS NULL "
            "ORDER BY a.due_date ASC NULLS LAST, a.created_at ASC, a.id ASC LIMIT :limit",
            {"u": me, "limit": MAX_TASKS},
        )
        return [DashboardTask(**r) for r in found]

    async def recent_decisions() -> list[DashboardDecision]:
        """Confirmed decisions from meetings the caller was in, newest first. Participant-scoped
        like `GET /api/v1/decisions` (ADR-0033 D1)."""
        if me is None:
            return []
        found = await rows(
            "SELECT d.id, d.meeting_id, m.title AS meeting_title, d.content, d.rationale, "
            "d.created_at FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
            "WHERE d.status = 'confirmed' AND d.deleted_at IS NULL AND m.deleted_at IS NULL "
            "AND EXISTS (SELECT 1 FROM meeting_participants p "
            "            WHERE p.meeting_id = d.meeting_id AND p.user_id = :u) "
            "ORDER BY d.created_at DESC, d.id DESC LIMIT :limit",
            {"u": me, "limit": RECENT_DECISIONS},
        )
        return [DashboardDecision(**r) for r in found]

    async def recent_meetings() -> list[DashboardRecentMeeting]:
        """The caller's ended meetings, most recent first, with how many people were in each
        and how many decisions and tasks were confirmed there."""
        if me is None:
            return []
        found = await rows(
            "SELECT m.id, m.title, m.scheduled_start, m.scheduled_end, m.created_at, "
            "(SELECT count(*) FROM meeting_participants p2 WHERE p2.meeting_id = m.id) "
            "  AS participant_count, "
            "(SELECT count(*) FROM decisions d WHERE d.meeting_id = m.id "
            "  AND d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL) "
            "  AS decision_count, "
            "(SELECT count(*) FROM action_items a WHERE a.meeting_id = m.id "
            "  AND a.status IN ('confirmed', 'done') AND a.deleted_at IS NULL) "
            "  AS action_count "
            "FROM meetings m JOIN meeting_participants p ON p.meeting_id = m.id "
            "WHERE p.user_id = :u AND m.status = 'ended' AND m.deleted_at IS NULL "
            "ORDER BY COALESCE(m.scheduled_start, m.created_at) DESC, m.id DESC LIMIT :limit",
            {"u": me, "limit": RECENT_MEETINGS},
        )
        return [DashboardRecentMeeting(**r) for r in found]

    async def needs_review() -> list[DashboardReviewItem]:
        """Proposed candidates in the caller's meetings that `GET /meetings/:id/candidates`
        would surface (above the tenant's per-agent threshold), newest first. The 0.4 to 0.7
        silent band is stored but never served, here as there. A card handed to a named owner
        at the room's close-out (`review_owner_id`, polish program) is that person's alone."""
        if me is None:
            return []
        async with db:
            # the workspace's calibrated thresholds (polish wave 2), as the candidates read
            decision_surface, _ = await band_for(session, "DecisionAgent")
            action_surface, _ = await band_for(session, "ActionAgent")
        found = await rows(
            "SELECT * FROM ("
            " SELECT 'decision' AS kind, d.id, d.meeting_id, m.title AS meeting_title, "
            " d.content, d.created_at FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
            " WHERE d.status = 'proposed' AND d.confidence > :ds AND d.deleted_at IS NULL "
            " AND (d.review_owner_id IS NULL OR d.review_owner_id = :u) "
            " AND m.deleted_at IS NULL AND EXISTS (SELECT 1 FROM meeting_participants p "
            "  WHERE p.meeting_id = d.meeting_id AND p.user_id = :u) "
            " UNION ALL "
            " SELECT 'action' AS kind, a.id, a.meeting_id, m.title AS meeting_title, "
            " a.content, a.created_at FROM action_items a JOIN meetings m ON m.id = a.meeting_id "
            " WHERE a.status = 'proposed' AND a.confidence > :as AND a.deleted_at IS NULL "
            " AND (a.review_owner_id IS NULL OR a.review_owner_id = :u) "
            " AND m.deleted_at IS NULL AND EXISTS (SELECT 1 FROM meeting_participants p "
            "  WHERE p.meeting_id = a.meeting_id AND p.user_id = :u) "
            ") c ORDER BY created_at DESC, id DESC LIMIT :limit",
            {"u": me, "ds": decision_surface, "as": action_surface, "limit": MAX_REVIEW},
        )
        return [DashboardReviewItem(**r) for r in found]

    # Gathered, so each arm keeps its own timeout and failure isolation.
    (
        meetings_arm,
        actions_arm,
        worklogs_arm,
        decisions_arm,
        recent_arm,
        review_arm,
    ) = await asyncio.gather(
        _arm("upcoming_meetings", MeetingsArm, upcoming_meetings),
        _arm("action_items", TasksArm, my_tasks),
        _arm("worklogs", WorklogsArm, my_notes),
        _arm("decisions", DecisionsArm, recent_decisions),
        _arm("recent_meetings", RecentMeetingsArm, recent_meetings),
        _arm("needs_review", NeedsReviewArm, needs_review),
    )

    return Dashboard(
        upcoming_meetings=meetings_arm,
        action_items=actions_arm,
        worklogs=worklogs_arm,
        decisions=decisions_arm,
        recent_meetings=recent_arm,
        needs_review=review_arm,
    )
