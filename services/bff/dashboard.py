"""The dashboard composite. Plan section 3.9.

`bff` is screen-shaped, not resource-shaped: one request per screen, so the browser is not
making six round trips over a mobile connection to paint one page. It owns no domain and
writes nothing.

Two properties matter more than the payload itself:

- **Every arm degrades independently.** A slow or failing arm returns its `status` and an
  empty list; it never fails the screen. A dashboard that 500s because the decisions panel is
  down is worse than one that renders four panels and an apology in the fifth.
- **The unbuilt arms are shaped, not absent.** `action_items`, `worklogs` and `decisions`
  return `[]` with a settled status today. The frontend renders its empty states against the
  real shape now, so the band that populates them is a data change rather than a UI change.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["bff"])

# Per-arm budget. Deliberately well under any sane client timeout: the screen must come back
# with a degraded panel long before the browser gives up on the whole request.
ARM_TIMEOUT_SECONDS = 2.0
DEFAULT_UPCOMING = 10

ArmStatus = Literal["ok", "degraded", "empty"]


class Arm(BaseModel):
    """One panel. `status` is part of the contract, not diagnostics.

    The client needs to tell "you have no meetings" from "we could not load your meetings",
    because those call for completely different empty states, and guessing from an empty list
    gets it wrong half the time.
    """

    status: ArmStatus
    items: list[dict[str, Any]] = Field(default_factory=list)


class Dashboard(BaseModel):
    upcoming_meetings: Arm
    action_items: Arm
    worklogs: Arm
    decisions: Arm

    @property
    def degraded(self) -> bool:
        return any(
            arm.status == "degraded"
            for arm in (self.upcoming_meetings, self.action_items, self.worklogs, self.decisions)
        )


async def _arm(name: str, load: Callable[[], Awaitable[list[dict[str, Any]]]]) -> Arm:
    """Run one arm under a timeout, degrading rather than raising.

    Catches broadly on purpose. Anything an arm can raise - a timeout, a downstream 500, a
    serialization error - has the same correct handling at this layer: show the rest of the
    screen. Letting one arm's exception escape would make the composite strictly worse than
    the six round trips it replaced.
    """
    try:
        items = await asyncio.wait_for(load(), timeout=ARM_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning("bff dashboard: arm %s timed out after %ss", name, ARM_TIMEOUT_SECONDS)
        return Arm(status="degraded")
    except Exception:  # noqa: BLE001 - one arm must never take down the screen
        logger.exception("bff dashboard: arm %s failed", name)
        return Arm(status="degraded")
    return Arm(status="ok" if items else "empty", items=items)


@router.get("/dashboard", response_model=Dashboard)
async def dashboard(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=50)] = DEFAULT_UPCOMING,
) -> Dashboard:
    """One composite payload for the user dashboard, org-scoped by RLS."""

    async def upcoming_meetings() -> list[dict[str, Any]]:
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
        return [
            {
                "id": str(r["id"]),
                "title": r["title"],
                "status": r["status"],
                "scheduled_start": (
                    r["scheduled_start"].isoformat() if r["scheduled_start"] else None
                ),
                "scheduled_end": r["scheduled_end"].isoformat() if r["scheduled_end"] else None,
                "livekit_room": r["livekit_room"],
            }
            for r in rows
        ]

    async def not_yet_built() -> list[dict[str, Any]]:
        """Shaped and settled, not degraded.

        `empty` is the honest status: these read models genuinely have nothing in them yet, as
        opposed to having failed to load. Reporting `degraded` would make the client render a
        permanent error state for something that is working exactly as intended.
        """
        return []

    # Arms are gathered concurrently, so the screen costs the slowest arm rather than the sum.
    meetings_arm, actions_arm, worklogs_arm, decisions_arm = await asyncio.gather(
        _arm("upcoming_meetings", upcoming_meetings),
        _arm("action_items", not_yet_built),
        _arm("worklogs", not_yet_built),
        _arm("decisions", not_yet_built),
    )

    return Dashboard(
        upcoming_meetings=meetings_arm,
        action_items=actions_arm,
        worklogs=worklogs_arm,
        decisions=decisions_arm,
    )
