"""Per-user channel preferences: the defaults and the one read both the consumer and the routes use.

A channel delivers when `enabled` and `priority <= max_priority` (priority 1 is the most
urgent). A user with no row for a channel gets the default below: in-app for everything, and
email and Slack OFF until the user opts in. Email is opt-in on purpose: nobody is mailed by
the product until they asked to be. (P12 builds the Slack send; until then an enabled Slack
channel records a `skipped` receipt.)

`in_app` is always on: the `notification_log` row IS the in-app delivery and the recipient's
inbox, so it is written for every notification regardless. `PUT /notifications/preferences`
forces `in_app` to `{enabled: true, max_priority: 3}` so the stored value never claims
otherwise.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

Channel = Literal["in_app", "email", "slack"]
CHANNELS: tuple[Channel, ...] = ("in_app", "email", "slack")


@dataclass(frozen=True)
class ChannelPref:
    channel: Channel
    enabled: bool
    max_priority: int

    def allows(self, priority: int) -> bool:
        return self.enabled and priority <= self.max_priority


DEFAULTS: dict[Channel, ChannelPref] = {
    "in_app": ChannelPref("in_app", True, 3),
    "email": ChannelPref("email", False, 1),  # opt-in: no email until the user turns it on
    "slack": ChannelPref("slack", False, 1),
}
IN_APP_FORCED = DEFAULTS["in_app"]


async def load_prefs(session: AsyncSession, user_id: uuid.UUID) -> dict[Channel, ChannelPref]:
    """The user's three channel preferences, defaults filled in. RLS scopes the read."""
    out = dict(DEFAULTS)
    rows = await session.execute(
        text(
            "SELECT channel, enabled, max_priority FROM notification_preferences "
            "WHERE user_id = :u"
        ),
        {"u": str(user_id)},
    )
    for channel, enabled, max_priority in rows:
        if channel in out:
            out[channel] = ChannelPref(channel, bool(enabled), int(max_priority))
    out["in_app"] = IN_APP_FORCED
    return out
