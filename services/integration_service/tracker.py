"""Which project tracker a tenant uses: Jira or Linear, never both (R8, ADR-0044 D1, D2).

Everything that reads "the tenant's tracker" (the `actions` consumer, issue status for promise
drift and bets, the assumption watch) asks here instead of naming Jira, so it works for whichever
of the two is connected.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from services.integration_service import config, store
from services.integration_service.clients import Clients
from services.integration_service.connectors import jira, linear


async def active(session: AsyncSession) -> tuple[str, store.Connection] | None:
    """The tenant's one active tracker connection, provider name first, or None."""
    for provider in (config.JIRA, config.LINEAR):
        conn = await store.get_connection(session, provider, active_only=True)
        if conn is not None:
            return provider, conn
    return None


async def context(session: AsyncSession, provider: str, conn: store.Connection) -> dict[str, Any]:
    """What a re-read needs besides the token: the Jira site, or the Linear team cursor."""
    if provider == config.JIRA:
        return jira._site(await store.get_cursor(session, conn.id, jira.SITE_KIND), conn)
    return await linear.team(session, conn)


async def push_action(
    session: AsyncSession, org_id: Any, action: dict[str, Any], clients: Clients
) -> str:
    """Outbound for one confirmed action, to whichever tracker is connected."""
    found = await active(session)
    if found is None:
        return "not_connected"
    if found[0] == config.LINEAR:
        return await linear.push_action(session, org_id, action, clients)
    return await jira.push_action(session, org_id, action, clients)


async def read_issue(
    provider: str, clients: Clients, token: str, ctx: dict[str, Any], key: str
) -> dict[str, Any] | None:
    """Re-read one issue from its tracker; None when the tracker no longer has it."""
    if provider == config.LINEAR:
        return await clients.linear.get_issue(token, key)
    return await clients.jira.get_issue(token, str(ctx["cloud_id"]), key)


def map_status(provider: str, issue: dict[str, Any]) -> str:
    return linear.map_status(issue) if provider == config.LINEAR else jira.map_status(issue)


def browse_url(provider: str, ctx: dict[str, Any], issue: dict[str, Any], key: str) -> str | None:
    if provider == config.LINEAR:
        url = issue.get("url")
        return str(url) if url else None
    return jira._browse_url(ctx, key)
