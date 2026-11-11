"""bff - backend-for-frontend: screen-shaped read aggregation.

No domain ownership and no writes: every screen route here is a GET. Two Phase 4 routes are
POSTs that still write nothing: `POST /api/v1/ask` (a question, answered by rag-service) and
`POST /mcp` (the MCP server, JSON-RPC; its one write tool calls core-api's own route as the
caller). The only caller of `/bff/*` is
`mytheus-ui`. If a screen needs to change something, it calls the owning service directly,
because a write path through here would make `bff` a second writer for a store it does not own.
"""

from libs.shared.app_factory import create_app
from services.bff import (
    ask,
    dashboard,
    lineage,
    mcp,
    meeting_screens,
    oauth_metadata,
    org_chart,
    people,
    precheck,
)

app = create_app("bff")
app.include_router(dashboard.router)
app.include_router(org_chart.router)
app.include_router(people.router)
app.include_router(lineage.router)
app.include_router(meeting_screens.router)
app.include_router(ask.router)  # POST /api/v1/ask (public API, Phase 4)
app.include_router(mcp.router)  # POST /mcp (MCP server, Phase 4)
# GET /.well-known/oauth-protected-resource[/mcp] (RFC 9728): how an MCP client such as Claude
# finds the authorization server (identity-tenant-service) for `/mcp`.
app.include_router(oauth_metadata.router)
app.include_router(precheck.router)  # POST /bff/meetings/precheck (polish wave 3)
