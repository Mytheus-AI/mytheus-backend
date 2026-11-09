"""bff - backend-for-frontend: screen-shaped read aggregation.

No domain ownership and no writes: every route here is a GET. The only caller of `/bff/*` is
`mytheus-ui`. If a screen needs to change something, it calls the owning service directly,
because a write path through here would make `bff` a second writer for a store it does not own.
"""

from libs.shared.app_factory import create_app
from services.bff import dashboard, lineage, org_chart, people

app = create_app("bff")
app.include_router(dashboard.router)
app.include_router(org_chart.router)
app.include_router(people.router)
app.include_router(lineage.router)
