"""The uniform confidence policy (doc 08 section 6), shared by the consumer and the routes.

Per tenant via `tenant_settings.agent_confidence_overrides`, shaped
`{"<Agent>": {"surface": x, "silent": y}}` (mytheus-docs ADR-0030 D3). `core-api` decides the
band, not the AI tier: the consumer applies `silent` when it persists, and
`GET /meetings/:id/candidates` applies `surface` when it serves.
"""

from __future__ import annotations

from typing import Any

SURFACE_ABOVE = 0.7
SILENT_FROM = 0.4


def thresholds(overrides: Any, agent: str) -> tuple[float, float]:
    """(surface_above, silent_from) for this agent, from the tenant's overrides if valid."""
    surface, silent = SURFACE_ABOVE, SILENT_FROM
    entry = overrides.get(agent) if isinstance(overrides, dict) else None
    if isinstance(entry, dict):
        try:
            surface = float(entry.get("surface", surface))
            silent = float(entry.get("silent", silent))
        except (TypeError, ValueError):
            return SURFACE_ABOVE, SILENT_FROM
    return surface, silent
