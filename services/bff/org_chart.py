"""The org-chart composite: departments -> teams -> members. Plan 5-6 section 3.5.

Two properties matter more than the payload shape itself, per this band's resolution of the
task brief's judgement call:

- **Flat query count, proven not assumed.** The whole tree is three SELECTs - departments,
  teams, team-members-joined-to-users - regardless of how deep or wide the tree is. The tree
  itself is assembled from those three already-fetched result sets in Python; no node triggers
  a query of its own. `tests/test_bff_org_chart.py::test_query_count_stays_constant_as_the_
  tree_grows` proves this against a tree that is deliberately grown mid-test, rather than
  trusting that it "looks fine" against the seeder's small fixture.
- **Every row in the walk is tenant-scoped by construction, not by re-checking `org_id` at
  each hop.** All three SELECTs run through `session`, which `tenant_session` has already
  bound to `app.current_org` transaction-locally before this module ever sees it (same
  contract as every other tenant-scoped query in this codebase - see `libs/shared/authz.py`).
  A `parent_id`/`department_id`/`team_id` reference that pointed outside the tenant would
  require a row in another org's `departments`/`teams` to have been readable in the first
  place, which RLS already prevents, and which `core_api/routes/org_hierarchy.py::create_
  department`'s own comment names directly: "a parent in another tenant is invisible to RLS,
  so the FK cannot resolve and insert fails." So the walk cannot cross a tenant boundary even
  in principle - there is no cross-tenant row for a Python-side `parent_id` lookup to find.
  `test_chart_is_tenant_scoped_for_two_orgs_with_sibling_shaped_hierarchies` proves this with
  two orgs whose department/team names are IDENTICAL, asserting isolation by id rather than by
  name (a name-only assertion would not catch a leak between same-named nodes).

Follows `bff/dashboard.py`'s degrade pattern - the one rule `bff` must not break is that a
failing or slow arm degrades the screen, never fails it. This endpoint is one arm (the tree
build is a single coupled unit: a department subtree missing its teams is not a meaningfully
different failure from missing everything), so on timeout or exception it returns `status:
"degraded"` with empty collections rather than a 500.

`teams.department_id` is nullable (`core_api/routes/org_hierarchy.py::create_team` allows it).
A team with no department cannot appear in a department-rooted tree, but dropping it silently
would be a data loss the UI could not detect - so it surfaces separately in `unassigned_teams`
rather than vanishing, echoing `dashboard.py`'s "shaped, not absent" principle for the arms it
has nothing to show yet.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from typing import Annotated, Literal, NamedTuple

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["bff"])

# Same budget as dashboard.py's ARM_TIMEOUT_SECONDS, kept local rather than imported: each bff
# composite owns its own timeout constant so one screen's tuning cannot silently retune another.
ARM_TIMEOUT_SECONDS = 2.0

ChartStatus = Literal["ok", "degraded", "empty"]

_SELECT_DEPARTMENTS = text(
    "SELECT id, name, parent_id FROM departments WHERE deleted_at IS NULL ORDER BY name"
)
_SELECT_TEAMS = text(
    "SELECT id, name, department_id FROM teams WHERE deleted_at IS NULL ORDER BY name"
)
_SELECT_MEMBERS = text(
    "SELECT tm.team_id, u.id AS user_id, u.email, u.role, tm.role AS role_in_team "
    "FROM team_members tm JOIN users u ON u.id = tm.user_id "
    "WHERE u.deleted_at IS NULL ORDER BY u.email"
)


class Member(BaseModel):
    """`role` is the domain role from `users.role` (e.g. "engineer"); `role_in_team` is
    `team_members.role` (e.g. "lead"/"member") - two different concepts that happen to share
    a column name in Postgres, kept distinctly named on the wire so the UI never conflates
    them (same discipline `libs/shared/authz.py` applies to `users.role` vs `rbac_grants.tier`).
    """

    user_id: uuid.UUID
    email: str
    role: str | None
    role_in_team: str | None


class TeamNode(BaseModel):
    id: uuid.UUID
    name: str
    members: list[Member]


class DepartmentNode(BaseModel):
    id: uuid.UUID
    name: str
    teams: list[TeamNode]
    children: list[DepartmentNode]


DepartmentNode.model_rebuild()


class OrgChart(BaseModel):
    status: ChartStatus
    departments: list[DepartmentNode]  # roots only; nesting expresses the hierarchy
    unassigned_teams: list[TeamNode]


class _ChartData(NamedTuple):
    departments: list[DepartmentNode]
    unassigned_teams: list[TeamNode]


async def _build_tree(session: AsyncSession) -> _ChartData:
    """Exactly three SELECTs, no matter the tree's shape. See module docstring."""
    dept_rows = (await session.execute(_SELECT_DEPARTMENTS)).mappings().all()
    team_rows = (await session.execute(_SELECT_TEAMS)).mappings().all()
    member_rows = (await session.execute(_SELECT_MEMBERS)).mappings().all()

    members_by_team: dict[str, list[Member]] = defaultdict(list)
    for m in member_rows:
        members_by_team[str(m["team_id"])].append(
            Member(
                user_id=m["user_id"],
                email=m["email"],
                role=m["role"],
                role_in_team=m["role_in_team"],
            )
        )

    teams_by_department: dict[str, list[TeamNode]] = defaultdict(list)
    unassigned_teams: list[TeamNode] = []
    for t in team_rows:
        node = TeamNode(id=t["id"], name=t["name"], members=members_by_team.get(str(t["id"]), []))
        if t["department_id"] is None:
            unassigned_teams.append(node)
        else:
            teams_by_department[str(t["department_id"])].append(node)

    dept_nodes: dict[str, DepartmentNode] = {
        str(d["id"]): DepartmentNode(
            id=d["id"],
            name=d["name"],
            teams=teams_by_department.get(str(d["id"]), []),
            children=[],
        )
        for d in dept_rows
    }

    roots: list[DepartmentNode] = []
    for d in dept_rows:
        dept_node = dept_nodes[str(d["id"])]
        parent_id = d["parent_id"]
        if parent_id is not None and str(parent_id) in dept_nodes:
            dept_nodes[str(parent_id)].children.append(dept_node)
        else:
            # `parent_id` is a genuine root (NULL), or points at a row this same RLS-scoped
            # query did not return (e.g. a soft-deleted parent). Fail OPEN into a root rather
            # than dropping the subtree: every row here already passed RLS for this tenant, so
            # there is no tenant-safety reason to hide it, only a cosmetic one.
            roots.append(dept_node)

    return _ChartData(departments=roots, unassigned_teams=unassigned_teams)


@router.get("/org/chart", response_model=OrgChart)
async def org_chart(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> OrgChart:
    """One composite payload for the org-chart screen, org-scoped by RLS via `session`."""
    try:
        data = await asyncio.wait_for(_build_tree(session), timeout=ARM_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning("bff org/chart: build timed out after %ss", ARM_TIMEOUT_SECONDS)
        return OrgChart(status="degraded", departments=[], unassigned_teams=[])
    except Exception:  # noqa: BLE001 - one arm must never take down the screen
        logger.exception("bff org/chart: build failed")
        return OrgChart(status="degraded", departments=[], unassigned_teams=[])

    has_content = bool(data.departments) or bool(data.unassigned_teams)
    return OrgChart(
        status="ok" if has_content else "empty",
        departments=data.departments,
        unassigned_teams=data.unassigned_teams,
    )
