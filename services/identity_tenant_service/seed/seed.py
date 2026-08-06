"""Idempotent dev-data seeder. Plan 5-6 section 3.4, `make seed`.

Creates 2 orgs x 10 users x 5 meetings, with realistic roles/skills, departments, teams,
team memberships and meeting participants, so the downstream P6 embedding/search/reranker work
(gated on this task being green) has real rows to point at. Run with `make seed`; safe to
re-run any number of times (see "Idempotency" below).

WHO WRITES WHAT, AND WHY (read this before touching the table list)
---------------------------------------------------------------------
This script runs as `mytheus_admin` (`ADMIN_DATABASE_URL` - see `libs/shared/db.get_admin_
sessionmaker`), the "operator seam" `.env.example` names for exactly two callers: "the tenant
seed script and RTBF purges." Elevated in GRANTS, never in isolation - every write below still
goes through `set_rls(session, org_id)` first, so FORCE ROW LEVEL SECURITY applies exactly as
it does on the request path. That grant breadth is NOT a license to bypass table ownership:

- `organizations`, `users`, `rbac_grants` - owned by `identity-tenant-service`, i.e. by this
  script's own home service. Written directly. No conflict.
- `departments`, `teams`, `meetings`, `meeting_participants` - owned by `core-api`
  (`services/core_api/routes/org_hierarchy.py`'s own docstring: "One writer per store: identity
  never writes these tables"). This script does NOT raw-INSERT into them. It calls `core-api`'s
  own route functions (`create_department`, `create_team`, `create_meeting`, `update_meeting`)
  IN-PROCESS - this is a modular monolith today (one deployable, every service a Python
  module), so "call the owner's real write path" and "import the owner's function" are the same
  operation. A hand-built `RequestContext` (tier `owner`, matching the `rbac_grants` row this
  script itself seeds for each org's first user) stands in for the FastAPI `Depends()` chain,
  which only exists to authenticate an HTTP caller - there is no HTTP caller here, only the
  owner's own code, invoked with the same session and the same tenant scoping a real request
  would carry. Every meeting created this way genuinely emits `meeting-events` through the real
  outbox, exactly as a live `POST /api/v1/meetings` call would.
- `team_members` - owned by `core-api` per the same docstring, but per this task's brief:
  *"NO route in this codebase writes `team_members` yet. If your seeder needs team membership,
  you are likely the first thing to write that table."* This IS a one-writer-per-store
  exception, and it is disclosed as one, not silently taken: both `org_hierarchy.py`'s module
  docstring and `context/service.py::invalidate_user_context`'s docstring (written by two
  earlier tasks in this same band, independently) say a `team_members` writer belongs in
  `core-api`. This script writes it anyway, on this task's explicit, considered instruction,
  scoped narrowly to this one admin-role operator script (the request-serving code in
  `identity-tenant-service` still never writes it). See `work-done-till-now` band file / the
  task report for the full citation trail. `invalidate_user_context` is called after, per the
  same instruction, even though nothing is cached yet at seed time (first-ever write, so this
  is a documented no-op today, wired for the day something IS cached).
- `transcript_chunks` (owned by `transcription-service` in `mytheus-realtime`) and `Topic`
  nodes in the AGE `nexus_knowledge` graph (owned by `knowledge-graph-service` in
  `mytheus-intelligence`, currently a bare scaffold with zero routes) are NOT written here.
  Both are genuine "no legitimate write path exists" cases - see the task report for the full
  investigation (no REST/gRPC route, no consumable topic, only each owner's own in-process
  write or a raw superuser INSERT their own tests use). Seeding them would make this script a
  second writer of two stores this repo does not own, exactly the outcome the brief calls "the
  worst available." `taxonomy.loader.load_taxonomy()` remains the ready import for whichever
  future change gets a real write path for the `Topic` side of this.

IDEMPOTENCY
------------
Every id this script creates is derived deterministically from a fixed namespace UUID plus a
stable string key (`_sid`), so re-running produces the SAME ids, not new ones. Combined with
`ON CONFLICT ... DO NOTHING` (organizations, users, rbac_grants, team_members) or an explicit
"does this already exist" `SELECT` before calling an owner function that has no natural
upsert (departments, teams, meetings - `create_department`/`create_team`/`create_meeting`
always INSERT), a second run of `make seed` writes zero additional rows. All ids are minted
under one fixed namespace (`_SEED_NAMESPACE`), so nothing this script writes can collide with
another agent's ad hoc test data in this same shared dev database.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fastapi import Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext
from libs.shared.db import get_admin_sessionmaker
from libs.shared.tenancy import set_rls
from services.core_api.routes.meetings import create_meeting, update_meeting
from services.core_api.routes.org_hierarchy import create_department, create_team
from services.core_api.schemas import (
    DepartmentCreate,
    MeetingCreate,
    MeetingUpdate,
    TeamCreate,
)
from services.identity_tenant_service.context.service import invalidate_user_context
from services.identity_tenant_service.provisioning import provision_organization

logger = logging.getLogger(__name__)

# Fixed namespace: every id this script mints is uuid5(_SEED_NAMESPACE, "<kind>:<key>"),
# deterministic across runs and impossible to collide with `uuid4()`-derived ids from anything
# else running against this shared dev database (another agent's tests, `tests/conftest.py`'s
# `tenant` fixture, etc).
_SEED_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "seed.mytheus.internal")


def _sid(*parts: str) -> uuid.UUID:
    return uuid.uuid5(_SEED_NAMESPACE, ":".join(parts))


ROLES = [
    "engineer",
    "designer",
    "product_manager",
    "sales",
    "finance",
    "engineer",
    "designer",
    "product_manager",
    "member",
    "member",
]

_SKILLS_BY_ROLE: dict[str, list[str]] = {
    "engineer": ["python", "postgresql", "kubernetes", "system-design"],
    "designer": ["figma", "user-research", "design-systems", "prototyping"],
    "product_manager": ["roadmapping", "stakeholder-management", "analytics", "prioritization"],
    "sales": ["negotiation", "crm", "pipeline-management", "closing"],
    "finance": ["forecasting", "budgeting", "financial-modeling"],
    "member": ["communication", "collaboration"],
}

_DEPARTMENTS = ["Engineering", "Product & Design", "Revenue & Operations"]

# (team name, department name, [user index -> "lead" for the first, "member" for the rest])
_TEAMS: list[tuple[str, str, list[int]]] = [
    ("Platform Engineering", "Engineering", [0, 5]),
    ("Product & Design", "Product & Design", [1, 6, 2, 7]),
    ("Revenue & Ops", "Revenue & Operations", [3, 4, 8, 9]),
]

_MEETINGS: list[tuple[str, int]] = [
    # (title, day offset from "now" - negative is past, positive is future)
    ("Sprint Planning", -12),
    ("Design Review: Onboarding Flow", -9),
    ("Customer Escalation Review", -5),
    ("Q3 Roadmap Sync", -2),
    ("All-Hands: August Update", 3),
]


@dataclass(frozen=True)
class OrgSpec:
    slug: str
    name: str
    plan_code: str
    names: list[tuple[str, str]]  # (first, last) x10, index-aligned with ROLES


ORGS = [
    OrgSpec(
        slug="acme",
        name="Acme Corporation",
        plan_code="team",
        names=[
            ("Maria", "Alvarez"),
            ("James", "Whitfield"),
            ("Priya", "Natarajan"),
            ("Tom", "Becker"),
            ("Aisha", "Bello"),
            ("Lucas", "Ferreira"),
            ("Sofia", "Kowalski"),
            ("Daniel", "Osei"),
            ("Emma", "Larsson"),
            ("Noah", "Whitman"),
        ],
    ),
    OrgSpec(
        slug="globex",
        name="Globex Corporation",
        plan_code="team",
        names=[
            ("Grace", "Lindqvist"),
            ("Marcus", "Chen"),
            ("Fatima", "Haidari"),
            ("Oliver", "Bennett"),
            ("Yuki", "Tanaka"),
            ("Isabella", "Rossi"),
            ("Ethan", "Brooks"),
            ("Chidinma", "Okafor"),
            ("Lucas", "Meier"),
            ("Ana", "Costa"),
        ],
    ),
]

_INSERT_USER = text(
    "INSERT INTO users (id, org_id, clerk_user_id, email, role, skills, bio) "
    "VALUES (:id, :org, :clerk_user_id, :email, :role, :skills, :bio) "
    "ON CONFLICT ON CONSTRAINT uq_users_org_id_clerk_user_id DO NOTHING"
)

_INSERT_RBAC_GRANT = text(
    "INSERT INTO rbac_grants (id, org_id, user_id, tier) VALUES (:id, :org, :user, :tier) "
    "ON CONFLICT ON CONSTRAINT uq_rbac_grants_org_id_user_id DO NOTHING"
)

_INSERT_TEAM_MEMBER = text(
    "INSERT INTO team_members (org_id, team_id, user_id, role) "
    "VALUES (:org, :team, :user, :role) ON CONFLICT (team_id, user_id) DO NOTHING"
)

_SELECT_DEPARTMENT_BY_NAME = text(
    "SELECT id FROM departments WHERE name = :name AND deleted_at IS NULL"
)
_SELECT_TEAM_BY_NAME = text("SELECT id FROM teams WHERE name = :name AND deleted_at IS NULL")
_SELECT_MEETING_BY_TITLE = text(
    "SELECT id FROM meetings WHERE title = :title AND deleted_at IS NULL"
)


async def _seed_users(
    session: AsyncSession, org_id: uuid.UUID, org: OrgSpec
) -> list[dict[str, object]]:
    """Identity-owned. Direct write. Returns the 10 seeded users, index-aligned with ROLES."""
    users: list[dict[str, object]] = []
    for i, ((first, last), role) in enumerate(zip(org.names, ROLES, strict=True)):
        user_id = _sid("user", org.slug, str(i))
        clerk_user_id = f"seed_user_{org.slug}_{i:02d}"
        email = f"{first.lower()}.{last.lower()}@{org.slug}.example.com"
        await session.execute(
            _INSERT_USER,
            {
                "id": str(user_id),
                "org": str(org_id),
                "clerk_user_id": clerk_user_id,
                "email": email,
                "role": role,
                "skills": _SKILLS_BY_ROLE[role],
                "bio": f"{role.replace('_', ' ').title()} at {org.name}.",
            },
        )
        users.append(
            {
                "id": user_id,
                "clerk_user_id": clerk_user_id,
                "role": role,
                "name": f"{first} {last}",
            }
        )

    # One real owner and one real admin per org, so the seeded tenant is actually usable
    # end-to-end (someone who can call an admin-gated route) rather than a directory nobody
    # can administer. Tier is orthogonal to `users.role` (`libs/shared/authz.py`'s own
    # docstring: `users.role` is NEVER consulted for permissions) - `users[0]`'s domain role
    # stays "engineer" even though they also hold the `owner` RBAC tier.
    await session.execute(
        _INSERT_RBAC_GRANT,
        {
            "id": str(_sid("grant", org.slug, "0")),
            "org": str(org_id),
            "user": str(users[0]["id"]),
            "tier": "owner",
        },
    )
    await session.execute(
        _INSERT_RBAC_GRANT,
        {
            "id": str(_sid("grant", org.slug, "1")),
            "org": str(org_id),
            "user": str(users[1]["id"]),
            "tier": "admin",
        },
    )
    for i in range(2, 10):
        await session.execute(
            _INSERT_RBAC_GRANT,
            {
                "id": str(_sid("grant", org.slug, str(i))),
                "org": str(org_id),
                "user": str(users[i]["id"]),
                "tier": "member",
            },
        )
    return users


async def _seed_departments(session: AsyncSession, ctx: RequestContext) -> dict[str, uuid.UUID]:
    """core-api-owned. Written through `create_department`, not raw SQL - see module
    docstring. Idempotent: an existing department (by name, within this org via RLS) is read,
    never re-created."""
    ids: dict[str, uuid.UUID] = {}
    for name in _DEPARTMENTS:
        existing = (await session.execute(_SELECT_DEPARTMENT_BY_NAME, {"name": name})).first()
        if existing is not None:
            ids[name] = uuid.UUID(str(existing[0]))
            continue
        dept = await create_department(DepartmentCreate(name=name), ctx, session)
        ids[name] = dept.id
    return ids


async def _seed_teams(
    session: AsyncSession, ctx: RequestContext, dept_ids: dict[str, uuid.UUID]
) -> dict[str, uuid.UUID]:
    """core-api-owned. Written through `create_team` - see module docstring."""
    ids: dict[str, uuid.UUID] = {}
    for team_name, dept_name, _members in _TEAMS:
        existing = (await session.execute(_SELECT_TEAM_BY_NAME, {"name": team_name})).first()
        if existing is not None:
            ids[team_name] = uuid.UUID(str(existing[0]))
            continue
        team = await create_team(
            TeamCreate(name=team_name, department_id=dept_ids[dept_name]), ctx, session
        )
        ids[team_name] = team.id
    return ids


async def _seed_team_members(
    session: AsyncSession,
    org_id: uuid.UUID,
    team_ids: dict[str, uuid.UUID],
    users: list[dict[str, object]],
) -> None:
    """`team_members` exception - see module docstring. Direct write, `invalidate_user_context`
    called after (best-effort, no-op today since nothing is cached this early)."""
    for team_name, _dept_name, member_idxs in _TEAMS:
        team_id = team_ids[team_name]
        for position, idx in enumerate(member_idxs):
            role = "lead" if position == 0 else "member"
            user_id = users[idx]["id"]
            await session.execute(
                _INSERT_TEAM_MEMBER,
                {"org": str(org_id), "team": str(team_id), "user": str(user_id), "role": role},
            )
            await invalidate_user_context(org_id, user_id)  # type: ignore[arg-type]


async def _seed_meetings(
    session: AsyncSession,
    ctx: RequestContext,
    users: list[dict[str, object]],
) -> None:
    """core-api-owned. Written through `create_meeting` (+ `update_meeting` for the four past
    meetings, so they carry a real `status = 'ended'` transition and a real second
    `meeting-events` row) - see module docstring."""
    now = datetime.now(UTC)
    for m, (title, day_offset) in enumerate(_MEETINGS):
        existing = (await session.execute(_SELECT_MEETING_BY_TITLE, {"title": title})).first()
        if existing is not None:
            continue  # already seeded on a prior run; do not re-create or re-transition

        participant_idxs = [(2 * m + k) % 10 for k in range(4)]
        participant_ids = [users[i]["id"] for i in participant_idxs]
        host = participant_ids[0]
        start = now + timedelta(days=day_offset)

        host_ctx = RequestContext(
            org_id=ctx.org_id,
            user_id=host,  # type: ignore[arg-type]
            clerk_user_id=str(users[participant_idxs[0]]["clerk_user_id"]),
            rbac_tier="member",
            ip=None,
        )
        body = MeetingCreate(
            title=title,
            scheduled_start=start,
            scheduled_end=start + timedelta(hours=1),
            participant_ids=participant_ids,  # type: ignore[arg-type]
        )
        meeting = await create_meeting(body, Response(), host_ctx, session)

        if day_offset < 0:
            # A past meeting that nobody ever marks `ended` is a real, if slightly stale,
            # system state - but this seeder can do better: use the SAME real transition
            # `PATCH /meetings/{id}` uses, so downstream P6 work sees `status = 'ended'`
            # meetings, the natural embedding candidates, and a real second `meeting-events`
            # row (`event_type: "ended"`) rather than only ever `event_type: "created"`.
            await update_meeting(meeting.id, MeetingUpdate(status="ended"), host_ctx, session)


async def seed_organization(session: AsyncSession, org: OrgSpec) -> uuid.UUID:
    org_id = _sid("org", org.slug)

    async with session.begin():
        result = await provision_organization(
            session, org_id=org_id, name=org.name, slug=org.slug, plan_code=org.plan_code
        )
    org_id = result.org_id
    logger.info(
        "org %s (%s): %s", org.slug, org_id, "provisioned" if result.created else "already existed"
    )

    async with session.begin():
        await set_rls(session, org_id)
        users = await _seed_users(session, org_id, org)

    owner_ctx = RequestContext(
        org_id=org_id,
        user_id=users[0]["id"],  # type: ignore[arg-type]
        clerk_user_id=str(users[0]["clerk_user_id"]),
        rbac_tier="owner",
        ip=None,
    )

    async with session.begin():
        await set_rls(session, org_id)
        dept_ids = await _seed_departments(session, owner_ctx)

    async with session.begin():
        await set_rls(session, org_id)
        team_ids = await _seed_teams(session, owner_ctx, dept_ids)

    async with session.begin():
        await set_rls(session, org_id)
        await _seed_team_members(session, org_id, team_ids, users)

    async with session.begin():
        await set_rls(session, org_id)
        await _seed_meetings(session, owner_ctx, users)

    return org_id


_COUNT_USERS = text("SELECT count(*) FROM users WHERE org_id = :org AND deleted_at IS NULL")
_COUNT_MEETINGS = text("SELECT count(*) FROM meetings WHERE org_id = :org AND deleted_at IS NULL")


async def _verify(session: AsyncSession, org_ids: list[uuid.UUID]) -> None:
    """The acceptance check ('2 orgs, 20 users, 10 meetings'), run as an assertion, scoped to
    the two seeded org ids specifically - NOT a raw `count(*)` over the whole table, since this
    is a shared dev database other agents are actively writing to."""
    total_users = 0
    total_meetings = 0
    for org_id in org_ids:
        async with session.begin():
            await set_rls(session, org_id)
            u = (await session.execute(_COUNT_USERS, {"org": str(org_id)})).scalar_one()
            m = (await session.execute(_COUNT_MEETINGS, {"org": str(org_id)})).scalar_one()
        total_users += u
        total_meetings += m

    assert len(org_ids) == 2, f"expected 2 seeded orgs, resolved {len(org_ids)}"
    assert total_users == 20, f"expected 20 seeded users total, found {total_users}"
    assert total_meetings == 10, f"expected 10 seeded meetings total, found {total_meetings}"
    logger.info(
        "verified: %d orgs, %d users, %d meetings", len(org_ids), total_users, total_meetings
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    sessionmaker = get_admin_sessionmaker()
    org_ids: list[uuid.UUID] = []
    async with sessionmaker() as session:
        for org in ORGS:
            org_ids.append(await seed_organization(session, org))
        await _verify(session, org_ids)

    logger.warning(
        "NOT seeded (no legitimate write path from this repo - see module docstring): "
        "transcript_chunks (owned by transcription-service, mytheus-realtime), "
        "Topic nodes in nexus_knowledge (owned by knowledge-graph-service, "
        "mytheus-intelligence, currently a bare scaffold with zero routes)."
    )


if __name__ == "__main__":
    asyncio.run(main())
