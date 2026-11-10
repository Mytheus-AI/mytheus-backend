"""Internal reads for ProactiveAgent (P11 contract section 3).

core-api: `GET /internal/actions/{id}/context` (open confirmed count for the assignee, including
this action; depends_on; the meeting's participants; the tenant's admins; the organizer) and
`GET /internal/meetings/{id}/participants` (name and tier), both behind the
agent-runtime-service caller token, 404 across tenants.

billing-metering-service: `GET /internal/plan-features` per org (Free provisioned on first
read), agent-runtime-service only.
"""

from __future__ import annotations

import os
import uuid

import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.billing_metering_service import internal as billing_internal
from services.core_api.routes import internal_agents, internal_meetings
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, add_participant, seed_meeting, seed_tenant
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

AGENT_SECRET = "a" * 40
RAG_SECRET = "r" * 40
BILLING_SECRET = "b" * 40


def _swap_db() -> str | None:
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    return previous


def _restore_db(previous: str | None) -> None:
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(scope="module")
def core(migrated_db):
    previous = _swap_db()
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},rag-service={RAG_SECRET}"
    )
    app = create_app("core-api-proactive-test")
    app.include_router(internal_agents.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    _restore_db(previous)


@pytest.fixture(scope="module")
def billing(migrated_db):
    previous = _swap_db()
    os.environ["MYTHEUS_BILLING_CALLER_TOKENS"] = (
        f"agent-runtime-service={BILLING_SECRET},rag-service={RAG_SECRET}"
    )
    app = create_app("billing-plan-features-test")
    app.include_router(billing_internal.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop("MYTHEUS_BILLING_CALLER_TOKENS", None)
    _restore_db(previous)


def _headers(
    org_id: uuid.UUID, service: str = "agent-runtime-service", secret: str = AGENT_SECRET
) -> dict[str, str]:
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def seed_action(
    t: Tenant,
    meeting_id: uuid.UUID,
    assignee: uuid.UUID | None,
    status: str = "confirmed",
    *,
    deleted: bool = False,
) -> uuid.UUID:
    aid = uuid.uuid4()
    _sql(
        "INSERT INTO action_items (id, org_id, meeting_id, content, assignee_id, status, "
        "confidence, deleted_at) VALUES (%(i)s, %(o)s, %(m)s, 'Do it', %(a)s, %(s)s, 0.9, "
        "CASE WHEN %(d)s THEN now() END)",
        {
            "i": str(aid),
            "o": str(t.org_id),
            "m": str(meeting_id),
            "a": str(assignee) if assignee else None,
            "s": status,
            "d": deleted,
        },
    )
    return aid


# --- core-api: action context ----------------------------------------------------------------


def test_action_context_counts_open_actions_and_names_the_people(core):
    org = seed_tenant("owner")
    priya = seed_user(org)
    admin = seed_user(org, "admin")
    bystander = seed_user(org)
    _sql("UPDATE users SET email = 'priya@example.com' WHERE id = %(u)s", {"u": str(priya.user_id)})
    m = seed_meeting(org.org_id)
    _sql(
        "UPDATE meetings SET created_by = %(u)s WHERE id = %(m)s",
        {"u": str(org.user_id), "m": str(m)},
    )
    for p in (org, priya, bystander):
        add_participant(m, org.org_id, p.user_id)
    action = seed_action(org, m, priya.user_id)
    other_m = seed_meeting(org.org_id)
    for _ in range(4):
        seed_action(org, other_m, priya.user_id)  # open elsewhere: counted
    seed_action(org, other_m, priya.user_id, "done")
    seed_action(org, other_m, priya.user_id, "proposed")
    seed_action(org, other_m, priya.user_id, "dismissed")
    seed_action(org, other_m, priya.user_id, deleted=True)
    seed_action(org, other_m, bystander.user_id)  # someone else's
    elsewhere = seed_tenant()
    seed_action(elsewhere, seed_meeting(elsewhere.org_id), None)

    resp = core.get(f"/internal/actions/{action}/context", headers=_headers(org.org_id))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["assignee_id"] == str(priya.user_id)
    assert body["assignee_name"] == "priya@example.com"
    assert body["meeting_id"] == str(m) and body["organizer_id"] == str(org.user_id)
    assert body["open_action_count"] == 5  # this one + 4 others
    assert body["depends_on"] == []
    assert sorted(body["participants"]) == sorted(str(p.user_id) for p in (org, priya, bystander))
    assert sorted(body["admins"]) == sorted([str(org.user_id), str(admin.user_id)])


def test_action_context_without_an_assignee(core):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    action = seed_action(t, m, None, "proposed")
    body = core.get(f"/internal/actions/{action}/context", headers=_headers(t.org_id)).json()
    assert body["assignee_id"] is None and body["assignee_name"] is None
    assert body["open_action_count"] == 0 and body["organizer_id"] is None


def test_action_context_requires_the_token_and_hides_other_tenants(core):
    t = seed_tenant()
    action = seed_action(t, seed_meeting(t.org_id), t.user_id)
    url = f"/internal/actions/{action}/context"
    assert core.get(url).status_code == 401
    assert core.get(url, headers=_headers(t.org_id, secret="x" * 40)).status_code == 401
    rag = _headers(t.org_id, service="rag-service", secret=RAG_SECRET)
    assert core.get(url, headers=rag).status_code == 401
    assert core.get(url, headers=_headers(t.org_id)).status_code == 200
    other = seed_tenant()
    assert core.get(url, headers=_headers(other.org_id)).status_code == 404
    gone = seed_action(t, seed_meeting(t.org_id), t.user_id, deleted=True)
    assert (
        core.get(f"/internal/actions/{gone}/context", headers=_headers(t.org_id)).status_code == 404
    )


# --- core-api: meeting participants ----------------------------------------------------------


def test_meeting_participants_with_names_and_tiers(core):
    org = seed_tenant("owner")
    member = seed_user(org)
    no_grant = seed_user(org)
    _sql("DELETE FROM rbac_grants WHERE user_id = %(u)s", {"u": str(no_grant.user_id)})
    _sql("UPDATE users SET email = 'a@example.com' WHERE id = %(u)s", {"u": str(member.user_id)})
    _sql("UPDATE users SET email = 'b@example.com' WHERE id = %(u)s", {"u": str(no_grant.user_id)})
    _sql("UPDATE users SET email = 'c@example.com' WHERE id = %(u)s", {"u": str(org.user_id)})
    m = seed_meeting(org.org_id)
    for p in (org, member, no_grant):
        add_participant(m, org.org_id, p.user_id)
    seed_user(org)  # not a participant
    url = f"/internal/meetings/{m}/participants"
    resp = core.get(url, headers=_headers(org.org_id))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "participants": [
            {"user_id": str(member.user_id), "name": "a@example.com", "rbac_tier": "member"},
            {"user_id": str(no_grant.user_id), "name": "b@example.com", "rbac_tier": None},
            {"user_id": str(org.user_id), "name": "c@example.com", "rbac_tier": "owner"},
        ]
    }
    assert core.get(url).status_code == 401
    other = seed_tenant()
    assert core.get(url, headers=_headers(other.org_id)).status_code == 404
    unknown = f"/internal/meetings/{uuid.uuid4()}/participants"
    assert core.get(unknown, headers=_headers(org.org_id)).status_code == 404


# --- billing: plan features ------------------------------------------------------------------


def _subscribe(t: Tenant, plan: str) -> None:
    _sql(
        "INSERT INTO subscriptions (org_id, plan_id) SELECT %(o)s, id FROM plans "
        "WHERE code = %(p)s ORDER BY version DESC LIMIT 1",
        {"o": str(t.org_id), "p": plan},
    )


def test_plan_features_are_per_org(billing):
    free, ent, pro = seed_tenant(), seed_tenant(), seed_tenant()
    _subscribe(ent, "enterprise")
    _subscribe(pro, "pro")

    def get(t: Tenant) -> dict:
        resp = billing.get(
            "/internal/plan-features", headers=_headers(t.org_id, secret=BILLING_SECRET)
        )
        assert resp.status_code == 200, resp.text
        return resp.json()

    f = get(free)  # no subscription yet: provisioned onto Free
    assert f["plan_code"] == "free" and f["features"]["proactive_agent"] is False
    assert _sql("SELECT 1 FROM subscriptions WHERE org_id = %(o)s", {"o": str(free.org_id)})
    e = get(ent)
    assert e["plan_code"] == "enterprise" and e["features"]["proactive_agent"] is True
    p = get(pro)
    assert p["plan_code"] == "pro" and p["features"]["context_gap_agent"] is True


def test_plan_features_auth(billing):
    t = seed_tenant()
    url = "/internal/plan-features"
    assert billing.get(url).status_code == 401
    assert billing.get(url, headers=_headers(t.org_id, secret="x" * 40)).status_code == 401
    rag = _headers(t.org_id, service="rag-service", secret=RAG_SECRET)
    assert billing.get(url, headers=rag).status_code == 401  # valid secret, wrong caller
    unknown = _headers(uuid.uuid4(), secret=BILLING_SECRET)
    assert billing.get(url, headers=unknown).status_code == 404
