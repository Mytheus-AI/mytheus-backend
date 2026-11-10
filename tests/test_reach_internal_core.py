"""core-api's internal reads for the Phase 4 reach features (`routes/internal_reach.py`), against
a real Postgres:

1. `GET /internal/meetings/{id}/crm-digest`: only integration-service may call it; it carries
   the confirmed outcome (never proposed or dismissed candidates), the participant and guest
   addresses and the tenant's own domains; a confidential meeting is 404 `reason: confidential`;
   another tenant's meeting is a plain 404.
2. `GET /internal/decisions/{id}/record?viewer=`: the record as the viewer may share it, with
   lineage (an earlier decision from a meeting the viewer was not in is a placeholder); 404 for a
   non-participant, a proposed decision, and a confidential meeting the viewer does not host.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import internal_reach
from tests import _integration_fakes as fx
from tests.conftest import APP_URL
from tests.test_chat_api import Tenant, _sql, add_participant, seed_meeting, seed_tenant
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")

HEADERS = {
    "X-Mytheus-Service": "integration-service",
    "X-Mytheus-Service-Token": fx.CORE_SECRET_INTEG,
}


@pytest.fixture(scope="module")
def core(migrated_db):
    previous_env = fx.install_env()
    previous_db = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = create_app("core-api-reach-test")
    app.include_router(internal_reach.router)
    with TestClient(app) as c:
        yield c
    fx.restore_env(previous_env)
    if previous_db is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous_db
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _h(t: Tenant) -> dict[str, str]:
    return {**HEADERS, "X-Mytheus-Org": str(t.org_id)}


def set_email(t: Tenant, email: str) -> None:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})


def decision(
    t: Tenant, meeting: uuid.UUID, content: str, status: str = "confirmed", supersedes=None
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status, confirmed_by, "
        "supersedes) VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(u)s, %(p)s) RETURNING id",
        {
            "o": str(t.org_id),
            "m": str(meeting),
            "c": content,
            "s": status,
            "u": str(t.user_id),
            "p": str(supersedes) if supersedes else None,
        },
    )
    return uuid.UUID(str(row[0]["id"]))


def action(t: Tenant, meeting: uuid.UUID, content: str, status: str = "confirmed") -> None:
    _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, %(a)s, 0.8, %(s)s)",
        {"o": str(t.org_id), "m": str(meeting), "c": content, "a": str(t.user_id), "s": status},
    )


def meeting_with(*people: Tenant, confidential: bool = False, host: Tenant | None = None):
    m = seed_meeting(people[0].org_id)
    for p in people:
        add_participant(m, p.org_id, p.user_id)
    if host is not None:
        _sql(
            "UPDATE meeting_participants SET role_in_meeting = 'host' "
            "WHERE meeting_id = %(m)s AND user_id = %(u)s",
            {"m": str(m), "u": str(host.user_id)},
        )
    if confidential:
        _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(m)})
    return m


def test_crm_digest_is_for_integration_service_only(core):
    t = seed_tenant()
    m = meeting_with(t)
    url = f"/internal/meetings/{m}/crm-digest"
    assert core.get(url, headers={"X-Mytheus-Org": str(t.org_id)}).status_code == 401
    wrong = {**_h(t), "X-Mytheus-Service": "workflow-service"}
    assert core.get(url, headers=wrong).status_code == 401
    assert core.get(url, headers=_h(t)).status_code == 200


def test_crm_digest_carries_confirmed_facts_addresses_and_tenant_domains(core):
    host = seed_tenant()
    set_email(host, "maya.patel@acme.io")
    m = meeting_with(host)
    _sql(
        "INSERT INTO meeting_followups (meeting_id, org_id, guest_emails) "
        "VALUES (%(m)s, %(o)s, ARRAY['Buyer@Customer.com', 'pal@gmail.com'])",
        {"m": str(m), "o": str(host.org_id)},
    )
    decision(host, m, "Ship in March")
    decision(host, m, "A proposal nobody confirmed", status="proposed")
    action(host, m, "Send the contract")
    action(host, m, "A candidate", status="proposed")

    body = core.get(f"/internal/meetings/{m}/crm-digest", headers=_h(host)).json()
    assert body["participant_emails"] == ["maya.patel@acme.io"]
    assert body["guest_emails"] == ["buyer@customer.com", "pal@gmail.com"]
    assert body["tenant_domains"] == ["acme.io"]
    assert [d["content"] for d in body["decisions"]] == ["Ship in March"]
    assert [(a["content"], a["owner_name"]) for a in body["actions"]] == [
        ("Send the contract", "Maya Patel")
    ]
    assert body["link"] == f"/meetings/{m}"


def test_a_confidential_meeting_never_reaches_a_crm(core):
    t = seed_tenant()
    m = meeting_with(t, confidential=True)
    r = core.get(f"/internal/meetings/{m}/crm-digest", headers=_h(t))
    assert r.status_code == 404
    assert r.json()["reason"] == "confidential"


def test_another_tenants_meeting_is_a_plain_404(core):
    a, b = seed_tenant(), seed_tenant()
    m = meeting_with(a)
    r = core.get(f"/internal/meetings/{m}/crm-digest", headers=_h(b))
    assert r.status_code == 404
    assert "reason" not in r.json()


def _record(core: TestClient, t: Tenant, d: uuid.UUID, viewer: Tenant) -> Any:
    return core.get(
        f"/internal/decisions/{d}/record", params={"viewer": str(viewer.user_id)}, headers=_h(t)
    )


def test_decision_record_with_lineage_as_the_viewer_may_see_it(core):
    me = seed_tenant()
    other = seed_user(me)
    old_meeting = meeting_with(other)  # I was not in this one
    mine = meeting_with(me, other)
    earlier = decision(other, old_meeting, "Use vendor A")
    subject = decision(me, mine, "Use vendor B", supersedes=earlier)
    later = decision(me, mine, "Use vendor B, two-year term", supersedes=subject)
    _sql("UPDATE decisions SET status = 'superseded' WHERE id = %(d)s", {"d": str(subject)})
    action(me, mine, "Tell vendor A")

    r = _record(core, me, subject, me)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "superseded"
    relations = [(s["relation"], s["visible"], s["content"]) for s in body["lineage"]]
    assert relations == [
        ("earlier", False, None),
        ("subject", True, "Use vendor B"),
        ("later", True, "Use vendor B, two-year term"),
    ]
    assert body["lineage"][2]["decision_id"] == str(later)
    assert [a["content"] for a in body["meeting_actions"]] == ["Tell vendor A"]


def test_decision_record_is_404_unless_the_viewer_may_share_it(core):
    me = seed_tenant()
    outsider = seed_user(me)
    host = seed_user(me)
    m = meeting_with(me, host)
    d = decision(me, m, "Hire two engineers")
    proposed = decision(me, m, "Unconfirmed", status="proposed")
    assert _record(core, me, d, outsider).status_code == 404
    assert _record(core, me, proposed, me).status_code == 404

    secret = meeting_with(me, host, confidential=True, host=host)
    sd = decision(me, secret, "Confidential call")
    assert _record(core, me, sd, me).status_code == 404  # a participant, but not the host
    assert _record(core, me, sd, host).status_code == 200

    stranger = seed_tenant()
    assert _record(core, stranger, d, me).status_code == 404
