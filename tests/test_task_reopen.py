"""`POST /api/v1/actions/{id}/reopen` (wave 3): done -> confirmed for the assignee or the owner.

Covered: the assignee reopens a done task (audited once; it moves back to the open list); a
repeat is a 200 with no second audit row; the owner (`confirmed_by`) may reopen; anyone else is
403; a proposed candidate is 409; another tenant's id and a deleted task are 404; nothing is
written to the outbox.
"""

from __future__ import annotations

import uuid

from tests.test_chat_api import _sql, seed_meeting, seed_tenant, sign_in
from tests.test_my_tasks import action_items_table, add_user, client, task  # noqa: F401


def _reopen_audits(action_id: uuid.UUID) -> int:
    return len(
        _sql(
            "SELECT 1 FROM audit_logs WHERE action = 'action.reopened' AND target_id = %(t)s",
            {"t": str(action_id)},
        )
    )


def test_the_assignee_reopens_a_done_task_once(client):  # noqa: F811
    me = seed_tenant()
    sign_in(me)
    m = seed_meeting(me.org_id)
    tid = task(me, m, me.user_id, "write it up", status="done")
    res = client.post(f"/api/v1/actions/{tid}/reopen")
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "confirmed"
    assert _reopen_audits(tid) == 1
    open_ids = [
        i["id"] for i in client.get("/api/v1/actions", params={"assignee_id": "me"}).json()["items"]
    ]
    assert str(tid) in open_ids
    again = client.post(f"/api/v1/actions/{tid}/reopen")
    assert again.status_code == 200 and again.json()["status"] == "confirmed"
    assert _reopen_audits(tid) == 1
    outbox = _sql(
        "SELECT 1 FROM outbox WHERE org_id = %(o)s AND topic = 'actions'", {"o": str(me.org_id)}
    )
    assert outbox == []


def test_the_owner_may_reopen_others_may_not(client):  # noqa: F811
    me = seed_tenant()
    owner = add_user(me)
    stranger = add_user(me, "admin")
    m = seed_meeting(me.org_id)
    tid = task(me, m, me.user_id, "ship", status="done")
    _sql(
        "UPDATE action_items SET confirmed_by = %(u)s WHERE id = %(t)s",
        {"u": str(owner.user_id), "t": str(tid)},
    )
    sign_in(stranger)
    assert client.post(f"/api/v1/actions/{tid}/reopen").status_code == 403
    sign_in(owner)
    assert client.post(f"/api/v1/actions/{tid}/reopen").status_code == 200


def test_candidates_conflict_and_invisible_tasks_are_not_found(client):  # noqa: F811
    me = seed_tenant()
    other = seed_tenant()
    sign_in(me)
    m = seed_meeting(me.org_id)
    proposed = task(me, m, me.user_id, "maybe", status="proposed")
    deleted = task(me, m, me.user_id, "gone", status="done", deleted=True)
    theirs = task(other, seed_meeting(other.org_id), other.user_id, "theirs", status="done")
    assert client.post(f"/api/v1/actions/{proposed}/reopen").status_code == 409
    assert client.post(f"/api/v1/actions/{deleted}/reopen").status_code == 404
    assert client.post(f"/api/v1/actions/{theirs}/reopen").status_code == 404
    assert client.post(f"/api/v1/actions/{uuid.uuid4()}/reopen").status_code == 404
