"""Journal plan Part 3 fix 11: every card stores the prompt `key@version` and the model id the
agent runtime sent on `agent-results` (migration e3f5a7b9c1d2), and the card reads expose them.

A producer still on the previous `agent-results` schema sends neither: the card stores null."""

from __future__ import annotations

import json
from typing import Any

from tests.test_candidates import acand, candidate_result, dcand, participant_meeting
from tests.test_card_moments import client, one  # noqa: F401 - `client` is a fixture
from tests.test_chat_api import _sql, seed_meeting, seed_tenant, sign_in
from tests.test_suggestions import meeting_with, scand, suggestion_result
from tests.test_worklogs import apply, result

PV, MID = "decision_agent@v3", "claude-sonnet-5-5"


def attributed(value: dict[str, Any], pv: str = PV, mid: str = MID) -> dict[str, Any]:
    return {**value, "prompt_version": pv, "model_id": mid}


async def test_every_card_table_stores_the_attribution(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    d, a = dcand(0.9), acand(0.9)
    assert await apply(attributed(candidate_result(t, m, [d]))) is True
    assert await apply(
        attributed(candidate_result(t, m, [a], agent="ActionAgent", window="w1"), "action@v2")
    )
    row = one("decisions", d["id"])
    assert (row["prompt_version"], row["model_id"]) == (PV, MID)
    assert one("action_items", a["id"])["prompt_version"] == "action@v2"

    note = result(t, m, 0.9, window="w2")
    note["payload_json"] = json.dumps({"surfaced": True})
    assert await apply(attributed(note, "personal_listener@v4", "claude-haiku-5-5")) is True
    (log,) = _sql("SELECT * FROM worklogs WHERE meeting_id = %(m)s", {"m": str(m)})
    assert (log["prompt_version"], log["model_id"]) == ("personal_listener@v4", "claude-haiku-5-5")

    m2 = meeting_with(t)
    s = scand(0.9)
    assert await apply(attributed(suggestion_result(t, m2, [s]), "question_agent@v2")) is True
    (sug,) = _sql("SELECT * FROM agent_suggestions WHERE id = %(id)s", {"id": s["id"]})
    assert (sug["prompt_version"], sug["model_id"]) == ("question_agent@v2", MID)


async def test_a_result_without_attribution_stores_null(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    d = dcand(0.9)
    assert await apply(candidate_result(t, m, [d])) is True
    row = one("decisions", d["id"])
    assert row["prompt_version"] is None and row["model_id"] is None


async def test_a_non_string_or_overlong_attribution_is_bounded(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    d = dcand(0.9)
    value = {**candidate_result(t, m, [d]), "prompt_version": 7, "model_id": "m" * 500}
    assert await apply(value) is True
    row = one("decisions", d["id"])
    assert row["prompt_version"] is None
    assert row["model_id"] == "m" * 120


def test_the_candidate_reads_expose_the_attribution(client, migrated_db):  # noqa: F811
    import asyncio

    t = seed_tenant()
    m = participant_meeting(t)
    d = dcand(0.9)
    asyncio.run(apply(attributed(candidate_result(t, m, [d]))))
    sign_in(t)
    (got,) = client.get(f"/api/v1/meetings/{m}/candidates").json()["decisions"]
    assert (got["prompt_version"], got["model_id"]) == (PV, MID)
    state = client.get(f"/api/v1/meetings/{m}/candidates/{d['id']}").json()
    assert (state["prompt_version"], state["model_id"]) == (PV, MID)
