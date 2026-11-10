"""Internal policy reads for the AI tier (polish plan, Phase 0).

`GET /internal/agent-policy`: the tenant's `agent_confidence_overrides`, normalized as core-api
applies them, for agent-runtime-service only (one threshold source for live and reload).
`GET /internal/meetings/{id}/index-policy`: a meeting's `confidential` flag, for
embedding-service and search-service only; 404 across tenants and for a deleted meeting.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import internal_meetings, internal_policy
from tests.conftest import APP_URL
from tests.test_chat_api import _sql, seed_meeting, seed_tenant

pytestmark = pytest.mark.usefixtures("migrated_db")

AGENT_SECRET = "a" * 40
EMBED_SECRET = "e" * 40
SEARCH_SECRET = "s" * 40
RAG_SECRET = "r" * 40


@pytest.fixture(scope="module")
def core(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},embedding-service={EMBED_SECRET},"
        f"search-service={SEARCH_SECRET},rag-service={RAG_SECRET}"
    )
    app = create_app("core-api-policy-test")
    app.include_router(internal_policy.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _headers(org_id: uuid.UUID, service: str, secret: str) -> dict[str, str]:
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def _overrides(org_id: uuid.UUID, overrides: dict) -> None:
    _sql(
        "INSERT INTO tenant_settings (org_id, agent_confidence_overrides) "
        "VALUES (%(o)s, CAST(%(v)s AS jsonb)) ON CONFLICT (org_id) DO UPDATE "
        "SET agent_confidence_overrides = EXCLUDED.agent_confidence_overrides",
        {"o": str(org_id), "v": json.dumps(overrides)},
    )


def test_agent_policy_returns_the_tenants_overrides_as_core_api_applies_them(core):
    org = seed_tenant("owner").org_id
    agent = _headers(org, "agent-runtime-service", AGENT_SECRET)
    assert core.get("/internal/agent-policy", headers=agent).json() == {
        "agent_confidence_overrides": {}
    }
    _overrides(
        org,
        {
            "PersonalListenerAgent": {"surface": 0.8, "silent": 0.5},
            "DecisionAgent": {"surface": 0.9},
            "QuestionAgent": {"surface": "high"},
        },
    )
    body = core.get("/internal/agent-policy", headers=agent).json()
    assert body["agent_confidence_overrides"] == {
        "PersonalListenerAgent": {"surface": 0.8, "silent": 0.5},
        "DecisionAgent": {"surface": 0.9, "silent": 0.4},
        "QuestionAgent": {"surface": 0.7, "silent": 0.4},  # malformed: the defaults
    }
    other = seed_tenant("owner").org_id
    assert core.get(
        "/internal/agent-policy", headers=_headers(other, "agent-runtime-service", AGENT_SECRET)
    ).json() == {"agent_confidence_overrides": {}}


def test_agent_policy_is_agent_runtime_only(core):
    org = seed_tenant("owner").org_id
    for service, secret in (
        ("embedding-service", EMBED_SECRET),
        ("rag-service", RAG_SECRET),
        ("agent-runtime-service", "x" * 40),
    ):
        res = core.get("/internal/agent-policy", headers=_headers(org, service, secret))
        assert res.status_code == 401, service
    assert core.get("/internal/agent-policy").status_code == 401


def test_index_policy_answers_the_meetings_confidential_flag_to_the_indexers(core):
    t = seed_tenant("owner")
    open_meeting, secret_meeting = seed_meeting(t.org_id), seed_meeting(t.org_id)
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": str(secret_meeting)})
    for service, token in (("embedding-service", EMBED_SECRET), ("search-service", SEARCH_SECRET)):
        h = _headers(t.org_id, service, token)
        assert core.get(f"/internal/meetings/{open_meeting}/index-policy", headers=h).json() == {
            "confidential": False
        }
        assert core.get(f"/internal/meetings/{secret_meeting}/index-policy", headers=h).json() == {
            "confidential": True
        }


def test_index_policy_is_404_across_tenants_and_for_a_deleted_meeting_and_indexers_only(core):
    t, other = seed_tenant("owner"), seed_tenant("owner")
    meeting = seed_meeting(t.org_id)
    url = f"/internal/meetings/{meeting}/index-policy"
    foreign = _headers(other.org_id, "search-service", SEARCH_SECRET)
    assert core.get(url, headers=foreign).status_code == 404
    _sql("UPDATE meetings SET deleted_at = now() WHERE id = %(m)s", {"m": str(meeting)})
    own = _headers(t.org_id, "search-service", SEARCH_SECRET)
    assert core.get(url, headers=own).status_code == 404
    live = seed_meeting(t.org_id)
    agent = _headers(t.org_id, "agent-runtime-service", AGENT_SECRET)
    assert core.get(f"/internal/meetings/{live}/index-policy", headers=agent).status_code == 401


def test_the_indexers_may_read_their_budget_through_billing():
    """Their never-record check is a metered triage call: billing must accept them on the
    budget read-through, and still only agent-runtime-service on plan features."""
    from services.billing_metering_service import internal as billing_internal

    tokens = billing_internal.parse_caller_tokens(
        f"embedding-service={EMBED_SECRET},search-service={SEARCH_SECRET}"
    )
    assert set(tokens) == {"embedding-service", "search-service"}
    with pytest.raises(ValueError):
        billing_internal.parse_caller_tokens(f"bff={EMBED_SECRET}")
