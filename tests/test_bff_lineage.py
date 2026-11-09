"""The decision-lineage screen (`bff`), against a real Postgres, with `knowledge-graph-service`
replaced at the HTTP boundary (`httpx.MockTransport`) so the request `bff` actually sends is
asserted: the template path, the decision id, the service headers and the caller's `org_id`.

1. the chain comes back oldest first with content and status, and the graph is asked with the
   caller's own `org_id` and `bff`'s service credentials;
2. another tenant's meeting is a 404 before the graph is ever called;
3. a decision the graph does not know, or one from a different meeting, is a 404;
4. the graph unreachable, failing or unconfigured is a 503, never an empty chain.

The graph side of the same contract (`POST /graph/query/decision_lineage` with these headers)
is tested live in `mytheus-intelligence` `tests/test_knowledge_graph_api.py`.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.bff import lineage as lineage_mod
from tests.conftest import APP_URL
from tests.test_chat_api import PRINCIPAL, seed_meeting, seed_tenant, sign_in

TOKEN = "k" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = create_app("bff-lineage-test")
    app.include_router(lineage_mod.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


class FakeGraph:
    def __init__(self, nodes: list[dict[str, Any]] | None = None, status: int = 200) -> None:
        self.nodes = nodes or []
        self.status = status
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body = {"template": "decision_lineage", "nodes": self.nodes}
        return httpx.Response(self.status, json=body)


@pytest.fixture()
def graph(monkeypatch: pytest.MonkeyPatch) -> FakeGraph:
    fake = FakeGraph()
    real = httpx.AsyncClient

    def factory(**kwargs: Any) -> httpx.AsyncClient:
        return real(transport=httpx.MockTransport(fake.handler), **kwargs)

    monkeypatch.setattr(lineage_mod.httpx, "AsyncClient", factory)
    monkeypatch.setenv("KNOWLEDGE_GRAPH_URL", "http://kg.internal:8202")
    monkeypatch.setenv("MYTHEUS_KG_SERVICE_TOKEN", TOKEN)
    return fake


def node(d: uuid.UUID, m: uuid.UUID, role: str, depth: int, content: str, status: str):
    return {
        "label": "Decision",
        "id": str(d),
        "role": role,
        "depth": depth,
        "properties": {
            "decision_id": str(d),
            "meeting_id": str(m),
            "content": content,
            "status": status,
            "confidence": 0.9,
            "confirmed_at": "2026-11-09T10:00:00+00:00",
        },
    }


def test_the_chain_is_returned_oldest_first_for_the_callers_org(client, graph):
    t = seed_tenant()
    sign_in(t)
    m = seed_meeting(t.org_id)
    d1, d2, d3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    graph.nodes = [
        node(d1, m, "ancestor", 1, "v1", "superseded"),
        node(d2, m, "subject", 0, "v2", "superseded"),
        node(d3, uuid.uuid4(), "descendant", 1, "v3", "confirmed"),
    ]

    res = client.get(f"/bff/meetings/{m}/decisions/{d2}/lineage")
    assert res.status_code == 200
    body = res.json()
    assert [(c["content"], c["role"], c["status"]) for c in body["chain"]] == [
        ("v1", "ancestor", "superseded"),
        ("v2", "subject", "superseded"),
        ("v3", "descendant", "confirmed"),
    ]

    [req] = graph.requests
    assert str(req.url) == "http://kg.internal:8202/graph/query/decision_lineage"
    assert json.loads(req.content) == {"params": {"decision_id": str(d2)}}
    assert req.headers["x-mytheus-service"] == "bff"
    assert req.headers["x-mytheus-service-token"] == TOKEN
    assert req.headers["x-mytheus-org"] == str(t.org_id)


def test_another_tenants_meeting_is_404_and_the_graph_is_not_asked(client, graph):
    a = seed_tenant()
    m = seed_meeting(a.org_id)
    sign_in(seed_tenant())
    res = client.get(f"/bff/meetings/{m}/decisions/{uuid.uuid4()}/lineage")
    assert res.status_code == 404
    assert graph.requests == []


def test_unknown_decision_or_wrong_meeting_is_404(client, graph):
    t = seed_tenant()
    sign_in(t)
    m = seed_meeting(t.org_id)
    d = uuid.uuid4()
    assert client.get(f"/bff/meetings/{m}/decisions/{d}/lineage").status_code == 404
    graph.nodes = [node(d, uuid.uuid4(), "subject", 0, "elsewhere", "confirmed")]
    assert client.get(f"/bff/meetings/{m}/decisions/{d}/lineage").status_code == 404


def test_graph_failures_are_503(client, graph, monkeypatch):
    t = seed_tenant()
    sign_in(t)
    m = seed_meeting(t.org_id)
    url = f"/bff/meetings/{m}/decisions/{uuid.uuid4()}/lineage"
    graph.status = 500
    res = client.get(url)
    assert res.status_code == 503 and res.json()["code"] == "DEPENDENCY_UNAVAILABLE"
    monkeypatch.delenv("MYTHEUS_KG_SERVICE_TOKEN")
    assert client.get(url).status_code == 503
