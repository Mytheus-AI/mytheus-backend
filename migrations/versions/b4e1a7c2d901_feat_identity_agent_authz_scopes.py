"""feat(identity): agent_authz_scopes, the A2A grant store (ADR-0013)

Plan 10-13 section 3.2; mytheus-docs ADR-0013 section 3. Owned and written only by
`identity-tenant-service` (the PDP). One row per (grantee_role, query_class, target_scope) per
tenant, `effect` allow or deny. Deny by default: a tenant with no rows has agent-to-agent
collaboration off, and an `effect = 'deny'` row always wins in `authorize()`.

`grantee_role` is the requester's RBAC tier (`owner | admin | member`), `query_class` one of
`meeting_context | decision_lineage | action_status`, `target_scope` one of
`self | same_team | department | org`. The value sets are CHECKed here so a typo in a grant is
refused by the database rather than stored as a row the PDP can never match. Revoke is a soft
delete (`deleted_at`); re-granting the same tuple re-activates the row, which is why the
UNIQUE constraint is on the tuple and not on (tuple, deleted_at).

RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the explicit
`grant_tenant_table` (ADR-0021).

Revision ID: b4e1a7c2d901
Revises: a9c4e2f7b813
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "b4e1a7c2d901"
down_revision: Union[str, None] = "a9c4e2f7b813"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE agent_authz_scopes (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          grantee_role text        NOT NULL,
          query_class  text        NOT NULL,
          target_scope text        NOT NULL,
          effect       text        NOT NULL DEFAULT 'allow',
          created_by   uuid        NOT NULL,
          created_at   timestamptz NOT NULL DEFAULT now(),
          deleted_at   timestamptz,
          CONSTRAINT pk_agent_authz_scopes PRIMARY KEY (id),
          CONSTRAINT uq_agent_authz_scopes_tuple
            UNIQUE (org_id, grantee_role, query_class, target_scope),
          CONSTRAINT ck_agent_authz_scopes_effect CHECK (effect IN ('allow', 'deny')),
          CONSTRAINT ck_agent_authz_scopes_grantee_role
            CHECK (grantee_role IN ('owner', 'admin', 'member')),
          CONSTRAINT ck_agent_authz_scopes_query_class
            CHECK (query_class IN ('meeting_context', 'decision_lineage', 'action_status')),
          CONSTRAINT ck_agent_authz_scopes_target_scope
            CHECK (target_scope IN ('self', 'same_team', 'department', 'org'))
        );
        """)
    op.execute("ALTER TABLE agent_authz_scopes ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE agent_authz_scopes FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON agent_authz_scopes "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "agent_authz_scopes")
    op.execute(
        "COMMENT ON TABLE agent_authz_scopes IS "
        "'A2A grant store (ADR-0013). Written only by identity-tenant-service. Deny by "
        "default: no matching allow row, or any matching deny row, is a deny.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_authz_scopes CASCADE;")
