"""feat(identity-tenant-service): OAuth for the MCP server (Claude custom connectors)

Claude's custom connectors (claude.ai, Claude Desktop) connect to a remote MCP server only
through OAuth per the MCP authorization spec (2025-06-18): protected resource metadata,
authorization server metadata, Dynamic Client Registration, PKCE S256 and RFC 8707 `resource`.
identity-tenant-service becomes that authorization server, because it already owns the thing
an access token has to become: an `api_tokens` row the gateway resolves unchanged.

- `oauth_clients` (GLOBAL, no `org_id`): one row per Dynamic Client Registration. Registration
  happens before anyone signs in, so there is no tenant to key it by: the deliberate exception,
  like `plans`. A client holds no tenant data (a name and redirect URIs); every grant it later
  receives is tenant-scoped below. Readable and writable by the request role, no RLS. The row
  is also the registration's audit record (`registered_ip`, `created_at`), since `audit_logs`
  is tenant-scoped and a registration has no tenant. `revoked_at` is an operator kill switch:
  a revoked client can neither authorize nor redeem, and its access tokens stop at once (the
  token lookup in `libs/shared/pat.py` joins it).
- `oauth_authorization_codes` (tenant-scoped, RLS): the code a person's consent produces.
  Short-lived (5 minutes), single use (`consumed_at`), bound to user, client, redirect URI,
  PKCE challenge, scope and resource. Only the SHA-256 is stored. The code's own text carries
  the tenant (`mytheus_oac_<org hex>_<secret>`), like a personal access token, so redemption
  at the unauthenticated token endpoint sets `app.current_org` from the code and looks it up
  under RLS: no RLS exception. `grant_id` is set on redemption, so a replayed code can revoke
  what it produced (RFC 6749 section 4.1.2).
- `oauth_grants` (tenant-scoped, RLS): one connection of one app for one person in one
  workspace, i.e. one refresh-token family. Settings > API access lists the active ones as
  "Connected apps"; revoking one kills its refresh tokens and access tokens together.
- `oauth_refresh_tokens` (tenant-scoped, RLS): the rotation chain. Only SHA-256 hashes.
  `rotated_at` marks a used token; presenting it again is reuse and revokes the family.
- `api_tokens` gains `kind` (`personal` | `oauth`, default `personal`, so every existing row is
  unchanged), `oauth_client_id` and `oauth_grant_id`. An OAuth access token is a `kind = 'oauth'`
  row in the existing `mytheus_pat_` format with a one-hour expiry, so `edge.py` and every
  service's `PatAuthMiddleware` resolve it with no new code path.

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import ADMIN_ROLE, APP_ROLE, grant_tenant_table

revision: str = "e7f8a9b0c1d2"
down_revision: Union[str, None] = "d6e7f8a9b0c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _rls(table: str, comment: str) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    # --- oauth_clients (global) -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE oauth_clients (
          client_id                  text        NOT NULL,
          client_name                text        NOT NULL,
          redirect_uris              text[]      NOT NULL,
          grant_types                text[]      NOT NULL,
          response_types             text[]      NOT NULL,
          token_endpoint_auth_method text        NOT NULL DEFAULT 'none',
          scope                      text        NOT NULL,
          client_uri                 text,
          software_id                text,
          software_version           text,
          registered_ip              inet,
          created_at                 timestamptz NOT NULL DEFAULT now(),
          revoked_at                 timestamptz,
          CONSTRAINT pk_oauth_clients PRIMARY KEY (client_id),
          CONSTRAINT ck_oauth_clients_name CHECK (char_length(client_name) BETWEEN 1 AND 120),
          CONSTRAINT ck_oauth_clients_redirects
            CHECK (cardinality(redirect_uris) BETWEEN 1 AND 10),
          CONSTRAINT ck_oauth_clients_public CHECK (token_endpoint_auth_method = 'none')
        );
        """)
    op.execute("CREATE INDEX ix_oauth_clients_created ON oauth_clients (created_at);")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON oauth_clients TO {APP_ROLE}, {ADMIN_ROLE};")
    op.execute(
        "COMMENT ON TABLE oauth_clients IS 'OAuth clients from Dynamic Client Registration (MCP "
        "connectors such as Claude). GLOBAL, no org_id: registration precedes sign-in, so there "
        "is no tenant; the deliberate exception like plans. Written only by "
        "identity-tenant-service /oauth/register. Public clients only (PKCE, no secret).';"
    )

    # --- oauth_grants (tenant) ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE oauth_grants (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id           uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          client_id         text        NOT NULL REFERENCES oauth_clients(client_id),
          scope             text        NOT NULL,
          resource          text        NOT NULL,
          created_at        timestamptz NOT NULL DEFAULT now(),
          last_refreshed_at timestamptz,
          revoked_at        timestamptz,
          revoked_reason    text,
          revoked_by        uuid        REFERENCES users(id) ON DELETE SET NULL,
          CONSTRAINT pk_oauth_grants PRIMARY KEY (id),
          CONSTRAINT ck_oauth_grants_scope CHECK (scope IN ('read', 'read_write')),
          CONSTRAINT ck_oauth_grants_reason CHECK (
            revoked_reason IS NULL
            OR revoked_reason IN ('user', 'admin', 'client', 'refresh_reuse', 'code_reuse')
          )
        );
        """)
    op.execute("CREATE INDEX ix_oauth_grants_user ON oauth_grants (user_id, created_at DESC);")
    _rls(
        "oauth_grants",
        "One OAuth connection (refresh-token family) of an MCP client for one person in one "
        "workspace. Written only by identity-tenant-service (/oauth/token, /oauth/revoke, "
        "/api/v1/oauth/grants).",
    )

    # --- oauth_authorization_codes (tenant) -----------------------------------------------------
    op.execute("""
        CREATE TABLE oauth_authorization_codes (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          client_id      text        NOT NULL REFERENCES oauth_clients(client_id),
          code_hash      text        NOT NULL,
          redirect_uri   text        NOT NULL,
          code_challenge text        NOT NULL,
          scope          text        NOT NULL,
          resource       text        NOT NULL,
          created_at     timestamptz NOT NULL DEFAULT now(),
          expires_at     timestamptz NOT NULL,
          consumed_at    timestamptz,
          grant_id       uuid        REFERENCES oauth_grants(id) ON DELETE SET NULL,
          CONSTRAINT pk_oauth_authorization_codes PRIMARY KEY (id),
          CONSTRAINT ck_oauth_codes_hash CHECK (code_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_oauth_codes_scope CHECK (scope IN ('read', 'read_write')),
          CONSTRAINT ck_oauth_codes_expiry CHECK (expires_at > created_at)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_oauth_authorization_codes_hash "
        "ON oauth_authorization_codes (code_hash);"
    )
    _rls(
        "oauth_authorization_codes",
        "Single-use, five-minute OAuth authorization codes bound to user, client, redirect URI, "
        "PKCE challenge, scope and resource. SHA-256 only. Written only by "
        "identity-tenant-service.",
    )

    # --- oauth_refresh_tokens (tenant) ----------------------------------------------------------
    op.execute("""
        CREATE TABLE oauth_refresh_tokens (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          grant_id    uuid        NOT NULL REFERENCES oauth_grants(id) ON DELETE CASCADE,
          token_hash  text        NOT NULL,
          parent_id   uuid        REFERENCES oauth_refresh_tokens(id) ON DELETE SET NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          expires_at  timestamptz NOT NULL,
          rotated_at  timestamptz,
          revoked_at  timestamptz,
          CONSTRAINT pk_oauth_refresh_tokens PRIMARY KEY (id),
          CONSTRAINT ck_oauth_refresh_hash CHECK (token_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_oauth_refresh_expiry CHECK (expires_at > created_at)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_oauth_refresh_tokens_hash ON oauth_refresh_tokens (token_hash);"
    )
    op.execute("CREATE INDEX ix_oauth_refresh_tokens_grant ON oauth_refresh_tokens (grant_id);")
    _rls(
        "oauth_refresh_tokens",
        "Rotating OAuth refresh tokens (SHA-256 only), one chain per oauth_grants row; reuse of "
        "a rotated token revokes the family. Written only by identity-tenant-service.",
    )

    # --- api_tokens: OAuth access tokens (additive) ---------------------------------------------
    op.execute("""
        ALTER TABLE api_tokens
          ADD COLUMN kind text NOT NULL DEFAULT 'personal',
          ADD COLUMN oauth_client_id text REFERENCES oauth_clients(client_id),
          ADD COLUMN oauth_grant_id uuid REFERENCES oauth_grants(id) ON DELETE CASCADE,
          ADD CONSTRAINT ck_api_tokens_kind CHECK (kind IN ('personal', 'oauth')),
          ADD CONSTRAINT ck_api_tokens_oauth_binding CHECK (
            (kind = 'personal' AND oauth_client_id IS NULL AND oauth_grant_id IS NULL)
            OR (kind = 'oauth' AND oauth_client_id IS NOT NULL AND oauth_grant_id IS NOT NULL)
          );
        """)
    op.execute(
        "CREATE INDEX ix_api_tokens_oauth_grant ON api_tokens (oauth_grant_id) "
        "WHERE oauth_grant_id IS NOT NULL;"
    )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works. OAuth
    # access tokens cannot survive without their grant, so they go first.
    op.execute("DELETE FROM api_tokens WHERE kind = 'oauth';")
    op.execute("DROP INDEX IF EXISTS ix_api_tokens_oauth_grant;")
    op.execute("""
        ALTER TABLE api_tokens
          DROP CONSTRAINT IF EXISTS ck_api_tokens_oauth_binding,
          DROP CONSTRAINT IF EXISTS ck_api_tokens_kind,
          DROP COLUMN IF EXISTS oauth_grant_id,
          DROP COLUMN IF EXISTS oauth_client_id,
          DROP COLUMN IF EXISTS kind;
        """)
    op.execute("DROP TABLE IF EXISTS oauth_refresh_tokens CASCADE;")
    op.execute("DROP TABLE IF EXISTS oauth_authorization_codes CASCADE;")
    op.execute("DROP TABLE IF EXISTS oauth_grants CASCADE;")
    op.execute("DROP TABLE IF EXISTS oauth_clients CASCADE;")
