"""feat: personal access tokens, share links, follow-ups, email deliveries, reach providers (Phase 4)

The backend half of the polish plan's Phase 4 "reach" items, in one forward-only change. Every
new table is tenant-scoped by `org_id`, ENABLE + FORCE RLS with the canonical NULLIF
`tenant_isolation` policy and the explicit `grant_tenant_table` (ADR-0021).

- `api_tokens` (identity-tenant-service): personal access tokens for the public API and the MCP
  server. Only the SHA-256 of the token is stored (`token_hash`, unique across tenants) plus its
  last four characters (`token_hint`) for the list view; the plaintext is shown once. A token
  carries the tenant in its own text (`mytheus_pat_<org hex>_<secret>`), so resolving it needs
  no RLS exception: the resolver sets `app.current_org` from the token and then looks the hash
  up under RLS, and a token naming the wrong tenant simply finds nothing. `scope` is `read`
  (GET/HEAD only) or `read_write`; `expires_at` is mandatory.
- `share_links` (core-api): signed, expiring, revocable public links to a meeting recap or a
  decision record, with a view count. The link's token is an HMAC over (org, link id); the row
  is the revocation and expiry authority.
- `meeting_followups` (core-api): the host's follow-up settings for one meeting (on/off, the
  external guests to email) and the last dispatch result.
- `email_deliveries` (notification-service): one row per email sent to an address that is not
  a user (the follow-up's external guests). `UNIQUE (org_id, idempotency_key)` makes a replayed
  dispatch a no-op.
- `action_items.meeting_id` becomes nullable: a person can create a task directly
  (`POST /api/v1/actions`), with no meeting behind it.
- `integration_connections.provider` gains `hubspot`, `notion` and `confluence`;
  `integration_id_map.nexus_kind` gains `decision` (a decision record exported to Notion,
  Confluence or Slack).

Revision ID: a7b8c9d0e1f2
Revises: d2b3c4d5e6f7
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, None] = "d2b3c4d5e6f7"
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
    # --- api_tokens (identity-tenant-service) ---------------------------------------------------
    op.execute("""
        CREATE TABLE api_tokens (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          name        text        NOT NULL,
          token_hash  text        NOT NULL,
          token_hint  text        NOT NULL,
          scope       text        NOT NULL,
          expires_at  timestamptz NOT NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          revoked_at  timestamptz,
          revoked_by  uuid        REFERENCES users(id) ON DELETE SET NULL,
          CONSTRAINT pk_api_tokens PRIMARY KEY (id),
          CONSTRAINT ck_api_tokens_name CHECK (char_length(name) BETWEEN 1 AND 80),
          CONSTRAINT ck_api_tokens_scope CHECK (scope IN ('read', 'read_write')),
          CONSTRAINT ck_api_tokens_hash CHECK (token_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT ck_api_tokens_hint CHECK (char_length(token_hint) = 4),
          CONSTRAINT ck_api_tokens_expiry CHECK (expires_at > created_at)
        );
        """)
    op.execute("CREATE UNIQUE INDEX uq_api_tokens_token_hash ON api_tokens (token_hash);")
    op.execute("CREATE INDEX ix_api_tokens_user_created ON api_tokens (user_id, created_at DESC);")
    _rls(
        "api_tokens",
        "Personal access tokens for the public API and MCP server (Phase 4). Written only by "
        "identity-tenant-service /api/v1/api-tokens. Stores the SHA-256 of the token, never the "
        "token.",
    )

    # --- action_items: a task with no meeting -------------------------------------------------
    op.execute("ALTER TABLE action_items ALTER COLUMN meeting_id DROP NOT NULL;")

    # --- share_links (core-api) -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE share_links (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          kind           text        NOT NULL,
          meeting_id     uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          decision_id    uuid        REFERENCES decisions(id) ON DELETE CASCADE,
          created_by     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          expires_at     timestamptz NOT NULL,
          created_at     timestamptz NOT NULL DEFAULT now(),
          revoked_at     timestamptz,
          revoked_by     uuid        REFERENCES users(id) ON DELETE SET NULL,
          view_count     integer     NOT NULL DEFAULT 0,
          last_viewed_at timestamptz,
          CONSTRAINT pk_share_links PRIMARY KEY (id),
          CONSTRAINT ck_share_links_kind CHECK (kind IN ('meeting_recap', 'decision_record')),
          CONSTRAINT ck_share_links_decision
            CHECK ((kind = 'decision_record') = (decision_id IS NOT NULL)),
          CONSTRAINT ck_share_links_expiry CHECK (expires_at > created_at),
          CONSTRAINT ck_share_links_views CHECK (view_count >= 0)
        );
        """)
    op.execute(
        "CREATE INDEX ix_share_links_meeting_created ON share_links (meeting_id, created_at DESC);"
    )
    _rls(
        "share_links",
        "Public share links to a meeting recap or decision record (Phase 4). Written only by "
        "core-api: participants create and revoke them; the public read increments view_count.",
    )

    # --- meeting_followups (core-api) -------------------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_followups (
          meeting_id    uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          enabled       boolean     NOT NULL DEFAULT true,
          guest_emails  text[]      NOT NULL DEFAULT '{}',
          share_link_id uuid        REFERENCES share_links(id) ON DELETE SET NULL,
          last_sent_at  timestamptz,
          last_result   jsonb,
          updated_by    uuid        REFERENCES users(id) ON DELETE SET NULL,
          created_at    timestamptz NOT NULL DEFAULT now(),
          updated_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_followups PRIMARY KEY (meeting_id),
          CONSTRAINT ck_meeting_followups_guests CHECK (cardinality(guest_emails) <= 20)
        );
        """)
    _rls(
        "meeting_followups",
        "The host''s follow-up email settings for one meeting and the last dispatch (Phase 4). "
        "Written only by core-api.",
    )

    # --- email_deliveries (notification-service) ------------------------------------------------
    op.execute("""
        CREATE TABLE email_deliveries (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          idempotency_key text        NOT NULL,
          kind            text        NOT NULL,
          meeting_id      uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          recipient       text        NOT NULL,
          status          text        NOT NULL,
          detail          text,
          provider        text,
          created_at      timestamptz NOT NULL DEFAULT now(),
          updated_at      timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_email_deliveries PRIMARY KEY (id),
          CONSTRAINT uq_email_deliveries_key UNIQUE (org_id, idempotency_key),
          CONSTRAINT ck_email_deliveries_kind CHECK (kind IN ('meeting_followup')),
          CONSTRAINT ck_email_deliveries_status CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
          CONSTRAINT ck_email_deliveries_recipient CHECK (char_length(recipient) BETWEEN 3 AND 320)
        );
        """)
    op.execute(
        "CREATE INDEX ix_email_deliveries_meeting ON email_deliveries (meeting_id, created_at DESC);"
    )
    _rls(
        "email_deliveries",
        "Emails sent to addresses that are not users, e.g. a meeting follow-up''s external "
        "guests (Phase 4). Written only by notification-service.",
    )

    # --- integration-service: the reach providers -------------------------------------------------
    op.execute("""
        ALTER TABLE integration_connections
          DROP CONSTRAINT ck_integration_connections_provider,
          ADD CONSTRAINT ck_integration_connections_provider CHECK (
            provider IN ('google_calendar', 'slack', 'jira', 'linear', 'hubspot', 'notion',
                         'confluence')
          );
        """)
    op.execute("""
        ALTER TABLE integration_id_map
          DROP CONSTRAINT ck_integration_id_map_nexus_kind,
          ADD CONSTRAINT ck_integration_id_map_nexus_kind
            CHECK (nexus_kind IN ('action', 'meeting', 'decision'));
        """)


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("DELETE FROM integration_id_map WHERE nexus_kind = 'decision';")
    op.execute("""
        ALTER TABLE integration_id_map
          DROP CONSTRAINT ck_integration_id_map_nexus_kind,
          ADD CONSTRAINT ck_integration_id_map_nexus_kind
            CHECK (nexus_kind IN ('action', 'meeting'));
        """)
    op.execute(
        "DELETE FROM integration_connections WHERE provider IN ('hubspot', 'notion', 'confluence');"
    )
    op.execute("""
        ALTER TABLE integration_connections
          DROP CONSTRAINT ck_integration_connections_provider,
          ADD CONSTRAINT ck_integration_connections_provider
            CHECK (provider IN ('google_calendar', 'slack', 'jira', 'linear'));
        """)
    op.execute("DROP TABLE IF EXISTS email_deliveries CASCADE;")
    op.execute("DROP TABLE IF EXISTS meeting_followups CASCADE;")
    op.execute("DROP TABLE IF EXISTS share_links CASCADE;")
    # A task with no meeting cannot survive the NOT NULL coming back.
    op.execute("DELETE FROM action_status_requests WHERE action_id IN "
               "(SELECT id FROM action_items WHERE meeting_id IS NULL);")
    op.execute("DELETE FROM action_items WHERE meeting_id IS NULL;")
    op.execute("ALTER TABLE action_items ALTER COLUMN meeting_id SET NOT NULL;")
    op.execute("DROP TABLE IF EXISTS api_tokens CASCADE;")
