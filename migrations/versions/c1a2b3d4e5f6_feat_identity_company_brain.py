"""feat: the company brain in identity-tenant-service (ADR-0039 Part A)

ADR-0039 A1 to A6, in one forward-only change.

- `org_profiles` (A1): one row per org, keyed by `org_id`. `description` and `industry` are
  `NOT NULL DEFAULT ''` so an org with no profile and an org with an empty one read the same,
  and the gRPC `OrgContext` strings never carry a NULL.
- `org_glossary_terms` (A2): unique per org on `lower(term)` (`ux_org_glossary_terms_org_term`,
  the conflict target of the import's upsert).
- `topic_owners` (A3): unique per org on `lower(topic)`; the owner references `users` with
  `ON DELETE CASCADE`, so a purged person's topics go with them.
- `org_goals` (A4): the owner references `users` with `ON DELETE SET NULL`, so a purged person
  leaves the goal, unowned.
- `topic_watches` (A6): unique per user on `lower(topic)`; `ON DELETE CASCADE` from `users`.
- `tenant_settings.never_record_topics` (A5). A7's `ground_rules` check reads it as done when
  it holds at least one entry (ADR-0039 A7 as amended 2026-11-10: an empty list cannot be told
  apart from never set, so no extra column records "saved once").

Every length and per-row count the ADR fixes is a CHECK here as well as an API rule. A
per-element length on a `text[]` needs a function, because a CHECK cannot hold a subquery:
`text_items_within(arr, n)` is IMMUTABLE and true when every element is 1 to `n` characters.
The per-org row caps (500 terms, 200 owners, 30 active goals, 20 watches per user) are the
API's, taken under a transaction-level advisory lock so two concurrent inserts cannot both pass
the count.

RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the explicit
`grant_tenant_table` (ADR-0021), same as every tenant table since `9b2c4d1e6f30`.

Revision ID: c1a2b3d4e5f6
Revises: 0b91a00079a7
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "c1a2b3d4e5f6"
down_revision: Union[str, None] = "0b91a00079a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

_TABLES = ("org_profiles", "org_glossary_terms", "topic_owners", "org_goals", "topic_watches")


def _rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION text_items_within(arr text[], max_len integer) RETURNS boolean
        LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
          SELECT COALESCE(bool_and(char_length(x) BETWEEN 1 AND max_len), true)
          FROM unnest(arr) AS x
        $$;
        """)

    # --- A1 org_profiles ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE org_profiles (
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          description text        NOT NULL DEFAULT '',
          industry    text        NOT NULL DEFAULT '',
          products    text[]      NOT NULL DEFAULT '{}',
          customers   text[]      NOT NULL DEFAULT '{}',
          updated_by  uuid        REFERENCES users(id) ON DELETE SET NULL,
          updated_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_org_profiles PRIMARY KEY (org_id),
          CONSTRAINT ck_org_profiles_description CHECK (char_length(description) <= 2000),
          CONSTRAINT ck_org_profiles_industry CHECK (char_length(industry) <= 100),
          CONSTRAINT ck_org_profiles_products
            CHECK (cardinality(products) <= 30 AND text_items_within(products, 80)),
          CONSTRAINT ck_org_profiles_customers
            CHECK (cardinality(customers) <= 50 AND text_items_within(customers, 80))
        );
        """)
    _rls("org_profiles")

    # --- A2 org_glossary_terms --------------------------------------------------------------
    op.execute("""
        CREATE TABLE org_glossary_terms (
          id         uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id     uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          term       text        NOT NULL,
          kind       text        NOT NULL DEFAULT 'other',
          definition text,
          aliases    text[]      NOT NULL DEFAULT '{}',
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_org_glossary_terms PRIMARY KEY (id),
          CONSTRAINT ck_org_glossary_terms_term CHECK (char_length(term) BETWEEN 1 AND 80),
          CONSTRAINT ck_org_glossary_terms_kind
            CHECK (kind IN ('product', 'codename', 'acronym', 'customer', 'person', 'other')),
          CONSTRAINT ck_org_glossary_terms_definition
            CHECK (definition IS NULL OR char_length(definition) <= 300),
          CONSTRAINT ck_org_glossary_terms_aliases
            CHECK (cardinality(aliases) <= 10 AND text_items_within(aliases, 80))
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_org_glossary_terms_org_term "
        "ON org_glossary_terms (org_id, lower(term));"
    )
    _rls("org_glossary_terms")

    # --- A3 topic_owners --------------------------------------------------------------------
    op.execute("""
        CREATE TABLE topic_owners (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          topic         text        NOT NULL,
          owner_user_id uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          created_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_topic_owners PRIMARY KEY (id),
          CONSTRAINT ck_topic_owners_topic CHECK (char_length(topic) BETWEEN 1 AND 80)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_topic_owners_org_topic ON topic_owners (org_id, lower(topic));"
    )
    op.execute("CREATE INDEX ix_topic_owners_owner_user_id ON topic_owners (owner_user_id);")
    _rls("topic_owners")

    # --- A4 org_goals -----------------------------------------------------------------------
    op.execute("""
        CREATE TABLE org_goals (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          title         text        NOT NULL,
          description   text,
          period        text,
          status        text        NOT NULL DEFAULT 'active',
          owner_user_id uuid        REFERENCES users(id) ON DELETE SET NULL,
          position      integer     NOT NULL DEFAULT 0,
          created_at    timestamptz NOT NULL DEFAULT now(),
          updated_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_org_goals PRIMARY KEY (id),
          CONSTRAINT ck_org_goals_title CHECK (char_length(title) BETWEEN 1 AND 200),
          CONSTRAINT ck_org_goals_description
            CHECK (description IS NULL OR char_length(description) <= 1000),
          CONSTRAINT ck_org_goals_period CHECK (period IS NULL OR char_length(period) <= 20),
          CONSTRAINT ck_org_goals_status CHECK (status IN ('active', 'achieved', 'dropped')),
          CONSTRAINT ck_org_goals_position CHECK (position >= 0)
        );
        """)
    op.execute("CREATE INDEX ix_org_goals_org_position ON org_goals (org_id, position);")
    op.execute(
        "CREATE INDEX ix_org_goals_owner_user_id ON org_goals (owner_user_id) "
        "WHERE owner_user_id IS NOT NULL;"
    )
    _rls("org_goals")

    # --- A6 topic_watches -------------------------------------------------------------------
    op.execute("""
        CREATE TABLE topic_watches (
          id         uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id     uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id    uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          topic      text        NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_topic_watches PRIMARY KEY (id),
          CONSTRAINT ck_topic_watches_topic CHECK (char_length(topic) BETWEEN 1 AND 80)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_topic_watches_org_user_topic "
        "ON topic_watches (org_id, user_id, lower(topic));"
    )
    op.execute("CREATE INDEX ix_topic_watches_user_id ON topic_watches (user_id);")
    _rls("topic_watches")

    # --- A5 tenant_settings.never_record_topics ---------------------------------------------
    op.execute("""
        ALTER TABLE tenant_settings
          ADD COLUMN never_record_topics text[] NOT NULL DEFAULT '{}',
          ADD CONSTRAINT ck_tenant_settings_never_record_topics
            CHECK (cardinality(never_record_topics) <= 30
                   AND text_items_within(never_record_topics, 80));
        """)

    for table, what in (
        ("org_profiles", "The company profile agents read (ADR-0039 A1)."),
        ("org_glossary_terms", "Company vocabulary, unique per org on lower(term) (ADR-0039 A2)."),
        ("topic_owners", "Who owns which topic, unique per org on lower(topic) (ADR-0039 A3)."),
        ("org_goals", "Company goals; at most 30 active per org, by the API (ADR-0039 A4)."),
        ("topic_watches", "Topic watches, one person each; self only by the API (ADR-0039 A6)."),
    ):
        op.execute(
            f"COMMENT ON TABLE {table} IS '{what} Written only by identity-tenant-service.';"
        )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute(
        "ALTER TABLE tenant_settings "
        "DROP CONSTRAINT IF EXISTS ck_tenant_settings_never_record_topics, "
        "DROP COLUMN IF EXISTS never_record_topics;"
    )
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    op.execute("DROP FUNCTION IF EXISTS text_items_within(text[], integer);")
