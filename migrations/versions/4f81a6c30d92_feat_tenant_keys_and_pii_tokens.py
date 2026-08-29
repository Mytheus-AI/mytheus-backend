"""feat(identity-tenant-service): add tenant_keys and pii_tokens

The migration ADR-0020 refused to authorize, now authorized by ADR-0024.

ADR-0020 ratified `pii_tokens` and then blocked its implementation on one precondition: key
custody, which no part of the program provided. ADR-0024 supplies it - per-tenant envelope
encryption behind a `KeyProvider` seam, with data keys held only in wrapped form - so both
tables land here, in that order, because `pii_tokens.dek_id` is a foreign key into
`tenant_keys` and the vault is not buildable without the registry.

WHY BOTH TABLES ARE IN ONE MIGRATION

They are one change, not two that happen to be adjacent: `pii_tokens` has a NOT NULL FK to
`tenant_keys`, so a database at the intermediate revision would have a vault with no key
registry to reference. There is no useful state between them.

`oauth_tokens` is NOT created here. It is the other table that needed this substrate
(`_canon/02-data-model.md` §2.2 describes it as envelope-encrypted under a per-tenant KMS data
key) and it is now unblocked, but it belongs to `integration-service` and to whichever band
builds the integrations. Building it here would be inventing that service's schema from
outside it.

THE TWO PARTIAL UNIQUE INDEXES ARE THE SCHEMA'S LOAD-BEARING PART (ADR-0024 D4)

  uq_tenant_keys_active_enc    UNIQUE (org_id) WHERE purpose='enc' AND status='active'
  uq_tenant_keys_index         UNIQUE (org_id) WHERE purpose='index'

The first permits exactly one ACTIVE encryption key per tenant while allowing any number of
retired ones, which is what makes rotation possible at all: a rotation inserts a new active row
and flips the old to `retired`, and every `pii_tokens` row keeps pointing at the key that
actually encrypted it through `dek_id`.

The second has NO status predicate, and that asymmetry is the point. One `index` key per tenant
for the lifetime of the tenant, retired or not, because `value_hmac` carries a UNIQUE
constraint that is the entire cross-boundary coherence mechanism. Re-keying the index would
make every existing hmac unreachable and mint a second token for a value that already had one,
silently. The database refuses to hold a second `index` row so that the code path cannot exist.

WHY `value_hmac` IS INDEXED AND `value_ciphertext` IS NOT

`UNIQUE (org_id, entity_type, value_hmac)` is the lookup path AND the coherence guarantee in
one constraint: the same raw value in the same tenant always finds the row it already minted.
It is safe to index only because the hmac is KEYED (ADR-0020 D3) - an unkeyed digest of an
email address is equivalent to storing the email, since the plausible-email space is
enumerable. The ciphertext is never searched and never indexed.

GRANTS ARE EXPLICIT, PER ADR-0021

Migration `9b2c4d1e6f30` retired the schema-wide `ALTER DEFAULT PRIVILEGES`, so these tables
are born with an empty ACL and `grant_tenant_table` is not optional. `mytheus_relay` is
correctly absent: the relay holds a grant on `outbox` alone.

Revision ID: 4f81a6c30d92
Revises: 9b2c4d1e6f30
Create Date: 2026-08-29 10:12:44.518233

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._grants import grant_tenant_table

# revision identifiers, used by Alembic.
revision: str = "4f81a6c30d92"
down_revision: Union[str, None] = "9b2c4d1e6f30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Identical to every other tenant table's predicate: NULLIF(current_setting(..., true), '') so
# an unset OR reset-to-empty app.current_org yields NULL, the predicate is false, and the query
# returns zero rows (fail-closed) rather than raising.
_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

# ADR-0020 D3's typed vocabulary. The first five are what the deterministic regex layer already
# detects; the last three are exactly what NER adds and regex cannot.
_ENTITY_TYPES = ("EMAIL", "PHONE", "IBAN", "CARD", "GOVID", "PERSON", "ORG", "LOC")


def _force_rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )


def upgrade() -> None:
    op.create_table(
        "tenant_keys",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("purpose", sa.Text(), nullable=False),
        # The DEK, encrypted under the root key. The plaintext data key is never stored, which
        # is the entire difference between envelope encryption and a key in a column.
        sa.Column("wrapped_key", sa.LargeBinary(), nullable=False),
        # Which ROOT key wrapped it, so the root key can rotate too and a re-wrap job can find
        # the rows that are behind. Without this a root-key rotation is undiscoverable.
        sa.Column("wrap_key_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'active'"), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("retired_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name=op.f("fk_tenant_keys_org_id_organizations"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tenant_keys")),
        sa.CheckConstraint("purpose IN ('enc','index')", name="purpose_valid"),
        sa.CheckConstraint(
            "status IN ('active','retired')", name="status_valid"
        ),
        # A retired key must say when. The pair is what makes "was this key live at the time"
        # answerable during an incident, which is the only question anyone asks of this table
        # after the fact.
        sa.CheckConstraint(
            "(status = 'active' AND retired_at IS NULL) OR "
            "(status = 'retired' AND retired_at IS NOT NULL)",
            name="retired_at_matches_status",
        ),
    )
    op.create_index(op.f("ix_tenant_keys_org_id"), "tenant_keys", ["org_id"], unique=False)
    # See the module docstring: the presence of a status predicate on one of these and not the
    # other is deliberate and is ADR-0024 D4 expressed as a constraint.
    op.create_index(
        "uq_tenant_keys_active_enc",
        "tenant_keys",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("purpose = 'enc' AND status = 'active'"),
    )
    op.create_index(
        "uq_tenant_keys_index",
        "tenant_keys",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("purpose = 'index'"),
    )
    _force_rls("tenant_keys")
    grant_tenant_table(op, "tenant_keys")

    op.create_table(
        "pii_tokens",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        # The opaque placeholder body, e.g. the `7f3a` in `<PERSON:7f3a>`.
        sa.Column("token", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("value_ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("value_hmac", sa.LargeBinary(), nullable=False),
        sa.Column("dek_id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name=op.f("fk_pii_tokens_org_id_organizations"),
            ondelete="RESTRICT",
        ),
        # RESTRICT, so a key cannot be deleted out from under the rows it is the only way to
        # read. Crypto-shredding a tenant (ADR-0024 § Consequences) therefore deletes the
        # pii_tokens rows first and the keys second, which is the correct order anyway: the
        # reverse would leave undecryptable rows behind and call it erasure.
        sa.ForeignKeyConstraint(
            ["dek_id"],
            ["tenant_keys.id"],
            name=op.f("fk_pii_tokens_dek_id_tenant_keys"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_pii_tokens")),
        sa.CheckConstraint(
            "entity_type IN (" + ",".join(f"'{t}'" for t in _ENTITY_TYPES) + ")",
            name="entity_type_valid",
        ),
        # One token resolves to exactly one value.
        sa.UniqueConstraint("org_id", "token", name="uq_pii_tokens_org_id_token"),
        # ...and one value always mints the same token. This is the coherence property, and it
        # is why the same customer named in a transcript and in an uploaded document links.
        sa.UniqueConstraint(
            "org_id",
            "entity_type",
            "value_hmac",
            name="uq_pii_tokens_org_id_entity_type_value_hmac",
        ),
    )
    op.create_index(op.f("ix_pii_tokens_org_id"), "pii_tokens", ["org_id"], unique=False)
    _force_rls("pii_tokens")
    grant_tenant_table(op, "pii_tokens")

    # ADR-0020 § The unavailable-NER decision, carried as configuration rather than left as a
    # blocker. `regex_only` is the default because it is the behaviour that already ships: the
    # dependency-free layer still removes every structured identifier, and the alternative
    # (`drop`) discards a meeting record that `transcript_chunks` is the system of record for.
    # ADR-0020 rejected persisting raw outright, so there is no third value to represent.
    op.add_column(
        "tenant_settings",
        sa.Column(
            "ner_unavailable_policy",
            sa.Text(),
            server_default=sa.text("'regex_only'"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "ner_unavailable_policy_valid",
        "tenant_settings",
        "ner_unavailable_policy IN ('drop','regex_only')",
    )

    op.execute(
        """
        COMMENT ON TABLE tenant_keys IS
        'Per-tenant envelope-encryption key registry (ADR-0024). Data keys are stored WRAPPED '
        'only; the root key that wraps them is held by a KeyProvider outside the database. '
        'One active purpose=enc key per tenant (rotatable), one purpose=index key per tenant '
        'ever (NOT rotatable - re-keying it invalidates every pii_tokens.value_hmac). Sole '
        'writer: identity-tenant-service.';
        """
    )
    op.execute(
        """
        COMMENT ON TABLE pii_tokens IS
        'Per-tenant reversible PII token vault (ADR-0020). Sole writer: '
        'identity-tenant-service; the three redaction boundaries call its mint/resolve API. '
        'value_hmac is a KEYED HMAC, never a plain digest. Resolving a token is a PII '
        'disclosure and writes audit_logs.';
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_tenant_settings_ner_unavailable_policy_valid", "tenant_settings", type_="check"
    )
    op.drop_column("tenant_settings", "ner_unavailable_policy")
    op.drop_index(op.f("ix_pii_tokens_org_id"), table_name="pii_tokens")
    op.drop_table("pii_tokens")
    op.drop_index("uq_tenant_keys_index", table_name="tenant_keys")
    op.drop_index("uq_tenant_keys_active_enc", table_name="tenant_keys")
    op.drop_index(op.f("ix_tenant_keys_org_id"), table_name="tenant_keys")
    op.drop_table("tenant_keys")
