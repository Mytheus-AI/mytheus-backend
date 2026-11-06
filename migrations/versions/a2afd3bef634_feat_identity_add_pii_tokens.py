"""feat(identity-tenant-service): add pii_tokens (the per-tenant PII token vault)

ADR-0020 D1/D3, unblocked by ADR-0024 (key custody). Shape is canon verbatim
(`_canon/02-data-model.md` section 2.2): `id`, `org_id`, `token`, `entity_type`,
`value_ciphertext`, `value_hmac`, `dek_id`, UNIQUE `(org_id, token)`, UNIQUE
`(org_id, entity_type, value_hmac)`.

- `value_ciphertext` is a `libs/shared/crypto` envelope (CHECKed by its magic prefix, so a raw
  value written by mistake is refused), AAD-bound to `org_id`, purpose `pii_tokens.value`,
  `entity_type` and `value_hmac` - so a ciphertext moved onto another row, even in the same
  tenant, fails to decrypt instead of resolving a token to the wrong person.
- `value_hmac` is the keyed blind index (HMAC-SHA256 under a per-tenant derived key), never a
  plain digest; exactly 32 bytes, CHECKed. The second UNIQUE is the coherence property: the
  same raw value in the same tenant always mints the same token, across all three redaction
  boundaries.
- `dek_id` duplicates the DEK id already inside the envelope header, as canon specifies, so a
  rotation job can select "rows under DEK X" without decrypting anything.

Owned and written ONLY by identity-tenant-service (ADR-0020 D1). Postgres cannot express
"only this service" - every module of the monolith connects as `mytheus_app` - so what the
grants CAN express is the least privilege the vault needs: `mytheus_app` gets SELECT and
INSERT only. A mapping, once minted, is immutable on the request path: no UPDATE (a token can
never be re-pointed at a different value) and no DELETE (erasure is the RTBF saga's job, which
runs as the operator seam). `mytheus_admin` gets the full set for that saga and for KEK-rotation
rewraps. `mytheus_relay` gets nothing. RLS is ENABLE + FORCE + `tenant_isolation`.

Revision ID: a2afd3bef634
Revises: 24e134664d84
Create Date: 2026-11-06

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._grants import ADMIN_ROLE, APP_ROLE

# revision identifiers, used by Alembic.
revision: str = "a2afd3bef634"
down_revision: Union[str, None] = "24e134664d84"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

ENTITY_TYPES = ("EMAIL", "PHONE", "IBAN", "CARD", "GOVID", "PERSON", "ORG", "LOC")


def upgrade() -> None:
    entity_list = ", ".join(f"'{e}'" for e in ENTITY_TYPES)
    op.create_table(
        "pii_tokens",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("token", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("value_ciphertext", postgresql.BYTEA(), nullable=False),
        sa.Column("value_hmac", postgresql.BYTEA(), nullable=False),
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
            name="fk_pii_tokens_org_id_organizations",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_pii_tokens"),
        sa.UniqueConstraint("org_id", "token", name="uq_pii_tokens_org_id_token"),
        sa.UniqueConstraint(
            "org_id",
            "entity_type",
            "value_hmac",
            name="uq_pii_tokens_org_id_entity_type_value_hmac",
        ),
        sa.CheckConstraint(
            f"entity_type IN ({entity_list})", name="entity_type_valid"
        ),
        sa.CheckConstraint("token ~ '^[0-9a-f]{16}$'", name="token_shape"),
        sa.CheckConstraint("octet_length(value_hmac) = 32", name="value_hmac_len"),
        sa.CheckConstraint(
            "substring(value_ciphertext from 1 for 4) = '\\x4d594501'::bytea",
            name="value_ciphertext_is_envelope",
        ),
    )
    # The two UNIQUE constraints already index both lookup paths; `org_id` leads both, so no
    # separate org_id index is needed. `dek_id` is for rotation jobs.
    op.create_index("ix_pii_tokens_org_id_dek_id", "pii_tokens", ["org_id", "dek_id"])

    op.execute("ALTER TABLE pii_tokens ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE pii_tokens FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON pii_tokens "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    # ADR-0021: explicit, and narrower than `grant_tenant_table` on purpose (see docstring).
    op.execute(f"GRANT SELECT, INSERT ON pii_tokens TO {APP_ROLE};")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON pii_tokens TO {ADMIN_ROLE};")


def downgrade() -> None:
    op.drop_index("ix_pii_tokens_org_id_dek_id", table_name="pii_tokens")
    op.drop_table("pii_tokens")
