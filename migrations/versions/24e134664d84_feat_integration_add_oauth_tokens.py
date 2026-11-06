"""feat(integration-service): add oauth_tokens (envelope-encrypted)

Gap 22 / ADR-0024. `_canon/02-data-model.md` section 2.2 has defined `oauth_tokens` since the
start - "tokens encrypted at rest (envelope encryption, per-tenant KMS data key) ... never
returned by API" - and ADR-0020 recorded that it had no migration and no encryption substrate.
`libs/shared/crypto` is that substrate; this is the table.

Shape, per canon: `id`, `org_id`, `connection_id`, `access_token_enc`, `refresh_token_enc`,
`expires_at`, `token_version`.

- `*_enc` are `bytea` holding a self-describing envelope (`libs/shared/crypto/envelope.py`):
  KEK id, DEK id and wrapped DEK ride inside the value, which is how "the key version is
  stored with the ciphertext" without a key table that two services would share. Canon's ERD
  said `text`; it is amended to `bytea` in the same change (canon first).
- Encryption AAD is `org_id` + purpose (`oauth_tokens.access_token` /
  `oauth_tokens.refresh_token`) + `connection_id`, so a token blob copied to another tenant,
  another column, or another connection's row fails to decrypt. A CHECK on the magic prefix
  makes a plaintext token written by mistake a constraint violation rather than a leak.
- `token_version` is the OAuth token generation (bumped on refresh), not a key version. One
  row per (`org_id`, `connection_id`, `token_version`).

`connection_id` carries NO foreign key yet. Its target, `integration_connections`, has no
migration in this repository; adding a dangling FK would fail the upgrade. The FK is owed by
whichever migration creates `integration_connections` (recorded as an open item).

RLS: ENABLE + FORCE + `tenant_isolation`, the canonical NULLIF predicate (fail closed when
`app.current_org` is unset or empty). Grants per ADR-0021: explicit, full DML for
`mytheus_app` because integration-service inserts on connect, updates on refresh and deletes
on disconnect; same for the `mytheus_admin` operator seam. `mytheus_relay` gets nothing.

Revision ID: 24e134664d84
Revises: 9b2c4d1e6f30
Create Date: 2026-11-06

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._grants import grant_tenant_table

# revision identifiers, used by Alembic.
revision: str = "24e134664d84"
down_revision: Union[str, None] = "9b2c4d1e6f30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

# b"MYE\x01": the envelope magic + format version (libs/shared/crypto/envelope.py).
_ENVELOPE_CHECK = "substring({col} from 1 for 4) = '\\x4d594501'::bytea"


def upgrade() -> None:
    op.create_table(
        "oauth_tokens",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=False),
        sa.Column("access_token_enc", postgresql.BYTEA(), nullable=False),
        sa.Column("refresh_token_enc", postgresql.BYTEA(), nullable=True),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("token_version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name="fk_oauth_tokens_org_id_organizations",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_oauth_tokens"),
        sa.UniqueConstraint(
            "org_id",
            "connection_id",
            "token_version",
            name="uq_oauth_tokens_org_id_connection_id_token_version",
        ),
        sa.CheckConstraint("token_version >= 1", name="token_version_positive"),
        sa.CheckConstraint(
            _ENVELOPE_CHECK.format(col="access_token_enc"),
            name="access_token_enc_is_envelope",
        ),
        sa.CheckConstraint(
            "refresh_token_enc IS NULL OR " + _ENVELOPE_CHECK.format(col="refresh_token_enc"),
            name="refresh_token_enc_is_envelope",
        ),
    )
    op.create_index("ix_oauth_tokens_org_id", "oauth_tokens", ["org_id"], unique=False)

    op.execute("ALTER TABLE oauth_tokens ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE oauth_tokens FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON oauth_tokens "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "oauth_tokens")


def downgrade() -> None:
    op.drop_index("ix_oauth_tokens_org_id", table_name="oauth_tokens")
    op.drop_table("oauth_tokens")
