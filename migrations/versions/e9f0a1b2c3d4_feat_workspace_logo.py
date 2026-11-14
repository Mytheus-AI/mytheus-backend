"""feat(identity): the workspace logo on tenant_settings (ADR-0048)

Seven nullable/defaulted columns on `tenant_settings`, so no existing row changes meaning. The
table is already `tenant_isolation`-policied (RLS on org_id), and the columns inherit its
table-level grants to `mytheus_app`, so the policy covers them with no new grant.

- `logo_key`: the object in the private `mytheus-branding` bucket. The CHECK pins it under the
  row's own tenant prefix (`{org_id}/branding/`), so a bug can never point one tenant's row at
  another tenant's object.
- `logo_content_type`: what the bytes proved (PNG, JPEG, WebP; SVG is refused by the API).
- `logo_version`: bumped on every upload; part of the key and of the served URL (cache busting).
- `logo_bytes`, `logo_width`, `logo_height`: the upload's size, at most 2 MB and 4096 px.
- `logo_updated_at`: when it last changed (set on upload and on removal).

Revision ID: e9f0a1b2c3d4
Revises: d8e3f4a5b6c7
Create Date: 2026-11-14
"""

from typing import Sequence, Union

from alembic import op

revision: str = "e9f0a1b2c3d4"
down_revision: Union[str, None] = "d8e3f4a5b6c7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE tenant_settings
          ADD COLUMN logo_key text,
          ADD COLUMN logo_content_type text,
          ADD COLUMN logo_version integer NOT NULL DEFAULT 0,
          ADD COLUMN logo_bytes integer,
          ADD COLUMN logo_width integer,
          ADD COLUMN logo_height integer,
          ADD COLUMN logo_updated_at timestamptz,
          ADD CONSTRAINT ck_tenant_settings_logo_complete
            CHECK ((logo_key IS NULL) = (logo_content_type IS NULL)),
          ADD CONSTRAINT ck_tenant_settings_logo_key_prefix
            CHECK (logo_key IS NULL OR logo_key LIKE org_id::text || '/branding/%'),
          ADD CONSTRAINT ck_tenant_settings_logo_type
            CHECK (logo_content_type IS NULL
                   OR logo_content_type IN ('image/png', 'image/jpeg', 'image/webp')),
          ADD CONSTRAINT ck_tenant_settings_logo_version CHECK (logo_version >= 0),
          ADD CONSTRAINT ck_tenant_settings_logo_bytes
            CHECK (logo_bytes IS NULL OR (logo_bytes > 0 AND logo_bytes <= 2097152)),
          ADD CONSTRAINT ck_tenant_settings_logo_dims
            CHECK ((logo_width IS NULL OR logo_width BETWEEN 16 AND 4096)
                   AND (logo_height IS NULL OR logo_height BETWEEN 16 AND 4096));
        """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE tenant_settings
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_dims,
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_bytes,
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_version,
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_type,
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_key_prefix,
          DROP CONSTRAINT IF EXISTS ck_tenant_settings_logo_complete,
          DROP COLUMN IF EXISTS logo_updated_at,
          DROP COLUMN IF EXISTS logo_height,
          DROP COLUMN IF EXISTS logo_width,
          DROP COLUMN IF EXISTS logo_bytes,
          DROP COLUMN IF EXISTS logo_version,
          DROP COLUMN IF EXISTS logo_content_type,
          DROP COLUMN IF EXISTS logo_key;
        """)
