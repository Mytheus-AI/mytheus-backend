"""feat(identity): users.country and users.subdivision, where a person says they work

Two nullable columns on `users`, so no existing row changes meaning. The table is already
`tenant_isolation`-policied (RLS on org_id) and the columns inherit its grants.

- `country`: ISO 3166-1 alpha-2, upper case (`IN`).
- `subdivision`: ISO 3166-2 (`IN-KA`), only with a country and starting with it; NULL when the
  person did not pick one or the country has no list in the app.

Set by the person only (`PATCH /users/{id}`, self). The ops console counts members by them and
never lists who; the time zone alone cannot place a member by state, and
`data_residency.region` is a storage setting, not a location.

Revision ID: f1a2b3c4d5e6
Revises: b6d1e4a7c902
Create Date: 2026-11-14
"""

from collections.abc import Sequence

from alembic import op

revision: str = "f1a2b3c4d5e6"
down_revision: str | None = "b6d1e4a7c902"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE users
          ADD COLUMN country text,
          ADD COLUMN subdivision text,
          ADD CONSTRAINT ck_users_country_format
            CHECK (country IS NULL OR country ~ '^[A-Z]{2}$'),
          ADD CONSTRAINT ck_users_subdivision_format
            CHECK (
              subdivision IS NULL
              OR (
                country IS NOT NULL
                AND subdivision ~ '^[A-Z]{2}-[A-Z0-9]{1,3}$'
                AND left(subdivision, 2) = country
              )
            );
        """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE users
          DROP CONSTRAINT IF EXISTS ck_users_subdivision_format,
          DROP CONSTRAINT IF EXISTS ck_users_country_format,
          DROP COLUMN IF EXISTS subdivision,
          DROP COLUMN IF EXISTS country;
        """)
