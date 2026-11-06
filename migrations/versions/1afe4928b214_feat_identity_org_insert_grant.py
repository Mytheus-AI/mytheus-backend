"""feat(identity-tenant-service): column-level INSERT on organizations for POST /orgs

ADR-0025, approved by the user 2026-11-06. Supersedes the narrow decision recorded in
`a1c4e7f20b18` section 5 ("a tenant is born through the operator seam, never through a request
handler"): `POST /orgs` (identity-tenant-service, canon `_canon/01-service-catalog.md`) may now
create a tenant root on the request path.

Exactly the columns `services/identity_tenant_service/provisioning.py` writes, and nothing
wider:

- INSERT (id, name, slug, status, plan_id, clerk_org_id) - column-level.
- NO UPDATE, NO DELETE: suspension, renaming and deletion stay on the `mytheus_admin` seam.
- The SELECT carve-out from `a1c4e7f20b18` is untouched (a blanket `SELECT *` still refused).
  `RETURNING id` and the `ON CONFLICT (slug)` arbiter only need columns already readable.

Who may call the route is decided by the route, not the role: `require_tier("owner")` in a
tenant listed in `PLATFORM_OPERATOR_ORG_IDS` (a proposed name, ADR-0025 D3). CI:
`tests/test_roles_and_policies.py::test_app_role_insert_on_organizations_is_exactly_the_provisioning_columns`.

Revision ID: 1afe4928b214
Revises: a2afd3bef634
Create Date: 2026-11-06

"""

from typing import Sequence, Union

from alembic import op

from migrations._grants import APP_ROLE

# revision identifiers, used by Alembic.
revision: str = "1afe4928b214"
down_revision: Union[str, None] = "a2afd3bef634"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

ORG_INSERTABLE_COLUMNS = "id, name, slug, status, plan_id, clerk_org_id"


def upgrade() -> None:
    op.execute(f"GRANT INSERT ({ORG_INSERTABLE_COLUMNS}) ON organizations TO {APP_ROLE};")


def downgrade() -> None:
    op.execute(f"REVOKE INSERT ({ORG_INSERTABLE_COLUMNS}) ON organizations FROM {APP_ROLE};")
