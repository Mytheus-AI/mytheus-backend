"""Find a staff account's Clerk user id, to add it to MYTHEUS_STAFF_CLERK_USER_IDS (ADR-0046 D1).

    uv run python -m scripts.staff_lookup staff@example.com

On the server, inside the admin-ops-service container (it has CLERK_SECRET_KEY and the
database URL):

    docker exec <admin-ops-service container> python -m scripts.staff_lookup staff@example.com

Read-only. A superadmin is a dedicated account that belongs to NO workspace, so this refuses an
account that is a member of any Clerk organization or has a `users` row in any Mytheus
workspace (`clerk_user_in_any_tenant`, which binds each workspace in turn so RLS still applies).
For an account that passes, it prints the Clerk user id to add and the Staff ID that support
grants will name. Granting staff access is then a deployment edit: append the Clerk user id to
`MYTHEUS_STAFF_CLERK_USER_IDS` in admin-ops-service's environment and recreate that service.

The database URL is `ADMIN_DATABASE_URL` when set, else `DATABASE_URL`.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.shared import clerk_admin
from services.admin_ops_service.common import STAFF_ENV, staff_id_for


async def in_any_workspace(clerk_user_id: str) -> bool:
    url = os.environ.get("ADMIN_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("Set ADMIN_DATABASE_URL or DATABASE_URL.")
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            found = await conn.execute(
                text("SELECT clerk_user_in_any_tenant(:c)"), {"c": clerk_user_id}
            )
            return bool(found.scalar())
    finally:
        await engine.dispose()


async def lookup(email: str) -> int:
    try:
        accounts = await clerk_admin.users_by_email(email)
    except clerk_admin.ClerkUnavailable as exc:
        print(f"Could not ask Clerk: {exc}. Is CLERK_SECRET_KEY set?")
        return 2
    if not accounts:
        print("No Clerk account with that email. Sign up for a new Mytheus account with it first,")
        print("and stop at the page that asks you to join or create an organization.")
        return 1
    ok = 0
    for account in accounts:
        print(f"Clerk account {account.id} ({account.name or 'no name'}, {account.email})")
        if account.organization_count:
            print(
                f"  REFUSED: it belongs to {account.organization_count} organization(s). "
                "A superadmin must be a separate account that belongs to no workspace."
            )
            continue
        if await in_any_workspace(account.id):
            print("  REFUSED: it is a member of a Mytheus workspace. Use a separate account.")
            continue
        ok += 1
        print("  OK: belongs to no workspace, so it can be a superadmin.")
        print(f"  Add to {STAFF_ENV}:  {account.id}")
        print(f"  Its Staff ID (for support grants): {staff_id_for(account.id)}")
    return 0 if ok else 1


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m scripts.staff_lookup <email>")
    raise SystemExit(asyncio.run(lookup(sys.argv[1].strip())))


if __name__ == "__main__":
    main()
