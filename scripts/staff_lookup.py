"""Print the internal `users.id` for an email, to add that person to MYTHEUS_STAFF_USER_IDS.

    uv run python -m scripts.staff_lookup someone@example.com

Read-only. Runs as `mytheus_admin` through `ADMIN_DATABASE_URL` (the operator seam, never the
request path), and binds each workspace in turn so RLS still applies to every `users` read.
A person can have a user row in more than one workspace; every match is printed. Granting
staff access is then a deployment edit (ADR-0046 D1): append the id to
`MYTHEUS_STAFF_USER_IDS` on admin-ops-service and restart it.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def lookup(email: str) -> list[tuple[str, str, str]]:
    url = os.environ.get("ADMIN_DATABASE_URL")
    if not url:
        raise SystemExit("ADMIN_DATABASE_URL is not set")
    engine = create_async_engine(url)
    found: list[tuple[str, str, str]] = []
    try:
        async with engine.connect() as conn:
            orgs = (
                await conn.execute(
                    text("SELECT id, name FROM organizations WHERE deleted_at IS NULL")
                )
            ).all()
            for org_id, name in orgs:
                async with conn.begin():
                    await conn.execute(
                        text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_id)}
                    )
                    rows = (
                        await conn.execute(
                            text(
                                "SELECT id FROM users WHERE lower(email) = lower(:e) "
                                "AND deleted_at IS NULL"
                            ),
                            {"e": email},
                        )
                    ).all()
                found.extend((str(r[0]), str(org_id), name) for r in rows)
    finally:
        await engine.dispose()
    return found


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m scripts.staff_lookup <email>")
    matches = asyncio.run(lookup(sys.argv[1]))
    if not matches:
        raise SystemExit("No user with that email. They must sign in to Mytheus once first.")
    for user_id, org_id, name in matches:
        print(f"{user_id}  (workspace {name}, {org_id})")
    print("\nAdd the id to MYTHEUS_STAFF_USER_IDS on admin-ops-service, then restart it.")


if __name__ == "__main__":
    main()
