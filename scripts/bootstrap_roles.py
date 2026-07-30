"""Set login passwords for the three application roles. Run once per environment, after
`alembic upgrade head`.

Migration `a1c4e7f20b18` creates `mytheus_app`, `mytheus_relay` and `mytheus_admin` with no
password, so they exist and hold exactly the right grants but cannot authenticate. This
script supplies the passwords, because a password in a migration is a password in git
history and in every clone of it, forever.

Usage:

    MYTHEUS_APP_PASSWORD=... MYTHEUS_RELAY_PASSWORD=... MYTHEUS_ADMIN_PASSWORD=... \\
      BOOTSTRAP_DATABASE_URL=postgresql://<owner>:<pw>@host:5432/mytheus \\
      uv run python -m scripts.bootstrap_roles

`BOOTSTRAP_DATABASE_URL` must name the schema owner or another role holding CREATEROLE. It is
NOT `DATABASE_URL`: that one names `mytheus_app`, which by design cannot alter itself.

In production the three passwords come from the secrets manager (mytheus-platform's `secrets`
Terraform layer), and each service receives only the one it needs. Do not put all three in one
env file: the whole point of splitting the roles is that the web process cannot obtain the
relay's or the operator's credentials.
"""

from __future__ import annotations

import os
import sys

import psycopg2

ROLE_ENV_VARS = {
    "mytheus_app": "MYTHEUS_APP_PASSWORD",
    "mytheus_relay": "MYTHEUS_RELAY_PASSWORD",
    "mytheus_admin": "MYTHEUS_ADMIN_PASSWORD",
}


def main() -> int:
    url = os.environ.get("BOOTSTRAP_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("BOOTSTRAP_DATABASE_URL (or DATABASE_URL) is required", file=sys.stderr)
        return 2
    url = url.replace("+asyncpg", "")

    missing = [var for var in ROLE_ENV_VARS.values() if not os.environ.get(var)]
    if missing:
        # Fail rather than default. A default password on the role that holds every tenant's
        # data is worse than a role that cannot log in at all.
        print(f"missing required password env vars: {', '.join(missing)}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for role, var in ROLE_ENV_VARS.items():
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
                if cur.fetchone() is None:
                    print(
                        f"role {role} does not exist; run `alembic upgrade head` first",
                        file=sys.stderr,
                    )
                    return 1
                # Parameterised so the password is never interpolated into the statement text.
                cur.execute(f"ALTER ROLE {role} WITH LOGIN PASSWORD %s", (os.environ[var],))
                print(f"set password for {role}")

            # The property the whole exercise exists for. Assert it here too, not only in CI:
            # this script is the last thing to touch these roles before they serve traffic.
            cur.execute(
                "SELECT rolname FROM pg_roles "
                "WHERE rolname = ANY(%s) AND (rolsuper OR rolbypassrls)",
                (list(ROLE_ENV_VARS),),
            )
            offenders = [r[0] for r in cur.fetchall()]
            if offenders:
                print(
                    f"REFUSING: {', '.join(offenders)} hold rolsuper or rolbypassrls, "
                    "so row-level security does not apply to them (defect D1)",
                    file=sys.stderr,
                )
                return 1
    finally:
        conn.close()

    print("all three roles are set and none bypasses RLS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
