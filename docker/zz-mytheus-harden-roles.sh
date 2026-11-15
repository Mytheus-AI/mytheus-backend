#!/bin/sh
# Runs once, on a NEW data volume, after supabase/postgres's own bootstrap (`migrate.sh` sorts
# before this file in /docker-entrypoint-initdb.d). Applies harden-roles.sql as supabase_admin,
# the image's superuser, using the same connection settings migrate.sh uses. A failure stops the
# first boot, which is what we want: a database that came up unhardened must not look healthy.
set -eu
export PGHOST="${POSTGRES_HOST:-localhost}" PGPORT="${POSTGRES_PORT:-5432}"
export PGDATABASE="${POSTGRES_DB:-postgres}" PGPASSWORD="${POSTGRES_PASSWORD:-}"
echo "$0: hardening image roles (harden-roles.sql)"
psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supabase_admin \
  -f /etc/mytheus/harden-roles.sql
