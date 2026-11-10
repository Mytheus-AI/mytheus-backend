"""integration-service persistence: connections, encrypted tokens, cursors, the id map, the
webhook inbox. Raw SQL over the caller's session, which must already have `app.current_org`
set (every function takes the session the route or worker opened with `set_rls`).

Tokens. `oauth_tokens.*_enc` hold `libs/shared/crypto` envelopes (ADR-0024): AES-256-GCM under a
per-tenant DEK wrapped by the KEK, AAD bound to `org_id`, the purpose (column) and the
`connection_id`. A plaintext token exists only inside `TokenSet` in process memory, is never
returned by a public route, never logged (`TokenSet.__repr__` is redacted) and never written
anywhere but through `EnvelopeCipher`.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.crypto import EnvelopeCipher, key_provider_from_env
from services.integration_service.clients import OAuthClient, ProviderError, TokenGrant

ACCESS_PURPOSE = "oauth_tokens.access_token"
REFRESH_PURPOSE = "oauth_tokens.refresh_token"
REFRESH_SKEW = timedelta(seconds=90)


@lru_cache(maxsize=1)
def get_cipher() -> EnvelopeCipher:
    """Built on first use from the configured custodian; fails closed when keys are missing."""
    return EnvelopeCipher(key_provider_from_env())


@dataclass(frozen=True)
class Connection:
    id: uuid.UUID
    org_id: uuid.UUID
    provider: str
    external_account_id: str | None
    status: str
    scopes: list[str]
    installed_by: uuid.UUID
    created_at: datetime
    updated_at: datetime
    last_synced_at: datetime | None


@dataclass
class TokenSet:
    access_token: str
    refresh_token: str | None
    expires_at: datetime | None
    token_version: int

    def __repr__(self) -> str:
        return f"TokenSet(version={self.token_version}, expires_at={self.expires_at}, <redacted>)"


class NotConnected(Exception):
    """The tenant has no usable connection for the provider."""


_CONN_COLS = (
    "id, org_id, provider, external_account_id, status, scopes, installed_by, created_at, "
    "updated_at, last_synced_at"
)


def _conn(row: Any) -> Connection:
    data = dict(row)
    data["scopes"] = list(data.get("scopes") or [])
    return Connection(**data)


async def list_connections(session: AsyncSession) -> dict[str, Connection]:
    rows = await session.execute(text(f"SELECT {_CONN_COLS} FROM integration_connections"))
    return {r["provider"]: _conn(r) for r in rows.mappings()}


async def get_connection(
    session: AsyncSession, provider: str, *, active_only: bool = False, for_update: bool = False
) -> Connection | None:
    sql = f"SELECT {_CONN_COLS} FROM integration_connections WHERE provider = :p"
    if active_only:
        sql += " AND status = 'active'"
    if for_update:
        sql += " FOR UPDATE"
    row = (await session.execute(text(sql), {"p": provider})).mappings().first()
    return _conn(row) if row else None


async def route_connection(
    session: AsyncSession, provider: str, external_account_id: str
) -> Connection | None:
    """The webhook-route lookup (migration 7c3e9a1f5b20, policy `webhook_route`): with no tenant
    set yet, read the ONE connection whose `provider:external_account_id` the verified webhook
    names. The GUC is transaction-local, like `app.current_org`."""
    await session.execute(
        text("SELECT set_config('app.integration_route', :r, true)"),
        {"r": f"{provider}:{external_account_id}"},
    )
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_CONN_COLS} FROM integration_connections "
                    "WHERE provider = :p AND external_account_id = :e AND status <> 'disconnected'"
                ),
                {"p": provider, "e": external_account_id},
            )
        )
        .mappings()
        .first()
    )
    await session.execute(text("SELECT set_config('app.integration_route', '', true)"))
    return _conn(row) if row else None


async def upsert_connection(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    provider: str,
    external_account_id: str | None,
    scopes: list[str],
    installed_by: uuid.UUID,
) -> Connection:
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO integration_connections "
                    "(org_id, provider, external_account_id, status, scopes, installed_by) "
                    "VALUES (:o, :p, :e, 'active', :s, :u) "
                    "ON CONFLICT (org_id, provider) DO UPDATE SET "
                    "external_account_id = EXCLUDED.external_account_id, status = 'active', "
                    "scopes = EXCLUDED.scopes, installed_by = EXCLUDED.installed_by, "
                    "updated_at = now() "
                    f"RETURNING {_CONN_COLS}"
                ),
                {
                    "o": str(org_id),
                    "p": provider,
                    "e": external_account_id,
                    "s": scopes,
                    "u": str(installed_by),
                },
            )
        )
        .mappings()
        .one()
    )
    return _conn(row)


async def set_status(session: AsyncSession, connection_id: uuid.UUID, status: str) -> None:
    await session.execute(
        text("UPDATE integration_connections SET status = :s, updated_at = now() WHERE id = :id"),
        {"s": status, "id": str(connection_id)},
    )


async def touch_synced(session: AsyncSession, connection_id: uuid.UUID) -> None:
    await session.execute(
        text("UPDATE integration_connections SET last_synced_at = now() WHERE id = :id"),
        {"id": str(connection_id)},
    )


# --- tokens ----------------------------------------------------------------------------------


def _expiry(expires_in: int | None) -> datetime | None:
    return datetime.now(UTC) + timedelta(seconds=int(expires_in)) if expires_in else None


async def store_tokens(session: AsyncSession, conn: Connection, grant: TokenGrant) -> None:
    """Replace the connection's tokens (a reconnect starts again at version 1)."""
    cipher = get_cipher()
    ctx = {"connection_id": str(conn.id)}
    access = cipher.encrypt_str(conn.org_id, ACCESS_PURPOSE, grant.access_token, ctx)
    refresh = (
        cipher.encrypt_str(conn.org_id, REFRESH_PURPOSE, grant.refresh_token, ctx)
        if grant.refresh_token
        else None
    )
    await session.execute(
        text("DELETE FROM oauth_tokens WHERE connection_id = :c"), {"c": str(conn.id)}
    )
    await session.execute(
        text(
            "INSERT INTO oauth_tokens (org_id, connection_id, access_token_enc, "
            "refresh_token_enc, expires_at, token_version) VALUES (:o, :c, :a, :r, :x, 1)"
        ),
        {
            "o": str(conn.org_id),
            "c": str(conn.id),
            "a": access,
            "r": refresh,
            "x": _expiry(grant.expires_in),
        },
    )


async def load_tokens(session: AsyncSession, conn: Connection) -> TokenSet:
    row = (
        (
            await session.execute(
                text(
                    "SELECT access_token_enc, refresh_token_enc, expires_at, token_version "
                    "FROM oauth_tokens WHERE connection_id = :c "
                    "ORDER BY token_version DESC LIMIT 1"
                ),
                {"c": str(conn.id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise NotConnected(conn.provider)
    cipher = get_cipher()
    ctx = {"connection_id": str(conn.id)}
    return TokenSet(
        access_token=cipher.decrypt_str(
            conn.org_id, ACCESS_PURPOSE, bytes(row["access_token_enc"]), ctx
        ),
        refresh_token=(
            cipher.decrypt_str(conn.org_id, REFRESH_PURPOSE, bytes(row["refresh_token_enc"]), ctx)
            if row["refresh_token_enc"] is not None
            else None
        ),
        expires_at=row["expires_at"],
        token_version=int(row["token_version"]),
    )


async def delete_tokens(session: AsyncSession, connection_id: uuid.UUID) -> None:
    await session.execute(
        text("DELETE FROM oauth_tokens WHERE connection_id = :c"), {"c": str(connection_id)}
    )


async def access_token(session: AsyncSession, conn: Connection, oauth: OAuthClient) -> TokenSet:
    """A usable access token, refreshing (and bumping `token_version`) when it is about to
    expire. A refresh the provider rejects marks the connection `degraded` and raises
    `NotConnected`: the tenant has to reconnect."""
    if conn.status == "disconnected":
        raise NotConnected(conn.provider)
    tokens = await load_tokens(session, conn)
    if tokens.expires_at is None or tokens.expires_at - REFRESH_SKEW > datetime.now(UTC):
        return tokens
    if not tokens.refresh_token:
        return tokens  # nothing to refresh with; let the provider answer
    try:
        grant = await oauth.refresh(tokens.refresh_token)
    except ProviderError as exc:
        if exc.status is not None and 400 <= exc.status < 500 or exc.reason.startswith("invalid"):
            await set_status(session, conn.id, "degraded")
            raise NotConnected(conn.provider) from None
        raise
    cipher = get_cipher()
    ctx = {"connection_id": str(conn.id)}
    new_version = tokens.token_version + 1
    refresh_token = grant.refresh_token or tokens.refresh_token
    await session.execute(
        text(
            "UPDATE oauth_tokens SET access_token_enc = :a, refresh_token_enc = :r, "
            "expires_at = :x, token_version = :v, updated_at = now() "
            "WHERE connection_id = :c AND token_version = :old"
        ),
        {
            "a": cipher.encrypt_str(conn.org_id, ACCESS_PURPOSE, grant.access_token, ctx),
            "r": cipher.encrypt_str(conn.org_id, REFRESH_PURPOSE, refresh_token, ctx),
            "x": _expiry(grant.expires_in),
            "v": new_version,
            "c": str(conn.id),
            "old": tokens.token_version,
        },
    )
    return TokenSet(grant.access_token, refresh_token, _expiry(grant.expires_in), new_version)


# --- cursors ---------------------------------------------------------------------------------


async def get_cursor(
    session: AsyncSession, connection_id: uuid.UUID, kind: str
) -> tuple[dict[str, Any], datetime | None] | None:
    row = (
        await session.execute(
            text(
                "SELECT cursor, expires_at FROM integration_cursors "
                "WHERE connection_id = :c AND kind = :k"
            ),
            {"c": str(connection_id), "k": kind},
        )
    ).first()
    return (dict(row[0]), row[1]) if row else None


async def put_cursor(
    session: AsyncSession,
    org_id: uuid.UUID,
    connection_id: uuid.UUID,
    kind: str,
    cursor: dict[str, Any],
    expires_at: datetime | None = None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO integration_cursors (org_id, connection_id, kind, cursor, expires_at) "
            "VALUES (:o, :c, :k, CAST(:cur AS jsonb), :x) "
            "ON CONFLICT (org_id, connection_id, kind) DO UPDATE SET "
            "cursor = EXCLUDED.cursor, expires_at = EXCLUDED.expires_at, updated_at = now()"
        ),
        {
            "o": str(org_id),
            "c": str(connection_id),
            "k": kind,
            "cur": json.dumps(cursor),
            "x": expires_at,
        },
    )


async def delete_cursors(session: AsyncSession, connection_id: uuid.UUID) -> None:
    await session.execute(
        text("DELETE FROM integration_cursors WHERE connection_id = :c"),
        {"c": str(connection_id)},
    )


# --- id map ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Link:
    nexus_id: uuid.UUID
    external_id: str
    external_url: str | None
    last_synced_status: str | None


async def link_for_nexus(
    session: AsyncSession, provider: str, kind: str, nexus_id: uuid.UUID
) -> Link | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT nexus_id, external_id, external_url, last_synced_status "
                    "FROM integration_id_map WHERE provider = :p AND nexus_kind = :k "
                    "AND nexus_id = :n"
                ),
                {"p": provider, "k": kind, "n": str(nexus_id)},
            )
        )
        .mappings()
        .first()
    )
    return Link(**dict(row)) if row else None


async def link_for_external(
    session: AsyncSession, provider: str, kind: str, external_id: str
) -> Link | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT nexus_id, external_id, external_url, last_synced_status "
                    "FROM integration_id_map WHERE provider = :p AND nexus_kind = :k "
                    "AND external_id = :e"
                ),
                {"p": provider, "k": kind, "e": external_id},
            )
        )
        .mappings()
        .first()
    )
    return Link(**dict(row)) if row else None


async def put_link(
    session: AsyncSession,
    org_id: uuid.UUID,
    provider: str,
    kind: str,
    nexus_id: uuid.UUID,
    external_id: str,
    external_url: str | None,
    last_synced_status: str | None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO integration_id_map (org_id, provider, nexus_kind, nexus_id, "
            "external_id, external_url, last_synced_status) VALUES (:o, :p, :k, :n, :e, :u, :s) "
            "ON CONFLICT (org_id, provider, nexus_kind, nexus_id) DO UPDATE SET "
            "external_id = EXCLUDED.external_id, external_url = EXCLUDED.external_url, "
            "last_synced_status = COALESCE(EXCLUDED.last_synced_status, "
            "integration_id_map.last_synced_status), updated_at = now()"
        ),
        {
            "o": str(org_id),
            "p": provider,
            "k": kind,
            "n": str(nexus_id),
            "e": external_id,
            "u": external_url,
            "s": last_synced_status,
        },
    )


async def set_synced_status(
    session: AsyncSession, provider: str, kind: str, nexus_id: uuid.UUID, status: str
) -> None:
    await session.execute(
        text(
            "UPDATE integration_id_map SET last_synced_status = :s, updated_at = now() "
            "WHERE provider = :p AND nexus_kind = :k AND nexus_id = :n"
        ),
        {"s": status, "p": provider, "k": kind, "n": str(nexus_id)},
    )


# --- webhook inbox ---------------------------------------------------------------------------


async def inbox_insert(
    session: AsyncSession,
    org_id: uuid.UUID,
    provider: str,
    delivery_id: str,
    event_type: str | None,
    payload: dict[str, Any],
) -> bool:
    """One row per provider delivery. False when the delivery id was seen before (replay)."""
    row = (
        await session.execute(
            text(
                "INSERT INTO integration_webhook_inbox "
                "(org_id, provider, delivery_id, event_type, payload) "
                "VALUES (:o, :p, :d, :t, CAST(:b AS jsonb)) "
                "ON CONFLICT (org_id, provider, delivery_id) DO NOTHING RETURNING id"
            ),
            {
                "o": str(org_id),
                "p": provider,
                "d": delivery_id[:300],
                "t": event_type,
                "b": json.dumps(payload),
            },
        )
    ).first()
    return row is not None
