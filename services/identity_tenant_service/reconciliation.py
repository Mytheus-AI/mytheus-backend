"""Hourly Clerk-versus-mirror reconciliation. Plan section 3.5 step 2.

The webhook is the fast path and it is not reliable on its own: a delivery can be dropped, a
signature secret can be rotated mid-flight, and the service can be down for a deploy while
Clerk exhausts its retries. Without a sweep, a user removed in Clerk keeps their local mirror
row indefinitely, which means they keep being resolvable as a principal.

This is a DIFF, not a replay, so it is idempotent by construction: running it twice changes
nothing the first run did not already change, and it needs no dedupe of its own.

The activity is pure with respect to Clerk: it takes the roster as an argument rather than
fetching it, so the diff logic is testable without a Clerk account or a network stub. The
Temporal schedule that supplies the roster is wired in `workflow-service`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_USER_SYNCED, audit
from libs.shared.tenancy import set_rls


@dataclass
class ClerkUser:
    """One entry of a tenant's Clerk roster."""

    clerk_user_id: str
    email: str
    avatar_url: str | None = None


@dataclass
class ReconcileResult:
    created: list[str] = field(default_factory=list)
    soft_deleted: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.created or self.soft_deleted)


async def reconcile_tenant(
    session: AsyncSession,
    org_id: UUID,
    roster: list[ClerkUser],
) -> ReconcileResult:
    """Bring the local `users` mirror into line with a tenant's Clerk roster.

    Present in Clerk, missing locally  -> create.
    Active locally, absent from Clerk  -> soft delete.

    Deliberately does NOT update the fields of users present on both sides. The webhook owns
    edits; this owns membership. Overwriting here would make a slow sweep silently revert a
    just-delivered webhook update, and the loser would depend on timing.
    """
    await set_rls(session, org_id)

    local = (
        (await session.execute(text("SELECT clerk_user_id FROM users WHERE deleted_at IS NULL")))
        .scalars()
        .all()
    )
    local_ids = set(local)
    remote_by_id = {u.clerk_user_id: u for u in roster}

    result = ReconcileResult()

    for clerk_user_id, user in remote_by_id.items():
        if clerk_user_id in local_ids:
            continue
        await session.execute(
            text(
                "INSERT INTO users (org_id, clerk_user_id, email, avatar_url) "
                "VALUES (:org, :cuid, :email, :avatar) "
                # A previously soft-deleted user who is back in the roster is a re-invite.
                "ON CONFLICT (org_id, clerk_user_id) DO UPDATE SET "
                "  deleted_at = NULL, email = EXCLUDED.email, updated_at = now()"
            ),
            {
                "org": str(org_id),
                "cuid": clerk_user_id,
                "email": user.email,
                "avatar": user.avatar_url,
            },
        )
        result.created.append(clerk_user_id)

    for clerk_user_id in sorted(local_ids - set(remote_by_id)):
        await session.execute(
            text(
                "UPDATE users SET deleted_at = now(), updated_at = now() "
                "WHERE clerk_user_id = :cuid AND deleted_at IS NULL"
            ),
            {"cuid": clerk_user_id},
        )
        result.soft_deleted.append(clerk_user_id)

    if result.changed:
        # Only audited when something actually diverged. An hourly sweep that logs "no drift"
        # every hour buries the one entry that matters under 8,760 that do not.
        await audit(
            session,
            org_id=org_id,
            action=ACTION_USER_SYNCED,
            actor_type="system",
            target_type="reconciliation",
            metadata={"created": result.created, "soft_deleted": result.soft_deleted},
        )

    return result
