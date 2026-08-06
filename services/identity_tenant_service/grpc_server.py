"""gRPC server for identity-tenant-service.GetUserContext.

org_id is read from gRPC metadata ("org-id"), NOT the message body. Missing metadata fails
closed (UNAUTHENTICATED). The handler sets app.current_org (so RLS applies), calls the shared,
Redis-cached `context.service.get_user_context` for role/skills/bio/avatar_url/team/department
(Plan 5-6 section 3.3 - this is the fix for the gap Plan 1-2's verification found: this RPC used
to hit Postgres on every call), and reads `rbac_grants` separately and UNCACHED for the tier
(ADR-0010 D2: cache the mapping, never the verdict - an authorization decision is exactly the
kind of value a 5-minute-stale cache must not be allowed to answer).

Run: python -m services.identity_tenant_service.grpc_server
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import grpc
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import identity_pb2
import identity_pb2_grpc
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.context.service import cache_expires_at_ms, get_user_context

# rbac_grants is RLS-policied like every other tenant table, so this cannot reach across
# tenants even if `users` somehow did. Kept as its own uncached query, deliberately separate
# from get_user_context (see module docstring).
_SELECT_TIER = text("SELECT tier FROM rbac_grants WHERE user_id = :uid AND org_id = :org_id")
_SELECT_EMAIL = text("SELECT email FROM users WHERE id = :uid AND deleted_at IS NULL")


class IdentityService(identity_pb2_grpc.IdentityServiceServicer):  # type: ignore[misc]
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def GetUserContext(self, request: Any, context: Any) -> Any:
        """Resolve a user's context. FAILS CLOSED on every path that is not a real read.

        Defect D3. The Plan 0 handler wrapped everything in a bare `except Exception` and,
        on any failure, returned a well-formed UserContext echoing the caller's own
        asserted org_id with a default RBAC tier. Three distinct conditions - a malformed
        user id, an unknown user, and the database being unreachable - all collapsed into
        "here is a valid member context". Section 3.4 makes this handler read
        `rbac_grants`, at which point that same code path reads as "database unreachable
        therefore grant access". Each condition now gets its own status and returns the
        caller nothing.
        """
        metadata = dict(context.invocation_metadata())
        raw_org_id = metadata.get("org-id")
        if not raw_org_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing org-id metadata")

        try:
            org_id = UUID(raw_org_id)
        except ValueError:
            # org_id rides gRPC metadata from the mesh, but a malformed one can never be
            # used to set app.current_org, so it is an auth failure rather than a bad arg.
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "malformed org-id metadata")

        try:
            uid = UUID(request.user_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "user_id is not a UUID")

        try:
            async with self._session_factory() as session:
                async with session.begin():
                    await set_rls(session, org_id)
                    # get_user_context() is cache-then-compute and, on any Redis failure,
                    # falls straight through to this same Postgres compute internally (see
                    # its docstring) - it never raises for a cache problem, only for a real
                    # Postgres failure, which is exactly what this method's own
                    # SQLAlchemyError/OSError handling below is for.
                    ctx = await get_user_context(session, org_id, uid)
                    email_row = None
                    tier_row = None
                    if ctx is not None:
                        email_row = (
                            (await session.execute(_SELECT_EMAIL, {"uid": str(uid)}))
                            .mappings()
                            .first()
                        )
                        tier_row = (
                            (
                                await session.execute(
                                    _SELECT_TIER, {"uid": str(uid), "org_id": str(org_id)}
                                )
                            )
                            .mappings()
                            .first()
                        )
        except (SQLAlchemyError, OSError) as exc:
            # Infrastructure failure. Never synthesise a context from it.
            #
            # OSError is caught alongside SQLAlchemyError deliberately: asyncpg raises
            # ConnectionRefusedError (an OSError) directly when the server is unreachable,
            # before SQLAlchemy ever wraps it, so catching SQLAlchemyError alone lets a
            # refused connection escape as an UNKNOWN status. This does NOT catch Redis
            # failures - get_user_context swallows those internally by design, so an OSError
            # reaching here is genuinely Postgres, never Redis.
            #
            # Both branches DENY. That is the whole difference from the defect this
            # replaces: the old bare `except Exception` answered failure by granting a
            # context. Every argument-shaped error is already validated above, so anything
            # reaching here is genuinely infrastructure.
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"identity store unavailable: {exc}")

        if ctx is None:
            # Either no such user, or the user belongs to another tenant and RLS hid the
            # row. Both are NOT_FOUND: existence is tenant-confidential, so this must not
            # distinguish them (the REST analogue is 404-never-403).
            await context.abort(grpc.StatusCode.NOT_FOUND, "no such user in this tenant")
            # abort() always raises. The servicer base is typed Any, so mypy cannot see
            # that; this keeps `ctx` narrowed below instead of silencing it with a cast.
            raise AssertionError("unreachable: context.abort() raises")

        department = ctx["department"]
        return identity_pb2.UserContext(
            user_id=ctx["user_id"],
            org_id=ctx["org_id"],
            email=(email_row["email"] if email_row else None) or "",
            # DOMAIN role. Not a permission. A caller that branches on this for authorization
            # is reading the wrong field; `rbac_tier` is the one that decides.
            role=ctx["role"] or "",
            skills=list(ctx["skills"] or []),
            bio=ctx["bio"] or "",
            avatar_url=ctx["avatar_url"] or "",
            # Empty when the user holds no grant, which callers must read as "no permissions"
            # rather than defaulting. Plan 0 returned a hardcoded "member" here for want of a
            # table to read; that is a grant this service was never told to make. Read
            # UNCACHED (see module docstring) - never sourced from ctx.
            rbac_tier=(tier_row["tier"] if tier_row else None) or "",
            team_ids=[t["id"] for t in ctx["team"]],
            department_id=department["id"] if department else "",
            cache_expires_at=cache_expires_at_ms(),
        )


async def serve(port: int = 50051) -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        IdentityService(session_factory), server
    )
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
