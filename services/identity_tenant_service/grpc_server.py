"""gRPC server for identity-tenant-service.GetUserContext.

org_id is read from gRPC metadata ("org-id"), NOT the message body. Missing metadata fails
closed (UNAUTHENTICATED). The handler sets app.current_org (so RLS applies) and reads the users
row + RBAC tier. This band returns a typed UserContext (real data where the user exists, a typed
stub otherwise, RBAC tier "member" default) so the contract compiles and the agent tier can bind
against it in later bands.

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

# The user row and the authorization tier in one round trip. A LEFT JOIN, not an inner one:
# a user with no grant is a real state (invited, not yet authorized) and must resolve to "no
# tier" rather than to "no such user", which would be indistinguishable from a cross-tenant
# probe. `rbac_grants` is RLS-policied like every other tenant table, so this join cannot
# reach across tenants even if `users` somehow did.
_SELECT_USER_CONTEXT = text(
    "SELECT u.id, u.org_id, u.email, u.role, u.skills, u.bio, u.avatar_url, g.tier "
    "FROM users u "
    "LEFT JOIN rbac_grants g ON g.user_id = u.id AND g.org_id = u.org_id "
    "WHERE u.id = :uid AND u.deleted_at IS NULL"
)


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
                    row = (
                        (await session.execute(_SELECT_USER_CONTEXT, {"uid": str(uid)}))
                        .mappings()
                        .first()
                    )
        except (SQLAlchemyError, OSError) as exc:
            # Infrastructure failure. Never synthesise a context from it.
            #
            # OSError is caught alongside SQLAlchemyError deliberately: asyncpg raises
            # ConnectionRefusedError (an OSError) directly when the server is unreachable,
            # before SQLAlchemy ever wraps it, so catching SQLAlchemyError alone lets a
            # refused connection escape as an UNKNOWN status.
            #
            # Both branches DENY. That is the whole difference from the defect this
            # replaces: the old bare `except Exception` answered failure by granting a
            # context. Every argument-shaped error is already validated above, so anything
            # reaching here is genuinely infrastructure.
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"identity store unavailable: {exc}")

        if row is None:
            # Either no such user, or the user belongs to another tenant and RLS hid the
            # row. Both are NOT_FOUND: existence is tenant-confidential, so this must not
            # distinguish them (the REST analogue is 404-never-403).
            await context.abort(grpc.StatusCode.NOT_FOUND, "no such user in this tenant")
            # abort() always raises. The servicer base is typed Any, so mypy cannot see
            # that; this keeps `row` narrowed below instead of silencing it with a cast.
            raise AssertionError("unreachable: context.abort() raises")

        return identity_pb2.UserContext(
            user_id=str(row["id"]),
            org_id=str(row["org_id"]),
            email=row["email"] or "",
            # DOMAIN role. Not a permission. A caller that branches on this for authorization
            # is reading the wrong field; `rbac_tier` is the one that decides.
            role=row["role"] or "",
            skills=list(row["skills"] or []),
            bio=row["bio"] or "",
            avatar_url=row["avatar_url"] or "",
            # Empty when the user holds no grant, which callers must read as "no permissions"
            # rather than defaulting. Plan 0 returned a hardcoded "member" here for want of a
            # table to read; that is a grant this service was never told to make.
            rbac_tier=row["tier"] or "",
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
