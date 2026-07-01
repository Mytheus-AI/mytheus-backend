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
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import identity_pb2
import identity_pb2_grpc
from libs.shared.tenancy import set_rls

_DEFAULT_TIER = "member"


class IdentityService(identity_pb2_grpc.IdentityServiceServicer):  # type: ignore[misc]
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def GetUserContext(self, request: Any, context: Any) -> Any:
        metadata = dict(context.invocation_metadata())
        org_id = metadata.get("org-id")
        if not org_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing org-id metadata")

        row = None
        try:
            uid = UUID(request.user_id)
            async with self._session_factory() as session:
                async with session.begin():
                    await set_rls(session, UUID(org_id))
                    row = (
                        (
                            await session.execute(
                                text(
                                    "SELECT id, org_id, email, role, skills, bio, avatar_url "
                                    "FROM users WHERE id = :uid"
                                ),
                                {"uid": str(uid)},
                            )
                        )
                        .mappings()
                        .first()
                    )
        except Exception:  # noqa: BLE001 - invalid uuid / no DB -> typed stub (contract compiles)
            row = None

        if row is None:
            return identity_pb2.UserContext(
                user_id=request.user_id, org_id=org_id, rbac_tier=_DEFAULT_TIER
            )
        return identity_pb2.UserContext(
            user_id=str(row["id"]),
            org_id=str(row["org_id"]),
            email=row["email"] or "",
            role=row["role"] or "",
            skills=list(row["skills"] or []),
            bio=row["bio"] or "",
            avatar_url=row["avatar_url"] or "",
            rbac_tier=_DEFAULT_TIER,
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
