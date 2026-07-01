"""identity-tenant-service.GetUserContext gRPC: org-id metadata required (fail-closed)."""

from __future__ import annotations

import uuid

import grpc
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import identity_pb2
import identity_pb2_grpc
from services.identity_tenant_service.grpc_server import IdentityService


async def _start_server(db_url: str):
    engine = create_async_engine(db_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        IdentityService(session_factory), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    return server, port


async def test_get_user_context_requires_org_id(migrated_db):
    server, port = await _start_server(migrated_db)
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)

            # With org-id metadata -> a typed UserContext (stub, tier "member").
            resp = await stub.GetUserContext(
                identity_pb2.GetUserContextRequest(user_id=str(uuid.uuid4())),
                metadata=(("org-id", str(uuid.uuid4())),),
            )
            assert resp.rbac_tier == "member"

            # Without org-id metadata -> UNAUTHENTICATED (fail closed).
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await stub.GetUserContext(
                    identity_pb2.GetUserContextRequest(user_id=str(uuid.uuid4()))
                )
            assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    finally:
        await server.stop(None)
