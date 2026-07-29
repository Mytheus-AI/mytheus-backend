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

            # Without org-id metadata -> UNAUTHENTICATED (fail closed).
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await stub.GetUserContext(
                    identity_pb2.GetUserContextRequest(user_id=str(uuid.uuid4()))
                )
            assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    finally:
        await server.stop(None)


async def test_unknown_user_is_not_found_not_a_synthesized_context(migrated_db):
    """Defect D3. A user that does not exist must NOT yield a usable context.

    The Plan 0 handler returned a well-formed UserContext echoing the caller's own asserted
    org_id with rbac_tier="member" for any unknown user id. That is an authorization
    decision invented out of nothing: once section 3.4 makes this handler read
    rbac_grants, the same code path becomes "unknown user, therefore member".
    """
    server, port = await _start_server(migrated_db)
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await stub.GetUserContext(
                    identity_pb2.GetUserContextRequest(user_id=str(uuid.uuid4())),
                    metadata=(("org-id", str(uuid.uuid4())),),
                )
            assert exc.value.code() == grpc.StatusCode.NOT_FOUND
    finally:
        await server.stop(None)


async def test_malformed_user_id_is_invalid_argument(migrated_db):
    """A non-UUID user_id is a caller error, not grounds for a default-tier context."""
    server, port = await _start_server(migrated_db)
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await stub.GetUserContext(
                    identity_pb2.GetUserContextRequest(user_id="not-a-uuid"),
                    metadata=(("org-id", str(uuid.uuid4())),),
                )
            assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    finally:
        await server.stop(None)


async def test_database_failure_fails_closed():
    """Defect D3, the severe half: infrastructure failure must never grant a context.

    Deliberately no `migrated_db` fixture: this points the servicer at a dead port so the
    connection cannot succeed. The Plan 0 handler swallowed this in a bare `except
    Exception` and returned a context anyway, which is "database unreachable therefore
    grant access". It must surface as UNAVAILABLE and give the caller nothing.
    """
    server, port = await _start_server("postgresql+asyncpg://postgres:postgres@127.0.0.1:1/nope")
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await stub.GetUserContext(
                    identity_pb2.GetUserContextRequest(user_id=str(uuid.uuid4())),
                    metadata=(("org-id", str(uuid.uuid4())),),
                )
            assert exc.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        await server.stop(None)
