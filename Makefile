.PHONY: install dev migrate upgrade test test-fast lint typecheck fix proto \
        run-relay run-identity-grpc ci

ALEMBIC = uv run alembic -c migrations/alembic.ini

install:
	uv sync

dev:
	docker compose up -d
	$(ALEMBIC) upgrade head

migrate:
	$(ALEMBIC) revision --autogenerate -m "$(msg)"
	$(ALEMBIC) upgrade head

upgrade:
	$(ALEMBIC) upgrade head

test:
	uv run pytest

# lint + typecheck + test (the standing gate; alias used across repos).
# `proto` runs first because identity_pb2*.py are gitignored ("regenerate in CI"): without
# it, pytest fails at COLLECTION on a clean clone and the gate never reaches a real test.
ci: proto lint typecheck test

test-fast:
	uv run pytest -m "not slow"

lint:
	uv run ruff check . && uv run black --check .

typecheck:
	uv run mypy .

fix:
	uv run ruff check --fix . && uv run black .

# Run a single service (modular-monolith dev mode)
run-gateway:
	uv run uvicorn services.api_gateway.main:app --reload --port 8080

run-core-api:
	uv run uvicorn services.core_api.main:app --reload --port 8002

run-identity:
	uv run uvicorn services.identity_tenant_service.main:app --reload --port 8001

run-bff:
	uv run uvicorn services.bff.main:app --reload --port 8000

run-meeting:
	uv run uvicorn services.meeting_service.main:app --reload --port 8003

run-billing:
	uv run uvicorn services.billing_metering_service.main:app --reload --port 8005

run-workers:
	uv run python -m services.workflow_service.worker

# Transactional-outbox relay (worker process in the same deployable)
run-relay:
	uv run python -m services.relay.main

# identity-tenant-service gRPC server (GetUserContext)
run-identity-grpc:
	uv run python -m services.identity_tenant_service.grpc_server

# Generate gRPC stubs from proto/
proto:
	uv run python -m grpc_tools.protoc \
		-I proto \
		--python_out=. \
		--grpc_python_out=. \
		proto/identity.proto
