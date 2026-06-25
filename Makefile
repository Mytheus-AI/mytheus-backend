.PHONY: install dev migrate test lint typecheck

install:
	uv sync

dev:
	docker compose up -d
	uv run alembic upgrade head

migrate:
	uv run alembic revision --autogenerate -m "$(msg)"
	uv run alembic upgrade head

test:
	uv run pytest

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

# Generate gRPC stubs from proto/
proto:
	uv run python -m grpc_tools.protoc \
		-I proto \
		--python_out=. \
		--grpc_python_out=. \
		proto/identity.proto
