.PHONY: install dev roles migrate upgrade test test-fast lint typecheck fix proto \
        run-relay run-identity-grpc ci seed

ALEMBIC = uv run alembic -c migrations/alembic.ini

install:
	uv sync

dev:
	docker compose up -d
	$(ALEMBIC) upgrade head
	$(MAKE) roles

# Give the three least-privilege roles a login password. The migration creates them without
# one on purpose (a password in a migration is a password in git history), so the stack is
# not usable until this has run once.
roles:
	uv run python -m scripts.bootstrap_roles

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
ci: proto lint typecheck openapi-check test

# Emit an OpenAPI 3.1 document per HTTP service, from the running app. Run this after touching
# a route or a Pydantic model, and commit the result: `openapi-check` fails the build when the
# committed file and the code disagree, which is what stops the spec from quietly going stale
# the way the hand-written ones in mytheus-docs did.
openapi:
	uv run python -m scripts.emit_openapi

openapi-check:
	uv run python -m scripts.emit_openapi --check

# The house rules (13 of them) over the emitted documents. Node, so it is not in `ci`: a
# missing npx would fail the standing gate for a developer who has no Node installed, and the
# authoritative run is the CI job. --fail-severity=error so the built-in spectral:oas style
# warnings (missing descriptions, tags, servers) report without blocking.
openapi-lint:
	npx --yes @stoplight/spectral-cli@6 lint "design/openapi/*.yaml" 		--ruleset design/.spectral.yaml --fail-severity=error

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

# Seed 2 orgs x 10 users x 5 meetings (idempotent - safe to re-run). Runs as `mytheus_admin`
# (ADMIN_DATABASE_URL), the operator seam - never the request-path role.
seed:
	uv run python -m services.identity_tenant_service.seed.seed

# Generate gRPC stubs from proto/
proto:
	uv run python -m grpc_tools.protoc \
		-I proto \
		--python_out=. \
		--grpc_python_out=. \
		proto/identity.proto
