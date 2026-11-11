# syntax=docker/dockerfile:1
#
# Multi-stage build for the mytheus-backend modular monolith (16-deployment-and-infrastructure.md
# §3.1). One image runs every process; the command selects the role:
#   web      -> uvicorn services.<svc>.main:app   (default CMD below)
#   relay    -> python -m services.relay.main
#   workflow -> python -m services.workflow_service.worker
# Builder resolves wheels with uv; runtime is slim, non-root, tini as PID 1.

FROM python:3.12-slim AS builder
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app

# Install dependencies first (cached until the lockfile changes), then the project itself.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --python /usr/local/bin/python
# identity_pb2 / identity_pb2_grpc are generated, never committed (`make proto`). Without them
# the image cannot run `python -m services.identity_tenant_service.grpc_server`, the
# get_user_context server the agent runtime calls. grpcio-tools is a runtime dependency, so the
# generator version is the locked one and matches the protobuf runtime.
RUN /app/.venv/bin/python -m grpc_tools.protoc -I proto \
    --python_out=. --grpc_python_out=. proto/identity.proto


FROM python:3.12-slim AS runtime
# ffmpeg: meeting-service cuts a shared clip's own video out of a meeting recording
# (`ffmpeg -c copy`, no re-encode), so a public clip link never exposes the whole recording.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER appuser
EXPOSE 8000

# tini reaps zombies and forwards signals (clean SIGTERM on rollout/scale-down).
ENTRYPOINT ["tini", "--"]
# Default role: the core-api web process. Override `command:` for relay / workflow workers.
CMD ["uvicorn", "services.core_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
