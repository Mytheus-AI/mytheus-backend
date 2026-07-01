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
    uv sync --frozen --no-dev


FROM python:3.12-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
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
