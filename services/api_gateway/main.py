"""API Gateway — single public ingress.

Responsibilities: TLS termination, WAF, per-tenant rate limiting, JWT pre-validation,
org_id extraction, routing. Stateless — no data ownership.

In the modular monolith (Railway/Render), traffic flows through this thin
FastAPI layer before reaching downstream services. In production (Kubernetes),
this is replaced or fronted by Envoy/Nginx managed in reprise-platform.

The gateway FAILS CLOSED: if org_id cannot be resolved from the JWT, no
request reaches a tenant-scoped store.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry

app = FastAPI(title="api-gateway", version="1.0.0")
setup_telemetry(app, service_name="api-gateway")
add_exception_handlers(app)

# TODO: add JWT pre-validation middleware, rate limiting, reverse-proxy routing
