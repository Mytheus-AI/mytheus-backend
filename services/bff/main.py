"""Backend-for-frontend service.

Screen-shaped read aggregation. No domain ownership; no data written here.
The only caller of /bff/* is mytheus-ui.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry

app = FastAPI(title="bff", version="1.0.0")
setup_telemetry(app, service_name="bff")
add_exception_handlers(app)

# TODO: add screen-shaped read routers here as features are implemented
