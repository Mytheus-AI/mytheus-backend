"""bff — backend-for-frontend: screen-shaped read aggregation. No domain ownership; the only
caller of /bff/* is mytheus-ui.
"""

from libs.shared.app_factory import create_app

app = create_app("bff")

# TODO: add screen-shaped read routers as features are implemented
