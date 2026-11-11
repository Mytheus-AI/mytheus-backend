"""feat: push_devices (Expo push for the phone) and meetings.source 'in_person'

Polish program, wave 3 follow-ups ("Mobile backend gaps").

`push_devices`: one row per (tenant, Expo push token), owned by notification-service. The phone
registers its token for the signed-in person on sign-in (`POST /notifications/push-devices`) and
removes it on sign-out (`DELETE /notifications/push-devices/{id}`). When `PUSH_PROVIDER=expo`,
notification-service's consumer mirrors each NEW in-app notification to that person's devices
through the Expo Push API; a token Expo reports as `DeviceNotRegistered` is deleted. A token
belongs to one person in a tenant at a time (`uq_push_devices_org_token`): registering it again
moves it to whoever is signed in now.

`meetings.source` gains `in_person`: a meeting recorded on the phone in a room with no call
(core-api `POST /api/v1/capture/sessions`). Like the captured calls, it has no Mytheus LiveKit
room, so meeting-service opens none.

Revision ID: a9b0c1d2e3f4
Revises: e1f2a3b4c5d6
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "a9b0c1d2e3f4"
down_revision: Union[str, None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE push_devices (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id       uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          token         text        NOT NULL,
          platform      text        NOT NULL,
          created_at    timestamptz NOT NULL DEFAULT now(),
          last_seen_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_push_devices PRIMARY KEY (id),
          CONSTRAINT uq_push_devices_org_token UNIQUE (org_id, token),
          CONSTRAINT ck_push_devices_platform CHECK (platform IN ('ios', 'android')),
          CONSTRAINT ck_push_devices_token CHECK (char_length(token) BETWEEN 1 AND 255)
        );
        """)
    op.execute("CREATE INDEX ix_push_devices_org_id ON push_devices (org_id);")
    op.execute("CREATE INDEX ix_push_devices_user ON push_devices (user_id);")
    op.execute("ALTER TABLE push_devices ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE push_devices FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON push_devices "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "push_devices")
    op.execute(
        "COMMENT ON TABLE push_devices IS 'Expo push tokens of signed-in phones, one person per "
        "token per tenant. Written only by notification-service.';"
    )
    op.drop_constraint("ck_meetings_source", "meetings", type_="check")
    op.create_check_constraint(
        "ck_meetings_source",
        "meetings",
        "source IN ('mytheus', 'meet', 'teams', 'zoom', 'in_person')",
    )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("UPDATE meetings SET source = 'mytheus' WHERE source = 'in_person';")
    op.drop_constraint("ck_meetings_source", "meetings", type_="check")
    op.create_check_constraint(
        "ck_meetings_source", "meetings", "source IN ('mytheus', 'meet', 'teams', 'zoom')"
    )
    op.execute("DROP TABLE IF EXISTS push_devices CASCADE;")
