"""feat(meeting-service): meeting_rooms.livekit_metadata_synced_at

Task 4 fix round 1: the durable "has this room's LiveKit metadata been set" marker that lets
`transport.ensure_room` be called OUT OF BAND from both the token-mint route and the
`meeting-events` consumer, instead of synchronously inside their write transactions (see the
task report for the full incident: a LiveKit blip could crash-loop the `meeting-events`
consumer and blocked its partition for every tenant, and two unbounded-timeout LiveKit calls
were running inside the token-mint route's open Postgres transaction, undercutting the
`_BIND_STATEMENT_TIMEOUT` SLO protection two lines above it).

Nullable, defaults to unset (NULL = "still needs a sync attempt"). No RLS change needed: it is
just another column on a table `tenant_isolation` already covers, read and written by the same
`mytheus_app` role, inside the SAME tenant-scoped session/transaction that already exists at
both call sites - no cross-tenant background sweep, no new role, no new permissive policy.
`meeting_rooms` is not yet represented in `_canon/02-data-model.md` (a pre-existing gap: Task 2's
migration that created the table did not add it there either); not fixed in this fix round,
which is scoped to the reviewer's three flagged issues.

Revision ID: f3a7c9e0b21d
Revises: 25e7cede5672
Create Date: 2026-08-06 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f3a7c9e0b21d"
down_revision: Union[str, None] = "25e7cede5672"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "meeting_rooms",
        sa.Column("livekit_metadata_synced_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("meeting_rooms", "livekit_metadata_synced_at")
