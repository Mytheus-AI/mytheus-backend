"""feat(core-api): agent_suggestions confidence, rationale, citations (QuestionAgent, P9)

Plan 7-9 section 3.10; mytheus-docs `_canon/02-data-model.md` (the `agent_suggestions` row) and
ADR-0034. Three nullable columns on the partitioned parent, which Postgres propagates to every
partition, existing and future:

- `confidence real` (CHECK 0..1): the band is applied at read time against the tenant's
  `agent_confidence_overrides` (ADR-0034 D4), like decision candidates. A NULL never surfaces.
- `rationale text`: one line on why the question matters to its owner.
- `citations jsonb` (default `[]`): the grounding passages' citations (ADR-0033 D3 kinds).

Plus `(org_id, user_id, meeting_id, status)` on every partition, which is what
`GET /api/v1/suggestions?meeting_id=` filters on; `scripts/ensure_partitions.py` adds it to
partitions created later. Indexes stay per partition, as in `e7a3c9d14b52`.

Revision ID: a9c4e2f7b813
Revises: e7a3c9d14b52
Create Date: 2026-11-09
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "a9c4e2f7b813"
down_revision: Union[str, None] = "e7a3c9d14b52"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_{p}_org_user_meeting_status "
    "ON {p} (org_id, user_id, meeting_id, status)"
)
_PARTITIONS = """
SELECT c.relname FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'agent_suggestions' ORDER BY c.relname
"""


def upgrade() -> None:
    op.execute("""
        ALTER TABLE agent_suggestions
          ADD COLUMN confidence real,
          ADD COLUMN rationale  text,
          ADD COLUMN citations  jsonb NOT NULL DEFAULT '[]'::jsonb,
          ADD CONSTRAINT ck_agent_suggestions_confidence_range
            CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1);
        """)
    for (name,) in op.get_bind().exec_driver_sql(_PARTITIONS).fetchall():
        op.execute(_INDEX.format(p=name) + ";")


def downgrade() -> None:
    for (name,) in op.get_bind().exec_driver_sql(_PARTITIONS).fetchall():
        op.execute(f"DROP INDEX IF EXISTS ix_{name}_org_user_meeting_status;")
    op.execute("""
        ALTER TABLE agent_suggestions
          DROP CONSTRAINT IF EXISTS ck_agent_suggestions_confidence_range,
          DROP COLUMN IF EXISTS citations,
          DROP COLUMN IF EXISTS rationale,
          DROP COLUMN IF EXISTS confidence;
        """)
