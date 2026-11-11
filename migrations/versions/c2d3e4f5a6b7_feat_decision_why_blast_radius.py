"""feat(core-api): decisions that know why, blast radius and settle threads (Beyond, Layer 1)

All tables are tenant-scoped (`org_id`, RLS `tenant_isolation` FORCE, explicit grants) and
written only by core-api.

- `decision_assumptions`: the explicit reasons behind a confirmed decision, as watchable items.
  `kind` (price, date, metric, dependency, person, other), the plain `statement`, the verbatim
  `quote`, who stated it, `source` (`agent`: extracted by `decision_assumptions@v1`; `owner`:
  added by the decision's owner), `status` (`watching` | `broken` | `removed`), and the watch
  fields the schedule checks (`watch_date`, `watch_value` + `watch_comparator` + `watch_unit`,
  `watch_jira_key`, `watch_deal_id`). An agent row is unique per decision and statement.
- `assumption_evidence`: one row per time an assumption broke: the new evidence (a later
  meeting's words, a later decision, a Jira issue, a HubSpot deal), its source, and what the
  owner did (`open` -> `reconfirmed` | `reopened` | `revisit_booked`). `dedupe_key` keeps a
  replayed check from breaking it twice.
- `decision_why_state`: per decision, when the agent's `decision_why` record was applied
  (`read_at`) and whether the owner reopened it (`reopened_*`). Kept off `decisions`, whose
  published status enum is frozen.
- `decision_ripple_links`: the blast radius links the agent found (goals, actions, customer
  actions) and HubSpot deals, with the ring (1..3), the effect and the why.
- `decision_clashes`: the existing ProactiveAgent `conflict` alert, kept once per pair of
  decisions, with whether the two owners are on different teams.
- `decision_settlements` + `settlement_messages`: a settle thread between the two owners of a
  cross-team clash (keep both, one supersedes the other, or meet).
- `watch_source_status`: per tenant, whether Jira, Linear and HubSpot answered the last watch
  run (`connected` | `not_connected`), so the screens say "not connected" honestly.
- `decision_reversals.kind`: `superseded` (default: every existing row) | `reopened` (the
  decision behind the task was reopened because a reason for it no longer holds).

Revision ID: c2d3e4f5a6b7
Revises: d3e4f5a6b7c8
Create Date: 2026-11-11
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "c2d3e4f5a6b7"
down_revision: str | None = "d3e4f5a6b7c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_TABLES = (
    "decision_assumptions",
    "assumption_evidence",
    "decision_why_state",
    "decision_ripple_links",
    "decision_clashes",
    "decision_settlements",
    "settlement_messages",
    "watch_source_status",
)


def _rls(table: str, comment: str) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    op.execute("""
        CREATE TABLE decision_assumptions (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id      uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          kind             text        NOT NULL,
          statement        text        NOT NULL,
          quote            text,
          stated_by        uuid        REFERENCES users(id) ON DELETE SET NULL,
          source           text        NOT NULL,
          status           text        NOT NULL DEFAULT 'watching',
          confidence       real,
          watch_date       date,
          watch_value      numeric,
          watch_comparator text,
          watch_unit       text,
          watch_jira_key   text,
          watch_deal_id    text,
          reconfirmed_at   timestamptz,
          reconfirm_note   text,
          last_checked_at  timestamptz,
          created_by       uuid        REFERENCES users(id) ON DELETE SET NULL,
          created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
          updated_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_assumptions PRIMARY KEY (id),
          CONSTRAINT ck_decision_assumptions_kind
            CHECK (kind IN ('price', 'date', 'metric', 'dependency', 'person', 'other')),
          CONSTRAINT ck_decision_assumptions_statement
            CHECK (char_length(statement) BETWEEN 1 AND 300),
          CONSTRAINT ck_decision_assumptions_quote
            CHECK (quote IS NULL OR char_length(quote) BETWEEN 1 AND 600),
          CONSTRAINT ck_decision_assumptions_source CHECK (source IN ('agent', 'owner')),
          CONSTRAINT ck_decision_assumptions_status
            CHECK (status IN ('watching', 'broken', 'removed')),
          CONSTRAINT ck_decision_assumptions_comparator
            CHECK (watch_comparator IS NULL
                   OR watch_comparator IN ('lt', 'lte', 'gt', 'gte', 'eq')),
          CONSTRAINT ck_decision_assumptions_unit
            CHECK (watch_unit IS NULL OR char_length(watch_unit) BETWEEN 1 AND 20),
          CONSTRAINT ck_decision_assumptions_jira
            CHECK (watch_jira_key IS NULL OR watch_jira_key ~ '^[A-Z][A-Z0-9_]{0,19}-[0-9]{1,9}$'),
          CONSTRAINT ck_decision_assumptions_deal
            CHECK (watch_deal_id IS NULL OR watch_deal_id ~ '^[0-9]{1,30}$'),
          CONSTRAINT ck_decision_assumptions_note
            CHECK (reconfirm_note IS NULL OR char_length(reconfirm_note) BETWEEN 1 AND 500)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_decision_assumptions_agent_statement "
        "ON decision_assumptions (decision_id, md5(lower(statement))) WHERE source = 'agent';"
    )
    op.execute(
        "CREATE INDEX ix_decision_assumptions_decision "
        "ON decision_assumptions (decision_id, created_at);"
    )
    op.execute(
        "CREATE INDEX ix_decision_assumptions_watching "
        "ON decision_assumptions (org_id, created_at DESC) WHERE status = 'watching';"
    )
    _rls(
        "decision_assumptions",
        "The explicit reasons behind a confirmed decision, watched for evidence that breaks "
        "them. Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE assumption_evidence (
          id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id             uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          assumption_id      uuid        NOT NULL
                                         REFERENCES decision_assumptions(id) ON DELETE CASCADE,
          decision_id        uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          source             text        NOT NULL,
          quote              text,
          speaker_id         uuid        REFERENCES users(id) ON DELETE SET NULL,
          source_meeting_id  uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          source_decision_id uuid        REFERENCES decisions(id) ON DELETE SET NULL,
          source_ref         text,
          source_url         text,
          rationale          text        NOT NULL,
          confidence         real        NOT NULL,
          status             text        NOT NULL DEFAULT 'open',
          dedupe_key         text        NOT NULL,
          resolved_by        uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at        timestamptz,
          resolution_note    text,
          revisit_meeting_id uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          created_at         timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_assumption_evidence PRIMARY KEY (id),
          CONSTRAINT uq_assumption_evidence_dedupe UNIQUE (assumption_id, dedupe_key),
          CONSTRAINT ck_assumption_evidence_source
            CHECK (source IN ('meeting', 'decision', 'jira', 'linear', 'hubspot')),
          CONSTRAINT ck_assumption_evidence_status
            CHECK (status IN ('open', 'reconfirmed', 'reopened', 'revisit_booked')),
          CONSTRAINT ck_assumption_evidence_resolved
            CHECK ((status = 'open') = (resolved_at IS NULL)),
          CONSTRAINT ck_assumption_evidence_quote
            CHECK (quote IS NULL OR char_length(quote) BETWEEN 1 AND 600),
          CONSTRAINT ck_assumption_evidence_rationale
            CHECK (char_length(rationale) BETWEEN 1 AND 300),
          CONSTRAINT ck_assumption_evidence_confidence CHECK (confidence BETWEEN 0 AND 1)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_assumption_evidence_one_open "
        "ON assumption_evidence (assumption_id) WHERE status = 'open';"
    )
    op.execute(
        "CREATE INDEX ix_assumption_evidence_decision "
        "ON assumption_evidence (decision_id, created_at DESC);"
    )
    _rls(
        "assumption_evidence",
        "Evidence that broke a decision assumption, and what its owner did about it. Written "
        "only by core-api.",
    )

    op.execute("""
        CREATE TABLE decision_why_state (
          decision_id  uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          read_at      timestamptz,
          reopened_at  timestamptz,
          reopened_by  uuid        REFERENCES users(id) ON DELETE SET NULL,
          reopen_note  text,
          updated_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_why_state PRIMARY KEY (decision_id),
          CONSTRAINT ck_decision_why_state_note
            CHECK (reopen_note IS NULL OR char_length(reopen_note) BETWEEN 1 AND 500)
        );
        """)
    _rls(
        "decision_why_state",
        "Per decision: when the agent read its reasons, and whether its owner reopened it. "
        "Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE decision_ripple_links (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          target_kind text        NOT NULL,
          target_id   text        NOT NULL,
          label       text        NOT NULL,
          effect      text        NOT NULL,
          why         text,
          ring        smallint    NOT NULL,
          confidence  real        NOT NULL DEFAULT 1,
          source      text        NOT NULL,
          detail      jsonb       NOT NULL DEFAULT '{}'::jsonb,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_ripple_links PRIMARY KEY (id),
          CONSTRAINT uq_decision_ripple_links_target UNIQUE (decision_id, target_kind, target_id),
          CONSTRAINT ck_decision_ripple_links_kind
            CHECK (target_kind IN ('action', 'goal', 'customer_action', 'deal')),
          CONSTRAINT ck_decision_ripple_links_effect
            CHECK (effect IN ('changes', 'blocks', 'unblocks', 'advances', 'risks', 'commits')),
          CONSTRAINT ck_decision_ripple_links_ring CHECK (ring BETWEEN 1 AND 3),
          CONSTRAINT ck_decision_ripple_links_source
            CHECK (source IN ('agent', 'graph', 'hubspot')),
          CONSTRAINT ck_decision_ripple_links_label CHECK (char_length(label) BETWEEN 1 AND 300),
          CONSTRAINT ck_decision_ripple_links_why CHECK (why IS NULL OR char_length(why) <= 300)
        );
        """)
    _rls(
        "decision_ripple_links",
        "What a confirmed decision touches (its blast radius): actions, goals, customer "
        "actions and deals, by ring. Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE decision_clashes (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id       uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          other_decision_id uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          confidence        real        NOT NULL,
          rationale         text        NOT NULL DEFAULT '',
          cross_team        boolean     NOT NULL,
          created_at        timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_clashes PRIMARY KEY (id),
          CONSTRAINT uq_decision_clashes_pair UNIQUE (decision_id, other_decision_id),
          CONSTRAINT ck_decision_clashes_distinct CHECK (decision_id <> other_decision_id),
          CONSTRAINT ck_decision_clashes_confidence CHECK (confidence BETWEEN 0 AND 1)
        );
        """)
    op.execute("CREATE INDEX ix_decision_clashes_other ON decision_clashes (other_decision_id);")
    _rls(
        "decision_clashes",
        "A confirmed decision that may contradict an earlier one (ProactiveAgent conflict). "
        "Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE decision_settlements (
          id                      uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id                  uuid        NOT NULL
                                              REFERENCES organizations(id) ON DELETE RESTRICT,
          clash_id                uuid        NOT NULL
                                              REFERENCES decision_clashes(id) ON DELETE CASCADE,
          decision_a_id           uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          decision_b_id           uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          owner_a                 uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          owner_b                 uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          status                  text        NOT NULL DEFAULT 'open',
          a_shared                boolean     NOT NULL DEFAULT false,
          b_shared                boolean     NOT NULL DEFAULT false,
          proposal_winner_id      uuid        REFERENCES decisions(id) ON DELETE SET NULL,
          proposal_by             uuid        REFERENCES users(id) ON DELETE SET NULL,
          proposal_at             timestamptz,
          winner_decision_id      uuid        REFERENCES decisions(id) ON DELETE SET NULL,
          meeting_id              uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          resolution_note         text,
          resolved_by             uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at             timestamptz,
          created_at              timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_settlements PRIMARY KEY (id),
          CONSTRAINT uq_decision_settlements_clash UNIQUE (clash_id),
          CONSTRAINT ck_decision_settlements_status
            CHECK (status IN ('open', 'kept_both', 'superseded', 'meeting_booked')),
          CONSTRAINT ck_decision_settlements_resolved
            CHECK ((status = 'open') = (resolved_at IS NULL)),
          CONSTRAINT ck_decision_settlements_note
            CHECK (resolution_note IS NULL OR char_length(resolution_note) BETWEEN 1 AND 500)
        );
        """)
    op.execute(
        "CREATE INDEX ix_decision_settlements_owner_a ON decision_settlements (owner_a, status);"
    )
    op.execute(
        "CREATE INDEX ix_decision_settlements_owner_b ON decision_settlements (owner_b, status);"
    )
    _rls(
        "decision_settlements",
        "A settle thread between the owners of two clashing decisions from different teams. "
        "Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE settlement_messages (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          settlement_id uuid        NOT NULL
                                    REFERENCES decision_settlements(id) ON DELETE CASCADE,
          author_id     uuid        REFERENCES users(id) ON DELETE SET NULL,
          body          text        NOT NULL,
          created_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_settlement_messages PRIMARY KEY (id),
          CONSTRAINT ck_settlement_messages_body CHECK (char_length(body) BETWEEN 1 AND 1000)
        );
        """)
    op.execute(
        "CREATE INDEX ix_settlement_messages_thread "
        "ON settlement_messages (settlement_id, created_at);"
    )
    _rls(
        "settlement_messages",
        "Messages between the two owners in a settle thread. Written only by core-api.",
    )

    op.execute("""
        CREATE TABLE watch_source_status (
          org_id     uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          jira       text        NOT NULL DEFAULT 'unknown',
          linear     text        NOT NULL DEFAULT 'unknown',
          hubspot    text        NOT NULL DEFAULT 'unknown',
          checked_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_watch_source_status PRIMARY KEY (org_id),
          CONSTRAINT ck_watch_source_status_values CHECK (
            jira IN ('connected', 'not_connected', 'unknown')
            AND linear IN ('connected', 'not_connected', 'unknown')
            AND hubspot IN ('connected', 'not_connected', 'unknown'))
        );
        """)
    _rls(
        "watch_source_status",
        "Whether Jira, Linear and HubSpot answered the tenant''s last assumption watch run. "
        "Written only by core-api.",
    )

    op.execute("""
        ALTER TABLE decision_reversals
          ADD COLUMN kind text NOT NULL DEFAULT 'superseded',
          ADD CONSTRAINT ck_decision_reversals_kind CHECK (kind IN ('superseded', 'reopened'));
        """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE decision_reversals DROP CONSTRAINT IF EXISTS ck_decision_reversals_kind;"
    )
    op.execute("ALTER TABLE decision_reversals DROP COLUMN IF EXISTS kind;")
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table};")
