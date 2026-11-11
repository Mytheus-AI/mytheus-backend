"""feat(core-api): decisions that follow you (challenges, reopen requests, check dismissals)

"Beyond the meeting" program, Layer 2. Text written anywhere (a Slack message, a Google Doc, a
Gmail draft, a Jira comment, a Claude draft) is checked against the org's confirmed decisions by
rag-service (`POST /internal/decisions/check`); core-api records only what has to outlive the
check. Every table is written only by core-api, tenant-scoped, RLS FORCE, `grant_tenant_table`.

- `decision_challenges`: one place a confirmed decision was contradicted or reopened, shown on
  the decision page as "Challenged in N places" and to its owner in the Inbox. Recorded for
  published text (a Slack channel message) and when a person reopens a decision from their own
  draft; never for a draft that was only checked. `quote` is the conflicting span (at most 500
  characters), `quote_hash` the SHA-256 of its normalised form: UNIQUE (decision_id, quote_hash)
  makes the same sentence one challenge, however often it is re-checked or the Slack event is
  redelivered. `status` open -> dismissed | reopened. `notified_at` marks the challenge whose
  owner notification went out (at most one per decision per 24 hours).
- `decision_reopen_requests`: "Reopen it". The decision's owner sees it and accepts (a revisit
  meeting or a follow-up task, linked here) or declines; the requester can withdraw. One pending
  request per (decision, requester): a partial unique index.
- `decision_check_dismissals`: "Dismiss", remembered so the same sentence is not flagged to the
  same person again. Only the hash of the normalised quote is kept, never the text.

Plus a GIN full-text index on confirmed decisions (`content` + `rationale`), the lexical arm of
the check's hybrid retrieval (`POST /internal/retrieval/decisions/search`).

Revision ID: f8a9b0c1d2e3
Revises: e7f8a9b0c1d2
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "f8a9b0c1d2e3"
down_revision: Union[str, None] = "e7f8a9b0c1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_TABLES = ("decision_reopen_requests", "decision_challenges", "decision_check_dismissals")
_SOURCES = "('slack', 'extension', 'mcp', 'app')"
FTS_EXPRESSION = "to_tsvector('english', content || ' ' || coalesce(rationale, ''))"


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
    # --- decision_reopen_requests (first: challenges point at it) --------------------------------
    op.execute(f"""
        CREATE TABLE decision_reopen_requests (
          id                  uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id              uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id         uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          requested_by        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          owner_user_id       uuid        REFERENCES users(id) ON DELETE SET NULL,
          reason              text,
          quote               text,
          source              text        NOT NULL DEFAULT 'app',
          place_label         text,
          link                text,
          status              text        NOT NULL DEFAULT 'pending',
          outcome             text,
          follow_up_action_id uuid        REFERENCES action_items(id) ON DELETE SET NULL,
          revisit_meeting_id  uuid        REFERENCES meetings(id) ON DELETE SET NULL,
          response_note       text,
          resolved_by         uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at         timestamptz,
          created_at          timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_reopen_requests PRIMARY KEY (id),
          CONSTRAINT ck_decision_reopen_requests_status
            CHECK (status IN ('pending', 'accepted', 'declined', 'withdrawn')),
          CONSTRAINT ck_decision_reopen_requests_outcome
            CHECK (outcome IS NULL OR outcome IN ('revisit', 'follow_up')),
          CONSTRAINT ck_decision_reopen_requests_accepted
            CHECK ((status = 'accepted') = (outcome IS NOT NULL)),
          CONSTRAINT ck_decision_reopen_requests_resolved
            CHECK ((status = 'pending') = (resolved_at IS NULL)),
          CONSTRAINT ck_decision_reopen_requests_source CHECK (source IN {_SOURCES}),
          CONSTRAINT ck_decision_reopen_requests_reason
            CHECK (reason IS NULL OR char_length(reason) <= 1000),
          CONSTRAINT ck_decision_reopen_requests_quote
            CHECK (quote IS NULL OR char_length(quote) <= 500),
          CONSTRAINT ck_decision_reopen_requests_place
            CHECK (place_label IS NULL OR char_length(place_label) <= 120),
          CONSTRAINT ck_decision_reopen_requests_link
            CHECK (link IS NULL OR (char_length(link) <= 2000 AND link ~ '^https?://')),
          CONSTRAINT ck_decision_reopen_requests_note
            CHECK (response_note IS NULL OR char_length(response_note) <= 1000)
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_decision_reopen_requests_pending "
        "ON decision_reopen_requests (decision_id, requested_by) WHERE status = 'pending';"
    )
    op.execute(
        "CREATE INDEX ix_decision_reopen_requests_owner "
        "ON decision_reopen_requests (owner_user_id, status, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_decision_reopen_requests_requester "
        "ON decision_reopen_requests (requested_by, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_decision_reopen_requests_decision "
        "ON decision_reopen_requests (decision_id, created_at DESC);"
    )
    _rls(
        "decision_reopen_requests",
        "A request to reopen a confirmed decision, and its owner''s answer (a revisit meeting "
        "or a follow-up task, or declined). Written only by core-api.",
    )

    # --- decision_challenges ----------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE decision_challenges (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id       uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          source            text        NOT NULL,
          author_user_id    uuid        REFERENCES users(id) ON DELETE SET NULL,
          quote             text        NOT NULL,
          quote_hash        text        NOT NULL,
          why               text        NOT NULL DEFAULT '',
          relation          text        NOT NULL,
          confidence        real        NOT NULL,
          place_label       text,
          link              text,
          external_ref      text,
          status            text        NOT NULL DEFAULT 'open',
          reopen_request_id uuid        REFERENCES decision_reopen_requests(id) ON DELETE SET NULL,
          dismissed_by      uuid        REFERENCES users(id) ON DELETE SET NULL,
          dismissed_at      timestamptz,
          notified_at       timestamptz,
          created_at        timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_challenges PRIMARY KEY (id),
          CONSTRAINT uq_decision_challenges_quote UNIQUE (decision_id, quote_hash),
          CONSTRAINT ck_decision_challenges_source CHECK (source IN {_SOURCES}),
          CONSTRAINT ck_decision_challenges_relation
            CHECK (relation IN ('contradicts', 'reopens')),
          CONSTRAINT ck_decision_challenges_status
            CHECK (status IN ('open', 'dismissed', 'reopened')),
          CONSTRAINT ck_decision_challenges_dismissed
            CHECK ((status = 'dismissed') = (dismissed_at IS NOT NULL)),
          CONSTRAINT ck_decision_challenges_confidence CHECK (confidence BETWEEN 0 AND 1),
          CONSTRAINT ck_decision_challenges_quote CHECK (char_length(quote) BETWEEN 1 AND 500),
          CONSTRAINT ck_decision_challenges_why CHECK (char_length(why) <= 500),
          CONSTRAINT ck_decision_challenges_place
            CHECK (place_label IS NULL OR char_length(place_label) <= 120),
          CONSTRAINT ck_decision_challenges_link
            CHECK (link IS NULL OR (char_length(link) <= 2000 AND link ~ '^https?://')),
          CONSTRAINT ck_decision_challenges_external_ref
            CHECK (external_ref IS NULL OR char_length(external_ref) <= 200)
        );
        """)
    op.execute(
        "CREATE INDEX ix_decision_challenges_decision "
        "ON decision_challenges (decision_id, created_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_decision_challenges_author "
        "ON decision_challenges (author_user_id, created_at DESC);"
    )
    _rls(
        "decision_challenges",
        "A place a confirmed decision was contradicted or reopened (a Slack message, or a "
        "draft its author chose to reopen the decision from). Written only by core-api.",
    )

    # --- decision_check_dismissals ----------------------------------------------------------------
    op.execute("""
        CREATE TABLE decision_check_dismissals (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          decision_id uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          quote_hash  text        NOT NULL,
          source      text        NOT NULL DEFAULT 'app',
          created_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_check_dismissals PRIMARY KEY (id),
          CONSTRAINT uq_decision_check_dismissals UNIQUE (user_id, decision_id, quote_hash),
          CONSTRAINT ck_decision_check_dismissals_hash CHECK (char_length(quote_hash) = 64)
        );
        """)
    _rls(
        "decision_check_dismissals",
        "A sentence a person dismissed as not conflicting with a decision: only the SHA-256 "
        "of the normalised quote, never the text. Written only by core-api.",
    )

    # --- the lexical arm ----------------------------------------------------------------------
    op.execute(
        f"CREATE INDEX ix_decisions_fts ON decisions USING gin ({FTS_EXPRESSION}) "
        "WHERE status = 'confirmed' AND deleted_at IS NULL;"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_decisions_fts;")
    op.execute("DROP TABLE IF EXISTS decision_check_dismissals;")
    op.execute("DROP TABLE IF EXISTS decision_challenges;")
    op.execute("DROP TABLE IF EXISTS decision_reopen_requests;")
