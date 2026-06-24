-- Canonical RLS policy template
-- ===============================
-- Apply to every tenant-scoped table. `plans` is the deliberate exception (global, no org_id).
-- The GUC MUST be set transaction-local (true = 3rd arg) before any tenant-scoped query.
-- See libs/shared/tenancy.py:set_rls().
--
-- Usage: replace <table_name> with the actual table name.

/*
ALTER TABLE <table_name> ENABLE ROW LEVEL SECURITY;
ALTER TABLE <table_name> FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON <table_name>
    USING (org_id = current_setting('app.current_org')::uuid)
    WITH CHECK (org_id = current_setting('app.current_org')::uuid);
*/

-- outbox table (shared by all services via libs/shared/outbox.py)
CREATE TABLE IF NOT EXISTS outbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          uuid NOT NULL REFERENCES organizations(id),
    topic           text NOT NULL,
    idempotency_key text NOT NULL,
    payload         jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    UNIQUE (topic, idempotency_key)
);

ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON outbox
    USING (org_id = current_setting('app.current_org')::uuid)
    WITH CHECK (org_id = current_setting('app.current_org')::uuid);

-- inbox table (shared by all services via libs/shared/inbox.py)
CREATE TABLE IF NOT EXISTS inbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          uuid NOT NULL,
    topic           text NOT NULL,
    idempotency_key text NOT NULL,
    consumed_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, idempotency_key)
);

ALTER TABLE inbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON inbox
    USING (org_id = current_setting('app.current_org')::uuid)
    WITH CHECK (org_id = current_setting('app.current_org')::uuid);
