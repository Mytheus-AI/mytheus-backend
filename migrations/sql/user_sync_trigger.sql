-- User sync trigger: auth.users → public.users
-- ============================================
-- Replaces Clerk's webhook user-sync with a Postgres trigger that fires on
-- every INSERT/UPDATE/DELETE on auth.users (the Supabase-managed table).
-- This is the PRIMARY sync mechanism (immediate, transactional, reliable).
--
-- The Supabase DB webhook (POST /supabase/webhook) is a secondary notification
-- path for cross-service concerns (e.g. triggering the org provisioning outbox).
-- The hourly Temporal reconciliation job (workflow-service) remains as the
-- dropped-event safety net, now calling the Supabase Admin API instead of Clerk API.
--
-- public.users.id == auth.users.id (Supabase user UUID). No separate
-- supabase_user_id column needed — the UUID IS the identity link.

-- Ensure the public.users table has a compatible primary key.
-- (Full DDL is in the Alembic migration; this file handles trigger only.)

CREATE OR REPLACE FUNCTION public.handle_auth_user_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.users (id, email, created_at, updated_at)
        VALUES (NEW.id, NEW.email, NEW.created_at, NOW())
        ON CONFLICT (id) DO UPDATE
            SET email      = EXCLUDED.email,
                updated_at = NOW();

    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE public.users
        SET email      = NEW.email,
            updated_at = NOW()
        WHERE id = NEW.id;

    ELSIF TG_OP = 'DELETE' THEN
        -- Soft-delete; hard purge is done by the RTBF Temporal workflow.
        UPDATE public.users
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE id = OLD.id;
    END IF;

    RETURN NEW;
END;
$$;

-- Drop existing trigger before re-creating (idempotent migration).
DROP TRIGGER IF EXISTS on_auth_user_change ON auth.users;

CREATE TRIGGER on_auth_user_change
    AFTER INSERT OR UPDATE OR DELETE ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION public.handle_auth_user_change();
