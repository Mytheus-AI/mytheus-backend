-- Supabase custom JWT claims hook
-- ================================
-- Embeds org_id into every Supabase JWT as a custom claim so the existing
-- `org_id`-from-JWT pattern (used by api-gateway, every service's authz middleware,
-- and RLS app.current_org) works identically with Supabase Auth as it did with Clerk.
--
-- Setup (once per project):
--   1. Run this SQL in your Supabase project (SQL Editor or migration).
--   2. In Supabase Dashboard: Auth → Hooks → "Customize Access Token (JWT) Claims"
--      → select the function public.custom_access_token_hook
--
-- The hook runs on every token issue and refresh; it reads the user's org_id
-- from public.users (where id = auth.users.id = the Supabase user UUID) and
-- adds it to the JWT claims as the string "org_id".
-- If the user has no org yet (e.g. mid-onboarding), org_id is omitted and the
-- service tier rejects with 401 ("Token missing org_id claim").

CREATE OR REPLACE FUNCTION public.custom_access_token_hook(event jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    claims  jsonb;
    user_org_id uuid;
BEGIN
    claims := event -> 'claims';

    SELECT org_id
    INTO user_org_id
    FROM public.users
    WHERE id = (event ->> 'user_id')::uuid;

    IF user_org_id IS NOT NULL THEN
        claims := jsonb_set(claims, '{org_id}', to_jsonb(user_org_id::text));
    END IF;

    RETURN jsonb_set(event, '{claims}', claims);
END;
$$;

-- Grant the supabase_auth_admin role (the internal hook executor) execute permission.
-- Revoke from all other roles — this function reads public.users and must be tightly scoped.
GRANT EXECUTE ON FUNCTION public.custom_access_token_hook TO supabase_auth_admin;
REVOKE EXECUTE ON FUNCTION public.custom_access_token_hook FROM authenticated, anon, public;
