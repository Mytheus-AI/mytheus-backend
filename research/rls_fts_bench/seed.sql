-- Seed for the FTS-under-RLS benchmark (Part 3 fix 1 of the journal plan).
--
-- Run as a SUPERUSER (supabase_admin on supabase/postgres:15.8.1.060) against a THROWAWAY
-- database that already has the backend, intelligence and transcription-service migrations
-- applied. Never against a real database: it writes synthetic rows into tenant tables and
-- switches off FK checks for the session (session_replication_role = replica) so it does not
-- have to fabricate meetings/users for every row.
--
-- Shape (deterministic, setseed 0.42):
--   200 tenants. Tenant 1 is "big" (20% of every table); the other 199 share the rest.
--   intelligence.transcript_chunk_search  1,000,000 rows  (12 words each)
--   public.decisions                         200,000 rows  (70% confirmed)
--   public.chat_messages                     300,000 rows  (one monthly partition)
--   A rare term `zephyr` appears in ~0.05% of rows; `tok7` is common (~0.4% per word slot).

SET session_replication_role = replica;
SELECT setseed(0.42);

CREATE TEMP TABLE bench_orgs AS
SELECT n, ('00000000-0000-4000-8000-' || lpad(n::text, 12, '0'))::uuid AS org_id
FROM generate_series(1, 200) n;

-- org for row i: 20% go to tenant 1, the rest round-robin over tenants 2..200.
CREATE OR REPLACE FUNCTION pg_temp.bench_org(i bigint) RETURNS uuid LANGUAGE sql IMMUTABLE AS $$
  SELECT ('00000000-0000-4000-8000-' || lpad(
           (CASE WHEN i % 5 = 0 THEN 1 ELSE 2 + (i % 199) END)::text, 12, '0'))::uuid
$$;

CREATE OR REPLACE FUNCTION pg_temp.bench_text(i bigint, words int) RETURNS text LANGUAGE sql VOLATILE AS $$
  SELECT string_agg(
           CASE WHEN random() < 0.00005 THEN 'zephyr'
                ELSE 'tok' || floor(power(random(), 2) * 3000)::int END, ' ')
  FROM generate_series(1, words)
$$;

INSERT INTO intelligence.transcript_chunk_search
  (org_id, meeting_id, chunk_id, speaker_id, start_ms, end_ms, text, confidential)
SELECT pg_temp.bench_org(i), gen_random_uuid(), gen_random_uuid(), 'spk' || (i % 7),
       (i % 3600000)::int, (i % 3600000)::int + 4000, pg_temp.bench_text(i, 12), (i % 50 = 0)
FROM generate_series(1, 1000000) i;

INSERT INTO public.decisions (org_id, meeting_id, content, rationale, confidence, status)
SELECT pg_temp.bench_org(i), gen_random_uuid(), pg_temp.bench_text(i, 14),
       pg_temp.bench_text(i + 7, 10), 0.8,
       CASE WHEN i % 10 < 7 THEN 'confirmed' ELSE 'proposed' END
FROM generate_series(1, 200000) i;

INSERT INTO public.chat_messages (org_id, meeting_id, user_id, content, created_at)
SELECT pg_temp.bench_org(i), gen_random_uuid(), gen_random_uuid(), pg_temp.bench_text(i, 10),
       '2026-11-10'::timestamptz + (i || ' seconds')::interval
FROM generate_series(1, 300000) i;

SET session_replication_role = origin;
ANALYZE intelligence.transcript_chunk_search;
ANALYZE public.decisions;
ANALYZE public.chat_messages;
