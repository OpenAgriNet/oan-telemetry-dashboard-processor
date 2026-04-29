-- =============================================================================
-- Migration: Phase 1-5 Performance Overhaul
-- Date: 2026-04-18
-- Author: OpenCode
-- Purpose:
--   Dashboard wide performance overhaul.
--   Creates new materialized views and indexes used by the service after
--   Phase 1-5 controller rewrites. Idempotent and safe to run multiple
--   times. All new MVs have a UNIQUE index so the processor cron can
--   REFRESH MATERIALIZED VIEW CONCURRENTLY.
--
-- Scope:
--   1. /calls listing & count   -> mv_call_message_counts (exists) + mv_calls_daily_counts (new)
--   2. /dashboard/stats         -> mv_users_daily_firstseen_ist + mv_users_daily_returning_ist + mv_feedback_daily (new)
--   3. /devices/graph and /userss/graph-user -> same user-first-seen MVs (new)
--   4. /sessions graph/stats/list -> mv_sessions_daily (new)
--   5. /dashboard/user-analytics hourly -> mv_hourly_active_users (new)
--   6. /feedback graph/stats    -> mv_feedback_daily (new)
--   7. /questions graph/stats   -> existing mv_question_answer_rates (already created elsewhere)
--   8. /errors graph/stats      -> mv_errors_daily (new)
--   9. /asr + /tts              -> mv_asr_daily, mv_tts_daily (new)
--  10. /users                   -> mv_user_rollup (new, no-date-filter case)
--  11. /leaderboard/top10/month -> mv_monthly_leaderboard (new)
--
-- Runbook:
--   1. Ensure bh-main branch deployed on processor + service.
--   2. psql -f migrations/20260418_phase1to5_performance.sql
--      Creates extensions, MVs, indexes, does initial REFRESH.
--      Expected runtime: 30s - 5min depending on table volume.
--   3. Deploy processor (new MVs added to cron refresh list).
--   4. Deploy service (queries use new MVs, with fallback).
--   5. Verify via meta.source == 'mv' on GET /v1/calls, /v1/dashboard/stats, /v1/devices/graph.
-- =============================================================================

-- Required extension for trigram indexes that support ILIKE '%term%' search.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- PHASE 1: /calls daily counts + /users first/returning by day (IST)
-- =============================================================================

-- 1a. mv_calls_daily_counts
--  Used to avoid COUNT(*) on large calls table during pagination total.
--  Bucket: IST (Asia/Kolkata).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='calls') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_calls_daily_counts CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_calls_daily_counts AS
      SELECT
        DATE(c.start_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS call_date,
        COUNT(*) AS call_count
      FROM calls c
      WHERE c.start_datetime IS NOT NULL
      GROUP BY 1
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_calls_daily_counts_date ON mv_calls_daily_counts(call_date);
  ELSE
    RAISE NOTICE 'Skipping mv_calls_daily_counts: calls table does not exist';
  END IF;
END $$;

-- 1b. mv_users_daily_firstseen_ist
--  Replaces the per-request "new users" CTE on /dashboard/stats, /devices/graph, /userss/graph-user.
DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_firstseen_ist CASCADE;
CREATE MATERIALIZED VIEW mv_users_daily_firstseen_ist AS
SELECT
  DATE(u.first_seen_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS bucket_date,
  COUNT(DISTINCT u.fingerprint_id) AS new_users
FROM users u
WHERE u.fingerprint_id IS NOT NULL
  AND u.first_seen_at IS NOT NULL
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_users_daily_firstseen_ist_date ON mv_users_daily_firstseen_ist(bucket_date);

-- 1c. mv_users_daily_returning_ist
--  Replaces the expensive questions JOIN users + DATE(...) != DATE(...) CTE.
--  A user is counted as "returning" on a given bucket_date if they
--  asked a question on that date but their first_seen_at is on a
--  different (earlier) date.
DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_returning_ist CASCADE;
CREATE MATERIALIZED VIEW mv_users_daily_returning_ist AS
SELECT
  DATE(TO_TIMESTAMP(q.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS bucket_date,
  COUNT(DISTINCT q.fingerprint_id) AS returning_users
FROM questions q
JOIN users u ON q.fingerprint_id = u.fingerprint_id
WHERE q.fingerprint_id IS NOT NULL
  AND q.ets IS NOT NULL
  AND DATE(TO_TIMESTAMP(q.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')
      <> DATE(u.first_seen_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_users_daily_returning_ist_date ON mv_users_daily_returning_ist(bucket_date);

-- =============================================================================
-- PHASE 2: /dashboard/stats feedback + sessions-daily + hourly active users
-- =============================================================================

-- 2a. mv_feedback_daily
--  IST-bucketed per-day feedback aggregates. Includes voice-feedback filter
--  (OR COALESCE(feedback_source,'chat')='voice') matching dashboard.controller.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='feedback') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_feedback_daily CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_feedback_daily AS
      SELECT
        DATE(TO_TIMESTAMP(f.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS feedback_date,
        COALESCE(f.channel, 'unknown') AS channel,
        COALESCE(f.feedback_source, 'chat') AS feedback_source,
        COUNT(*) AS total_feedback,
        COUNT(*) FILTER (WHERE f.feedbacktype = 'like')    AS likes,
        COUNT(*) FILTER (WHERE f.feedbacktype = 'dislike') AS dislikes
      FROM feedback f
      WHERE f.ets IS NOT NULL
        AND f.feedbacktext IS NOT NULL
        AND (
          (f.questiontext IS NOT NULL AND f.fingerprint_id IS NOT NULL)
          OR COALESCE(f.feedback_source, 'chat') = 'voice'
        )
      GROUP BY 1, 2, 3
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_feedback_daily_key
      ON mv_feedback_daily(feedback_date, channel, feedback_source);
    CREATE INDEX IF NOT EXISTS idx_mv_feedback_daily_date ON mv_feedback_daily(feedback_date);
  ELSE
    RAISE NOTICE 'Skipping mv_feedback_daily: feedback table does not exist';
  END IF;
END $$;

-- 2b. mv_sessions_daily
--  Replaces the 3-table UNION ALL + GROUP BY (sid, uid) that powers
--  /sessions list, /sessions/stats, /sessions/graph.
--  One row per (sid, uid) session. Use session_date_ist for graph/stats filters.
--  Requires questions, feedback, errordetails tables to all exist.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='questions')
     AND EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='feedback')
     AND EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='errordetails') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_sessions_daily CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_sessions_daily AS
      WITH combined AS (
        SELECT sid, fingerprint_id AS uid, ets
        FROM questions
        WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND answertext IS NOT NULL AND ets IS NOT NULL
        UNION ALL
        SELECT sid, fingerprint_id AS uid, ets
        FROM feedback
        WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND ets IS NOT NULL
        UNION ALL
        SELECT sid, fingerprint_id AS uid, ets
        FROM errordetails
        WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND ets IS NOT NULL
      ),
      session_counts AS (
        SELECT sid, uid, MIN(ets) AS first_ets, MAX(ets) AS last_ets, COUNT(*) AS event_count
        FROM combined
        GROUP BY sid, uid
      ),
      question_counts AS (
        SELECT sid, fingerprint_id AS uid, COUNT(*) AS question_count
        FROM questions
        WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND answertext IS NOT NULL AND ets IS NOT NULL
        GROUP BY sid, fingerprint_id
      )
      SELECT
        sc.sid, sc.uid, sc.first_ets, sc.last_ets, sc.event_count,
        COALESCE(qc.question_count, 0) AS question_count,
        DATE(TO_TIMESTAMP(sc.first_ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS session_date_ist
      FROM session_counts sc
      LEFT JOIN question_counts qc ON qc.sid = sc.sid AND qc.uid = sc.uid
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sessions_daily_sid_uid ON mv_sessions_daily(sid, uid);
    CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_session_date ON mv_sessions_daily(session_date_ist);
    CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_last_ets ON mv_sessions_daily(last_ets DESC);
    CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_uid ON mv_sessions_daily(uid);
  ELSE
    RAISE NOTICE 'Skipping mv_sessions_daily: questions/feedback/errordetails table(s) missing';
  END IF;
END $$;

-- 2c. mv_hourly_active_users
--  Rolling 48-hour hourly unique user counts. Hours are bucketed in IST.
--  Used by GET /v1/dashboard/user-analytics?granularity=hourly.
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_active_users CASCADE;
CREATE MATERIALIZED VIEW mv_hourly_active_users AS
WITH combined AS (
  SELECT uid, ets FROM questions WHERE uid IS NOT NULL AND ets IS NOT NULL
  UNION ALL
  SELECT uid, ets FROM errordetails WHERE uid IS NOT NULL AND ets IS NOT NULL
)
SELECT
  DATE_TRUNC('hour', TO_TIMESTAMP(c.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS hour_bucket_ist,
  COUNT(DISTINCT c.uid) AS active_users
FROM combined c
WHERE c.ets >= (EXTRACT(EPOCH FROM (NOW() - INTERVAL '48 hours')) * 1000)
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_hourly_active_users_hour ON mv_hourly_active_users(hour_bucket_ist);

-- =============================================================================
-- PHASE 3: errors / asr / tts / users-rollup / monthly-leaderboard
-- =============================================================================

-- 3a. mv_errors_daily
--  Used by /errors/graph + /errors/stats. Keyed by (date, channel).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='errordetails') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_errors_daily CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_errors_daily AS
      SELECT
        DATE(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS error_date,
        COALESCE(e.channel, 'unknown') AS channel,
        COUNT(*) AS error_count,
        COUNT(DISTINCT e.uid) AS unique_users,
        COUNT(DISTINCT e.sid) AS unique_sessions,
        COUNT(DISTINCT e.channel) AS unique_channels
      FROM errordetails e
      WHERE e.errortext IS NOT NULL AND e.created_at IS NOT NULL
      GROUP BY 1, 2
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_errors_daily_key ON mv_errors_daily(error_date, channel);
    CREATE INDEX IF NOT EXISTS idx_mv_errors_daily_date ON mv_errors_daily(error_date);
  ELSE
    RAISE NOTICE 'Skipping mv_errors_daily: errordetails table does not exist';
  END IF;
END $$;

-- 3b. mv_asr_daily
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='asr_details') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_asr_daily CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_asr_daily AS
      SELECT
        DATE(TO_TIMESTAMP(a.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS stat_date,
        COUNT(*) AS total_calls,
        COUNT(*) FILTER (WHERE a.success IS TRUE) AS success_count,
        AVG(a.latencyms) AS avg_latency,
        MAX(a.latencyms) AS max_latency
      FROM asr_details a
      WHERE a.ets IS NOT NULL
      GROUP BY 1
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_asr_daily_date ON mv_asr_daily(stat_date);
  ELSE
    RAISE NOTICE 'Skipping mv_asr_daily: asr_details table does not exist';
  END IF;
END $$;

-- 3c. mv_tts_daily
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='tts_details') THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_tts_daily CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_tts_daily AS
      SELECT
        DATE(TO_TIMESTAMP(t.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS stat_date,
        COUNT(*) AS total_calls,
        COUNT(*) FILTER (WHERE t.success IS TRUE) AS success_count,
        AVG(t.latencyms) AS avg_latency,
        MAX(t.latencyms) AS max_latency
      FROM tts_details t
      WHERE t.ets IS NOT NULL
      GROUP BY 1
    $mv$;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_tts_daily_date ON mv_tts_daily(stat_date);
  ELSE
    RAISE NOTICE 'Skipping mv_tts_daily: tts_details table does not exist';
  END IF;
END $$;

-- 3d. mv_user_rollup
--  Per-user aggregates (no date window). Powers the /users list fast-path
--  when no startDate/endDate is provided.
DROP MATERIALIZED VIEW IF EXISTS mv_user_rollup CASCADE;
CREATE MATERIALIZED VIEW mv_user_rollup AS
WITH q AS (
  SELECT
    uid,
    COUNT(DISTINCT sid) AS session_count,
    COUNT(*) AS total_questions,
    MIN(ets) AS first_session,
    MAX(ets) AS latest_session,
    MAX(created_at) AS last_activity
  FROM questions
  WHERE uid IS NOT NULL AND answertext IS NOT NULL
  GROUP BY uid
),
f AS (
  SELECT
    uid,
    COUNT(*) AS feedback_count,
    COUNT(*) FILTER (WHERE feedbacktype = 'like') AS likes,
    COUNT(*) FILTER (WHERE feedbacktype = 'dislike') AS dislikes
  FROM feedback
  WHERE uid IS NOT NULL AND answertext IS NOT NULL
  GROUP BY uid
),
latest AS (
  SELECT DISTINCT ON (uid)
    uid,
    sid AS session_id
  FROM questions
  WHERE uid IS NOT NULL AND answertext IS NOT NULL
  ORDER BY uid, ets DESC
)
SELECT
  q.uid AS user_id,
  q.session_count,
  q.total_questions,
  q.first_session,
  q.latest_session,
  q.last_activity,
  COALESCE(f.feedback_count, 0) AS feedback_count,
  COALESCE(f.likes, 0) AS likes,
  COALESCE(f.dislikes, 0) AS dislikes,
  latest.session_id
FROM q
LEFT JOIN f ON f.uid = q.uid
LEFT JOIN latest ON latest.uid = q.uid;

CREATE UNIQUE INDEX idx_mv_user_rollup_user ON mv_user_rollup(user_id);
CREATE INDEX idx_mv_user_rollup_latest ON mv_user_rollup(latest_session DESC NULLS LAST);

-- 3e. mv_monthly_leaderboard
--  Month-aggregated leaderboard from questions table.
--  Keyed by (month_start, lgd_code, unique_id).
--
--  Guarded: only created if both `questions` and the required columns
--  (unique_id, farmer_id, registered_location) exist in this database.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'questions'
      AND column_name IN ('unique_id', 'farmer_id', 'registered_location', 'username')
    GROUP BY table_name
    HAVING COUNT(DISTINCT column_name) = 4
  ) THEN
    DROP MATERIALIZED VIEW IF EXISTS mv_monthly_leaderboard CASCADE;
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_monthly_leaderboard AS
      SELECT
        DATE_TRUNC('month', q.created_at)::date AS month_start,
        (q.registered_location->>'lgd_code') AS lgd_code,
        q.unique_id,
        MAX(q.username) AS username,
        (MAX(q.registered_location::text))::jsonb AS registered_location,
        COUNT(*) FILTER (WHERE q.answertext IS NOT NULL) AS record_count
      FROM questions q
      WHERE q.registered_location IS NOT NULL
        AND (q.registered_location->>'lgd_code') IS NOT NULL
        AND q.unique_id IS NOT NULL
        AND q.unique_id <> ''
        AND q.unique_id <> '696354'
        AND q.farmer_id IS NOT NULL
        AND q.farmer_id <> ''
        AND q.created_at IS NOT NULL
      GROUP BY 1, 2, 3
    $mv$;

    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_leaderboard_key
      ON mv_monthly_leaderboard(month_start, lgd_code, unique_id);
    CREATE INDEX IF NOT EXISTS idx_mv_monthly_leaderboard_ordered
      ON mv_monthly_leaderboard(month_start, lgd_code, record_count DESC);
  ELSE
    RAISE NOTICE 'Skipping mv_monthly_leaderboard: questions table missing required columns';
  END IF;
END $$;

-- =============================================================================
-- BASE-TABLE INDEXES
-- All partial/composite. pg_trgm already enabled above.
-- =============================================================================

-- calls table: trigram indexes for ILIKE '%term%' search (guarded)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='calls') THEN
    CREATE INDEX IF NOT EXISTS idx_calls_interaction_id_trgm
      ON calls USING gin (interaction_id gin_trgm_ops);
    CREATE INDEX IF NOT EXISTS idx_calls_user_contact_masked_trgm
      ON calls USING gin (user_contact_masked gin_trgm_ops);
  END IF;
END $$;

-- users
CREATE INDEX IF NOT EXISTS idx_users_first_seen_at ON users(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_users_fingerprint_firstseen
  ON users(fingerprint_id, first_seen_at)
  WHERE fingerprint_id IS NOT NULL;

-- questions
CREATE INDEX IF NOT EXISTS idx_questions_fingerprint_ets
  ON questions(fingerprint_id, ets)
  WHERE fingerprint_id IS NOT NULL AND answertext IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_uid_ets
  ON questions(uid, ets DESC)
  WHERE uid IS NOT NULL AND answertext IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_sid_ets
  ON questions(sid, ets)
  WHERE sid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_questiontext_trgm
  ON questions USING gin (questiontext gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_questions_answertext_trgm
  ON questions USING gin (answertext gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_questions_lgd_code_expr
  ON questions ((registered_location->>'lgd_code'))
  WHERE registered_location IS NOT NULL;

-- feedback
CREATE INDEX IF NOT EXISTS idx_feedback_ets_partial
  ON feedback(ets)
  WHERE feedbacktext IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_feedback_sid_ets
  ON feedback(sid, ets)
  WHERE sid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_feedback_uid_ets
  ON feedback(uid, ets)
  WHERE uid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_feedback_fingerprint_ets
  ON feedback(fingerprint_id, ets)
  WHERE fingerprint_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_feedback_feedbacktext_trgm
  ON feedback USING gin (feedbacktext gin_trgm_ops);

-- errordetails
CREATE INDEX IF NOT EXISTS idx_errordetails_ets ON errordetails(ets);
CREATE INDEX IF NOT EXISTS idx_errordetails_sid_ets
  ON errordetails(sid, ets)
  WHERE sid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_errordetails_uid_ets
  ON errordetails(uid, ets)
  WHERE uid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_errordetails_created_at ON errordetails(created_at);
CREATE INDEX IF NOT EXISTS idx_errordetails_errortext_trgm
  ON errordetails USING gin (errortext gin_trgm_ops);

-- asr / tts (guarded — tables may not exist in all environments)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='asr_details') THEN
    CREATE INDEX IF NOT EXISTS idx_asr_details_ets ON asr_details(ets);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='tts_details') THEN
    CREATE INDEX IF NOT EXISTS idx_tts_details_ets ON tts_details(ets);
  END IF;
END $$;

-- leaderboard (guarded — table may not exist in all environments)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'leaderboard'
  ) THEN
    CREATE INDEX IF NOT EXISTS idx_leaderboard_record_count_desc
      ON leaderboard(record_count DESC NULLS LAST);
    CREATE INDEX IF NOT EXISTS idx_leaderboard_district_code_txt
      ON leaderboard((district_code::text));
    CREATE INDEX IF NOT EXISTS idx_leaderboard_taluka_code_txt
      ON leaderboard((taluka_code::text));
    CREATE INDEX IF NOT EXISTS idx_leaderboard_village_code_txt
      ON leaderboard((village_code::text));
    CREATE INDEX IF NOT EXISTS idx_leaderboard_lgd_code_expr
      ON leaderboard((registered_location->>'lgd_code'));
  ELSE
    RAISE NOTICE 'Skipping leaderboard indexes: table does not exist';
  END IF;
END $$;

-- =============================================================================
-- INITIAL REFRESH
-- Non-concurrent is fine on first load (MVs are empty).
-- =============================================================================

-- Refresh each MV only if it was successfully created.
DO $$
DECLARE
  mv_list text[] := ARRAY[
    'mv_calls_daily_counts',
    'mv_users_daily_firstseen_ist',
    'mv_users_daily_returning_ist',
    'mv_feedback_daily',
    'mv_sessions_daily',
    'mv_hourly_active_users',
    'mv_errors_daily',
    'mv_asr_daily',
    'mv_tts_daily',
    'mv_user_rollup',
    'mv_monthly_leaderboard'
  ];
  mv_name text;
BEGIN
  FOREACH mv_name IN ARRAY mv_list LOOP
    IF EXISTS (
      SELECT 1 FROM pg_matviews
      WHERE schemaname = 'public' AND matviewname = mv_name
    ) THEN
      EXECUTE format('REFRESH MATERIALIZED VIEW %I', mv_name);
      RAISE NOTICE 'Refreshed %', mv_name;
    ELSE
      RAISE NOTICE 'Skipping refresh: % does not exist', mv_name;
    END IF;
  END LOOP;
END $$;

-- Done. Processor cron (15 min) will keep them up to date.
