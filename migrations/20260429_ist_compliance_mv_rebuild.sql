-- =============================================================================
-- Migration: 20260429_ist_compliance_mv_rebuild.sql
-- Purpose:   Drop and unconditionally recreate all IST-affected materialized
--            views to fix two classes of bugs:
--
--   (1) UTC date bucketing → correct IST (Asia/Kolkata) date bucketing.
--       Affected all time-series MVs created before this migration.
--
--   (2) mv_question_answer_rates metric definition:
--       Old: COUNT(*) — counted every row (no filter)
--       New: WHERE answertext IS NOT NULL AND fingerprint_id IS NOT NULL
--            (matches the definition used by /dashboard/stats base path)
--
-- NOTE: No REFRESH is issued here.  The processor's next cron cycle
--       (every 15 minutes) will REFRESH CONCURRENTLY each view, keeping
--       lock contention off this migration run.
--
-- Definitions in this file are kept in sync with the processor's
-- refreshMaterializedViews() in index.js (canonical source of truth).
-- =============================================================================


-- ─── DROP ALL IST-AFFECTED MVS (unconditional) ───────────────────────────────
-- CASCADE handles any dependent indexes automatically.

DROP MATERIALIZED VIEW IF EXISTS mv_daily_call_stats          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_question_answer_rates     CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_active_users              CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_calls_daily_counts        CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_firstseen_ist CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_returning_ist CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_feedback_daily            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sessions_daily            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_hourly_active_users       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_errors_daily              CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_asr_daily                 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_tts_daily                 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_session_counts      CASCADE;


-- ─── 1. mv_daily_call_stats ──────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_daily_call_stats AS
SELECT
  DATE(timezone('Asia/Kolkata', c.start_datetime AT TIME ZONE 'UTC')) AS call_date,
  COALESCE(c.channel_direction, 'unknown') AS channel,
  COUNT(*) AS total_calls,
  COUNT(DISTINCT c.user_id) AS unique_users,
  AVG(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS avg_duration_seconds,
  MIN(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS min_duration_seconds,
  MAX(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS max_duration_seconds,
  COUNT(CASE WHEN c.end_reason IS NULL OR c.end_reason = 'completed' THEN 1 END) AS completed_calls,
  COUNT(CASE WHEN c.end_reason IS NOT NULL AND c.end_reason != 'completed' THEN 1 END) AS failed_calls
FROM calls c
WHERE c.start_datetime IS NOT NULL AND c.end_datetime IS NOT NULL
GROUP BY
  DATE(timezone('Asia/Kolkata', c.start_datetime AT TIME ZONE 'UTC')),
  COALESCE(c.channel_direction, 'unknown');

CREATE UNIQUE INDEX idx_mv_daily_call_stats_date_channel
  ON mv_daily_call_stats(call_date, channel);


-- ─── 2. mv_question_answer_rates ─────────────────────────────────────────────
-- KEY FIX: WHERE answertext IS NOT NULL AND fingerprint_id IS NOT NULL
--          Count(*) now counts only "answered" questions (matches /dashboard/stats).

CREATE MATERIALIZED VIEW mv_question_answer_rates AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0))) AS question_date,
  COALESCE(q.channel, 'unknown') AS channel,
  COUNT(*) AS total_questions,
  COUNT(DISTINCT q.uid) AS unique_users,
  COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) AS answered_questions,
  ROUND(
    COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) * 100.0
      / NULLIF(COUNT(*), 0),
    2
  ) AS answer_rate_pct,
  COUNT(f.id) AS feedback_count,
  COUNT(CASE WHEN f.feedbacktype = 'like' THEN 1 END)    AS likes,
  COUNT(CASE WHEN f.feedbacktype = 'dislike' THEN 1 END) AS dislikes
FROM questions q
LEFT JOIN feedback f ON q.sid = f.sid AND q.ets = f.ets
WHERE q.answertext IS NOT NULL
  AND q.fingerprint_id IS NOT NULL
  AND q.ets IS NOT NULL
GROUP BY
  DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0))),
  COALESCE(q.channel, 'unknown');

CREATE UNIQUE INDEX idx_mv_question_answer_rates_date_channel
  ON mv_question_answer_rates(question_date, channel);


-- ─── 3. mv_active_users ──────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_active_users AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((ets)::double precision / 1000.0))) AS activity_date,
  COUNT(DISTINCT uid) AS active_users
FROM (
  SELECT uid, ets FROM questions    WHERE uid IS NOT NULL
  UNION ALL
  SELECT uid, ets FROM errordetails WHERE uid IS NOT NULL
) combined
GROUP BY DATE(timezone('Asia/Kolkata', to_timestamp((ets)::double precision / 1000.0)));

CREATE UNIQUE INDEX idx_mv_active_users_date ON mv_active_users(activity_date);


-- ─── 4. mv_calls_daily_counts ────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_calls_daily_counts AS
SELECT
  DATE(timezone('Asia/Kolkata', c.start_datetime AT TIME ZONE 'UTC')) AS call_date,
  COUNT(*) AS call_count
FROM calls c
WHERE c.start_datetime IS NOT NULL
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_calls_daily_counts_date ON mv_calls_daily_counts(call_date);


-- ─── 5. mv_users_daily_firstseen_ist ─────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_users_daily_firstseen_ist AS
SELECT
  DATE(timezone('Asia/Kolkata', u.first_seen_at AT TIME ZONE 'UTC')) AS bucket_date,
  COUNT(DISTINCT u.fingerprint_id) AS new_users
FROM users u
WHERE u.fingerprint_id IS NOT NULL
  AND u.first_seen_at IS NOT NULL
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_users_daily_firstseen_ist_date
  ON mv_users_daily_firstseen_ist(bucket_date);


-- ─── 6. mv_users_daily_returning_ist ─────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_users_daily_returning_ist AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0))) AS bucket_date,
  COUNT(DISTINCT q.fingerprint_id) AS returning_users
FROM questions q
JOIN users u ON q.fingerprint_id = u.fingerprint_id
WHERE q.fingerprint_id IS NOT NULL
  AND q.ets IS NOT NULL
  AND DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0)))
      <> DATE(timezone('Asia/Kolkata', u.first_seen_at AT TIME ZONE 'UTC'))
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_users_daily_returning_ist_date
  ON mv_users_daily_returning_ist(bucket_date);


-- ─── 7. mv_feedback_daily ────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_feedback_daily AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((f.ets)::double precision / 1000.0))) AS feedback_date,
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
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX idx_mv_feedback_daily_key
  ON mv_feedback_daily(feedback_date, channel, feedback_source);
CREATE INDEX idx_mv_feedback_daily_date ON mv_feedback_daily(feedback_date);


-- ─── 8. mv_sessions_daily ────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_sessions_daily AS
WITH combined AS (
  SELECT sid, fingerprint_id AS uid, ets
  FROM questions
  WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
    AND answertext IS NOT NULL AND ets IS NOT NULL
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
  SELECT sid, uid,
    MIN(ets) AS first_ets,
    MAX(ets) AS last_ets,
    COUNT(*) AS event_count
  FROM combined
  GROUP BY sid, uid
),
question_counts AS (
  SELECT sid, fingerprint_id AS uid, COUNT(*) AS question_count
  FROM questions
  WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
    AND answertext IS NOT NULL AND ets IS NOT NULL
  GROUP BY sid, fingerprint_id
)
SELECT
  sc.sid,
  sc.uid,
  sc.first_ets,
  sc.last_ets,
  sc.event_count,
  COALESCE(qc.question_count, 0) AS question_count,
  DATE(timezone('Asia/Kolkata', to_timestamp((sc.first_ets)::double precision / 1000.0))) AS session_date_ist
FROM session_counts sc
LEFT JOIN question_counts qc ON qc.sid = sc.sid AND qc.uid = sc.uid;

CREATE UNIQUE INDEX idx_mv_sessions_daily_sid_uid   ON mv_sessions_daily(sid, uid);
CREATE INDEX        idx_mv_sessions_daily_session_date ON mv_sessions_daily(session_date_ist);
CREATE INDEX        idx_mv_sessions_daily_last_ets   ON mv_sessions_daily(last_ets DESC);
CREATE INDEX        idx_mv_sessions_daily_uid        ON mv_sessions_daily(uid);


-- ─── 9. mv_hourly_active_users ───────────────────────────────────────────────
-- Note: this MV is time-windowed (last 48 h at creation time).
-- After the migration run it is empty until the processor refreshes it.

CREATE MATERIALIZED VIEW mv_hourly_active_users AS
WITH combined AS (
  SELECT uid, ets FROM questions    WHERE uid IS NOT NULL AND ets IS NOT NULL
  UNION ALL
  SELECT uid, ets FROM errordetails WHERE uid IS NOT NULL AND ets IS NOT NULL
)
SELECT
  DATE_TRUNC('hour', timezone('Asia/Kolkata', to_timestamp((c.ets)::double precision / 1000.0))) AS hour_bucket_ist,
  COUNT(DISTINCT c.uid) AS active_users
FROM combined c
WHERE c.ets >= (EXTRACT(EPOCH FROM (NOW() - INTERVAL '48 hours')) * 1000)
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_hourly_active_users_hour ON mv_hourly_active_users(hour_bucket_ist);


-- ─── 10. mv_errors_daily ─────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_errors_daily AS
SELECT
  DATE(timezone('Asia/Kolkata', e.created_at AT TIME ZONE 'UTC')) AS error_date,
  COALESCE(e.channel, 'unknown') AS channel,
  COUNT(*) AS error_count,
  COUNT(DISTINCT e.uid) AS unique_users,
  COUNT(DISTINCT e.sid) AS unique_sessions,
  COUNT(DISTINCT e.channel) AS unique_channels
FROM errordetails e
WHERE e.errortext IS NOT NULL AND e.created_at IS NOT NULL
GROUP BY 1, 2;

CREATE UNIQUE INDEX idx_mv_errors_daily_key  ON mv_errors_daily(error_date, channel);
CREATE INDEX        idx_mv_errors_daily_date ON mv_errors_daily(error_date);


-- ─── 11. mv_asr_daily ────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_asr_daily AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((a.ets)::double precision / 1000.0))) AS stat_date,
  COUNT(*) AS total_calls,
  COUNT(*) FILTER (WHERE a.success IS TRUE) AS success_count,
  AVG(a.latencyms) AS avg_latency,
  MAX(a.latencyms) AS max_latency
FROM asr_details a
WHERE a.ets IS NOT NULL
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_asr_daily_date ON mv_asr_daily(stat_date);


-- ─── 12. mv_tts_daily ────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_tts_daily AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((t.ets)::double precision / 1000.0))) AS stat_date,
  COUNT(*) AS total_calls,
  COUNT(*) FILTER (WHERE t.success IS TRUE) AS success_count,
  AVG(t.latencyms) AS avg_latency,
  MAX(t.latencyms) AS max_latency
FROM tts_details t
WHERE t.ets IS NOT NULL
GROUP BY 1;

CREATE UNIQUE INDEX idx_mv_tts_daily_date ON mv_tts_daily(stat_date);


-- ─── 13. mv_daily_session_counts ─────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_daily_session_counts AS
SELECT
  first_event_date AS stat_date,
  COUNT(*) AS session_count
FROM (
  SELECT
    sid,
    fingerprint_id AS uid,
    DATE(timezone('Asia/Kolkata', to_timestamp((MIN(ets))::double precision / 1000.0))) AS first_event_date
  FROM (
    SELECT sid, fingerprint_id, ets FROM questions
    WHERE sid IS NOT NULL AND answertext IS NOT NULL AND fingerprint_id IS NOT NULL
    UNION ALL
    SELECT sid, fingerprint_id, ets FROM feedback
    WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
    UNION ALL
    SELECT sid, fingerprint_id, ets FROM errordetails
    WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
  ) all_events
  GROUP BY sid, fingerprint_id
) session_dates
GROUP BY first_event_date;

CREATE UNIQUE INDEX idx_mv_daily_session_counts_date ON mv_daily_session_counts(stat_date);


-- =============================================================================
-- End of migration.
-- Next step: deploy the processor, then wait ≤15 min for REFRESH CONCURRENTLY
-- to populate all views.  Smoke test: compare /v1/dashboard/stats vs
-- /v1/questions/graph totals for the same date range.
-- =============================================================================
