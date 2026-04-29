-- Migration: Add dashboard analytics materialized views
-- Date: 2024-04-16
-- Purpose: Pre-compute heavy aggregations for dashboard stats to improve query performance
-- Refresh: Every 15 minutes via processor cron
-- NOTE: Updated to match actual bh-dev-2 schema

-- =============================================================================
-- NEW HIGH-IMPACT MVs FOR DASHBOARD ANALYTICS
-- =============================================================================

-- 1. Daily call analytics: volumes, durations, channel breakdown
-- NOTE: calls table uses start_datetime/end_datetime (timestamps), not epoch
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_call_stats AS
SELECT
  DATE(c.start_datetime) AS call_date,
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
GROUP BY DATE(c.start_datetime), COALESCE(c.channel_direction, 'unknown');

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_call_stats_date_channel ON mv_daily_call_stats(call_date, channel);

-- 2. Daily user engagement metrics (from sessions table)
-- NOTE: sessions table uses session_start_at as bigint (epoch ms), user_id not fingerprint_id
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_engagement_daily AS
SELECT
  DATE(TO_TIMESTAMP(s.session_start_at / 1000)) AS activity_date,
  COUNT(DISTINCT s.user_id) AS daily_active_users,
  COUNT(DISTINCT s.user_id) AS daily_devices,  -- using user_id as proxy
  COUNT(*) AS total_sessions,
  AVG(s.duration_seconds) AS avg_session_duration,
  COUNT(DISTINCT CASE WHEN s.channel = 'voice' THEN s.user_id END) AS voice_users,
  COUNT(DISTINCT CASE WHEN s.channel = 'chat' THEN s.user_id END) AS chat_users
FROM sessions s
WHERE s.session_start_at IS NOT NULL
GROUP BY DATE(TO_TIMESTAMP(s.session_start_at / 1000));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_user_engagement_daily_date ON mv_user_engagement_daily(activity_date);

-- 3. Question/answer rates and feedback aggregation
-- NOTE: questions table uses ets (bigint epoch ms), no scope column, feedback has feedbacktype not rating
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_question_answer_rates AS
SELECT
  DATE(TO_TIMESTAMP(q.ets / 1000)) AS question_date,
  COALESCE(q.channel, 'unknown') AS channel,
  COUNT(*) AS total_questions,
  COUNT(DISTINCT q.uid) AS unique_users,
  COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) AS answered_questions,
  ROUND(
    COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0),
    2
  ) AS answer_rate_pct,
  COUNT(f.id) AS feedback_count,
  COUNT(CASE WHEN f.feedbacktype = 'like' THEN 1 END) AS likes,
  COUNT(CASE WHEN f.feedbacktype = 'dislike' THEN 1 END) AS dislikes
FROM questions q
LEFT JOIN feedback f ON q.sid = f.sid AND q.ets = f.ets
GROUP BY DATE(TO_TIMESTAMP(q.ets / 1000)), COALESCE(q.channel, 'unknown');

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_question_answer_rates_date_channel ON mv_question_answer_rates(question_date, channel);

-- 4. Channel performance comparison (voice vs chat)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_channel_performance AS
SELECT
  DATE(TO_TIMESTAMP(s.session_start_at / 1000)) AS performance_date,
  COALESCE(s.channel, 'unknown') AS channel,
  COUNT(DISTINCT s.user_id) AS users,
  COUNT(*) AS sessions,
  SUM(CASE WHEN c.id IS NOT NULL THEN 1 ELSE 0 END) AS calls,
  SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) AS messages,
  AVG(CASE WHEN c.duration_in_seconds IS NOT NULL THEN c.duration_in_seconds END) AS avg_call_duration
FROM sessions s
LEFT JOIN calls c ON s.user_id = c.user_id AND DATE(TO_TIMESTAMP(s.session_start_at / 1000)) = DATE(c.start_datetime)
LEFT JOIN messages m ON m.call_id = c.id
WHERE s.session_start_at IS NOT NULL
GROUP BY DATE(TO_TIMESTAMP(s.session_start_at / 1000)), COALESCE(s.channel, 'unknown');

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_channel_performance_date_channel ON mv_channel_performance(performance_date, channel);

-- 5. Active users by date
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_active_users AS
SELECT
  DATE(TO_TIMESTAMP(session_start_at / 1000)) AS activity_date,
  COUNT(DISTINCT user_id) AS active_users
FROM sessions
WHERE session_start_at IS NOT NULL
GROUP BY DATE(TO_TIMESTAMP(session_start_at / 1000));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_active_users_date ON mv_active_users(activity_date);

-- 6. Session duration aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_session_duration AS
SELECT
  COUNT(*) AS total_sessions,
  AVG(duration_seconds) AS avg_duration,
  MIN(duration_seconds) AS min_duration,
  MAX(duration_seconds) AS max_duration,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS p50_duration,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_duration
FROM sessions
WHERE duration_seconds IS NOT NULL AND duration_seconds > 0;

-- 7. Device/Browser/OS density (existing V2 MVs - ensuring they exist)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_total_devices AS
SELECT COUNT(DISTINCT fingerprint_id) AS total_devices
FROM users;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_browser_density AS
SELECT browser_name, COUNT(*) AS count
FROM users
WHERE browser_name IS NOT NULL
GROUP BY browser_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_browser_density_name ON mv_browser_density(browser_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_device_density AS
SELECT device_name, COUNT(*) AS count
FROM users
WHERE device_name IS NOT NULL
GROUP BY device_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_device_density_name ON mv_device_density(device_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_os_density AS
SELECT os_name, COUNT(*) AS count
FROM users
WHERE os_name IS NOT NULL
GROUP BY os_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_os_density_name ON mv_os_density(os_name);

-- =============================================================================
-- CALL LOGS OPTIMIZATION: Pre-computed call-message aggregations
-- Solves the ~27s chokepoint in getCallsStats() caused by LEFT JOIN + GROUP BY
-- =============================================================================

-- mv_call_message_counts: Pre-joins calls with messages to eliminate runtime JOIN
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_call_message_counts AS
SELECT
  c.id,
  c.user_id,
  c.duration_in_seconds,
  c.start_datetime,
  c.channel_direction,
  c.end_reason,
  c.language_name,
  COUNT(m.id) AS total_interactions,
  COUNT(m.id) FILTER (WHERE m.role = 'user') AS questions_count
FROM calls c
LEFT JOIN messages m ON m.call_id = c.id
GROUP BY c.id, c.user_id, c.duration_in_seconds, c.start_datetime, c.channel_direction, c.end_reason, c.language_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_call_message_counts_id ON mv_call_message_counts(id);
CREATE INDEX IF NOT EXISTS idx_mv_call_message_counts_start_datetime ON mv_call_message_counts(start_datetime);
CREATE INDEX IF NOT EXISTS idx_mv_call_message_counts_user_id ON mv_call_message_counts(user_id);

-- =============================================================================
-- Additional indexes on base tables for MV refresh performance
-- =============================================================================

-- Index for faster call stats aggregation
CREATE INDEX IF NOT EXISTS idx_calls_start_datetime ON calls(start_datetime);
CREATE INDEX IF NOT EXISTS idx_calls_start_date ON calls(DATE(start_datetime));

-- Index for faster question date aggregation  
CREATE INDEX IF NOT EXISTS idx_questions_ets_date ON questions(DATE(TO_TIMESTAMP(ets / 1000)));

-- Index for faster session date aggregation
CREATE INDEX IF NOT EXISTS idx_sessions_start_date ON sessions(DATE(TO_TIMESTAMP(session_start_at / 1000)));

-- =============================================================================
-- Refresh Notes
-- Run manually after schema changes: REFRESH MATERIALIZED VIEW CONCURRENTLY <view_name>;
-- Processor auto-refreshes via cron every 15 minutes
-- =============================================================================

-- Initial refresh of all views
REFRESH MATERIALIZED VIEW mv_call_message_counts;
REFRESH MATERIALIZED VIEW mv_daily_call_stats;
REFRESH MATERIALIZED VIEW mv_user_engagement_daily;
REFRESH MATERIALIZED VIEW mv_question_answer_rates;
REFRESH MATERIALIZED VIEW mv_channel_performance;
REFRESH MATERIALIZED VIEW mv_active_users;
REFRESH MATERIALIZED VIEW mv_session_duration;
REFRESH MATERIALIZED VIEW mv_total_devices;
REFRESH MATERIALIZED VIEW mv_browser_density;
REFRESH MATERIALIZED VIEW mv_device_density;
REFRESH MATERIALIZED VIEW mv_os_density;