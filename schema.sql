-- ============================================================
-- BharatVistaar Voice Agent — Clean Schema (reference / re-run)
-- This file defines the production tables, indexes, and views.
--
-- For a full data import from CSV, use import_data.sql instead.
-- ============================================================

-- Drop in reverse dependency order
DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS calls    CASCADE;

-- ============================================================
-- calls — one row per voice call
-- ============================================================
CREATE TABLE calls (
    id                                        SERIAL PRIMARY KEY,
    interaction_id                            TEXT UNIQUE NOT NULL,
    user_id                                   TEXT,
    connectivity_status                       TEXT,
    failure_reason                            TEXT,
    end_reason                                TEXT,
    duration_in_seconds                       NUMERIC(10,2),
    start_datetime                            TIMESTAMP,
    end_datetime                              TIMESTAMP,
    language_name                             TEXT,
    num_messages                              INTEGER,
    average_agent_response_time_in_seconds    NUMERIC(10,2),
    average_user_response_time_in_seconds     NUMERIC(10,2),
    user_contact_masked                       TEXT,
    channel_direction                         TEXT,
    retry_attempt                             INTEGER DEFAULT 0,
    campaign_id                               TEXT,
    cohort_id                                 TEXT,
    user_contact_hashed                       TEXT,
    is_debug_call                             BOOLEAN DEFAULT FALSE,
    audio_url                                 TEXT,
    server_retry_attempt                      INTEGER DEFAULT 0,
    has_log_issues                            BOOLEAN DEFAULT FALSE,
    channel_provider                          TEXT,
    channel_protocol                          TEXT,
    channel_type                              TEXT,
    provider_type                             TEXT,
    current_language                          TEXT,
    language_changed                          BOOLEAN DEFAULT FALSE,
    source                                    VARCHAR DEFAULT 'csv',
    created_at                                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- messages — conversation turns parsed from transcript
-- ============================================================
CREATE TABLE messages (
    id             SERIAL PRIMARY KEY,
    call_id        INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    role           TEXT    NOT NULL CHECK (role IN ('user', 'assistant')),
    content        TEXT    NOT NULL,
    message_order  INTEGER NOT NULL,
    UNIQUE (call_id, message_order)
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_calls_user_id        ON calls(user_id);
CREATE INDEX IF NOT EXISTS idx_calls_start_datetime ON calls(start_datetime);
CREATE INDEX IF NOT EXISTS idx_calls_end_reason     ON calls(end_reason);
CREATE INDEX IF NOT EXISTS idx_calls_language       ON calls(language_name);
CREATE INDEX IF NOT EXISTS idx_messages_call_id     ON messages(call_id);

-- ============================================================
-- ingest_log — tracks each daily CSV import run
-- ============================================================
CREATE TABLE IF NOT EXISTS ingest_log (
    id                  SERIAL PRIMARY KEY,
    filename            TEXT NOT NULL,
    run_at              TIMESTAMP DEFAULT NOW(),
    csv_rows            INTEGER,
    new_calls_added     INTEGER,
    messages_parsed     INTEGER
);

-- ============================================================
-- voice_call_tracking — tracks voice telemetry ingestion state
-- ============================================================
CREATE TABLE IF NOT EXISTS voice_call_tracking (
    sid                 TEXT PRIMARY KEY,
    call_id             INTEGER REFERENCES calls(id),
    turns_processed     INTEGER DEFAULT 0,
    last_ets            BIGINT,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_voice_tracking_sid ON voice_call_tracking(sid);

-- ============================================================
-- Views
-- ============================================================

-- call_log: one row per call, includes parsed message count
CREATE OR REPLACE VIEW call_log AS
SELECT
    c.interaction_id,
    c.user_contact_masked,
    c.start_datetime,
    c.end_datetime,
    c.duration_in_seconds,
    c.end_reason,
    c.connectivity_status,
    c.language_name,
    c.current_language,
    c.num_messages,
    c.average_agent_response_time_in_seconds,
    c.average_user_response_time_in_seconds,
    c.channel_direction,
    c.channel_provider,
    c.retry_attempt,
    c.has_log_issues,
    c.audio_url,
    COUNT(m.id) AS parsed_message_count
FROM calls c
LEFT JOIN messages m ON m.call_id = c.id
GROUP BY c.id
ORDER BY c.start_datetime DESC;

-- session_detail: call metadata + each message turn
CREATE OR REPLACE VIEW session_detail AS
SELECT
    c.interaction_id,
    c.user_contact_masked,
    c.start_datetime,
    c.end_datetime,
    c.duration_in_seconds,
    c.language_name,
    c.current_language,
    c.end_reason,
    c.audio_url,
    m.message_order,
    m.role,
    m.content
FROM calls c
JOIN messages m ON m.call_id = c.id
ORDER BY c.start_datetime DESC, m.message_order ASC;

-- ============================================================
-- Materialized Views — Dashboard Analytics
-- Refreshed every 15 minutes via processor cron
-- NOTE: See migrations/ for authoritative MV definitions
-- ============================================================

-- 1. Daily call analytics: volumes, durations, channel breakdown
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

-- 2. Daily user engagement metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_engagement_daily AS
SELECT
  DATE(TO_TIMESTAMP(s.session_start_at / 1000)) AS activity_date,
  COUNT(DISTINCT s.user_id) AS daily_active_users,
  COUNT(DISTINCT s.user_id) AS daily_devices,
  COUNT(*) AS total_sessions,
  AVG(s.duration_seconds) AS avg_session_duration,
  COUNT(DISTINCT CASE WHEN s.channel = 'voice' THEN s.user_id END) AS voice_users,
  COUNT(DISTINCT CASE WHEN s.channel = 'chat' THEN s.user_id END) AS chat_users
FROM sessions s
WHERE s.session_start_at IS NOT NULL
GROUP BY DATE(TO_TIMESTAMP(s.session_start_at / 1000));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_user_engagement_daily_date ON mv_user_engagement_daily(activity_date);

-- 3. Question/answer rates and feedback aggregation
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

-- 5. Call message counts — eliminates runtime JOIN for call stats
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

-- 6. Active users by date
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_active_users AS
SELECT
  DATE(TO_TIMESTAMP(session_start_at / 1000)) AS activity_date,
  COUNT(DISTINCT user_id) AS active_users
FROM sessions
WHERE session_start_at IS NOT NULL
GROUP BY DATE(TO_TIMESTAMP(session_start_at / 1000));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_active_users_date ON mv_active_users(activity_date);

-- 7. Session duration aggregates
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

-- 8. Device/Browser/OS density (existing V2 MVs)
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

-- ============================================================
-- Refresh Notes
-- Run manually after schema changes: REFRESH MATERIALIZED VIEW CONCURRENTLY <view_name>;
-- Processor auto-refreshes via cron every 15 minutes
-- ============================================================
