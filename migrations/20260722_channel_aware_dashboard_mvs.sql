-- =============================================================================
-- Migration: 20260722_channel_aware_dashboard_mvs.sql
-- Purpose:   Make dashboard user/session materialized views channel-aware so
--            state-scoped requests can use the pre-aggregated fast path.
--
-- Rollout order:
--   1. Run this migration during a low-traffic window.
--   2. Deploy the processor definition changes.
--   3. Deploy the query-service and dashboard-app changes.
--
-- Replacement views are populated before the short transactional name swap,
-- so the current views remain available during the expensive build phase.
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_firstseen_ist_v2;

CREATE MATERIALIZED VIEW mv_users_daily_firstseen_ist_v2 AS
SELECT
  DATE(timezone('Asia/Kolkata', u.first_seen_at AT TIME ZONE 'UTC')) AS bucket_date,
  COALESCE(u.channel, 'unknown') AS channel,
  COUNT(DISTINCT u.fingerprint_id) AS new_users
FROM users u
WHERE u.fingerprint_id IS NOT NULL
  AND u.first_seen_at IS NOT NULL
GROUP BY 1, 2;

CREATE UNIQUE INDEX idx_mv_users_daily_firstseen_ist_date_channel
  ON mv_users_daily_firstseen_ist_v2(bucket_date, channel);


DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_returning_ist_v2;

CREATE MATERIALIZED VIEW mv_users_daily_returning_ist_v2 AS
SELECT
  DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0))) AS bucket_date,
  COALESCE(u.channel, 'unknown') AS channel,
  COUNT(DISTINCT q.fingerprint_id) AS returning_users
FROM questions q
JOIN users u ON q.fingerprint_id = u.fingerprint_id
WHERE q.fingerprint_id IS NOT NULL
  AND q.ets IS NOT NULL
  AND DATE(timezone('Asia/Kolkata', to_timestamp((q.ets)::double precision / 1000.0)))
      <> DATE(timezone('Asia/Kolkata', u.first_seen_at AT TIME ZONE 'UTC'))
GROUP BY 1, 2;

CREATE UNIQUE INDEX idx_mv_users_daily_returning_ist_date_channel
  ON mv_users_daily_returning_ist_v2(bucket_date, channel);


DROP MATERIALIZED VIEW IF EXISTS mv_sessions_daily_v2;

CREATE MATERIALIZED VIEW mv_sessions_daily_v2 AS
SELECT
  q.sid,
  q.fingerprint_id AS uid,
  COALESCE(q.channel, 'unknown') AS channel,
  MIN(q.ets) AS first_ets,
  MAX(q.ets) AS last_ets,
  COUNT(*) AS event_count,
  COUNT(*) AS question_count,
  DATE(
    timezone(
      'Asia/Kolkata',
      to_timestamp((MIN(q.ets))::double precision / 1000.0)
    )
  ) AS session_date_ist
FROM questions q
WHERE q.sid IS NOT NULL
  AND q.fingerprint_id IS NOT NULL
  AND q.answertext IS NOT NULL
  AND q.ets IS NOT NULL
GROUP BY q.sid, q.fingerprint_id, COALESCE(q.channel, 'unknown');

CREATE UNIQUE INDEX idx_mv_sessions_daily_channel_sid_uid
  ON mv_sessions_daily_v2(channel, sid, uid);
CREATE INDEX idx_mv_sessions_daily_channel_session_date
  ON mv_sessions_daily_v2(channel, session_date_ist);
CREATE INDEX idx_mv_sessions_daily_channel_last_ets
  ON mv_sessions_daily_v2(channel, last_ets DESC);
CREATE INDEX idx_mv_sessions_daily_channel_uid
  ON mv_sessions_daily_v2(channel, uid);


BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_firstseen_ist;
ALTER MATERIALIZED VIEW mv_users_daily_firstseen_ist_v2
  RENAME TO mv_users_daily_firstseen_ist;

DROP MATERIALIZED VIEW IF EXISTS mv_users_daily_returning_ist;
ALTER MATERIALIZED VIEW mv_users_daily_returning_ist_v2
  RENAME TO mv_users_daily_returning_ist;

DROP MATERIALIZED VIEW IF EXISTS mv_sessions_daily;
ALTER MATERIALIZED VIEW mv_sessions_daily_v2
  RENAME TO mv_sessions_daily;

COMMIT;

ANALYZE mv_users_daily_firstseen_ist;
ANALYZE mv_users_daily_returning_ist;
ANALYZE mv_sessions_daily;
