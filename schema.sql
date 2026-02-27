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
