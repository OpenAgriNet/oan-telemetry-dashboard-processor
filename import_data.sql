-- ============================================================
-- BharatVistaar Voice Agent — Full Schema + Data Import
-- Usage:
--   psql -d <database> -v csvfile="'$(dirname $0)/BharatVistaarVoiceAgentLID-2026-02-26.csv'" -f import_data.sql
--
-- Or set the csvfile variable manually inside psql:
--   \set csvfile '/absolute/path/to/BharatVistaarVoiceAgentLID-2026-02-26.csv'
--   \i import_data.sql
-- ============================================================

-- ============================================================
-- STEP 1: STAGING TABLE (raw CSV → TEXT columns)
-- ============================================================

DROP TABLE IF EXISTS staging_calls CASCADE;

CREATE TABLE staging_calls (
    job_id                              TEXT,
    interaction_id                      TEXT,
    user_id                             TEXT,
    connectivity_status                 TEXT,
    failure_reason                      TEXT,
    end_reason                          TEXT,
    duration_in_seconds                 TEXT,
    start_datetime                      TEXT,
    end_datetime                        TEXT,
    language_name                       TEXT,
    num_messages                        TEXT,
    average_agent_response_time_in_seconds TEXT,
    average_user_response_time_in_seconds  TEXT,
    user_contact_masked                 TEXT,
    channel_direction                   TEXT,
    retry_attempt                       TEXT,
    campaign_id                         TEXT,
    cohort_id                           TEXT,
    user_contact_hashed                 TEXT,
    is_debug_call                       TEXT,
    audio_url                           TEXT,
    server_retry_attempt                TEXT,
    has_log_issues                      TEXT,
    channel_provider                    TEXT,
    channel_protocol                    TEXT,
    channel_type                        TEXT,
    provider_type                       TEXT,
    transcript                          TEXT,
    -- CSV columns "agent_variables.current_language" and
    -- "agent_variables.language_changed" are quoted on import
    agent_variables_current_language    TEXT,
    agent_variables_language_changed    TEXT
);

-- ============================================================
-- STEP 2: LOAD CSV
-- The CSV is comma-delimited with a header row.
-- The file is expected to live in the same directory as this
-- script.  Run psql with -v csvfile="'/path/to/file.csv'" or
-- set :csvfile before running \i import_data.sql
-- ============================================================

\if :{?csvfile}
    \echo 'Loading CSV from ' :csvfile
    \copy staging_calls (
        job_id, interaction_id, user_id, connectivity_status,
        failure_reason, end_reason, duration_in_seconds,
        start_datetime, end_datetime, language_name, num_messages,
        average_agent_response_time_in_seconds,
        average_user_response_time_in_seconds,
        user_contact_masked, channel_direction, retry_attempt,
        campaign_id, cohort_id, user_contact_hashed, is_debug_call,
        audio_url, server_retry_attempt, has_log_issues,
        channel_provider, channel_protocol, channel_type,
        provider_type, transcript,
        agent_variables_current_language,
        agent_variables_language_changed
    ) FROM :csvfile WITH (FORMAT csv, HEADER true, DELIMITER ',');
\else
    \echo 'ERROR: Set the csvfile variable before running this script.'
    \echo 'Example: psql -d mydb -v csvfile='\''/path/to/BharatVistaarVoiceAgentLID-2026-02-26.csv'\'' -f import_data.sql'
    \quit
\endif

-- ============================================================
-- STEP 3: PRODUCTION TABLES (drop & recreate)
-- ============================================================

DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS calls    CASCADE;

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
    imported_at                               TIMESTAMP DEFAULT NOW()
);

CREATE TABLE messages (
    id             SERIAL PRIMARY KEY,
    call_id        INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    role           TEXT    NOT NULL CHECK (role IN ('user', 'assistant')),
    content        TEXT    NOT NULL,
    message_order  INTEGER NOT NULL,
    UNIQUE (call_id, message_order)
);

-- ============================================================
-- STEP 4: POPULATE calls FROM STAGING
-- Datetime format in CSV: "Feb 17, 2026, 12:14:06 AM"
-- ============================================================

INSERT INTO calls (
    interaction_id, user_id, connectivity_status, failure_reason,
    end_reason, duration_in_seconds, start_datetime, end_datetime,
    language_name, num_messages,
    average_agent_response_time_in_seconds,
    average_user_response_time_in_seconds,
    user_contact_masked, channel_direction,
    retry_attempt, campaign_id, cohort_id, user_contact_hashed,
    is_debug_call, audio_url, server_retry_attempt, has_log_issues,
    channel_provider, channel_protocol, channel_type, provider_type,
    current_language, language_changed
)
SELECT
    interaction_id,
    user_id,
    connectivity_status,
    failure_reason,
    end_reason,
    NULLIF(duration_in_seconds, '')::NUMERIC(10,2),
    TO_TIMESTAMP(NULLIF(start_datetime, ''), 'Mon FMDD, YYYY, HH12:MI:SS AM'),
    TO_TIMESTAMP(NULLIF(end_datetime,   ''), 'Mon FMDD, YYYY, HH12:MI:SS AM'),
    language_name,
    NULLIF(num_messages, '')::INTEGER,
    NULLIF(average_agent_response_time_in_seconds, '')::NUMERIC(10,2),
    NULLIF(average_user_response_time_in_seconds,  '')::NUMERIC(10,2),
    user_contact_masked,
    channel_direction,
    COALESCE(NULLIF(retry_attempt, '')::INTEGER, 0),
    NULLIF(campaign_id,  'NO_CAMPAIGN_ID'),
    NULLIF(cohort_id,    'NO_COHORT_ID'),
    user_contact_hashed,
    COALESCE(NULLIF(is_debug_call, '')::INTEGER, 0) = 1,
    audio_url,
    COALESCE(NULLIF(server_retry_attempt, '')::INTEGER, 0),
    COALESCE(NULLIF(has_log_issues, '')::INTEGER, 0) = 1,
    channel_provider,
    channel_protocol,
    channel_type,
    provider_type,
    NULLIF(agent_variables_current_language, ''),
    COALESCE(NULLIF(agent_variables_language_changed, '')::INTEGER, 0) = 1
FROM staging_calls
WHERE NULLIF(interaction_id, '') IS NOT NULL;

-- ============================================================
-- STEP 5: PARSE TRANSCRIPTS INTO messages
-- Splits on "Assistant:" / "User:" markers
-- ============================================================

CREATE OR REPLACE FUNCTION parse_transcripts()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_call_id     INTEGER;
    v_interaction TEXT;
    v_transcript  TEXT;
    v_role        TEXT;
    v_content     TEXT;
    v_order       INTEGER;
    v_match       TEXT[];
BEGIN
    FOR v_call_id, v_interaction, v_transcript IN
        SELECT c.id, c.interaction_id, s.transcript
        FROM   calls c
        JOIN   staging_calls s ON s.interaction_id = c.interaction_id
        WHERE  NULLIF(s.transcript, '') IS NOT NULL
    LOOP
        v_order := 1;

        FOR v_match IN
            SELECT regexp_matches(
                v_transcript,
                '(Assistant|User):\s*(.*?)(?=(?:Assistant|User):|$)',
                'g'
            )
        LOOP
            v_role    := CASE v_match[1] WHEN 'Assistant' THEN 'assistant' ELSE 'user' END;
            v_content := TRIM(v_match[2]);

            IF v_content <> '' THEN
                INSERT INTO messages (call_id, role, content, message_order)
                VALUES (v_call_id, v_role, v_content, v_order)
                ON CONFLICT (call_id, message_order) DO NOTHING;
                v_order := v_order + 1;
            END IF;
        END LOOP;
    END LOOP;
END;
$$;

SELECT parse_transcripts();

-- ============================================================
-- STEP 6: INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_calls_user_id        ON calls(user_id);
CREATE INDEX IF NOT EXISTS idx_calls_start_datetime ON calls(start_datetime);
CREATE INDEX IF NOT EXISTS idx_calls_end_reason     ON calls(end_reason);
CREATE INDEX IF NOT EXISTS idx_calls_language       ON calls(language_name);
CREATE INDEX IF NOT EXISTS idx_messages_call_id     ON messages(call_id);

-- ============================================================
-- STEP 7: VIEWS FOR DASHBOARD
-- ============================================================

-- View 1: Call log — one row per call with parsed message count
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

-- View 2: Session detail — call metadata + each message turn
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
-- STEP 8: CLEANUP & VERIFY
-- ============================================================

DROP TABLE staging_calls;

SELECT 'calls'    AS tbl, COUNT(*) AS row_count FROM calls
UNION ALL
SELECT 'messages' AS tbl, COUNT(*) AS row_count FROM messages;
