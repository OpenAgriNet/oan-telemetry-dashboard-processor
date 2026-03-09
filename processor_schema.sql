-- ============================================================
-- OAN Telemetry Dashboard Processor — Runtime Schema
-- Generated from ensureTablesExist() in index.js
--
-- Tables created/managed by the processor at startup.
-- Run this to bootstrap a fresh database.
-- ============================================================

-- ============================================================
-- winston_logs — source log table consumed by the processor
-- ============================================================
CREATE TABLE IF NOT EXISTS public.winston_logs (
    id          UUID DEFAULT gen_random_uuid(),
    level       VARCHAR,
    message     VARCHAR,
    meta        JSON,
    "timestamp" TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    sync_status INTEGER DEFAULT 0
);

-- ============================================================
-- dead_letter_logs — events that could not be processed
-- ============================================================
CREATE TABLE IF NOT EXISTS public.dead_letter_logs (
    level       VARCHAR,
    event_name  VARCHAR,
    message     VARCHAR,
    meta        JSON,
    "timestamp" TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    sync_status INTEGER DEFAULT 0
);

-- ============================================================
-- questions — OE_ITEM_RESPONSE events
-- ============================================================
CREATE TABLE IF NOT EXISTS public.questions (
    id                  UUID DEFAULT gen_random_uuid(),
    unique_id           VARCHAR,
    uid                 VARCHAR,
    sid                 VARCHAR,
    groupdetails        TEXT,
    channel             VARCHAR,
    ets                 BIGINT,
    questiontext        TEXT,
    questionsource      VARCHAR,
    answertext          TEXT,
    answer              TEXT,
    mobile              VARCHAR,
    username            VARCHAR,
    email               VARCHAR,
    role                VARCHAR,
    farmer_id           VARCHAR,
    registered_location JSONB,
    device_location     JSONB,
    agristack_location  JSONB,
    fingerprint_id      VARCHAR(64),
    is_new              SMALLINT NOT NULL DEFAULT 0,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- errorDetails — error telemetry events
-- ============================================================
CREATE TABLE IF NOT EXISTS public.errorDetails (
    id                  UUID DEFAULT gen_random_uuid(),
    unique_id           VARCHAR,
    uid                 VARCHAR,
    sid                 VARCHAR,
    groupdetails        TEXT,
    channel             VARCHAR,
    ets                 BIGINT,
    qid                 VARCHAR,
    errorText           TEXT,
    mobile              VARCHAR,
    username            VARCHAR,
    email               VARCHAR,
    role                VARCHAR,
    farmer_id           VARCHAR,
    registered_location JSONB,
    device_location     JSONB,
    agristack_location  JSONB,
    fingerprint_id      VARCHAR(64),
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- feedback — user feedback events
-- ============================================================
CREATE TABLE IF NOT EXISTS public.feedback (
    id                  UUID DEFAULT gen_random_uuid(),
    unique_id           VARCHAR,
    uid                 VARCHAR,
    sid                 VARCHAR,
    groupdetails        JSONB,
    channel             VARCHAR,
    ets                 BIGINT,
    feedbackText        TEXT,
    questionText        TEXT,
    answerText          TEXT,
    qid                 VARCHAR,
    feedbackType        TEXT,
    mobile              VARCHAR,
    username            VARCHAR,
    email               VARCHAR,
    role                VARCHAR,
    farmer_id           VARCHAR,
    registered_location JSONB,
    device_location     JSONB,
    agristack_location  JSONB,
    fingerprint_id      VARCHAR(64),
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- tts_details — Text-to-Speech API telemetry
-- ============================================================
CREATE TABLE IF NOT EXISTS public.tts_details (
    id             UUID DEFAULT gen_random_uuid(),
    unique_id      VARCHAR,
    uid            VARCHAR,
    sid            VARCHAR,
    channel        VARCHAR,
    ets            BIGINT,
    qid            VARCHAR,
    apiType        VARCHAR,
    apiService     VARCHAR,
    success        BOOLEAN,
    latencyMs      NUMERIC,
    statusCode     INTEGER,
    errorCode      VARCHAR,
    errorMessage   TEXT,
    language       VARCHAR,
    sessionId      VARCHAR,
    text           TEXT,
    mobile         VARCHAR,
    farmer_id      VARCHAR,
    fingerprint_id VARCHAR(64),
    created_at     TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- asr_details — Automatic Speech Recognition API telemetry
-- (identical schema to tts_details)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.asr_details (
    id             UUID DEFAULT gen_random_uuid(),
    unique_id      VARCHAR,
    uid            VARCHAR,
    sid            VARCHAR,
    channel        VARCHAR,
    ets            BIGINT,
    qid            VARCHAR,
    apiType        VARCHAR,
    apiService     VARCHAR,
    success        BOOLEAN,
    latencyMs      NUMERIC,
    statusCode     INTEGER,
    errorCode      VARCHAR,
    errorMessage   TEXT,
    language       VARCHAR,
    sessionId      VARCHAR,
    text           TEXT,
    mobile         VARCHAR,
    farmer_id      VARCHAR,
    fingerprint_id VARCHAR(64),
    created_at     TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- network_api_table — Beckn network API telemetry
-- ============================================================
CREATE TABLE IF NOT EXISTS public.network_api_table (
    id         SERIAL PRIMARY KEY,
    uid        VARCHAR,
    sid        VARCHAR,
    channel    VARCHAR,
    ets        BIGINT,
    input      JSONB,
    output     JSONB,
    success    TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- event_processors — dynamic processor configuration
-- ============================================================
CREATE TABLE IF NOT EXISTS public.event_processors (
    id                 SERIAL PRIMARY KEY,
    event_type         VARCHAR NOT NULL,
    table_name         VARCHAR NOT NULL,
    field_verification VARCHAR NOT NULL,
    field_mappings     JSONB NOT NULL,
    is_active          BOOLEAN DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT NOW(),
    updated_at         TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS event_processors_table_name_key
    ON public.event_processors (table_name);

-- ============================================================
-- users — V2 user profile + device fingerprint metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS public.users (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    uid                 VARCHAR(255) UNIQUE NOT NULL,
    -- User profile
    mobile              VARCHAR(20),
    username            VARCHAR(255),
    email               VARCHAR(255),
    role                VARCHAR(50),
    farmer_id           VARCHAR(100),
    registered_location JSONB,
    device_location     JSONB,
    agristack_location  JSONB,
    -- Device metadata (Fingerprint.js)
    fingerprint_id      VARCHAR(64),
    browser_code        VARCHAR(10),
    browser_name        VARCHAR(50),
    browser_version     VARCHAR(20),
    device_code         VARCHAR(10),
    device_name         VARCHAR(50),
    device_model        VARCHAR(100),
    os_code             VARCHAR(10),
    os_name             VARCHAR(50),
    os_version          VARCHAR(20),
    -- Timestamps
    first_seen_at       TIMESTAMP,
    last_seen_at        TIMESTAMP,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- Indexes
-- ============================================================

-- winston_logs: optimise the main processing query (sync_status = 0 ORDER BY timestamp)
CREATE INDEX IF NOT EXISTS idx_winston_logs_sync_status
    ON public.winston_logs (sync_status)
    WHERE sync_status = 0;
CREATE INDEX IF NOT EXISTS idx_winston_logs_timestamp
    ON public.winston_logs ("timestamp");
CREATE INDEX IF NOT EXISTS idx_winston_logs_sync_ts
    ON public.winston_logs (sync_status, "timestamp" ASC)
    WHERE sync_status = 0;

-- questions
CREATE INDEX IF NOT EXISTS idx_questions_created_at ON public.questions (created_at);
CREATE INDEX IF NOT EXISTS idx_questions_uid        ON public.questions (uid);
CREATE INDEX IF NOT EXISTS idx_questions_sid        ON public.questions (sid);
CREATE INDEX IF NOT EXISTS idx_questions_ets        ON public.questions (ets);

-- feedback
CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON public.feedback (created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_uid        ON public.feedback (uid);

-- errorDetails
CREATE INDEX IF NOT EXISTS idx_errordetails_created_at ON public.errorDetails (created_at);
CREATE INDEX IF NOT EXISTS idx_errordetails_uid        ON public.errorDetails (uid);

-- dead_letter_logs
CREATE INDEX IF NOT EXISTS idx_dead_letter_event_name ON public.dead_letter_logs (event_name);

-- users
CREATE INDEX IF NOT EXISTS idx_users_uid         ON public.users (uid);
CREATE INDEX IF NOT EXISTS idx_users_mobile      ON public.users (mobile);
CREATE INDEX IF NOT EXISTS idx_users_last_seen   ON public.users (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_users_fingerprint ON public.users (fingerprint_id);
CREATE INDEX IF NOT EXISTS idx_users_browser     ON public.users (browser_code);
CREATE INDEX IF NOT EXISTS idx_users_device_type ON public.users (device_code);
CREATE INDEX IF NOT EXISTS idx_users_os          ON public.users (os_code);

-- ============================================================
-- llm_telemetry — one row per LLM call, duplicate session_ids fine
-- Join on session_id with questions/feedback to get full picture
-- ============================================================
CREATE TABLE IF NOT EXISTS public.llm_telemetry (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id            VARCHAR(255) NOT NULL,
    uid                   VARCHAR(255),               -- user_id from payload
    total_input_tokens    INTEGER NOT NULL DEFAULT 0,
    total_output_tokens   INTEGER NOT NULL DEFAULT 0,
    tools_used            JSONB NOT NULL DEFAULT '[]', -- ["forward_geocode"]
    total_latency_seconds NUMERIC,
    created_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_telemetry_session_id ON public.llm_telemetry (session_id);
CREATE INDEX IF NOT EXISTS idx_llm_telemetry_uid        ON public.llm_telemetry (uid);
CREATE INDEX IF NOT EXISTS idx_llm_telemetry_created_at ON public.llm_telemetry (created_at);
CREATE INDEX IF NOT EXISTS idx_llm_telemetry_tools      ON public.llm_telemetry USING GIN (tools_used);
