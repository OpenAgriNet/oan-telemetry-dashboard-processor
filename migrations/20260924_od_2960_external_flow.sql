-- OD-2960: normalized, sanitized external API telemetry.
-- Run this migration before backfilling existing Winston rows in an environment.

CREATE TABLE IF NOT EXISTS public.external_api_traces (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trace_type TEXT NOT NULL DEFAULT 'unlinked',
  question_id TEXT,
  session_id TEXT,
  transaction_id TEXT,
  message_id TEXT,
  first_event_at TIMESTAMPTZ,
  last_event_at TIMESTAMPTZ,
  outcome TEXT,
  first_failure_event TEXT,
  observed_event_count INTEGER NOT NULL DEFAULT 0,
  observed_stages JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.external_api_trace_aliases (
  trace_id UUID NOT NULL REFERENCES public.external_api_traces(id) ON DELETE CASCADE,
  identifier_type TEXT NOT NULL,
  identifier_value TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (identifier_type, identifier_value)
);

CREATE TABLE IF NOT EXISTS public.external_api_events (
  id BIGSERIAL PRIMARY KEY,
  event_mid TEXT NOT NULL UNIQUE,
  trace_id UUID REFERENCES public.external_api_traces(id) ON DELETE SET NULL,
  raw_log_id UUID,
  raw_event_index INTEGER,
  eid TEXT,
  event_name TEXT NOT NULL,
  trace_scope TEXT NOT NULL,
  trace_schema_version INTEGER,
  question_id TEXT,
  session_id TEXT,
  transaction_id TEXT,
  message_id TEXT,
  http_call_id TEXT,
  dependency_operation_id TEXT,
  layer TEXT,
  service TEXT,
  dependency TEXT,
  stage TEXT,
  action TEXT,
  method TEXT,
  endpoint TEXT,
  outcome TEXT,
  http_status INTEGER,
  response_status TEXT,
  error_payload JSONB,
  event_time TIMESTAMPTZ,
  request_started_at TIMESTAMPTZ,
  response_received_at TIMESTAMPTZ,
  duration_ms BIGINT,
  stored_at TIMESTAMPTZ,
  request_payload JSONB,
  response_payload JSONB,
  request_payload_bytes BIGINT,
  response_payload_bytes BIGINT,
  response_payload_truncated BOOLEAN,
  first_token_ms BIGINT,
  first_visible_ms BIGINT,
  first_audio_ms BIGINT,
  total_ms BIGINT,
  tool_calls JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.external_api_traces
  ADD COLUMN IF NOT EXISTS trace_type TEXT NOT NULL DEFAULT 'unlinked';

ALTER TABLE public.external_api_events
  DROP COLUMN IF EXISTS raw_event;

CREATE INDEX IF NOT EXISTS idx_external_api_events_trace_id ON public.external_api_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_external_api_events_event_time ON public.external_api_events(event_time);
CREATE INDEX IF NOT EXISTS idx_external_api_events_transaction_id ON public.external_api_events(transaction_id);
CREATE INDEX IF NOT EXISTS idx_external_api_events_http_call_id ON public.external_api_events(http_call_id);
