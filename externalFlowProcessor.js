const logger = require("./logger");
const { redactSensitiveData } = require("./externalFlowPii");

const EXTERNAL_TRACE_SCOPE = "beckn_external_api";

function getEks(event) {
  return event?.edata?.eks || {};
}

function getTarget(event) {
  return getEks(event).target || {};
}

function firstValue(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function isExternalFlowEvent(event) {
  const eks = getEks(event);
  const target = getTarget(event);
  return (eks.trace_scope || target.trace_scope) === EXTERNAL_TRACE_SCOPE;
}

function logicalEventName(event) {
  const eks = getEks(event);
  const target = getTarget(event);
  return firstValue(eks.event_name, target.event_name, target.type, event?.eid, "unknown");
}

function toTimestamp(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  return new Date(numeric);
}

function toInteger(value) {
  if (value === undefined || value === null || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? Math.trunc(numeric) : null;
}

function toJsonbValue(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "string") {
    try {
      return redactSensitiveData(JSON.parse(value));
    } catch (_) {
      return redactSensitiveData(value);
    }
  }
  return redactSensitiveData(value);
}

function normalizeExternalFlowEvent(event, source = {}) {
  const eks = getEks(event);
  const target = getTarget(event);
  const transactionIds = firstValue(eks.transaction_ids, target.transactionIds, []);

  return {
    eventMid: event?.mid || null,
    rawLogId: source.rawLogId || null,
    rawEventIndex: source.rawEventIndex ?? null,
    eid: event?.eid || null,
    eventName: logicalEventName(event),
    traceScope: firstValue(eks.trace_scope, target.trace_scope),
    traceSchemaVersion: toInteger(firstValue(eks.trace_schema_version, target.trace_schema_version)),
    questionId: firstValue(eks.question_id, target.question_id, eks.qid),
    sessionId: firstValue(eks.session_id, target.session_id, event?.sid),
    transactionId: firstValue(eks.transaction_id, target.transaction_id, transactionIds?.[0]),
    messageId: firstValue(eks.message_id, target.message_id),
    httpCallId: firstValue(eks.http_call_id, target.http_call_id),
    dependencyOperationId: firstValue(eks.dependency_operation_id, target.dependency_operation_id),
    layer: firstValue(eks.layer, target.layer),
    service: firstValue(eks.service, target.service, event?.pdata?.id),
    dependency: firstValue(eks.dependency, target.dependency),
    stage: firstValue(eks.stage, target.stage),
    action: firstValue(eks.action, target.action),
    method: firstValue(eks.method, target.method),
    endpoint: firstValue(eks.endpoint, eks.provider_endpoint, target.endpoint),
    outcome: firstValue(eks.outcome, target.outcome),
    httpStatus: toInteger(firstValue(eks.http_status, target.http_status)),
    responseStatus: firstValue(eks.response_status, target.response_status),
    errorPayload: toJsonbValue(firstValue(eks.error, target.error)),
    eventTime: toTimestamp(event?.ets),
    requestStartedAt: firstValue(eks.request_started_at, target.request_started_at),
    responseReceivedAt: firstValue(eks.response_received_at, target.response_received_at),
    durationMs: toInteger(firstValue(eks.duration_ms, target.duration_ms)),
    storedAt: source.storedAt || null,
    requestPayload: toJsonbValue(firstValue(eks.request_payload, target.request_payload)),
    responsePayload: toJsonbValue(firstValue(eks.response_payload, target.response_payload)),
    requestPayloadBytes: toInteger(firstValue(eks.request_payload_bytes, target.request_payload_bytes)),
    responsePayloadBytes: toInteger(firstValue(eks.response_payload_bytes, target.response_payload_bytes)),
    responsePayloadTruncated: firstValue(eks.response_payload_truncated, target.response_payload_truncated, false),
    firstTokenMs: toInteger(firstValue(eks.firstTokenMs, target.firstTokenMs)),
    firstVisibleMs: toInteger(firstValue(eks.firstVisibleMs, target.firstVisibleMs)),
    firstAudioMs: toInteger(firstValue(eks.firstAudioMs, target.firstAudioMs)),
    totalMs: toInteger(firstValue(eks.totalMs, target.totalMs)),
    toolCalls: toJsonbValue(firstValue(eks.toolCalls, target.toolCalls)),
    rawEvent: redactSensitiveData(event),
  };
}

function correlationAliases(record) {
  return [
    ["question_id", record.questionId],
    ["session_id", record.sessionId],
    ["transaction_id", record.transactionId],
    ["message_id", record.messageId],
    ["http_call_id", record.httpCallId],
    ["dependency_operation_id", record.dependencyOperationId],
  ].filter(([, value]) => value);
}

async function ensureExternalFlowSchema(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.external_api_traces (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.external_api_trace_aliases (
      trace_id UUID NOT NULL REFERENCES public.external_api_traces(id) ON DELETE CASCADE,
      identifier_type TEXT NOT NULL,
      identifier_value TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (identifier_type, identifier_value)
    )
  `);
  await client.query(`
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
      raw_event JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await client.query("CREATE INDEX IF NOT EXISTS idx_external_api_events_trace_id ON public.external_api_events(trace_id)");
  await client.query("CREATE INDEX IF NOT EXISTS idx_external_api_events_event_time ON public.external_api_events(event_time)");
  await client.query("CREATE INDEX IF NOT EXISTS idx_external_api_events_transaction_id ON public.external_api_events(transaction_id)");
  await client.query("CREATE INDEX IF NOT EXISTS idx_external_api_events_http_call_id ON public.external_api_events(http_call_id)");
}

async function resolveTrace(client, record) {
  const aliases = correlationAliases(record);
  const values = aliases.map(([, value]) => value);
  let traceIds = [];

  if (values.length) {
    const result = await client.query(
      "SELECT DISTINCT trace_id FROM public.external_api_trace_aliases WHERE identifier_value = ANY($1::text[])",
      [values],
    );
    traceIds = result.rows.map((row) => row.trace_id);
  }

  let traceId = traceIds[0];
  if (!traceId) {
    const created = await client.query("INSERT INTO public.external_api_traces DEFAULT VALUES RETURNING id");
    traceId = created.rows[0].id;
  }

  for (const otherTraceId of traceIds.slice(1)) {
    if (otherTraceId === traceId) continue;
    await client.query("UPDATE public.external_api_events SET trace_id = $1 WHERE trace_id = $2", [traceId, otherTraceId]);
    const otherAliases = await client.query(
      "SELECT identifier_type, identifier_value FROM public.external_api_trace_aliases WHERE trace_id = $1",
      [otherTraceId],
    );
    for (const alias of otherAliases.rows) {
      await client.query(
        `INSERT INTO public.external_api_trace_aliases (trace_id, identifier_type, identifier_value)
         VALUES ($1, $2, $3)
         ON CONFLICT (identifier_type, identifier_value) DO UPDATE SET trace_id = EXCLUDED.trace_id`,
        [traceId, alias.identifier_type, alias.identifier_value],
      );
    }
    await client.query("DELETE FROM public.external_api_trace_aliases WHERE trace_id = $1", [otherTraceId]);
    await client.query("DELETE FROM public.external_api_traces WHERE id = $1", [otherTraceId]);
  }

  for (const [type, value] of aliases) {
    await client.query(
      `INSERT INTO public.external_api_trace_aliases (trace_id, identifier_type, identifier_value)
       VALUES ($1, $2, $3)
       ON CONFLICT (identifier_type, identifier_value) DO UPDATE SET trace_id = EXCLUDED.trace_id`,
      [traceId, type, value],
    );
  }

  await client.query(
    `UPDATE public.external_api_traces
     SET question_id = COALESCE(question_id, $2),
         session_id = COALESCE(session_id, $3),
         transaction_id = COALESCE(transaction_id, $4),
         message_id = COALESCE(message_id, $5),
         updated_at = NOW()
     WHERE id = $1`,
    [traceId, record.questionId, record.sessionId, record.transactionId, record.messageId],
  );
  return traceId;
}

async function refreshTraceSummary(client, traceId) {
  const result = await client.query(
    `SELECT event_name, event_time, outcome, layer, service, stage
     FROM public.external_api_events
     WHERE trace_id = $1
     ORDER BY event_time NULLS LAST, id ASC`,
    [traceId],
  );
  const rows = result.rows;
  const failures = rows.filter((row) => row.outcome === "failure");
  const partial = rows.some((row) => row.outcome === "partial");
  const successful = rows.some((row) => row.outcome === "success");
  const outcome = failures.length ? "failure" : partial ? "partial" : successful ? "success" : "incomplete";
  const stages = {};
  for (const row of rows) {
    const key = row.event_name || row.stage || row.layer || "unknown";
    stages[key] = (stages[key] || 0) + 1;
  }

  await client.query(
    `UPDATE public.external_api_traces
     SET first_event_at = $2,
         last_event_at = $3,
         outcome = $4,
         first_failure_event = $5,
         observed_event_count = $6,
         observed_stages = $7::jsonb,
         updated_at = NOW()
     WHERE id = $1`,
    [
      traceId,
      rows[0]?.event_time || null,
      rows[rows.length - 1]?.event_time || null,
      outcome,
      failures[0]?.event_name || null,
      rows.length,
      JSON.stringify(stages),
    ],
  );
}

async function processExternalFlowEvent(client, event, source) {
  const record = normalizeExternalFlowEvent(event, source);
  if (!record.eventMid) {
    return { processed: false, reason: "missing event mid" };
  }

  await client.query("SAVEPOINT external_flow_event");
  try {
    const traceId = await resolveTrace(client, record);
    const values = [
      record.eventMid, traceId, record.rawLogId, record.rawEventIndex, record.eid, record.eventName,
      record.traceScope, record.traceSchemaVersion, record.questionId, record.sessionId, record.transactionId,
      record.messageId, record.httpCallId, record.dependencyOperationId, record.layer, record.service,
      record.dependency, record.stage, record.action, record.method, record.endpoint, record.outcome,
      record.httpStatus, record.responseStatus, JSON.stringify(record.errorPayload), record.eventTime,
      record.requestStartedAt, record.responseReceivedAt, record.durationMs, record.storedAt,
      JSON.stringify(record.requestPayload), JSON.stringify(record.responsePayload), record.requestPayloadBytes,
      record.responsePayloadBytes, Boolean(record.responsePayloadTruncated), record.firstTokenMs,
      record.firstVisibleMs, record.firstAudioMs, record.totalMs, JSON.stringify(record.toolCalls),
      JSON.stringify(record.rawEvent),
    ];
    await client.query(
      `INSERT INTO public.external_api_events (
        event_mid, trace_id, raw_log_id, raw_event_index, eid, event_name, trace_scope, trace_schema_version,
        question_id, session_id, transaction_id, message_id, http_call_id, dependency_operation_id,
        layer, service, dependency, stage, action, method, endpoint, outcome, http_status, response_status,
        error_payload, event_time, request_started_at, response_received_at, duration_ms, stored_at,
        request_payload, response_payload, request_payload_bytes, response_payload_bytes,
        response_payload_truncated, first_token_ms, first_visible_ms, first_audio_ms, total_ms,
        tool_calls, raw_event
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,
        $25::jsonb,$26,$27,$28,$29,$30,$31::jsonb,$32::jsonb,$33,$34,$35,$36,$37,$38,$39,$40::jsonb,$41::jsonb
      ) ON CONFLICT (event_mid) DO UPDATE SET
        trace_id = EXCLUDED.trace_id,
        raw_log_id = EXCLUDED.raw_log_id,
        raw_event_index = EXCLUDED.raw_event_index,
        raw_event = EXCLUDED.raw_event,
        updated_at = NOW()`,
      values,
    );
    await refreshTraceSummary(client, traceId);
    await client.query("RELEASE SAVEPOINT external_flow_event");
    return { processed: true, traceId };
  } catch (err) {
    await client.query("ROLLBACK TO SAVEPOINT external_flow_event");
    logger.error(`External flow event processing failed (mid=${record.eventMid}): ${err.message}`);
    return { processed: false, reason: err.message };
  }
}

module.exports = {
  EXTERNAL_TRACE_SCOPE,
  ensureExternalFlowSchema,
  isExternalFlowEvent,
  logicalEventName,
  normalizeExternalFlowEvent,
  processExternalFlowEvent,
};
