const {
  isExternalFlowEvent,
  logicalEventName,
  normalizeExternalFlowEvent,
  processExternalFlowEvent,
} = require("../externalFlowProcessor");
const { redactSensitiveData } = require("../externalFlowPii");

const fixture = require("../../gist-telemetry/mh_full_telemetry_export.json");

function fixtureEvents(example) {
  return example.captured_winston_logs.flatMap((row) =>
    row.telemetry_envelope.events.filter((event) => !["OE_START", "OE_END"].includes(event.eid)),
  );
}

describe("external-flow detection and normalization", () => {
  test("only routes events carrying the external trace scope", () => {
    const events = fixtureEvents(fixture.success_example);
    const question = events.find((event) => event.edata.eks.target?.type === "Question");
    const roundTrip = events.find(
      (event) => event.edata.eks.target?.event_name === "chatbot_round_trip",
    );

    expect(isExternalFlowEvent(question)).toBe(false);
    expect(isExternalFlowEvent(roundTrip)).toBe(true);
    expect(logicalEventName(roundTrip)).toBe("chatbot_round_trip");
  });

  test("retains every permitted correlation identifier and location coordinate", () => {
    const event = fixtureEvents(fixture.success_example).find(
      (candidate) => candidate.edata.eks.event_name === "provider_webservice_call",
    );
    const record = normalizeExternalFlowEvent(event, {
      rawLogId: "4a1a3fc8-f779-421c-9d83-b3144e52d66f",
      rawEventIndex: 1,
    });

    expect(record.eventMid).toBe(event.mid);
    expect(record.transactionId).toBe(event.edata.eks.transaction_id);
    expect(record.messageId).toBe(event.edata.eks.message_id);
    expect(record.httpCallId).toBe(event.edata.eks.http_call_id);
    expect(record.requestPayload.latitude).toBe(event.edata.eks.request_payload.latitude);
    expect(record.requestPayload.longitude).toBe(event.edata.eks.request_payload.longitude);
    expect(record.rawEvent).toBeUndefined();
  });

  test("redacts a real farmer identifier from the verified failure fixture", () => {
    const event = fixtureEvents(fixture.failure_example).find(
      (candidate) => candidate.edata.eks.event_name === "provider_web_request",
    );
    const record = normalizeExternalFlowEvent(event);
    const serialized = JSON.stringify(record);

    expect(record.transactionId).toBe(event.edata.eks.transaction_id);
    expect(record.httpCallId).toBe(event.edata.eks.http_call_id);
    expect(record.requestPayload.farmer_id).toBe("[REDACTED_FARMER_ID]");
    expect(serialized).not.toContain("00000000000");
  });
});

describe("external-flow PII redaction", () => {
  test("redacts direct fields and embedded sensitive values without redacting correlation IDs", () => {
    const payload = {
      question_id: "question-correlation-id",
      session_id: "session-correlation-id",
      transaction_id: "transaction-correlation-id",
      http_call_id: "http-correlation-id",
      latitude: 20.741,
      longitude: 78.6,
      mobile: "9876543210",
      email: "farmer@example.com",
      farmer_id: "MH123456789",
      user_id: "internal-user-42",
      authorization: "Bearer secret-value",
      questionText: "My farmer ID is MH123456789 and email is farmer@example.com",
      nested: {
        response: "Call me on 9876543210 with token=secret-value",
        coordinates: { gps: "20.741,78.6" },
      },
    };

    const redacted = redactSensitiveData(payload);
    const serialized = JSON.stringify(redacted);

    expect(redacted.question_id).toBe(payload.question_id);
    expect(redacted.session_id).toBe(payload.session_id);
    expect(redacted.transaction_id).toBe(payload.transaction_id);
    expect(redacted.http_call_id).toBe(payload.http_call_id);
    expect(redacted.latitude).toBe(payload.latitude);
    expect(redacted.longitude).toBe(payload.longitude);
    expect(redacted.mobile).toBe("[REDACTED_PHONE]");
    expect(redacted.email).toBe("[REDACTED_EMAIL]");
    expect(redacted.farmer_id).toBe("[REDACTED_FARMER_ID]");
    expect(redacted.user_id).toBe("[REDACTED_PII]");
    expect(redacted.authorization).toBe("[REDACTED_TOKEN]");
    expect(serialized).not.toContain("9876543210");
    expect(serialized).not.toContain("farmer@example.com");
    expect(serialized).not.toContain("MH123456789");
    expect(serialized).not.toContain("secret-value");
  });

  test("sanitizes JSON encoded inside a request or response field", () => {
    const raw = JSON.stringify({
      farmer_id: "MH123456789",
      question_id: "question-correlation-id",
      response: "OTP 123456 for farmer@example.com",
    });

    const redacted = redactSensitiveData(raw, "response_payload");

    expect(typeof redacted).toBe("string");
    expect(redacted).toContain("[REDACTED_FARMER_ID]");
    expect(redacted).toContain("[REDACTED_OTP]");
    expect(redacted).toContain("[REDACTED_EMAIL]");
    expect(redacted).toContain("question-correlation-id");
  });

  test("never binds unredacted payload values when inserting an external event", async () => {
    const client = {
      query: jest.fn(async (sql) => {
        if (sql.includes("INSERT INTO public.external_api_traces")) {
          return { rows: [{ id: "00000000-0000-4000-8000-000000000001" }] };
        }
        if (sql.includes("SELECT DISTINCT trace_id")) return { rows: [] };
        if (sql.includes("SELECT event_name")) return { rows: [] };
        return { rows: [] };
      }),
    };
    const event = {
      eid: "OE_CHATBOT_AGENT_REQUEST",
      mid: "event-mid-1",
      edata: {
        eks: {
          trace_scope: "beckn_external_api",
          event_name: "chatbot_agent_request",
          question_id: "question-correlation-id",
          session_id: "session-correlation-id",
          request_payload: {
            mobile: "9876543210",
            farmer_id: "MH123456789",
            query: "Email farmer@example.com, token=secret-value",
          },
          response_payload: JSON.stringify({ authorization: "Bearer secret-value" }),
        },
      },
    };

    const result = await processExternalFlowEvent(client, event, {
      rawLogId: "4a1a3fc8-f779-421c-9d83-b3144e52d66f",
      rawEventIndex: 1,
    });
    const eventInsert = client.query.mock.calls.find(([sql]) =>
      sql.includes("INSERT INTO public.external_api_events"),
    );
    const boundValues = JSON.stringify(eventInsert[1]);

    expect(result.processed).toBe(true);
    expect(boundValues).toContain("question-correlation-id");
    expect(boundValues).toContain("session-correlation-id");
    expect(boundValues).not.toContain("9876543210");
    expect(boundValues).not.toContain("MH123456789");
    expect(boundValues).not.toContain("farmer@example.com");
    expect(boundValues).not.toContain("secret-value");
  });

  test("merges every matching trace even when the preferred alias is not the first database result", async () => {
    const chatTraceId = "00000000-0000-4000-8000-000000000001";
    const transactionTraceId = "00000000-0000-4000-8000-000000000002";
    const client = {
      query: jest.fn(async (sql) => {
        if (sql.includes("SELECT DISTINCT existing.trace_id")) {
          // PostgreSQL does not guarantee this result order. The session/question
          // alias is the preferred parent, even though it appears second here.
          return {
            rows: [
              {
                trace_id: transactionTraceId,
                identifier_type: "transaction_id",
                identifier_value: "transaction-1",
              },
              {
                trace_id: chatTraceId,
                identifier_type: "session_question",
                identifier_value: JSON.stringify(["session-1", "question-1"]),
              },
            ],
          };
        }
        if (sql.includes("SELECT identifier_type, identifier_value")) {
          return { rows: [{ identifier_type: "transaction_id", identifier_value: "transaction-1" }] };
        }
        if (sql.includes("SELECT event_name")) return { rows: [] };
        return { rows: [] };
      }),
    };
    const event = {
      eid: "OE_CHATBOT_ROUND_TRIP",
      mid: "event-mid-that-joins-traces",
      edata: {
        eks: {
          trace_scope: "beckn_external_api",
          question_id: "question-1",
          session_id: "session-1",
          transaction_id: "transaction-1",
        },
      },
    };

    const result = await processExternalFlowEvent(client, event);
    const movedEvents = client.query.mock.calls.find(
      ([sql, values]) =>
        sql.includes("UPDATE public.external_api_events SET trace_id") &&
        values[0] === chatTraceId &&
        values[1] === transactionTraceId,
    );
    const deletedTrace = client.query.mock.calls.find(
      ([sql, values]) =>
        sql.includes("DELETE FROM public.external_api_traces") && values[0] === transactionTraceId,
    );

    expect(result).toEqual({ processed: true, traceId: chatTraceId });
    expect(movedEvents).toBeDefined();
    expect(deletedTrace).toBeDefined();
  });

  test("keeps a retried event on its original trace without creating a new trace", async () => {
    const originalTraceId = "00000000-0000-4000-8000-000000000003";
    const client = {
      query: jest.fn(async (sql) => {
        if (sql.includes("SELECT trace_id FROM public.external_api_events")) {
          return { rows: [{ trace_id: originalTraceId }] };
        }
        return { rows: [] };
      }),
    };
    const event = {
      eid: "OE_PROVIDER_DEPENDENCY_CALL",
      mid: "already-processed-event-mid",
      edata: { eks: { trace_scope: "beckn_external_api" } },
    };

    const result = await processExternalFlowEvent(client, event);

    expect(result).toEqual({ processed: true, traceId: originalTraceId, duplicate: true });
    expect(client.query.mock.calls.some(([sql]) => sql.includes("INSERT INTO public.external_api_traces"))).toBe(false);
    expect(client.query.mock.calls.some(([sql]) => sql.includes("INSERT INTO public.external_api_events"))).toBe(false);
  });

  test("rejects an external event without a telemetry MID", async () => {
    const client = { query: jest.fn() };
    const result = await processExternalFlowEvent(client, {
      eid: "OE_PROVIDER_DEPENDENCY_CALL",
      edata: { eks: { trace_scope: "beckn_external_api" } },
    });

    expect(result).toEqual({ processed: false, reason: "missing event mid" });
    expect(client.query).not.toHaveBeenCalled();
  });
});
