const { FeedbackValidationError } = require('./feedbackPolicy');

class TelemetryMessageError extends Error {
  constructor() {
    super('Telemetry message must be valid JSON containing an events array');
    this.code = 'INVALID_TELEMETRY_MESSAGE';
  }
}

function parseTelemetryMessage(message) {
  let envelope;
  try {
    envelope = JSON.parse(message);
  } catch (_) {
    throw new TelemetryMessageError();
  }
  if (!envelope || Array.isArray(envelope) || !Array.isArray(envelope.events)) {
    throw new TelemetryMessageError();
  }
  return envelope.events;
}

function emptyCounts() {
  return {
    processed: 0,
    eventsSeen: 0,
    eventsProcessed: 0,
    duplicateEvents: 0,
    ignoredEvents: 0,
    deadLetterEvents: 0,
    malformedLogs: 0,
    deadLetterCount: 0,
  };
}

function isRecordError(error) {
  // Invalid values and constraint failures are specific to the current event.
  // Connection, permission, schema, timeout and transaction failures must retry
  // the batch rather than silently disposing of valid input.
  return error instanceof FeedbackValidationError
    || (typeof error.code === 'string' && /^(22|23)/.test(error.code));
}

function createTelemetryBatchProcessor({
  pool,
  eventProcessors,
  getNestedValue,
  isExternalFlowEvent,
  processExternalFlowEvent,
  batchSize = 10,
  logger = {},
}) {
  const log = { info() {}, warn() {}, error() {}, ...logger };

  async function quarantine(client, raw, event, eventIndex, stage, code) {
    await client.query(
      `INSERT INTO dead_letter_logs(level, message, meta, event_name)
       VALUES ($1, $2, $3, $4)`,
      [
        raw.level,
        // Unreadable input remains in the original raw row. A reference avoids
        // copying an unparseable private payload into operational failure data.
        JSON.stringify(stage === 'parse'
          ? { raw_log_id: raw.id, reason: 'invalid telemetry message' }
          : { raw_log_id: raw.id, raw_event_index: eventIndex, failure_code: code }),
        {
          raw_log_id: raw.id,
          raw_event_index: eventIndex,
          failure_stage: stage,
          failure_code: code,
        },
        stage === 'parse' ? 'INVALID_TELEMETRY_MESSAGE' : event?.eid || 'INVALID_EVENT',
      ],
    );
  }

  return async function processTelemetryLogs(batchId = `batch_${Date.now()}`) {
    let client;
    let inTransaction = false;
    const counts = emptyCounts();
    try {
      client = await pool.connect();
      await client.query('BEGIN');
      inTransaction = true;
      const result = await client.query(
        `SELECT id, level, message, meta, "timestamp" AS stored_at
         FROM winston_logs WHERE sync_status = 0
         ORDER BY "timestamp" ASC, id ASC LIMIT $1`,
        [batchSize],
      );

      for (const raw of result.rows) {
        let events;
        try {
          events = parseTelemetryMessage(raw.message);
        } catch (error) {
          if (!(error instanceof TelemetryMessageError)) throw error;
          await quarantine(client, raw, null, null, 'parse', error.code);
          counts.malformedLogs++;
          counts.deadLetterCount++;
          events = [];
        }

        for (let index = 0; index < events.length; index++) {
          const event = events[index];
          counts.eventsSeen++;
          if (!event || typeof event !== 'object' || Array.isArray(event)
              || typeof event.eid !== 'string' || !event.eid.trim()) {
            await quarantine(client, raw, event, index + 1, 'validation', 'INVALID_EVENT');
            counts.deadLetterEvents++;
            counts.deadLetterCount++;
            continue;
          }
          if (event.eid === 'OE_START' || event.eid === 'OE_END') {
            counts.ignoredEvents++;
            continue;
          }

          if (isExternalFlowEvent(event)) {
            const outcome = await processExternalFlowEvent(client, event, {
              rawLogId: raw.id,
              rawEventIndex: index + 1,
              storedAt: raw.stored_at,
            });
            if (outcome.processed) {
              if (outcome.duplicate) counts.duplicateEvents++;
              else counts.eventsProcessed++;
            } else {
              if (outcome.errorCode && !isRecordError({ code: outcome.errorCode })) {
                const error = new Error('External processing requires a batch retry');
                error.code = outcome.errorCode;
                throw error;
              }
              await quarantine(client, raw, event, index + 1, 'external', 'EXTERNAL_EVENT_REJECTED');
              counts.deadLetterEvents++;
              counts.deadLetterCount++;
            }
            continue;
          }

          const processor = Object.values(eventProcessors).find((candidate) =>
            candidate && candidate.eventType === event.eid
              && getNestedValue(event, candidate.fieldVerification) !== undefined);
          if (!processor) {
            await quarantine(client, raw, event, index + 1, 'dispatch', 'NO_MATCHING_PROCESSOR');
            counts.deadLetterEvents++;
            counts.deadLetterCount++;
            continue;
          }

          await client.query('SAVEPOINT generic_event');
          try {
            const outcome = await processor.process(client, event);
            if (outcome?.success !== true) {
              const error = new Error('Event processor did not confirm persistence');
              error.code = 'UNCONFIRMED_WRITE';
              throw error;
            }
            await client.query('RELEASE SAVEPOINT generic_event');
            counts.eventsProcessed++;
          } catch (error) {
            if (!isRecordError(error)) throw error;
            await client.query('ROLLBACK TO SAVEPOINT generic_event');
            await client.query('RELEASE SAVEPOINT generic_event');
            await quarantine(client, raw, event, index + 1,
              error instanceof FeedbackValidationError ? 'validation' : 'persistence', error.code);
            counts.deadLetterEvents++;
            counts.deadLetterCount++;
          }
        }

        const updated = await client.query(
          'UPDATE winston_logs SET sync_status = 1 WHERE id = $1', [raw.id],
        );
        if (updated.rowCount !== 1) {
          const error = new Error('Raw delivery identity is missing or ambiguous');
          error.code = 'RAW_DELIVERY_IDENTITY';
          throw error;
        }
        counts.processed++;
      }

      const committed = await client.query('COMMIT');
      if (committed.command !== 'COMMIT') throw new Error('Batch was not committed');
      inTransaction = false;
      log.info(`[${batchId}] Committed telemetry outcomes: ${JSON.stringify(counts)}`);
      return { ...counts, status: 'success' };
    } catch (error) {
      if (client && inTransaction) {
        try { await client.query('ROLLBACK'); }
        catch (_) { /* A lost connection cannot acknowledge a rollback. */ }
      }
      // Database messages may contain rejected private values. Keep operational
      // output to a code and batch reference; raw evidence is retained.
      log.error(`[${batchId}] Telemetry batch failed (code=${error.code || 'PROCESSING_ERROR'})`);
      return {
        ...emptyCounts(),
        status: 'error',
        error: 'Telemetry batch could not be committed',
        errorCode: error.code || 'PROCESSING_ERROR',
      };
    } finally {
      if (client) client.release();
    }
  };
}

module.exports = { createTelemetryBatchProcessor, parseTelemetryMessage };
