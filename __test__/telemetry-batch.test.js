const { get } = require('lodash');
const { createTelemetryBatchProcessor, parseTelemetryMessage } = require('../telemetryBatch');

jest.mock('../logger', () => ({ debug: jest.fn(), info: jest.fn(), error: jest.fn() }));

const event = (extra = {}) => ({ eid: 'QUESTION', mid: 'event-1', payload: {}, ...extra });
const raw = (events) => ({
  id: 'raw-1', level: 'info', meta: {}, stored_at: new Date(0),
  message: JSON.stringify({ events }),
});

function harness(rows, options = {}) {
  const client = {
    query: jest.fn(async (sql) => {
      if (sql.startsWith('SELECT')) return { rows };
      if (sql.startsWith('UPDATE winston_logs')) return { rowCount: 1 };
      return { rows: [], command: sql };
    }),
    release: jest.fn(),
  };
  const process = jest.fn(async () => ({ success: true }));
  const dependencies = {
    pool: { connect: jest.fn(async () => client) },
    eventProcessors: [{ eventType: 'QUESTION', fieldVerification: 'payload', process }],
    getNestedValue: get,
    isExternalFlowEvent: (value) => value.external === true,
    processExternalFlowEvent: jest.fn(async () => ({ processed: true })),
    ...options,
  };
  return { client, process, dependencies, run: createTelemetryBatchProcessor(dependencies) };
}

describe('durable telemetry batch outcomes', () => {
  test('counts saved events, repeats, intentional ignores and failures separately', async () => {
    const external = jest.fn()
      .mockResolvedValueOnce({ processed: true })
      .mockResolvedValueOnce({ processed: true, duplicate: true })
      .mockResolvedValueOnce({ processed: false, reason: 'missing event mid' });
    const test = harness([raw([
      event(), event({ eid: 'OE_START' }), event({ eid: 'OE_END' }),
      event({ external: true }), event({ external: true }), event({ external: true }),
      event({ eid: 'UNKNOWN' }),
    ])], { processExternalFlowEvent: external });
    const result = await test.run();
    expect(result).toEqual({
      status: 'success', processed: 1, eventsSeen: 7, eventsProcessed: 2,
      duplicateEvents: 1, ignoredEvents: 2, deadLetterEvents: 2,
      malformedLogs: 0, deadLetterCount: 2,
    });
    expect(result.eventsSeen).toBe(result.eventsProcessed + result.duplicateEvents
      + result.ignoredEvents + result.deadLetterEvents);
    expect(test.process).toHaveBeenCalledTimes(1);
    expect(test.client.release).toHaveBeenCalledTimes(1);
  });

  test('restores the transaction before quarantining a bad event and saving a good one', async () => {
    const test = harness([raw([event({ mid: 'bad' }), event({ mid: 'good' })])]);
    test.process.mockRejectedValueOnce(Object.assign(new Error('private rejected value'), { code: '22P02' }))
      .mockResolvedValueOnce({ success: true });
    const result = await test.run();
    expect(result).toMatchObject({ status: 'success', eventsProcessed: 1, deadLetterEvents: 1 });
    const statements = test.client.query.mock.calls.map(([sql]) => sql);
    expect(statements.indexOf('ROLLBACK TO SAVEPOINT generic_event'))
      .toBeLessThan(statements.findIndex((sql) => sql.startsWith('INSERT INTO dead_letter_logs')));
    expect(statements.filter((sql) => sql === 'SAVEPOINT generic_event')).toHaveLength(2);
    const failure = test.client.query.mock.calls.find(([sql]) => sql.startsWith('INSERT INTO dead_letter_logs'))[1];
    expect(failure[2]).toMatchObject({ raw_log_id: 'raw-1', raw_event_index: 1, failure_code: '22P02' });
    expect(JSON.stringify(failure)).not.toContain('private rejected value');
  });

  test.each(['{broken', 'null', '{}', '[]', '{"events":{}}'])('records an invalid envelope before marking it handled: %s', async (message) => {
    const test = harness([{ ...raw([]), message }]);
    const result = await test.run();
    expect(result).toMatchObject({ status: 'success', processed: 1, malformedLogs: 1, deadLetterCount: 1, eventsSeen: 0 });
    const statements = test.client.query.mock.calls.map(([sql]) => sql);
    expect(statements.findIndex((sql) => sql.startsWith('INSERT INTO dead_letter_logs')))
      .toBeLessThan(statements.findIndex((sql) => sql.startsWith('UPDATE winston_logs')));
    const failure = test.client.query.mock.calls.find(([sql]) => sql.startsWith('INSERT INTO dead_letter_logs'))[1];
    expect(JSON.parse(failure[1])).toEqual({ raw_log_id: 'raw-1', reason: 'invalid telemetry message' });
    expect(failure[2].failure_stage).toBe('parse');
  });

  test('a valid empty envelope is handled without inventing a failure', async () => {
    const result = await harness([raw([])]).run();
    expect(result).toMatchObject({ status: 'success', processed: 1, eventsProcessed: 0, malformedLogs: 0, deadLetterCount: 0 });
    expect(parseTelemetryMessage('{"events":[]}')).toEqual([]);
  });

  test('an invalid array member does not block the following valid event', async () => {
    const result = await harness([raw([null, event()])]).run();
    expect(result).toMatchObject({ status: 'success', eventsSeen: 2, eventsProcessed: 1, deadLetterEvents: 1 });
  });

  test('does not change generic repeated-delivery or separate question/answer behavior', async () => {
    const test = harness([raw([event(), event(), event({ mid: 'answer-stage' })])]);
    const result = await test.run();
    expect(test.process).toHaveBeenCalledTimes(3);
    expect(result).toMatchObject({ eventsProcessed: 3, duplicateEvents: 0 });
  });

  test('a failure-store error rolls back successful events and leaves raw deliveries pending', async () => {
    const test = harness([raw([event(), event({ eid: 'UNKNOWN' })])]);
    const query = test.client.query.getMockImplementation();
    test.client.query.mockImplementation(async (sql, values) => {
      if (sql.startsWith('INSERT INTO dead_letter_logs')) throw Object.assign(new Error('write failed'), { code: '53100' });
      return query(sql, values);
    });
    const result = await test.run();
    expect(result).toMatchObject({ status: 'error', processed: 0, eventsProcessed: 0, deadLetterCount: 0 });
    expect(test.client.query).toHaveBeenCalledWith('ROLLBACK');
    expect(test.client.query.mock.calls.some(([sql]) => sql.startsWith('UPDATE winston_logs'))).toBe(false);
  });

  test.each(['08006', '57P01', '57014', '40001', '42501', '42P01'])('retries infrastructure/configuration errors instead of discarding input: %s', async (code) => {
    const test = harness([raw([event()])]);
    test.process.mockRejectedValueOnce(Object.assign(new Error('failure'), { code }));
    const result = await test.run();
    expect(result).toMatchObject({ status: 'error', processed: 0, eventsProcessed: 0, errorCode: code });
    expect(test.client.query).toHaveBeenCalledWith('ROLLBACK');
    expect(test.client.query.mock.calls.some(([sql]) => sql.startsWith('INSERT INTO dead_letter_logs'))).toBe(false);
  });

  test('never counts a handler result that did not confirm saving', async () => {
    const test = harness([raw([event()])]);
    test.process.mockResolvedValueOnce({ success: false, skipped: true });
    expect(await test.run()).toMatchObject({ status: 'error', eventsProcessed: 0, processed: 0, errorCode: 'UNCONFIRMED_WRITE' });
  });

  test('does not report success when PostgreSQL rolls back at commit', async () => {
    const test = harness([raw([event()])]);
    const query = test.client.query.getMockImplementation();
    test.client.query.mockImplementation((sql, values) => sql === 'COMMIT'
      ? Promise.resolve({ command: 'ROLLBACK' }) : query(sql, values));
    expect(await test.run()).toMatchObject({ status: 'error', processed: 0, eventsProcessed: 0 });
  });

  test('does not mark ambiguous raw identities as successfully processed', async () => {
    const test = harness([raw([event()])]);
    const query = test.client.query.getMockImplementation();
    test.client.query.mockImplementation((sql, values) => sql.startsWith('UPDATE winston_logs')
      ? Promise.resolve({ rowCount: 2 }) : query(sql, values));
    expect(await test.run()).toMatchObject({ status: 'error', processed: 0, errorCode: 'RAW_DELIVERY_IDENTITY' });
  });

  test('connection acquisition failure returns zero committed outcomes', async () => {
    const test = harness([], { pool: { connect: jest.fn().mockRejectedValue(new Error('unavailable')) } });
    expect(await test.run()).toMatchObject({ status: 'error', processed: 0, eventsProcessed: 0 });
  });

  test('an empty queue reports all committed counters as zero', async () => {
    expect(await harness([]).run()).toMatchObject({ status: 'success', processed: 0, eventsSeen: 0, deadLetterCount: 0 });
  });
});

describe('generic handler persistence contract', () => {
  const registry = require('../eventProcessors');
  beforeEach(() => { registry.eventProcessors.length = 0; });

  test('returns success only after saving and propagates the original database error', async () => {
    registry._testing.registerProcessor(1, 'QUESTION', 'questions', { ets: 'ets' }, 'payload');
    const process = registry.eventProcessors[0].process;
    expect(await process({ query: jest.fn().mockResolvedValue({ rowCount: 1 }) }, { ets: 1 })).toEqual({ success: true });
    const error = Object.assign(new Error('bad timestamp'), { code: '22P02' });
    await expect(process({ query: jest.fn().mockRejectedValue(error) }, { ets: 'bad' })).rejects.toBe(error);
  });
});
