const { validateFeedback, FeedbackValidationError } = require('../feedbackPolicy');
const registry = require('../eventProcessors');
const { createTelemetryBatchProcessor } = require('../telemetryBatch');

jest.mock('../logger', () => ({ debug: jest.fn(), info: jest.fn(), error: jest.fn() }));

test.each([null, undefined, '', ' \t\r\n', '\u00a0', 123, {}])(
  'a dislike requires a nonblank text comment: %p', (comment) => {
    expect(() => validateFeedback('dislike', comment)).toThrow(FeedbackValidationError);
  },
);

test.each([null, undefined, '', ' \t\n', 'Helpful'])('likes keep comments optional: %p', (comment) => {
  expect(() => validateFeedback('like', comment)).not.toThrow();
});

test('a dislike with an explanation is accepted without changing its text', () => {
  expect(() => validateFeedback('dislike', ' The answer is incomplete. ')).not.toThrow();
});

test('an invalid dislike is quarantined while valid feedback in the same raw log is saved', async () => {
  registry.eventProcessors.length = 0;
  registry._testing.registerProcessor(1, 'FEEDBACK', 'feedback', {
    feedbackType: 'payload.rating', feedbackText: 'payload.comment', ets: 'ets',
  }, 'payload');
  const events = [
    { eid: 'FEEDBACK', ets: 1, payload: { rating: 'dislike', comment: ' \t\n' } },
    { eid: 'FEEDBACK', ets: 2, payload: { rating: 'like', comment: null } },
    { eid: 'FEEDBACK', ets: 3, payload: { rating: 'dislike', comment: 'Incomplete answer' } },
  ];
  const client = {
    query: jest.fn(async (sql) => {
      if (sql.startsWith('SELECT')) return { rows: [{ id: 'raw-feedback', message: JSON.stringify({ events }) }] };
      if (sql.startsWith('UPDATE winston_logs')) return { rowCount: 1 };
      return { rows: [], command: sql };
    }),
    release: jest.fn(),
  };
  const run = createTelemetryBatchProcessor({ pool: { connect: async () => client },
    eventProcessors: registry.eventProcessors, getNestedValue: registry.getNestedValue,
    isExternalFlowEvent: () => false });
  const result = await run();
  expect(result).toMatchObject({ status: 'success', processed: 1, eventsProcessed: 2, deadLetterCount: 1 });
  const inserts = client.query.mock.calls.filter(([sql]) => /INSERT INTO "feedback"/.test(sql));
  expect(inserts).toHaveLength(2);
  expect(inserts.map(([, values]) => values[0])).toEqual(['like', 'dislike']);
  expect(inserts[0][1][1]).toBeNull();
  const failure = client.query.mock.calls.find(([sql]) => sql.startsWith('INSERT INTO dead_letter_logs'));
  expect(failure[1][2]).toMatchObject({ raw_log_id: 'raw-feedback', raw_event_index: 1,
    failure_stage: 'validation', failure_code: 'FEEDBACK_COMMENT_REQUIRED' });
  expect(client.query.mock.calls.some(([sql]) => /DELETE FROM|TRUNCATE/.test(sql))).toBe(false);
  registry.eventProcessors.length = 0;
});
