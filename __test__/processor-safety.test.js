let mockLoggerLevel;
const mockPool = { connect: jest.fn() };
jest.mock('dotenv', () => ({ config: () => { process.env.LOG_LEVEL = 'warn'; } }));
jest.mock('pg', () => ({ Pool: jest.fn(() => mockPool) }));
jest.mock('node-cron', () => ({ schedule: jest.fn() }));
jest.mock('../logger', () => {
  mockLoggerLevel = process.env.LOG_LEVEL;
  return { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() };
});
const { app } = require('../index');
const registry = require('../eventProcessors');
const { requireManagementAuth } = require('../managementAuth');
const { validateRegistration } = require('../processorConfig');
const { redactEndpoint } = require('../externalFlowPii');
const { processExternalFlowEvent } = require('../externalFlowProcessor');

function response() {
  return { status: jest.fn().mockReturnThis(), json: jest.fn().mockReturnThis() };
}
function route(method, path) {
  return app._router.stack.find(layer => layer.route?.path === path && layer.route.methods[method]).route.stack[0].handle;
}
const originalKey = process.env.PROCESSOR_MANAGEMENT_API_KEY;
afterAll(() => {
  if (originalKey === undefined) delete process.env.PROCESSOR_MANAGEMENT_API_KEY;
  else process.env.PROCESSOR_MANAGEMENT_API_KEY = originalKey;
});

test('loads environment settings before logging and imports without starting background work', () => {
  expect(mockLoggerLevel).toBe('warn');
  expect(require('node-cron').schedule).not.toHaveBeenCalled();
  expect(mockPool.connect).not.toHaveBeenCalled();
});

test.each([
  [undefined, undefined, 503], ['configured-test-key', undefined, 401],
  ['configured-test-key', 'Bearer wrong-key', 401], ['configured-test-key', 'configured-test-key', 401],
])('management access fails closed', (key, header, status) => {
  if (key === undefined) delete process.env.PROCESSOR_MANAGEMENT_API_KEY;
  else process.env.PROCESSOR_MANAGEMENT_API_KEY = key;
  const res = response(); const next = jest.fn();
  requireManagementAuth({ get: () => header }, res, next);
  expect(res.status).toHaveBeenCalledWith(status);expect(next).not.toHaveBeenCalled();
});

test('permits only a matching management bearer key and protects routes before handlers', () => {
  process.env.PROCESSOR_MANAGEMENT_API_KEY = 'configured-test-key';
  const next = jest.fn();requireManagementAuth({ get: () => 'Bearer configured-test-key' }, response(), next);
  expect(next).toHaveBeenCalledTimes(1);
  const auth = app._router.stack.findIndex(layer => layer.handle === requireManagementAuth);
  const firstRoute = app._router.stack.findIndex(layer => layer.route);
  expect(auth).toBeGreaterThan(-1);expect(auth).toBeLessThan(firstRoute);
});

test.each(['questions; DROP TABLE x', 'public.questions', 'winston_logs', 'pg_authid'])('rejects unsafe or unapproved registration tables: %s', table => {
  expect(() => validateRegistration('TEST_EVENT', table, { ets: 'ets' }, 'payload')).toThrow();
});
test.each([{ id: 'id' }, { created_at: 'date' }, { ets: 'ets', ETS: 'other' }, { ets: 4 }])('rejects invalid field mappings', mappings => {
  expect(() => validateRegistration('TEST_EVENT', 'questions', mappings, 'payload')).toThrow();
});

test('the actual registration route passes its complete contract and replaces the in-memory rule', async () => {
  registry.eventProcessors.length = 0;
  const client = {
    query: jest.fn(async sql => {
      if (sql.startsWith('SELECT id')) return { rows: [{ id: 2, table_name: 'errorDetails' }] };
      if (sql.includes('SELECT EXISTS')) return { rows: [{ exists: true }] };
      if (sql.includes('RETURNING id')) return { rows: [{ id: 2 }] };
      return { rows: [] };
    }), release: jest.fn(),
  };
  mockPool.connect.mockResolvedValue(client);
  const body = { eventType: 'TEST_EVENT', tableName: 'errorDetails', fieldMappings: { ets: 'ets' }, fieldVerification: 'payload' };
  const handler = route('post', '/api/event-processors');
  const first = response();await handler({ body }, first);
  expect(first.status).toHaveBeenCalledWith(201);
  expect(client.query).toHaveBeenCalledWith('COMMIT');
  const second = response();await handler({ body: { ...body, fieldMappings: { ets: 'new_ets' } } }, second);
  expect(second.status).toHaveBeenCalledWith(201);expect(registry.eventProcessors).toHaveLength(1);
  const insert = client.query.mock.calls.find(([sql]) => sql.includes('INSERT INTO event_processors'));
  expect(insert[1][1]).toBe('errorDetails');
});

test('invalid registration reaches no database and a failed manual batch does not return HTTP 200', async () => {
  mockPool.connect.mockClear();
  const invalid = response();
  await route('post', '/api/event-processors')({ body: { eventType: 'TEST', tableName: 'winston_logs', fieldMappings: { ets: 'ets' }, fieldVerification: 'payload' } }, invalid);
  expect(invalid.status).toHaveBeenCalledWith(403);expect(mockPool.connect).not.toHaveBeenCalled();
  mockPool.connect.mockRejectedValueOnce(new Error('unavailable'));
  const failed = response();await route('post', '/api/process-logs')({}, failed);
  expect(failed.status).toHaveBeenCalledWith(500);
});

test('masks URL credentials and private parameters while keeping safe correlation values', () => {
  const address = 'https://test-user:test-password@example.invalid/check?api_key=secret-one&api_key=secret-two&farmer_id=TEST123&transaction_id=trace-safe&latitude=20.741';
  const redacted = redactEndpoint(address);
  for (const secret of ['test-user', 'test-password', 'secret-one', 'secret-two', 'TEST123']) expect(redacted).not.toContain(secret);
  const parsed = new URL(redacted);
  expect(parsed.searchParams.get('transaction_id')).toBe('trace-safe');
  expect(parsed.searchParams.get('latitude')).toBe('20.741');
  expect(parsed.searchParams.getAll('api_key')).toEqual(['[REDACTED_TOKEN]', '[REDACTED_TOKEN]']);
  expect(redactEndpoint('/safe/path?category=chc')).toBe('/safe/path?category=chc');
  expect(redactEndpoint('http://[?api_key=secret')).not.toContain('secret');
});

test('binds SQL NULL for absent payloads and preserves captured empty JSON objects', async () => {
  const client = { query: jest.fn(async sql => sql.includes('INSERT INTO public.external_api_traces')
    ? { rows: [{ id: '00000000-0000-4000-8000-000000000001' }] } : { rows: [] }) };
  const event = { eid: 'OE_ITEM_RESPONSE', mid: 'synthetic-null-payloads', edata: { eks: { trace_scope: 'beckn_external_api' } } };
  expect((await processExternalFlowEvent(client, event)).processed).toBe(true);
  let values = client.query.mock.calls.find(([sql]) => sql.includes('INSERT INTO public.external_api_events'))[1];
  expect([values[24], values[30], values[31]]).toEqual([null, null, null]);
  client.query.mockClear();event.mid = 'synthetic-captured-object';event.edata.eks.request_payload = {};
  await processExternalFlowEvent(client, event);
  values = client.query.mock.calls.find(([sql]) => sql.includes('INSERT INTO public.external_api_events'))[1];
  expect(values[30]).toBe('{}');
});
