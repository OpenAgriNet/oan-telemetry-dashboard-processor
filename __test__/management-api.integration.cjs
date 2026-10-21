const assert = require('node:assert/strict');
const { once } = require('node:events');

// No connected database is used: HTTP is bound to loopback and every pool
// connection is replaced with a deterministic local test client.
process.env.LOG_LEVEL = 'error';
process.env.PROCESSOR_MANAGEMENT_API_KEY = 'synthetic-local-management-key';
const { app, pool } = require('../index');
const cron = require('node-cron');
const logger = require('../logger');
let connections = 0;
let unavailable = false;
const client = {
  async query(sql) {
    if (sql.includes('SELECT EXISTS')) return { rows: [{ exists: true }] };
    if (sql.includes('RETURNING id')) return { rows: [{ id: 1 }] };
    return { rows: [], command: sql };
  }, release() {},
};
pool.connect = async () => {
  connections++;
  if (unavailable) throw new Error('Synthetic database outage');
  return client;
};
let server;
(async () => {
  try {
    assert.equal(cron.getTasks().size, 0, 'Importing must not start scheduled jobs');
    server = app.listen(0, '127.0.0.1');await once(server, 'listening');
    const base = `http://127.0.0.1:${server.address().port}`;
    const header = { Authorization: 'Bearer synthetic-local-management-key', 'Content-Type': 'application/json' };
    assert.equal((await fetch(`${base}/health`)).status, 200);
    for (const [method, url] of [['GET', '/api/event-processors'], ['POST', '/api/process-logs'],
      ['POST', '/api/event-processors'], ['POST', '/api/refresh-leaderboard']]) {
      assert.equal((await fetch(`${base}${url}`, { method })).status, 401);
    }
    assert.equal(connections, 0);
    const body = { eventType: 'LOCAL_TEST_EVENT', tableName: 'questions', fieldMappings: { ets: 'ets' }, fieldVerification: 'payload' };
    assert.equal((await fetch(`${base}/api/event-processors`, { method: 'POST', headers: header,
      body: JSON.stringify({ ...body, tableName: 'winston_logs' }) })).status, 403);
    assert.equal(connections, 0);
    const registered = await fetch(`${base}/api/event-processors`, { method: 'POST', headers: header, body: JSON.stringify(body) });
    assert.equal(registered.status, 201);
    const rules = await (await fetch(`${base}/api/event-processors`, { headers: header })).json();
    assert.equal(rules.processors[0].eventType, 'LOCAL_TEST_EVENT');
    const processed = await fetch(`${base}/api/process-logs`, { method: 'POST', headers: header });
    assert.equal(processed.status, 200);assert.equal((await processed.json()).processed, 0);
    unavailable = true;
    assert.equal((await fetch(`${base}/api/process-logs`, { method: 'POST', headers: header })).status, 500);
    delete process.env.PROCESSOR_MANAGEMENT_API_KEY;
    assert.equal((await fetch(`${base}/api/event-processors`, { headers: header })).status, 503);
    console.log(JSON.stringify({ success: true, loopbackOnly: true, realDatabaseConnections: 0,
      checks: ['public health', 'management authorization', 'table allowlist', 'registration',
        'configured rule response', 'empty batch', 'failed batch status', 'unset-key rejection', 'no import-time jobs'] }));
  } catch (error) {
    console.error(error.message);process.exitCode = 1;
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    await pool.end();logger.close();
  }
})();
