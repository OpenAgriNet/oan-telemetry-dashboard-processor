// Explicit local-only integration command. Never imports the app entry point,
// starts schedules or HTTP, or writes restored public business tables.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const vm = require('node:vm');
const { Pool } = require('pg');
const dotenv = require('dotenv');
const root = path.resolve(__dirname, '..');
const config = { ...dotenv.parse(fs.readFileSync(path.join(root, '.env'))), ...process.env };
if (!['localhost', '127.0.0.1', '::1'].includes(config.DB_HOST)) {
  throw new Error('Integration tests require an explicitly local DB_HOST');
}
process.env.LOG_LEVEL = 'error';
const registry = require('../eventProcessors');
const external = require('../externalFlowProcessor');
const { redactSensitiveData } = require('../externalFlowPii');
const { createTelemetryBatchProcessor } = require('../telemetryBatch');
const logger = require('../logger');
const quiet = { debug() {}, info() {}, warn() {}, error() {} };
const pool = new Pool({
  host: config.DB_HOST, port: Number(config.DB_PORT || 5432),
  database: 'mh-17-10', user: config.DB_USER, password: config.DB_PASSWORD,
  ssl: false, max: 1, connectionTimeoutMillis: 5000,
  application_name: 'mh-audit-batch-integration-local-only',
  options: '-c timezone=UTC -c statement_timeout=60000 -c lock_timeout=2000',
});
const results = { database: 'mh-17-10', branch: 'major-fix/OD-3109-audit-fixes', cases: {}, publicBusinessWrites: 0 };
const tables = ['winston_logs', 'dead_letter_logs', 'questions', 'feedback', 'errordetails',
  'external_api_traces', 'external_api_trace_aliases', 'external_api_events'];

(async () => {
  let connection;
  try {
    connection = await pool.connect();
    const identity = (await connection.query('SELECT current_database() AS db, inet_server_addr()::text AS address')).rows[0];
    assert.equal(identity.db, 'mh-17-10');
    assert.ok(['127.0.0.1/32', '::1/128'].includes(identity.address));
    for (const table of tables) {
      // Copy current dump structure and indexes, not any business rows.
      await connection.query(`CREATE TEMP TABLE ${table} (LIKE public.${table} INCLUDING ALL)`);
    }
    // The copied BIGSERIAL default must not advance the restored public sequence.
    await connection.query('CREATE TEMP SEQUENCE audit_external_event_ids');
    await connection.query("ALTER TABLE pg_temp.external_api_events ALTER COLUMN id SET DEFAULT nextval('pg_temp.audit_external_event_ids')");
    const client = {
      async query(sql, values) {
        const isolated = sql.replace(/public\.(external_api_(?:traces|trace_aliases|events))/g, 'pg_temp.$1');
        assert.ok(!/\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|ALTER\s+TABLE|TRUNCATE)\s+public\./i.test(isolated), 'Public writes are forbidden');
        return connection.query(isolated, values);
      },
      release() {},
    };
    const localPool = { connect: async () => client };
    const mappings = (await connection.query('SELECT * FROM public.event_processors WHERE is_active=true ORDER BY id')).rows;
    registry.eventProcessors.length = 0;
    for (const mapping of mappings) registry._testing.registerProcessor(mapping.id, mapping.event_type,
      mapping.table_name, mapping.field_mappings, mapping.field_verification);
    const dependencies = {
      pool: localPool, eventProcessors: registry.eventProcessors,
      getNestedValue: registry.getNestedValue,
      isExternalFlowEvent: external.isExternalFlowEvent,
      processExternalFlowEvent: external.processExternalFlowEvent,
      batchSize: 500, logger: quiet,
    };
    const run = createTelemetryBatchProcessor(dependencies);
    const baselineSource = execFileSync('git', ['show', 'major-fix/OD-3109:index.js'], { cwd: root, encoding: 'utf8' });
    const baselineFunctions = baselineSource.slice(baselineSource.indexOf('// Parse telemetry message JSON'),
      baselineSource.indexOf('// question\n// Process OE_ITEM_RESPONSE data'));
    const baseline = vm.runInNewContext(`${baselineFunctions}\nprocessTelemetryLogs`, {
      ...dependencies, pool: localPool, logger: quiet, BATCH_SIZE: 500,
      redactSensitiveData, Date,
    });
    async function clear() {
      await connection.query(`TRUNCATE ${tables.map(t => `pg_temp.${t}`).join(', ')}`);
    }
    async function add(eventsOrMessage) {
      const message = typeof eventsOrMessage === 'string' ? eventsOrMessage : JSON.stringify({ events: eventsOrMessage });
      const id = randomUUID();
      await connection.query('INSERT INTO pg_temp.winston_logs(id,level,message,meta,"timestamp",sync_status) VALUES($1,$2,$3,$4,$5,0)',
        [id, 'info', message, {}, new Date()]);
      return id;
    }
    async function counts() {
      const row = (await connection.query(`SELECT
        (SELECT count(*)::int FROM pg_temp.questions) AS questions,
        (SELECT count(*)::int FROM pg_temp.dead_letter_logs) AS failures,
        (SELECT count(*)::int FROM pg_temp.winston_logs WHERE sync_status=0) AS pending`)).rows[0];
      return row;
    }
    const question = (ets = 1792150690444) => ({
      eid: 'OE_ITEM_RESPONSE', mid: `AUDIT_TEST_${randomUUID()}`, uid: 'synthetic-user',
      sid: 'synthetic-session', channel: 'test', ets,
      edata: { eks: { target: { questionsDetails: {
        questionText: 'Synthetic local question', answerText: { answer: 'Synthetic answer' }, questionSource: 'local-test',
      } } } },
    });

    // Reproduce the old behavior and compare it with the fix on the same schema.
    await clear(); await add([question('not-an-epoch'), question()]);
    const oldBad = await baseline('baseline-bad-then-good');
    const oldBadCounts = await counts();
    assert.equal(oldBad.status, 'error'); assert.equal(oldBadCounts.questions, 0);
    await clear(); const badRawId = await add([question('not-an-epoch'), question()]);
    const fixedBad = await run('fixed-bad-then-good');
    assert.equal(fixedBad.status, 'success');
    assert.deepEqual(await counts(), { questions: 1, failures: 1, pending: 0 });
    const failure = (await connection.query('SELECT meta FROM pg_temp.dead_letter_logs')).rows[0];
    assert.equal(failure.meta.raw_log_id, badRawId);assert.equal(failure.meta.raw_event_index, 1);
    assert.equal(failure.meta.failure_code, '22P02');
    results.cases.bad_event_then_good = { baseline: oldBadCounts, fixed: await counts(), counters: fixedBad };

    await clear(); await add('{ unreadable private input');
    await baseline('baseline-malformed');
    const oldParse = await counts(); assert.equal(oldParse.failures, 0);assert.equal(oldParse.pending, 0);
    await clear(); await add('{ unreadable private input');
    const fixedParse = await run('fixed-malformed');
    assert.deepEqual(await counts(), { questions: 0, failures: 1, pending: 0 });
    const stored = (await connection.query('SELECT message,meta FROM pg_temp.dead_letter_logs')).rows[0];
    assert.ok(!stored.message.includes('private input')); assert.ok(stored.meta.raw_log_id);
    results.cases.malformed_message = { baseline: oldParse, fixed: await counts(), counters: fixedParse };

    await clear(); await add([question(), { eid: 'UNKNOWN', mid: 'test-unknown' }]);
    await connection.query('ALTER TABLE pg_temp.dead_letter_logs ADD CONSTRAINT audit_reject_failure CHECK(false) NOT VALID');
    const storeFailure = await run('failure-store-unavailable');
    assert.equal(storeFailure.status, 'error');assert.equal(storeFailure.eventsProcessed, 0);
    assert.deepEqual(await counts(), { questions: 0, failures: 0, pending: 1 });
    await connection.query('ALTER TABLE pg_temp.dead_letter_logs DROP CONSTRAINT audit_reject_failure');
    results.cases.failure_store_error_rolls_back_batch = true;

    await clear(); await add([question(), question()]);
    let calls = 0;
    const unavailable = createTelemetryBatchProcessor({ ...dependencies, eventProcessors: [{
      eventType: 'OE_ITEM_RESPONSE', fieldVerification: 'edata.eks.target.questionsDetails',
      async process(c, event) {
        if (++calls === 2) throw Object.assign(new Error('simulated connection interruption'), { code: '08006' });
        return registry.eventProcessors[0].process(c, event);
      },
    }] });
    assert.equal((await unavailable('connection-failure')).status, 'error');
    assert.deepEqual(await counts(), { questions: 0, failures: 0, pending: 1 });
    results.cases.connection_error_keeps_input_pending = true;

    await clear(); const repeated = question();
    await add([{ eid: 'OE_START' }, repeated, repeated, { eid: 'OE_END' }]);
    const unchanged = await run('generic-repeat-policy-unchanged');
    assert.equal(unchanged.eventsProcessed, 2);assert.equal(unchanged.ignoredEvents, 2);
    assert.equal((await counts()).questions, 2);
    results.cases.generic_repeat_policy_unchanged = true;

    await clear();
    const api = { eid: 'OE_ITEM_RESPONSE', mid: `AUDIT_API_${randomUUID()}`, ets: 1792150690444,
      edata: { eks: { trace_scope: 'beckn_external_api', event_name: 'provider_webservice_call',
        service: 'mh-provider-backend', layer: 'provider', http_call_id: randomUUID(),
        endpoint: '/synthetic-local-api', method: 'POST', outcome: 'success', duration_ms: 20 } } };
    await add([{ eid: 'OE_START' }, api, api, { eid: 'OE_END' }]);
    const externalCounts = await run('external-repeat-counters');
    assert.equal(externalCounts.eventsProcessed, 1); assert.equal(externalCounts.duplicateEvents, 1);
    assert.equal(externalCounts.ignoredEvents, 2); assert.equal(externalCounts.eventsSeen, 4);
    assert.equal(Number((await connection.query('SELECT count(*) FROM pg_temp.external_api_events')).rows[0].count), 1);
    results.cases.external_repeat_counters = externalCounts;

    // Replay recent real dump deliveries into temporary reporting tables only.
    await clear();
    await connection.query(`INSERT INTO pg_temp.winston_logs(id,level,message,meta,"timestamp",sync_status)
      SELECT id,level,message,meta,"timestamp",0 FROM public.winston_logs WHERE sync_status=1
      ORDER BY "timestamp" DESC,id DESC LIMIT 21310`);
    const initial = Number((await connection.query('SELECT count(*) FROM pg_temp.winston_logs')).rows[0].count);
    assert.ok(initial > 0, 'Dump contains no raw deliveries');
    const totals = { processed: 0, eventsSeen: 0, eventsProcessed: 0, duplicateEvents: 0,
      ignoredEvents: 0, deadLetterEvents: 0, malformedLogs: 0, deadLetterCount: 0 };
    let batches = 0;
    while (true) {
      const result = await run(`dump-temp-replay-${++batches}`);
      assert.equal(result.status, 'success', `Replay failed: ${result.errorCode}`);
      for (const key of Object.keys(totals)) totals[key] += result[key];
      if (batches % 10 === 0) console.log(JSON.stringify({ stage: 'temporary-dump-replay', batches, processed: totals.processed }));
      if (result.processed === 0) break;
      assert.ok(batches <= Math.ceil(initial / 500) + 1, 'Replay did not drain');
    }
    assert.equal(totals.processed, initial);
    assert.equal(totals.eventsSeen, totals.eventsProcessed + totals.duplicateEvents + totals.ignoredEvents + totals.deadLetterEvents);
    assert.equal((await counts()).pending, 0);
    const reporting = (await connection.query(`SELECT
      (SELECT count(*)::int FROM pg_temp.questions) AS questions,
      (SELECT count(*)::int FROM pg_temp.feedback) AS feedback,
      (SELECT count(*)::int FROM pg_temp.errordetails) AS errors,
      (SELECT count(*)::int FROM pg_temp.external_api_events) AS external_events`)).rows[0];
    results.cases.restored_dump_replay = { inputLogs: initial, nonemptyBatches: batches - 1, counters: totals, reportingRows: reporting };
    assert.equal(totals.deadLetterCount, 0, 'Unexpected failure record in previously processed real dump sample');
    results.success = true;
  } catch (error) {
    results.success = false;results.error = { code: error.code || error.name, message: error.message };
    process.exitCode = 1;
  } finally {
    if (connection) { await connection.query('ROLLBACK').catch(() => {}); connection.release(); }
    await pool.end();logger.close();
    const target = process.env.LOCAL_TEST_REPORT;
    if (target) fs.writeFileSync(target, JSON.stringify(results, null, 2), { mode: 0o600 });
    console.log(JSON.stringify(results));
  }
})();
