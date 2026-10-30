// Explicit local-only integration test. The restored source is always read-only.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { Pool } = require('pg');
const config = { ...require('dotenv').parse(fs.readFileSync(path.join(__dirname, '../.env'))), ...process.env };
assert.ok(['localhost', '127.0.0.1', '::1'].includes(config.DB_HOST), 'Loopback database host required');
const sourceName = 'mh-17-10';
const copyName = `mh-id-check-20261014-${randomUUID().slice(0, 8)}`;
const core = ['winston_logs', 'questions', 'feedback', 'errordetails'];
const tables = [...core, 'dead_letter_logs', 'event_processors', 'external_api_traces', 'external_api_trace_aliases', 'external_api_events'];
const migration = path.join(__dirname, '../migrations/20261018_od3109_core_row_ids');
const statements = name => fs.readFileSync(path.join(migration, name), 'utf8').split(/\n-- audit-statement\n/).map(s => s.trim()).filter(Boolean);
const up = statements('apply.sql'), down = statements('rollback.sql');
const base = { host: config.DB_HOST, port: Number(config.DB_PORT || 5432), user: config.DB_USER,
  password: config.DB_PASSWORD, ssl: false, max: 1, connectionTimeoutMillis: 5000 };
const source = new Pool({ ...base, database: sourceName, options: '-c default_transaction_read_only=on -c statement_timeout=30000 -c application_name=mh-row-id-source-read-only' });
const admin = new Pool({ ...base, database: 'postgres', options: '-c statement_timeout=10000 -c application_name=mh-row-id-copy-creation' });
let copy, c, sourceClient, logger, servicePool;
const quote = x => `"${x.replaceAll('"', '""')}"`;
const quiet = { debug() {}, info() {}, warn() {}, error() {} };
const report = { sourceDatabase: sourceName, testDatabase: copyName, date: '2026-10-14', sourceReadOnly: true,
  production: 'Not checked in production.', sourceBusinessWrites: 0, sourceSchemaChanges: 0, checks: {} };
const response = () => ({ statusCode: 200, status(n) { this.statusCode = n;return this; }, json(body) { this.body = body;return this; } });
const dateQuery = { startDate: '2026-10-14T00:00:00.000', endDate: '2026-10-14T23:59:59.999', granularity: 'daily' };
const etsStart = Date.UTC(2026, 9, 13, 18, 30), etsEnd = Date.UTC(2026, 9, 14, 18, 30);
async function identity(client, expected) {
  const row = (await client.query('SELECT current_database() AS db,inet_server_addr()::text AS address')).rows[0];
  assert.equal(row.db, expected);assert.ok(['127.0.0.1/32', '::1/128'].includes(row.address));
}
async function applyParts(parts) { for (const sql of parts) await c.query(sql); }
async function catalog(client) {
  const columns = (await client.query(`SELECT r.relname,a.attname,a.attnotnull,format_type(a.atttypid,a.atttypmod) AS type,
    pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a JOIN pg_class r ON r.oid=a.attrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE n.nspname='public' AND r.relname=ANY($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY r.relname,a.attnum`, [core])).rows;
  const constraints = (await client.query(`SELECT r.relname,co.conname,co.contype,pg_get_constraintdef(co.oid) AS definition FROM pg_constraint co
    JOIN pg_class r ON r.oid=co.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='public'
    AND r.relname=ANY($1) ORDER BY r.relname,co.conname`, [core])).rows;
  const indexes = (await client.query("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=ANY($1) ORDER BY tablename,indexname", [core])).rows;
  return { columns, constraints, indexes };
}
async function fingerprints(client, sourceOnly = false) {
  const values = {};
  for (const table of core) {
    const filter = !sourceOnly ? '' : table === 'winston_logs'
      ? "WHERE timestamp>=timestamp '2026-10-14' AND timestamp<timestamp '2026-10-15'"
      : `WHERE ets>=${etsStart} AND ets<${etsEnd}`;
    values[table] = (await client.query(`SELECT count(*)::int AS rows,md5(coalesce(string_agg(md5(to_jsonb(t)::text),'' ORDER BY id),'')) AS fingerprint FROM public.${table} t ${filter}`)).rows[0];
  }
  return values;
}
async function expectedSqlError(sql, values, code) {
  await c.query('SAVEPOINT expected_error');let caught;
  try { await c.query(sql, values); } catch (error) { caught = error; }
  await c.query('ROLLBACK TO SAVEPOINT expected_error');await c.query('RELEASE SAVEPOINT expected_error');
  assert.equal(caught?.code, code, `Expected SQLSTATE ${code}`);
}
async function controllerSnapshot() {
  const service = path.resolve(__dirname, '../../oan-telemetry-dashboard-service');
  if (!fs.existsSync(path.join(service, 'controllers/questions.controller.js'))) return null;
  // Optional cross-repository verification: standalone processor copies still run every database/processor check.
  if (!servicePool) {
    Object.assign(process.env, { DB_HOST: base.host, DB_PORT: String(base.port), DB_USER: base.user, DB_PASSWORD: base.password,
      DB_NAME: copyName, DB_POOL_MAX: '1', PGOPTIONS: '-c default_transaction_read_only=on -c timezone=Asia/Kolkata' });
    servicePool = require(path.join(service, 'services/db'));
    await identity(servicePool, copyName);
    assert.equal((await servicePool.query("SELECT current_setting('transaction_read_only') AS value")).rows[0].value, 'on');
  }
  const q = require(path.join(service, 'controllers/questions.controller'));
  const f = require(path.join(service, 'controllers/feedback.controller'));
  const e = require(path.join(service, 'controllers/error.controller'));
  const qId = (await c.query("SELECT id FROM questions WHERE uid IS NOT NULL AND answertext IS NOT NULL AND answertext<>'' ORDER BY id LIMIT 1")).rows[0].id;
  const fId = (await c.query('SELECT id FROM feedback ORDER BY id LIMIT 1')).rows[0].id;
  const eId = (await c.query('SELECT id FROM errordetails WHERE errortext IS NOT NULL ORDER BY id LIMIT 1')).rows[0].id;
  const tasks = [
    ['question-list', q.getQuestions, null], ['feedback-list', f.getAllFeedback, null], ['error-list', e.getAllErrors, null],
    ['question-detail', q.getQuestionById, qId], ['feedback-detail', f.getFeedbackByid, fId], ['error-detail', e.getErrorById, eId],
    ['question-stats', q.getQuestionStats, null], ['feedback-stats', f.getFeedbackStats, null], ['error-stats', e.getErrorStatistics, null],
    ['question-graph', q.getQuestionsGraph, null], ['feedback-graph', f.getFeedbackGraph, null], ['error-graph', e.getErrorGraph, null],
  ];
  const responses = {};
  for (const [name, handler, id] of tasks) {
    const res = response();await handler({ query: { ...dateQuery, sortBy: 'id', sortOrder: 'asc', page: '1', limit: '10' }, params: { id } }, res);
    assert.equal(res.statusCode, 200, `Controller check failed: ${name}`);
    responses[name] = name.endsWith('-list') ? { pagination: res.body.pagination, total: res.body.total } : res.body;
  }
  return responses;
}
async function cloneSchema() {
  const columns = (await sourceClient.query(`SELECT r.relname AS table_name,a.attname AS name,format_type(a.atttypid,a.atttypmod) AS type,
    pg_get_expr(d.adbin,d.adrelid) AS default_expr,a.attnotnull AS not_null,a.attidentity,a.attgenerated
    FROM pg_attribute a JOIN pg_class r ON r.oid=a.attrelid JOIN pg_namespace n ON n.oid=r.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE n.nspname='public' AND r.relname=ANY($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY r.relname,a.attnum`, [tables])).rows;
  assert.equal(new Set(columns.map(row => row.table_name)).size, tables.length);
  const triggers = (await sourceClient.query(`SELECT t.tgname FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='public' AND r.relname=ANY($1) AND NOT t.tgisinternal`, [tables])).rows;
  assert.equal(triggers.length, 0, 'Copy needs an explicit trigger dependency review');
  const sequences = new Map();
  for (const column of columns) {
    assert.equal(column.attidentity, '');assert.equal(column.attgenerated, '');
    const match = column.default_expr?.match(/^nextval\('([^']+)'::regclass\)$/);
    if (match) sequences.set(match[1], column);
  }
  for (const name of sequences.keys()) {
    const seq = (await sourceClient.query('SELECT * FROM pg_sequence WHERE seqrelid=$1::regclass', [name])).rows[0];
    const type = Number(seq.seqtypid) === 20 ? 'bigint' : 'integer';
    await c.query(`CREATE SEQUENCE public.${quote(name.replace(/^public\./, ''))} AS ${type} INCREMENT BY ${seq.seqincrement} MINVALUE ${seq.seqmin} MAXVALUE ${seq.seqmax} START WITH ${seq.seqstart} CACHE ${seq.seqcache} ${seq.seqcycle ? 'CYCLE' : 'NO CYCLE'}`);
  }
  for (const table of tables) {
    const definitions = columns.filter(row => row.table_name === table).map(row => `${quote(row.name)} ${row.type}${row.default_expr ? ` DEFAULT ${row.default_expr}` : ''}${row.not_null ? ' NOT NULL' : ''}`);
    await c.query(`CREATE TABLE public.${quote(table)} (${definitions.join(',')})`);
  }
  const constraints = (await sourceClient.query(`SELECT r.relname AS table_name,co.conname,co.contype,pg_get_constraintdef(co.oid) AS definition
    FROM pg_constraint co JOIN pg_class r ON r.oid=co.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='public' AND r.relname=ANY($1) AND co.contype<>'n' ORDER BY (co.contype='f'),r.relname,co.conname`, [tables])).rows;
  for (const row of constraints) await c.query(`ALTER TABLE public.${quote(row.table_name)} ADD CONSTRAINT ${quote(row.conname)} ${row.definition}`);
  const indexes = (await sourceClient.query(`SELECT pi.indexdef FROM pg_indexes pi JOIN pg_class idx ON idx.relname=pi.indexname
    JOIN pg_namespace n ON n.oid=idx.relnamespace AND n.nspname=pi.schemaname
    WHERE pi.schemaname='public' AND pi.tablename=ANY($1) AND NOT EXISTS(SELECT 1 FROM pg_constraint co WHERE co.conindid=idx.oid)`, [tables])).rows;
  for (const row of indexes) await c.query(row.indexdef);
  for (const [name, col] of sequences) await c.query(`ALTER SEQUENCE public.${quote(name.replace(/^public\./, ''))} OWNED BY public.${quote(col.table_name)}.${quote(col.name)}`);
}
async function copyDayRows() {
  for (const table of [...core, 'event_processors']) {
    const predicate = table === 'event_processors' ? '' : table === 'winston_logs'
      ? "WHERE timestamp>=timestamp '2026-10-14' AND timestamp<timestamp '2026-10-15'"
      : `WHERE ets>=${etsStart} AND ets<${etsEnd}`;
    const rows = (await sourceClient.query(`SELECT row_to_json(t) AS row FROM public.${table} t ${predicate}`)).rows.map(row => row.row);
    for (let i = 0; i < rows.length; i += 250) await c.query(`INSERT INTO public.${table} SELECT * FROM json_populate_recordset(NULL::public.${table},$1::json)`, [JSON.stringify(rows.slice(i, i + 250))]);
    console.log(JSON.stringify({ stage: 'copied-day-rows', table, rows: rows.length }));
  }
  await c.query("SELECT setval('public.event_processors_id_seq',coalesce((SELECT max(id) FROM event_processors),1),true)");
}
async function processorChecks() {
  process.env.LOG_LEVEL = 'error';
  const registry = require('../eventProcessors');logger = require('../logger');logger.silent = true;
  const { createTelemetryBatchProcessor } = require('../telemetryBatch');
  const mappings = (await c.query('SELECT * FROM event_processors WHERE is_active=true ORDER BY id')).rows;
  registry.eventProcessors.length = 0;
  for (const row of mappings) registry._testing.registerProcessor(row.id, row.event_type, row.table_name, row.field_mappings, row.field_verification);
  const wrapper = { query: (sql, values) => c.query(sql, values), release() {} };
  const run = createTelemetryBatchProcessor({ pool: { connect: async () => wrapper }, eventProcessors: registry.eventProcessors,
    getNestedValue: registry.getNestedValue, isExternalFlowEvent: () => false, processExternalFlowEvent: async () => { throw Error('Unexpected external event'); }, batchSize: 50, logger: quiet });
  const synthetic = (kind, extra = {}) => ({ eid: 'OE_ITEM_RESPONSE', mid: randomUUID(), ets: etsStart + 3600000,
    uid: 'local-id-test', sid: 'local-id-session', channel: 'local-test', edata: { eks: { target: { [kind]: kind === 'questionsDetails'
      ? { questionText: 'Local ID test', answerText: { answer: 'Local answer' } } : kind === 'feedbackDetails'
        ? { questionText: 'Local ID test', feedbackType: 'like' } : { errorText: 'Local ID test' } } } }, ...extra });
  const baselineCounts = async () => (await c.query(`SELECT (SELECT count(*)::int FROM questions) AS questions,(SELECT count(*)::int FROM feedback) AS feedback,(SELECT count(*)::int FROM errordetails) AS errors,(SELECT count(*)::int FROM dead_letter_logs) AS failures,(SELECT count(*)::int FROM winston_logs WHERE sync_status=0) AS pending`)).rows[0];
  const stagedRows = [];
  try {
    // This is a small sample of the requested day, not a multi-day replay.
    const rows = (await c.query("SELECT id,message FROM winston_logs WHERE timestamp>=timestamp '2026-10-14' AND timestamp<timestamp '2026-10-15' ORDER BY timestamp,id")).rows;
    const samples = [];
    for (const row of rows) {
      try { const envelope = JSON.parse(row.message);const events = (envelope.events || []).filter(event => mappings.some(rule => event.eid === rule.event_type && registry.getNestedValue(event, rule.field_verification) !== undefined));
        if (events.length) samples.push({ message: JSON.stringify({ events }), events: events.length });
      } catch (_) {}
      if (samples.length === 30) break;
    }
    assert.equal(samples.length, 30);
    const repeated = synthetic('questionsDetails');
    const checks = [synthetic('questionsDetails', { ets: 'invalid-epoch' }), synthetic('questionsDetails'), synthetic('feedbackDetails'), synthetic('errorDetails'), repeated, repeated, { eid: 'OE_START' }, { eid: 'OE_END' }];
    const messages = [...samples.map(row => row.message), JSON.stringify({ events: checks })];
    const pendingBefore = (await c.query('SELECT id FROM winston_logs WHERE sync_status=0')).rows.map(row => row.id);
    await c.query('BEGIN');
    for (const id of pendingBefore) await c.query('UPDATE winston_logs SET sync_status=1 WHERE id=$1', [id]);
    await c.query('CREATE TEMP TABLE replay_baseline_questions AS TABLE questions');
    await c.query('CREATE TEMP TABLE replay_baseline_feedback AS TABLE feedback');
    await c.query('CREATE TEMP TABLE replay_baseline_errors AS TABLE errordetails');
    for (const message of messages) stagedRows.push((await c.query('INSERT INTO winston_logs(message,sync_status) VALUES($1,0) RETURNING id', [message])).rows[0].id);
    await c.query('COMMIT');const before = await baselineCounts();
    const result = await run('row-id-keys-day-sample');assert.equal(result.status, 'success');
    const expectedSaved = samples.reduce((n, row) => n + row.events, 0) + 5;
    assert.equal(result.eventsProcessed, expectedSaved);assert.equal(result.deadLetterCount, 1);assert.equal(result.ignoredEvents, 2);
    const after = await baselineCounts();assert.equal(after.pending, 0);assert.equal(after.questions + after.feedback + after.errors - before.questions - before.feedback - before.errors, expectedSaved);
    assert.equal(Number((await c.query("SELECT count(*) FROM questions WHERE uid='local-id-test'")).rows[0].count), 3);
    report.checks.processor = { rawDaySamples: samples.length, syntheticRawLogs: 1, savedEvents: result.eventsProcessed,
      ignoredEvents: result.ignoredEvents, rejectedBadEvents: result.deadLetterCount, pendingAfter: after.pending,
      sameMidSavedTwice: true, genericQuestionFeedbackErrorFlow: true, badEventDidNotBlockGoodEvents: true };
    // Remove only test-copy inserts; the restored source is never touched.
    await c.query('BEGIN');
    await c.query('DELETE FROM questions WHERE id NOT IN(SELECT id FROM replay_baseline_questions)');
    await c.query('DELETE FROM feedback WHERE id NOT IN(SELECT id FROM replay_baseline_feedback)');
    await c.query('DELETE FROM errordetails WHERE id NOT IN(SELECT id FROM replay_baseline_errors)');
    await c.query('DELETE FROM dead_letter_logs');
    await c.query('DELETE FROM winston_logs WHERE id=ANY($1::uuid[])', [stagedRows]);
    if (pendingBefore.length) await c.query('UPDATE winston_logs SET sync_status=0 WHERE id=ANY($1::uuid[])', [pendingBefore]);
    await c.query('DROP TABLE replay_baseline_questions,replay_baseline_feedback,replay_baseline_errors');await c.query('COMMIT');
  } catch (error) { await c.query('ROLLBACK').catch(() => {});throw error; }
}
(async () => {
  try {
    sourceClient = await source.connect();await identity(sourceClient, sourceName);await sourceClient.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    assert.equal((await sourceClient.query("SELECT current_setting('transaction_read_only') AS value")).rows[0].value, 'on');
    await identity(admin, 'postgres');assert.match(copyName, /^mh-id-check-20261014-[a-f0-9]{8}$/);assert.notEqual(copyName, sourceName);
    await admin.query(`CREATE DATABASE ${quote(copyName)} TEMPLATE template0`);
    copy = new Pool({ ...base, database: copyName, options: '-c timezone=Asia/Kolkata -c statement_timeout=30000 -c lock_timeout=2000 -c application_name=mh-row-id-isolated-copy' });
    c = await copy.connect();await identity(c, copyName);
    await cloneSchema();await copyDayRows();
    const sourceDay = await fingerprints(sourceClient, true), before = await fingerprints(c), beforeCatalog = await catalog(c);
    assert.deepEqual(before, sourceDay);assert.deepEqual(beforeCatalog, await catalog(sourceClient));report.copiedRows = Object.fromEntries(core.map(table => [table, before[table].rows]));
    const reportsBefore = await controllerSnapshot();
    console.log(JSON.stringify({ stage: 'apply-migration', testDatabase: copyName }));await applyParts(up);
    const after = await catalog(c);assert.equal(after.constraints.filter(row => row.contype === 'p').length, 4);
    assert.ok(after.columns.filter(row => row.attname === 'id').every(row => row.attnotnull && row.type === 'uuid' && row.default_expr === 'gen_random_uuid()'));
    assert.deepEqual(await fingerprints(c), before);report.checks.existingDataUnchanged = true;
    await c.query('BEGIN');
    for (const table of core) {
      const old = (await c.query(`SELECT id FROM ${table} LIMIT 1`)).rows[0].id;
      const id = (await c.query(`INSERT INTO ${table} DEFAULT VALUES RETURNING id`)).rows[0].id;assert.match(id, /^[a-f0-9-]{36}$/);assert.notEqual(id, old);
      await expectedSqlError(`INSERT INTO ${table}(id) VALUES($1)`, [old], '23505');
      await expectedSqlError(`INSERT INTO ${table}(id) VALUES(NULL)`, [], '23502');
      await expectedSqlError(`UPDATE ${table} SET id=$1 WHERE id=$2`, [old, id], '23505');
      await expectedSqlError(`UPDATE ${table} SET id=NULL WHERE id=$1`, [id], '23502');
    }
    await c.query('ROLLBACK');report.checks.allFourTablesRejectEmptyAndRepeatedIds = true;report.checks.defaultUuidGeneration = true;
    const reportsAfter = await controllerSnapshot();assert.deepEqual(reportsAfter, reportsBefore);
    report.checks.serviceReports = reportsBefore ? { controllersCompared: Object.keys(reportsBefore).length, unchanged: true, detailsUnchanged: true,
      questionRowsReported: reportsBefore['question-list'].pagination.totalItems, feedbackRowsReported: reportsBefore['feedback-list'].pagination.totalItems, errorRowsReported: reportsBefore['error-list'].total } : { optionalWorkspaceCheck: 'Service repository not present; SQL fingerprints still checked' };
    await processorChecks();assert.deepEqual(await fingerprints(c), before);
    console.log(JSON.stringify({ stage: 'rollback-migration' }));await applyParts(down);assert.deepEqual(await catalog(c), beforeCatalog);assert.deepEqual(await fingerprints(c), before);
    assert.deepEqual(await controllerSnapshot(), reportsBefore);report.checks.rollbackRestoresSchemaAndData = true;
    // Test recovery from interruptions after a guard, an index, and a completed first table.
    for (const cut of [4, 6, 7]) {
      await applyParts(up.slice(0, cut));await applyParts(down);
      assert.deepEqual(await catalog(c), beforeCatalog);assert.deepEqual(await fingerprints(c), before);
    }
    report.checks.partialApplyRollback = ['guard added', 'unique index built', 'first table completed'];
    // A failed concurrent build leaves an invalid index; rollback must remove it too.
    await applyParts(up.slice(0, 5));
    const duplicateCopy = (await c.query('INSERT INTO winston_logs(id) SELECT id FROM winston_logs LIMIT 1 RETURNING ctid::text AS row_location')).rows[0].row_location;
    let indexError;try { await c.query(up[5]); } catch (error) { indexError = error; }
    assert.equal(indexError?.code, '23505');
    assert.equal((await c.query("SELECT indisvalid FROM pg_index WHERE indexrelid='public.od3109_winston_logs_id_pk'::regclass")).rows[0].indisvalid, false);
    await applyParts(down);
    await c.query('DELETE FROM winston_logs WHERE ctid=$1::tid', [duplicateCopy]);
    assert.deepEqual(await catalog(c), beforeCatalog);assert.deepEqual(await fingerprints(c), before);
    report.checks.failedConcurrentIndexRollback = true;
    const preflight = up.find(sql => sql.startsWith('DO $preflight$'));
    for (const mode of ['empty', 'repeated']) {
      await c.query('BEGIN');
      if (mode === 'empty') await c.query('INSERT INTO questions(id) VALUES(NULL)');
      else await c.query('INSERT INTO questions(id) SELECT id FROM questions LIMIT 1');
      let error;try { await c.query(preflight); } catch (e) { error = e; }
      await c.query('ROLLBACK');assert.equal(error?.code, 'P0001');assert.deepEqual(await catalog(c), beforeCatalog);
    }
    report.checks.preflightRejectsBadExistingIds = true;
    // Leave the isolated copy with the verified protection applied for local inspection.
    await applyParts(up);assert.deepEqual(await fingerprints(c), before);
    assert.deepEqual(await fingerprints(sourceClient, true), sourceDay);assert.deepEqual(await catalog(sourceClient), beforeCatalog);
    report.checks.sourceUnchanged = true;report.checks.finalTestCopyHasFourPrimaryKeys = true;
    report.success = true;
  } catch (error) {
    if (c) await c.query('ROLLBACK').catch(() => {});
    report.success = false;report.error = { code: error.code || error.name, message: error.message };process.exitCode = 1;
  } finally {
    if (sourceClient) { await sourceClient.query('ROLLBACK').catch(() => {});sourceClient.release(); }
    if (c) c.release();if (logger) logger.close();
    await Promise.allSettled([source.end(), admin.end(), copy?.end(), servicePool?.end()]);
    if (process.env.LOCAL_TEST_REPORT) fs.writeFileSync(process.env.LOCAL_TEST_REPORT, JSON.stringify(report, null, 2), { mode: 0o600 });
    console.log(JSON.stringify(report));
  }
})();
