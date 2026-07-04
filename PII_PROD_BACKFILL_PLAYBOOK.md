# PII Production Backfill Playbook

This playbook replicates the `bh-dev-2` cleanup flow on production. `bh-dev-2` was a prod replica, so use the same order: confirm `qid`, install DB-side PII functions, run the SQL column-level PII backfill twice, then verify zero remaining maskable text rows.

For the scripted version of these steps, use:

```bash
DB_USER='<prod_user>' DB_HOST='<prod_host>' DB_NAME='<prod_db>' DB_PASSWORD='<prod_password>' DB_PORT='<prod_port>' DB_SSL=true \
I_UNDERSTAND_PROD_BACKFILL=true npm run backfill:prod:qid-pii
```

For a non-mutating check first:

```bash
DB_USER='<prod_user>' DB_HOST='<prod_host>' DB_NAME='<prod_db>' DB_PASSWORD='<prod_password>' DB_PORT='<prod_port>' DB_SSL=true \
npm run backfill:prod:qid-pii -- --dry-run
```

## 0. Safety Setup

Run this from the processor repo on the machine/container that can reach prod DB:

```bash
cd /usr/src/app
# or local checkout:
# cd "/Users/shashankanil/Documents/OAN(open agri net)/oan-telemetry-dashboard-processor"
```

Set prod DB env vars. Replace placeholders with prod values:

```bash
export DB_USER='<prod_user>'
export DB_HOST='<prod_host>'
export DB_NAME='<prod_db>'
export DB_PASSWORD='<prod_password>'
export DB_PORT='<prod_port>'
export DB_SSL=true
```

Before any mutation, take a DB snapshot/backup using the normal production backup process.

Recommended: stop or pause telemetry ingestion while the backfill runs, or at least run during low traffic. The scripts are idempotent, but pausing ingest avoids newly inserted rows arriving mid-run.

## 1. Confirm Required Code Is Deployed

These files must exist on the prod processor branch/container:

```bash
ls migrations/20260624_pii_candidate_function.sql
ls scripts/backfill-pii-sql-column.js
ls scripts/backfill-questions-qid.js
```

Check scripts compile:

```bash
node -c scripts/backfill-pii-sql-column.js
node -c scripts/backfill-questions-qid.js
node -c index.js
```

## 2. Confirm `qid` Backfill State

Run this first because future debugging depends on `questions.qid` existing and being indexed:

```bash
node - <<'NODE'
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  const result = await pool.query(`
    SELECT
      COUNT(*)::bigint AS total,
      COUNT(*) FILTER (WHERE qid IS NOT NULL)::bigint AS qid_filled,
      COUNT(*) FILTER (WHERE qid IS NULL)::bigint AS qid_missing
    FROM public.questions
  `);
  console.log(result.rows[0]);
  await pool.end();
})().catch((err) => { console.error(err); process.exit(1); });
NODE
```

Expected from `bh-dev-2`-like prod replica:

```text
qid_filled should be around 98%
qid_missing should mostly be empty-question/test/unmatchable rows
```

If `qid` column is missing or mostly empty, run:

```bash
node scripts/backfill-questions-qid.js --dry-run
node scripts/backfill-questions-qid.js
```

Then ensure the qid index:

```bash
node - <<'NODE'
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_questions_qid ON public.questions(qid) WHERE qid IS NOT NULL`);
  await pool.end();
})();
NODE
```

## 3. Install DB-Side PII Regex Functions

This creates/updates:

- `public.contains_pii_candidate(text)`
- `public.mask_pii_text(text)`

```bash
node - <<'NODE'
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  const sql = fs.readFileSync(path.join(process.cwd(), 'migrations/20260624_pii_candidate_function.sql'), 'utf8');
  await pool.query(sql);
  const check = await pool.query(`
    SELECT
      to_regprocedure('public.contains_pii_candidate(text)') IS NOT NULL AS has_candidate_fn,
      to_regprocedure('public.mask_pii_text(text)') IS NOT NULL AS has_mask_fn,
      public.contains_pii_candidate('call me at 9876543210') AS phone_detected,
      public.mask_pii_text('call me at 9876543210') AS masked_sample
  `);
  console.log(check.rows[0]);
  await pool.end();
})().catch((err) => { console.error(err); process.exit(1); });
NODE
```

Expected:

```text
has_candidate_fn: true
has_mask_fn: true
phone_detected: true
masked_sample: call me at [REDACTED_PHONE]
```

## 4. Preflight Audit

This reports rows where `mask_pii_text()` would still change text columns:

```bash
node - <<'NODE'
const { Pool } = require('pg');
const targets = [
  { table: 'messages', cols: ['content'] },
  { table: 'questions', cols: ['questiontext', 'answer', 'answertext', 'groupdetails'] },
  { table: 'feedback', cols: ['feedbacktext', 'questiontext', 'answertext', 'groupdetails'] },
  { table: 'errordetails', cols: ['errormessage', 'errortext'] },
];
function qi(name) { return `"${name.replace(/"/g, '""')}"`; }
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  const client = await pool.connect();
  let totalRemaining = 0;
  try {
    for (const target of targets) {
      const exists = await client.query('SELECT to_regclass($1) AS reg', [`public.${target.table}`]);
      if (!exists.rows[0].reg) continue;
      const cols = await client.query(
        `SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`,
        [target.table]
      );
      const colMap = new Map(cols.rows.map((r) => [r.column_name, r.udt_name]));
      for (const col of target.cols) {
        const type = colMap.get(col);
        if (!type || (type !== 'text' && type !== 'varchar')) continue;
        const result = await client.query(`
          SELECT COUNT(*)::bigint AS remaining
          FROM public.${qi(target.table)}
          WHERE ${qi(col)} IS NOT NULL
            AND public.contains_pii_candidate(${qi(col)}::text)
            AND ${qi(col)}::text IS DISTINCT FROM public.mask_pii_text(${qi(col)}::text)
        `);
        const remaining = Number(result.rows[0].remaining || 0);
        totalRemaining += remaining;
        console.log(`${target.table}.${col}: remaining=${remaining}`);
      }
    }
    console.log(`TOTAL_REMAINING=${totalRemaining}`);
  } finally {
    client.release();
    await pool.end();
  }
})().catch((err) => { console.error(err); process.exit(1); });
NODE
```

For `bh-dev-2`, this was initially non-zero, mostly:

```text
messages.content
questions.questiontext
questions.answertext
feedback.feedbacktext
feedback.questiontext
feedback.answertext
```

## 5. Trial Run On 100 Rows

Run one limited production trial first:

```bash
node scripts/backfill-pii-sql-column.js --only=questions.answertext --limit=100 --batch-size=100
```

Expected shape:

```text
[stage] questions.answertext: candidates=100 elapsed=...
[progress] questions.answertext: updated=100/100 ...
[done] questions.answertext: updated=100 ...
```

If this fails, stop here and inspect the error before running the full job.

## 6. Full SQL Column-Level Backfill

Run the full text-column backfill:

```bash
node scripts/backfill-pii-sql-column.js --batch-size=5000
```

On `bh-dev-2`, this updated:

```text
209,488 rows in 29m 37s
```

Expected largest columns:

```text
messages.content
questions.questiontext
questions.answertext
feedback.questiontext
feedback.answertext
```

The script skips JSONB by default. In our local run, `feedback.groupdetails` was JSONB and skipped. Earlier audit showed no remaining candidates there.

## 7. Run A Second Pass

Run it again:

```bash
node scripts/backfill-pii-sql-column.js --batch-size=5000
```

Why: after first-pass redaction, adjacent values can become newly matchable, for example:

```text
[REDACTED_PHONE]/8460136593
```

On `bh-dev-2`, second pass updated:

```text
78 rows in 2m 6s
```

## 8. Final Verification

Repeat the preflight audit from step 4.

Success condition:

```text
messages.content: remaining=0
questions.questiontext: remaining=0
questions.answer: remaining=0
questions.answertext: remaining=0
questions.groupdetails: remaining=0
feedback.feedbacktext: remaining=0
feedback.questiontext: remaining=0
feedback.answertext: remaining=0
errordetails.errortext: remaining=0
TOTAL_REMAINING=0
```

If `TOTAL_REMAINING` is not zero, run one more pass and audit again. If the same rows remain, pull samples:

```bash
node - <<'NODE'
const { Pool } = require('pg');
const table = 'questions';
const col = 'questiontext';
function qi(name) { return `"${name.replace(/"/g, '""')}"`; }
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  const result = await pool.query(`
    SELECT id::text, ${qi(col)}::text AS raw, public.mask_pii_text(${qi(col)}::text) AS masked
    FROM public.${qi(table)}
    WHERE ${qi(col)} IS NOT NULL
      AND public.contains_pii_candidate(${qi(col)}::text)
      AND ${qi(col)}::text IS DISTINCT FROM public.mask_pii_text(${qi(col)}::text)
    LIMIT 20
  `);
  console.log(result.rows);
  await pool.end();
})().catch((err) => { console.error(err); process.exit(1); });
NODE
```

Change `table` and `col` in the snippet for whichever column still reports remaining rows.

## 9. Optional JSONB Pass

Only use this if the audit shows JSONB columns still contain candidates and you accept text-to-jsonb rewrite behavior:

```bash
node scripts/backfill-pii-sql-column.js --include-jsonb --only=feedback.groupdetails --batch-size=1000
```

Do not run JSONB mode casually. Keep it column-specific.

## 10. Post-Run Checks

Confirm qid is still populated:

```bash
node - <<'NODE'
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  const result = await pool.query(`
    SELECT
      COUNT(*)::bigint AS total,
      COUNT(*) FILTER (WHERE qid IS NOT NULL)::bigint AS qid_filled,
      COUNT(*) FILTER (WHERE qid IS NULL)::bigint AS qid_missing
    FROM public.questions
  `);
  console.log(result.rows[0]);
  await pool.end();
})();
NODE
```

Spot-check recent app reads/API pages and confirm redaction markers appear instead of raw IDs/phones.

## Operational Notes

- The SQL functions are idempotent via `CREATE OR REPLACE FUNCTION`.
- The SQL backfill is idempotent: reruns only update rows where `mask_pii_text()` would still change the value.
- Production should run the backfill before opening a PR/release verification if the DB snapshot is used for QA.
- Keep the JS masking middleware enabled after this. The SQL backfill cleans historical rows; app-side masking protects future writes and reads.
