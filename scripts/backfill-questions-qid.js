#!/usr/bin/env node
/**
 * Backfill qid into public.questions from telemetry payloads stored in winston_logs.
 *
 * Strategy:
 * 1. Add questions.qid if missing.
 * 2. Parse OE_ITEM_RESPONSE events from winston_logs.message JSON.
 * 3. Build an exact-match staging set on (sid, ets, questiontext) where qid is unambiguous.
 * 4. Fill remaining rows with a safer fallback on (sid, ets) only when qid is still unique.
 * 5. Create an index on questions.qid for lookup speed after the backfill.
 *
 * Usage:
 *   node scripts/backfill-questions-qid.js
 *   node scripts/backfill-questions-qid.js --dry-run
 */

const path = require("path");
const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const dryRun = process.argv.includes("--dry-run");
const HEARTBEAT_MS = parseInt(process.env.QID_BACKFILL_HEARTBEAT_MS || "10000", 10);

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatDuration(ms) {
  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatRate(rows, ms) {
  if (!ms) return 0;
  return rows / (ms / 1000);
}

function formatEta(remainingRows, rate) {
  if (!Number.isFinite(rate) || rate <= 0 || remainingRows <= 0) {
    return "unknown";
  }

  return formatDuration((remainingRows / rate) * 1000);
}

function formatInterval(value) {
  if (!value) return "unknown";

  if (typeof value === "string") {
    return value.trim();
  }

  if (typeof value === "object") {
    const parts = [];

    if (value.days) parts.push(`${value.days}d`);
    if (value.hours) parts.push(`${value.hours}h`);
    if (value.minutes) parts.push(`${value.minutes}m`);
    if (value.seconds) parts.push(`${Math.floor(value.seconds)}s`);

    if (parts.length > 0) {
      return parts.join(" ");
    }
  }

  return String(value).trim();
}

async function getQuestionsQidState(client) {
  const hasColumnResult = await client.query(`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'questions'
        AND column_name = 'qid'
    ) AS exists
  `);

  const hasQidColumn = Boolean(hasColumnResult.rows[0]?.exists);
  const countSql = hasQidColumn
    ? `
      SELECT
        COUNT(*)::bigint AS total_rows,
        COUNT(*) FILTER (WHERE qid IS NULL)::bigint AS missing_qid_rows,
        COUNT(*) FILTER (WHERE qid IS NOT NULL)::bigint AS filled_qid_rows
      FROM public.questions
    `
    : `
      SELECT
        COUNT(*)::bigint AS total_rows,
        COUNT(*)::bigint AS missing_qid_rows,
        0::bigint AS filled_qid_rows
      FROM public.questions
    `;

  const countResult = await client.query(countSql);
  return {
    hasQidColumn,
    totalRows: Number(countResult.rows[0]?.total_rows || 0),
    missingRows: Number(countResult.rows[0]?.missing_qid_rows || 0),
    filledRows: Number(countResult.rows[0]?.filled_qid_rows || 0),
  };
}

async function ensureQuestionsQidColumn(client) {
  await client.query(`
    ALTER TABLE public.questions
    ADD COLUMN IF NOT EXISTS qid VARCHAR
  `);
}

async function runWithHeartbeat(pool, backendPid, label, work) {
  const observer = await pool.connect();
  const startedAt = Date.now();

  const timer = setInterval(async () => {
    try {
      const result = await observer.query(
        `
          SELECT
            state,
            wait_event_type,
            wait_event,
            now() - query_start AS running_for,
            left(query, 120) AS query
          FROM pg_stat_activity
          WHERE pid = $1
        `,
        [backendPid],
      );

      const row = result.rows[0];
      if (!row) {
        console.log(`[phase] ${label}: backend session no longer visible`);
        return;
      }

      console.log(
        `[phase] ${label}: running_for=${formatInterval(row.running_for)} state=${row.state} wait=${row.wait_event_type || "none"}/${row.wait_event || "none"} sql=${row.query}`,
      );
    } catch (err) {
      console.log(`[phase] ${label}: heartbeat check failed: ${err.message}`);
    }
  }, HEARTBEAT_MS);

  try {
    console.log(`[phase] ${label}: started`);
    const result = await work();
    console.log(
      `[phase] ${label}: finished in ${formatDuration(Date.now() - startedAt)}`,
    );
    return result;
  } finally {
    clearInterval(timer);
    observer.release();
  }
}

async function buildStageTables(pool, client, backendPid) {
  await client.query(`DROP TABLE IF EXISTS tmp_questions_qid_exact`);
  await client.query(`DROP TABLE IF EXISTS tmp_questions_qid_sid_ets_unique`);

  await runWithHeartbeat(pool, backendPid, "build exact qid stage", () =>
    client.query(`
      CREATE TEMP TABLE tmp_questions_qid_exact AS
      WITH log_events AS (
        SELECT
          event->>'sid' AS sid,
          (event->>'ets')::bigint AS ets,
          NULLIF(event->'edata'->'eks'->>'qid', '') AS qid,
          NULLIF(
            event->'edata'->'eks'->'target'->'questionsDetails'->>'questionText',
            ''
          ) AS questiontext
        FROM public.winston_logs wl
        CROSS JOIN LATERAL json_array_elements((wl.message::json)->'events') event
        WHERE wl.message LIKE '%ekstep.telemetry%'
          AND event->>'eid' = 'OE_ITEM_RESPONSE'
      )
      SELECT
        ROW_NUMBER() OVER (ORDER BY sid, ets, questiontext) AS stage_seq,
        sid,
        ets,
        questiontext,
        md5(COALESCE(questiontext, '')) AS questiontext_hash,
        MIN(qid) AS qid,
        COUNT(*)::bigint AS source_rows
      FROM log_events
      WHERE sid IS NOT NULL
        AND ets IS NOT NULL
        AND qid IS NOT NULL
        AND questiontext IS NOT NULL
      GROUP BY sid, ets, questiontext
      HAVING COUNT(DISTINCT qid) = 1
    `),
  );

  await client.query(`
    CREATE INDEX tmp_questions_qid_exact_stage_seq_idx
    ON tmp_questions_qid_exact (stage_seq)
  `);

  await client.query(`
    CREATE INDEX tmp_questions_qid_exact_sid_ets_questiontext_hash_idx
    ON tmp_questions_qid_exact (sid, ets, questiontext_hash)
  `);

  await runWithHeartbeat(pool, backendPid, "build sid+ets fallback stage", () =>
    client.query(`
      CREATE TEMP TABLE tmp_questions_qid_sid_ets_unique AS
      SELECT
        ROW_NUMBER() OVER (ORDER BY sid, ets) AS stage_seq,
        sid,
        ets,
        MIN(qid) AS qid
      FROM tmp_questions_qid_exact
      GROUP BY sid, ets
      HAVING COUNT(DISTINCT qid) = 1
    `),
  );

  await client.query(`
    CREATE INDEX tmp_questions_qid_sid_ets_unique_stage_seq_idx
    ON tmp_questions_qid_sid_ets_unique (stage_seq)
  `);

  await client.query(`
    CREATE INDEX tmp_questions_qid_sid_ets_unique_idx
    ON tmp_questions_qid_sid_ets_unique (sid, ets)
  `);
}

async function getStageStats(client, hasQidColumn) {
  const missingQidPredicate = hasQidColumn ? "q.qid IS NULL" : "TRUE";

  const result = await client.query(`
    SELECT
      (SELECT COUNT(*)::bigint FROM tmp_questions_qid_exact) AS exact_stage_rows,
      (SELECT COUNT(*)::bigint FROM tmp_questions_qid_sid_ets_unique) AS sid_ets_unique_rows,
      (
        SELECT COUNT(*)::bigint
        FROM public.questions q
        JOIN tmp_questions_qid_exact s
          ON q.sid = s.sid
         AND q.ets = s.ets
         AND md5(COALESCE(q.questiontext, '')) = s.questiontext_hash
         AND q.questiontext = s.questiontext
        WHERE ${missingQidPredicate}
      ) AS exact_matchable_rows,
      (
        SELECT COUNT(*)::bigint
        FROM public.questions q
        JOIN tmp_questions_qid_sid_ets_unique s
          ON q.sid = s.sid
         AND q.ets = s.ets
        WHERE ${missingQidPredicate}
      ) AS sid_ets_matchable_rows
  `);

  return {
    exactStageRows: Number(result.rows[0]?.exact_stage_rows || 0),
    sidEtsUniqueRows: Number(result.rows[0]?.sid_ets_unique_rows || 0),
    exactMatchableRows: Number(result.rows[0]?.exact_matchable_rows || 0),
    sidEtsMatchableRows: Number(result.rows[0]?.sid_ets_matchable_rows || 0),
  };
}

async function runBatchedUpdate(
  client,
  {
    label,
    stageTable,
    totalStageRows,
    totalTargetRows,
    batchSize,
    batchColumns,
    joinPredicate,
  },
) {
  let lastSeq = 0;
  let scannedRows = 0;
  let updatedRows = 0;
  let batches = 0;
  const startedAt = Date.now();

  while (true) {
    const result = await client.query(
      `
        WITH batch AS (
          SELECT ${batchColumns}
          FROM ${stageTable}
          WHERE stage_seq > $1
          ORDER BY stage_seq
          LIMIT $2
        ),
        updated AS (
          UPDATE public.questions q
          SET qid = batch.qid
          FROM batch
          WHERE q.qid IS NULL
            AND ${joinPredicate}
          RETURNING 1
        ),
        stats AS (
          SELECT
            COALESCE(MAX(stage_seq), $1) AS max_stage_seq,
            COUNT(*)::bigint AS batch_rows
          FROM batch
        )
        SELECT
          stats.max_stage_seq,
          stats.batch_rows,
          (SELECT COUNT(*)::bigint FROM updated) AS updated_rows
        FROM stats
      `,
      [lastSeq, batchSize],
    );

    const row = result.rows[0];
    const batchRows = Number(row?.batch_rows || 0);
    if (batchRows === 0) {
      break;
    }

    const batchUpdated = Number(row?.updated_rows || 0);
    lastSeq = Number(row?.max_stage_seq || lastSeq);
    scannedRows += batchRows;
    updatedRows += batchUpdated;
    batches += 1;

    const elapsedMs = Date.now() - startedAt;
    const rate = formatRate(updatedRows, elapsedMs);
    const eta = formatEta(Math.max(totalTargetRows - updatedRows, 0), rate);
    console.log(
      `[progress] ${label}: scanned=${formatNumber(scannedRows)}/${formatNumber(totalStageRows)} updated=${formatNumber(updatedRows)}/${formatNumber(totalTargetRows)} batches=${batches} rate=${rate.toFixed(1)} rows/s eta=${eta}`,
    );
  }

  console.log(
    `[done] ${label}: scanned=${formatNumber(scannedRows)} updated=${formatNumber(updatedRows)} elapsed=${formatDuration(Date.now() - startedAt)}`,
  );

  return updatedRows;
}

async function runExactUpdate(client, stage, batchSize) {
  return runBatchedUpdate(client, {
    label: "exact qid updates",
    stageTable: "tmp_questions_qid_exact",
    totalStageRows: stage.exactStageRows,
    totalTargetRows: stage.exactMatchableRows,
    batchSize,
    batchColumns: "stage_seq, sid, ets, questiontext, questiontext_hash, qid",
    joinPredicate:
      "q.sid = batch.sid AND q.ets = batch.ets AND md5(COALESCE(q.questiontext, '')) = batch.questiontext_hash AND q.questiontext = batch.questiontext",
  });
}

async function runFallbackUpdate(client, stage, exactUpdated, batchSize) {
  const totalTargetRows = Math.max(stage.sidEtsMatchableRows - exactUpdated, 0);
  return runBatchedUpdate(client, {
    label: "fallback qid updates",
    stageTable: "tmp_questions_qid_sid_ets_unique",
    totalStageRows: stage.sidEtsUniqueRows,
    totalTargetRows,
    batchSize,
    batchColumns: "stage_seq, sid, ets, qid",
    joinPredicate: "q.sid = batch.sid AND q.ets = batch.ets",
  });
}

async function ensureIndexes(client) {
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_questions_id
    ON public.questions (id)
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_questions_sid_ets
    ON public.questions (sid, ets)
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_questions_qid
    ON public.questions (qid)
    WHERE qid IS NOT NULL
  `);
}

async function main() {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT || "5432", 10),
    ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,
  });

  const client = await pool.connect();
  const startedAt = Date.now();

  try {
    console.log(`questions qid backfill starting (dryRun=${dryRun})`);
    const backendPidResult = await client.query(`SELECT pg_backend_pid() AS pid`);
    const backendPid = backendPidResult.rows[0].pid;
    console.log(`[session] backend_pid=${backendPid} heartbeat_ms=${HEARTBEAT_MS}`);

    const before = await getQuestionsQidState(client);
    console.log(
      `[before] total=${formatNumber(before.totalRows)} missing_qid=${formatNumber(before.missingRows)} filled_qid=${formatNumber(before.filledRows)} qid_column=${before.hasQidColumn}`,
    );

    if (!dryRun) {
      await ensureQuestionsQidColumn(client);
      console.log(`[schema] ensured public.questions.qid exists`);
    }

    await buildStageTables(pool, client, backendPid);
    const stage = await getStageStats(client, dryRun ? before.hasQidColumn : true);
    console.log(
      `[stage] exact_keys=${formatNumber(stage.exactStageRows)} unique_sid_ets=${formatNumber(stage.sidEtsUniqueRows)} exact_matchable=${formatNumber(stage.exactMatchableRows)} sid_ets_matchable=${formatNumber(stage.sidEtsMatchableRows)}`,
    );

    if (dryRun) {
      console.log(
        `[dry-run] would_update_exact=${formatNumber(stage.exactMatchableRows)} would_update_fallback_up_to=${formatNumber(stage.sidEtsMatchableRows)}`,
      );
      return;
    }

    const batchSize = parseInt(process.env.QID_BACKFILL_BATCH_SIZE || "25000", 10);
    console.log(`[update] batch_size=${formatNumber(batchSize)}`);

    const exactUpdated = await runWithHeartbeat(
      pool,
      backendPid,
      "apply exact qid updates",
      () => runExactUpdate(client, stage, batchSize),
    );
    console.log(`[update] exact_match_rows=${formatNumber(exactUpdated)}`);

    const fallbackUpdated = await runWithHeartbeat(
      pool,
      backendPid,
      "apply fallback qid updates",
      () => runFallbackUpdate(client, stage, exactUpdated, batchSize),
    );
    console.log(`[update] fallback_sid_ets_rows=${formatNumber(fallbackUpdated)}`);

    await ensureIndexes(client);
    console.log(`[index] ensured idx_questions_id and idx_questions_qid`);

    const after = await getQuestionsQidState(client);
    console.log(
      `[after] total=${formatNumber(after.totalRows)} missing_qid=${formatNumber(after.missingRows)} filled_qid=${formatNumber(after.filledRows)}`,
    );

    console.log(
      `questions qid backfill complete in ${formatDuration(Date.now() - startedAt)}`,
    );
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
