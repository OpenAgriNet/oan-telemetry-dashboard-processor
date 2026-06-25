#!/usr/bin/env node
/**
 * Observable PII backfill job runner.
 *
 * Adds:
 * - total row counts per table before processing
 * - per-batch progress logging
 * - percent complete, rows/sec, and ETA
 * - overall run summary
 *
 * Usage:
 *   node scripts/backfill-pii-masking-job.js
 *   node scripts/backfill-pii-masking-job.js --dry-run
 *   node scripts/backfill-pii-masking-job.js --batch-size=2000
 *   node scripts/backfill-pii-masking-job.js --progress-every=5000
 */

const path = require("path");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const { pii } = require("../middleware/pii");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const dryRun = process.argv.includes("--dry-run");
const rowLog = process.argv.includes("--row-log");
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const progressEveryArg = process.argv.find((arg) =>
  arg.startsWith("--progress-every="),
);

const BATCH_SIZE = batchSizeArg
  ? parseInt(batchSizeArg.split("=")[1], 10)
  : parseInt(process.env.PII_BACKFILL_BATCH_SIZE || "500", 10);

const PROGRESS_EVERY = progressEveryArg
  ? parseInt(progressEveryArg.split("=")[1], 10)
  : parseInt(process.env.PII_BACKFILL_PROGRESS_EVERY || "5000", 10);

const TABLES = [
  {
    table: "messages",
    idColumn: "id",
    pagination: "id",
    columns: [{ name: "content", mask: (v) => pii.maskMessage(v) }],
  },
  {
    table: "questions",
    idColumn: "id",
    pagination: "created_at",
    columns: [
      { name: "questiontext" },
      { name: "answer" },
      { name: "answertext", json: true },
      { name: "groupdetails", json: true },
    ],
  },
  {
    table: "feedback",
    idColumn: "id",
    pagination: "created_at",
    columns: [
      { name: "feedbacktext" },
      { name: "questiontext" },
      { name: "answertext" },
      { name: "groupdetails", json: true },
    ],
  },
  {
    table: "errordetails",
    idColumn: "id",
    pagination: "created_at",
    columns: [{ name: "errormessage" }],
  },
];

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatDuration(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "unknown";

  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatRate(rows, startedAt) {
  const elapsedMs = Date.now() - startedAt;
  if (elapsedMs <= 0 || rows <= 0) return "0.0";
  return (rows / (elapsedMs / 1000)).toFixed(1);
}

function computeEta(scanned, totalRows, startedAt) {
  if (!totalRows || scanned <= 0 || scanned >= totalRows) return "unknown";

  const elapsedMs = Date.now() - startedAt;
  const rowsPerMs = scanned / elapsedMs;
  if (!Number.isFinite(rowsPerMs) || rowsPerMs <= 0) return "unknown";

  const remainingRows = totalRows - scanned;
  return formatDuration(remainingRows / rowsPerMs);
}

function formatPercent(scanned, totalRows) {
  if (!totalRows) return "0.0";
  return ((scanned / totalRows) * 100).toFixed(1);
}

function maskCell(column, value) {
  if (value === null || value === undefined) return value;
  if (column.mask) return column.mask(value);
  if (column.json && typeof value === "object") {
    return pii.maskColumn(column.name, value);
  }
  return pii.maskColumn(column.name, value);
}

function valuesEqual(a, b) {
  if (a === b) return true;
  if (a === null || b === null) return false;
  if (typeof a === "object" || typeof b === "object") {
    return JSON.stringify(a) === JSON.stringify(b);
  }
  return String(a) === String(b);
}

async function getTableRowCount(client, tableName) {
  const result = await client.query(`SELECT COUNT(*)::bigint AS count FROM ${tableName}`);
  return Number(result.rows[0]?.count || 0);
}

function printBatchProgress(spec, stats) {
  const rate = formatRate(stats.scanned, stats.startedAt);
  const eta = computeEta(stats.scanned, stats.totalRows, stats.startedAt);
  const percent = formatPercent(stats.scanned, stats.totalRows);

  console.log(
    `[progress] ${spec.table}: scanned=${formatNumber(stats.scanned)}/${formatNumber(stats.totalRows)} (${percent}%) updated=${formatNumber(stats.updated)} batches=${formatNumber(stats.batches)} rate=${rate} rows/s eta=${eta}`,
  );
}

async function backfillTable(client, spec, totalRows) {
  const cols = spec.columns.map((c) => c.name);
  const selectCols = ["id", ...cols].join(", ");

  let lastId = null;
  let lastCreatedAt = null;
  const stats = {
    scanned: 0,
      updated: 0,
      batches: 0,
      totalRows,
      startedAt: Date.now(),
      lastLoggedAt: Date.now(),
      nextProgressThreshold: PROGRESS_EVERY,
    };

  console.log(
    `[start] ${spec.table}: total_rows=${formatNumber(totalRows)} batch_size=${formatNumber(BATCH_SIZE)} dry_run=${dryRun}`,
  );

  for (;;) {
    // created_at is `timestamp without time zone`. The pg driver parses it into a
    // JS Date in UTC, and re-serializing that Date back into a `timestamp` param
    // shifts it across the server's TZ, so the keyset never advances (infinite
    // loop on the same batch). To avoid any Date/TZ round-trip, we carry the
    // cursor as a raw ISO string: read created_at out as ::text and bind it back
    // as text::timestamp so it compares byte-for-byte against the stored value.
    const query =
      spec.pagination === "id"
        ? {
            text: `SELECT ${selectCols} FROM ${spec.table} WHERE ${spec.idColumn} > $1 ORDER BY ${spec.idColumn} ASC LIMIT $2`,
            values: [lastId ?? 0, BATCH_SIZE],
          }
        : lastCreatedAt === null
          ? {
              text: `SELECT ${selectCols}, created_at::text AS created_at FROM ${spec.table} ORDER BY created_at ASC, ${spec.idColumn} ASC LIMIT $1`,
              values: [BATCH_SIZE],
            }
          : {
              text: `SELECT ${selectCols}, created_at::text AS created_at FROM ${spec.table} WHERE (created_at, ${spec.idColumn}) > ($1::timestamp, $2::uuid) ORDER BY created_at ASC, ${spec.idColumn} ASC LIMIT $3`,
              values: [lastCreatedAt, lastId, BATCH_SIZE],
            };

    const { rows } = await client.query(query.text, query.values);
    if (rows.length === 0) break;

    stats.batches += 1;

    // Compute masks in JS (regex engine), then flush changed rows in ONE
    // UPDATE per batch via a VALUES join. This replaces N per-row UPDATEs
    // (which collapsed to ~10 rows/s) with a single round-trip + sequential
    // write per batch. Columns that didn't change carry their current value
    // through unchanged so the VALUES row is uniform.
    const changedRows = [];

    for (const row of rows) {
      stats.scanned += 1;
      lastId = row.id;
      if (spec.pagination !== "id") {
        lastCreatedAt = row.created_at;
      }

      let changed = false;
      const maskedValues = {};

      for (const col of spec.columns) {
        const raw = row[col.name];
        const masked = maskCell(col, raw);
        maskedValues[col.name] = masked;
        if (!valuesEqual(raw, masked)) {
          changed = true;
        }
      }

      if (changed) {
        changedRows.push({ id: row.id, values: maskedValues });
      }

      if (
        stats.scanned >= stats.nextProgressThreshold ||
        Date.now() - stats.lastLoggedAt >= 30000
      ) {
        printBatchProgress(spec, stats);
        stats.lastLoggedAt = Date.now();
        while (stats.nextProgressThreshold <= stats.scanned) {
          stats.nextProgressThreshold += PROGRESS_EVERY;
        }
      }
    }

    if (changedRows.length > 0) {
      stats.updated += changedRows.length;

      if (dryRun && rowLog) {
        for (const r of changedRows) {
          console.log(`[dry-run] ${spec.table} id=${r.id}: ${spec.columns.length} column(s)`);
        }
      }

      if (!dryRun) {
        // VALUES ($1,$2,$3,...), ($4,$5,$6,...), ...
        // column order: id, <maskable columns...>
        const colNames = spec.columns.map((c) => c.name);
        const rowWidth = 1 + colNames.length;
        const placeholders = changedRows
          .map((_, i) => {
            const base = i * rowWidth;
            // id is uuid on all target tables; cast so the join below stays
            // uuid = uuid (pg infers text from the VALUES list otherwise, and
            // "uuid = text" has no implicit operator).
            const cols = [`$${base + 1}::uuid`];
            for (let c = 0; c < colNames.length; c++) {
              cols.push(`$${base + 2 + c}`);
            }
            return `(${cols.join(", ")})`;
          })
          .join(", ");

        const params = [];
        for (const r of changedRows) {
          params.push(r.id);
          for (const c of colNames) {
            params.push(r.values[c]);
          }
        }

        const setClause = colNames.map((c) => `${c} = v.${c}`).join(", ");
        const sql = `
          UPDATE ${spec.table} AS t
          SET ${setClause}
          FROM (VALUES ${placeholders}) AS v(id, ${colNames.join(", ")})
          WHERE t.${spec.idColumn} = v.id
        `;
        await client.query(sql, params);
      }
    }
  }

  printBatchProgress(spec, stats);
  console.log(
    `[done] ${spec.table}: scanned=${formatNumber(stats.scanned)} rows_updated=${formatNumber(stats.updated)} elapsed=${formatDuration(Date.now() - stats.startedAt)}${dryRun ? " (dry-run)" : ""}`,
  );

  return stats;
}

async function main() {
  if (!pii.isEnabled()) {
    console.error("MASK_PII_ON_WRITE=false — enable masking before backfill.");
    process.exit(1);
  }

  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT || "5432", 10),
  });

  const client = await pool.connect();
  const jobStartedAt = Date.now();
  const jobStats = [];

  try {
    console.log(
      `PII backfill job starting (batch=${BATCH_SIZE}, progress_every=${PROGRESS_EVERY}, dryRun=${dryRun})`,
    );

    for (const spec of TABLES) {
      const exists = await client.query(
        `SELECT to_regclass($1) AS reg`,
        [`public.${spec.table}`],
      );

      if (!exists.rows[0]?.reg) {
        console.log(`[skip] ${spec.table}: table not found`);
        continue;
      }

      const totalRows = await getTableRowCount(client, spec.table);
      jobStats.push(await backfillTable(client, spec, totalRows));
    }

    const totalScanned = jobStats.reduce((sum, stat) => sum + stat.scanned, 0);
    const totalUpdated = jobStats.reduce((sum, stat) => sum + stat.updated, 0);

    console.log(
      `PII backfill complete. scanned=${formatNumber(totalScanned)} updated=${formatNumber(totalUpdated)} elapsed=${formatDuration(Date.now() - jobStartedAt)}`,
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
