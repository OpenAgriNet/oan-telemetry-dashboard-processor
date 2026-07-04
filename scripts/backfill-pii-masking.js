#!/usr/bin/env node
/**
 * One-off backfill: mask embedded PII in free-text columns (questions, answers, feedback, errors, messages).
 *
 * Usage:
 *   node scripts/backfill-pii-masking.js [--dry-run] [--batch-size=500]
 *
 * Requires .env DB_* settings (same as index.js).
 */

const path = require("path");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const { pii } = require("../middleware/pii");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const dryRun = process.argv.includes("--dry-run");
const batchSizeArg = process.argv.find((a) => a.startsWith("--batch-size="));
const BATCH_SIZE = batchSizeArg
  ? parseInt(batchSizeArg.split("=")[1], 10)
  : parseInt(process.env.PII_BACKFILL_BATCH_SIZE || "500", 10);

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

async function backfillTable(client, spec) {
  const cols = spec.columns.map((c) => c.name);
  const selectCols = ["id", ...cols].join(", ");

  let lastId = null;
  let lastCreatedAt = null;
  let scanned = 0;
  let updated = 0;

  for (;;) {
    const query =
      spec.pagination === "id"
        ? {
            text: `SELECT ${selectCols} FROM ${spec.table} WHERE ${spec.idColumn} > $1 ORDER BY ${spec.idColumn} ASC LIMIT $2`,
            values: [lastId ?? 0, BATCH_SIZE],
          }
        : lastCreatedAt === null
          ? {
              text: `SELECT ${selectCols}, created_at FROM ${spec.table} ORDER BY created_at ASC, ${spec.idColumn} ASC LIMIT $1`,
              values: [BATCH_SIZE],
            }
          : {
              text: `SELECT ${selectCols}, created_at FROM ${spec.table} WHERE (created_at, ${spec.idColumn}) > ($1::timestamp, $2::uuid) ORDER BY created_at ASC, ${spec.idColumn} ASC LIMIT $3`,
              values: [lastCreatedAt, lastId, BATCH_SIZE],
            };

    const { rows } = await client.query(query.text, query.values);

    if (rows.length === 0) break;

    for (const row of rows) {
      scanned += 1;
      lastId = row.id;
      if (spec.pagination !== "id") {
        lastCreatedAt = row.created_at;
      }

      const sets = [];
      const params = [];
      let paramIdx = 1;

      for (const col of spec.columns) {
        const raw = row[col.name];
        const masked = maskCell(col, raw);
        if (!valuesEqual(raw, masked)) {
          sets.push(`${col.name} = $${paramIdx++}`);
          params.push(masked);
        }
      }

      if (sets.length === 0) continue;

      updated += 1;
      params.push(row.id);

      const sql = `UPDATE ${spec.table} SET ${sets.join(", ")} WHERE ${spec.idColumn} = $${paramIdx}`;

      if (dryRun) {
        console.log(`[dry-run] ${spec.table} id=${row.id}: ${sets.length} column(s)`);
      } else {
        await client.query(sql, params);
      }
    }
  }

  console.log(
    `${spec.table}: scanned=${scanned} rows_updated=${updated}${dryRun ? " (dry-run)" : ""}`,
  );
  return { scanned, updated };
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

  try {
    console.log(`PII backfill starting (batch=${BATCH_SIZE}, dryRun=${dryRun})`);

    for (const spec of TABLES) {
      const exists = await client.query(
        `SELECT to_regclass($1) AS reg`,
        [`public.${spec.table}`],
      );
      if (!exists.rows[0]?.reg) {
        console.log(`${spec.table}: skipped (table not found)`);
        continue;
      }
      await backfillTable(client, spec);
    }

    console.log("PII backfill complete.");
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
