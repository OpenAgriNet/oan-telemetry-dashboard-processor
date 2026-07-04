#!/usr/bin/env node
/**
 * DB-side PII backfill using column-level regex predicates.
 *
 * This is faster to operate and easier to observe than the JS masking path for
 * plain text columns: each column is filtered by public.contains_pii_candidate()
 * and updated with public.mask_pii_text().
 *
 * Usage:
 *   node scripts/backfill-pii-sql-column.js
 *   node scripts/backfill-pii-sql-column.js --dry-run
 *   node scripts/backfill-pii-sql-column.js --batch-size=5000
 *   node scripts/backfill-pii-sql-column.js --only=questions.answertext
 */

const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const dryRun = process.argv.includes("--dry-run");
const includeJsonb = process.argv.includes("--include-jsonb");
const batchSizeArg = process.argv.find((arg) => arg.startsWith("--batch-size="));
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const onlyArg = process.argv.find((arg) => arg.startsWith("--only="));

const BATCH_SIZE = batchSizeArg
  ? parseInt(batchSizeArg.split("=")[1], 10)
  : parseInt(process.env.PII_SQL_BACKFILL_BATCH_SIZE || "5000", 10);

const ROW_LIMIT = limitArg ? parseInt(limitArg.split("=")[1], 10) : null;
const ONLY = onlyArg ? onlyArg.split("=")[1] : null;

const TARGETS = [
  { table: "messages", idColumn: "id", columns: ["content"] },
  {
    table: "questions",
    idColumn: "id",
    columns: ["questiontext", "answer", "answertext", "groupdetails"],
  },
  {
    table: "feedback",
    idColumn: "id",
    columns: ["feedbacktext", "questiontext", "answertext", "groupdetails"],
  },
  { table: "errordetails", idColumn: "id", columns: ["errormessage", "errortext"] },
];

function quoteIdent(identifier) {
  return `"${identifier.replace(/"/g, '""')}"`;
}

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

function ratePerSecond(rows, startedAt) {
  const elapsedMs = Date.now() - startedAt;
  if (elapsedMs <= 0 || rows <= 0) return 0;
  return rows / (elapsedMs / 1000);
}

function formatEta(done, total, startedAt) {
  const rate = ratePerSecond(done, startedAt);
  if (!Number.isFinite(rate) || rate <= 0 || done >= total) return "unknown";
  return formatDuration(((total - done) / rate) * 1000);
}

function buildColumnPredicate(columnSql) {
  return `
    ${columnSql} IS NOT NULL
    AND public.contains_pii_candidate(${columnSql}::text)
    AND ${columnSql}::text IS DISTINCT FROM public.mask_pii_text(${columnSql}::text)
  `;
}

async function ensurePiiSqlFunctions(client) {
  const functionSql = fs.readFileSync(
    path.join(__dirname, "..", "migrations", "20260624_pii_candidate_function.sql"),
    "utf8",
  );
  await client.query(functionSql);
}

async function getAvailableColumns(client, tableName) {
  const result = await client.query(
    `
      SELECT column_name, data_type, udt_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
    `,
    [tableName],
  );

  return new Map(result.rows.map((row) => [row.column_name, row]));
}

function shouldRunColumn(table, column) {
  return !ONLY || ONLY === `${table}.${column}` || ONLY === table;
}

function canUpdateColumn(columnInfo) {
  if (!columnInfo) return false;
  if (columnInfo.udt_name === "jsonb") return includeJsonb;
  return columnInfo.udt_name === "text" || columnInfo.udt_name === "varchar";
}

function maskedExpression(columnSql, columnInfo) {
  if (columnInfo.udt_name === "jsonb") {
    return `public.mask_pii_text(${columnSql}::text)::jsonb`;
  }
  return `public.mask_pii_text(${columnSql}::text)`;
}

async function countRemaining(client, table, column) {
  const tableSql = `public.${quoteIdent(table)}`;
  const columnSql = quoteIdent(column);
  const result = await client.query(`
    SELECT COUNT(*)::bigint AS count
    FROM ${tableSql}
    WHERE ${buildColumnPredicate(columnSql)}
  `);
  return Number(result.rows[0]?.count || 0);
}

function tempTableName(table, column) {
  return `tmp_pii_sql_${table}_${column}`.replace(/[^a-zA-Z0-9_]/g, "_");
}

async function buildCandidateTable(client, table, idColumn, column) {
  const tableSql = `public.${quoteIdent(table)}`;
  const idSql = quoteIdent(idColumn);
  const columnSql = quoteIdent(column);
  const tempSql = quoteIdent(tempTableName(table, column));
  const predicate = buildColumnPredicate(`t.${columnSql}`);
  const limitSql = Number.isFinite(ROW_LIMIT) && ROW_LIMIT > 0 ? `LIMIT ${ROW_LIMIT}` : "";
  const startedAt = Date.now();

  await client.query(`DROP TABLE IF EXISTS ${tempSql}`);
  await client.query(`
    CREATE TEMP TABLE ${tempSql} AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY ${idSql}) AS seq,
      tid
    FROM (
      SELECT t.ctid AS tid, t.${idSql} AS ${idSql}
      FROM ${tableSql} t
      WHERE ${predicate}
      ORDER BY t.${idSql}
      ${limitSql}
    ) candidates
  `);

  await client.query(`CREATE INDEX ON ${tempSql} (seq)`);
  const countResult = await client.query(`SELECT COUNT(*)::bigint AS count FROM ${tempSql}`);
  const count = Number(countResult.rows[0]?.count || 0);
  console.log(
    `[stage] ${table}.${column}: candidates=${formatNumber(count)} elapsed=${formatDuration(Date.now() - startedAt)}`,
  );

  return { count, tempSql };
}

async function updateColumn(client, table, idColumn, column, columnInfo) {
  const tableSql = `public.${quoteIdent(table)}`;
  const idSql = quoteIdent(idColumn);
  const columnSql = quoteIdent(column);
  const startedAt = Date.now();

  const { count: targetTotal, tempSql } = await buildCandidateTable(
    client,
    table,
    idColumn,
    column,
  );

  console.log(
    `[start] ${table}.${column}: target=${formatNumber(targetTotal)} batch_size=${formatNumber(BATCH_SIZE)} dry_run=${dryRun}`,
  );

  if (targetTotal === 0 || dryRun) {
    return { scanned: targetTotal, updated: 0, remaining: targetTotal };
  }

  let updated = 0;
  let batches = 0;

  while (updated < targetTotal) {
    const limit = Math.min(BATCH_SIZE, targetTotal - updated);
    const result = await client.query(
      `
        WITH batch AS (
          SELECT tid
          FROM ${tempSql}
          WHERE seq > $1
          ORDER BY seq
          LIMIT $2
        )
        UPDATE ${tableSql} t
        SET ${columnSql} = ${maskedExpression(`t.${columnSql}`, columnInfo)}
        FROM batch
        WHERE t.ctid = batch.tid
        RETURNING t.${idSql}
      `,
      [updated, limit],
    );

    if (result.rowCount === 0) break;

    updated += result.rowCount;
    batches += 1;

    const rate = ratePerSecond(updated, startedAt);
    console.log(
      `[progress] ${table}.${column}: updated=${formatNumber(updated)}/${formatNumber(targetTotal)} batches=${formatNumber(batches)} rate=${rate.toFixed(1)} rows/s eta=${formatEta(updated, targetTotal, startedAt)}`,
    );
  }

  console.log(
    `[done] ${table}.${column}: updated=${formatNumber(updated)} elapsed=${formatDuration(Date.now() - startedAt)}`,
  );

  return { scanned: targetTotal, updated, remaining: targetTotal - updated };
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
  const jobStartedAt = Date.now();
  const results = [];

  try {
    console.log(
      `PII SQL column backfill starting (batch=${formatNumber(BATCH_SIZE)}, dryRun=${dryRun}, only=${ONLY || "all"}, include_jsonb=${includeJsonb})`,
    );
    await ensurePiiSqlFunctions(client);
    console.log("[schema] ensured public.contains_pii_candidate(text) and public.mask_pii_text(text)");

    for (const target of TARGETS) {
      const exists = await client.query(`SELECT to_regclass($1) AS reg`, [
        `public.${target.table}`,
      ]);

      if (!exists.rows[0]?.reg) {
        console.log(`[skip] ${target.table}: table not found`);
        continue;
      }

      const availableColumns = await getAvailableColumns(client, target.table);
      for (const column of target.columns) {
        if (!shouldRunColumn(target.table, column)) continue;

        const columnInfo = availableColumns.get(column);
        if (!columnInfo) {
          console.log(`[skip] ${target.table}.${column}: column not found`);
          continue;
        }

        if (!canUpdateColumn(columnInfo)) {
          console.log(
            `[skip] ${target.table}.${column}: ${columnInfo.udt_name} skipped${columnInfo.udt_name === "jsonb" ? " (pass --include-jsonb to enable)" : ""}`,
          );
          continue;
        }

        results.push(
          await updateColumn(client, target.table, target.idColumn, column, columnInfo),
        );
      }
    }

    const totalUpdated = results.reduce((sum, result) => sum + result.updated, 0);
    const totalRemaining = results.reduce((sum, result) => sum + result.remaining, 0);
    console.log(
      `PII SQL column backfill complete. updated=${formatNumber(totalUpdated)} remaining=${formatNumber(totalRemaining)} elapsed=${formatDuration(Date.now() - jobStartedAt)}`,
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
