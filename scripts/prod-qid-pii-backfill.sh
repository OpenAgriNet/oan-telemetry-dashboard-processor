#!/bin/sh
set -eu

DRY_RUN=false
SKIP_QID=false
SKIP_PII=false
PII_PASSES="${PII_PASSES:-2}"
PII_BATCH_SIZE="${PII_BATCH_SIZE:-5000}"
QID_BATCH_SIZE="${QID_BACKFILL_BATCH_SIZE:-25000}"

usage() {
  cat <<'EOF'
Usage:
  scripts/prod-qid-pii-backfill.sh [options]

Options:
  --dry-run       Run non-mutating qid and PII checks where supported.
  --skip-qid      Skip questions.qid refill.
  --skip-pii      Skip PII SQL column masking.
  --pii-passes=N  Number of PII masking passes to run. Default: 2.

Required env:
  DB_USER DB_HOST DB_NAME DB_PASSWORD DB_PORT

Recommended prod env:
  DB_SSL=true
  I_UNDERSTAND_PROD_BACKFILL=true

Examples:
  DB_USER=... DB_HOST=... DB_NAME=... DB_PASSWORD=... DB_PORT=5432 DB_SSL=true \\
    I_UNDERSTAND_PROD_BACKFILL=true scripts/prod-qid-pii-backfill.sh

  DB_USER=... DB_HOST=... DB_NAME=... DB_PASSWORD=... DB_PORT=5432 DB_SSL=true \\
    scripts/prod-qid-pii-backfill.sh --dry-run
EOF
}

for arg in "$@"; do
  case "$arg" in
    --dry-run)
      DRY_RUN=true
      ;;
    --skip-qid)
      SKIP_QID=true
      ;;
    --skip-pii)
      SKIP_PII=true
      ;;
    --pii-passes=*)
      PII_PASSES="${arg#*=}"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage
      exit 1
      ;;
  esac
done

require_env() {
  name="$1"
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "Missing required env var: $name" >&2
    exit 1
  fi
}

run_node_sql() {
  label="$1"
  sql="$2"
  echo
  echo "==> $label"
  SQL_TO_RUN="$sql" node - <<'NODE'
const { Pool } = require("pg");

(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT || 5432),
    ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,
  });

  const result = await pool.query(process.env.SQL_TO_RUN);
  if (result.rows?.length) {
    console.table(result.rows);
  } else {
    console.log(`ok rows=${result.rowCount ?? 0}`);
  }
  await pool.end();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
NODE
}

require_env DB_USER
require_env DB_HOST
require_env DB_NAME
require_env DB_PASSWORD
require_env DB_PORT

if [ "$DRY_RUN" != "true" ] && [ "${I_UNDERSTAND_PROD_BACKFILL:-}" != "true" ]; then
  cat >&2 <<'EOF'
Refusing to run mutating production backfill.

Before running:
  1. Confirm prod DB snapshot/backup exists.
  2. Prefer low-traffic window or pause telemetry ingestion.
  3. Export I_UNDERSTAND_PROD_BACKFILL=true.

For a non-mutating check, pass --dry-run.
EOF
  exit 1
fi

echo "Production qid + PII backfill runner"
echo "DB_HOST=$DB_HOST DB_NAME=$DB_NAME DB_PORT=$DB_PORT DB_SSL=${DB_SSL:-false}"
echo "dry_run=$DRY_RUN skip_qid=$SKIP_QID skip_pii=$SKIP_PII pii_passes=$PII_PASSES pii_batch_size=$PII_BATCH_SIZE qid_batch_size=$QID_BATCH_SIZE"

echo
echo "==> Compile checks"
node -c scripts/backfill-questions-qid.js
node -c scripts/backfill-pii-sql-column.js
node -c index.js

run_node_sql "qid state before" "
  SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE qid IS NOT NULL)::bigint AS qid_filled,
    COUNT(*) FILTER (WHERE qid IS NULL)::bigint AS qid_missing
  FROM public.questions
"

if [ "$SKIP_QID" != "true" ]; then
  echo
  echo "==> questions.qid refill"
  if [ "$DRY_RUN" = "true" ]; then
    QID_BACKFILL_BATCH_SIZE="$QID_BATCH_SIZE" node scripts/backfill-questions-qid.js --dry-run
  else
    QID_BACKFILL_BATCH_SIZE="$QID_BATCH_SIZE" node scripts/backfill-questions-qid.js
    run_node_sql "ensure qid indexes" "
      CREATE INDEX IF NOT EXISTS idx_questions_qid ON public.questions(qid) WHERE qid IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_questions_sid_ets ON public.questions(sid, ets);
    "
  fi
else
  echo
  echo "==> questions.qid refill skipped"
fi

run_node_sql "qid state after qid step" "
  SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE qid IS NOT NULL)::bigint AS qid_filled,
    COUNT(*) FILTER (WHERE qid IS NULL)::bigint AS qid_missing
  FROM public.questions
"

echo
echo "==> Ensure SQL PII functions"
node - <<'NODE'
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT || 5432),
    ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,
  });

  const sql = fs.readFileSync(
    path.join(process.cwd(), "migrations", "20260624_pii_candidate_function.sql"),
    "utf8",
  );
  await pool.query(sql);
  const check = await pool.query(`
    SELECT
      to_regprocedure('public.contains_pii_candidate(text)') IS NOT NULL AS has_candidate_fn,
      to_regprocedure('public.mask_pii_text(text)') IS NOT NULL AS has_mask_fn,
      public.contains_pii_candidate('call me at 9876543210') AS phone_detected,
      public.mask_pii_text('call me at 9876543210') AS masked_sample
  `);
  console.table(check.rows);
  await pool.end();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
NODE

audit_pii() {
  node - <<'NODE'
const { Pool } = require("pg");
const targets = [
  { table: "messages", cols: ["content"] },
  { table: "questions", cols: ["questiontext", "answer", "answertext", "groupdetails"] },
  { table: "feedback", cols: ["feedbacktext", "questiontext", "answertext", "groupdetails"] },
  { table: "errordetails", cols: ["errormessage", "errortext"] },
];

function qi(name) {
  return `"${name.replace(/"/g, '""')}"`;
}

(async () => {
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT || 5432),
    ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,
  });
  const client = await pool.connect();
  let totalRemaining = 0;
  try {
    for (const target of targets) {
      const exists = await client.query("SELECT to_regclass($1) AS reg", [`public.${target.table}`]);
      if (!exists.rows[0].reg) continue;
      const cols = await client.query(
        "SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1",
        [target.table],
      );
      const colMap = new Map(cols.rows.map((row) => [row.column_name, row.udt_name]));
      for (const col of target.cols) {
        const type = colMap.get(col);
        if (!type || (type !== "text" && type !== "varchar")) continue;
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
    if (totalRemaining > 0) {
      process.exitCode = 2;
    }
  } finally {
    client.release();
    await pool.end();
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
NODE
}

echo
echo "==> PII audit before masking"
set +e
audit_pii
AUDIT_BEFORE_STATUS=$?
set -e

if [ "$SKIP_PII" != "true" ]; then
  echo
  echo "==> PII SQL column masking"
  if [ "$DRY_RUN" = "true" ]; then
    node scripts/backfill-pii-sql-column.js --dry-run --batch-size="$PII_BATCH_SIZE"
  else
    pass=1
    while [ "$pass" -le "$PII_PASSES" ]; do
      echo
      echo "==> PII pass $pass/$PII_PASSES"
      node scripts/backfill-pii-sql-column.js --batch-size="$PII_BATCH_SIZE"
      pass=$((pass + 1))
    done
  fi
else
  echo
  echo "==> PII masking skipped"
fi

echo
echo "==> Final PII audit"
if audit_pii; then
  echo
  echo "Backfill finished successfully: final PII audit is clean."
else
  echo
  echo "Backfill finished, but final PII audit still has remaining rows. Run another PII pass or inspect samples." >&2
  exit 2
fi
