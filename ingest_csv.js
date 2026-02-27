#!/usr/bin/env node
/**
 * ingest_csv.js — Daily CSV ingestion for BharatVistaar call data
 *
 * USAGE:
 *   node ingest_csv.js --file <csvfile>
 *   node ingest_csv.js --file BharatVistaarVoiceAgentLID-2026-02-26.csv
 *
 * - Reads DB credentials from .env (same as the main processor)
 * - Creates tables + indexes if they don't exist (idempotent)
 * - Streams the CSV, inserts calls via ON CONFLICT DO NOTHING (skip dupes)
 * - Parses transcripts for newly inserted calls
 * - Prints a summary at the end
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { Pool } = require("pg");
const dotenv = require("dotenv");

// ── Load .env ────────────────────────────────────────────────
dotenv.config({ path: path.join(__dirname, ".env") });

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT || "5432", 10),
});

// ── Parse --file flag ────────────────────────────────────────
function getCSVArg() {
    const args = process.argv.slice(2);
    const fileIdx = args.indexOf("--file");
    if (fileIdx !== -1 && args[fileIdx + 1]) {
        return args[fileIdx + 1];
    }
    // Also support positional arg (backwards compat)
    if (args.length > 0 && !args[0].startsWith("--")) {
        return args[0];
    }
    return null;
}

// ── CSV column order (must match the CSV header exactly) ─────
const CSV_COLUMNS = [
    "job_id",
    "interaction_id",
    "user_id",
    "connectivity_status",
    "failure_reason",
    "end_reason",
    "duration_in_seconds",
    "start_datetime",
    "end_datetime",
    "language_name",
    "num_messages",
    "average_agent_response_time_in_seconds",
    "average_user_response_time_in_seconds",
    "user_contact_masked",
    "channel_direction",
    "retry_attempt",
    "campaign_id",
    "cohort_id",
    "user_contact_hashed",
    "is_debug_call",
    "audio_url",
    "server_retry_attempt",
    "has_log_issues",
    "channel_provider",
    "channel_protocol",
    "channel_type",
    "provider_type",
    "transcript",
    "agent_variables.current_language",
    "agent_variables.language_changed",
];

// ── Helpers ──────────────────────────────────────────────────

/**
 * Parse a single CSV line handling quoted fields (including multiline
 * content within quotes). Returns null when a row spans multiple lines
 * and sets `ctx.partial` for the next call.
 */
function parseCSVLine(line, ctx) {
    if (ctx.partial) {
        // Continue a multiline quoted field
        ctx.buffer += "\n" + line;
        const quoteCount = (ctx.buffer.match(/"/g) || []).length;
        if (quoteCount % 2 !== 0) {
            // Still inside a quoted field
            return null;
        }
        line = ctx.buffer;
        ctx.partial = false;
        ctx.buffer = "";
    }

    const fields = [];
    let i = 0;
    while (i < line.length) {
        if (line[i] === '"') {
            // Quoted field
            let j = i + 1;
            let value = "";
            while (j < line.length) {
                if (line[j] === '"') {
                    if (j + 1 < line.length && line[j + 1] === '"') {
                        value += '"';
                        j += 2;
                    } else {
                        j++; // closing quote
                        break;
                    }
                } else {
                    value += line[j];
                    j++;
                }
            }
            // Check if we reached end of line without closing quote
            if (j >= line.length && line[j - 1] !== '"') {
                ctx.partial = true;
                ctx.buffer = line;
                return null;
            }
            fields.push(value);
            // Skip comma after closing quote
            if (j < line.length && line[j] === ",") j++;
            i = j;
        } else {
            // Unquoted field
            let j = line.indexOf(",", i);
            if (j === -1) j = line.length;
            fields.push(line.substring(i, j));
            i = j + 1;
        }
    }
    // Handle trailing comma → empty last field
    if (line.endsWith(",")) fields.push("");

    return fields;
}

/**
 * Parse datetime string like "Feb 17, 2026, 12:14:06 AM" into a JS Date.
 * Returns null for empty/invalid strings.
 */
function parseDatetime(str) {
    if (!str || str.trim() === "") return null;
    const d = new Date(str.trim());
    return isNaN(d.getTime()) ? null : d;
}

function toNumeric(val) {
    if (!val || val.trim() === "") return null;
    const n = parseFloat(val);
    return isNaN(n) ? null : n;
}

function toInt(val) {
    if (!val || val.trim() === "") return null;
    const n = parseInt(val, 10);
    return isNaN(n) ? null : n;
}

function toBool(val) {
    if (!val || val.trim() === "") return false;
    const v = val.trim().toLowerCase();
    return v === "1" || v === "true";
}

function nullIfSentinel(val, sentinel) {
    if (!val || val.trim() === "" || val.trim() === sentinel) return null;
    return val.trim();
}

/**
 * Parse transcript text into [{role, content}, ...]
 */
function parseTranscript(transcript) {
    if (!transcript || transcript.trim() === "") return [];
    const regex = /(Assistant|User):\s*([\s\S]*?)(?=(?:Assistant|User):|$)/g;
    const messages = [];
    let match;
    while ((match = regex.exec(transcript)) !== null) {
        const role = match[1] === "Assistant" ? "assistant" : "user";
        const content = match[2].trim();
        if (content) {
            messages.push({ role, content });
        }
    }
    return messages;
}

// ── Ensure tables exist (idempotent) ─────────────────────────
async function ensureTables(client) {
    await client.query(`
    CREATE TABLE IF NOT EXISTS calls (
      id                                        SERIAL PRIMARY KEY,
      interaction_id                            TEXT UNIQUE NOT NULL,
      user_id                                   TEXT,
      connectivity_status                       TEXT,
      failure_reason                            TEXT,
      end_reason                                TEXT,
      duration_in_seconds                       NUMERIC(10,2),
      start_datetime                            TIMESTAMP,
      end_datetime                              TIMESTAMP,
      language_name                             TEXT,
      num_messages                              INTEGER,
      average_agent_response_time_in_seconds    NUMERIC(10,2),
      average_user_response_time_in_seconds     NUMERIC(10,2),
      user_contact_masked                       TEXT,
      channel_direction                         TEXT,
      retry_attempt                             INTEGER DEFAULT 0,
      campaign_id                               TEXT,
      cohort_id                                 TEXT,
      user_contact_hashed                       TEXT,
      is_debug_call                             BOOLEAN DEFAULT FALSE,
      audio_url                                 TEXT,
      server_retry_attempt                      INTEGER DEFAULT 0,
      has_log_issues                            BOOLEAN DEFAULT FALSE,
      channel_provider                          TEXT,
      channel_protocol                          TEXT,
      channel_type                              TEXT,
      provider_type                             TEXT,
      current_language                          TEXT,
      language_changed                          BOOLEAN DEFAULT FALSE,
      created_at                                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

    await client.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id             SERIAL PRIMARY KEY,
      call_id        INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
      role           TEXT    NOT NULL CHECK (role IN ('user', 'assistant')),
      content        TEXT    NOT NULL,
      message_order  INTEGER NOT NULL,
      UNIQUE (call_id, message_order)
    );
  `);

    await client.query(`
    CREATE TABLE IF NOT EXISTS ingest_log (
      id                  SERIAL PRIMARY KEY,
      filename            TEXT NOT NULL,
      run_at              TIMESTAMP DEFAULT NOW(),
      csv_rows            INTEGER,
      new_calls_added     INTEGER,
      messages_parsed     INTEGER
    );
  `);

    // Indexes (IF NOT EXISTS is safe to repeat)
    await client.query(`CREATE INDEX IF NOT EXISTS idx_calls_user_id        ON calls(user_id);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_calls_start_datetime ON calls(start_datetime);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_calls_end_reason     ON calls(end_reason);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_calls_language       ON calls(language_name);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_messages_call_id     ON messages(call_id);`);

    // Views
    await client.query(`
    CREATE OR REPLACE VIEW call_log AS
    SELECT
      c.interaction_id, c.user_contact_masked, c.start_datetime,
      c.end_datetime, c.duration_in_seconds, c.end_reason,
      c.connectivity_status, c.language_name, c.current_language,
      c.num_messages,
      c.average_agent_response_time_in_seconds,
      c.average_user_response_time_in_seconds,
      c.channel_direction, c.channel_provider, c.retry_attempt,
      c.has_log_issues, c.audio_url,
      COUNT(m.id) AS parsed_message_count
    FROM calls c
    LEFT JOIN messages m ON m.call_id = c.id
    GROUP BY c.id
    ORDER BY c.start_datetime DESC;
  `);

    await client.query(`
    CREATE OR REPLACE VIEW session_detail AS
    SELECT
      c.interaction_id, c.user_contact_masked, c.start_datetime,
      c.end_datetime, c.duration_in_seconds, c.language_name,
      c.current_language, c.end_reason, c.audio_url,
      m.message_order, m.role, m.content
    FROM calls c
    JOIN messages m ON m.call_id = c.id
    ORDER BY c.start_datetime DESC, m.message_order ASC;
  `);
}

// ── Main ─────────────────────────────────────────────────────
async function main() {
    const csvArg = getCSVArg();
    if (!csvArg) {
        console.error("Usage: node ingest_csv.js --file <csvfile>");
        console.error("  e.g. node ingest_csv.js --file BharatVistaarVoiceAgentLID-2026-02-26.csv");
        process.exit(1);
    }

    // Resolve CSV path: check script dir first, then cwd
    let csvPath;
    if (path.isAbsolute(csvArg)) {
        csvPath = csvArg;
    } else if (fs.existsSync(path.join(__dirname, csvArg))) {
        csvPath = path.join(__dirname, csvArg);
    } else if (fs.existsSync(csvArg)) {
        csvPath = path.resolve(csvArg);
    } else {
        console.error(`❌ CSV file not found: ${csvArg}`);
        console.error(`   Looked in: ${__dirname}/ and current directory`);
        process.exit(1);
    }

    const filename = path.basename(csvPath);

    console.log("");
    console.log("╔══════════════════════════════════════════════╗");
    console.log("║       BharatVistaar Daily CSV Ingest         ║");
    console.log("╠══════════════════════════════════════════════╣");
    console.log(`║ File : ${filename}`);
    console.log(`║ DB   : ${process.env.DB_USER}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`);
    console.log("╚══════════════════════════════════════════════╝");
    console.log("");

    const client = await pool.connect();

    try {
        // 1. Ensure schema
        console.log("⏳ Ensuring tables exist...");
        await ensureTables(client);
        console.log("✅ Tables ready\n");

        // 2. Stream-parse CSV and collect rows
        console.log("⏳ Reading CSV...");
        const rows = [];
        const transcripts = new Map(); // interaction_id → transcript

        const rl = readline.createInterface({
            input: fs.createReadStream(csvPath, { encoding: "utf-8" }),
            crlfDelay: Infinity,
        });

        let lineNum = 0;
        let headerSkipped = false;
        const ctx = { partial: false, buffer: "" };

        for await (const line of rl) {
            lineNum++;

            // Skip header
            if (!headerSkipped) {
                headerSkipped = true;
                continue;
            }

            const fields = parseCSVLine(line, ctx);
            if (fields === null) continue; // partial multiline row

            if (fields.length < 28) continue; // malformed row

            const interactionId = (fields[1] || "").trim();
            if (!interactionId) continue;

            const transcript = (fields[27] || "").trim();
            const currentLang = (fields[28] || "").trim() || null;
            const langChanged = toBool(fields[29] || "");

            rows.push({
                interaction_id: interactionId,
                user_id: (fields[2] || "").trim() || null,
                connectivity_status: (fields[3] || "").trim() || null,
                failure_reason: (fields[4] || "").trim() || null,
                end_reason: (fields[5] || "").trim() || null,
                duration_in_seconds: toNumeric(fields[6]),
                start_datetime: parseDatetime(fields[7]),
                end_datetime: parseDatetime(fields[8]),
                language_name: (fields[9] || "").trim() || null,
                num_messages: toInt(fields[10]),
                avg_agent_resp: toNumeric(fields[11]),
                avg_user_resp: toNumeric(fields[12]),
                user_contact_masked: (fields[13] || "").trim() || null,
                channel_direction: (fields[14] || "").trim() || null,
                retry_attempt: toInt(fields[15]) || 0,
                campaign_id: nullIfSentinel(fields[16], "NO_CAMPAIGN_ID"),
                cohort_id: nullIfSentinel(fields[17], "NO_COHORT_ID"),
                user_contact_hashed: (fields[18] || "").trim() || null,
                is_debug_call: toBool(fields[19]),
                audio_url: (fields[20] || "").trim() || null,
                server_retry_attempt: toInt(fields[21]) || 0,
                has_log_issues: toBool(fields[22]),
                channel_provider: (fields[23] || "").trim() || null,
                channel_protocol: (fields[24] || "").trim() || null,
                channel_type: (fields[25] || "").trim() || null,
                provider_type: (fields[26] || "").trim() || null,
                current_language: currentLang,
                language_changed: langChanged,
            });

            if (transcript) {
                transcripts.set(interactionId, transcript);
            }

            // Progress log every 50K rows
            if (rows.length % 50000 === 0) {
                console.log(`   ... ${rows.length.toLocaleString()} rows read`);
            }
        }

        console.log(`✅ CSV parsed: ${rows.length.toLocaleString()} data rows\n`);

        // 3. Batch insert calls with ON CONFLICT DO NOTHING
        console.log("⏳ Inserting calls (skipping duplicates)...");

        const BATCH_SIZE = 500;
        let newCalls = 0;

        await client.query("BEGIN");

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const values = [];
            const placeholders = [];

            batch.forEach((row, batchIdx) => {
                const offset = batchIdx * 28;
                placeholders.push(
                    `($${offset + 1},$${offset + 2},$${offset + 3},$${offset + 4},` +
                    `$${offset + 5},$${offset + 6},$${offset + 7},$${offset + 8},` +
                    `$${offset + 9},$${offset + 10},$${offset + 11},$${offset + 12},` +
                    `$${offset + 13},$${offset + 14},$${offset + 15},$${offset + 16},` +
                    `$${offset + 17},$${offset + 18},$${offset + 19},$${offset + 20},` +
                    `$${offset + 21},$${offset + 22},$${offset + 23},$${offset + 24},` +
                    `$${offset + 25},$${offset + 26},$${offset + 27},$${offset + 28})`
                );
                values.push(
                    row.interaction_id, row.user_id, row.connectivity_status,
                    row.failure_reason, row.end_reason, row.duration_in_seconds,
                    row.start_datetime, row.end_datetime, row.language_name,
                    row.num_messages, row.avg_agent_resp, row.avg_user_resp,
                    row.user_contact_masked, row.channel_direction, row.retry_attempt,
                    row.campaign_id, row.cohort_id, row.user_contact_hashed,
                    row.is_debug_call, row.audio_url, row.server_retry_attempt,
                    row.has_log_issues, row.channel_provider, row.channel_protocol,
                    row.channel_type, row.provider_type, row.current_language,
                    row.language_changed
                );
            });

            const result = await client.query(`
        INSERT INTO calls (
          interaction_id, user_id, connectivity_status, failure_reason,
          end_reason, duration_in_seconds, start_datetime, end_datetime,
          language_name, num_messages,
          average_agent_response_time_in_seconds,
          average_user_response_time_in_seconds,
          user_contact_masked, channel_direction, retry_attempt,
          campaign_id, cohort_id, user_contact_hashed,
          is_debug_call, audio_url, server_retry_attempt, has_log_issues,
          channel_provider, channel_protocol, channel_type, provider_type,
          current_language, language_changed
        ) VALUES ${placeholders.join(",")}
        ON CONFLICT (interaction_id) DO NOTHING
      `, values);

            newCalls += result.rowCount;

            if ((i + BATCH_SIZE) % 50000 < BATCH_SIZE) {
                console.log(`   ... ${Math.min(i + BATCH_SIZE, rows.length).toLocaleString()} / ${rows.length.toLocaleString()} processed`);
            }
        }

        console.log(`✅ Calls: ${newCalls.toLocaleString()} new, ${(rows.length - newCalls).toLocaleString()} skipped (duplicates)\n`);

        // 4. Parse transcripts for new calls (those with no messages yet)
        console.log("⏳ Parsing transcripts for new calls...");
        let messagesParsed = 0;

        if (newCalls > 0 && transcripts.size > 0) {
            // Get IDs of calls that have no messages yet
            const interactionIds = Array.from(transcripts.keys());

            // Process in batches to avoid huge IN clauses
            const MSG_BATCH = 200;
            for (let i = 0; i < interactionIds.length; i += MSG_BATCH) {
                const batch = interactionIds.slice(i, i + MSG_BATCH);
                const ph = batch.map((_, idx) => `$${idx + 1}`).join(",");

                const callRows = await client.query(`
          SELECT c.id, c.interaction_id
          FROM calls c
          WHERE c.interaction_id IN (${ph})
            AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.call_id = c.id)
        `, batch);

                for (const callRow of callRows.rows) {
                    const transcript = transcripts.get(callRow.interaction_id);
                    const msgs = parseTranscript(transcript);

                    for (let order = 0; order < msgs.length; order++) {
                        await client.query(`
              INSERT INTO messages (call_id, role, content, message_order)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (call_id, message_order) DO NOTHING
            `, [callRow.id, msgs[order].role, msgs[order].content, order + 1]);
                        messagesParsed++;
                    }
                }

                if ((i + MSG_BATCH) % 10000 < MSG_BATCH) {
                    console.log(`   ... transcripts: ${Math.min(i + MSG_BATCH, interactionIds.length).toLocaleString()} / ${interactionIds.length.toLocaleString()}`);
                }
            }
        }

        // 5. Log the run
        await client.query(`
      INSERT INTO ingest_log (filename, csv_rows, new_calls_added, messages_parsed)
      VALUES ($1, $2, $3, $4)
    `, [filename, rows.length, newCalls, messagesParsed]);

        await client.query("COMMIT");

        // 6. Summary
        const totals = await client.query(`
      SELECT 'calls' AS tbl, COUNT(*)::INTEGER AS total FROM calls
      UNION ALL
      SELECT 'messages' AS tbl, COUNT(*)::INTEGER AS total FROM messages
    `);

        console.log(`✅ Messages parsed: ${messagesParsed.toLocaleString()}\n`);
        console.log("═══════════════════════════════════════════════");
        console.log("  INGEST SUMMARY");
        console.log("═══════════════════════════════════════════════");
        console.log(`  File:             ${filename}`);
        console.log(`  CSV rows:         ${rows.length.toLocaleString()}`);
        console.log(`  New calls added:  ${newCalls.toLocaleString()}`);
        console.log(`  Duplicates skip:  ${(rows.length - newCalls).toLocaleString()}`);
        console.log(`  Messages parsed:  ${messagesParsed.toLocaleString()}`);
        console.log("───────────────────────────────────────────────");
        for (const row of totals.rows) {
            console.log(`  ${row.tbl.padEnd(16)}  ${parseInt(row.total).toLocaleString()} total`);
        }
        console.log("═══════════════════════════════════════════════\n");

    } catch (err) {
        await client.query("ROLLBACK").catch(() => { });
        console.error("\n❌ Ingest failed:", err.message);
        console.error(err.stack);
        process.exit(1);
    } finally {
        client.release();
        await pool.end();
    }
}

main();
