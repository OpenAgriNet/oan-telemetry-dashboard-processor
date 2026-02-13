// seed_villages_stream.js
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream");
const { parser } = require("stream-json");
const { streamArray } = require("stream-json/streamers/StreamArray");
const { Pool } = require("pg");
require("dotenv").config();

const FILE_PATH = path.join(__dirname, "village_list.json"); // adjust as needed
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 5000;

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: Number(process.env.DB_PORT || 5432),
  ssl: {
    rejectUnauthorized: false
  }
});

// replace existing insertBatch with this deduping version
async function insertBatch(client, rows) {
  if (!rows.length) return;

  // Deduplicate by village_code: keep the *last* occurrence in the batch.
  const map = new Map(); // village_code -> { village_code, taluka_code, district_code }
  for (const r of rows) {
    // r.village_code is numeric
    map.set(String(r.village_code), r);
  }
  const uniqueRows = Array.from(map.values());
  if (uniqueRows.length === 0) return;

  // Build multi-row parameterized INSERT from uniqueRows
  const values = [];
  const params = [];
  let p = 1;
  for (const r of uniqueRows) {
    values.push(`($${p++}, $${p++}, $${p++})`);
    params.push(r.village_code, r.taluka_code, r.district_code);
  }

  const sql = `
    INSERT INTO public.village_list (village_code, taluka_code, district_code)
    VALUES ${values.join(",")}
    ON CONFLICT (village_code) DO UPDATE
      SET taluka_code = EXCLUDED.taluka_code,
          district_code = EXCLUDED.district_code;
  `;
  await client.query(sql, params);
}

async function seed() {
  const client = await pool.connect();
  try {
    // Create the village_list table if it doesn't exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.village_list (
        village_code INTEGER PRIMARY KEY,
        taluka_code INTEGER,
        district_code INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await client.query("BEGIN");

    const input = fs.createReadStream(FILE_PATH);
    const jsonParser = parser();
    const arrStreamer = streamArray();

    let buffer = [];
    let processed = 0;
    let batches = 0;

    await new Promise((resolve, reject) => {
      pipeline(
        input,
        jsonParser,
        arrStreamer,
        async function (source) {
          try {
            for await (const { value } of source) {
              // pick only fields we need
              const village_code =
                value.village_code != null ? Number(value.village_code) : null;
              if (village_code == null) continue; // skip rows without village_code
              const taluka_code =
                value.taluka_code != null ? Number(value.taluka_code) : null;
              const district_code =
                value.district_code != null
                  ? Number(value.district_code)
                  : null;

              buffer.push({ village_code, taluka_code, district_code });
              processed++;

              if (buffer.length >= BATCH_SIZE) {
                await insertBatch(client, buffer);
                batches++;
                console.log(
                  `Inserted batch ${batches} (${processed} rows processed).`
                );
                buffer = [];
              }
            }

            // flush final buffer
            if (buffer.length > 0) {
              await insertBatch(client, buffer);
              batches++;
              console.log(
                `Inserted final batch ${batches} (${processed} rows processed total).`
              );
              buffer = [];
            }
            resolve();
          } catch (err) {
            reject(err);
          }
        },
        (err) => {
          if (err) reject(err);
        }
      );
    });

    await client.query("COMMIT");
    console.log(
      "Seeding completed successfully. Total rows processed:",
      processed
    );
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Seeding failed, rolled back:", err);
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

seed().catch((err) => {
  console.error("Fatal error in seeder:", err);
  process.exit(1);
});
