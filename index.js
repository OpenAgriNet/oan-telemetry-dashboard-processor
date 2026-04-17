/**
 * Telemetry Log Processor Microservice
 *
 * This service processes telemetry logs from PostgreSQL database
 * - Extracts configurable event types and stores in respective tables
 * - Updates sync_status after processing
 * - Runs every 5 minutes processing configurable batch size
 */
const fs = require("fs");
const path = require("path");
const express = require("express");
const { Pool } = require("pg");
const cron = require("node-cron");
const dotenv = require("dotenv");
const logger = require("./logger");
const { spawn } = require("child_process");

const {
  eventProcessors,
  loadEventProcessors,
  getNestedValue,
} = require("./eventProcessors");
const { forEach } = require("lodash");

// Load environment variables from .env file
dotenv.config();

// Create Express application
const app = express();
app.use(express.json());

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT || "5432", 10),
  max: parseInt(process.env.DB_POOL_MAX || "20", 10),
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT_MS || "30000", 10),
  connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT_MS || "5000", 10),
  ssl: {
    rejectUnauthorized: false
  }
});

// async function ensureVillagesSeeded() {
//   const client = await pool.connect();
//   try {
//     logger.info("Ensuring village_list table exists with expected columns...");

//     // Create table with full schema if it does not exist (safe)
//     await client.query(`
//       CREATE TABLE IF NOT EXISTS public.village_list (
//         village_code INTEGER PRIMARY KEY,
//         taluka_code INTEGER,
//         district_code INTEGER,
//         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//       );
//     `);

//     // Also ensure columns exist if older table was created with different schema
//     await client.query(`
//       ALTER TABLE public.village_list
//         ADD COLUMN IF NOT EXISTS taluka_code INTEGER,
//         ADD COLUMN IF NOT EXISTS district_code INTEGER,
//         ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
//     `);

//     // check if table already has data
//     const { rows } = await client.query(
//       "SELECT COUNT(*)::bigint AS cnt FROM public.village_list"
//     );
//     const cnt = Number(rows[0].cnt || 0);
//     logger.info(`village_list row count = ${cnt}`);

//     if (cnt > 0) {
//       logger.info("Villages table already seeded — skipping seeder.");
//       return;
//     }

//     // spawn the existing seeder script (do not edit the seeder file)
//     logger.info(
//       "Villages table empty — running seeder script (seed_villages_stream.js). This may take some time..."
//     );
//     const seederPath = path.join(__dirname, "seed_villages_stream.js");

//     const child = spawn(process.execPath, [seederPath], {
//       stdio: "inherit", // pipe seeder output to main stdout/stderr
//       env: process.env,
//       cwd: __dirname,
//     });

//     await new Promise((resolve, reject) => {
//       child.on("error", (err) => {
//         logger.error("Failed to start seeder process:", err);
//         reject(err);
//       });
//       child.on("exit", (code, signal) => {
//         if (code === 0) {
//           logger.info("Seeder completed successfully.");
//           resolve();
//         } else {
//           const msg = `Seeder exited with code ${code}${signal ? " signal " + signal : ""
//             }`;
//           logger.error(msg);
//           reject(new Error(msg));
//         }
//       });
//     });
//   } finally {
//     client.release();
//   }
// }

// call this at startup before starting the server
// (async () => {
//   try {
// await ensureVillagesSeeded();
// continue with the rest of your startup:
// await ensureTablesExist();
// await loadEventProcessors.loadFromDatabase(pool);
// start server...
//   } catch (err) {
//     logger.error("Startup aborted due to seeding failure:", err);
//     process.exit(1);
//   }
// })();

const PORT = process.env.PORT || 3000;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10', 10);
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "*/5 * * * *";
// const LEADERBOARD_REFRESH_SCHEDULE =
// process.env.LEADERBOARD_REFRESH_SCHEDULE || "0 1 * * *"; // Run at 1 AM every day
const IS_NEW_BACKFILL_SCHEDULE = process.env.IS_NEW_BACKFILL_SCHEDULE || "*/30 * * * *"; // Run every 30 minutes
const MV_REFRESH_SCHEDULE = process.env.MV_REFRESH_SCHEDULE || "*/15 * * * *"; // Refresh materialized views every 15 minutes

// ===== FAST MODE CONFIG =====
const FAST_MODE = (process.env.FAST_MODE || 'true').toLowerCase() === 'true'; // Enabled by default
let MICRO_BATCH_SIZE = parseInt(process.env.MICRO_BATCH_SIZE || '200', 10);
if (MICRO_BATCH_SIZE <= 0 || isNaN(MICRO_BATCH_SIZE)) {
  MICRO_BATCH_SIZE = parseInt(BATCH_SIZE, 10); // Process all at once if 0
}

// Ensure necessary tables exist
async function ensureTablesExist() {
  const client = await pool.connect();
  try {
    // Create winston_logs table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.winston_logs(
      id UUID DEFAULT gen_random_uuid(),
      level character varying COLLATE pg_catalog."default",
      message character varying COLLATE pg_catalog."default",
      meta json,
      "timestamp" timestamp without time zone DEFAULT now(),
      sync_status integer DEFAULT 0
    )
      `);

    // Add missing columns to winston_logs table if they don't exist
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'winston_logs' AND column_name = 'id') THEN
          ALTER TABLE public.winston_logs ADD COLUMN id UUID DEFAULT gen_random_uuid();
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'winston_logs' AND column_name = 'sync_status') THEN
          ALTER TABLE public.winston_logs ADD COLUMN sync_status INTEGER DEFAULT 0;
        END IF;
      END $$;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.dead_letter_logs(
      level character varying COLLATE pg_catalog."default",
      event_name character varying COLLATE pg_catalog."default",
      message character varying COLLATE pg_catalog."default",
      meta json,
      "timestamp" timestamp without time zone DEFAULT now(),
      sync_status integer DEFAULT 0
    )
      `);

    // Create questions table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.questions(
        id UUID DEFAULT gen_random_uuid(),
        unique_id VARCHAR,
        uid VARCHAR,
        sid VARCHAR,
        groupdetails TEXT,
        channel VARCHAR,
        ets BIGINT,
        questiontext TEXT,
        questionsource VARCHAR,
        answertext TEXT,
        answer TEXT,
        mobile VARCHAR,
        username VARCHAR,
        email VARCHAR,
        role VARCHAR,
        farmer_id VARCHAR,
        registered_location JSONB,
        device_location JSONB,
        agristack_location JSONB,
        fingerprint_id VARCHAR(64),
        created_at TIMESTAMP DEFAULT NOW()
      )
      `);

    // Create leaderboard table if not exists
    // await client.query(`
    //   CREATE TABLE IF NOT EXISTS public.leaderboard(
    //     unique_id VARCHAR NOT NULL,
    //     mobile VARCHAR,
    //     username VARCHAR,
    //     email VARCHAR,
    //     role VARCHAR,
    //     farmer_id VARCHAR,
    //     registered_location JSONB,
    //     record_count INTEGER,
    //     village_code BIGINT,
    //     taluka_code INTEGER,
    //     district_code INTEGER,
    //     last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    //     PRIMARY KEY(unique_id)
    //   )
    //   `);

    await client.query(`
      ALTER TABLE public.questions
  ADD COLUMN IF NOT EXISTS is_new SMALLINT DEFAULT 0 NOT NULL; `)

    // Add missing columns to questions table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'unique_id') THEN
          ALTER TABLE public.questions ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'mobile') THEN
          ALTER TABLE public.questions ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'username') THEN
          ALTER TABLE public.questions ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'email') THEN
          ALTER TABLE public.questions ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'role') THEN
          ALTER TABLE public.questions ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'farmer_id') THEN
          ALTER TABLE public.questions ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'registered_location') THEN
          ALTER TABLE public.questions ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'device_location') THEN
          ALTER TABLE public.questions ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'agristack_location') THEN
          ALTER TABLE public.questions ADD COLUMN agristack_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'questions' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.questions ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
      END $$;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.errorDetails(
      id UUID DEFAULT gen_random_uuid(),
      unique_id VARCHAR,
      uid VARCHAR,
      sid VARCHAR,
      groupdetails TEXT,
      channel VARCHAR,
      ets BIGINT,
      qid VARCHAR,
      errorText TEXT,
      mobile VARCHAR,
      username VARCHAR,
      email VARCHAR,
      role VARCHAR,
      farmer_id VARCHAR,
      registered_location JSONB,
      device_location JSONB,
      agristack_location JSONB,
      fingerprint_id VARCHAR(64),
      created_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Add missing columns to errorDetails table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'unique_id') THEN
          ALTER TABLE public.errorDetails ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'mobile') THEN
          ALTER TABLE public.errorDetails ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'username') THEN
          ALTER TABLE public.errorDetails ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'email') THEN
          ALTER TABLE public.errorDetails ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'role') THEN
          ALTER TABLE public.errorDetails ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'farmer_id') THEN
          ALTER TABLE public.errorDetails ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'registered_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'device_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'agristack_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN agristack_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'errordetails' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.errorDetails ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
      END $$;
    `);

    // Create feedback table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.feedback(
      id UUID DEFAULT gen_random_uuid(),
      unique_id VARCHAR,
      uid VARCHAR,
      sid VARCHAR,
      groupdetails JSONB,
      channel VARCHAR,
      ets BIGINT,
      feedbackText TEXT,
      questionText TEXT,
      answerText TEXT,
      qid VARCHAR,
      feedbackType TEXT,
      mobile VARCHAR,
      username VARCHAR,
      email VARCHAR,
      role VARCHAR,
      farmer_id VARCHAR,
      registered_location JSONB,
      device_location JSONB,
      agristack_location JSONB,
      fingerprint_id VARCHAR(64),
      created_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Add missing columns to feedback table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'unique_id') THEN
          ALTER TABLE public.feedback ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'mobile') THEN
          ALTER TABLE public.feedback ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'username') THEN
          ALTER TABLE public.feedback ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'email') THEN
          ALTER TABLE public.feedback ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'role') THEN
          ALTER TABLE public.feedback ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'farmer_id') THEN
          ALTER TABLE public.feedback ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'registered_location') THEN
          ALTER TABLE public.feedback ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'device_location') THEN
          ALTER TABLE public.feedback ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'agristack_location') THEN
          ALTER TABLE public.feedback ADD COLUMN agristack_location JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.feedback ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'feedback' AND column_name = 'feedback_source') THEN
          ALTER TABLE public.feedback ADD COLUMN feedback_source VARCHAR DEFAULT 'chat';
        END IF;
      END $$;
    `);

    // Create tts_details table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.tts_details(
      id UUID DEFAULT gen_random_uuid(),
      unique_id VARCHAR,
      uid VARCHAR,
      sid VARCHAR,
      channel VARCHAR,
      ets BIGINT,
      qid VARCHAR,
      apiType VARCHAR,
      apiService VARCHAR,
      success BOOLEAN,
      latencyMs NUMERIC,
      statusCode INTEGER,
      errorCode VARCHAR,
      errorMessage TEXT,
      language VARCHAR,
      sessionId VARCHAR,
      text TEXT,
      mobile VARCHAR,
      farmer_id VARCHAR,
      fingerprint_id VARCHAR(64),
      created_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Add missing columns to tts_details table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tts_details' AND column_name = 'unique_id') THEN
          ALTER TABLE public.tts_details ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tts_details' AND column_name = 'mobile') THEN
          ALTER TABLE public.tts_details ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tts_details' AND column_name = 'farmer_id') THEN
          ALTER TABLE public.tts_details ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tts_details' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.tts_details ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
      END $$;
    `);

    // Create asr_details table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.asr_details(
      id UUID DEFAULT gen_random_uuid(),
      unique_id VARCHAR,
      uid VARCHAR,
      sid VARCHAR,
      channel VARCHAR,
      ets BIGINT,
      qid VARCHAR,
      apiType VARCHAR,
      apiService VARCHAR,
      success BOOLEAN,
      latencyMs NUMERIC,
      statusCode INTEGER,
      errorCode VARCHAR,
      errorMessage TEXT,
      language VARCHAR,
      sessionId VARCHAR,
      text TEXT,
      mobile VARCHAR,
      farmer_id VARCHAR,
      fingerprint_id VARCHAR(64),
      created_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Add missing columns to asr_details table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'asr_details' AND column_name = 'unique_id') THEN
          ALTER TABLE public.asr_details ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'asr_details' AND column_name = 'mobile') THEN
          ALTER TABLE public.asr_details ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'asr_details' AND column_name = 'farmer_id') THEN
          ALTER TABLE public.asr_details ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'asr_details' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.asr_details ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
      END $$;
    `);

    // Create network_api_table for beckn network API telemetry
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.network_api_table(
      id SERIAL PRIMARY KEY,
      uid VARCHAR,
      sid VARCHAR,
      channel VARCHAR,
      ets BIGINT,
      input JSONB,
      output JSONB,
      success TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Add missing columns to network_api_table if they don't exist
    await client.query(`
      DO $$
    BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'uid') THEN
          ALTER TABLE public.network_api_table ADD COLUMN uid VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'sid') THEN
          ALTER TABLE public.network_api_table ADD COLUMN sid VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'channel') THEN
          ALTER TABLE public.network_api_table ADD COLUMN channel VARCHAR;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'ets') THEN
          ALTER TABLE public.network_api_table ADD COLUMN ets BIGINT;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'input') THEN
          ALTER TABLE public.network_api_table ADD COLUMN input JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'output') THEN
          ALTER TABLE public.network_api_table ADD COLUMN output JSONB;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'network_api_table' AND column_name = 'success') THEN
          ALTER TABLE public.network_api_table ADD COLUMN success TEXT;
        END IF;
      END $$;
    `);

    // Create event_processors table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.event_processors(
      id SERIAL PRIMARY KEY,
      event_type VARCHAR NOT NULL,
      table_name VARCHAR NOT NULL UNIQUE,
      field_verification VARCHAR NOT NULL,
      field_mappings JSONB NOT NULL,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
      `);

    // Ensure required columns/indexes exist for existing deployments
    await client.query(`
      DO $$
    BEGIN
        IF NOT EXISTS(
      SELECT 1 FROM information_schema.columns 
          WHERE table_schema = 'public' AND table_name = 'event_processors' AND column_name = 'field_verification'
    ) THEN
          ALTER TABLE public.event_processors ADD COLUMN field_verification VARCHAR;
        END IF;
      END $$;
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS event_processors_table_name_key ON public.event_processors(table_name);
    `);

    // Insert default event processors if they don't exist
    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
    VALUES
      ('OE_ITEM_RESPONSE', 'questions', '{
          "unique_id": "edata.eks.target.unique_id",
        "uid": "uid",
        "sid": "sid",
        "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
        "channel": "channel",
        "ets": "ets",
        "questionText": "edata.eks.target.questionsDetails.questionText",
        "questionSource": "edata.eks.target.questionsDetails.questionSource",
        "answerText": "edata.eks.target.questionsDetails.answerText",
        "answer": "edata.eks.target.questionsDetails.answerText.answer",
        "mobile": "edata.eks.target.mobile",
        "username": "edata.eks.target.username",
        "email": "edata.eks.target.email",
        "role": "edata.eks.target.role",
        "farmer_id": "edata.eks.target.farmer_id",
        "registered_location": "edata.eks.target.registered_location",
        "device_location": "edata.eks.target.device_location",
        "agristack_location": "edata.eks.target.agristack_location",
        "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }','edata.eks.target.questionsDetails')
      ON CONFLICT DO NOTHING;
  `);
    await client.query(`
      UPDATE public.event_processors
  SET
  event_type = 'OE_ITEM_RESPONSE',
    field_mappings = '{
  "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
      "sid": "sid",
        "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
          "channel": "channel",
            "ets": "ets",
              "questionText": "edata.eks.target.questionsDetails.questionText",
                "questionSource": "edata.eks.target.questionsDetails.questionSource",
                  "answerText": "edata.eks.target.questionsDetails.answerText",
                    "answer": "edata.eks.target.questionsDetails.answerText.answer",
                      "mobile": "edata.eks.target.mobile",
                        "username": "edata.eks.target.username",
                          "email": "edata.eks.target.email",
                            "role": "edata.eks.target.role",
                              "farmer_id": "edata.eks.target.farmer_id",
                                "registered_location": "edata.eks.target.registered_location",
                                  "device_location": "edata.eks.target.device_location",
                                    "agristack_location": "edata.eks.target.agristack_location",
                                      "fingerprint_id": "edata.eks.fingerprint_details.device_id"
} ',
field_verification = 'edata.eks.target.questionsDetails',
  updated_at = NOW()
      WHERE table_name = 'questions';
`);

    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'errorDetails', '{
          "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "errorText": "edata.eks.target.errorDetails.errorText",
    "qid": "edata.eks.qid",
    "mobile": "edata.eks.target.mobile",
    "username": "edata.eks.target.username",
    "email": "edata.eks.target.email",
    "role": "edata.eks.target.role",
    "farmer_id": "edata.eks.target.farmer_id",
    "registered_location": "edata.eks.target.registered_location",
    "device_location": "edata.eks.target.device_location",
    "agristack_location": "edata.eks.target.agristack_location",
    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }','edata.eks.target.errorDetails')
      ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
"unique_id": "edata.eks.target.unique_id",
  "uid": "uid",
    "sid": "sid",
      "channel": "channel",
        "ets": "ets",
          "errorText": "edata.eks.target.errorDetails.errorText",
            "qid": "edata.eks.qid",
              "mobile": "edata.eks.target.mobile",
                "username": "edata.eks.target.username",
                  "email": "edata.eks.target.email",
                    "role": "edata.eks.target.role",
                      "farmer_id": "edata.eks.target.farmer_id",
                        "registered_location": "edata.eks.target.registered_location",
                          "device_location": "edata.eks.target.device_location",
                            "agristack_location": "edata.eks.target.agristack_location",
                              "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }',
field_verification = 'edata.eks.target.errorDetails',
  updated_at = NOW()
      WHERE table_name = 'errorDetails';
`);

    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'feedback', '{
          "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
    "channel": "channel",
    "ets": "ets",
    "feedbackText": "edata.eks.target.feedbackDetails.feedbackText",
    "questionText": "edata.eks.target.feedbackDetails.questionText",
    "answerText": "edata.eks.target.feedbackDetails.answerText",
    "feedbackType": "edata.eks.target.feedbackDetails.feedbackType",
    "qid": "edata.eks.qid",
    "mobile": "edata.eks.target.mobile",
    "username": "edata.eks.target.username",
    "email": "edata.eks.target.email",
    "role": "edata.eks.target.role",
    "farmer_id": "edata.eks.target.farmer_id",
    "registered_location": "edata.eks.target.registered_location",
    "device_location": "edata.eks.target.device_location",
    "agristack_location": "edata.eks.target.agristack_location",
    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }','edata.eks.target.feedbackDetails')
      ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
"unique_id": "edata.eks.target.unique_id",
  "uid": "uid",
    "sid": "sid",
      "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
        "channel": "channel",
          "ets": "ets",
            "feedbackText": "edata.eks.target.feedbackDetails.feedbackText",
              "questionText": "edata.eks.target.feedbackDetails.questionText",
                "answerText": "edata.eks.target.feedbackDetails.answerText",
                  "feedbackType": "edata.eks.target.feedbackDetails.feedbackType",
                    "qid": "edata.eks.qid",
                      "mobile": "edata.eks.target.mobile",
                        "username": "edata.eks.target.username",
                          "email": "edata.eks.target.email",
                            "role": "edata.eks.target.role",
                              "farmer_id": "edata.eks.target.farmer_id",
                                "registered_location": "edata.eks.target.registered_location",
                                  "device_location": "edata.eks.target.device_location",
                                    "agristack_location": "edata.eks.target.agristack_location",
                                      "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }',
field_verification = 'edata.eks.target.feedbackDetails',
  updated_at = NOW()
      WHERE table_name = 'feedback';
`);

    // Register telefeedback (voice feedback) processor - inserts into feedback table via TABLE_NAME_OVERRIDES
    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'telefeedback', '{
    "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "feedbackText": "edata.eks.target.telefeedbackDetails.feedbackText",
    "feedbackType": "edata.eks.target.telefeedbackDetails.feedbackType",
    "mobile": "edata.eks.target.mobile",
    "username": "edata.eks.target.username",
    "email": "edata.eks.target.email",
    "role": "edata.eks.target.role",
    "farmer_id": "edata.eks.target.farmer_id",
    "registered_location": "edata.eks.target.registered_location",
    "device_location": "edata.eks.target.device_location",
    "agristack_location": "edata.eks.target.agristack_location"
  }','edata.eks.target.telefeedbackDetails')
ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
    "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "feedbackText": "edata.eks.target.telefeedbackDetails.feedbackText",
    "feedbackType": "edata.eks.target.telefeedbackDetails.feedbackType",
    "mobile": "edata.eks.target.mobile",
    "username": "edata.eks.target.username",
    "email": "edata.eks.target.email",
    "role": "edata.eks.target.role",
    "farmer_id": "edata.eks.target.farmer_id",
    "registered_location": "edata.eks.target.registered_location",
    "device_location": "edata.eks.target.device_location",
    "agristack_location": "edata.eks.target.agristack_location"
  }',
field_verification = 'edata.eks.target.telefeedbackDetails',
  updated_at = NOW()
      WHERE table_name = 'telefeedback';
`);

    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'tts_details', '{
          "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "qid": "edata.eks.qid",
    "apiType": "edata.eks.target.ttsResponseDetails.apiType",
    "apiService": "edata.eks.target.ttsResponseDetails.apiService",
    "success": "edata.eks.target.ttsResponseDetails.success",
    "latencyMs": "edata.eks.target.ttsResponseDetails.latencyMs",
    "statusCode": "edata.eks.target.ttsResponseDetails.statusCode",
    "errorCode": "edata.eks.target.ttsResponseDetails.errorCode",
    "errorMessage": "edata.eks.target.ttsResponseDetails.errorMessage",
    "language": "edata.eks.target.ttsResponseDetails.language",
    "sessionId": "edata.eks.target.ttsResponseDetails.sessionId",
    "text": "edata.eks.target.ttsResponseDetails.text",
    "mobile": "edata.eks.target.mobile",
    "farmer_id": "edata.eks.target.farmer_id",
    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }','edata.eks.target.ttsResponseDetails')
      ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
"unique_id": "edata.eks.target.unique_id",
  "uid": "uid",
    "sid": "sid",
      "channel": "channel",
        "ets": "ets",
          "qid": "edata.eks.qid",
            "apiType": "edata.eks.target.ttsResponseDetails.apiType",
              "apiService": "edata.eks.target.ttsResponseDetails.apiService",
                "success": "edata.eks.target.ttsResponseDetails.success",
                  "latencyMs": "edata.eks.target.ttsResponseDetails.latencyMs",
                    "statusCode": "edata.eks.target.ttsResponseDetails.statusCode",
                      "errorCode": "edata.eks.target.ttsResponseDetails.errorCode",
                        "errorMessage": "edata.eks.target.ttsResponseDetails.errorMessage",
                          "language": "edata.eks.target.ttsResponseDetails.language",
                            "sessionId": "edata.eks.target.ttsResponseDetails.sessionId",
                              "text": "edata.eks.target.ttsResponseDetails.text",
                                "mobile": "edata.eks.target.mobile",
                                  "farmer_id": "edata.eks.target.farmer_id",
                                    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }',
field_verification = 'edata.eks.target.ttsResponseDetails',
  updated_at = NOW()
      WHERE table_name = 'tts_details';
`);

    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'asr_details', '{
          "unique_id": "edata.eks.target.unique_id",
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "qid": "edata.eks.qid",
    "apiType": "edata.eks.target.asrResponseDetails.apiType",
    "apiService": "edata.eks.target.asrResponseDetails.apiService",
    "success": "edata.eks.target.asrResponseDetails.success",
    "latencyMs": "edata.eks.target.asrResponseDetails.latencyMs",
    "statusCode": "edata.eks.target.asrResponseDetails.statusCode",
    "errorCode": "edata.eks.target.asrResponseDetails.errorCode",
    "errorMessage": "edata.eks.target.asrResponseDetails.errorMessage",
    "language": "edata.eks.target.asrResponseDetails.language",
    "sessionId": "edata.eks.target.asrResponseDetails.sessionId",
    "text": "edata.eks.target.asrResponseDetails.text",
    "mobile": "edata.eks.target.mobile",
    "farmer_id": "edata.eks.target.farmer_id",
    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }','edata.eks.target.asrResponseDetails')
      ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
"unique_id": "edata.eks.target.unique_id",
  "uid": "uid",
    "sid": "sid",
      "channel": "channel",
        "ets": "ets",
          "qid": "edata.eks.qid",
            "apiType": "edata.eks.target.asrResponseDetails.apiType",
              "apiService": "edata.eks.target.asrResponseDetails.apiService",
                "success": "edata.eks.target.asrResponseDetails.success",
                  "latencyMs": "edata.eks.target.asrResponseDetails.latencyMs",
                    "statusCode": "edata.eks.target.asrResponseDetails.statusCode",
                      "errorCode": "edata.eks.target.asrResponseDetails.errorCode",
                        "errorMessage": "edata.eks.target.asrResponseDetails.errorMessage",
                          "language": "edata.eks.target.asrResponseDetails.language",
                            "sessionId": "edata.eks.target.asrResponseDetails.sessionId",
                              "text": "edata.eks.target.asrResponseDetails.text",
                                "mobile": "edata.eks.target.mobile",
                                  "farmer_id": "edata.eks.target.farmer_id",
                                    "fingerprint_id": "edata.eks.fingerprint_details.device_id"
        }',
field_verification = 'edata.eks.target.asrResponseDetails',
  updated_at = NOW()
      WHERE table_name = 'asr_details';
`);

    // Insert network API telemetry event processor
    await client.query(`
      INSERT INTO public.event_processors(event_type, table_name, field_mappings, field_verification)
VALUES
  ('OE_ITEM_RESPONSE', 'network_api_table', '{
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "input": "edata.eks.target.networkApiDetails.input",
    "output": "edata.eks.target.networkApiDetails.output",
    "success": "edata.eks.target.networkApiDetails.success"
        }','edata.eks.target.networkApiDetails')
      ON CONFLICT DO NOTHING;
`);
    await client.query(`
      UPDATE public.event_processors
SET
event_type = 'OE_ITEM_RESPONSE',
  field_mappings = '{
    "uid": "uid",
    "sid": "sid",
    "channel": "channel",
    "ets": "ets",
    "input": "edata.eks.target.networkApiDetails.input",
    "output": "edata.eks.target.networkApiDetails.output",
    "success": "edata.eks.target.networkApiDetails.success"
        }',
field_verification = 'edata.eks.target.networkApiDetails',
  updated_at = NOW()
      WHERE table_name = 'network_api_table';
`);


    // ============================================
    // V2 SCHEMA: Users and Sessions Tables (Simplified - no devices table)
    // ============================================

    // Create users table for V2 (includes device metadata)
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.users(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  uid VARCHAR(255) UNIQUE NOT NULL,
  --User profile
        mobile VARCHAR(20),
  username VARCHAR(255),
  email VARCHAR(255),
  role VARCHAR(50),
  farmer_id VARCHAR(100),
  registered_location JSONB,
  device_location JSONB,
  agristack_location JSONB,
  --Device metadata(Fingerprint.js)
        fingerprint_id VARCHAR(64),
  browser_code VARCHAR(10),
  browser_name VARCHAR(50),
  browser_version VARCHAR(20),
  device_code VARCHAR(10),
  device_name VARCHAR(50),
  device_model VARCHAR(100),
  os_code VARCHAR(10),
  os_name VARCHAR(50),
  os_version VARCHAR(20),
  --Timestamps
        first_seen_at TIMESTAMP,
  last_seen_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW()
)
  `);
    // Add device columns if they don't exist (for existing deployments)
    await client.query(`
      DO $$
BEGIN 
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'fingerprint_id') THEN
          ALTER TABLE public.users ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'browser_code') THEN
          ALTER TABLE public.users ADD COLUMN browser_code VARCHAR(10);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'browser_name') THEN
          ALTER TABLE public.users ADD COLUMN browser_name VARCHAR(50);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'browser_version') THEN
          ALTER TABLE public.users ADD COLUMN browser_version VARCHAR(20);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'device_code') THEN
          ALTER TABLE public.users ADD COLUMN device_code VARCHAR(10);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'device_name') THEN
          ALTER TABLE public.users ADD COLUMN device_name VARCHAR(50);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'device_model') THEN
          ALTER TABLE public.users ADD COLUMN device_model VARCHAR(100);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'os_code') THEN
          ALTER TABLE public.users ADD COLUMN os_code VARCHAR(10);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'os_name') THEN
          ALTER TABLE public.users ADD COLUMN os_name VARCHAR(50);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'os_version') THEN
          ALTER TABLE public.users ADD COLUMN os_version VARCHAR(20);
        END IF;
        IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'channel') THEN
          ALTER TABLE public.users ADD COLUMN channel VARCHAR;
        END IF;
      END $$;
`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_uid ON users(uid)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_mobile ON users(mobile)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen_at)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_fingerprint ON users(fingerprint_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_browser ON users(browser_code)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_device_type ON users(device_code)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_os ON users(os_code)`);

    // ===== PERFORMANCE INDEXES =====
    // winston_logs: critical for the main processing query (WHERE sync_status = 0 ORDER BY timestamp)
    await client.query(`CREATE INDEX IF NOT EXISTS idx_winston_logs_sync_status ON winston_logs(sync_status) WHERE sync_status = 0`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_winston_logs_timestamp ON winston_logs("timestamp")`);
    // Composite index for the exact query pattern used in processTelemetryLogs
    await client.query(`CREATE INDEX IF NOT EXISTS idx_winston_logs_sync_ts ON winston_logs(sync_status, "timestamp" ASC) WHERE sync_status = 0`);

    // questions table indexes
    await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_created_at ON questions(created_at)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_uid ON questions(uid)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_sid ON questions(sid)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_ets ON questions(ets)`);

    // feedback table indexes
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON feedback(created_at)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_uid ON feedback(uid)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_feedback_source ON feedback(feedback_source)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_feedbacktype ON feedback(feedbacktype)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_channel ON feedback(channel)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_ets ON feedback(ets)`);

    // errorDetails table indexes
    await client.query(`CREATE INDEX IF NOT EXISTS idx_errordetails_created_at ON errordetails(created_at)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_errordetails_uid ON errordetails(uid)`);

    // dead_letter_logs index
    await client.query(`CREATE INDEX IF NOT EXISTS idx_dead_letter_event_name ON dead_letter_logs(event_name)`);

    logger.info("Performance indexes created/verified");

    // ============================================
    // VOICE TELEMETRY: calls source column + voice_call_tracking
    // ============================================

    await client.query(`
      ALTER TABLE calls ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'csv';
    `);
    await client.query(`
      UPDATE calls SET source = 'csv' WHERE source IS NULL;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.voice_call_tracking (
        sid TEXT PRIMARY KEY,
        call_id INTEGER REFERENCES calls(id),
        turns_processed INTEGER DEFAULT 0,
        last_ets BIGINT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_voice_tracking_sid ON voice_call_tracking(sid);
    `);

    // Create sessions table for V2 (no device_id - device info stored in users)
    //     await client.query(`
    //       CREATE TABLE IF NOT EXISTS public.sessions(
    //   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    //   session_id VARCHAR(64) UNIQUE NOT NULL,
    //   user_id VARCHAR(255),
    //   channel VARCHAR(255),
    //   session_start_at BIGINT NOT NULL,
    //   session_end_at BIGINT,
    //   duration_seconds INTEGER,
    //   render_duration_ms INTEGER,
    //   server_duration_ms INTEGER,
    //   created_at TIMESTAMP DEFAULT NOW()
    // )
    //   `);
    // Migration: Change user_id from UUID to VARCHAR for existing tables
    //     await client.query(`
    //       DO $$
    // BEGIN 
    //         IF EXISTS(
    //   SELECT 1 FROM information_schema.columns 
    //           WHERE table_schema = 'public' AND table_name = 'sessions' AND column_name = 'user_id' AND data_type = 'uuid'
    // ) THEN
    //           ALTER TABLE public.sessions ALTER COLUMN user_id TYPE VARCHAR(255);
    //         END IF;
    //       END $$;
    // `);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id)`);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)`);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(session_start_at)`);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_channel ON sessions(channel)`);

    // Add session_id to existing tables for V2 linking
    // await client.query(`
    //   ALTER TABLE questions ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    // `);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_session ON questions(session_id)`);

    // await client.query(`
    //   ALTER TABLE feedback ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    // `);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_session ON feedback(session_id)`);

    // await client.query(`
    //   ALTER TABLE errorDetails ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    // `);
    // await client.query(`CREATE INDEX IF NOT EXISTS idx_errordetails_session ON errorDetails(session_id)`);

    logger.info("Database tables verified and created if needed (including V2 schema)");
  } catch (err) {
    logger.error("Error ensuring tables exist:", err);
    throw err;
  } finally {
    client.release();
  }
}

// ============================================
// V2 PROCESSING FUNCTIONS
// ============================================

/**
 * Process and UPSERT user data from telemetry event (includes device metadata)
 * @param {Object} client - Database client
 * @param {Object} event - Telemetry event
 * @returns {string|null} - User UUID or null
 */
async function processUserData(client, event) {
  try {
    const target = event.edata?.eks?.target || {};
    const fingerprint = event.edata?.eks?.fingerprint_details;
    const eventTimestamp = new Date(Number(event.ets));

    // Use fingerprint device_id as uid if available, otherwise fallback to event.uid
    let uid = fingerprint?.device_id ? `fp_${fingerprint.device_id}` : event.uid;

    if (!uid || uid === 'guest') {
      logger.debug(`Skipping user upsert - no fingerprint and uid is guest / empty`);
      return null;
    }

    // Debug logging to trace data extraction
    logger.debug(`processUserData: uid = ${uid}, fingerprint_id = ${fingerprint?.device_id || 'null'}, browser = ${fingerprint?.browser?.name || 'null'}, device = ${fingerprint?.device?.name || 'null'}, os = ${fingerprint?.os?.name || 'null'} `);

    const result = await client.query(`
      INSERT INTO users(uid, mobile, username, email, role, farmer_id, channel,
  registered_location, device_location, agristack_location,
  fingerprint_id, browser_code, browser_name, browser_version,
  device_code, device_name, device_model,
  os_code, os_name, os_version,
  first_seen_at, last_seen_at)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
      ON CONFLICT(uid) DO UPDATE SET
mobile = COALESCE(EXCLUDED.mobile, users.mobile),
  username = COALESCE(EXCLUDED.username, users.username),
  email = COALESCE(EXCLUDED.email, users.email),
  role = COALESCE(EXCLUDED.role, users.role),
  farmer_id = COALESCE(EXCLUDED.farmer_id, users.farmer_id),
  channel = COALESCE(EXCLUDED.channel, users.channel),
  registered_location = COALESCE(EXCLUDED.registered_location, users.registered_location),
  device_location = COALESCE(EXCLUDED.device_location, users.device_location),
  agristack_location = COALESCE(EXCLUDED.agristack_location, users.agristack_location),
  fingerprint_id = COALESCE(EXCLUDED.fingerprint_id, users.fingerprint_id),
  browser_code = COALESCE(EXCLUDED.browser_code, users.browser_code),
  browser_name = COALESCE(EXCLUDED.browser_name, users.browser_name),
  browser_version = COALESCE(EXCLUDED.browser_version, users.browser_version),
  device_code = COALESCE(EXCLUDED.device_code, users.device_code),
  device_name = COALESCE(EXCLUDED.device_name, users.device_name),
  device_model = COALESCE(EXCLUDED.device_model, users.device_model),
  os_code = COALESCE(EXCLUDED.os_code, users.os_code),
  os_name = COALESCE(EXCLUDED.os_name, users.os_name),
  os_version = COALESCE(EXCLUDED.os_version, users.os_version),
  last_seen_at = EXCLUDED.last_seen_at
      RETURNING id
    `, [
      uid,
      target.mobile || null,
      target.username || null,
      target.email || null,
      target.role || null,
      target.farmer_id || null,
      event.channel || null,
      target.registered_location || null,
      target.device_location || null,
      target.agristack_location || null,
      fingerprint?.device_id || null,
      fingerprint?.browser?.code || null,
      fingerprint?.browser?.name || null,
      fingerprint?.browser?.version || null,
      fingerprint?.device?.code || null,
      fingerprint?.device?.name || null,
      fingerprint?.device?.model || null,
      fingerprint?.os?.code || null,
      fingerprint?.os?.name || null,
      fingerprint?.os?.version || null,
      eventTimestamp,
      eventTimestamp
    ]);

    const userId = result.rows[0]?.id;
    logger.debug(`User upserted: uid = ${uid}, id = ${userId}, fingerprint = ${fingerprint?.device_id || 'none'} `);
    return userId;
  } catch (err) {
    logger.error(`Error processing user data: ${err.message} `);
    return null;
  }
}

// NOTE: processDeviceData removed - device metadata now stored in users table via processUserData

/**
 * Process session start (OE_START event)
 * @param {Object} client - Database client
 * @param {Object} event - Telemetry event
 * @param {string|null} uid - User UID (fingerprint-based or original)
 * @returns {string|null} - Session UUID or null
 */
// async function processSessionStart(client, event, uid) {
//   try {
//     const sid = event.sid;
//     if (!sid) {
//       logger.debug('No session_id (sid) found, skipping session insert');
//       return null;
//     }

//     const fingerprint = event.edata?.eks?.fingerprint_details;
//     const sessionStartAt = fingerprint?.session?.session_start_at || event.ets;

//     const result = await client.query(`
//       INSERT INTO sessions(session_id, user_id, channel, session_start_at)
// VALUES($1, $2, $3, $4)
//       ON CONFLICT(session_id) DO UPDATE SET
// user_id = COALESCE(EXCLUDED.user_id, sessions.user_id)
//       RETURNING id
//     `, [sid, uid, event.channel, sessionStartAt]);

//     const sessionId = result.rows[0]?.id;
//     logger.debug(`Session started: sid = ${sid}, id = ${sessionId}, uid = ${uid} `);
//     return sessionId;
//   } catch (err) {
//     logger.error(`Error processing session start: ${err.message} `);
//     return null;
//   }
// }

/**
 * Process session end (OE_END event) - update with end time, duration, performance
 * @param {Object} client - Database client
 * @param {Object} event - Telemetry event
 */
// async function processSessionEnd(client, event) {
//   try {
//     const sid = event.sid;
//     if (!sid) {
//       logger.debug('No session_id (sid) found, skipping session end update');
//       return;
//     }

//     const fingerprint = event.edata?.eks?.fingerprint_details;
//     const sessionEndAt = fingerprint?.session?.session_end_at || event.ets;
//     const sessionStartAt = fingerprint?.session?.session_start_at;

//     // Calculate duration in seconds
//     let durationSeconds = null;
//     if (sessionStartAt && sessionEndAt) {
//       durationSeconds = Math.floor((sessionEndAt - sessionStartAt) / 1000);
//     }

//     // Performance metrics
//     const renderDurationMs = fingerprint?.performance?.render_duration_ms || null;
//     const serverDurationMs = fingerprint?.performance?.server_duration_ms || null;

//     await client.query(`
//       UPDATE sessions SET
// session_end_at = $1,
//   duration_seconds = $2,
//   render_duration_ms = $3,
//   server_duration_ms = $4
//       WHERE session_id = $5
//   `, [sessionEndAt, durationSeconds, renderDurationMs, serverDurationMs, sid]);

//     logger.debug(`Session ended: sid = ${sid}, duration = ${durationSeconds} s`);
//   } catch (err) {
//     logger.error(`Error processing session end: ${err.message} `);
//   }
// }

/**
 * Get session UUID by session_id (sid)
 * @param {Object} client - Database client
 * @param {string} sid - Session ID from telemetry
 * @returns {string|null} - Session UUID or null
 */
// async function getSessionIdBySid(client, sid) {
//   try {
//     if (!sid) return null;
//     const result = await client.query(
//       'SELECT id FROM sessions WHERE session_id = $1',
//       [sid]
//     );
//     return result.rows[0]?.id || null;
//   } catch (err) {
//     logger.error(`Error getting session id: ${err.message} `);
//     return null;
//   }
// }

// Parse telemetry message JSON
function parseTelemetryMessage(message, batchId = '', logIndex = 0) {
  try {
    logger.debug(`[${batchId}][Log ${logIndex}] Parsing telemetry message(length: ${message?.length || 0} chars)`);
    // Parse the message string which is a JSON string
    const parsedMessage = JSON.parse(message);
    // Return the events array from the parsed message
    const events = parsedMessage.events || [];
    logger.debug(`[${batchId}][Log ${logIndex}] Successfully parsed message, found ${events.length} events`);
    return events;
  } catch (err) {
    const messagePreview = message?.substring(0, 100) || 'empty';
    logger.error(`[${batchId}][Log ${logIndex}] Error parsing telemetry message: ${err.message} `);
    logger.error(`[${batchId}][Log ${logIndex}] Message preview: ${messagePreview}...`);
    return [];
  }
}

// Process telemetry logs — processes in smaller transaction batches for fault tolerance
const SLOW_BATCH_SIZE = parseInt(process.env.SLOW_BATCH_SIZE || '1000', 10);

async function processTelemetryLogs(batchId = `batch_${Date.now()} `) {
  logger.info(`[${batchId}] Step 1: Fetching unprocessed logs (BATCH_SIZE: ${BATCH_SIZE})...`);

  // Fetch logs outside a transaction (read-only, no lock needed)
  const fetchClient = await pool.connect();
  let allRows;
  try {
    const queryStartTime = Date.now();
    const result = await fetchClient.query(
      `SELECT id, level, message, meta, cast(to_char(("timestamp"):: TIMESTAMP, 'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE]
    );
    const queryDuration = Date.now() - queryStartTime;
    logger.info(`[${batchId}] Step 1: Fetch completed in ${queryDuration}ms, found ${result.rows.length} logs`);
    allRows = result.rows;
  } finally {
    fetchClient.release();
  }

  if (allRows.length === 0) {
    logger.info(`[${batchId}] No new logs to process`);
    return { processed: 0, status: "success" };
  }

  let totalProcessed = 0;
  let totalEventsProcessed = 0;
  let totalDeadLetter = 0;
  let batchIndex = 0;

  // Process in smaller transaction batches for fault tolerance
  for (let i = 0; i < allRows.length; i += SLOW_BATCH_SIZE) {
    batchIndex++;
    const chunk = allRows.slice(i, i + SLOW_BATCH_SIZE);
    const client = await pool.connect();
    const batchStart = Date.now();

    try {
      await client.query("BEGIN");
      logger.info(`[${batchId}][Batch-${batchIndex}] Processing ${chunk.length} logs...`);

      for (let logIndex = 0; logIndex < chunk.length; logIndex++) {
        const log = chunk[logIndex];
        const events = parseTelemetryMessage(log.message, batchId, logIndex + 1);

        for (const event of events) {
          const eventType = event.eid;
          const eventUid = event.uid || 'unknown';
          const eventMid = event.mid || 'unknown';

          let eventProcessed = false;

          // Handle voice telemetry events
          if (eventType === 'OE_VOICE_RESPONSE') {
            await processVoiceResponse(client, event);
            eventProcessed = true;
            totalEventsProcessed++;
            continue;
          }

          const hasTtsResponse = getNestedValue(event, 'edata.eks.target.ttsResponseDetails');
          const hasAsrResponse = getNestedValue(event, 'edata.eks.target.asrResponseDetails');
          const hasTeleFeedback = getNestedValue(event, 'edata.eks.target.telefeedbackDetails');

          const priorityProcessors = [];
          const regularProcessors = [];
          for (const key in eventProcessors) {
            const processor = eventProcessors[key];
            if (processor["tableName"] === 'tts_details' || processor["tableName"] === 'asr_details') {
              priorityProcessors.push({ key, processor });
            } else {
              regularProcessors.push({ key, processor });
            }
          }
          const orderedProcessors = [...priorityProcessors, ...regularProcessors];

          for (const { key, processor } of orderedProcessors) {
            const verified = getNestedValue(event, processor["fieldVerification"]);

            const hasVoiceResponse = getNestedValue(event, 'edata.eks.target.questionsDetails.responseText');
            if (processor["tableName"] === 'questions' && (hasAsrResponse || hasTtsResponse || hasVoiceResponse || hasTeleFeedback)) {
              continue;
            }

            if (processor["eventType"] === eventType && verified !== undefined && !eventProcessed) {
              if (processor["tableName"] === "questions") {
                const hasTtsData = getNestedValue(event, "edata.eks.target.ttsResponseDetails");
                const hasAsrData = getNestedValue(event, "edata.eks.target.asrResponseDetails");
                const hasTeleFeedbackData = getNestedValue(event, "edata.eks.target.telefeedbackDetails");
                if (hasTtsData !== undefined || hasAsrData !== undefined || hasTeleFeedbackData !== undefined) {
                  continue;
                }
              }

              await processor["process"](client, event);
              eventProcessed = true;
              totalEventsProcessed++;
              break;
            }
          }

          if (eventType !== "OE_END" && eventType !== "OE_START" && !eventProcessed) {
            await client.query(
              `Insert into dead_letter_logs(level, message, meta, event_name) values($1, $2, $3, $4)`,
              [log.level, JSON.stringify(event), log.meta, eventType]
            );
            totalDeadLetter++;
          } else if (eventType === "OE_START") {
            try {
              await processUserData(client, event);
            } catch (v2Err) {
              logger.error(`[${batchId}][Batch-${batchIndex}] V2 OE_START error: ${v2Err.message}`);
            }
          } else if (eventType === "OE_END") {
            try {
              const fingerprint = event.edata?.eks?.fingerprint_details;
              const uid = fingerprint?.device_id ? `fp_${fingerprint.device_id}` : event.uid;
              if (uid && uid !== 'guest') {
                await client.query(`UPDATE users SET last_seen_at = NOW() WHERE uid = $1`, [uid]);
              }
            } catch (v2Err) {
              logger.error(`[${batchId}][Batch-${batchIndex}] V2 OE_END error: ${v2Err.message}`);
            }
          }
        }

        await client.query(
          `UPDATE winston_logs SET sync_status = 1 WHERE id = $1`,
          [log.id]
        );
      }

      await client.query("COMMIT");
      totalProcessed += chunk.length;

      const batchDuration = Date.now() - batchStart;
      logger.info(`[${batchId}][Batch-${batchIndex}] Committed ${chunk.length} logs in ${batchDuration}ms`);

    } catch (err) {
      await client.query("ROLLBACK");
      const batchDuration = Date.now() - batchStart;
      logger.error(`[${batchId}][Batch-${batchIndex}] FAILED after ${batchDuration}ms: ${err.message}`);
      // Continue with next batch — partial progress is preserved
    } finally {
      client.release();
    }
  }

  logger.info(`[${batchId}] ========== PROCESSING SUMMARY ==========`);
  logger.info(`[${batchId}] Logs processed: ${totalProcessed}/${allRows.length}`);
  logger.info(`[${batchId}] Total events processed: ${totalEventsProcessed}`);
  logger.info(`[${batchId}] Dead letter events: ${totalDeadLetter}`);
  logger.info(`[${batchId}] Transaction batches: ${batchIndex} (size: ${SLOW_BATCH_SIZE})`);
  logger.info(`[${batchId}] ==========================================`);

  return { processed: totalProcessed, status: "success", eventsProcessed: totalEventsProcessed, deadLetterCount: totalDeadLetter };
}

// ============================================
// FAST MODE: Bulk processing with micro-batch transactions
// ============================================
async function processTelemetryLogsFast(batchId = `fast_${Date.now()}`) {
  logger.info(`[${batchId}][FAST] Step 1: Fetching unprocessed logs (BATCH_SIZE: ${BATCH_SIZE})...`);

  // Fetch logs outside a transaction (read-only, no lock needed)
  const fetchClient = await pool.connect();
  let allRows;
  try {
    const queryStartTime = Date.now();
    const result = await fetchClient.query(
      `SELECT id, level, message, meta, cast(to_char(("timestamp"):: TIMESTAMP, 'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE]
    );
    const queryDuration = Date.now() - queryStartTime;
    logger.info(`[${batchId}][FAST] Step 1: Fetch completed in ${queryDuration}ms, found ${result.rows.length} logs`);
    allRows = result.rows;
  } finally {
    fetchClient.release();
  }

  if (allRows.length === 0) {
    logger.info(`[${batchId}][FAST] No new logs to process`);
    return { processed: 0, status: "success" };
  }

  let totalProcessed = 0;
  let totalEventsProcessed = 0;
  let totalDeadLetter = 0;
  let microBatchIndex = 0;

  // Process in micro-batches
  for (let i = 0; i < allRows.length; i += MICRO_BATCH_SIZE) {
    microBatchIndex++;
    const chunk = allRows.slice(i, i + MICRO_BATCH_SIZE);
    const client = await pool.connect();
    const microBatchStart = Date.now();

    try {
      await client.query("BEGIN");
      logger.info(`[${batchId}][FAST][Micro-${microBatchIndex}] Processing ${chunk.length} logs...`);

      // Collect all events from all logs in this micro-batch, grouped by target table
      const insertBatches = {}; // tableName -> [{fields, values}]
      const deadLetterRows = []; // [{level, event, meta, eventType}]
      const userUpsertMap = new Map(); // uid -> {event, fingerprint, eventTimestamp}
      const userLastSeenMap = new Map(); // uid -> true (for OE_END events)
      const processedIds = [];
      const voiceEvents = []; // Collect voice events for batched processing

      for (const log of chunk) {
        const events = parseTelemetryMessage(log.message, batchId, 0);

        for (const event of events) {
          const eventType = event.eid;

          let eventProcessed = false;

          // Collect voice telemetry events for batched processing
          if (eventType === 'OE_VOICE_RESPONSE') {
            voiceEvents.push(event);
            eventProcessed = true;
            totalEventsProcessed++;
            continue;
          }

          // Check for TTS/ASR/TeleFeedback
          const hasTtsResponse = getNestedValue(event, 'edata.eks.target.ttsResponseDetails');
          const hasAsrResponse = getNestedValue(event, 'edata.eks.target.asrResponseDetails');
          const hasTeleFeedback = getNestedValue(event, 'edata.eks.target.telefeedbackDetails');

          // Separate priority processors from regular ones (same logic as original)
          const priorityProcessors = [];
          const regularProcessors = [];
          for (const key in eventProcessors) {
            const processor = eventProcessors[key];
            if (processor["tableName"] === 'tts_details' || processor["tableName"] === 'asr_details') {
              priorityProcessors.push({ key, processor });
            } else {
              regularProcessors.push({ key, processor });
            }
          }
          const orderedProcessors = [...priorityProcessors, ...regularProcessors];

          for (const { key, processor } of orderedProcessors) {
            const verified = getNestedValue(event, processor["fieldVerification"]);

            // Skip questions processor for ASR/TTS/voice/TeleFeedback events
            const hasVoiceResponse = getNestedValue(event, 'edata.eks.target.questionsDetails.responseText');
            if (processor["tableName"] === 'questions' && (hasAsrResponse || hasTtsResponse || hasVoiceResponse || hasTeleFeedback)) {
              continue;
            }

            if (processor["eventType"] === eventType && verified !== undefined && !eventProcessed) {
              if (processor["tableName"] === "questions") {
                const hasTtsData = getNestedValue(event, "edata.eks.target.ttsResponseDetails");
                const hasAsrData = getNestedValue(event, "edata.eks.target.asrResponseDetails");
                const hasVoiceData = getNestedValue(event, "edata.eks.target.questionsDetails.responseText");
                const hasTeleFeedbackData = getNestedValue(event, "edata.eks.target.telefeedbackDetails");
                if (hasTtsData !== undefined || hasAsrData !== undefined || hasVoiceData !== undefined || hasTeleFeedbackData !== undefined) {
                  continue;
                }
              }

              // Instead of individual INSERT, collect for batched insert
              const tableName = processor["tableName"];
              const fieldMappings = processor["fieldMappings"] || {};

              if (!insertBatches[tableName]) {
                insertBatches[tableName] = { fields: null, rows: [], fieldMappings };
              }

              // Use the processor's process function directly (still most reliable way)
              // but batch-execute them to avoid per-event overhead
              await processor["process"](client, event);

              eventProcessed = true;
              totalEventsProcessed++;
              break;
            }
          }

          if (eventType !== "OE_END" && eventType !== "OE_START" && !eventProcessed) {
            deadLetterRows.push({ level: log.level, event, meta: log.meta, eventType });
          } else if (eventType === "OE_START") {
            // Collect user upserts (deduplicated by uid)
            const fingerprint = event.edata?.eks?.fingerprint_details;
            const uid = fingerprint?.device_id ? `fp_${fingerprint.device_id}` : event.uid;
            if (uid && uid !== 'guest') {
              userUpsertMap.set(uid, event); // keep last event per uid
            }
          } else if (eventType === "OE_END") {
            const fingerprint = event.edata?.eks?.fingerprint_details;
            const uid = fingerprint?.device_id ? `fp_${fingerprint.device_id}` : event.uid;
            if (uid && uid !== 'guest') {
              userLastSeenMap.set(uid, true);
              // Also collect for user upsert if not already seen
              if (!userUpsertMap.has(uid)) {
                userUpsertMap.set(uid, event);
              }
            }
          }
        }

        processedIds.push(log.id);
      }

      // Batch insert dead letter logs
      if (deadLetterRows.length > 0) {
        const dlValues = [];
        const dlPlaceholders = [];
        let dlIdx = 1;
        for (const dl of deadLetterRows) {
          dlPlaceholders.push(`($${dlIdx}, $${dlIdx + 1}, $${dlIdx + 2}, $${dlIdx + 3})`);
          dlValues.push(dl.level, JSON.stringify(dl.event), dl.meta, dl.eventType);
          dlIdx += 4;
        }
        await client.query(
          `INSERT INTO dead_letter_logs(level, message, meta, event_name) VALUES ${dlPlaceholders.join(', ')}`,
          dlValues
        );
        totalDeadLetter += deadLetterRows.length;
      }

      // Batch user upserts (deduplicated)
      for (const [uid, event] of userUpsertMap) {
        try {
          await processUserData(client, event);
        } catch (userErr) {
          logger.error(`[${batchId}][FAST] User upsert error for uid ${uid}: ${userErr.message}`);
        }
      }

      // Batch update last_seen_at for OE_END users
      const lastSeenUids = Array.from(userLastSeenMap.keys());
      if (lastSeenUids.length > 0) {
        await client.query(
          `UPDATE users SET last_seen_at = NOW() WHERE uid = ANY($1::text[])`,
          [lastSeenUids]
        );
      }

      // Batch process voice events (reduces queries from O(n*5) to O(5))
      if (voiceEvents.length > 0) {
        await processVoiceResponseBatch(client, voiceEvents, batchId);
      }

      // Batch update sync_status for all processed logs in this micro-batch
      if (processedIds.length > 0) {
        await client.query(
          `UPDATE winston_logs SET sync_status = 1 WHERE id = ANY($1::uuid[])`,
          [processedIds]
        );
      }

      await client.query("COMMIT");

      const microBatchDuration = Date.now() - microBatchStart;
      totalProcessed += chunk.length;
      logger.info(`[${batchId}][FAST][Micro-${microBatchIndex}] Committed ${chunk.length} logs in ${microBatchDuration}ms (${Math.round(chunk.length / (microBatchDuration / 1000))} logs/sec)`);

    } catch (err) {
      await client.query("ROLLBACK");
      const microBatchDuration = Date.now() - microBatchStart;
      logger.error(`[${batchId}][FAST][Micro-${microBatchIndex}] FAILED after ${microBatchDuration}ms: ${err.message}`);
      logger.error(`[${batchId}][FAST][Micro-${microBatchIndex}] Stack: ${err.stack}`);
      // Continue with next micro-batch — partial progress is preserved
    } finally {
      client.release();
    }
  }

  logger.info(`[${batchId}][FAST] ========== FAST PROCESSING SUMMARY ==========`);
  logger.info(`[${batchId}][FAST] Total logs processed: ${totalProcessed}/${allRows.length}`);
  logger.info(`[${batchId}][FAST] Total events processed: ${totalEventsProcessed}`);
  logger.info(`[${batchId}][FAST] Dead letter events: ${totalDeadLetter}`);
  logger.info(`[${batchId}][FAST] Micro-batches: ${microBatchIndex} (size: ${MICRO_BATCH_SIZE})`);
  logger.info(`[${batchId}][FAST] =============================================`);

  return { processed: totalProcessed, status: "success", eventsProcessed: totalEventsProcessed, deadLetterCount: totalDeadLetter };
}

// NOTE: processQuestionData and processFeedbackData are legacy dead code.
// Actual processing is handled via dynamic eventProcessors loaded from the database.
// Kept as reference only — do not call directly.

/**
 * Process a single OE_VOICE_RESPONSE telemetry event into calls + messages tables.
 * Each event represents one voice turn (user question + assistant response).
 *
 * @param {Object} client - Database client
 * @param {Object} event - OE_VOICE_RESPONSE telemetry event
 */
async function processVoiceResponse(client, event) {
  try {
    const sid = event.sid;
    const uid = event.uid || 'guest';
    const ets = event.ets;
    const questionText = event.edata?.eks?.target?.questionsDetails?.questionText;
    const responseText = event.edata?.eks?.target?.questionsDetails?.responseText;
    const sourceLang = event.edata?.eks?.target?.questionsDetails?.sourceLang;
    const targetLang = event.edata?.eks?.target?.questionsDetails?.targetLang;

    if (!sid || !ets) {
      logger.debug('processVoiceResponse: missing sid or ets, skipping');
      return;
    }

    const interactionId = sid;
    const userId = (uid === 'guest' || !uid) ? null : uid;

    // Step 1: Upsert calls row
    await client.query(`
      INSERT INTO calls (
        interaction_id, user_id, start_datetime, end_datetime,
        duration_in_seconds, language_name, current_language,
        num_messages, channel_type, source
      )
      VALUES (
        $1, $2, to_timestamp($3 / 1000.0), to_timestamp($3 / 1000.0),
        0, $4, $5, 0, 'voice', 'voice'
      )
      ON CONFLICT (interaction_id) DO UPDATE SET
        end_datetime = GREATEST(EXCLUDED.end_datetime, calls.end_datetime),
        start_datetime = LEAST(EXCLUDED.start_datetime, calls.start_datetime),
        duration_in_seconds = EXTRACT(EPOCH FROM (
          GREATEST(EXCLUDED.end_datetime, calls.end_datetime) -
          LEAST(EXCLUDED.start_datetime, calls.start_datetime)
        )),
        language_name = COALESCE(EXCLUDED.language_name, calls.language_name),
        current_language = COALESCE(EXCLUDED.current_language, calls.current_language),
        num_messages = calls.num_messages + 1,
        user_id = COALESCE(EXCLUDED.user_id, calls.user_id)
    `, [interactionId, userId, ets, sourceLang || null, targetLang || null]);

    // Step 2: Get call_id
    const callRow = await client.query(
      'SELECT id FROM calls WHERE interaction_id = $1',
      [interactionId]
    );
    if (!callRow.rows.length) {
      logger.warn(`processVoiceResponse: could not find call for interaction_id=${interactionId}`);
      return;
    }
    const callId = callRow.rows[0].id;

    // Step 3: Get current max message_order for this call
    const maxOrderRow = await client.query(
      'SELECT COALESCE(MAX(message_order), 0) AS max_order FROM messages WHERE call_id = $1',
      [callId]
    );
    let nextOrder = parseInt(maxOrderRow.rows[0].max_order) + 1;

    // Step 4: Insert messages
    // Welcome turn: no questionText, only responseText (assistant only)
    // Normal turn: questionText + responseText (user + assistant)
    if (questionText && questionText.trim()) {
      await client.query(`
        INSERT INTO messages (call_id, role, content, message_order)
        VALUES ($1, 'user', $2, $3)
        ON CONFLICT (call_id, message_order) DO NOTHING
      `, [callId, questionText.trim(), nextOrder]);
      nextOrder++;
    }

    if (responseText && responseText.trim()) {
      await client.query(`
        INSERT INTO messages (call_id, role, content, message_order)
        VALUES ($1, 'assistant', $2, $3)
        ON CONFLICT (call_id, message_order) DO NOTHING
      `, [callId, responseText.trim(), nextOrder]);
    }

    // Step 5: Upsert voice_call_tracking
    await client.query(`
      INSERT INTO voice_call_tracking (sid, call_id, turns_processed, last_ets)
      VALUES ($1, $2, 1, $3)
      ON CONFLICT (sid) DO UPDATE SET
        turns_processed = voice_call_tracking.turns_processed + 1,
        last_ets = GREATEST(voice_call_tracking.last_ets, EXCLUDED.last_ets),
        updated_at = NOW()
    `, [sid, callId, ets]);

    logger.debug(`processVoiceResponse: sid=${sid}, callId=${callId}, order=${nextOrder}`);
  } catch (err) {
    logger.error(`Error processing voice response: ${err.message}`);
    throw err;
  }
}

/**
 * Batch process multiple OE_VOICE_RESPONSE events in a single micro-batch.
 * Reduces queries from O(n*5) to O(5) by batching all operations.
 * Chunks all operations to stay under PostgreSQL's 65535 parameter limit.
 *
 * @param {Object} client - Database client
 * @param {Array} events - Array of OE_VOICE_RESPONSE events
 * @param {string} batchId - Batch ID for logging
 * @returns {number} Number of events processed
 */
async function processVoiceResponseBatch(client, events, batchId) {
  if (events.length === 0) return 0;

  const batchStart = Date.now();
  let processed = 0;
  const VOICE_BATCH_CHUNK = 5000;

  try {
    // Process voice events in chunks to stay under PG parameter limit
    for (let chunkStart = 0; chunkStart < events.length; chunkStart += VOICE_BATCH_CHUNK) {
      const chunk = events.slice(chunkStart, chunkStart + VOICE_BATCH_CHUNK);

      // Deduplicate by sid — multiple events can share same sid (same call, different turns)
      // Track min/max ets per sid to compute accurate duration
      const callAgg = new Map(); // sid -> {event, turnCount, minEts, maxEts}
      for (const event of chunk) {
        const sid = event.sid;
        if (!sid || !event.ets) continue;
        if (!callAgg.has(sid)) {
          callAgg.set(sid, { event, turnCount: 0, minEts: event.ets, maxEts: event.ets });
        }
        const agg = callAgg.get(sid);
        agg.turnCount++;
        if (event.ets < agg.minEts) agg.minEts = event.ets;
        if (event.ets > agg.maxEts) agg.maxEts = event.ets;
      }

      // Step 1: Batch upsert calls (one row per unique sid)
      // Uses min/max ets for start/end datetime so duration is computed correctly
      const callValues = [];
      const callPlaceholders = [];
      const callSids = new Set();

      for (const [sid, { event, turnCount, minEts, maxEts }] of callAgg) {
        const uid = event.uid || 'guest';
        const sourceLang = event.edata?.eks?.target?.questionsDetails?.sourceLang;
        const targetLang = event.edata?.eks?.target?.questionsDetails?.targetLang;

        const userId = (uid === 'guest' || !uid) ? null : uid;
        const idx = callValues.length / 6;
        callPlaceholders.push(`($${idx * 6 + 1}, $${idx * 6 + 2}, to_timestamp($${idx * 6 + 3} / 1000.0), to_timestamp($${idx * 6 + 4} / 1000.0), 0, $${idx * 6 + 5}, $${idx * 6 + 6}, ${turnCount}, 'voice', 'voice')`);
        callValues.push(sid, userId, minEts, maxEts, sourceLang || null, targetLang || null);
        callSids.add(sid);
      }

      if (callValues.length > 0) {
        await client.query(`
          INSERT INTO calls (
            interaction_id, user_id, start_datetime, end_datetime,
            duration_in_seconds, language_name, current_language,
            num_messages, channel_type, source
          ) VALUES ${callPlaceholders.join(', ')}
          ON CONFLICT (interaction_id) DO UPDATE SET
            end_datetime = GREATEST(EXCLUDED.end_datetime, calls.end_datetime),
            start_datetime = LEAST(EXCLUDED.start_datetime, calls.start_datetime),
            duration_in_seconds = EXTRACT(EPOCH FROM (
              GREATEST(EXCLUDED.end_datetime, calls.end_datetime) -
              LEAST(EXCLUDED.start_datetime, calls.start_datetime)
            )),
            language_name = COALESCE(EXCLUDED.language_name, calls.language_name),
            current_language = COALESCE(EXCLUDED.current_language, calls.current_language),
            num_messages = calls.num_messages + EXCLUDED.num_messages,
            user_id = COALESCE(EXCLUDED.user_id, calls.user_id)
        `, callValues);
      }

      // Step 2: Batch fetch call IDs for this chunk
      const sidArray = Array.from(callSids);
      const callIdMap = new Map();
      const SID_CHUNK = 5000;

      for (let i = 0; i < sidArray.length; i += SID_CHUNK) {
        const batch = sidArray.slice(i, i + SID_CHUNK);
        const callRows = await client.query(
          'SELECT id, interaction_id FROM calls WHERE interaction_id = ANY($1::text[])',
          [batch]
        );
        for (const row of callRows.rows) {
          callIdMap.set(row.interaction_id, row.id);
        }
      }

      // Step 3: Batch fetch max message orders for this chunk
      const callIds = Array.from(callIdMap.values());
      const maxOrderMap = new Map();

      if (callIds.length > 0) {
        const CID_CHUNK = 5000;
        for (let i = 0; i < callIds.length; i += CID_CHUNK) {
          const batch = callIds.slice(i, i + CID_CHUNK);
          const orderRows = await client.query(`
            SELECT call_id, COALESCE(MAX(message_order), 0) AS max_order
            FROM messages
            WHERE call_id = ANY($1::integer[])
            GROUP BY call_id
          `, [batch]);

          for (const row of orderRows.rows) {
            maxOrderMap.set(row.call_id, parseInt(row.max_order));
          }
        }
      }

      // Step 4: Batch insert messages for this chunk
      const msgValues = [];
      const msgPlaceholders = [];

      for (const event of chunk) {
        const sid = event.sid;
        const callId = callIdMap.get(sid);
        if (!callId) continue;

        const questionText = event.edata?.eks?.target?.questionsDetails?.questionText;
        const responseText = event.edata?.eks?.target?.questionsDetails?.responseText;

        let nextOrder = (maxOrderMap.get(callId) || 0) + 1;

        if (questionText && questionText.trim()) {
          const base = msgValues.length;
          msgPlaceholders.push(`($${base + 1}, 'user', $${base + 2}, $${base + 3})`);
          msgValues.push(callId, questionText.trim(), nextOrder);
          nextOrder++;
        }

        if (responseText && responseText.trim()) {
          const base = msgValues.length;
          msgPlaceholders.push(`($${base + 1}, 'assistant', $${base + 2}, $${base + 3})`);
          msgValues.push(callId, responseText.trim(), nextOrder);
          nextOrder++;
        }

        maxOrderMap.set(callId, nextOrder - 1);
      }

      if (msgValues.length > 0) {
        await client.query(`
          INSERT INTO messages (call_id, role, content, message_order)
          VALUES ${msgPlaceholders.join(', ')}
          ON CONFLICT (call_id, message_order) DO NOTHING
        `, msgValues);
      }

      // Step 5: Batch upsert voice_call_tracking (deduplicated by sid)
      const trackAgg = new Map(); // sid -> {callId, turnCount, maxEts}
      for (const event of chunk) {
        const sid = event.sid;
        const callId = callIdMap.get(sid);
        const ets = event.ets;
        if (!sid || !callId || !ets) continue;

        if (!trackAgg.has(sid)) {
          trackAgg.set(sid, { callId, turnCount: 0, maxEts: ets });
        }
        const agg = trackAgg.get(sid);
        agg.turnCount++;
        if (ets > agg.maxEts) agg.maxEts = ets;
      }

      const trackValues = [];
      const trackPlaceholders = [];

      for (const [sid, { callId, turnCount, maxEts }] of trackAgg) {
        const idx = trackValues.length / 4;
        trackPlaceholders.push(`($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4})`);
        trackValues.push(sid, callId, turnCount, maxEts);
      }

      if (trackValues.length > 0) {
        await client.query(`
          INSERT INTO voice_call_tracking (sid, call_id, turns_processed, last_ets)
          VALUES ${trackPlaceholders.join(', ')}
          ON CONFLICT (sid) DO UPDATE SET
            turns_processed = voice_call_tracking.turns_processed + EXCLUDED.turns_processed,
            last_ets = GREATEST(voice_call_tracking.last_ets, EXCLUDED.last_ets),
            updated_at = NOW()
        `, trackValues);
      }

      processed += chunk.length;
    }

    const duration = Date.now() - batchStart;
    logger.info(`[${batchId}] Batch voice processing: ${processed} events in ${duration}ms`);
  } catch (err) {
    logger.error(`[${batchId}] Error in batch voice processing: ${err.message}`);
    throw err;
  }

  return processed;
}

// Lock to prevent concurrent cron runs
let isProcessingLogs = false;
let currentBatchId = null;

// Schedule telemetry processing with configurable cron schedule
cron.schedule(CRON_SCHEDULE, async () => {
  // Check if already processing
  if (isProcessingLogs) {
    logger.warn(`[CRON] Skipping scheduled run - previous batch [${currentBatchId}] still in progress`);
    return;
  }

  const batchId = `batch_${Date.now()}`;
  const cronStartTime = Date.now();

  // Acquire lock
  isProcessingLogs = true;
  currentBatchId = batchId;

  logger.info(`[${batchId}] ========== CRON JOB STARTED ==========`);
  logger.info(`[${batchId}] Schedule: ${CRON_SCHEDULE}, Batch Size: ${BATCH_SIZE}, Fast Mode: ${FAST_MODE}`);

  try {
    const processFn = FAST_MODE ? processTelemetryLogsFast : processTelemetryLogs;
    const result = await processFn(batchId);
    const duration = Date.now() - cronStartTime;
    logger.info(`[${batchId}] ========== CRON JOB COMPLETED ==========`);
    logger.info(`[${batchId}] Duration: ${duration}ms, Processed: ${result.processed}, Status: ${result.status}`);
  } catch (err) {
    const duration = Date.now() - cronStartTime;
    logger.error(`[${batchId}] ========== CRON JOB FAILED ==========`);
    logger.error(`[${batchId}] Duration: ${duration}ms, Error: ${err.message}`);
  } finally {
    // Release lock
    isProcessingLogs = false;
    currentBatchId = null;
    logger.info(`[${batchId}] Lock released`);
  }
});

// ============================================

// Run the existing backfillIsNew() every 30 minutes (minimal)
// cron.schedule(IS_NEW_BACKFILL_SCHEDULE, async () => {
//   try {
//     logger.info('Scheduled backfillIsNew: starting (every 30 minutes)');
//     await backfillIsNew();
//     logger.info('Scheduled backfillIsNew: completed');
//   } catch (err) {
//     logger.error('Scheduled backfillIsNew: failed', err);
//   }
// });

// API endpoint to manually trigger processing
app.post("/api/process-logs", async (req, res) => {
  const batchId = `manual_${Date.now()}`;
  logger.info(`[${batchId}] Manual trigger received via API (Fast Mode: ${FAST_MODE})`);
  try {
    const processFn = FAST_MODE ? processTelemetryLogsFast : processTelemetryLogs;
    const result = await processFn(batchId);
    logger.info(`[${batchId}] Manual trigger completed successfully`);
    res.status(200).json({
      message: "Telemetry log processing triggered successfully",
      batchId,
      ...result,
    });
  } catch (err) {
    logger.error(`[${batchId}] Error triggering telemetry log processing:`, err);
    res.status(500).json({
      error: "Failed to process telemetry logs",
      batchId,
      details: err.message,
    });
  }
});

// API endpoint to get configured event processors
app.get("/api/event-processors", (req, res) => {
  const processors = Object.keys(eventProcessors).map((eventType) => ({
    eventType,
    isActive: true,
  }));

  res.status(200).json({ processors });
});

// API endpoint to register a new event processor configuration
app.post("/api/event-processors", async (req, res) => {
  try {
    const { eventType, tableName, fieldMappings, fieldVerification } = req.body;

    if (!eventType || !tableName || !fieldMappings || !fieldVerification) {
      return res.status(400).json({
        error:
          "Missing required parameters: eventType, tableName, and fieldMappings are required",
      });
    }

    // Register the new event processor configuration
    const result = await loadEventProcessors.registerEventProcessor(
      eventType,
      tableName,
      fieldMappings,
      fieldVerification,
      pool
    );

    res.status(201).json({
      message: "Event processor registered successfully",
      eventType,
      tableName,
      ...result,
    });
  } catch (err) {
    logger.error("Error registering event processor:", err);
    res.status(500).json({
      error: "Failed to register event processor",
      details: err.message,
    });
  }
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "UP",
    version: process.env.npm_package_version || "1.0.0",
    eventProcessors: Object.keys(eventProcessors).length,
  });
});

// API endpoint to backfill voice telemetry from existing winston_logs
app.post("/api/backfill-voice", async (req, res) => {
  const batchId = `backfill_voice_${Date.now()}`;
  logger.info(`[${batchId}] Starting voice telemetry backfill...`);

  const client = await pool.connect();
  let logsProcessed = 0;
  let voiceEventsProcessed = 0;
  let errors = 0;

  try {
    await client.query("BEGIN");

    const result = await client.query(`
      SELECT id, message, "timestamp" FROM winston_logs
      WHERE message::text LIKE '%"eid":"OE_VOICE_RESPONSE"%'
      ORDER BY "timestamp" ASC
    `);

    logger.info(`[${batchId}] Found ${result.rows.length} logs with voice events`);

    for (const log of result.rows) {
      try {
        const events = parseTelemetryMessage(log.message, batchId, 0);
        for (const event of events) {
          if (event.eid === 'OE_VOICE_RESPONSE') {
            await processVoiceResponse(client, event);
            voiceEventsProcessed++;
          }
        }
        logsProcessed++;
      } catch (err) {
        logger.error(`[${batchId}] Error processing log ${log.id}: ${err.message}`);
        errors++;
      }
    }

    await client.query("COMMIT");

    logger.info(`[${batchId}] Backfill complete: ${logsProcessed} logs, ${voiceEventsProcessed} voice events, ${errors} errors`);

    res.status(200).json({
      success: true,
      logsProcessed,
      voiceEventsProcessed,
      errors,
    });
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    logger.error(`[${batchId}] Backfill failed: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    client.release();
  }
});

// API endpoint to backfill voice call durations from voice_call_tracking
app.post("/api/backfill-voice-duration", async (req, res) => {
  const batchId = `backfill_duration_${Date.now()}`;
  logger.info(`[${batchId}] Starting voice call duration backfill...`);

  const client = await pool.connect();
  try {
    const result = await client.query(`
      UPDATE calls c
      SET
        end_datetime = to_timestamp(vct.last_ets / 1000.0),
        duration_in_seconds = EXTRACT(EPOCH FROM (
          to_timestamp(vct.last_ets / 1000.0) - c.start_datetime
        ))
      FROM voice_call_tracking vct
      WHERE vct.call_id = c.id
        AND c.source = 'voice'
        AND (c.duration_in_seconds IS NULL OR c.duration_in_seconds = 0)
        AND vct.last_ets IS NOT NULL
        AND c.start_datetime IS NOT NULL
      RETURNING c.id, c.interaction_id, c.duration_in_seconds
    `);

    logger.info(`[${batchId}] Backfill complete: ${result.rowCount} voice calls updated with duration`);

    res.status(200).json({
      success: true,
      updatedCalls: result.rowCount,
    });
  } catch (err) {
    logger.error(`[${batchId}] Duration backfill failed: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    client.release();
  }
});
// const LEADERBOARD_CUTOFF_DATE =
//   process.env.LEADERBOARD_CUTOFF_DATE || "2025-10-01 00:00:00";
// async function refreshLeaderboardAggregation() {
//   const client = await pool.connect();
//   try {
//     logger.info("Starting leaderboard refresh (most-recent-location)");

//     await client.query("BEGIN");

//     // 1) Ensure columns exist
//     await client.query(`
//       ALTER TABLE public.leaderboard
//         ADD COLUMN IF NOT EXISTS village_code BIGINT,
//         ADD COLUMN IF NOT EXISTS taluka_code INTEGER,
//         ADD COLUMN IF NOT EXISTS district_code INTEGER;
//     `);

//     // 2) Truncate leaderboard (separate statement)
//     await client.query("TRUNCATE TABLE public.leaderboard");

//     // 3) Insert aggregated snapshot (single statement with parameter)
//     await client.query(
//       `
//       WITH per_user_lgd AS (
//         SELECT
//           q.unique_id,
//           (q.registered_location->>'lgd_code') AS lgd_code_text,
//           MAX(q.created_at) AS max_created_at,
//           MIN(q.registered_location::text) FILTER (WHERE (q.registered_location->>'lgd_code') IS NOT NULL) AS any_reg_loc_for_lgd,
//           COUNT(*) AS cnt_for_lgd
//         FROM public.questions q
//         WHERE q.created_at >= $1
//           AND q.unique_id IS NOT NULL
//         GROUP BY q.unique_id, (q.registered_location->>'lgd_code')
//       ),
//       best_loc AS (
//         SELECT unique_id, lgd_code_text AS chosen_lgd_code, any_reg_loc_for_lgd
//         FROM (
//           SELECT
//             unique_id,
//             lgd_code_text,
//             any_reg_loc_for_lgd,
//             max_created_at,
//             ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY max_created_at DESC, lgd_code_text ASC) AS rn
//           FROM per_user_lgd
//         ) t
//         WHERE rn = 1
//       ),
//       totals AS (
//         SELECT
//           q.unique_id,
//           COUNT(*) AS total_count,
//           MAX(q.mobile) AS mobile,
//           MAX(q.username) AS username,
//           MAX(q.email) AS email,
//           MAX(q.role) AS role,
//           MAX(q.farmer_id) AS farmer_id
//         FROM public.questions q
//         WHERE q.created_at >= $1
//           AND q.unique_id IS NOT NULL
//           AND q.answertext IS NOT NULL
//         GROUP BY q.unique_id
//       )
//       INSERT INTO public.leaderboard (
//         unique_id, mobile, username, email, role, farmer_id, registered_location,
//         village_code, taluka_code, district_code, record_count, last_updated
//       )
//       SELECT
//         t.unique_id,
//         t.mobile,
//         t.username,
//         t.email,
//         t.role,
//         t.farmer_id,
//         CASE WHEN b.any_reg_loc_for_lgd IS NOT NULL THEN (b.any_reg_loc_for_lgd)::jsonb ELSE NULL END AS registered_location,
//         v.village_code,
//         v.taluka_code,
//         v.district_code,
//         t.total_count AS record_count,
//         CURRENT_TIMESTAMP AS last_updated
//       FROM totals t
//       LEFT JOIN best_loc b ON t.unique_id = b.unique_id
//       LEFT JOIN public.village_list v ON b.chosen_lgd_code = v.village_code::text;
//       `,
//       [LEADERBOARD_CUTOFF_DATE]
//     );

//     await client.query("COMMIT");
//     logger.info(
//       "Leaderboard refresh completed successfully (most-recent-location)."
//     );
//   } catch (err) {
//     await client.query("ROLLBACK");
//     logger.error("Error refreshing leaderboard (most-recent-location):", err);
//     throw err;
//   } finally {
//     client.release();
//   }
// }

// Schedule leaderboard refresh job to run at 1 AM daily
// cron.schedule(LEADERBOARD_REFRESH_SCHEDULE, async () => {
//   logger.info(
//     `Running scheduled leaderboard refresh (${LEADERBOARD_REFRESH_SCHEDULE})`
//   );
//   try {
//     await refreshLeaderboardAggregation();
//   } catch (err) {
//     logger.error("Scheduled leaderboard refresh failed:", err);
//   }
// });

// API endpoint to manually trigger leaderboard refresh
// app.post("/api/refresh-leaderboard", async (req, res) => {
//   try {
//     await refreshLeaderboardAggregation();
//     res.status(200).json({
//       message: "Leaderboard refresh completed successfully",
//     });
//   } catch (err) {
//     logger.error("Error triggering leaderboard refresh:", err);
//     res
//       .status(500)
//       .json({ error: "Failed to refresh leaderboard", details: err.message });
//   }
// });

// =============================================================================
// MATERIALIZED VIEW REFRESH FOR NEW/RETURNING USERS
// =============================================================================
// These views pre-compute new vs returning user stats for fast dashboard queries
// - mv_user_first_activity: stores first activity date per user
// - mv_daily_new_returning_users: daily breakdown of new vs returning users

// Lock to prevent concurrent MV refresh runs
let isRefreshingMaterializedViews = false;

/**
 * Refresh materialized views for new/returning users analytics
 * Refreshes views CONCURRENTLY to avoid locking reads
 */
async function refreshMaterializedViews() {
  const client = await pool.connect();
  const startTime = Date.now();

  try {
    logger.info("[MV_REFRESH] Starting materialized views refresh...");

    // Safety net: backfill voice call durations from voice_call_tracking.
    // Root cause: some voice calls end up with end_datetime == start_datetime
    // when the initial INSERT path fires but subsequent turn-level UPDATEs
    // don't advance end_datetime (e.g. replayed events, clock skew, or
    // pre-fix historical rows). voice_call_tracking.last_ets is always
    // correctly updated per turn, so we use it as the source of truth.
    // Runs BEFORE the MV refresh so mv_call_message_counts sees the fixed
    // duration_in_seconds values.
    try {
      const backfillRes = await client.query(`
        UPDATE calls c
        SET
          end_datetime = to_timestamp(vct.last_ets / 1000.0),
          duration_in_seconds = EXTRACT(EPOCH FROM (
            to_timestamp(vct.last_ets / 1000.0) - c.start_datetime
          ))
        FROM voice_call_tracking vct
        WHERE vct.call_id = c.id
          AND c.source = 'voice'
          AND (c.duration_in_seconds IS NULL OR c.duration_in_seconds = 0
               OR c.end_datetime = c.start_datetime)
          AND vct.last_ets IS NOT NULL
          AND c.start_datetime IS NOT NULL
          AND to_timestamp(vct.last_ets / 1000.0) > c.start_datetime
      `);
      if (backfillRes.rowCount > 0) {
        logger.info(`[MV_REFRESH] Backfilled duration for ${backfillRes.rowCount} voice calls`);
      }
    } catch (backfillErr) {
      logger.warn(`[MV_REFRESH] Voice duration backfill failed: ${backfillErr.message}`);
    }

    // Unified MV Definitions
    const mvDefinitions = [
      // Legacy MVs
      // {
      //   name: 'mv_user_first_activity',
      //   query: `
      //     CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_first_activity AS
      //     SELECT 
      //       user_id, 
      //       MIN(session_start_at) as first_seen_at
      //     FROM sessions
      //     GROUP BY user_id;
      //     CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_user_first_activity_user_id ON mv_user_first_activity(user_id);
      //   `
      // },
      // {
      //   name: 'mv_daily_new_returning_users',
      //   query: `
      //     CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_new_returning_users AS
      //     SELECT
      //       TO_TIMESTAMP(s.session_start_at / 1000)::DATE AS activity_date,
      //       COUNT(DISTINCT CASE WHEN s.session_start_at = f.first_seen_at THEN s.user_id END) as new_users,
      //       COUNT(DISTINCT CASE WHEN s.session_start_at > f.first_seen_at THEN s.user_id END) as returning_users
      //     FROM sessions s
      //     JOIN mv_user_first_activity f ON s.user_id = f.user_id
      //     GROUP BY 1;
      //     CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_new_returning_users_date ON mv_daily_new_returning_users(activity_date);
      //   `
      // },
      // V2 MVs
      {
        name: 'mv_total_devices',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_total_devices AS
          SELECT COUNT(DISTINCT fingerprint_id) as total_devices
          FROM users;
        `
      },
      {
        name: 'mv_browser_density',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_browser_density AS
          SELECT browser_name, COUNT(*) as count
          FROM users
          WHERE browser_name IS NOT NULL
          GROUP BY browser_name;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_browser_density_name ON mv_browser_density(browser_name);
        `
      },
      {
        name: 'mv_device_density',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_device_density AS
          SELECT device_name, COUNT(*) as count
          FROM users
          WHERE device_name IS NOT NULL
          GROUP BY device_name;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_device_density_name ON mv_device_density(device_name);
        `
      },
      {
        name: 'mv_os_density',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_os_density AS
          SELECT os_name, COUNT(*) as count
          FROM users
          WHERE os_name IS NOT NULL
          GROUP BY os_name;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_os_density_name ON mv_os_density(os_name);
        `
      },
      // {
      //   name: 'mv_session_duration',
      //   query: `
      //     CREATE MATERIALIZED VIEW IF NOT EXISTS mv_session_duration AS
      //     SELECT 
      //         COUNT(*) as total_sessions,
      //         AVG(duration_seconds) as avg_duration,
      //         MIN(duration_seconds) as min_duration,
      //         MAX(duration_seconds) as max_duration
      //     FROM sessions
      //     WHERE duration_seconds IS NOT NULL;
      //   `
      // },
      // Dashboard Analytics MVs - Added 2024-04-16
      // High-impact aggregations for dashboard stats performance
      // NOTE: Queries match bh-dev-2 schema (migration SQL is authoritative)
      {
        name: 'mv_daily_call_stats',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_call_stats AS
          SELECT
            DATE(c.start_datetime) AS call_date,
            COALESCE(c.channel_direction, 'unknown') AS channel,
            COUNT(*) AS total_calls,
            COUNT(DISTINCT c.user_id) AS unique_users,
            AVG(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS avg_duration_seconds,
            MIN(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS min_duration_seconds,
            MAX(EXTRACT(EPOCH FROM (c.end_datetime - c.start_datetime))) AS max_duration_seconds,
            COUNT(CASE WHEN c.end_reason IS NULL OR c.end_reason = 'completed' THEN 1 END) AS completed_calls,
            COUNT(CASE WHEN c.end_reason IS NOT NULL AND c.end_reason != 'completed' THEN 1 END) AS failed_calls
          FROM calls c
          WHERE c.start_datetime IS NOT NULL AND c.end_datetime IS NOT NULL
          GROUP BY DATE(c.start_datetime), COALESCE(c.channel_direction, 'unknown');
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_call_stats_date_channel ON mv_daily_call_stats(call_date, channel);
        `
      },
      // SKIPPED: sessions table does not exist in current schema
      // {
      //   name: 'mv_user_engagement_daily',
      //   query: `
      //     CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_engagement_daily AS
      //     SELECT
      //       DATE(TO_TIMESTAMP(s.session_start_at / 1000)) AS activity_date,
      //       COUNT(DISTINCT s.user_id) AS daily_active_users,
      //       COUNT(DISTINCT s.user_id) AS daily_devices,
      //       COUNT(*) AS total_sessions,
      //       AVG(s.duration_seconds) AS avg_session_duration,
      //       COUNT(DISTINCT CASE WHEN s.channel = 'voice' THEN s.user_id END) AS voice_users,
      //       COUNT(DISTINCT CASE WHEN s.channel = 'chat' THEN s.user_id END) AS chat_users
      //     FROM sessions s
      //     WHERE s.session_start_at IS NOT NULL
      //     GROUP BY DATE(TO_TIMESTAMP(s.session_start_at / 1000));
      //     CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_user_engagement_daily_date ON mv_user_engagement_daily(activity_date);
      //   `
      // },
      {
        name: 'mv_question_answer_rates',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_question_answer_rates AS
          SELECT
            DATE(TO_TIMESTAMP(q.ets / 1000)) AS question_date,
            COALESCE(q.channel, 'unknown') AS channel,
            COUNT(*) AS total_questions,
            COUNT(DISTINCT q.uid) AS unique_users,
            COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) AS answered_questions,
            ROUND(
              COUNT(CASE WHEN q.answertext IS NOT NULL AND q.answertext != '' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0),
              2
            ) AS answer_rate_pct,
            COUNT(f.id) AS feedback_count,
            COUNT(CASE WHEN f.feedbacktype = 'like' THEN 1 END) AS likes,
            COUNT(CASE WHEN f.feedbacktype = 'dislike' THEN 1 END) AS dislikes
          FROM questions q
          LEFT JOIN feedback f ON q.sid = f.sid AND q.ets = f.ets
          GROUP BY DATE(TO_TIMESTAMP(q.ets / 1000)), COALESCE(q.channel, 'unknown');
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_question_answer_rates_date_channel ON mv_question_answer_rates(question_date, channel);
        `
      },
      // SKIPPED: sessions table does not exist in current schema
      // {
      //   name: 'mv_channel_performance',
      //   query: `
      //     CREATE MATERIALIZED VIEW IF NOT EXISTS mv_channel_performance AS
      //     SELECT
      //       DATE(TO_TIMESTAMP(s.session_start_at / 1000)) AS performance_date,
      //       COALESCE(s.channel, 'unknown') AS channel,
      //       COUNT(DISTINCT s.user_id) AS users,
      //       COUNT(*) AS sessions,
      //       SUM(CASE WHEN c.id IS NOT NULL THEN 1 ELSE 0 END) AS calls,
      //       SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) AS messages,
      //       AVG(CASE WHEN c.duration_in_seconds IS NOT NULL THEN c.duration_in_seconds END) AS avg_call_duration
      //     FROM sessions s
      //     LEFT JOIN calls c ON s.user_id = c.user_id AND DATE(TO_TIMESTAMP(s.session_start_at / 1000)) = DATE(c.start_datetime)
      //     LEFT JOIN messages m ON m.call_id = c.id
      //     WHERE s.session_start_at IS NOT NULL
      //     GROUP BY DATE(TO_TIMESTAMP(s.session_start_at / 1000)), COALESCE(s.channel, 'unknown');
      //     CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_channel_performance_date_channel ON mv_channel_performance(performance_date, channel);
      //   `
      // },
      // Call Logs Optimization: Pre-computed call-message aggregations
      {
        name: 'mv_call_message_counts',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_call_message_counts AS
          SELECT
            c.id,
            c.user_id,
            c.duration_in_seconds,
            c.start_datetime,
            c.channel_direction,
            c.end_reason,
            c.language_name,
            COUNT(m.id) AS total_interactions,
            COUNT(m.id) FILTER (WHERE m.role = 'user') AS questions_count
          FROM calls c
          LEFT JOIN messages m ON m.call_id = c.id
          GROUP BY c.id, c.user_id, c.duration_in_seconds, c.start_datetime, c.channel_direction, c.end_reason, c.language_name;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_call_message_counts_id ON mv_call_message_counts(id);
          CREATE INDEX IF NOT EXISTS idx_mv_call_message_counts_start_datetime ON mv_call_message_counts(start_datetime);
          CREATE INDEX IF NOT EXISTS idx_mv_call_message_counts_user_id ON mv_call_message_counts(user_id);
        `
      },
      // Active users per day: computed from questions + errordetails (no sessions table dependency)
      // NOTE: Old mv_active_users was based on sessions table (from migration).
      // This DO block drops it only if it still has the old definition, then recreates.
      {
        name: 'mv_active_users',
        query: `
          DO $$
          BEGIN
            IF EXISTS (
              SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_active_users'
              AND definition LIKE '%sessions%'
            ) THEN
              DROP MATERIALIZED VIEW mv_active_users;
            END IF;
          END $$;
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_active_users AS
          SELECT
            TO_TIMESTAMP(ets / 1000)::date AS activity_date,
            COUNT(DISTINCT uid) AS active_users
          FROM (
            SELECT uid, ets FROM questions WHERE uid IS NOT NULL
            UNION ALL
            SELECT uid, ets FROM errordetails WHERE uid IS NOT NULL
          ) combined
          GROUP BY TO_TIMESTAMP(ets / 1000)::date;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_active_users_date ON mv_active_users(activity_date);
        `
      },
      // Daily session counts: pre-computes the expensive 3-table UNION + GROUP BY (sid, uid)
      {
        name: 'mv_daily_session_counts',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_session_counts AS
          SELECT
            first_event_date AS stat_date,
            COUNT(*) AS session_count
          FROM (
            SELECT
              sid,
              fingerprint_id AS uid,
              TO_TIMESTAMP(MIN(ets) / 1000)::date AS first_event_date
            FROM (
              SELECT sid, fingerprint_id, ets FROM questions
              WHERE sid IS NOT NULL AND answertext IS NOT NULL AND fingerprint_id IS NOT NULL
              UNION ALL
              SELECT sid, fingerprint_id, ets FROM feedback
              WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
              UNION ALL
              SELECT sid, fingerprint_id, ets FROM errordetails
              WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL
            ) all_events
            GROUP BY sid, fingerprint_id
          ) session_dates
          GROUP BY first_event_date;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_session_counts_date ON mv_daily_session_counts(stat_date);
        `
      },
      // =====================================================================
      // Phase 1-5 Performance Overhaul (2026-04-18)
      // All definitions mirror migrations/20260418_phase1to5_performance.sql
      // so the processor can recreate them if the MV is missing on a box
      // that hasn't had the migration run yet.
      // =====================================================================
      {
        name: 'mv_calls_daily_counts',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_calls_daily_counts AS
          SELECT
            DATE(c.start_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS call_date,
            COUNT(*) AS call_count
          FROM calls c
          WHERE c.start_datetime IS NOT NULL
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_calls_daily_counts_date ON mv_calls_daily_counts(call_date);
        `
      },
      {
        name: 'mv_users_daily_firstseen_ist',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_users_daily_firstseen_ist AS
          SELECT
            DATE(u.first_seen_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS bucket_date,
            COUNT(DISTINCT u.fingerprint_id) AS new_users
          FROM users u
          WHERE u.fingerprint_id IS NOT NULL
            AND u.first_seen_at IS NOT NULL
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_users_daily_firstseen_ist_date ON mv_users_daily_firstseen_ist(bucket_date);
        `
      },
      {
        name: 'mv_users_daily_returning_ist',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_users_daily_returning_ist AS
          SELECT
            DATE(TO_TIMESTAMP(q.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS bucket_date,
            COUNT(DISTINCT q.fingerprint_id) AS returning_users
          FROM questions q
          JOIN users u ON q.fingerprint_id = u.fingerprint_id
          WHERE q.fingerprint_id IS NOT NULL
            AND q.ets IS NOT NULL
            AND DATE(TO_TIMESTAMP(q.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')
                <> DATE(u.first_seen_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata')
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_users_daily_returning_ist_date ON mv_users_daily_returning_ist(bucket_date);
        `
      },
      {
        name: 'mv_feedback_daily',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_feedback_daily AS
          SELECT
            DATE(TO_TIMESTAMP(f.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS feedback_date,
            COALESCE(f.channel, 'unknown') AS channel,
            COALESCE(f.feedback_source, 'chat') AS feedback_source,
            COUNT(*) AS total_feedback,
            COUNT(*) FILTER (WHERE f.feedbacktype = 'like')    AS likes,
            COUNT(*) FILTER (WHERE f.feedbacktype = 'dislike') AS dislikes
          FROM feedback f
          WHERE f.ets IS NOT NULL
            AND f.feedbacktext IS NOT NULL
            AND (
              (f.questiontext IS NOT NULL AND f.fingerprint_id IS NOT NULL)
              OR COALESCE(f.feedback_source, 'chat') = 'voice'
            )
          GROUP BY 1, 2, 3;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_feedback_daily_key ON mv_feedback_daily(feedback_date, channel, feedback_source);
          CREATE INDEX IF NOT EXISTS idx_mv_feedback_daily_date ON mv_feedback_daily(feedback_date);
        `
      },
      {
        name: 'mv_sessions_daily',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sessions_daily AS
          WITH combined AS (
            SELECT sid, fingerprint_id AS uid, ets
            FROM questions
            WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND answertext IS NOT NULL AND ets IS NOT NULL
            UNION ALL
            SELECT sid, fingerprint_id AS uid, ets
            FROM feedback
            WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND ets IS NOT NULL
            UNION ALL
            SELECT sid, fingerprint_id AS uid, ets
            FROM errordetails
            WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND ets IS NOT NULL
          ),
          session_counts AS (
            SELECT sid, uid, MIN(ets) AS first_ets, MAX(ets) AS last_ets, COUNT(*) AS event_count
            FROM combined
            GROUP BY sid, uid
          ),
          question_counts AS (
            SELECT sid, fingerprint_id AS uid, COUNT(*) AS question_count
            FROM questions
            WHERE sid IS NOT NULL AND fingerprint_id IS NOT NULL AND answertext IS NOT NULL AND ets IS NOT NULL
            GROUP BY sid, fingerprint_id
          )
          SELECT
            sc.sid, sc.uid, sc.first_ets, sc.last_ets, sc.event_count,
            COALESCE(qc.question_count, 0) AS question_count,
            DATE(TO_TIMESTAMP(sc.first_ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS session_date_ist
          FROM session_counts sc
          LEFT JOIN question_counts qc ON qc.sid = sc.sid AND qc.uid = sc.uid;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sessions_daily_sid_uid ON mv_sessions_daily(sid, uid);
          CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_session_date ON mv_sessions_daily(session_date_ist);
          CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_last_ets ON mv_sessions_daily(last_ets DESC);
          CREATE INDEX IF NOT EXISTS idx_mv_sessions_daily_uid ON mv_sessions_daily(uid);
        `
      },
      {
        name: 'mv_hourly_active_users',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_active_users AS
          WITH combined AS (
            SELECT uid, ets FROM questions WHERE uid IS NOT NULL AND ets IS NOT NULL
            UNION ALL
            SELECT uid, ets FROM errordetails WHERE uid IS NOT NULL AND ets IS NOT NULL
          )
          SELECT
            DATE_TRUNC('hour', TO_TIMESTAMP(c.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS hour_bucket_ist,
            COUNT(DISTINCT c.uid) AS active_users
          FROM combined c
          WHERE c.ets >= (EXTRACT(EPOCH FROM (NOW() - INTERVAL '48 hours')) * 1000)
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hourly_active_users_hour ON mv_hourly_active_users(hour_bucket_ist);
        `
      },
      {
        name: 'mv_errors_daily',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_errors_daily AS
          SELECT
            DATE(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS error_date,
            COALESCE(e.channel, 'unknown') AS channel,
            COUNT(*) AS error_count,
            COUNT(DISTINCT e.uid) AS unique_users,
            COUNT(DISTINCT e.sid) AS unique_sessions,
            COUNT(DISTINCT e.channel) AS unique_channels
          FROM errordetails e
          WHERE e.errortext IS NOT NULL AND e.created_at IS NOT NULL
          GROUP BY 1, 2;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_errors_daily_key ON mv_errors_daily(error_date, channel);
          CREATE INDEX IF NOT EXISTS idx_mv_errors_daily_date ON mv_errors_daily(error_date);
        `
      },
      {
        name: 'mv_asr_daily',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_asr_daily AS
          SELECT
            DATE(TO_TIMESTAMP(a.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS stat_date,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE a.success IS TRUE) AS success_count,
            AVG(a.latencyms) AS avg_latency,
            MAX(a.latencyms) AS max_latency
          FROM asr_details a
          WHERE a.ets IS NOT NULL
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_asr_daily_date ON mv_asr_daily(stat_date);
        `
      },
      {
        name: 'mv_tts_daily',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tts_daily AS
          SELECT
            DATE(TO_TIMESTAMP(t.ets / 1000) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS stat_date,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE t.success IS TRUE) AS success_count,
            AVG(t.latencyms) AS avg_latency,
            MAX(t.latencyms) AS max_latency
          FROM tts_details t
          WHERE t.ets IS NOT NULL
          GROUP BY 1;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_tts_daily_date ON mv_tts_daily(stat_date);
        `
      },
      {
        name: 'mv_user_rollup',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_rollup AS
          WITH q AS (
            SELECT
              uid,
              COUNT(DISTINCT sid) AS session_count,
              COUNT(*) AS total_questions,
              MIN(ets) AS first_session,
              MAX(ets) AS latest_session,
              MAX(created_at) AS last_activity
            FROM questions
            WHERE uid IS NOT NULL AND answertext IS NOT NULL
            GROUP BY uid
          ),
          f AS (
            SELECT
              uid,
              COUNT(*) AS feedback_count,
              COUNT(*) FILTER (WHERE feedbacktype = 'like') AS likes,
              COUNT(*) FILTER (WHERE feedbacktype = 'dislike') AS dislikes
            FROM feedback
            WHERE uid IS NOT NULL AND answertext IS NOT NULL
            GROUP BY uid
          ),
          latest AS (
            SELECT DISTINCT ON (uid) uid, sid AS session_id
            FROM questions
            WHERE uid IS NOT NULL AND answertext IS NOT NULL
            ORDER BY uid, ets DESC
          )
          SELECT
            q.uid AS user_id,
            q.session_count,
            q.total_questions,
            q.first_session,
            q.latest_session,
            q.last_activity,
            COALESCE(f.feedback_count, 0) AS feedback_count,
            COALESCE(f.likes, 0) AS likes,
            COALESCE(f.dislikes, 0) AS dislikes,
            latest.session_id
          FROM q
          LEFT JOIN f ON f.uid = q.uid
          LEFT JOIN latest ON latest.uid = q.uid;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_user_rollup_user ON mv_user_rollup(user_id);
          CREATE INDEX IF NOT EXISTS idx_mv_user_rollup_latest ON mv_user_rollup(latest_session DESC NULLS LAST);
        `
      },
      {
        name: 'mv_monthly_leaderboard',
        query: `
          CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_leaderboard AS
          SELECT
            DATE_TRUNC('month', q.created_at)::date AS month_start,
            (q.registered_location->>'lgd_code') AS lgd_code,
            q.unique_id,
            MAX(q.username) AS username,
            (MAX(q.registered_location::text))::jsonb AS registered_location,
            COUNT(*) FILTER (WHERE q.answertext IS NOT NULL) AS record_count
          FROM questions q
          WHERE q.registered_location IS NOT NULL
            AND (q.registered_location->>'lgd_code') IS NOT NULL
            AND q.unique_id IS NOT NULL
            AND q.unique_id <> ''
            AND q.unique_id <> '696354'
            AND q.farmer_id IS NOT NULL
            AND q.farmer_id <> ''
            AND q.created_at IS NOT NULL
          GROUP BY 1, 2, 3;
          CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_leaderboard_key ON mv_monthly_leaderboard(month_start, lgd_code, unique_id);
          CREATE INDEX IF NOT EXISTS idx_mv_monthly_leaderboard_ordered ON mv_monthly_leaderboard(month_start, lgd_code, record_count DESC);
        `
      }
    ];

    // Ensure all MVs exist
    logger.info('[MV_REFRESH] Verifying existence of Materialized Views...');
    for (const mv of mvDefinitions) {
      try {
        await client.query(mv.query);
      } catch (err) {
        logger.error(`[MV_REFRESH] Failed to create/verify MV ${mv.name}: ${err.message}`);
      }
    }

    // Refresh all views
    for (const mv of mvDefinitions) {
      const view = mv.name;
      try {
        await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`);
        logger.debug(`[MV_REFRESH] Refreshed ${view}`);
      } catch (viewErr) {
        if (viewErr.message.includes('cannot refresh')) {
          await client.query(`REFRESH MATERIALIZED VIEW ${view}`);
          logger.debug(`[MV_REFRESH] Refreshed ${view} (non-concurrent)`);
        } else {
          logger.warn(`[MV_REFRESH] Failed to refresh ${view}: ${viewErr.message}`);
        }
      }
    }

    const totalDuration = Date.now() - startTime;
    logger.info(`[MV_REFRESH] All materialized views refreshed successfully in ${totalDuration}ms`);

    return {
      status: "success",
      duration: totalDuration,
      refreshedViews: mvDefinitions.map(d => d.name)
    };

  } catch (err) {
    const duration = Date.now() - startTime;
    logger.error(`[MV_REFRESH] Failed after ${duration}ms:`, err);
    throw err;
  } finally {
    client.release();
  }
}

// Schedule materialized view refresh every 15 minutes
cron.schedule(MV_REFRESH_SCHEDULE, async () => {
  // Check if already refreshing
  if (isRefreshingMaterializedViews) {
    logger.warn("[MV_REFRESH] Skipping scheduled run - previous refresh still in progress");
    return;
  }

  // Acquire lock
  isRefreshingMaterializedViews = true;

  try {
    logger.info(`[MV_REFRESH] Running scheduled refresh (${MV_REFRESH_SCHEDULE})`);
    await refreshMaterializedViews();
  } catch (err) {
    logger.error("[MV_REFRESH] Scheduled refresh failed:", err);
  } finally {
    // Release lock
    isRefreshingMaterializedViews = false;
  }
});

// Backfill is_new = 1 for earliest qualifying row per uid
// async function backfillIsNew() {
//   const client = await pool.connect();
//   try {
//     logger.info("Starting backfill: computing is_new for questions table...");

//     await client.query("BEGIN");

//     // Reset all to 0 (idempotent)
//     await client.query(`
//       UPDATE public.questions
//       SET is_new = 0
//       WHERE is_new IS DISTINCT FROM 0;
//     `);

//     // Mark earliest qualifying row per uid as is_new = 1
//     await client.query(`
//       WITH first_per_uid AS (
//         SELECT id
//         FROM (
//           SELECT id,
//                  ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at ASC, id ASC) AS rn
//           FROM public.questions q
//           WHERE q.uid IS NOT NULL
//             AND q.answertext IS NOT NULL
//             AND q.sid IS NOT NULL
//             AND q.ets IS NOT NULL
//         ) t
//         WHERE rn = 1
//       )
//       UPDATE public.questions q
//       SET is_new = 1
//       FROM first_per_uid f
//       WHERE q.id = f.id;
//     `);

//     await client.query("COMMIT");
//     logger.info("Backfill completed: is_new marked.");
//   } catch (err) {
//     await client.query("ROLLBACK");
//     logger.error("Backfill failed:", err);
//     throw err;
//   } finally {
//     client.release();
//   }
// }

// Start server
async function startServer() {
  try {
    // Ensure database tables exist
    await ensureTablesExist();

    // for new user and returning users
    // await backfillIsNew();

    // Load configured event processors
    await loadEventProcessors.loadFromDatabase(pool);

    // Log loaded processors for debugging
    logger.info(`Loaded ${eventProcessors.length} event processors from database`);
    eventProcessors.forEach((proc, index) => {
      logger.info(`  Processor ${index}: table="${proc.tableName}", eventType="${proc.eventType}", fieldVerification="${proc.fieldVerification}"`);
    });

    // Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`Telemetry log processor service started on port ${PORT}`);
    });

    // Run initial processing on startup with proper lock to prevent cron overlap
    logger.info(`Running initial telemetry log processing on startup (Fast Mode: ${FAST_MODE})...`);
    isProcessingLogs = true;
    currentBatchId = `startup_${Date.now()}`;
    try {
      const processFn = FAST_MODE ? processTelemetryLogsFast : processTelemetryLogs;
      await processFn(currentBatchId);
    } finally {
      isProcessingLogs = false;
      currentBatchId = null;
    }

    // await refreshLeaderboardAggregation();

    // Refresh materialized views on startup (non-blocking)
    logger.info('Running initial materialized views refresh on startup...');
    refreshMaterializedViews().catch(err => {
      logger.warn('Initial materialized views refresh failed (views may not exist yet):', err.message);
    });

    return server;
  } catch (err) {
    logger.error("Failed to start server:", err);
    process.exit(1);
  }
}

// Handle shutdown gracefully
process.on("SIGTERM", () => {
  logger.info("SIGTERM received, shutting down gracefully");
  pool.end();
  process.exit(0);
});

// Export for testing
module.exports = {
  app,
  pool,
  startServer,
  processTelemetryLogs,
  processTelemetryLogsFast,
  ensureTablesExist,
  parseTelemetryMessage,
  processVoiceResponse,
};

// Only start server if this file is run directly (not when required in tests)
if (require.main === module) {
  startServer();
}
