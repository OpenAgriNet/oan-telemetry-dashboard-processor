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

let isShuttingDown = false;
let server; // HTTP server reference

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  logger.error('[FATAL] Uncaught Exception', err);
  shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  logger.error('[FATAL] Unhandled Promise Rejection', reason);
  shutdown('unhandledRejection');
});

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
  // ssl: {
  //   rejectUnauthorized: true,
  //   ca: fs.readFileSync(path.join(__dirname, 'certs', 'rds-global.pem')).toString()
  // }
});

async function ensureVillagesSeeded() {
  const client = await pool.connect();
  try {
    logger.info("Ensuring village_list table exists with expected columns...");

    // Create table with full schema if it does not exist (safe)
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.village_list (
        village_code INTEGER PRIMARY KEY,
        taluka_code INTEGER,
        district_code INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Also ensure columns exist if older table was created with different schema
    await client.query(`
      ALTER TABLE public.village_list
        ADD COLUMN IF NOT EXISTS taluka_code INTEGER,
        ADD COLUMN IF NOT EXISTS district_code INTEGER,
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    `);

    // check if table already has data
    const { rows } = await client.query(
      "SELECT COUNT(*)::bigint AS cnt FROM public.village_list",
    );
    const cnt = Number(rows[0].cnt || 0);
    logger.info(`village_list row count = ${cnt}`);

    if (cnt > 0) {
      logger.info("Villages table already seeded — skipping seeder.");
      return;
    }

    // spawn the existing seeder script (do not edit the seeder file)
    logger.info(
      "Villages table empty — running seeder script (seed_villages_stream.js). This may take some time...",
    );
    const seederPath = path.join(__dirname, "seed_villages_stream.js");

    const child = spawn(process.execPath, [seederPath], {
      stdio: "inherit", // pipe seeder output to main stdout/stderr
      env: process.env,
      cwd: __dirname,
    });

    await new Promise((resolve, reject) => {
      child.on("error", (err) => {
        logger.error("Failed to start seeder process:", err);
        reject(err);
      });
      child.on("exit", (code, signal) => {
        if (code === 0) {
          logger.info("Seeder completed successfully.");
          resolve();
        } else {
          const msg = `Seeder exited with code ${code}${
            signal ? " signal " + signal : ""
          }`;
          logger.error(msg);
          reject(new Error(msg));
        }
      });
    });
  } finally {
    client.release();
  }
}

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
const BATCH_SIZE = process.env.BATCH_SIZE || 10;
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "*/5 * * * *";
const LEADERBOARD_REFRESH_SCHEDULE =
  process.env.LEADERBOARD_REFRESH_SCHEDULE || "0 1 * * *"; // Run at 1 AM every day
const IS_NEW_BACKFILL_SCHEDULE =
  process.env.IS_NEW_BACKFILL_SCHEDULE || "*/30 * * * *"; // Run every 30 minutes
const MV_REFRESH_SCHEDULE = process.env.MV_REFRESH_SCHEDULE || "*/15 * * * *"; // Refresh materialized views every 15 minutes

// Ensure necessary tables exist
async function ensureTablesExist() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.dead_letter_logs (
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
      CREATE TABLE IF NOT EXISTS public.questions (
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
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Create leaderboard table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.leaderboard (
        unique_id VARCHAR NOT NULL,
        mobile VARCHAR,
        username VARCHAR,
        email VARCHAR,
        role VARCHAR,
        farmer_id VARCHAR,
        registered_location JSONB,
        record_count INTEGER,
        village_code BIGINT,
        taluka_code INTEGER,
        district_code INTEGER,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (unique_id)
      )
    `);

    await client.query(`
      ALTER TABLE public.questions
  ADD COLUMN IF NOT EXISTS is_new SMALLINT DEFAULT 0 NOT NULL;`);

    // Add missing columns to questions table if they don't exist
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='unique_id') THEN
          ALTER TABLE public.questions ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='mobile') THEN
          ALTER TABLE public.questions ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='username') THEN
          ALTER TABLE public.questions ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='email') THEN
          ALTER TABLE public.questions ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='role') THEN
          ALTER TABLE public.questions ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='farmer_id') THEN
          ALTER TABLE public.questions ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='registered_location') THEN
          ALTER TABLE public.questions ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='device_location') THEN
          ALTER TABLE public.questions ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='questions' AND column_name='agristack_location') THEN
          ALTER TABLE public.questions ADD COLUMN agristack_location JSONB;
        END IF;
      END $$;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.errorDetails (
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
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Add missing columns to errorDetails table if they don't exist
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='unique_id') THEN
          ALTER TABLE public.errorDetails ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='mobile') THEN
          ALTER TABLE public.errorDetails ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='username') THEN
          ALTER TABLE public.errorDetails ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='email') THEN
          ALTER TABLE public.errorDetails ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='role') THEN
          ALTER TABLE public.errorDetails ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='farmer_id') THEN
          ALTER TABLE public.errorDetails ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='registered_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='device_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='errordetails' AND column_name='agristack_location') THEN
          ALTER TABLE public.errorDetails ADD COLUMN agristack_location JSONB;
        END IF;
      END $$;
    `);

    // Create feedback table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.feedback (
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
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Add missing columns to feedback table if they don't exist
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='unique_id') THEN
          ALTER TABLE public.feedback ADD COLUMN unique_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='mobile') THEN
          ALTER TABLE public.feedback ADD COLUMN mobile VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='username') THEN
          ALTER TABLE public.feedback ADD COLUMN username VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='email') THEN
          ALTER TABLE public.feedback ADD COLUMN email VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='role') THEN
          ALTER TABLE public.feedback ADD COLUMN role VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='farmer_id') THEN
          ALTER TABLE public.feedback ADD COLUMN farmer_id VARCHAR;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='registered_location') THEN
          ALTER TABLE public.feedback ADD COLUMN registered_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='device_location') THEN
          ALTER TABLE public.feedback ADD COLUMN device_location JSONB;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='feedback' AND column_name='agristack_location') THEN
          ALTER TABLE public.feedback ADD COLUMN agristack_location JSONB;
        END IF;
      END $$;
    `);

    // Create event_processors table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.event_processors (
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
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_schema = 'public' AND table_name = 'event_processors' AND column_name = 'field_verification'
        ) THEN
          ALTER TABLE public.event_processors ADD COLUMN field_verification VARCHAR;
        END IF;
      END $$;
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS event_processors_table_name_key ON public.event_processors (table_name);
    `);

    // Insert default event processors if they don't exist
    await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
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
          "agristack_location": "edata.eks.target.agristack_location"
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
          "agristack_location": "edata.eks.target.agristack_location"
        }',
        field_verification = 'edata.eks.target.questionsDetails',
        updated_at = NOW()
      WHERE table_name = 'questions';
    `);

    await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
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
          "agristack_location": "edata.eks.target.agristack_location"
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
          "agristack_location": "edata.eks.target.agristack_location"
        }',
        field_verification = 'edata.eks.target.errorDetails',
        updated_at = NOW()
      WHERE table_name = 'errorDetails';
    `);

    await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
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
          "agristack_location": "edata.eks.target.agristack_location"
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
          "agristack_location": "edata.eks.target.agristack_location"
        }',
        field_verification = 'edata.eks.target.feedbackDetails',
        updated_at = NOW()
      WHERE table_name = 'feedback';
    `);

    /*
        await client.query(`
          INSERT INTO public.event_processors (event_type, table_name, field_mappings)
          VALUES 
            ('Feedback', 'feedback', '{
              "uid": "uid",
              "sid": "sid",
              "groupDetails": "edata.eks.groupDetails",
              "channel": "channel",
              "ets": "ets",
              "feedbackText": "edata.eks.feedbackText",
              "sessionId": "edata.eks.sessionId",
              "questionText": "edata.eks.questionText",
              "answerText": "edata.eks.answerText",
              "feedbackType": "edata.eks.feedbackType"
            }')
          ON CONFLICT (event_type) DO NOTHING;
        `);
    */
    logger.info("Database tables verified and created if needed");
  } catch (err) {
    logger.error("Error ensuring tables exist:", err);
    throw err;
  } finally {
    client.release();
  }
}

// Parse telemetry message JSON
function parseTelemetryMessage(message, batchId = "", logIndex = 0) {
  try {
    logger.debug(
      `[${batchId}] [Log ${logIndex}] Parsing telemetry message (length: ${message?.length || 0} chars)`,
    );
    // Parse the message string which is a JSON string
    const parsedMessage = JSON.parse(message);
    // Return the events array from the parsed message
    const events = parsedMessage.events || [];
    logger.debug(
      `[${batchId}] [Log ${logIndex}] Successfully parsed message, found ${events.length} events`,
    );
    return events;
  } catch (err) {
    const messagePreview = message?.substring(0, 100) || "empty";
    logger.error(
      `[${batchId}] [Log ${logIndex}] Error parsing telemetry message: ${err.message}`,
    );
    logger.error(
      `[${batchId}] [Log ${logIndex}] Message preview: ${messagePreview}...`,
    );
    return [];
  }
}

// Process telemetry logs
async function processTelemetryLogs(batchId = `batch_${Date.now()}`) {
  logger.info(`[${batchId}] Step 1: Acquiring database connection...`);
  const client = await pool.connect();
  logger.info(`[${batchId}] Step 1: Database connection acquired`);

  let processedCount = 0;
  let totalEventsProcessed = 0;
  let deadLetterCount = 0;

  try {
    // Begin transaction
    logger.info(`[${batchId}] Step 2: Beginning database transaction...`);
    await client.query("BEGIN");
    logger.info(`[${batchId}] Step 2: Transaction started`);

    // Get unprocessed logs
    logger.info(
      `[${batchId}] Step 3: Fetching unprocessed logs (BATCH_SIZE: ${BATCH_SIZE})...`,
    );
    const queryStartTime = Date.now();
    const result = await client.query(
      `SELECT id, level, message, meta, cast(to_char(("timestamp")::TIMESTAMP,'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE],
    );
    const queryDuration = Date.now() - queryStartTime;
    logger.info(
      `[${batchId}] Step 3: Query completed in ${queryDuration}ms, found ${result.rows.length} logs`,
    );

    if (result.rows.length === 0) {
      logger.info(
        `[${batchId}] Step 4: No new telemetry logs to process - committing empty transaction`,
      );
      await client.query("COMMIT");
      logger.info(`[${batchId}] Step 4: Empty transaction committed`);
      return { processed: 0, status: "success" };
    }

    logger.info(
      `[${batchId}] Step 4: Starting to process ${result.rows.length} telemetry logs`,
    );

    // Process each log
    for (let logIndex = 0; logIndex < result.rows.length; logIndex++) {
      const log = result.rows[logIndex];
      const logStartTime = Date.now();
      logger.info(
        `[${batchId}] [Log ${logIndex + 1}/${result.rows.length}] Processing log (timestamp: ${log.timestamp}, level: ${log.level})`,
      );

      const events = parseTelemetryMessage(log.message, batchId, logIndex + 1);
      logger.info(
        `[${batchId}] [Log ${logIndex + 1}] Parsed ${events.length} events from message`,
      );

      if (events.length === 0) {
        logger.warn(
          `[${batchId}] [Log ${logIndex + 1}] No events found in message - skipping to sync status update`,
        );
      }

      for (let eventIndex = 0; eventIndex < events.length; eventIndex++) {
        const event = events[eventIndex];
        const eventType = event.eid;
        const eventUid = event.uid || "unknown";
        const eventMid = event.mid || "unknown";
        logger.debug(
          `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}/${events.length}] Processing event type: ${eventType}, uid: ${eventUid}, mid: ${eventMid}`,
        );

        let eventProcessed = false;
        let matchedProcessor = null;

        for (const key in eventProcessors) {
          const processor = eventProcessors[key];
          const verified = getNestedValue(
            event,
            processor["fieldVerification"],
          );

          logger.debug(
            `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Checking processor '${key}': eventType match=${processor["eventType"] === eventType}, verified=${verified !== undefined}`,
          );

          if (
            processor["eventType"] === eventType &&
            verified !== undefined &&
            !eventProcessed
          ) {
            matchedProcessor = key;
            logger.info(
              `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Matched processor '${key}' for event type '${eventType}'`,
            );

            // const processorStartTime = Date.now();
            // await processor["process"](client, event);
            // const processorDuration = Date.now() - processorStartTime;

            // logger.info(
            //   `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Processor '${key}' completed in ${processorDuration}ms`,
            // );
            // eventProcessed = true;
            // totalEventsProcessed++;
            // break;

            const processorStartTime = Date.now();

            try {
              await processor["process"](client, event);

              const processorDuration = Date.now() - processorStartTime;
              logger.info(
                `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Processor '${key}' completed in ${processorDuration}ms`,
              );

              eventProcessed = true;
              totalEventsProcessed++;
            } catch (err) {
              logger.error(
                `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Processor '${key}' failed: ${err.message}`,
              );

              await client.query(
                `INSERT INTO dead_letter_logs(level, message, meta, event_name)
     VALUES ($1, $2, $3, $4)`,
                [log.level, JSON.stringify(event), log.meta, eventType],
              );

              deadLetterCount++;
              eventProcessed = true; // IMPORTANT: mark as handled
            }

            break;
          }
        }

        if (
          eventType !== "OE_END" &&
          eventType !== "OE_START" &&
          eventProcessed === false
        ) {
          logger.warn(
            `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - No processor matched for event type: ${eventType} - sending to dead letter queue`,
          );
          logger.debug(
            `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Dead letter event uid: ${eventUid}, channel: ${event.channel || "unknown"}`,
          );

          await client.query(
            `Insert into dead_letter_logs(level, message, meta, event_name) values ($1, $2, $3, $4)`,
            [log.level, JSON.stringify(event), log.meta, eventType],
          );
          deadLetterCount++;
          logger.info(
            `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Event inserted into dead_letter_logs`,
          );
        } else if (eventType === "OE_END" || eventType === "OE_START") {
          logger.debug(
            `[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Skipping system event type: ${eventType}`,
          );
        }
      }

      // Update sync status for processed log
      logger.debug(
        `[${batchId}] [Log ${logIndex + 1}] Updating sync_status to 1 for processed log...`,
      );
      await client.query(
        `UPDATE winston_logs SET sync_status = 1 WHERE id = $1`,
        [log.id],
      );

      const logDuration = Date.now() - logStartTime;
      processedCount++;
      logger.info(
        `[${batchId}] [Log ${logIndex + 1}] Log processing completed in ${logDuration}ms, sync_status updated`,
      );
    }

    // Commit transaction
    logger.info(`[${batchId}] Step 5: Committing transaction...`);
    await client.query("COMMIT");
    logger.info(`[${batchId}] Step 5: Transaction committed successfully`);

    logger.info(`[${batchId}] ========== PROCESSING SUMMARY ==========`);
    logger.info(`[${batchId}] Logs processed: ${processedCount}`);
    logger.info(`[${batchId}] Total events processed: ${totalEventsProcessed}`);
    logger.info(`[${batchId}] Dead letter events: ${deadLetterCount}`);
    logger.info(`[${batchId}] ==========================================`);

    return {
      processed: result.rows.length,
      status: "success",
      eventsProcessed: totalEventsProcessed,
      deadLetterCount,
    };
  } catch (err) {
    logger.error(
      `[${batchId}] Step ERROR: Rolling back transaction due to error...`,
    );
    await client.query("ROLLBACK");
    logger.error(`[${batchId}] Step ERROR: Transaction rolled back`);
    logger.error(`[${batchId}] Error details: ${err.message}`);
    logger.error(`[${batchId}] Error stack: ${err.stack}`);
    logger.error(
      `[${batchId}] Processed before error: ${processedCount} logs, ${totalEventsProcessed} events`,
    );
    return { processed: 0, status: "error", error: err.message };
  } finally {
    logger.info(`[${batchId}] Step FINAL: Releasing database connection...`);
    client.release();
    logger.info(`[${batchId}] Step FINAL: Database connection released`);
  }
}

// question
// Process OE_ITEM_RESPONSE data
async function processQuestionData(client, event) {
  try {
    // Extract required fields
    const uid = event.uid;
    const sid = event.sid;
    const groupDetails =
      event.edata?.eks?.target?.questionsDetails?.groupDetails || [];
    const channel = event.channel;
    const etsRaw = event.ets;
    const ets = Number(etsRaw);
    const questionText =
      event.edata?.eks?.target?.questionsDetails?.questionText;
    const questionSource =
      event.edata?.eks?.target?.questionsDetails?.questionSource;
    const answerText = event.edata?.eks?.target?.questionsDetails?.answerText;
    const answer = answerText?.answer;

    if (!Number.isFinite(ets)) {
      throw new Error(
        `Invalid ets value. Expected bigint timestamp, got: ${JSON.stringify(etsRaw)}`
      );
    }

    // Insert data into questions table
    // await client.query(
    //   `INSERT INTO questions (
    //     uid, sid, group_details, channel, ets,
    //     question_text, question_source, answer_text, answer, is_new
    //   ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    //   [
    //     uid,
    //     sid,
    //     JSON.stringify(groupDetails),
    //     channel,
    //     ets,
    //     questionText,
    //     questionSource,
    //     JSON.stringify(answerText),
    //     answer,
    //   ]
    // );

    await client.query(
      `
 INSERT INTO questions (
  uid, sid, groupdetails, channel, ets,
  questiontext, questionsource, answertext, answer, is_new
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, 1
)
ON CONFLICT (uid)
DO UPDATE
SET is_new = 0;
  `,
      [
        uid,
        sid,
        JSON.stringify(groupDetails),
        channel,
        ets,
        questionText,
        questionSource,
        JSON.stringify(answerText),
        answer,
      ],
    );

    logger.info(
      `Processed question data for uid: ${uid}, question: ${questionText?.substring(
        0,
        30,
      )}...`,
    );
  } catch (err) {
    logger.error("Error processing question data:", err);
    throw err;
  }
}

// Process Feedback data
async function processFeedbackData(client, event) {
  try {
    // Extract required fields
    const uid = event.uid;
    const sid = event.sid;
    const groupDetails = event.edata?.eks?.groupDetails || [];
    const channel = event.channel;
    const ets = event.ets;
    const feedbackText = event.edata?.eks?.feedbackText;
    const sessionId = event.edata?.eks?.sessionId;
    const questionText = event.edata?.eks?.questionText;
    const answerText = event.edata?.eks?.answerText;
    const feedbackType = event.edata?.eks?.feedbackType;

    // Insert data into feedback table
    await client.query(
      `INSERT INTO feedback (
        uid, sid, group_details, channel, ets,
        feedback_text, session_id, question_text, 
        answer_text, feedback_type
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        uid,
        sid,
        JSON.stringify(groupDetails),
        channel,
        ets,
        feedbackText,
        sessionId,
        questionText,
        answerText,
        feedbackType,
      ],
    );

    logger.info(
      `Processed feedback data for uid: ${uid}, feedback type: ${feedbackType}`,
    );
  } catch (err) {
    logger.error("Error processing feedback data:", err);
    throw err;
  }
}

// Lock to prevent concurrent cron runs
let isProcessingLogs = false;
let currentBatchId = null;

// Schedule telemetry processing with configurable cron schedule
cron.schedule(CRON_SCHEDULE, async () => {
  if (isShuttingDown) {
    logger.warn('[CRON] Skipping run — application shutting down');
    return;
  }
  // Check if already processing
  if (isProcessingLogs) {
    logger.warn(
      `[CRON] Skipping scheduled run - previous batch [${currentBatchId}] still in progress`,
    );
    return;
  }

  const batchId = `batch_${Date.now()}`;
  const cronStartTime = Date.now();

  // Acquire lock
  isProcessingLogs = true;
  currentBatchId = batchId;

  logger.info(`[${batchId}] ========== CRON JOB STARTED ==========`);
  logger.info(
    `[${batchId}] Schedule: ${CRON_SCHEDULE}, Batch Size: ${BATCH_SIZE}`,
  );

  try {
    const result = await processTelemetryLogs(batchId);
    const duration = Date.now() - cronStartTime;
    logger.info(`[${batchId}] ========== CRON JOB COMPLETED ==========`);
    logger.info(
      `[${batchId}] Duration: ${duration}ms, Processed: ${result.processed}, Status: ${result.status}`,
    );
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
  logger.info(`[${batchId}] Manual trigger received via API`);
  try {
    const result = await processTelemetryLogs(batchId);
    logger.info(`[${batchId}] Manual trigger completed successfully`);
    res.status(200).json({
      message: "Telemetry log processing triggered successfully",
      batchId,
      ...result,
    });
  } catch (err) {
    logger.error(
      `[${batchId}] Error triggering telemetry log processing:`,
      err,
    );
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
  if (isShuttingDown) {
    return res.status(503).send('Shutting down');
  }
  res.status(200).json({
    status: "UP",
    version: process.env.npm_package_version || "1.0.0",
    eventProcessors: Object.keys(eventProcessors).length,
  });
});

// Function to refresh user location aggregation data
const LEADERBOARD_CUTOFF_DATE =
  process.env.LEADERBOARD_CUTOFF_DATE || 1767205800000;
async function refreshLeaderboardAggregation() {
  console.log(" LEADERBOARD CUTOFF DATE:", LEADERBOARD_CUTOFF_DATE);
  const client = await pool.connect();
  try {
    logger.info("Starting leaderboard refresh (most-recent-location)");

    await client.query("BEGIN");

    // 1) Ensure columns exist
    await client.query(`
      ALTER TABLE public.leaderboard
        ADD COLUMN IF NOT EXISTS village_code BIGINT,
        ADD COLUMN IF NOT EXISTS taluka_code INTEGER,
        ADD COLUMN IF NOT EXISTS district_code INTEGER;
    `);

    // 2) Truncate leaderboard (separate statement)
    await client.query("TRUNCATE TABLE public.leaderboard");

    // 3) Insert aggregated snapshot (single statement with parameter)
    await client.query(
      `
      WITH per_user_lgd AS (
        SELECT
          q.unique_id,
          (q.registered_location->>'lgd_code') AS lgd_code_text,
          MAX(q.created_at) AS max_created_at,
          MIN(q.registered_location::text) FILTER (WHERE (q.registered_location->>'lgd_code') IS NOT NULL) AS any_reg_loc_for_lgd,
          COUNT(*) AS cnt_for_lgd
        FROM public.questions q
        WHERE q.ets >= $1
          AND q.unique_id IS NOT NULL
        GROUP BY q.unique_id, (q.registered_location->>'lgd_code')
      ),
      best_loc AS (
        SELECT unique_id, lgd_code_text AS chosen_lgd_code, any_reg_loc_for_lgd
        FROM (
          SELECT
            unique_id,
            lgd_code_text,
            any_reg_loc_for_lgd,
            max_created_at,
            ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY max_created_at DESC, lgd_code_text ASC) AS rn
          FROM per_user_lgd
        ) t
        WHERE rn = 1
      ),
      totals AS (
        SELECT
          q.unique_id,
          COUNT(*) AS total_count,
          MAX(q.mobile) AS mobile,
          MAX(q.username) AS username,
          MAX(q.email) AS email,
          MAX(q.role) AS role,
          MAX(q.farmer_id) AS farmer_id
        FROM public.questions q
        WHERE q.ets >= $1
          AND q.unique_id IS NOT NULL
          AND q.answertext IS NOT NULL
        GROUP BY q.unique_id
      )
      INSERT INTO public.leaderboard (
        unique_id, mobile, username, email, role, farmer_id, registered_location,
        village_code, taluka_code, district_code, record_count, last_updated
      )
      SELECT
        t.unique_id,
        t.mobile,
        t.username,
        t.email,
        t.role,
        t.farmer_id,
        CASE WHEN b.any_reg_loc_for_lgd IS NOT NULL THEN (b.any_reg_loc_for_lgd)::jsonb ELSE NULL END AS registered_location,
        v.village_code,
        v.taluka_code,
        v.district_code,
        t.total_count AS record_count,
        CURRENT_TIMESTAMP AS last_updated
      FROM totals t
      LEFT JOIN best_loc b ON t.unique_id = b.unique_id
      LEFT JOIN public.village_list v ON b.chosen_lgd_code = v.village_code::text;
      `,
      [LEADERBOARD_CUTOFF_DATE],
    );

    await client.query("COMMIT");
    logger.info(
      "Leaderboard refresh completed successfully (most-recent-location).",
    );
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error("Error refreshing leaderboard (most-recent-location):", err);
    throw err;
  } finally {
    client.release();
  }
}

// Schedule leaderboard refresh job to run at 1 AM daily
cron.schedule(LEADERBOARD_REFRESH_SCHEDULE, async () => {
  logger.info(
    `Running scheduled leaderboard refresh (${LEADERBOARD_REFRESH_SCHEDULE})`,
  );
  try {
    await refreshLeaderboardAggregation();
  } catch (err) {
    logger.error("Scheduled leaderboard refresh failed:", err);
  }
});

// API endpoint to manually trigger leaderboard refresh
app.post("/api/refresh-leaderboard", async (req, res) => {
  try {
    await refreshLeaderboardAggregation();
    res.status(200).json({
      message: "Leaderboard refresh completed successfully",
    });
  } catch (err) {
    logger.error("Error triggering leaderboard refresh:", err);
    res
      .status(500)
      .json({ error: "Failed to refresh leaderboard", details: err.message });
  }
});

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

    // Check if materialized views exist before attempting refresh
    const viewCheck = await client.query(`
      SELECT matviewname FROM pg_matviews 
      WHERE schemaname = 'public' 
      AND matviewname IN ('mv_user_first_activity', 'mv_daily_new_returning_users')
    `);

    const existingViews = viewCheck.rows.map((r) => r.matviewname);

    if (existingViews.length === 0) {
      logger.warn(
        "[MV_REFRESH] No materialized views found. Please run the CREATE MATERIALIZED VIEW statements first.",
      );
      return { status: "skipped", reason: "views_not_found" };
    }

    // Refresh mv_user_first_activity first (foundation view)
    if (existingViews.includes("mv_user_first_activity")) {
      logger.info("[MV_REFRESH] Refreshing mv_user_first_activity...");
      const mv1Start = Date.now();
      await client.query(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_user_first_activity",
      );
      logger.info(
        `[MV_REFRESH] mv_user_first_activity refreshed in ${Date.now() - mv1Start}ms`,
      );
    }

    // Refresh mv_daily_new_returning_users (depends on first view)
    if (existingViews.includes("mv_daily_new_returning_users")) {
      logger.info("[MV_REFRESH] Refreshing mv_daily_new_returning_users...");
      const mv2Start = Date.now();
      await client.query(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_new_returning_users",
      );
      logger.info(
        `[MV_REFRESH] mv_daily_new_returning_users refreshed in ${Date.now() - mv2Start}ms`,
      );
    }

    const totalDuration = Date.now() - startTime;
    logger.info(
      `[MV_REFRESH] All materialized views refreshed successfully in ${totalDuration}ms`,
    );

    return {
      status: "success",
      duration: totalDuration,
      refreshedViews: existingViews,
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
  if (isShuttingDown) {
    logger.warn('[MV_REFRESH] Skipping run — application shutting down');
    return;
  }
  // Check if already refreshing
  if (isRefreshingMaterializedViews) {
    logger.warn(
      "[MV_REFRESH] Skipping scheduled run - previous refresh still in progress",
    );
    return;
  }

  // Acquire lock
  isRefreshingMaterializedViews = true;

  try {
    logger.info(
      `[MV_REFRESH] Running scheduled refresh (${MV_REFRESH_SCHEDULE})`,
    );
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

    // Start Express server
    server = app.listen(PORT, () => {
      logger.info(`Telemetry log processor service started on port ${PORT}`);
    });

    // Run initial processing
    logger.info("Running initial telemetry log processing on startup...");
    await processTelemetryLogs(`process_${Date.now()}`);

    await refreshLeaderboardAggregation();

    // Refresh materialized views on startup (non-blocking)
    logger.info("Running initial materialized views refresh on startup...");
    refreshMaterializedViews().catch((err) => {
      logger.warn(
        "Initial materialized views refresh failed (views may not exist yet):",
        err.message,
      );
    });

    return server;
  } catch (err) {
    logger.error("Failed to start server:", err);
    process.exit(1);
  }
}

async function shutdown(reason) {
  if (isShuttingDown) return; // avoid double shutdown
  isShuttingDown = true;

  console.log(`[SHUTDOWN] Initiated due to: ${reason || 'manual stop'}`);

  try {
    // Close server if running
    if (server) {
      console.log('[SHUTDOWN] Closing HTTP server...');
      await new Promise((resolve) => server.close(resolve));
      console.log('[SHUTDOWN] HTTP server closed');
    }

    // Close DB pool
    console.log('[SHUTDOWN] Closing DB pool...');
    await pool.end();
    console.log('[SHUTDOWN] DB pool closed');
  } catch (err) {
    console.error('[SHUTDOWN] Error during cleanup:', err);
  }

  // Decide exit code
  // 0 -> manual stop (docker won't restart)
  // 1 -> crash/error (docker restarts)
  const exitCode =
    reason === 'SIGINT' || reason === 'SIGTERM' ? 0 : 1;

  console.log(`[SHUTDOWN] Exiting with code ${exitCode}`);
  process.exit(exitCode);
}

// Export for testing
module.exports = {
  app,
  startServer,
  processTelemetryLogs,
  ensureTablesExist,
  parseTelemetryMessage,
};

// Only start server if this file is run directly (not when required in tests)
if (require.main === module) {
  startServer();
}

module.exports = { app, pool };
