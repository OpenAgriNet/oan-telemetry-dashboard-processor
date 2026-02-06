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
      "SELECT COUNT(*)::bigint AS cnt FROM public.village_list"
    );
    const cnt = Number(rows[0].cnt || 0);
    logger.info(`village_list row count = ${cnt}`);

    if (cnt > 0) {
      logger.info("Villages table already seeded — skipping seeder.");
      return;
    }

    // spawn the existing seeder script (do not edit the seeder file)
    logger.info(
      "Villages table empty — running seeder script (seed_villages_stream.js). This may take some time..."
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
(async () => {
  try {
    await ensureVillagesSeeded();
    // continue with the rest of your startup:
    // await ensureTablesExist();
    // await loadEventProcessors.loadFromDatabase(pool);
    // start server...
  } catch (err) {
    logger.error("Startup aborted due to seeding failure:", err);
    process.exit(1);
  }
})();

const PORT = process.env.PORT || 3000;
const BATCH_SIZE = process.env.BATCH_SIZE || 10;
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "*/5 * * * *";
const LEADERBOARD_REFRESH_SCHEDULE =
  process.env.LEADERBOARD_REFRESH_SCHEDULE || "0 1 * * *"; // Run at 1 AM every day
const IS_NEW_BACKFILL_SCHEDULE = process.env.IS_NEW_BACKFILL_SCHEDULE || "*/30 * * * *" ; // Run every 30 minutes

// Ensure winston_logs table has sync_status column
async function ensureWinstonLogsSyncStatus() {
  const client = await pool.connect();
  try {
    // Check if sync_status column exists
    const columnExists = await client.query(`
      SELECT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'winston_logs' 
        AND column_name = 'sync_status'
      )
    `);

    if (!columnExists.rows[0].exists) {
      logger.info("Adding sync_status column to winston_logs table...");
      await client.query(`
        ALTER TABLE public.winston_logs 
        ADD COLUMN IF NOT EXISTS sync_status integer DEFAULT 0
      `);
      
      // Set all existing rows to sync_status = 0 so they get processed
      // (This handles the case where column was just added with NULL values)
      await client.query(`
        UPDATE public.winston_logs 
        SET sync_status = 0 
        WHERE sync_status IS NULL
      `);
      
      logger.info("Successfully added sync_status column to winston_logs table");
    } else {
      logger.debug("sync_status column already exists in winston_logs table");
    }
  } catch (err) {
    logger.error("Error ensuring sync_status column exists:", err);
    throw err;
  } finally {
    client.release();
  }
}

// Ensure necessary tables exist
async function ensureTablesExist() {
  const client = await pool.connect();
  try {
    // First ensure winston_logs has sync_status column
    await ensureWinstonLogsSyncStatus();

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
  ADD COLUMN IF NOT EXISTS is_new SMALLINT DEFAULT 0 NOT NULL;`)

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
function parseTelemetryMessage(message) {
  try {
    // Parse the message string which is a JSON string
    const parsedMessage = JSON.parse(message);
    // Return the events array from the parsed message
    return parsedMessage.events || [];
  } catch (err) {
    logger.error("Error parsing telemetry message:", err);
    return [];
  }
}

// Process telemetry logs
async function processTelemetryLogs() {
  const client = await pool.connect();
  try {
    // Begin transaction
    await client.query("BEGIN");

    // Get unprocessed logs
    const result = await client.query(
      `SELECT  level, message, meta, cast(to_char(("timestamp")::TIMESTAMP,'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE]
    );
    if (result.rows.length === 0) {
      logger.info("No new telemetry logs to process");
      await client.query("COMMIT");
      return { processed: 0, status: "success" };
    }

    logger.info(`Processing ${result.rows.length} telemetry logs`);

    // Process each log
    for (const log of result.rows) {
      const events = parseTelemetryMessage(log.message);

      //console.log(JSON.stringify(eventProcessors));
      for (const event of events) {
        const eventType = event.eid;
        let eventProssed = false;
        for (const key in eventProcessors) {
          const processor = eventProcessors[key];
          const verified = getNestedValue(
            event,
            processor["fieldVerification"]
          );
          if (
            processor["eventType"] === eventType &&
            verified !== undefined &&
            !eventProssed
          ) {
            eventProssed = true;
            await processor["process"](client, event);
            eventProssed = true;
            break;
          }
        }
        if (
          eventType !== "OE_END" &&
          eventType !== "OE_START" &&
          eventProssed === false
        ) {
          logger.debug(`No processor defined for event type: ${eventType}`);

          await client.query(
            `Insert into dead_letter_logs(level, message, meta, event_name) values ($1, $2, $3, $4)`,
            [log.level, JSON.stringify(event), log.meta, eventType]
          );
        }
        // Check if we have a processor for this event type
      }
      // Update sync status for processed log
      //console.log(`Updating sync status for log with timestamp: ${log.timestamp}`);
      //console.log(`select * from winston_logs WHERE "timestamp" = ${log.timestamp} AND message = ${log.message}`);
      await client.query(
        `UPDATE winston_logs SET sync_status = 1 WHERE cast(to_char(("timestamp")::TIMESTAMP,'yyyymmddhhmiss') as BigInt) = $1  AND message = $2`,
        [log.timestamp, log.message]
      );
    }

    // Commit transaction
    await client.query("COMMIT");
    logger.info(`Successfully processed ${result.rows.length} telemetry logs`);
    return { processed: result.rows.length, status: "success" };
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error("Error processing telemetry logs:", err);
    return { processed: 0, status: "error", error: err.message };
  } finally {
    client.release();
  }
}

// Process OE_ITEM_RESPONSE data
async function processQuestionData(client, event) {
  try {
    // Extract required fields
    const uid = event.uid;
    const sid = event.sid;
    const groupDetails =
      event.edata?.eks?.target?.questionsDetails?.groupDetails || [];
    const channel = event.channel;
    const ets = event.ets;
    const questionText =
      event.edata?.eks?.target?.questionsDetails?.questionText;
    const questionSource =
      event.edata?.eks?.target?.questionsDetails?.questionSource;
    const answerText = event.edata?.eks?.target?.questionsDetails?.answerText;
    const answer = answerText?.answer;

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
  ]
);

    logger.info(
      `Processed question data for uid: ${uid}, question: ${questionText?.substring(
        0,
        30
      )}...`
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
      ]
    );

    logger.info(
      `Processed feedback data for uid: ${uid}, feedback type: ${feedbackType}`
    );
  } catch (err) {
    logger.error("Error processing feedback data:", err);
    throw err;
  }
}

// Schedule telemetry processing with configurable cron schedule
cron.schedule(CRON_SCHEDULE, async () => {
  logger.info(`Running scheduled telemetry log processing (${CRON_SCHEDULE})`);
  await processTelemetryLogs();
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
  try {
    const result = await processTelemetryLogs();
    res.status(200).json({
      message: "Telemetry log processing triggered successfully",
      ...result,
    });
  } catch (err) {
    logger.error("Error triggering telemetry log processing:", err);
    res.status(500).json({
      error: "Failed to process telemetry logs",
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
      fieldMappings
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

// Function to refresh user location aggregation data
const LEADERBOARD_CUTOFF_DATE =
  process.env.LEADERBOARD_CUTOFF_DATE || "2025-10-01 00:00:00";
async function refreshLeaderboardAggregation() {
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
        WHERE q.created_at >= $1
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
        WHERE q.created_at >= $1
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
      [LEADERBOARD_CUTOFF_DATE]
    );

    await client.query("COMMIT");
    logger.info(
      "Leaderboard refresh completed successfully (most-recent-location)."
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
    `Running scheduled leaderboard refresh (${LEADERBOARD_REFRESH_SCHEDULE})`
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
    const server = app.listen(PORT, () => {
      logger.info(`Telemetry log processor service started on port ${PORT}`);
    });

    // Run initial processing
    await processTelemetryLogs();

    await refreshLeaderboardAggregation();
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
  startServer,
  processTelemetryLogs,
  ensureTablesExist,
  ensureWinstonLogsSyncStatus,
  parseTelemetryMessage,
  pool,
};

// Only start server if this file is run directly (not when required in tests)
if (require.main === module) {
  startServer();
}
