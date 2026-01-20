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
          const msg = `Seeder exited with code ${code}${signal ? " signal " + signal : ""
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
const IS_NEW_BACKFILL_SCHEDULE = process.env.IS_NEW_BACKFILL_SCHEDULE || "*/30 * * * *"; // Run every 30 minutes
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


    // ============================================
    // V2 SCHEMA: Users and Sessions Tables (Simplified - no devices table)
    // ============================================

    // Create users table for V2 (includes device metadata)
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        uid VARCHAR(255) UNIQUE NOT NULL,
        -- User profile
        mobile VARCHAR(20),
        username VARCHAR(255),
        email VARCHAR(255),
        role VARCHAR(50),
        farmer_id VARCHAR(100),
        registered_location JSONB,
        device_location JSONB,
        agristack_location JSONB,
        -- Device metadata (Fingerprint.js)
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
        -- Timestamps
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    // Add device columns if they don't exist (for existing deployments)
    await client.query(`
      DO $$ 
      BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='fingerprint_id') THEN
          ALTER TABLE public.users ADD COLUMN fingerprint_id VARCHAR(64);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='browser_code') THEN
          ALTER TABLE public.users ADD COLUMN browser_code VARCHAR(10);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='browser_name') THEN
          ALTER TABLE public.users ADD COLUMN browser_name VARCHAR(50);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='browser_version') THEN
          ALTER TABLE public.users ADD COLUMN browser_version VARCHAR(20);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='device_code') THEN
          ALTER TABLE public.users ADD COLUMN device_code VARCHAR(10);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='device_name') THEN
          ALTER TABLE public.users ADD COLUMN device_name VARCHAR(50);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='device_model') THEN
          ALTER TABLE public.users ADD COLUMN device_model VARCHAR(100);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='os_code') THEN
          ALTER TABLE public.users ADD COLUMN os_code VARCHAR(10);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='os_name') THEN
          ALTER TABLE public.users ADD COLUMN os_name VARCHAR(50);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='os_version') THEN
          ALTER TABLE public.users ADD COLUMN os_version VARCHAR(20);
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

    // Create sessions table for V2 (no device_id - device info stored in users)
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.sessions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        session_id VARCHAR(64) UNIQUE NOT NULL,
        user_id UUID REFERENCES users(id) ON DELETE SET NULL,
        channel VARCHAR(255),
        session_start_at BIGINT NOT NULL,
        session_end_at BIGINT,
        duration_seconds INTEGER,
        render_duration_ms INTEGER,
        server_duration_ms INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(session_start_at)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sessions_channel ON sessions(channel)`);

    // Add session_id to existing tables for V2 linking
    await client.query(`
      ALTER TABLE questions ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_questions_session ON questions(session_id)`);

    await client.query(`
      ALTER TABLE feedback ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_feedback_session ON feedback(session_id)`);

    await client.query(`
      ALTER TABLE errorDetails ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id)
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_errordetails_session ON errorDetails(session_id)`);

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
    const uid = event.uid;
    if (!uid || uid === 'guest') {
      logger.debug(`Skipping user upsert for uid: ${uid}`);
      return null;
    }

    const target = event.edata?.eks?.target || {};
    const fingerprint = event.edata?.eks?.fingerprint_details;

    const result = await client.query(`
      INSERT INTO users (uid, mobile, username, email, role, farmer_id, 
                         registered_location, device_location, agristack_location,
                         fingerprint_id, browser_code, browser_name, browser_version,
                         device_code, device_name, device_model,
                         os_code, os_name, os_version,
                         first_seen_at, last_seen_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW(), NOW())
      ON CONFLICT (uid) DO UPDATE SET
        mobile = COALESCE(EXCLUDED.mobile, users.mobile),
        username = COALESCE(EXCLUDED.username, users.username),
        email = COALESCE(EXCLUDED.email, users.email),
        role = COALESCE(EXCLUDED.role, users.role),
        farmer_id = COALESCE(EXCLUDED.farmer_id, users.farmer_id),
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
        last_seen_at = NOW()
      RETURNING id
    `, [
      uid,
      target.mobile || null,
      target.username || null,
      target.email || null,
      target.role || null,
      target.farmer_id || null,
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
      fingerprint?.os?.version || null
    ]);

    const userId = result.rows[0]?.id;
    logger.debug(`User upserted: uid=${uid}, id=${userId}, fingerprint=${fingerprint?.device_id || 'none'}`);
    return userId;
  } catch (err) {
    logger.error(`Error processing user data: ${err.message}`);
    return null;
  }
}

// NOTE: processDeviceData removed - device metadata now stored in users table via processUserData

/**
 * Process session start (OE_START event)
 * @param {Object} client - Database client
 * @param {Object} event - Telemetry event
 * @param {string|null} userId - User UUID
 * @returns {string|null} - Session UUID or null
 */
async function processSessionStart(client, event, userId) {
  try {
    const sid = event.sid;
    if (!sid) {
      logger.debug('No session_id (sid) found, skipping session insert');
      return null;
    }

    const fingerprint = event.edata?.eks?.fingerprint_details;
    const sessionStartAt = fingerprint?.session?.session_start_at || event.ets;

    const result = await client.query(`
      INSERT INTO sessions (session_id, user_id, channel, session_start_at)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (session_id) DO UPDATE SET
        user_id = COALESCE(EXCLUDED.user_id, sessions.user_id)
      RETURNING id
    `, [sid, userId, event.channel, sessionStartAt]);

    const sessionId = result.rows[0]?.id;
    logger.debug(`Session started: sid=${sid}, id=${sessionId}`);
    return sessionId;
  } catch (err) {
    logger.error(`Error processing session start: ${err.message}`);
    return null;
  }
}

/**
 * Process session end (OE_END event) - update with end time, duration, performance
 * @param {Object} client - Database client
 * @param {Object} event - Telemetry event
 */
async function processSessionEnd(client, event) {
  try {
    const sid = event.sid;
    if (!sid) {
      logger.debug('No session_id (sid) found, skipping session end update');
      return;
    }

    const fingerprint = event.edata?.eks?.fingerprint_details;
    const sessionEndAt = fingerprint?.session?.session_end_at || event.ets;
    const sessionStartAt = fingerprint?.session?.session_start_at;

    // Calculate duration in seconds
    let durationSeconds = null;
    if (sessionStartAt && sessionEndAt) {
      durationSeconds = Math.floor((sessionEndAt - sessionStartAt) / 1000);
    }

    // Performance metrics
    const renderDurationMs = fingerprint?.performance?.render_duration_ms || null;
    const serverDurationMs = fingerprint?.performance?.server_duration_ms || null;

    await client.query(`
      UPDATE sessions SET
        session_end_at = $1,
        duration_seconds = $2,
        render_duration_ms = $3,
        server_duration_ms = $4
      WHERE session_id = $5
    `, [sessionEndAt, durationSeconds, renderDurationMs, serverDurationMs, sid]);

    logger.debug(`Session ended: sid=${sid}, duration=${durationSeconds}s`);
  } catch (err) {
    logger.error(`Error processing session end: ${err.message}`);
  }
}

/**
 * Get session UUID by session_id (sid)
 * @param {Object} client - Database client
 * @param {string} sid - Session ID from telemetry
 * @returns {string|null} - Session UUID or null
 */
async function getSessionIdBySid(client, sid) {
  try {
    if (!sid) return null;
    const result = await client.query(
      'SELECT id FROM sessions WHERE session_id = $1',
      [sid]
    );
    return result.rows[0]?.id || null;
  } catch (err) {
    logger.error(`Error getting session id: ${err.message}`);
    return null;
  }
}

// Parse telemetry message JSON
function parseTelemetryMessage(message, batchId = '', logIndex = 0) {
  try {
    logger.debug(`[${batchId}] [Log ${logIndex}] Parsing telemetry message (length: ${message?.length || 0} chars)`);
    // Parse the message string which is a JSON string
    const parsedMessage = JSON.parse(message);
    // Return the events array from the parsed message
    const events = parsedMessage.events || [];
    logger.debug(`[${batchId}] [Log ${logIndex}] Successfully parsed message, found ${events.length} events`);
    return events;
  } catch (err) {
    const messagePreview = message?.substring(0, 100) || 'empty';
    logger.error(`[${batchId}] [Log ${logIndex}] Error parsing telemetry message: ${err.message}`);
    logger.error(`[${batchId}] [Log ${logIndex}] Message preview: ${messagePreview}...`);
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
    logger.info(`[${batchId}] Step 3: Fetching unprocessed logs (BATCH_SIZE: ${BATCH_SIZE})...`);
    const queryStartTime = Date.now();
    const result = await client.query(
      `SELECT id, level, message, meta, cast(to_char(("timestamp")::TIMESTAMP,'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE]
    );
    const queryDuration = Date.now() - queryStartTime;
    logger.info(`[${batchId}] Step 3: Query completed in ${queryDuration}ms, found ${result.rows.length} logs`);

    if (result.rows.length === 0) {
      logger.info(`[${batchId}] Step 4: No new telemetry logs to process - committing empty transaction`);
      await client.query("COMMIT");
      logger.info(`[${batchId}] Step 4: Empty transaction committed`);
      return { processed: 0, status: "success" };
    }

    logger.info(`[${batchId}] Step 4: Starting to process ${result.rows.length} telemetry logs`);

    // Process each log
    for (let logIndex = 0; logIndex < result.rows.length; logIndex++) {
      const log = result.rows[logIndex];
      const logStartTime = Date.now();
      logger.info(`[${batchId}] [Log ${logIndex + 1}/${result.rows.length}] Processing log (timestamp: ${log.timestamp}, level: ${log.level})`);

      const events = parseTelemetryMessage(log.message, batchId, logIndex + 1);
      logger.info(`[${batchId}] [Log ${logIndex + 1}] Parsed ${events.length} events from message`);

      if (events.length === 0) {
        logger.warn(`[${batchId}] [Log ${logIndex + 1}] No events found in message - skipping to sync status update`);
      }

      for (let eventIndex = 0; eventIndex < events.length; eventIndex++) {
        const event = events[eventIndex];
        const eventType = event.eid;
        const eventUid = event.uid || 'unknown';
        const eventMid = event.mid || 'unknown';
        logger.debug(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}/${events.length}] Processing event type: ${eventType}, uid: ${eventUid}, mid: ${eventMid}`);

        let eventProcessed = false;
        let matchedProcessor = null;

        for (const key in eventProcessors) {
          const processor = eventProcessors[key];
          const verified = getNestedValue(
            event,
            processor["fieldVerification"]
          );

          logger.debug(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Checking processor '${key}': eventType match=${processor["eventType"] === eventType}, verified=${verified !== undefined}`);

          if (
            processor["eventType"] === eventType &&
            verified !== undefined &&
            !eventProcessed
          ) {
            matchedProcessor = key;
            logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Matched processor '${key}' for event type '${eventType}'`);

            const processorStartTime = Date.now();
            await processor["process"](client, event);
            const processorDuration = Date.now() - processorStartTime;

            logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Processor '${key}' completed in ${processorDuration}ms`);
            eventProcessed = true;
            totalEventsProcessed++;
            break;
          }
        }

        if (
          eventType !== "OE_END" &&
          eventType !== "OE_START" &&
          eventProcessed === false
        ) {
          logger.warn(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - No processor matched for event type: ${eventType} - sending to dead letter queue`);
          logger.debug(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] mid: ${eventMid} - Dead letter event uid: ${eventUid}, channel: ${event.channel || 'unknown'}`);

          await client.query(
            `Insert into dead_letter_logs(level, message, meta, event_name) values ($1, $2, $3, $4)`,
            [log.level, JSON.stringify(event), log.meta, eventType]
          );
          deadLetterCount++;
          logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Event inserted into dead_letter_logs`);
        } else if (eventType === "OE_START") {
          // V2: Process OE_START for user/session creation (device metadata stored in users)
          logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Processing OE_START for V2 (user/session)`);
          try {
            const userId = await processUserData(client, event);
            await processSessionStart(client, event, userId);
            logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] V2 OE_START processed: userId=${userId}`);
          } catch (v2Err) {
            logger.error(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] V2 OE_START error: ${v2Err.message}`);
          }
        } else if (eventType === "OE_END") {
          // V2: Process OE_END to update session with end time and performance
          logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] Processing OE_END for V2 (session end)`);
          try {
            await processSessionEnd(client, event);
            // Update user's last_seen_at with fingerprint info
            const uid = event.uid;
            if (uid && uid !== 'guest') {
              await client.query(`
                UPDATE users SET last_seen_at = NOW() WHERE uid = $1
              `, [uid]);
            }
            logger.info(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] V2 OE_END processed`);
          } catch (v2Err) {
            logger.error(`[${batchId}] [Log ${logIndex + 1}] [Event ${eventIndex + 1}] V2 OE_END error: ${v2Err.message}`);
          }
        }
      }

      // Update sync status for processed log
      logger.debug(`[${batchId}] [Log ${logIndex + 1}] Updating sync_status to 1 for processed log...`);
      await client.query(
        `UPDATE winston_logs SET sync_status = 1 WHERE id = $1`,
        [log.id]
      );

      const logDuration = Date.now() - logStartTime;
      processedCount++;
      logger.info(`[${batchId}] [Log ${logIndex + 1}] Log processing completed in ${logDuration}ms, sync_status updated`);
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

    return { processed: result.rows.length, status: "success", eventsProcessed: totalEventsProcessed, deadLetterCount };
  } catch (err) {
    logger.error(`[${batchId}] Step ERROR: Rolling back transaction due to error...`);
    await client.query("ROLLBACK");
    logger.error(`[${batchId}] Step ERROR: Transaction rolled back`);
    logger.error(`[${batchId}] Error details: ${err.message}`);
    logger.error(`[${batchId}] Error stack: ${err.stack}`);
    logger.error(`[${batchId}] Processed before error: ${processedCount} logs, ${totalEventsProcessed} events`);
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
  logger.info(`[${batchId}] Schedule: ${CRON_SCHEDULE}, Batch Size: ${BATCH_SIZE}`);

  try {
    const result = await processTelemetryLogs(batchId);
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
// V2: Materialized Views Refresh Cron
// Refresh analytics views every 15 minutes
// ============================================
const V2_MV_REFRESH_SCHEDULE = process.env.V2_MV_REFRESH_SCHEDULE || "*/15 * * * *";
let isRefreshingV2Views = false;

cron.schedule(V2_MV_REFRESH_SCHEDULE, async () => {
  if (isRefreshingV2Views) {
    logger.warn('[V2 MV] Skipping refresh - previous refresh still in progress');
    return;
  }

  isRefreshingV2Views = true;
  const refreshStartTime = Date.now();
  logger.info(`[V2 MV] Starting materialized views refresh (${V2_MV_REFRESH_SCHEDULE})`);

  const client = await pool.connect();
  try {
    // Check if views exist before refreshing
    const viewsExist = await client.query(`
      SELECT COUNT(*) as cnt FROM pg_matviews 
      WHERE matviewname IN ('mv_total_devices', 'mv_browser_density', 'mv_device_density', 
                            'mv_os_density', 'mv_session_duration', 'mv_active_users', 
                            'mv_performance_metrics')
    `);

    if (parseInt(viewsExist.rows[0].cnt) === 0) {
      logger.info('[V2 MV] No V2 materialized views found - skipping refresh');
      return;
    }

    // Refresh each view
    const views = [
      'mv_total_devices',
      'mv_browser_density',
      'mv_device_density',
      'mv_os_density',
      'mv_session_duration',
      'mv_active_users',
      'mv_performance_metrics'
    ];

    for (const view of views) {
      try {
        await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`);
        logger.debug(`[V2 MV] Refreshed ${view}`);
      } catch (viewErr) {
        if (viewErr.message.includes('does not exist')) {
          logger.debug(`[V2 MV] View ${view} does not exist yet`);
        } else if (viewErr.message.includes('cannot refresh')) {
          await client.query(`REFRESH MATERIALIZED VIEW ${view}`);
          logger.debug(`[V2 MV] Refreshed ${view} (non-concurrent)`);
        } else {
          logger.warn(`[V2 MV] Failed to refresh ${view}: ${viewErr.message}`);
        }
      }
    }

    const duration = Date.now() - refreshStartTime;
    logger.info(`[V2 MV] Materialized views refresh completed in ${duration}ms`);
  } catch (err) {
    logger.error(`[V2 MV] Refresh failed: ${err.message}`);
  } finally {
    client.release();
    isRefreshingV2Views = false;
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

    const existingViews = viewCheck.rows.map(r => r.matviewname);

    if (existingViews.length === 0) {
      logger.warn("[MV_REFRESH] No materialized views found. Please run the CREATE MATERIALIZED VIEW statements first.");
      return { status: "skipped", reason: "views_not_found" };
    }

    // Refresh mv_user_first_activity first (foundation view)
    if (existingViews.includes('mv_user_first_activity')) {
      logger.info("[MV_REFRESH] Refreshing mv_user_first_activity...");
      const mv1Start = Date.now();
      await client.query("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_user_first_activity");
      logger.info(`[MV_REFRESH] mv_user_first_activity refreshed in ${Date.now() - mv1Start}ms`);
    }

    // Refresh mv_daily_new_returning_users (depends on first view)
    if (existingViews.includes('mv_daily_new_returning_users')) {
      logger.info("[MV_REFRESH] Refreshing mv_daily_new_returning_users...");
      const mv2Start = Date.now();
      await client.query("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_new_returning_users");
      logger.info(`[MV_REFRESH] mv_daily_new_returning_users refreshed in ${Date.now() - mv2Start}ms`);
    }

    const totalDuration = Date.now() - startTime;
    logger.info(`[MV_REFRESH] All materialized views refreshed successfully in ${totalDuration}ms`);

    return {
      status: "success",
      duration: totalDuration,
      refreshedViews: existingViews
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

    // Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`Telemetry log processor service started on port ${PORT}`);
    });

    // Run initial processing
    logger.info('Running initial telemetry log processing on startup...');
    await processTelemetryLogs(`process_${Date.now()}`);

    await refreshLeaderboardAggregation();

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
