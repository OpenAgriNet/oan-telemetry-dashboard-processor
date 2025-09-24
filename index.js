/**
 * Telemetry Log Processor Microservice
 * 
 * This service processes telemetry logs from PostgreSQL database
 * - Extracts configurable event types and stores in respective tables
 * - Updates sync_status after processing
 * - Runs every 5 minutes processing configurable batch size
 */
const fs = require('fs');
const path = require('path');
const express = require('express');
const { Pool } = require('pg');
const cron = require('node-cron');
const dotenv = require('dotenv');
const logger = require('./logger');
const { eventProcessors, loadEventProcessors, getNestedValue } = require('./eventProcessors');
const { forEach } = require('lodash');

// Load environment variables from .env file
dotenv.config();

// Create Express application
const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const BATCH_SIZE = process.env.BATCH_SIZE || 10;
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '*/5 * * * *';

// PostgreSQL connection pool
// const pool = new Pool({
//   connectionString: process.env.DATABASE_URL,
// });

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

    // Add missing columns to questions table if they don't exist
    await client.query(`
      DO $$ 
      BEGIN 
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

    // Insert default event processors if they don't exist
    await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
      VALUES 
        ('OE_ITEM_RESPONSE', 'questions', '{
          "uid": "uid",
          "sid": "sid",
          "groupDetails": "edata.eks.target.questionsDetails.groupDetails",
          "channel": "channel",
          "ets": "ets",
          "questionText": "edata.eks.target.questionsDetails.questionText",
          "questionSource": "edata.eks.target.questionsDetails.questionSource",
          "answerText": "edata.eks.target.questionsDetails.answerText",
          "answer": "edata.eks.target.questionsDetails.answerText.answer",
          "mobile": "mobile",
          "username": "username",
          "email": "email",
          "role": "role",
          "farmer_id": "farmer_id",
          "registered_location": "registered_location",
          "device_location": "device_location",
          "agristack_location": "agristack_location"
        }','edata.eks.target.questionsDetails')
      ON CONFLICT (table_name) DO NOTHING;
    `);

     await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
      VALUES 
        ('OE_ITEM_RESPONSE', 'errorDetails', '{
          "uid": "uid",
          "sid": "sid",
          "channel": "channel",
          "ets": "ets",
          "errorText": "edata.eks.target.errorDetails.errorText",
          "qid": "edata.eks.qid",
          "mobile": "mobile",
          "username": "username",
          "email": "email",
          "role": "role",
          "farmer_id": "farmer_id",
          "registered_location": "registered_location",
          "device_location": "device_location",
          "agristack_location": "agristack_location"
        }','edata.eks.target.errorDetails')
      ON CONFLICT (table_name) DO NOTHING;
    `);

    await client.query(`
      INSERT INTO public.event_processors (event_type, table_name, field_mappings, field_verification)
      VALUES 
        ('OE_ITEM_RESPONSE', 'feedback', '{
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
          "mobile": "mobile",
          "username": "username",
          "email": "email",
          "role": "role",
          "farmer_id": "farmer_id",
          "registered_location": "registered_location",
          "device_location": "device_location",
          "agristack_location": "agristack_location"
        }','edata.eks.target.feedbackDetails')
      ON CONFLICT (table_name) DO NOTHING;
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
    logger.info('Database tables verified and created if needed');
  } catch (err) {
    logger.error('Error ensuring tables exist:', err);
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
    logger.error('Error parsing telemetry message:', err);
    return [];
  }
}

// Process telemetry logs
async function processTelemetryLogs() {
  const client = await pool.connect();
  try {
    // Begin transaction
    await client.query('BEGIN');

    // Get unprocessed logs
    const result = await client.query(
      `SELECT  level, message, meta, cast(to_char(("timestamp")::TIMESTAMP,'yyyymmddhhmiss') as BigInt) as timestamp, sync_status FROM winston_logs 
       WHERE sync_status = 0 
       ORDER BY "timestamp" ASC 
       LIMIT $1`,
      [BATCH_SIZE]
    );
    if (result.rows.length === 0) {
      logger.info('No new telemetry logs to process');
      await client.query('COMMIT');
      return { processed: 0, status: 'success' };
    }

    logger.info(`Processing ${result.rows.length} telemetry logs`);

    // Process each log
    for (const log of result.rows) {
      const events = parseTelemetryMessage(log.message);

      //console.log(JSON.stringify(eventProcessors));
      for (const event of events) {
        const eventType = event.eid;
        let eventProssed = false;
        forEach(eventProcessors, async (processor, key) => {
          const verified = getNestedValue(event, processor["fieldVerification"]);
          if (processor['eventType'] === eventType && verified !== undefined && !eventProssed) {
            eventProssed = true;
            await processor["process"](client, event);
            // Come out of the loop if we find a matching processo
            //logger.info(`Processed event type: ${eventType}`);
          }
        });
        if (eventType !== 'OE_END' && eventType !== 'OE_START' && eventProssed === false) {
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
    await client.query('COMMIT');
    logger.info(`Successfully processed ${result.rows.length} telemetry logs`);
    return { processed: result.rows.length, status: 'success' };
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error('Error processing telemetry logs:', err);
    return { processed: 0, status: 'error', error: err.message };
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
    const groupDetails = event.edata?.eks?.target?.questionsDetails?.groupDetails || [];
    const channel = event.channel;
    const ets = event.ets;
    const questionText = event.edata?.eks?.target?.questionsDetails?.questionText;
    const questionSource = event.edata?.eks?.target?.questionsDetails?.questionSource;
    const answerText = event.edata?.eks?.target?.questionsDetails?.answerText;
    const answer = answerText?.answer;

    // Insert data into questions table
    await client.query(
      `INSERT INTO questions (
        uid, sid, group_details, channel, ets, 
        question_text, question_source, answer_text, answer
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        uid, sid, JSON.stringify(groupDetails), channel, ets,
        questionText, questionSource, JSON.stringify(answerText), answer
      ]
    );

    logger.info(`Processed question data for uid: ${uid}, question: ${questionText?.substring(0, 30)}...`);
  } catch (err) {
    logger.error('Error processing question data:', err);
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
        uid, sid, JSON.stringify(groupDetails), channel, ets,
        feedbackText, sessionId, questionText,
        answerText, feedbackType
      ]
    );

    logger.info(`Processed feedback data for uid: ${uid}, feedback type: ${feedbackType}`);
  } catch (err) {
    logger.error('Error processing feedback data:', err);
    throw err;
  }
}

// Schedule telemetry processing with configurable cron schedule
cron.schedule(CRON_SCHEDULE, async () => {
  logger.info(`Running scheduled telemetry log processing (${CRON_SCHEDULE})`);
  await processTelemetryLogs();
});

// API endpoint to manually trigger processing
app.post('/api/process-logs', async (req, res) => {
  try {
    const result = await processTelemetryLogs();
    res.status(200).json({
      message: 'Telemetry log processing triggered successfully',
      ...result
    });
  } catch (err) {
    logger.error('Error triggering telemetry log processing:', err);
    res.status(500).json({ error: 'Failed to process telemetry logs', details: err.message });
  }
});

// API endpoint to get configured event processors
app.get('/api/event-processors', (req, res) => {
  const processors = Object.keys(eventProcessors).map(eventType => ({
    eventType,
    isActive: true
  }));

  res.status(200).json({ processors });
});

// API endpoint to register a new event processor configuration
app.post('/api/event-processors', async (req, res) => {
  try {
    const { eventType, tableName, fieldMappings } = req.body;

    if (!eventType || !tableName || !fieldMappings) {
      return res.status(400).json({
        error: 'Missing required parameters: eventType, tableName, and fieldMappings are required'
      });
    }

    // Register the new event processor configuration
    const result = await loadEventProcessors.registerEventProcessor(eventType, tableName, fieldMappings);

    res.status(201).json({
      message: 'Event processor registered successfully',
      eventType,
      tableName,
      ...result
    });
  } catch (err) {
    logger.error('Error registering event processor:', err);
    res.status(500).json({ error: 'Failed to register event processor', details: err.message });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'UP',
    version: process.env.npm_package_version || '1.0.0',
    eventProcessors: Object.keys(eventProcessors).length
  });
});

// Start server
async function startServer() {
  try {
    // Ensure database tables exist
    await ensureTablesExist();

    // Load configured event processors
    await loadEventProcessors.loadFromDatabase(pool);

    // Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`Telemetry log processor service started on port ${PORT}`);
    });

    // Run initial processing
    await processTelemetryLogs();

    return server;
  } catch (err) {
    logger.error('Failed to start server:', err);
    process.exit(1);
  }
}

// Handle shutdown gracefully
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  pool.end();
  process.exit(0);
});

// Export for testing
module.exports = {
  app,
  startServer,
  processTelemetryLogs,
  ensureTablesExist,
  parseTelemetryMessage
};

// Only start server if this file is run directly (not when required in tests)
if (require.main === module) {
  startServer();
}

module.exports = { app, pool };
