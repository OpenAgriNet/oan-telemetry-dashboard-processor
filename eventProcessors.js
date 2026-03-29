/**
 * Event Processors Module
 * 
 * Manages dynamic event processor configuration and execution
 */

const logger = require('./logger');
const _ = require('lodash');
const { escapeIdentifier } = require('pg');

// Event processor registry
let eventProcessors = [];
const IDENTIFIER_REGEX = /^[a-z_][a-z0-9_]{0,62}$/;

class ProcessorConfigValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProcessorConfigValidationError';
    this.code = 'INVALID_PROCESSOR_CONFIG';
    this.statusCode = 400;
  }
}

function isProcessorConfigValidationError(err) {
  return err instanceof ProcessorConfigValidationError || err?.code === 'INVALID_PROCESSOR_CONFIG';
}

function normalizeFieldMappings(fieldMappings) {
  if (!fieldMappings || typeof fieldMappings !== 'object' || Array.isArray(fieldMappings)) {
    throw new ProcessorConfigValidationError('fieldMappings must be a non-empty object');
  }

  return fieldMappings;
}

function resolveIdentifier(identifier, identifierType = 'identifier') {
  if (typeof identifier !== 'string') {
    throw new ProcessorConfigValidationError(`${identifierType} must be a string`);
  }

  const canonicalName = identifier.trim().toLowerCase();
  if (!IDENTIFIER_REGEX.test(canonicalName)) {
    throw new ProcessorConfigValidationError(`Invalid ${identifierType}: ${identifier}`);
  }

  return {
    canonicalName,
    escapedName: escapeIdentifier(canonicalName),
  };
}

function buildProcessorSqlConfig(tableName, fieldMappings) {
  const normalizedFieldMappings = normalizeFieldMappings(fieldMappings);
  const tableIdentifier = resolveIdentifier(tableName, 'table name');
  const seenColumns = new Map();
  const columns = Object.entries(normalizedFieldMappings).map(([field, path]) => {
    const columnIdentifier = resolveIdentifier(field, 'column name');
    const duplicateSourceField = seenColumns.get(columnIdentifier.canonicalName);

    if (duplicateSourceField) {
      throw new ProcessorConfigValidationError(
        `Duplicate column name after normalization: ${field} conflicts with ${duplicateSourceField}`
      );
    }

    seenColumns.set(columnIdentifier.canonicalName, field);

    return {
      sourceField: field,
      mappingPath: path,
      normalizedField: columnIdentifier.canonicalName,
      escapedField: columnIdentifier.escapedName,
    };
  });

  if (columns.length === 0) {
    throw new ProcessorConfigValidationError('fieldMappings must include at least one field');
  }

  const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
  const escapedColumns = columns.map((column) => column.escapedField).join(', ');

  return {
    canonicalTableName: tableIdentifier.canonicalName,
    escapedTableName: tableIdentifier.escapedName,
    columns,
    insertQuery: `
        INSERT INTO public.${tableIdentifier.escapedName} (${escapedColumns})
        VALUES (${placeholders})
      `,
  };
}

/**
 * Get a nested property from an object using dot notation path
 * 
 * @param {Object} obj - The object to access
 * @param {String} path - The path to the property (e.g., "edata.eks.target.questionText")
 * @returns {any} - The value at the specified path or undefined if not found
 */
function getNestedValue(obj, path) {
  return _.get(obj, path);
}

/**
 * Load event processors from the database
 * 
 * @param {Object} pool - Database connection pool
 */
async function loadFromDatabase(pool) {
  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT * FROM event_processors WHERE is_active = true`
    );

    // Clear existing processors
    eventProcessors.length = 0;
    // Register each processor from database
    for (const row of result.rows) {
      try {
        registerProcessor(
          row.id,
          row.event_type,
          row.table_name,
          row.field_mappings,
          row.field_verification
        );
      } catch (err) {
        if (isProcessorConfigValidationError(err)) {
          logger.error(
            `Skipping invalid event processor configuration (id: ${row.id}, event_type: ${row.event_type}, table_name: ${row.table_name}): ${err.message}`
          );
          continue;
        }
        throw err;
      }
    }

    logger.info(`Loaded ${result.rows.length} event processors from database`);
  } catch (err) {
    logger.error('Error loading event processors from database:', err);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Register a new event processor in the database
 * 
 * @param {String} eventType - The event type to process (e.g., "OE_ITEM_RESPONSE")
 * @param {String} tableName - The target table name
 * @param {Object} fieldMappings - Mapping of table columns to event data paths
 * @returns {Object} - Result of the registration
 */
async function registerEventProcessor(eventType, tableName, fieldMappings, fieldVerification, pool) {
  const client = await pool.connect();
  try {
    const sqlConfig = buildProcessorSqlConfig(tableName, fieldMappings);

    const collisionCheck = await client.query(
      `SELECT id, event_type, table_name
       FROM event_processors
       WHERE LOWER(TRIM(table_name)) = $1`,
      [sqlConfig.canonicalTableName]
    );

    const hasCanonicalCollision = collisionCheck.rows.some((row) => row.table_name !== sqlConfig.canonicalTableName);
    if (hasCanonicalCollision) {
      throw new ProcessorConfigValidationError(
        `Table name ${tableName} conflicts with an existing processor after canonicalization`
      );
    }

    // Check if the target table exists and create it if not
    await ensureTableExists(client, tableName, fieldMappings, sqlConfig);

    // Insert or update event processor configuration
    await client.query(
      `INSERT INTO event_processors (event_type, table_name, field_mappings, field_verification)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (table_name)
       DO UPDATE SET event_type = $1, field_mappings = $3, field_verification = $4, updated_at = NOW()`,
      [eventType, sqlConfig.canonicalTableName, JSON.stringify(fieldMappings), fieldVerification]
    );

    // Register the processor in memory
    registerProcessor(null, eventType, sqlConfig.canonicalTableName, fieldMappings, fieldVerification, sqlConfig);

    logger.info(`Registered event processor for event type: ${eventType}`);
    return { success: true, tableName: sqlConfig.canonicalTableName };
  } catch (err) {
    logger.error(`Error registering event processor for ${eventType}:`, err);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Create a target table if it doesn't already exist
 * 
 * @param {Object} client - Database client
 * @param {String} tableName - Table name to create
 * @param {Object} fieldMappings - Field mappings that define columns
 */
async function ensureTableExists(client, tableName, fieldMappings, sqlConfig = null) {
  try {
    const resolvedSqlConfig = sqlConfig || buildProcessorSqlConfig(tableName, fieldMappings);

    // Check if table exists
    const tableExists = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      )`,
      [resolvedSqlConfig.canonicalTableName]
    );

    if (!tableExists.rows[0].exists) {
      // Construct create table SQL
      let createTableSQL = `
        CREATE TABLE IF NOT EXISTS public.${resolvedSqlConfig.escapedTableName} (
          id SERIAL PRIMARY KEY,
      `;

      // Add columns for each field in the mapping
      resolvedSqlConfig.columns.forEach(({ normalizedField, escapedField }) => {
        // Determine column type based on field name hints
        let columnType = 'TEXT';
        if (normalizedField.includes('id')) columnType = 'VARCHAR';
        if (normalizedField.includes('details')) columnType = 'JSONB';
        if (normalizedField.includes('ets')) columnType = 'BIGINT';
        if (normalizedField.includes('text') && normalizedField !== 'answertext') columnType = 'TEXT';
        if (normalizedField === 'answertext') columnType = 'JSONB';

        createTableSQL += `${escapedField} ${columnType},\n`;
      });

      // Add created_at timestamp
      createTableSQL += `created_at TIMESTAMP DEFAULT NOW()\n)`;

      // Create the table
      await client.query(createTableSQL);
      logger.info(`Created new table: ${resolvedSqlConfig.canonicalTableName}`);
    }
  } catch (err) {
    logger.error(`Error ensuring table exists for ${tableName}:`, err);
    throw err;
  }
}

/**
 * Register an event processor in memory
 * 
 * @param {String} eventType - The event type to process
 * @param {String} tableName - The target table name
 * @param {Object} fieldMappings - Mapping of table columns to event data paths
 */
function registerProcessor(id, eventType, tableName, fieldMappings, fieldVerification, sqlConfig = null) {
  const resolvedSqlConfig = sqlConfig || buildProcessorSqlConfig(tableName, fieldMappings);
  let eventProcessorsData = {};
  eventProcessorsData["id"] = id;
  eventProcessorsData["eventType"] = eventType;
  eventProcessorsData["tableName"] = resolvedSqlConfig.canonicalTableName;
  eventProcessorsData["fieldVerification"] = fieldVerification;
  eventProcessorsData["process"] = async (client, event) => {
    try {
      // Extract field values using mappings
      const values = [];

      function resolveValueForField(fieldName, mappingPath) {
        const direct = getNestedValue(event, mappingPath);
        if (direct !== undefined && direct !== null) return direct;

        // Try common nested context: edata.eks.target.*
        const targetPath = `edata.eks.target.${mappingPath}`;
        const fromTarget = getNestedValue(event, targetPath);
        if (fromTarget !== undefined && fromTarget !== null) return fromTarget;

        // Fallbacks for plain fields
        if (event[mappingPath] !== undefined && event[mappingPath] !== null) return event[mappingPath];
        if (event[fieldName] !== undefined && event[fieldName] !== null) return event[fieldName];
        return null;
      }

      resolvedSqlConfig.columns.forEach(({ normalizedField, mappingPath }) => {
        let value = resolveValueForField(normalizedField, mappingPath);

        // Handle telemetry context fields that are not in individual events
        // but are part of the telemetry configuration
        const telemetryContextFields = ['mobile', 'username', 'email', 'role', 'farmer_id'];
        const locationFields = ['registered_location', 'device_location', 'agristack_location'];

        if (telemetryContextFields.includes(mappingPath) || telemetryContextFields.includes(normalizedField)) {
          // Try nested target context first
          value = resolveValueForField(normalizedField, mappingPath);
        }
        // Handle location JSON formation
        else if (locationFields.includes(normalizedField)) {
          // Construct JSON object for location data from individual district/village/taluka fields
          // The telemetry sends: registered_location_district, registered_location_village, registered_location_taluka
          const locationPrefix = normalizedField; // e.g., "registered_location"
          const districtPath = `${locationPrefix}_district`;
          const villagePath = `${locationPrefix}_village`;
          const talukaPath = `${locationPrefix}_taluka`;
          const lgdCodePath = `${locationPrefix}_lgd_code`;
          value = {
            district: resolveValueForField(districtPath, districtPath),
            village: resolveValueForField(villagePath, villagePath),
            taluka: resolveValueForField(talukaPath, talukaPath),
            lgd_code: resolveValueForField(lgdCodePath, lgdCodePath)
          };

          // If all location fields are null, set the entire value to null
          if (!value.district && !value.village && !value.taluka && !value.lgd_code) {
            value = null;
          }
        }

        // Ensure JSONB fields are sent as proper JSON values in SQL (pg driver handles JS objects)
        const isJsonField = ['registered_location', 'device_location', 'agristack_location', 'groupdetails', 'answertext'].includes(normalizedField);
        values.push(isJsonField ? (value === null ? null : value) : ((typeof value === 'object' && value !== null) ? JSON.stringify(value) : value));
      });

      // Execute the query
      await client.query(resolvedSqlConfig.insertQuery, values);
      logger.debug(`Processed ${eventType} event into ${resolvedSqlConfig.canonicalTableName} table`);
    } catch (err) {
      // Log the error with event details but skip this record instead of failing the batch
      const eventMid = event?.mid || 'unknown';
      const eventId = event?.edata?.eks?.target?.id || event?.object?.id || 'unknown';
      logger.error(`Error processing ${eventType} event (mid: ${eventMid}, id: ${eventId}): ${err.message}`);
      logger.error(`Skipping bad record. Event data: ${JSON.stringify(event).substring(0, 500)}...`);
      // Don't throw - allow batch to continue processing other events
      return { success: false, skipped: true, error: err.message };
    }
  };
  eventProcessors.push(eventProcessorsData);
}

module.exports = {
  eventProcessors,
  getNestedValue,
  buildProcessorSqlConfig,
  isProcessorConfigValidationError,
  loadEventProcessors: {
    loadFromDatabase,
    registerEventProcessor
  },
  // Expose internal functions for testing
  _testing: {
    getNestedValue,
    registerProcessor,
    ensureTableExists,
    buildProcessorSqlConfig,
    resolveIdentifier,
    isProcessorConfigValidationError,
    ProcessorConfigValidationError
  }
};
