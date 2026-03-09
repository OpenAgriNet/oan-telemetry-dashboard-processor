/**
 * Event Processors Module
 * 
 * Manages dynamic event processor configuration and execution
 */

const logger = require('./logger');
const _ = require('lodash');

// Event processor registry
let eventProcessors = [];

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
    Object.keys(eventProcessors).forEach(key => delete eventProcessors[key]);
    //eventProcessors = [];
    // Register each processor from database
    for (const row of result.rows) {
      console.log(row);
      registerProcessor(row.id,row.event_type, row.table_name, row.field_mappings, row.field_verification);
    }

    console.log(eventProcessors);
    
    logger.info(`Loaded ${result.rows.length} event processors from database`);
  } catch (err) {
    console.error(err);
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
    // Check if the target table exists and create it if not
    await ensureTableExists(client, tableName, fieldMappings);
    
    // Insert or update event processor configuration
    await client.query(
      `INSERT INTO event_processors (event_type, table_name, field_mappings, field_verification)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (table_name)
       DO UPDATE SET event_type = $1, field_mappings = $3, field_verification = $4, updated_at = NOW()`,
      [eventType, tableName, JSON.stringify(fieldMappings), fieldVerification]
    );
    
    // Register the processor in memory
    registerProcessor(null, eventType, tableName, fieldMappings, fieldVerification);
    
    logger.info(`Registered event processor for event type: ${eventType}`);
    return { success: true };
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
async function ensureTableExists(client, tableName, fieldMappings) {
  try {
    // Check if table exists
    const tableExists = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      )`,
      [tableName]
    );
    
    if (!tableExists.rows[0].exists) {
      // Construct create table SQL
      let createTableSQL = `
        CREATE TABLE IF NOT EXISTS public.${tableName} (
          id SERIAL PRIMARY KEY,
      `;
      
      // Add columns for each field in the mapping
      Object.keys(fieldMappings).forEach(field => {
        // Determine column type based on field name hints
        let columnType = 'TEXT';
        if (field.toLowerCase().includes('id')) columnType = 'VARCHAR';
        if (field.toLowerCase().includes('details')) columnType = 'JSONB';
        if (field.toLowerCase().includes('ets')) columnType = 'BIGINT';
        if (field.toLowerCase().includes('text') && field !== 'answerText') columnType = 'TEXT';
        if (field.toLowerCase() === 'answertext') columnType = 'JSONB';
        
        createTableSQL += `${field.toLowerCase()} ${columnType},\n`;
      });
      
      // Add created_at timestamp
      createTableSQL += `created_at TIMESTAMP DEFAULT NOW()\n)`;
      
      // Create the table
      await client.query(createTableSQL);
      logger.info(`Created new table: ${tableName}`);
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
function registerProcessor(id,eventType, tableName, fieldMappings, fieldVerification) {
  let eventProcessorsData = {};
  eventProcessorsData["id"] = id;
  eventProcessorsData["eventType"] = eventType;
  eventProcessorsData["tableName"] = tableName;
  eventProcessorsData["fieldVerification"] = fieldVerification;
  eventProcessorsData["process"] = async (client, event) => {
    try {
      // Extract field values using mappings
      const fields = [];
      const values = [];
      const placeholders = [];
      let paramIndex = 1;

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

      Object.entries(fieldMappings).forEach(([field, path]) => {
        fields.push(field.toLowerCase());
        let value = resolveValueForField(field.toLowerCase(), path);
        
        // Handle telemetry context fields that are not in individual events
        // but are part of the telemetry configuration
        const telemetryContextFields = ['mobile', 'username', 'email', 'role', 'farmer_id'];
        const locationFields = ['registered_location', 'device_location', 'agristack_location'];
        
        if (telemetryContextFields.includes(path) || telemetryContextFields.includes(field.toLowerCase())) {
          // Try nested target context first
          value = resolveValueForField(field.toLowerCase(), path);
        } 
        // Handle location JSON formation
        else if (locationFields.includes(field.toLowerCase())) {
          // Construct JSON object for location data from individual district/village/taluka fields
          // The telemetry sends: registered_location_district, registered_location_village, registered_location_taluka
          const locationPrefix = field.toLowerCase(); // e.g., "registered_location"
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
        const isJsonField = ['registered_location', 'device_location', 'agristack_location', 'groupdetails', 'answertext', 'tools_used'].includes(field.toLowerCase());
        values.push(isJsonField ? (value === null ? null : value) : ((typeof value === 'object' && value !== null) ? JSON.stringify(value) : value));
        placeholders.push(`$${paramIndex++}`);
      });

      
      // Construct the SQL query
      const query = `
        INSERT INTO ${tableName} (${fields.join(', ')})
        VALUES (${placeholders.join(', ')})
      `;
      
      // Execute the query
      await client.query(query, values);
      logger.debug(`Processed ${eventType} event into ${tableName} table`);
    } catch (err) {
      logger.error(`Error processing ${eventType} event:`, err);
      throw err;
    }
  };
  eventProcessors.push(eventProcessorsData);
}

module.exports = {
  eventProcessors,
  getNestedValue,
  loadEventProcessors: {
    loadFromDatabase,
    registerEventProcessor
  },
  // Expose internal functions for testing
  _testing: {
    getNestedValue,
    registerProcessor,
    ensureTableExists
  }
};