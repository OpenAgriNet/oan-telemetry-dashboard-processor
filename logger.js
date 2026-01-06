/**
 * Logger Module
 * 
 * Configures Winston logger for the telemetry query service
 * with daily rotation and automatic cleanup
 */

const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, colorize } = format;
require('winston-daily-rotate-file');

// Custom log format
const logFormat = printf(({ level, message, timestamp }) => {
  return `${timestamp} ${level}: ${message}`;
});

// Log configuration from environment
const LOG_MAX_SIZE = process.env.LOG_MAX_SIZE || '20m';
const LOG_MAX_FILES = process.env.LOG_MAX_FILES || '14d';
const LOG_COMPRESS = process.env.LOG_COMPRESS === 'true';

// Daily rotating file transport for all logs
const dailyRotateTransport = new transports.DailyRotateFile({
  filename: 'logs/telemetry-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  maxSize: LOG_MAX_SIZE,
  maxFiles: LOG_MAX_FILES,
  zippedArchive: LOG_COMPRESS,
  level: process.env.LOG_LEVEL || 'info',
});

// Daily rotating file transport for errors only
const errorRotateTransport = new transports.DailyRotateFile({
  filename: 'logs/error-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  maxSize: LOG_MAX_SIZE,
  maxFiles: LOG_MAX_FILES,
  zippedArchive: LOG_COMPRESS,
  level: 'error',
});

// Event handlers for rotate transport
dailyRotateTransport.on('rotate', (oldFilename, newFilename) => {
  console.log(`Log rotated: ${oldFilename} -> ${newFilename}`);
});

dailyRotateTransport.on('archive', (zipFilename) => {
  console.log(`Log archived: ${zipFilename}`);
});

// Create Winston logger
const logger = createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp(),
    logFormat
  ),
  transports: [
    // Console transport
    new transports.Console({
      format: combine(
        colorize(),
        timestamp(),
        logFormat
      )
    }),
    // Daily rotating file transport for all logs
    dailyRotateTransport,
    // Daily rotating file transport for errors
    errorRotateTransport,
  ]
});

// Log startup configuration
logger.info(`Logger initialized: level=${process.env.LOG_LEVEL || 'info'}, maxSize=${LOG_MAX_SIZE}, maxFiles=${LOG_MAX_FILES}, compress=${LOG_COMPRESS}`);

module.exports = logger;