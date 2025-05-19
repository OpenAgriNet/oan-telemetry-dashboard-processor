/**
 * Logger Module
 * 
 * Configures Winston logger for the telemetry query service
 */

const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, colorize } = format;

// Custom log format
const logFormat = printf(({ level, message, timestamp }) => {
  return `${timestamp} ${level}: ${message}`;
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
    // File transport for errors
    new transports.File({ 
      filename: 'error.log', 
      level: 'error' 
    }),
    // File transport for all logs
    new transports.File({ 
      filename: 'combined.log' 
    })
  ]
});

module.exports = logger;