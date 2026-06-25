/**
 * Modular PII masking middleware for the telemetry processor.
 *
 * @example
 * const { pii, createPiiMiddleware } = require('./middleware/pii');
 *
 * // Default singleton (env: MASK_PII_ON_WRITE !== 'false')
 * pii.maskColumn('questiontext', rawQuestion);
 * pii.maskMessage(transcriptLine);
 *
 * // Custom instance
 * const stagingPii = createPiiMiddleware({ enabled: true });
 *
 * // Express — mask JSON on /api/* responses
 * app.use(pii.express());
 *
 * // Isolated persist namespace
 * pii.persist.maskRecord({ questiontext: 'Call 9876543210' });
 */

const { resolveConfig } = require("./config");
const { createTransforms } = require("./transforms");
const { createExpressMiddleware } = require("./express");
const engine = require("./engine");

/**
 * @param {Object} [options] Passed to resolveConfig (enabled, jsonbColumns, pathPatterns, matchAllPaths)
 * @returns {PiiMiddlewareInstance}
 */
function createPiiMiddleware(options = {}) {
  const config = resolveConfig(options);
  const transforms = createTransforms(config);

  const instance = {
    config,
    engine,

    isEnabled() {
      return config.enabled;
    },

    /** Persistence / ingest */
    persist: transforms,

    /** Aliases at top level for ergonomic use */
    maskField: transforms.maskField,
    maskColumn: transforms.maskColumn,
    maskMessage: transforms.maskMessage,
    maskPayload: transforms.maskPayload,
    maskRecord: transforms.maskRecord,
    maskRow: transforms.maskRow,
    maskTelemetryEvent: transforms.maskTelemetryEvent,

    /**
     * Express middleware factory (response masking).
     * @param {Object} [expressOptions]
     * @returns {import('express').RequestHandler}
     */
    express(expressOptions = {}) {
      return createExpressMiddleware(config, expressOptions);
    },

    /** Alias for express() */
    response(expressOptions = {}) {
      return createExpressMiddleware(config, expressOptions);
    },

    /**
     * Wrap an async handler and mask its return value (non-Express pipelines).
     * @template T
     * @param {(...args: any[]) => Promise<T>|T} fn
     * @returns {(...args: any[]) => Promise<T>}
     */
    wrap(fn) {
      return async (...args) => {
        const result = await fn(...args);
        return transforms.maskPayload(result);
      };
    },

    /**
     * Wrap a DB/event processor: masks each mapped field via maskColumn.
     * @param {Record<string, *>} fieldValues columnName -> raw value
     * @returns {Record<string, *>}
     */
    maskInsertFields(fieldValues) {
      const out = {};
      for (const [column, value] of Object.entries(fieldValues)) {
        out[column] = transforms.maskColumn(column, value);
      }
      return out;
    },
  };

  return instance;
}

/** Process-wide default instance */
const pii = createPiiMiddleware();

module.exports = {
  createPiiMiddleware,
  pii,
  engine,
  resolveConfig,
  createTransforms,
  createExpressMiddleware,
  REDACTION_MARKERS: engine.REDACTION_MARKERS,
};