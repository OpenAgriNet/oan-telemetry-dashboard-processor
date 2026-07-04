const DEFAULT_JSONB_COLUMNS = [
  "registered_location",
  "device_location",
  "agristack_location",
  "groupdetails",
  "answertext",
];

const DEFAULT_EXPRESS_PATH_PATTERNS = [
  /^\/api\//i,
];

/**
 * @typedef {Object} PiiMiddlewareConfig
 * @property {boolean} enabled
 * @property {Set<string>} jsonbColumns
 * @property {RegExp[]} pathPatterns
 * @property {boolean} matchAllPaths
 * @property {typeof import('./engine')} engine
 */

/**
 * @param {Object} [options]
 * @param {boolean} [options.enabled]
 * @param {string[]} [options.jsonbColumns]
 * @param {RegExp[]} [options.pathPatterns]
 * @param {boolean} [options.matchAllPaths]
 * @returns {PiiMiddlewareConfig}
 */
function resolveConfig(options = {}) {
  const envEnabled = process.env.MASK_PII_ON_WRITE !== "false";

  return {
    enabled: options.enabled !== undefined ? Boolean(options.enabled) : envEnabled,
    jsonbColumns: new Set(options.jsonbColumns || DEFAULT_JSONB_COLUMNS),
    pathPatterns: options.pathPatterns || DEFAULT_EXPRESS_PATH_PATTERNS,
    matchAllPaths: Boolean(options.matchAllPaths),
    engine: require("./engine"),
  };
}

module.exports = {
  DEFAULT_JSONB_COLUMNS,
  DEFAULT_EXPRESS_PATH_PATTERNS,
  resolveConfig,
};