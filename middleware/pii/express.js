/**
 * Express middleware — masks JSON API responses (read-path / defense-in-depth).
 */

function getRequestPath(req) {
  const source = req.originalUrl || req.url || "";
  return source.split("?")[0];
}

function shouldMaskRequest(req, config) {
  if (!config.enabled) return false;
  if (config.matchAllPaths) return true;

  const path = getRequestPath(req);
  return config.pathPatterns.some((pattern) => pattern.test(path));
}

function shouldAttemptJsonParse(res, body) {
  if (typeof body !== "string") return false;

  const contentType = String(res.get("Content-Type") || "").toLowerCase();
  if (contentType.includes("application/json")) return true;

  const trimmed = body.trim();
  return trimmed.startsWith("{") || trimmed.startsWith("[");
}

/**
 * @param {import('./config').PiiMiddlewareConfig} config
 * @param {Object} [options]
 * @param {RegExp[]} [options.pathPatterns]
 * @param {boolean} [options.matchAllPaths]
 * @returns {import('express').RequestHandler}
 */
function createExpressMiddleware(config, options = {}) {
  const runtimeConfig = {
    ...config,
    pathPatterns: options.pathPatterns || config.pathPatterns,
    matchAllPaths:
      options.matchAllPaths !== undefined
        ? options.matchAllPaths
        : config.matchAllPaths,
  };

  const { maskPayload } = require("./transforms").createTransforms(runtimeConfig);

  function piiExpressMiddleware(req, res, next) {
    if (!shouldMaskRequest(req, runtimeConfig)) return next();
    if (res.locals.__piiMaskingWrapped) return next();

    res.locals.__piiMaskingWrapped = true;

    const originalJson = res.json.bind(res);
    const originalSend = res.send.bind(res);

    res.json = (body) => originalJson(maskPayload(body));

    res.send = (body) => {
      if (body && typeof body === "object" && !Buffer.isBuffer(body)) {
        return originalSend(maskPayload(body));
      }

      if (shouldAttemptJsonParse(res, body)) {
        try {
          const parsed = JSON.parse(body);
          return originalSend(JSON.stringify(maskPayload(parsed)));
        } catch {
          return originalSend(body);
        }
      }

      return originalSend(body);
    };

    return next();
  }

  piiExpressMiddleware.shouldMaskRequest = (req) =>
    shouldMaskRequest(req, runtimeConfig);
  piiExpressMiddleware.pathPatterns = runtimeConfig.pathPatterns;

  return piiExpressMiddleware;
}

module.exports = { createExpressMiddleware, shouldMaskRequest, getRequestPath };