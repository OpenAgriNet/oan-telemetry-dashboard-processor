/**
 * Persistence / ingest transforms — mask values before DB write or pipeline handoff.
 */

/**
 * @param {import('./config').PiiMiddlewareConfig} config
 */
function createTransforms(config) {
  const { engine, jsonbColumns, enabled: isEnabled } = config;

  function maskField(key, value) {
    if (!isEnabled) return value;
    if (value === null || value === undefined) return value;

    const column = String(key).toLowerCase();

    if (typeof value === "object") {
      return engine.maskApiResponse(value, key);
    }

    if (jsonbColumns.has(column) && typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        return JSON.stringify(engine.maskApiResponse(parsed, key));
      } catch {
        return engine.maskByKeyValue(key, value);
      }
    }

    return engine.maskByKeyValue(key, value);
  }

  function maskColumn(columnName, value) {
    return maskField(columnName, value);
  }

  function maskMessage(content) {
    if (!isEnabled || content === null || content === undefined) {
      return content;
    }
    const text = typeof content === "string" ? content : String(content);
    if (!text.length) return text;
    return engine.maskByKeyValue("content", text);
  }

  function maskPayload(payload, parentKey = null) {
    if (!isEnabled) return payload;
    return engine.maskApiResponse(payload, parentKey);
  }

  function maskRecord(record) {
    if (!isEnabled || record === null || record === undefined) {
      return record;
    }
    if (typeof record !== "object" || Array.isArray(record)) {
      return maskPayload(record);
    }

    const out = {};
    for (const [key, value] of Object.entries(record)) {
      if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        out[key] = maskPayload(value, key);
      } else {
        out[key] = maskField(key, value);
      }
    }
    return out;
  }

  /**
   * Mask parallel INSERT column lists: fields[i] maps to values[i].
   */
  function maskRow(fields, values) {
    if (!isEnabled) return values;
    if (!Array.isArray(fields) || !Array.isArray(values)) {
      return values;
    }
    return values.map((value, index) => maskField(fields[index], value));
  }

  /**
   * Deep-mask a telemetry event object (optional pre-process hook).
   */
  function maskTelemetryEvent(event) {
    return maskPayload(event);
  }

  return {
    maskField,
    maskColumn,
    maskMessage,
    maskPayload,
    maskRecord,
    maskRow,
    maskTelemetryEvent,
  };
}

module.exports = { createTransforms };