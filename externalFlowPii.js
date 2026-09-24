/**
 * Redaction for data copied from raw external-flow telemetry into processed
 * tables. Raw winston_logs remain the restricted capture source of truth.
 */
const REDACTED = Object.freeze({
  PII: "[REDACTED_PII]",
  FARMER_ID: "[REDACTED_FARMER_ID]",
  TOKEN: "[REDACTED_TOKEN]",
  EMAIL: "[REDACTED_EMAIL]",
  PHONE: "[REDACTED_PHONE]",
  AADHAAR: "[REDACTED_AADHAAR]",
  PAN: "[REDACTED_PAN]",
  OTP: "[REDACTED_OTP]",
});

const DIRECT_FIELD_MARKERS = new Map([
  ["mobile", REDACTED.PHONE],
  ["mobilenumber", REDACTED.PHONE],
  ["phone", REDACTED.PHONE],
  ["phonenumber", REDACTED.PHONE],
  ["email", REDACTED.EMAIL],
  ["emailaddress", REDACTED.EMAIL],
  ["farmerid", REDACTED.FARMER_ID],
  ["farmeridentifier", REDACTED.FARMER_ID],
  ["farmernumber", REDACTED.FARMER_ID],
  ["userid", REDACTED.PII],
  ["useridentifier", REDACTED.PII],
  ["uid", REDACTED.PII],
  ["uniqueid", REDACTED.PII],
  ["aadhaar", REDACTED.AADHAAR],
  ["aadhar", REDACTED.AADHAAR],
  ["pannumber", REDACTED.PAN],
  ["pan", REDACTED.PAN],
  ["address", REDACTED.PII],
  ["fulladdress", REDACTED.PII],
  ["otp", REDACTED.OTP],
  ["password", REDACTED.TOKEN],
  ["secret", REDACTED.TOKEN],
  ["apikey", REDACTED.TOKEN],
  ["accesstoken", REDACTED.TOKEN],
  ["refreshtoken", REDACTED.TOKEN],
  ["authorization", REDACTED.TOKEN],
  ["cookie", REDACTED.TOKEN],
  ["publickey", REDACTED.TOKEN],
  ["privatekey", REDACTED.TOKEN],
  ["clientsecret", REDACTED.TOKEN],
  ["credential", REDACTED.TOKEN],
  ["accountnumber", REDACTED.PII],
  ["bankaccount", REDACTED.PII],
  ["ifsc", REDACTED.PII],
  ["cardnumber", REDACTED.PII],
]);

const EMAIL_REGEX = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
const INDIAN_PHONE_REGEX = /(?<!\d)(?:\+?91[\s-]?)?[6-9]\d{9}(?!\d)/g;
const AADHAAR_REGEX = /(?<!\d)[2-9]\d{3}[\s-]?\d{4}[\s-]?\d{4}(?![\s-]?\d)/g;
const PAN_REGEX = /\b[A-Z]{5}[0-9]{4}[A-Z]\b/gi;
const JWT_REGEX = /\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9._-]{6,}\.[A-Za-z0-9._-]{6,}\b/g;
const BEARER_REGEX = /\b(Bearer\s+)([A-Za-z0-9\-._~+/]+=*)/gi;
const SECRET_KV_REGEX =
  /\b(api[_-]?key|token|access[_-]?token|refresh[_-]?token|authorization|secret|password|cookie|public[_-]?key|private[_-]?key|client[_-]?secret)\s*([:=])\s*([^\s,;"']+)/gi;
const FARMER_ID_CONTEXT_REGEX =
  /\b((?:farmer[\s_-]*id(?:entifier)?|fid|pm\s*[-_]?kisan\s*(?:registration\s*)?(?:number|no|id)|beneficiary\s*(?:id|number)|registration\s*(?:number|no|id))\s*(?::|=|is|as)?\s*)([A-Za-z0-9-]*\d[A-Za-z0-9-]{2,})\b/gi;
const OTP_CONTEXT_REGEX =
  /\b((?:otp|one[\s-]*time[\s-]*password|verification\s*code|auth(?:entication)?\s*code)\s*(?::|=|is|as)?\s*)(\d{4,8})\b/gi;

function normalizeKey(key) {
  return String(key || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function markerForKey(key) {
  const normalized = normalizeKey(key);
  if (DIRECT_FIELD_MARKERS.has(normalized)) {
    return DIRECT_FIELD_MARKERS.get(normalized);
  }
  if (normalized.endsWith("farmerid")) return REDACTED.FARMER_ID;
  if (normalized.endsWith("token") || normalized.endsWith("apikey")) {
    return REDACTED.TOKEN;
  }
  return null;
}

function redactText(value) {
  if (typeof value !== "string" || !value) return value;

  return value
    .replace(FARMER_ID_CONTEXT_REGEX, (_, prefix) => `${prefix}${REDACTED.FARMER_ID}`)
    .replace(OTP_CONTEXT_REGEX, (_, prefix) => `${prefix}${REDACTED.OTP}`)
    .replace(SECRET_KV_REGEX, (_, key, separator) => `${key}${separator}${REDACTED.TOKEN}`)
    .replace(BEARER_REGEX, (_, prefix) => `${prefix}${REDACTED.TOKEN}`)
    .replace(JWT_REGEX, REDACTED.TOKEN)
    .replace(EMAIL_REGEX, REDACTED.EMAIL)
    .replace(INDIAN_PHONE_REGEX, REDACTED.PHONE)
    .replace(AADHAAR_REGEX, REDACTED.AADHAAR)
    .replace(PAN_REGEX, REDACTED.PAN);
}

function looksLikeJson(text) {
  const trimmed = text.trim();
  return trimmed.startsWith("{") || trimmed.startsWith("[");
}

/**
 * Deeply redacts an object without changing correlation identifiers, GPS, or
 * location fields. JSON encoded in string fields is parsed and re-serialized.
 */
function redactSensitiveData(value, key = "", seen = new WeakSet()) {
  if (value === null || value === undefined) return value;

  const directMarker = markerForKey(key);
  if (directMarker) return directMarker;

  if (typeof value === "string") {
    if (looksLikeJson(value)) {
      try {
        return JSON.stringify(redactSensitiveData(JSON.parse(value), key, seen));
      } catch (_) {
        // Treat malformed JSON as normal text below.
      }
    }
    return redactText(value);
  }

  if (Array.isArray(value)) {
    return value.map((item) => redactSensitiveData(item, key, seen));
  }

  if (typeof value === "object") {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);
    const output = {};
    for (const [childKey, childValue] of Object.entries(value)) {
      output[childKey] = redactSensitiveData(childValue, childKey, seen);
    }
    return output;
  }

  return value;
}

module.exports = {
  REDACTED,
  redactText,
  redactSensitiveData,
};
