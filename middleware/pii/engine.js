const REDACTION_MARKERS = Object.freeze({
  AADHAAR: "[REDACTED_AADHAAR]",
  FARMER_ID: "[REDACTED_FARMER_ID]",
  OTP: "[REDACTED_OTP]",
  EMAIL: "[REDACTED_EMAIL]",
  PHONE: "[REDACTED_PHONE]",
  CARD: "[REDACTED_CARD]",
  TOKEN: "[REDACTED_TOKEN]",
});

const AADHAAR_REGEX = /(?<!\d)[2-9]\d{3}[\s-]?\d{4}[\s-]?\d{4}(?![\s-]?\d)/g;
const EMAIL_REGEX = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
const INDIAN_PHONE_REGEX = /(?<!\d)(?:\+?91[\s-]?)?[6-9]\d{9}(?!\d)/g;
const NANP_PHONE_REGEX = /(?<!\d)\d{3}[-.\s]\d{3}[-.\s]\d{4}(?!\d)/g;
const CARD_REGEX = /\b(?:\d[ -]*?){13,19}\b/g;
const JWT_REGEX = /\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9._-]{6,}\.[A-Za-z0-9._-]{6,}\b/g;
const BEARER_REGEX = /\b(Bearer\s+)([A-Za-z0-9\-._~+/]+=*)/gi;
const SECRET_KV_REGEX =
  /\b(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|secret|password)\s*([:=])\s*([^\s,;"']+)/gi;
const FARMER_ID_CONTEXT_REGEX =
  /\b((?:farmer[\s_-]*id(?:entifier)?|fid|pm\s*[-_]?kisan\s*(?:registration\s*(?:number|no|id)|beneficiary\s*(?:id|number)|id|number|no)|registration\s*(?:number|no|id)|beneficiary\s*(?:id|number))\s*(?::|=|is|as)?\s*)([A-Za-z0-9-]*\d[A-Za-z0-9-]{2,})\b/gi;
const FARMER_ID_JSON_SNIPPET_REGEX =
  /("farmer[_-]?id"\s*:\s*")([^"]+)(")/gi;
const FARMER_ID_PM_KISAN_TOKEN_REGEX =
  /(?<![A-Za-z0-9])[A-Za-z]{2}[-\s]?\d{9}(?![A-Za-z0-9])/g;
const FARMER_ID_STANDALONE_NUMERIC_FIELD_REGEX = /^\d{11}$/;
const OTP_CONTEXT_REGEX =
  /\b((?:otp|one[\s-]*time[\s-]*password|verification\s*code|auth(?:entication)?\s*code)\s*(?::|=|is|as)?\s*)(\d{4,8})\b/gi;
const OTP_STANDALONE_FIELD_REGEX = /^\d{6}$/;
const OTP_TOKEN_IN_CONVERSATION_REGEX = /(?<!\d)\d{6}(?!\d)/g;

/** Free-text fields where embedded PII is redacted. Indexed/query columns are excluded. */
const CONVERSATION_HEAVY_KEYS = new Set([
  "question",
  "questiontext",
  "answer",
  "answertext",
  "response",
  "responsetext",
  "feedback",
  "feedbacktext",
  "text",
  "content",
  "errormessage",
  "groupdetails",
  "message",
  "transcript",
]);

const TEMPORAL_KEYS = new Set([
  "timestamp",
  "timestamps",
  "ets",
  "createdat",
  "updatedat",
  "sessiontime",
  "starttime",
  "endtime",
  "firstoccurrence",
  "lastoccurrence",
]);

function normalizeKey(key) {
  if (typeof key !== "string") return "";
  return key.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function isFarmerIdKey(key) {
  const normalized = normalizeKey(key);
  if (!normalized) return false;

  return (
    normalized === "farmerid" ||
    normalized === "farmeridentifier" ||
    normalized === "farmernumber" ||
    normalized === "fid" ||
    normalized.endsWith("farmerid")
  );
}

function isAadhaarKey(key) {
  const normalized = normalizeKey(key);
  return normalized.includes("aadhaar") || normalized.includes("aadhar");
}

function isOtpKey(key) {
  const normalized = normalizeKey(key);
  return (
    normalized === "otp" ||
    normalized.endsWith("otp") ||
    normalized.includes("verificationcode") ||
    normalized.includes("onetimepassword")
  );
}

function isConversationHeavyKey(key) {
  return CONVERSATION_HEAVY_KEYS.has(normalizeKey(key));
}

function isTemporalKey(key) {
  return TEMPORAL_KEYS.has(normalizeKey(key));
}

function isLikelyTemporalValue(value) {
  if (typeof value !== "string") return false;

  const trimmed = value.trim();
  if (!trimmed) return false;

  if (/^\d{10,16}$/.test(trimmed)) return true;

  if (
    /^\d{4}-\d{2}-\d{2}(?:[tT\s].*)?$/.test(trimmed) &&
    !Number.isNaN(Date.parse(trimmed))
  ) {
    return true;
  }

  return false;
}

function shouldMaskStringByKey(key) {
  return isConversationHeavyKey(key);
}

function maskStringValue(value, options = {}) {
  if (typeof value !== "string" || value.length === 0) return value;

  const { isConversationField = false } = options;
  let masked = value;

  masked = masked.replace(
    FARMER_ID_CONTEXT_REGEX,
    (_, prefix) => `${prefix}${REDACTION_MARKERS.FARMER_ID}`,
  );
  masked = masked.replace(
    FARMER_ID_JSON_SNIPPET_REGEX,
    (_, start, __, end) => `${start}${REDACTION_MARKERS.FARMER_ID}${end}`,
  );
  masked = masked.replace(
    FARMER_ID_PM_KISAN_TOKEN_REGEX,
    REDACTION_MARKERS.FARMER_ID,
  );
  masked = masked.replace(
    OTP_CONTEXT_REGEX,
    (_, prefix) => `${prefix}${REDACTION_MARKERS.OTP}`,
  );
  masked = masked.replace(CARD_REGEX, REDACTION_MARKERS.CARD);
  masked = masked.replace(INDIAN_PHONE_REGEX, REDACTION_MARKERS.PHONE);
  masked = masked.replace(NANP_PHONE_REGEX, REDACTION_MARKERS.PHONE);
  masked = masked.replace(AADHAAR_REGEX, REDACTION_MARKERS.AADHAAR);
  masked = masked.replace(EMAIL_REGEX, REDACTION_MARKERS.EMAIL);
  masked = masked.replace(JWT_REGEX, REDACTION_MARKERS.TOKEN);
  masked = masked.replace(
    BEARER_REGEX,
    (_, prefix) => `${prefix}${REDACTION_MARKERS.TOKEN}`,
  );
  masked = masked.replace(
    SECRET_KV_REGEX,
    (_, secretName, separator) =>
      `${secretName}${separator}${REDACTION_MARKERS.TOKEN}`,
  );

  if (isConversationField) {
    masked = masked.replace(
      /\b(fid\s*[:=]?\s*)([A-Za-z0-9-]{3,})\b/gi,
      (_, prefix) => `${prefix}${REDACTION_MARKERS.FARMER_ID}`,
    );
    if (FARMER_ID_STANDALONE_NUMERIC_FIELD_REGEX.test(masked.trim())) {
      masked = REDACTION_MARKERS.FARMER_ID;
    }
    if (OTP_STANDALONE_FIELD_REGEX.test(masked.trim())) {
      masked = REDACTION_MARKERS.OTP;
    }
    masked = masked.replace(
      OTP_TOKEN_IN_CONVERSATION_REGEX,
      REDACTION_MARKERS.OTP,
    );
  }

  return masked;
}

function maskByKeyValue(key, value) {
  if (value === null || value === undefined) return value;

  if (typeof value === "string") {
    if (isTemporalKey(key) && isLikelyTemporalValue(value)) {
      return value;
    }

    if (!shouldMaskStringByKey(key)) {
      return value;
    }

    return maskStringValue(value, {
      isConversationField: isConversationHeavyKey(key),
    });
  }

  return value;
}

function maskApiResponse(payload, parentKey = null) {
  if (payload === null || payload === undefined) return payload;

  if (Array.isArray(payload)) {
    return payload.map((item) => maskApiResponse(item, parentKey));
  }

  if (typeof payload === "object") {
    const output = {};

    for (const [key, value] of Object.entries(payload)) {
      if (value === null || value === undefined) {
        output[key] = value;
        continue;
      }

      if (typeof value === "object") {
        output[key] = maskApiResponse(value, key);
        continue;
      }

      output[key] = maskByKeyValue(key, value);
    }

    return output;
  }

  if (typeof payload === "string") {
    if (isTemporalKey(parentKey) && isLikelyTemporalValue(payload)) {
      return payload;
    }

    if (!shouldMaskStringByKey(parentKey)) {
      return payload;
    }

    return maskStringValue(payload, {
      isConversationField: isConversationHeavyKey(parentKey),
    });
  }

  return payload;
}

module.exports = {
  REDACTION_MARKERS,
  isFarmerIdKey,
  isAadhaarKey,
  isOtpKey,
  isTemporalKey,
  maskStringValue,
  maskApiResponse,
  maskByKeyValue,
};
