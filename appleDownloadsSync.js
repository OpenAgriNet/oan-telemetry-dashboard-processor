const crypto = require("crypto");
const http = require("http");
const https = require("https");
const zlib = require("zlib");

const APPLE_API_BASE_URL =
  process.env.APPLE_API_BASE_URL || "https://api.appstoreconnect.apple.com";
const APPLE_AUDIENCE = "appstoreconnect-v1";
const DEFAULT_SYNC_WINDOW_DAYS = 7;
const DEFAULT_CRON_SCHEDULE = "15 6 * * *";
const DEFAULT_PLATFORM = "ios";
const DEFAULT_REPORT_REQUEST_FILTER = "downloads";
const DEFAULT_REPORT_CATEGORY = "COMMERCE";
const DEFAULT_MIN_ROWS_TO_WRITE = 1;
const DEFAULT_UNKNOWN_VERSION = "unknown";
const DEFAULT_REPORT_ACCESS_TYPE = "ONGOING";
const DEFAULT_APPLE_API_MAX_ATTEMPTS = 3;
const DEFAULT_APPLE_REQUEST_TIMEOUT_MS = 15000;
const DEFAULT_APPLE_MAX_REDIRECTS = 5;
const APPLE_RETRYABLE_STATUS_CODES = new Set([429, 500, 502, 503, 504]);
const REPORT_LIST_PATH_BUILDERS = [
  (appId) => `/v1/apps/${appId}/analyticsReportRequests`,
  (appId) => `/v1/apps/${appId}/appAnalyticsReportRequests`,
];
const DIRECT_REPORT_URL_ENV_KEYS = [
  "APPLE_DOWNLOADS_REPORT_URL",
  "APPLE_REPORT_SEGMENT_URL",
];
const REPORT_NAME_KEYS = [
  "name",
  "title",
  "displayName",
  "accessType",
  "category",
  "reportType",
];
const DATE_KEYS = [
  "date",
  "day",
  "Date",
  "Day",
  "report_date",
  "reportDate",
];
const VERSION_KEYS = [
  "app_version",
  "appVersion",
  "version",
  "App Version",
  "App Version String",
  "App Version Name",
  "App Version",
];
const INSTALL_KEYS = [
  "first_time_downloads",
  "firstTimeDownloads",
  "downloads",
  "installs",
  "units",
  "counts",
  "Counts",
  "First-Time Downloads",
  "First Time Downloads",
  "Downloads",
  "Installs",
  "Units",
];
const DOWNLOAD_TYPE_KEYS = [
  "download_type",
  "downloadType",
  "type",
  "Download Type",
  "Metric",
  "metric",
];
const FIRST_TIME_DOWNLOAD_VALUES = new Set([
  "first-time download",
  "first time download",
  "first-time downloads",
  "first time downloads",
  "first_time_downloads",
]);

function getAppleSyncConfigFromEnv() {
  return {
    issuerId: (process.env.APPLE_ISSUER_ID || "").trim(),
    keyId: (process.env.APPLE_KEY_ID || "").trim(),
    appId: (process.env.APPLE_APP_ID || "").trim(),
    appResourceId:
      (process.env.APPLE_APP_RESOURCE_ID || "").trim() ||
      (process.env.APPLE_APP_ID || "").trim(),
    privateKey: normalizeApplePrivateKey(process.env.APPLE_PRIVATE_KEY || ""),
    cronSchedule:
      (process.env.APPLE_CRON_SCHEDULE || "").trim() || DEFAULT_CRON_SCHEDULE,
    syncWindowDays: normalizePositiveInteger(
      process.env.APPLE_SYNC_WINDOW_DAYS,
      DEFAULT_SYNC_WINDOW_DAYS,
    ),
    reportRequestFilter:
      (process.env.APPLE_REPORT_REQUEST_FILTER || "").trim().toLowerCase() ||
      DEFAULT_REPORT_REQUEST_FILTER,
    minRowsToWrite: normalizePositiveInteger(
      process.env.APPLE_MIN_ROWS_TO_WRITE,
      DEFAULT_MIN_ROWS_TO_WRITE,
    ),
    directReportUrl:
      DIRECT_REPORT_URL_ENV_KEYS.map((key) => (process.env[key] || "").trim()).find(
        Boolean,
      ) || "",
  };
}

function normalizeApplePrivateKey(privateKey) {
  return privateKey
    .trim()
    .replace(/^"|"$/g, "")
    .replace(/\\n/g, "\n");
}

function isAppleSyncConfigured(config = getAppleSyncConfigFromEnv()) {
  return Boolean(
    config.issuerId && config.keyId && config.appId && config.privateKey,
  );
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function createAppleJwt(config = getAppleSyncConfigFromEnv(), now = Date.now()) {
  if (!isAppleSyncConfigured(config)) {
    throw new Error("Apple App Store Connect credentials are not fully configured");
  }

  const header = {
    alg: "ES256",
    kid: config.keyId,
    typ: "JWT",
  };
  const issuedAt = Math.floor(now / 1000);
  const payload = {
    iss: config.issuerId,
    aud: APPLE_AUDIENCE,
    iat: issuedAt,
    exp: issuedAt + 20 * 60,
  };

  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const unsignedToken = `${encodedHeader}.${encodedPayload}`;
  const signature = crypto.sign("sha256", Buffer.from(unsignedToken), {
    key: config.privateKey,
    dsaEncoding: "ieee-p1363",
  });

  return `${unsignedToken}.${base64UrlEncode(signature)}`;
}

function base64UrlEncode(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function syncAppleDownloads({ pool, logger, now = new Date() }) {
  const config = getAppleSyncConfigFromEnv();

  if (!isAppleSyncConfigured(config)) {
    logger.info(
      "[APPLE_DOWNLOADS] Skipping sync because Apple credentials are not configured",
    );
    return {
      status: "skipped",
      reason: "missing_configuration",
    };
  }

  const jwt = createAppleJwt(config, now.getTime());
  const reportBytes = await downloadAppleReport({
    config,
    logger,
    jwt,
  });

  if (reportBytes?.pending) {
    return reportBytes;
  }

  const reportRows = parseAppleReport(reportBytes, logger);
  const normalizedRows = normalizeAppleDownloadRows({
    rows: reportRows,
    logger,
    syncWindowDays: config.syncWindowDays,
    now,
  });

  if (normalizedRows.length < config.minRowsToWrite) {
    logger.warn(
      `[APPLE_DOWNLOADS] Parsed ${normalizedRows.length} rows, below minimum threshold ${config.minRowsToWrite}. No DB write performed.`,
    );
    return {
      status: "skipped",
      reason: "insufficient_rows",
      parsedRows: normalizedRows.length,
    };
  }

  const upsertedRows = await upsertAppleDownloadRows({
    pool,
    rows: normalizedRows,
  });

  logger.info(
    `[APPLE_DOWNLOADS] Sync completed successfully. Parsed ${normalizedRows.length} rows and upserted ${upsertedRows} rows.`,
  );

  return {
    status: "success",
    parsedRows: normalizedRows.length,
    upsertedRows,
    windowDays: config.syncWindowDays,
  };
}

async function downloadAppleReport({ config, logger, jwt }) {
  if (config.directReportUrl) {
    logger.info("[APPLE_DOWNLOADS] Using direct report URL from environment");
    return downloadReportSegment(config.directReportUrl);
  }

  const requests = await fetchExistingReportRequests({ config, jwt, logger });

  if (!requests.length) {
    logger.warn(
      "[APPLE_DOWNLOADS] No analytics report requests were returned by Apple. Attempting to create a downloads report request.",
    );

    const createResult = await createDownloadsReportRequest({
      config,
      jwt,
      logger,
    });

    return {
      pending: true,
      status: "pending_report_generation",
      reason: "report_request_created",
      message:
        "Apple Downloads report request created. Apple may take 24-48 hours to generate the first ongoing report.",
      requestId: createResult?.data?.id || null,
    };
  }

  const matchedRequest =
    requests.find((item) =>
      collectReportNames(item)
        .join(" ")
        .toLowerCase()
        .includes(config.reportRequestFilter),
    ) || requests[0];

  logger.info(
    `[APPLE_DOWNLOADS] Using analytics report request ${matchedRequest.id}`,
  );

  const reportsPath = `/v1/analyticsReportRequests/${matchedRequest.id}/reports?filter[category]=${DEFAULT_REPORT_CATEGORY}&fields[analyticsReports]=name,category,instances`;
  const reportsResponse = await fetchAppleJson({
    jwt,
    path: reportsPath,
    logger,
    requestLabel: `reports for request ${matchedRequest.id}`,
  });
  const reports = Array.isArray(reportsResponse.data)
    ? [...reportsResponse.data]
    : [];

  if (!reports.length) {
    throw new Error(
      `No analytics reports found for request ${matchedRequest.id}`,
    );
  }

  const matchedReport =
    reports.find((item) =>
      collectReportNames(item)
        .join(" ")
        .toLowerCase()
        .includes(config.reportRequestFilter),
    ) || reports[0];

  logger.info(
    `[APPLE_DOWNLOADS] Using analytics report ${matchedReport.id}`,
  );

  const instancesLink =
    matchedReport?.relationships?.instances?.links?.related ||
    `/v1/analyticsReports/${matchedReport.id}/instances`;
  const instancesResponse = await fetchAppleJson({
    jwt,
    pathOrUrl: instancesLink,
    logger,
    requestLabel: `instances for report ${matchedReport.id}`,
  });
  const instances = Array.isArray(instancesResponse.data)
    ? [...instancesResponse.data]
    : [];

  if (!instances.length) {
    throw new Error(
      `No analytics report instances found for report ${matchedReport.id}`,
    );
  }

  instances.sort((left, right) => {
    const leftDate = getSortableDate(left);
    const rightDate = getSortableDate(right);
    return rightDate.localeCompare(leftDate);
  });

  const latestInstance = instances[0];
  logger.info(
    `[APPLE_DOWNLOADS] Using analytics report instance ${latestInstance.id}`,
  );

  const segmentsLink =
    latestInstance?.relationships?.segments?.links?.related ||
    `/v1/analyticsReportInstances/${latestInstance.id}/segments`;
  const segmentsResponse = await fetchAppleJson({
    jwt,
    pathOrUrl: segmentsLink,
    logger,
    requestLabel: `segments for instance ${latestInstance.id}`,
  });
  const segments = Array.isArray(segmentsResponse.data)
    ? [...segmentsResponse.data]
    : [];

  if (!segments.length) {
    throw new Error(
      `No analytics report segments found for instance ${latestInstance.id}`,
    );
  }

  segments.sort((left, right) => getSortableDate(right).localeCompare(getSortableDate(left)));
  const latestSegment = segments[0];
  const reportUrl =
    latestSegment?.attributes?.url ||
    latestSegment?.attributes?.downloadUrl ||
    latestSegment?.links?.related ||
    latestSegment?.links?.self;

  if (!reportUrl) {
    throw new Error(
      `No downloadable URL was found for analytics report segment ${latestSegment.id}`,
    );
  }

  logger.info(
    `[APPLE_DOWNLOADS] Downloading analytics report segment ${latestSegment.id}`,
  );
  return downloadReportSegment(reportUrl);
}

async function fetchAppleJson({
  jwt,
  path,
  pathOrUrl,
  logger,
  requestLabel,
  maxAttempts = DEFAULT_APPLE_API_MAX_ATTEMPTS,
}) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await fetchAppleResponse({
      jwt,
      path,
      pathOrUrl,
    });
    const bodyText = await response.text();

    if (response.ok) {
      try {
        return JSON.parse(bodyText);
      } catch (error) {
        throw new Error(
          `Apple API returned non-JSON content for ${response.url}: ${truncateText(bodyText, 200)}`,
        );
      }
    }

    lastError = new Error(
      `Apple API request failed (${response.status}): ${truncateText(bodyText, 400)}`,
    );

    const shouldRetry =
      APPLE_RETRYABLE_STATUS_CODES.has(response.status) && attempt < maxAttempts;
    if (!shouldRetry) {
      throw lastError;
    }

    const delayMs = attempt * 1000;
    if (logger) {
      logger.warn(
        `[APPLE_DOWNLOADS] Retrying ${requestLabel || pathOrUrl || path || "Apple API request"} after ${response.status} response (attempt ${attempt}/${maxAttempts}, waiting ${delayMs}ms)`,
      );
    }
    await sleep(delayMs);
  }

  throw lastError || new Error("Apple API request failed");
}

async function fetchAppleResponse({ jwt, path, pathOrUrl, method = "GET", body }) {
  const url = pathOrUrl
    ? toAbsoluteAppleUrl(pathOrUrl)
    : `${APPLE_API_BASE_URL}${path}`;

  const payload = body ? JSON.stringify(body) : null;

  return executeHttpRequest({
    url,
    method,
    headers: {
      Authorization: `Bearer ${jwt}`,
      Accept: "application/json",
      ...(payload ? { "Content-Type": "application/json" } : {}),
    },
    body: payload,
  });
}

async function fetchExistingReportRequests({ config, jwt, logger }) {
  const errors = [];

  for (const pathBuilder of REPORT_LIST_PATH_BUILDERS) {
    const path = pathBuilder(config.appId);
    try {
      const response = await fetchAppleJson({
        jwt,
        path,
        logger,
        requestLabel: path,
      });
      const requests = Array.isArray(response.data) ? response.data : [];
      logger.info(
        `[APPLE_DOWNLOADS] Checked ${path} and found ${requests.length} report request(s)`,
      );
      if (requests.length) {
        return requests;
      }
    } catch (error) {
      errors.push(`${path}: ${error.message}`);
    }
  }

  if (errors.length) {
    logger.warn(
      `[APPLE_DOWNLOADS] Report request lookup attempts failed: ${errors.join(" | ")}`,
    );
  }

  return [];
}

async function createDownloadsReportRequest({ config, jwt, logger }) {
  const requestBody = {
    data: {
      type: "analyticsReportRequests",
      attributes: {
        accessType: DEFAULT_REPORT_ACCESS_TYPE,
      },
      relationships: {
        app: {
          data: {
            type: "apps",
            id: config.appResourceId,
          },
        },
      },
    },
  };

  logger.info(
    `[APPLE_DOWNLOADS] Attempting to create Apple downloads report request via POST /v1/analyticsReportRequests for app resource ${config.appResourceId}`,
  );

  const response = await fetchAppleResponse({
    jwt,
    path: "/v1/analyticsReportRequests",
    method: "POST",
    body: requestBody,
  });
  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `POST /v1/analyticsReportRequests -> ${response.status}: ${truncateText(responseText, 500)}`,
    );
  }

  let parsedResponse = {};
  try {
    parsedResponse = responseText ? JSON.parse(responseText) : {};
  } catch (error) {
    parsedResponse = {
      raw: responseText,
    };
  }

  logger.info(
    "[APPLE_DOWNLOADS] Successfully created report request using POST /v1/analyticsReportRequests",
  );
  return parsedResponse;
}

function toAbsoluteAppleUrl(pathOrUrl) {
  if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) {
    return pathOrUrl;
  }
  return `${APPLE_API_BASE_URL}${pathOrUrl}`;
}

function sleep(delayMs) {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function executeHttpRequest({
  url,
  method = "GET",
  headers = {},
  body = null,
  timeoutMs = DEFAULT_APPLE_REQUEST_TIMEOUT_MS,
  redirectCount = 0,
}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const transport = parsedUrl.protocol === "http:" ? http : https;

    const request = transport.request(
      parsedUrl,
      {
        method,
        headers: {
          ...headers,
          ...(body ? { "Content-Length": Buffer.byteLength(body) } : {}),
        },
      },
      (response) => {
        const chunks = [];

        response.on("data", (chunk) => {
          chunks.push(chunk);
        });

        response.on("end", async () => {
          const responseBody = Buffer.concat(chunks);
          const statusCode = response.statusCode || 0;

          if (
            statusCode >= 300 &&
            statusCode < 400 &&
            response.headers.location
          ) {
            if (redirectCount >= DEFAULT_APPLE_MAX_REDIRECTS) {
              reject(
                new Error(
                  `Apple request exceeded redirect limit for ${parsedUrl.toString()}`,
                ),
              );
              return;
            }

            const redirectedUrl = new URL(
              response.headers.location,
              parsedUrl,
            ).toString();

            try {
              const redirectedResponse = await executeHttpRequest({
                url: redirectedUrl,
                method: "GET",
                headers,
                timeoutMs,
                redirectCount: redirectCount + 1,
              });
              resolve(redirectedResponse);
            } catch (error) {
              reject(error);
            }
            return;
          }

          resolve({
            ok: statusCode >= 200 && statusCode < 300,
            status: statusCode,
            url: parsedUrl.toString(),
            headers: response.headers,
            body: responseBody,
            text: async () => responseBody.toString("utf8"),
          });
        });
      },
    );

    request.setTimeout(timeoutMs, () => {
      request.destroy(
        new Error(
          `Request timed out after ${timeoutMs}ms for ${parsedUrl.toString()}`,
        ),
      );
    });

    request.on("error", (error) => {
      reject(error);
    });

    if (body) {
      request.write(body);
    }

    request.end();
  });
}

async function downloadReportSegment(reportUrl) {
  const response = await executeHttpRequest({
    url: reportUrl,
    method: "GET",
    headers: {
      Accept: "*/*",
    },
  });
  const bytes = response.body;

  if (!response.ok) {
    throw new Error(
      `Apple report download failed (${response.status}): ${truncateText(
        bytes.toString("utf8"),
        300,
      )}`,
    );
  }

  if (isGzipBuffer(bytes)) {
    return zlib.gunzipSync(bytes);
  }

  return bytes;
}

function isGzipBuffer(buffer) {
  return buffer.length > 2 && buffer[0] === 0x1f && buffer[1] === 0x8b;
}

function parseAppleReport(reportBytes, logger) {
  const text = reportBytes.toString("utf8").trim();

  if (!text) {
    throw new Error("Apple report download was empty");
  }

  if (text.startsWith("{") || text.startsWith("[")) {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (Array.isArray(parsed.data)) {
      return parsed.data;
    }
    throw new Error("Apple JSON report format is not supported yet");
  }

  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    throw new Error("Apple report did not contain enough rows to parse");
  }

  const delimiter = detectDelimiter(lines[0]);
  const headers = splitDelimitedLine(lines[0], delimiter);

  logger.info(
    `[APPLE_DOWNLOADS] Parsing report using delimiter "${delimiter === "\t" ? "\\t" : delimiter}" with headers: ${headers.join(", ")}`,
  );

  return lines.slice(1).map((line) => {
    const values = splitDelimitedLine(line, delimiter);
    return headers.reduce((accumulator, header, index) => {
      accumulator[header] = values[index] || "";
      return accumulator;
    }, {});
  });
}

function detectDelimiter(headerLine) {
  if (headerLine.includes("\t")) {
    return "\t";
  }
  if (headerLine.includes(",")) {
    return ",";
  }
  return "\t";
}

function splitDelimitedLine(line, delimiter) {
  if (delimiter === "\t") {
    return line.split("\t").map((value) => value.trim());
  }

  return line
    .split(/,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)/)
    .map((value) => value.trim().replace(/^"|"$/g, ""));
}

function normalizeAppleDownloadRows({ rows, logger, syncWindowDays, now }) {
  const groupedRows = new Map();
  const earliestDate = new Date(now);
  earliestDate.setDate(earliestDate.getDate() - Math.max(syncWindowDays - 1, 0));
  earliestDate.setHours(0, 0, 0, 0);

  for (const row of rows) {
    const dateValue = getFirstValue(row, DATE_KEYS);
    const versionValue = getFirstValue(row, VERSION_KEYS);
    const installsValue = getFirstValue(row, INSTALL_KEYS);
    const downloadTypeValue = getFirstValue(row, DOWNLOAD_TYPE_KEYS);

    if (!dateValue || !installsValue) {
      continue;
    }

    if (
      downloadTypeValue &&
      !FIRST_TIME_DOWNLOAD_VALUES.has(String(downloadTypeValue).trim().toLowerCase())
    ) {
      continue;
    }

    const normalizedDate = normalizeDate(dateValue);
    if (!normalizedDate) {
      continue;
    }

    const parsedDate = new Date(`${normalizedDate}T00:00:00Z`);
    if (Number.isNaN(parsedDate.getTime()) || parsedDate < earliestDate) {
      continue;
    }

    const installs = Number.parseInt(
      String(installsValue).replace(/,/g, "").trim(),
      10,
    );
    if (!Number.isFinite(installs) || installs < 0) {
      continue;
    }

    const normalizedVersion =
      String(versionValue || "").trim() || DEFAULT_UNKNOWN_VERSION;

    const groupingKey = `${normalizedDate}::${normalizedVersion}`;
    const currentValue = groupedRows.get(groupingKey) || 0;
    groupedRows.set(groupingKey, currentValue + installs);
  }

  const normalizedRows = Array.from(groupedRows.entries()).map(([key, installs]) => {
    const [date, version] = key.split("::");
    return {
      date,
      platform: DEFAULT_PLATFORM,
      version,
      installs,
    };
  });

  logger.info(
    `[APPLE_DOWNLOADS] Normalized ${normalizedRows.length} grouped rows from ${rows.length} raw rows`,
  );

  return normalizedRows;
}

function getFirstValue(source, candidateKeys) {
  for (const key of candidateKeys) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      return source[key];
    }
  }

  const normalizedEntries = Object.entries(source).map(([key, value]) => [
    key.toLowerCase().replace(/[\s_-]+/g, ""),
    value,
  ]);

  for (const key of candidateKeys) {
    const normalizedCandidate = key.toLowerCase().replace(/[\s_-]+/g, "");
    const matchedEntry = normalizedEntries.find(
      ([normalizedKey]) => normalizedKey === normalizedCandidate,
    );
    if (matchedEntry) {
      return matchedEntry[1];
    }
  }

  return "";
}

function normalizeDate(value) {
  const trimmedValue = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmedValue)) {
    return trimmedValue;
  }

  const parsedDate = new Date(trimmedValue);
  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  const year = parsedDate.getUTCFullYear();
  const month = String(parsedDate.getUTCMonth() + 1).padStart(2, "0");
  const day = String(parsedDate.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function upsertAppleDownloadRows({ pool, rows }) {
  if (!rows.length) {
    return 0;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const row of rows) {
      await client.query(
        `
          INSERT INTO public.app_download_daily_metrics (
            date,
            platform,
            version,
            installs,
            updated_at
          )
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (date, platform, version)
          DO UPDATE SET
            installs = EXCLUDED.installs,
            updated_at = NOW()
        `,
        [row.date, row.platform, row.version, row.installs],
      );
    }

    await client.query("COMMIT");
    return rows.length;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function collectReportNames(item) {
  const attributes = item?.attributes || {};
  return REPORT_NAME_KEYS.map((key) => attributes[key]).filter(Boolean);
}

function getSortableDate(item) {
  const attributes = item?.attributes || {};
  return (
    attributes.createdDate ||
    attributes.processingDate ||
    attributes.startDate ||
    attributes.endDate ||
    ""
  );
}

function truncateText(text, maxLength) {
  const trimmed = String(text || "").trim();
  if (trimmed.length <= maxLength) {
    return trimmed;
  }
  return `${trimmed.slice(0, maxLength)}...`;
}

module.exports = {
  createAppleJwt,
  getAppleSyncConfigFromEnv,
  isAppleSyncConfigured,
  normalizeApplePrivateKey,
  normalizeAppleDownloadRows,
  parseAppleReport,
  syncAppleDownloads,
};
