const crypto = require("crypto");
const https = require("https");
const zlib = require("zlib");

const GOOGLE_OAUTH_TOKEN_URL =
  process.env.GOOGLE_PLAY_CREDENTIALS_JSON_TOKEN_URI ||
  "https://oauth2.googleapis.com/token";
const GOOGLE_STORAGE_API_BASE_URL = "https://storage.googleapis.com/storage/v1";
const DEFAULT_PLATFORM = "android";
const DEFAULT_CRON_SCHEDULE = "0 1 * * *";
const DEFAULT_SYNC_WINDOW_DAYS = 7;
const DEFAULT_UNKNOWN_VERSION = "unknown";
const STORAGE_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

const DATE_KEYS = ["Date", "date", "Day", "day"];
const PACKAGE_KEYS = ["Package name", "package name", "package_name", "packageName"];
const VERSION_KEYS = [
  "App version",
  "app version",
  "App version code",
  "app version code",
  "Version",
  "version",
];
const INSTALL_KEYS = [
  "Daily User Installs",
  "daily user installs",
  "Daily user installs",
  "Install events",
  "install events",
  "Daily Device Installs",
  "daily device installs",
];

function getGooglePlaySyncConfigFromEnv() {
  return {
    enabled: String(process.env.GOOGLE_PLAY_ENABLED || "").trim().toLowerCase() === "true",
    projectId: String(process.env.GOOGLE_PLAY_PROJECT_ID || "").trim(),
    packageName: String(process.env.GOOGLE_PLAY_PACKAGE_NAME || "").trim(),
    serviceAccountEmail: String(
      process.env.GOOGLE_PLAY_SERVICE_ACCOUNT_EMAIL ||
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_EMAIL ||
        "",
    ).trim(),
    reportsBucketUri: String(process.env.GOOGLE_PLAY_REPORTS_BUCKET_URI || "").trim(),
    cronSchedule:
      String(process.env.GOOGLE_PLAY_CRON_SCHEDULE || "").trim() ||
      DEFAULT_CRON_SCHEDULE,
    syncWindowDays: normalizePositiveInteger(
      process.env.GOOGLE_PLAY_SYNC_WINDOW_DAYS,
      DEFAULT_SYNC_WINDOW_DAYS,
    ),
    credentials: {
      type: String(process.env.GOOGLE_PLAY_CREDENTIALS_JSON_TYPE || "").trim(),
      project_id: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_PROJECT_ID || "",
      ).trim(),
      private_key_id: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_PRIVATE_KEY_ID || "",
      ).trim(),
      private_key: normalizePrivateKey(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_PRIVATE_KEY || "",
      ),
      client_email: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_EMAIL || "",
      ).trim(),
      client_id: String(process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_ID || "").trim(),
      auth_uri: String(process.env.GOOGLE_PLAY_CREDENTIALS_JSON_AUTH_URI || "").trim(),
      token_uri: String(process.env.GOOGLE_PLAY_CREDENTIALS_JSON_TOKEN_URI || "").trim(),
      auth_provider_x509_cert_url: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_AUTH_PROVIDER_X509_CERT_URL || "",
      ).trim(),
      client_x509_cert_url: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_X509_CERT_URL || "",
      ).trim(),
      universe_domain: String(
        process.env.GOOGLE_PLAY_CREDENTIALS_JSON_UNIVERSE_DOMAIN || "",
      ).trim(),
    },
  };
}

function isGooglePlaySyncConfigured(
  config = getGooglePlaySyncConfigFromEnv(),
) {
  return Boolean(
    config.enabled &&
      config.packageName &&
      config.reportsBucketUri &&
      config.credentials?.client_email &&
      config.credentials?.private_key,
  );
}

function normalizePrivateKey(privateKey) {
  return String(privateKey || "")
    .trim()
    .replace(/^"|"$/g, "")
    .replace(/\\n/g, "\n");
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

async function syncGooglePlayDownloads({ pool, logger, now = new Date() }) {
  const config = getGooglePlaySyncConfigFromEnv();

  if (!isGooglePlaySyncConfigured(config)) {
    logger.info(
      "[GOOGLE_PLAY_DOWNLOADS] Skipping sync because Google Play credentials are not configured",
    );
    return {
      status: "skipped",
      reason: "missing_configuration",
    };
  }

  const token = await fetchGoogleAccessToken(config.credentials);
  const { bucket, prefix } = parseGsUri(config.reportsBucketUri);
  const objects = await listStorageObjects({
    bucket,
    prefix,
    accessToken: token,
  });

  if (!objects.length) {
    throw new Error(
      `No Google Play report objects were found under ${config.reportsBucketUri}`,
    );
  }

  const cutoffDate = new Date(now);
  cutoffDate.setUTCDate(cutoffDate.getUTCDate() - config.syncWindowDays);
  cutoffDate.setUTCHours(0, 0, 0, 0);

  const selectedObjects = selectRelevantObjects({
    objects,
    packageName: config.packageName,
    cutoffDate,
  });

  if (!selectedObjects.versionObjects.length && !selectedObjects.overviewObjects.length) {
    throw new Error(
      `No matching Google Play install report files were found for package ${config.packageName}`,
    );
  }

  const versionRows = await downloadAndParseObjects({
    objects: selectedObjects.versionObjects,
    bucket,
    accessToken: token,
    logger,
    parser: (buffer) =>
      parseGooglePlayInstallReport({
        buffer,
        packageName: config.packageName,
        requireVersion: true,
      }),
  });

  const versionDates = new Set(versionRows.map((row) => row.date));

  const overviewRows = await downloadAndParseObjects({
    objects: selectedObjects.overviewObjects,
    bucket,
    accessToken: token,
    logger,
    parser: (buffer) =>
      parseGooglePlayInstallReport({
        buffer,
        packageName: config.packageName,
        requireVersion: false,
      }),
  });

  const normalizedRows = normalizeGooglePlayRows({
    versionRows,
    overviewRows,
    versionDates,
    cutoffDate,
    logger,
  });

  if (!normalizedRows.length) {
    return {
      status: "skipped",
      reason: "no_recent_rows",
      filesChecked: selectedObjects.versionObjects.length + selectedObjects.overviewObjects.length,
    };
  }

  const upsertedRows = await upsertGooglePlayDownloadRows({
    pool,
    rows: normalizedRows,
  });

  logger.info(
    `[GOOGLE_PLAY_DOWNLOADS] Sync completed successfully. Parsed ${normalizedRows.length} rows and upserted ${upsertedRows} rows.`,
  );

  return {
    status: "success",
    parsedRows: normalizedRows.length,
    upsertedRows,
    filesChecked:
      selectedObjects.versionObjects.length + selectedObjects.overviewObjects.length,
    versionFilesUsed: selectedObjects.versionObjects.map((item) => item.name),
    overviewFilesUsed: selectedObjects.overviewObjects.map((item) => item.name),
  };
}

async function fetchGoogleAccessToken(credentials) {
  const now = Math.floor(Date.now() / 1000);
  const header = {
    alg: "RS256",
    typ: "JWT",
  };
  const payload = {
    iss: credentials.client_email,
    scope: STORAGE_READ_SCOPE,
    aud: GOOGLE_OAUTH_TOKEN_URL,
    exp: now + 3600,
    iat: now,
  };

  const assertion = [
    base64UrlEncode(JSON.stringify(header)),
    base64UrlEncode(JSON.stringify(payload)),
  ].join(".");

  const signature = crypto.sign(
    "RSA-SHA256",
    Buffer.from(assertion),
    credentials.private_key,
  );
  const jwt = `${assertion}.${base64UrlEncode(signature)}`;

  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion: jwt,
  }).toString();

  const response = await httpRequest({
    url: GOOGLE_OAUTH_TOKEN_URL,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": Buffer.byteLength(body),
    },
    body,
  });

  const payloadJson = JSON.parse(response.body.toString("utf8"));
  if (!payloadJson.access_token) {
    throw new Error("Google OAuth token response did not include access_token");
  }

  return payloadJson.access_token;
}

function parseGsUri(uri) {
  const match = String(uri || "").trim().match(/^gs:\/\/([^/]+)\/?(.*)$/);
  if (!match) {
    throw new Error(`Invalid Google Storage URI: ${uri}`);
  }

  return {
    bucket: match[1],
    prefix: match[2] || "",
  };
}

async function listStorageObjects({ bucket, prefix, accessToken }) {
  const objects = [];
  let pageToken = "";

  do {
    const url = new URL(`${GOOGLE_STORAGE_API_BASE_URL}/b/${bucket}/o`);
    url.searchParams.set("prefix", prefix);
    if (pageToken) {
      url.searchParams.set("pageToken", pageToken);
    }

    const response = await httpRequest({
      url: url.toString(),
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    const payload = JSON.parse(response.body.toString("utf8"));
    objects.push(...(payload.items || []));
    pageToken = payload.nextPageToken || "";
  } while (pageToken);

  return objects;
}

function selectRelevantObjects({ objects, packageName, cutoffDate }) {
  const monthTokens = buildRelevantMonthTokens(cutoffDate, new Date());
  const filtered = objects.filter((item) => {
    const name = String(item.name || "").toLowerCase();
    if (!name.includes(packageName.toLowerCase())) {
      return false;
    }
    const monthToken = extractMonthToken(name);
    return !monthToken || monthTokens.has(monthToken);
  });

  const versionObjects = filtered.filter((item) => isVersionBreakdownObject(item.name));
  const overviewObjects = filtered.filter((item) => isOverviewObject(item.name));

  return {
    versionObjects: sortObjectsByName(versionObjects),
    overviewObjects: sortObjectsByName(overviewObjects),
  };
}

function buildRelevantMonthTokens(startDate, endDate) {
  const months = new Set();
  const cursor = new Date(Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), 1));
  const boundary = new Date(Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), 1));

  while (cursor <= boundary) {
    const year = cursor.getUTCFullYear();
    const month = String(cursor.getUTCMonth() + 1).padStart(2, "0");
    months.add(`${year}${month}`);
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }

  return months;
}

function extractMonthToken(name) {
  const match = String(name || "").match(/_(\d{6})_/);
  return match ? match[1] : "";
}

function isVersionBreakdownObject(name) {
  const normalized = String(name || "").toLowerCase();
  return (
    normalized.includes("app_version") ||
    normalized.includes("app-version") ||
    normalized.includes("version_breakdown") ||
    normalized.includes("version-breakdown") ||
    normalized.includes("android_version") ||
    normalized.includes("android-version")
  );
}

function isOverviewObject(name) {
  return String(name || "").toLowerCase().includes("overview");
}

function sortObjectsByName(objects) {
  return [...objects].sort((left, right) =>
    String(left.name || "").localeCompare(String(right.name || "")),
  );
}

async function downloadAndParseObjects({
  objects,
  bucket,
  accessToken,
  logger,
  parser,
}) {
  const rows = [];
  for (const object of objects) {
    logger.info(`[GOOGLE_PLAY_DOWNLOADS] Downloading report object ${object.name}`);
    // GCS JSON API expects the full object name to remain URL-encoded, including slashes.
    const objectName = encodeURIComponent(object.name);
    const url = `${GOOGLE_STORAGE_API_BASE_URL}/b/${bucket}/o/${objectName}?alt=media`;
    const response = await httpRequest({
      url,
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    rows.push(...parser(response.body));
  }
  return rows;
}

function parseGooglePlayInstallReport({
  buffer,
  packageName,
  requireVersion,
}) {
  const uncompressed = maybeGunzip(buffer);
  const content = decodeGoogleReportBuffer(uncompressed);
  const rows = parseDelimitedText(content);

  return rows
    .map((row) => normalizeGooglePlayReportRow({ row, packageName, requireVersion }))
    .filter(Boolean);
}

function maybeGunzip(buffer) {
  if (buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b) {
    return zlib.gunzipSync(buffer);
  }
  return buffer;
}

function decodeGoogleReportBuffer(buffer) {
  const utf16NullBytes = buffer.subarray(0, Math.min(buffer.length, 200)).filter((byte) => byte === 0).length;
  if (utf16NullBytes > 20) {
    return buffer.toString("utf16le").replace(/^\uFEFF/, "");
  }
  return buffer.toString("utf8").replace(/^\uFEFF/, "");
}

function parseDelimitedText(text) {
  const normalizedText = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = normalizedText
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    return [];
  }

  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = splitCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || "";
    });
    return row;
  });
}

function splitCsvLine(line) {
  const values = [];
  let current = "";
  let insideQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (char === '"') {
      if (insideQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
      continue;
    }

    if (char === "," && !insideQuotes) {
      values.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current.trim());
  return values;
}

function normalizeGooglePlayReportRow({ row, packageName, requireVersion }) {
  const rowPackage = String(getFirstValue(row, PACKAGE_KEYS) || "").trim();
  if (rowPackage && rowPackage !== packageName) {
    return null;
  }

  const date = normalizeDate(getFirstValue(row, DATE_KEYS));
  if (!date) {
    return null;
  }

  const installs = normalizeInteger(getFirstValue(row, INSTALL_KEYS));
  if (installs === null) {
    return null;
  }

  const versionRaw = String(getFirstValue(row, VERSION_KEYS) || "").trim();
  const version = versionRaw || DEFAULT_UNKNOWN_VERSION;

  if (requireVersion && version === DEFAULT_UNKNOWN_VERSION) {
    return null;
  }

  return {
    date,
    platform: DEFAULT_PLATFORM,
    version,
    installs,
  };
}

function normalizeGooglePlayRows({
  versionRows,
  overviewRows,
  versionDates,
  cutoffDate,
  logger,
}) {
  const grouped = new Map();

  for (const row of versionRows) {
    if (!isRowOnOrAfter(row.date, cutoffDate)) {
      continue;
    }
    grouped.set(`${row.date}::${row.version}`, row.installs);
  }

  for (const row of overviewRows) {
    if (!isRowOnOrAfter(row.date, cutoffDate)) {
      continue;
    }
    if (versionDates.has(row.date)) {
      continue;
    }
    grouped.set(`${row.date}::${DEFAULT_UNKNOWN_VERSION}`, row.installs);
  }

  const normalizedRows = Array.from(grouped.entries()).map(([key, installs]) => {
    const [date, version] = key.split("::");
    return {
      date,
      platform: DEFAULT_PLATFORM,
      version,
      installs,
    };
  });

  logger.info(
    `[GOOGLE_PLAY_DOWNLOADS] Normalized ${normalizedRows.length} grouped rows from ${versionRows.length + overviewRows.length} raw rows`,
  );

  return normalizedRows.sort((left, right) =>
    `${left.date}::${left.version}`.localeCompare(`${right.date}::${right.version}`),
  );
}

function isRowOnOrAfter(date, cutoffDate) {
  const rowDate = new Date(`${date}T00:00:00.000Z`);
  return rowDate >= cutoffDate;
}

function normalizeInteger(value) {
  const trimmed = String(value || "").replace(/,/g, "").trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
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
    const normalizedKey = key.toLowerCase().replace(/[\s_-]+/g, "");
    const match = normalizedEntries.find(([entryKey]) => entryKey === normalizedKey);
    if (match) {
      return match[1];
    }
  }

  return "";
}

function normalizeDate(value) {
  const trimmedValue = String(value || "").trim();
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

async function upsertGooglePlayDownloadRows({ pool, rows }) {
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

function base64UrlEncode(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function httpRequest({ url, method, headers = {}, body = "" }) {
  const targetUrl = new URL(url);

  return new Promise((resolve, reject) => {
    const request = https.request(
      {
        method,
        protocol: targetUrl.protocol,
        hostname: targetUrl.hostname,
        path: `${targetUrl.pathname}${targetUrl.search}`,
        headers,
      },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          const responseBody = Buffer.concat(chunks);
          const statusCode = response.statusCode || 500;
          if (statusCode >= 400) {
            return reject(
              new Error(
                `Google API request failed (${statusCode}): ${responseBody.toString("utf8")}`,
              ),
            );
          }
          return resolve({
            statusCode,
            headers: response.headers,
            body: responseBody,
          });
        });
      },
    );

    request.on("error", reject);
    if (body) {
      request.write(body);
    }
    request.end();
  });
}

module.exports = {
  getGooglePlaySyncConfigFromEnv,
  isGooglePlaySyncConfigured,
  parseGooglePlayInstallReport,
  syncGooglePlayDownloads,
};
