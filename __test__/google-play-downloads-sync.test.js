const {
  getGooglePlaySyncConfigFromEnv,
  isGooglePlaySyncConfigured,
  parseGooglePlayInstallReport,
} = require("../googlePlayDownloadsSync");

describe("googlePlayDownloadsSync", () => {
  afterEach(() => {
    delete process.env.GOOGLE_PLAY_ENABLED;
    delete process.env.GOOGLE_PLAY_PACKAGE_NAME;
    delete process.env.GOOGLE_PLAY_REPORTS_BUCKET_URI;
    delete process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_EMAIL;
    delete process.env.GOOGLE_PLAY_CREDENTIALS_JSON_PRIVATE_KEY;
  });

  test("recognizes configured Google Play sync env", () => {
    process.env.GOOGLE_PLAY_ENABLED = "true";
    process.env.GOOGLE_PLAY_PACKAGE_NAME = "com.vistaar.gov.in";
    process.env.GOOGLE_PLAY_REPORTS_BUCKET_URI =
      "gs://pubsite_prod_5938472787453840127/stats/installs/";
    process.env.GOOGLE_PLAY_CREDENTIALS_JSON_CLIENT_EMAIL =
      "play-downloads-automation@bharat-vistaar-app-prod.iam.gserviceaccount.com";
    process.env.GOOGLE_PLAY_CREDENTIALS_JSON_PRIVATE_KEY =
      "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n";

    const config = getGooglePlaySyncConfigFromEnv();
    expect(isGooglePlaySyncConfigured(config)).toBe(true);
    expect(config.credentials.private_key).toContain("BEGIN PRIVATE KEY");
  });

  test("parses UTF-16 Google Play overview report and extracts Daily User Installs", () => {
    const csv =
      "Date,Package name,Daily User Installs\n" +
      "2026-06-20,com.vistaar.gov.in,8\n";
    const buffer = Buffer.from(`\uFEFF${csv}`, "utf16le");

    const rows = parseGooglePlayInstallReport({
      buffer,
      packageName: "com.vistaar.gov.in",
      requireVersion: false,
    });

    expect(rows).toEqual([
      {
        date: "2026-06-20",
        platform: "android",
        version: "unknown",
        installs: 8,
      },
    ]);
  });
});
