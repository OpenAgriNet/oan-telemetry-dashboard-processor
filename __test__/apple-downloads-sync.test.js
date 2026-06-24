const crypto = require("crypto");
const {
  createAppleJwt,
  normalizeApplePrivateKey,
  normalizeAppleDownloadRows,
  parseAppleReport,
} = require("../appleDownloadsSync");

describe("appleDownloadsSync helpers", () => {
  test("normalizeApplePrivateKey should convert escaped newlines", () => {
    const value =
      "-----BEGIN PRIVATE KEY-----\\nline-1\\nline-2\\n-----END PRIVATE KEY-----";

    expect(normalizeApplePrivateKey(value)).toBe(
      "-----BEGIN PRIVATE KEY-----\nline-1\nline-2\n-----END PRIVATE KEY-----",
    );
  });

  test("createAppleJwt should create a three-part JWT", () => {
    const { privateKey } = crypto.generateKeyPairSync("ec", {
      namedCurve: "prime256v1",
    });
    const config = {
      issuerId: "issuer-id",
      keyId: "key-id",
      appId: "6760328735",
      privateKey: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
    };

    const token = createAppleJwt(config, Date.UTC(2026, 5, 22));
    expect(token.split(".")).toHaveLength(3);
  });

  test("parseAppleReport should parse tab-separated report content", () => {
    const buffer = Buffer.from(
      [
        "Date\tApp Version\tFirst-Time Downloads",
        "2026-06-20\t10.11.2\t12",
        "2026-06-20\t10.11.3\t15",
      ].join("\n"),
      "utf8",
    );
    const logger = {
      info: jest.fn(),
    };

    const rows = parseAppleReport(buffer, logger);
    expect(rows).toHaveLength(2);
    expect(rows[0]["App Version"]).toBe("10.11.2");
    expect(rows[1]["First-Time Downloads"]).toBe("15");
  });

  test("normalizeAppleDownloadRows should group duplicate version rows", () => {
    const rows = [
      {
        Date: "2026-06-20",
        "App Version": "10.11.2",
        "First-Time Downloads": "12",
      },
      {
        Date: "2026-06-20",
        "App Version": "10.11.2",
        "First-Time Downloads": "5",
      },
      {
        Date: "2026-06-21",
        "App Version": "10.11.3",
        "First-Time Downloads": "9",
      },
    ];
    const logger = {
      info: jest.fn(),
    };

    const normalizedRows = normalizeAppleDownloadRows({
      rows,
      logger,
      syncWindowDays: 7,
      now: new Date("2026-06-22T10:00:00Z"),
    });

    expect(normalizedRows).toEqual([
      {
        date: "2026-06-20",
        platform: "ios",
        version: "10.11.2",
        installs: 17,
      },
      {
        date: "2026-06-21",
        platform: "ios",
        version: "10.11.3",
        installs: 9,
      },
    ]);
  });
});
