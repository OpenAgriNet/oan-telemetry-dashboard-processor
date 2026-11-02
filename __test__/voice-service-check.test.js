const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

const noop = () => {};
const express = () => ({ use: noop, get: noop, post: noop });
express.json = noop;
const errors = [];
const mocks = {
  express,
  pg: { Pool: class {} },
  "node-cron": { schedule: noop },
  dotenv: { config: noop },
  "./logger": { info: noop, debug: noop, warn: noop, error: (message) => errors.push(message) },
  "./eventProcessors": {},
  "./appleDownloadsSync": {
    getAppleSyncConfigFromEnv: () => ({}), isAppleSyncConfigured: () => false,
  },
  "./googlePlayDownloadsSync": {
    getGooglePlaySyncConfigFromEnv: () => ({}), isGooglePlaySyncConfigured: () => false,
  },
  "./middleware/pii": { pii: { express: noop } },
  lodash: {},
};
const filename = resolve(__dirname, "../index.js");
const moduleStub = { exports: {} };
vm.runInNewContext(readFileSync(filename, "utf8"), {
  require: (id) => Object.hasOwn(mocks, id) ? mocks[id] : require(id),
  module: moduleStub,
  process: { env: {}, on: noop },
  console,
  Buffer,
}, { filename });
const { processBecknExtEvent } = moduleStub.exports;

function voiceEvent(outcome) {
  return {
    eid: "OE_ITEM_RESPONSE", mid: `voice-${outcome}`, ets: 1793577600000,
    channel: "BharatVistaar-voice", sid: "",
    edata: { eks: { type: "EXT_API_CALL", target: { networkApiDetails: {
      apiType: "VOICE_API", apiService: "voice_chat_completions",
      method: "POST", url: "/api/v1/chat/completions", latencyMs: 125,
      statusCode: outcome === "success" ? 200 : outcome === "rejected" ? 422 : 500,
      success: outcome === "success",
      output: { outcome, errorType: outcome === "error" ? "TimeoutError" : null },
    } } } },
  };
}

test("Voice outcomes insert once without needing Beckn IDs or creating a Beckn lifecycle", async () => {
  for (const [outcome, siblingIds] of [
    ["success", false], ["rejected", false], ["error", false], ["success", true],
  ]) {
    const calls = [];
    const client = { query: async (sql, params) => { calls.push({ sql, params }); } };
    const event = voiceEvent(outcome);
    if (siblingIds) {
      event._beckn_session_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
      event._beckn_question_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    }
    assert.equal(await processBecknExtEvent(client, event), true);
    assert.equal(calls.length, 3);
    const insert = calls[1];
    assert.match(insert.sql, /INSERT INTO ext_api_calls/);
    assert.match(insert.sql, /ON CONFLICT \(mid\) DO NOTHING/);
    assert.equal(insert.params[8], "voice_chat_completions");
    assert.equal(insert.params[13], outcome === "success");
    assert.equal(insert.params[14], outcome);
    assert.equal(insert.params[3], null);
    assert.equal(insert.params[4], null);
    assert.ok(calls.every(({ sql }) => !sql.includes("beckn_ext_events")));
  }
});

test("Voice insertion failures roll back and are not reported as processed", async () => {
  const calls = [];
  const client = { query: async (sql) => {
    calls.push(sql);
    if (sql.includes("INSERT INTO")) throw new Error("database test failure");
  } };
  assert.equal(await processBecknExtEvent(client, voiceEvent("error")), false);
  assert.equal(calls.at(-1), "ROLLBACK TO SAVEPOINT ext_api_call");
  assert.ok(errors.some((message) => message.includes("database test failure")));
});

test("Existing provider API events still update their Beckn lifecycle", async () => {
  const event = voiceEvent("success");
  event.channel = "BharatVistaar";
  event.sid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  event.edata.eks.qid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
  event.edata.eks.target.networkApiDetails.apiType = "EXT_API";
  event.edata.eks.target.networkApiDetails.apiService = "pmkisan_verify_otp";
  const calls = [];
  const client = { query: async (sql, params) => { calls.push({ sql, params }); } };
  assert.equal(await processBecknExtEvent(client, event), true);
  assert.ok(calls.some(({ sql }) => sql.includes("INSERT INTO beckn_ext_events")));
  const insert = calls.find(({ sql }) => sql.includes("INSERT INTO ext_api_calls"));
  assert.equal(insert.params[3], event.sid);
  assert.equal(insert.params[4], event.edata.eks.qid);
});
