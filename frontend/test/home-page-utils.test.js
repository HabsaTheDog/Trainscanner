import assert from "node:assert/strict";
import test from "node:test";

import {
  durationToText,
  fetchJson,
  formatDateTime,
  formatTime,
  mapStyleUrl,
  parseBracketId,
  pretty,
  protomapsStyleUrl,
} from "../src/home-page-utils.js";

test("parseBracketId extracts the bracketed station id", () => {
  assert.equal(parseBracketId("Berlin Hbf [8011160]"), "8011160");
  assert.equal(parseBracketId("Berlin Hbf"), "");
});

test("durationToText formats zero, minutes, and hour durations", () => {
  assert.equal(durationToText(0), "0m");
  assert.equal(durationToText(540), "9m");
  assert.equal(durationToText(5400), "1h 30m");
});

test("formatTime and formatDateTime preserve invalid values", () => {
  assert.equal(formatTime("bad-time"), "bad-time");
  assert.equal(formatDateTime("bad-date"), "bad-date");
  assert.equal(formatDateTime(""), "-");
});

test("pretty renders stable indented JSON", () => {
  assert.equal(pretty({ ok: true }), '{\n  "ok": true\n}');
});

test("mapStyleUrl prioritizes explicit style, then Protomaps, then fallback", () => {
  assert.equal(
    mapStyleUrl({ MAP_STYLE_URL: "https://tiles.example/style.json" }),
    "https://tiles.example/style.json",
  );
  assert.equal(
    protomapsStyleUrl({ PROTOMAPS_API_KEY: "abc 123" }),
    "https://api.protomaps.com/styles/v4/light/en.json?key=abc%20123",
  );
  assert.equal(
    mapStyleUrl({ PROTOMAPS_API_KEY: "abc123" }),
    "https://api.protomaps.com/styles/v4/light/en.json?key=abc123",
  );
  assert.equal(mapStyleUrl({}), "https://tiles.openfreemap.org/styles/liberty");
});

test("fetchJson returns parsed payloads and surfaces structured API failures", async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    async json() {
      return { status: "ok" };
    },
  });
  await assert.doesNotReject(fetchJson("/ok"));
  assert.deepEqual(await fetchJson("/ok"), { status: "ok" });

  globalThis.fetch = async () => ({
    ok: false,
    status: 409,
    async json() {
      return { error: "Conflict" };
    },
  });
  await assert.rejects(fetchJson("/conflict"), (error) => {
    assert.equal(error.message, "Conflict");
    assert.equal(error.status, 409);
    assert.deepEqual(error.payload, { error: "Conflict" });
    return true;
  });

  globalThis.fetch = async () => ({
    ok: true,
    status: 200,
    async json() {
      throw new Error("malformed");
    },
  });
  await assert.rejects(fetchJson("/bad-json"), (error) => {
    assert.equal(error.message, "Invalid JSON response");
    assert.equal(error.status, 200);
    assert.deepEqual(error.payload, {});
    return true;
  });

  globalThis.fetch = originalFetch;
});
