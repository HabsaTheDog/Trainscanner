import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

test("runtime-config initializes missing global map config values", async () => {
  const runtimeConfigUrl = pathToFileURL(
    path.resolve("frontend/src/runtime-config.js"),
  ).href;

  delete globalThis.PROTOMAPS_API_KEY;
  delete globalThis.MAP_STYLE_URL;
  delete globalThis.SATELLITE_MAP_STYLE_URL;

  await import(`${runtimeConfigUrl}?test=${Date.now()}`);

  assert.equal(globalThis.PROTOMAPS_API_KEY, "");
  assert.equal(globalThis.MAP_STYLE_URL, "");
  assert.equal(globalThis.SATELLITE_MAP_STYLE_URL, "");
});
