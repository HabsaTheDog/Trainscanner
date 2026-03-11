import assert from "node:assert/strict";
import test from "node:test";

import {
  createBoundsState,
  decodePolyline,
  legLineCoordinates,
  pickPrimaryRoute,
  updateBounds,
} from "../src/home-page-route-utils.js";

test("createBoundsState starts with empty bounds", () => {
  assert.deepEqual(createBoundsState(), {
    minLat: Infinity,
    maxLat: -Infinity,
    minLon: Infinity,
    maxLon: -Infinity,
    count: 0,
  });
});

test("updateBounds only mutates bounds for finite coordinates", () => {
  const bounds = createBoundsState();
  updateBounds(bounds, 16.37, 48.21);
  updateBounds(bounds, Number.NaN, 48.22);
  assert.deepEqual(bounds, {
    minLat: 48.21,
    maxLat: 48.21,
    minLon: 16.37,
    maxLon: 16.37,
    count: 1,
  });
});

test("decodePolyline decodes encoded coordinate sequences", () => {
  assert.deepEqual(decodePolyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@", 5), [
    [-120.2, 38.5],
    [-120.95, 40.7],
    [-126.453, 43.252],
  ]);
});

test("legLineCoordinates falls back to endpoint coordinates", () => {
  assert.deepEqual(
    legLineCoordinates({
      from: { lon: 16.37, lat: 48.21 },
      to: { lon: 13.4, lat: 52.52 },
    }),
    [
      [16.37, 48.21],
      [13.4, 52.52],
    ],
  );
});

test("pickPrimaryRoute prefers itineraries over direct routes", () => {
  assert.deepEqual(
    pickPrimaryRoute({
      itineraries: [{ id: "itinerary-1" }],
      direct: [{ id: "direct-1" }],
    }),
    { id: "itinerary-1" },
  );
  assert.deepEqual(
    pickPrimaryRoute({ itineraries: [], direct: [{ id: "direct-1" }] }),
    { id: "direct-1" },
  );
  assert.equal(pickPrimaryRoute({ itineraries: [], direct: [] }), null);
});
