import maplibregl from "maplibre-gl/dist/maplibre-gl-csp.js";
import workerUrl from "maplibre-gl/dist/maplibre-gl-csp-worker.js?url";

maplibregl.setWorkerUrl(workerUrl);

export default maplibregl;
