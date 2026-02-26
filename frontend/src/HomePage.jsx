import { useHomePageRuntime } from "./useHomePageRuntime";

export function HomePage() {
  useHomePageRuntime();

  return (
    <main className="page">
      <section className="card">
        <h1>MOTIS GTFS Profile Switcher</h1>
        <p className="muted">
          Switch GTFS runtime profiles without taking down the frontend.
        </p>
        <div style={{ marginBottom: 24 }}>
          <a
            href="/curation.html"
            style={{
              fontWeight: 600,
              color: "var(--accent)",
              textDecoration: "none",
            }}
          >
            &rarr; Open QA Curation Dashboard
          </a>
        </div>

        <div className="row">
          <label htmlFor="profileSelect">Profile</label>
          <select id="profileSelect"></select>
          <button id="activateBtn" type="button">
            Activate
          </button>
        </div>

        <div className="status-row">
          <span>System status</span>
          <span id="statusBadge" className="badge idle">
            idle
          </span>
        </div>
        <pre id="statusMessage" className="status-msg">
          Loading...
        </pre>
      </section>

      <section className="card">
        <h2>Route Query</h2>
        <form id="routeForm" className="route-form">
          <label htmlFor="origin">Origin</label>
          <input
            id="origin"
            name="origin"
            type="text"
            list="stationSuggestions"
            placeholder="e.g. Berlin Hbf [300099]"
            required
          />

          <label htmlFor="destination">Destination</label>
          <input
            id="destination"
            name="destination"
            type="text"
            list="stationSuggestions"
            placeholder="e.g. München Hbf [609678]"
            required
          />

          <datalist id="stationSuggestions"></datalist>

          <label htmlFor="datetime">Datetime</label>
          <input id="datetime" name="datetime" type="datetime-local" required />

          <button id="routeBtn" type="submit">
            Search Route
          </button>
        </form>

        <section id="routeSummary" className="route-summary">
          <p className="muted">No query executed yet.</p>
        </section>

        <div id="routeMapStatus" className="muted map-status">
          Map loading...
        </div>
        <section
          id="routeMap"
          className="route-map"
          aria-label="Route map"
        ></section>

        <details className="raw-section">
          <summary>Raw response</summary>
          <pre id="routeResult" className="result">
            No query executed yet.
          </pre>
        </details>
      </section>
    </main>
  );
}
