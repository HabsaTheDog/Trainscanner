import { useHomePageRuntime } from "./use-home-page-runtime";
import "./styles.css";

export function HomePage() {
  useHomePageRuntime();

  return (
    <main className="max-w-[920px] mx-auto my-6 px-4 grid grid-cols-1 gap-4">
      <section className="bg-surface-1 border border-border rounded-2xl p-5 shadow-[0_4px_20px_rgba(0,0,0,0.2)]">
        <h1 className="text-2xl font-bold tracking-tight m-0 mb-2 text-text-primary font-display">
          MOTIS GTFS Profile Switcher
        </h1>
        <p className="m-0 mb-4 text-text-secondary">
          Switch GTFS runtime profiles without taking down the frontend.
        </p>
        <div className="mb-6">
          <a
            href="/curation.html"
            className="font-semibold text-amber no-underline hover:text-amber-hover transition-colors"
          >
            &rarr; Open QA Curation Dashboard
          </a>
        </div>

        <div className="grid grid-cols-[auto_1fr_auto] gap-2 items-center mb-3">
          <label
            htmlFor="profileSelect"
            className="text-text-secondary text-sm font-display"
          >
            Profile
          </label>
          <select
            id="profileSelect"
            className="bg-surface-2 border border-border-strong rounded-lg px-2.5 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors"
          ></select>
          <button
            id="activateBtn"
            type="button"
            className="px-4 py-2 rounded-lg font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all shadow-[0_2px_10px_rgba(245,158,11,0.2)] cursor-pointer border-none"
          >
            Activate
          </button>
        </div>

        <div className="flex justify-between items-center mb-2">
          <span className="text-text-secondary text-sm">System status</span>
          <span
            id="statusBadge"
            className="inline-flex items-center px-3 py-1 rounded-full font-semibold text-sm bg-yellow-dim text-yellow border border-yellow/20"
          >
            idle
          </span>
        </div>
        <pre
          id="statusMessage"
          className="bg-surface-2 border border-border rounded-xl p-3 whitespace-pre-wrap break-words m-0 text-text-secondary text-sm font-display"
        >
          Loading...
        </pre>
      </section>

      <section className="bg-surface-1 border border-border rounded-2xl p-5 shadow-[0_4px_20px_rgba(0,0,0,0.2)]">
        <h2 className="text-xl font-bold tracking-tight m-0 mb-2 text-text-primary font-display">
          Route Query
        </h2>
        <form id="routeForm" className="grid grid-cols-1 gap-2">
          <label
            htmlFor="origin"
            className="text-text-secondary text-sm font-display"
          >
            Origin
          </label>
          <input
            id="origin"
            name="origin"
            type="text"
            list="stationSuggestions"
            placeholder="e.g. Berlin Hbf [300099]"
            required
            className="bg-surface-2 border border-border-strong rounded-lg px-3 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors placeholder:text-text-muted"
          />

          <label
            htmlFor="destination"
            className="text-text-secondary text-sm font-display"
          >
            Destination
          </label>
          <input
            id="destination"
            name="destination"
            type="text"
            list="stationSuggestions"
            placeholder="e.g. München Hbf [609678]"
            required
            className="bg-surface-2 border border-border-strong rounded-lg px-3 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors placeholder:text-text-muted"
          />

          <datalist id="stationSuggestions"></datalist>

          <label
            htmlFor="datetime"
            className="text-text-secondary text-sm font-display"
          >
            Datetime
          </label>
          <input
            id="datetime"
            name="datetime"
            type="datetime-local"
            required
            className="bg-surface-2 border border-border-strong rounded-lg px-3 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors"
          />

          <button
            id="routeBtn"
            type="submit"
            className="px-4 py-2.5 rounded-lg font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all shadow-[0_2px_10px_rgba(245,158,11,0.2)] cursor-pointer border-none mt-1"
          >
            Search Route
          </button>
        </form>

        <section
          id="routeSummary"
          className="mt-3 border border-border rounded-xl p-3 bg-surface-2"
        >
          <p className="text-text-muted m-0">No query executed yet.</p>
        </section>

        <div
          id="routeMapStatus"
          className="text-text-muted mt-3 mb-1.5 text-sm"
        >
          Map loading...
        </div>
        <section
          id="routeMap"
          className="w-full h-[340px] rounded-xl border border-border overflow-hidden bg-surface-3"
          aria-label="Route map"
        ></section>

        <details className="mt-3">
          <summary className="cursor-pointer text-text-muted text-sm mb-2 hover:text-text-secondary transition-colors">
            Raw response
          </summary>
          <pre
            id="routeResult"
            className="bg-surface-2 border border-border rounded-xl p-3 whitespace-pre-wrap break-words m-0 text-text-secondary text-sm font-display"
          >
            No query executed yet.
          </pre>
        </details>
      </section>
    </main>
  );
}
