import { useCurationPageRuntime } from "./useCurationPageRuntime";

export function CurationPage() {
  useCurationPageRuntime();

  return (
    <div className="curation-container">
      <aside className="issue-list">
        <div className="row">
          <h2>Station Review</h2>
          <a
            href="/"
            style={{ textDecoration: "none", color: "var(--accent)" }}
          >
            Back to Home
          </a>
        </div>
        <p className="muted">
          Pick a cluster, choose Merge/Split/Group, then resolve once.
        </p>

        <div id="uiNotice" className="ui-notice" hidden></div>

        <div className="row">
          <label htmlFor="countryFilter">Country</label>
          <select id="countryFilter">
            <option value="">All</option>
            <option value="DE">DE</option>
            <option value="AT">AT</option>
            <option value="CH">CH</option>
          </select>
          <button id="refreshBtn" type="button">
            Refresh Clusters
          </button>
        </div>

        <div className="row compact-row">
          <label htmlFor="statusFilter">Status</label>
          <select id="statusFilter">
            <option value="">All</option>
            <option value="open">Open</option>
            <option value="in_review">In Review</option>
            <option value="resolved">Resolved</option>
            <option value="dismissed">Dismissed</option>
          </select>
        </div>

        <div
          className="row compact-row"
          style={{ gridTemplateColumns: "auto 1fr" }}
        >
          <label htmlFor="scopeTagFilter">Scope</label>
          <select id="scopeTagFilter">
            <option value="latest">Latest</option>
            <option value="all">All scopes</option>
          </select>
        </div>

        <p id="clusterListMeta" className="muted tiny">
          Loading cluster summary...
        </p>
        <div id="clusterList">Loading clusters...</div>
      </aside>

      <main className="map-pane">
        <div className="map-toolbar">
          <span id="curationMapStatus" className="muted map-status">
            Select a cluster to view candidates.
          </span>
          <fieldset className="map-mode-toggle" aria-label="Map mode">
            <button
              id="mapModeDefaultBtn"
              type="button"
              className="btn-secondary"
            >
              Default
            </button>
            <button
              id="mapModeSatelliteBtn"
              type="button"
              className="btn-secondary"
            >
              Satellite
            </button>
          </fieldset>
        </div>
        <section
          id="curationMap"
          className="curation-map"
          aria-label="Curation map"
        ></section>

        <section className="cluster-section decision-editor staged-editor">
          <div
            className="row compact-row"
            style={{ gridTemplateColumns: "1fr auto" }}
          >
            <h4 style={{ margin: 0 }}>Conflict Editor</h4>
            <button id="resolveConflictBtn" type="button">
              Resolve Conflict
            </button>
          </div>

          <fieldset className="tool-strip" aria-label="Conflict tools">
            <button
              id="toolMergeBtn"
              className="tool-btn active"
              data-tool="merge"
              type="button"
            >
              Merge
            </button>
            <button
              id="toolSplitBtn"
              className="tool-btn"
              data-tool="split"
              type="button"
            >
              Split
            </button>
            <button
              id="toolGroupBtn"
              className="tool-btn"
              data-tool="group"
              type="button"
            >
              Group
            </button>
          </fieldset>
          <p id="toolAvailabilitySummary" className="muted tiny">
            No candidates selected.
          </p>

          <div
            id="toolPanelMerge"
            className="edit-panel"
            data-tool-panel="merge"
          >
            <label className="muted tiny" htmlFor="editMergeRenameInput">
              Merged station display name
            </label>
            <input
              id="editMergeRenameInput"
              type="text"
              placeholder="Example: Winterthur Main Station"
            />
          </div>

          <div
            id="toolPanelSplit"
            className="edit-panel"
            data-tool-panel="split"
            hidden
          >
            <p className="muted tiny">
              Split uses current candidate selection and submits one final split
              payload.
            </p>
          </div>

          <div
            id="toolPanelGroup"
            className="edit-panel"
            data-tool-panel="group"
            hidden
          >
            <label className="muted tiny" htmlFor="groupNameInput">
              New group name
            </label>
            <div
              className="row compact-row"
              style={{ gridTemplateColumns: "1fr auto auto" }}
            >
              <input
                id="groupNameInput"
                type="text"
                placeholder="Example: Bus Terminal"
              />
              <select id="groupSectionPresetType">
                <option value="main">main</option>
                <option value="secondary">secondary</option>
                <option value="subway">subway</option>
                <option value="bus">bus</option>
                <option value="tram">tram</option>
                <option value="other">other</option>
              </select>
              <button
                id="createGroupFromSelectionBtn"
                type="button"
                className="btn-secondary"
              >
                Create Group
              </button>
            </div>

            <label className="muted tiny" htmlFor="groupTargetSelect">
              Add current selection to existing group
            </label>
            <div
              className="row compact-row"
              style={{ gridTemplateColumns: "1fr auto" }}
            >
              <select id="groupTargetSelect"></select>
              <button
                id="addSelectionToGroupBtn"
                type="button"
                className="btn-secondary"
              >
                Add Selected
              </button>
            </div>

            <div id="groupSectionList"></div>

            <label className="muted tiny" htmlFor="groupPairWalkList">
              Pairwise walk-time links
            </label>
            <div id="groupPairWalkList"></div>
          </div>

          <label className="muted tiny" htmlFor="editNoteInput">
            Decision note (optional)
          </label>
          <textarea
            id="editNoteInput"
            rows="2"
            placeholder="Why this resolution is correct"
          ></textarea>

          <div id="editImpact" className="decision-impact muted tiny"></div>

          <details className="raw-section" style={{ marginTop: 8 }}>
            <summary>Resolve payload preview</summary>
            <pre id="editPayloadPreview" className="status-msg">
              {"{}"}
            </pre>
          </details>
        </section>

        <section className="cluster-detail-pane">
          <h3 id="clusterHeader">Select a cluster</h3>
          <p id="clusterMeta" className="muted">
            Cluster details will appear here.
          </p>

          <div className="cluster-section">
            <div
              className="row compact-row"
              style={{ gridTemplateColumns: "1fr auto auto auto" }}
            >
              <h4 style={{ margin: 0 }}>Candidates</h4>
              <button
                id="candidateSelectAllBtn"
                type="button"
                className="btn-secondary"
              >
                Select All
              </button>
              <button
                id="candidateClearBtn"
                type="button"
                className="btn-secondary"
              >
                Clear
              </button>
              <button
                id="askAiBtn"
                type="button"
                className="btn-secondary"
                style={{
                  border: "1px solid var(--accent)",
                  color: "var(--accent)",
                }}
              >
                ✨ Ask AI
              </button>
            </div>
            <p id="selectionSummary" className="muted tiny">
              No candidates selected.
            </p>
            <div
              id="aiScoreResult"
              className="ui-notice"
              style={{ marginTop: "8px" }}
              hidden
            ></div>
            <div id="candidateList"></div>
          </div>

          <div className="cluster-section">
            <h4>Selected Service Context</h4>
            <p className="muted tiny">
              Incoming and outgoing service/trains for currently selected nodes.
            </p>
            <div className="service-grid">
              <div>
                <strong className="tiny">Incoming</strong>
                <div
                  id="selectedServiceIncoming"
                  className="service-list"
                ></div>
              </div>
              <div>
                <strong className="tiny">Outgoing</strong>
                <div
                  id="selectedServiceOutgoing"
                  className="service-list"
                ></div>
              </div>
            </div>
          </div>

          <details className="cluster-section">
            <summary>
              <strong>Evidence</strong>
            </summary>
            <div id="evidenceList" style={{ marginTop: 8 }}></div>
          </details>

          <details className="cluster-section">
            <summary>
              <strong>Applied Edit History</strong>
            </summary>
            <div id="decisionHistoryList" style={{ marginTop: 8 }}></div>
          </details>
        </section>
      </main>
    </div>
  );
}
