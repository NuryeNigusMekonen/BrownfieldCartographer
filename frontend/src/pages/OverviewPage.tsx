import type { SummaryPayload, WorkspaceView } from "../types";

interface OverviewPageProps {
  summary: SummaryPayload;
  onNavigate: (view: WorkspaceView) => void;
}

export function OverviewPage({ summary, onNavigate }: OverviewPageProps) {
  const displayName = summary.repository?.display_name ?? summary.repo_name;
  return (
    <div className="stack">
      <section className="panel hero">
        <p className="eyebrow">Repository Summary</p>
        <h3>{displayName}</h3>
        <p className="muted">
          Brownfield Cartographer mapped architecture, lineage, semantics, and onboarding artifacts for this repository.
        </p>
      </section>

      <section className="metric-grid">
        <article className="panel metric-card metric-modules">
          <span>Total Modules</span>
          <strong>{summary.metrics.modules}</strong>
        </article>
        <article className="panel metric-card metric-datasets">
          <span>Total Datasets</span>
          <strong>{summary.metrics.datasets}</strong>
        </article>
        <article className="panel metric-card metric-edges">
          <span>Total Edges</span>
          <strong>{summary.metrics.lineage_edges}</strong>
        </article>
        <article className="panel metric-card metric-deadcode">
          <span>Dead Code Candidates</span>
          <strong>{summary.metrics.dead_code_candidates}</strong>
        </article>
        <article className="panel metric-card metric-centrality">
          <span>High-Centrality Modules</span>
          <strong>{summary.top_modules.length}</strong>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Critical Modules</p>
            <h3>Top Centrality Preview</h3>
          </div>
          <button onClick={() => onNavigate("surveyor")}>Open Surveyor</button>
        </div>
        <div className="chip-row">
          {summary.top_modules.map((module) => (
            <span className="chip module-chip" key={module.id}>
              {module.label} ({module.centrality})
            </span>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Quick Links</p>
            <h3>Agent Workspace</h3>
          </div>
        </div>
        <div className="pill-row quick-links">
          <button onClick={() => onNavigate("surveyor")}>Architecture Graph</button>
          <button onClick={() => onNavigate("hydrologist")}>Lineage Graph</button>
          <button onClick={() => onNavigate("semanticist")}>Semantic Index</button>
          <button onClick={() => onNavigate("archivist")}>CODEBASE + Brief</button>
          <button onClick={() => onNavigate("navigator")}>Navigator Query Console</button>
        </div>
      </section>
    </div>
  );
}
