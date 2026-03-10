import type { RepoSession, SummaryPayload } from "../types";

interface HeaderBarProps {
  session: RepoSession | undefined;
  summary: SummaryPayload | null;
  onRefresh: () => void;
  refreshPending: boolean;
}

export function HeaderBar({ session, summary, onRefresh, refreshPending }: HeaderBarProps) {
  return (
    <header className="header-bar panel compact">
      <div className="header-main">
        <p className="eyebrow">Repository Dashboard</p>
        <h2>{session?.repo_name ?? "No repository selected"}</h2>
        <p className="muted">{session?.repo_input ?? "Select or analyze a repository"}</p>
      </div>

      <div className="header-right">
        <div className="status-grid">
          <span className="status-pill">
            <strong>Last Analysis</strong>
            {summary?.last_analysis_timestamp ?? session?.last_analysis_timestamp ?? "Unknown"}
          </span>
          <span className="status-pill">
            <strong>Artifacts</strong>
            {session?.available_artifacts?.length ?? 0}
          </span>
          <span className="status-pill good">
            <strong>Status</strong>
            {summary?.artifact_status ?? "Ready"}
          </span>
        </div>
        <button className="primary-action" onClick={onRefresh} disabled={!session || refreshPending}>
          {refreshPending ? "Analyzing..." : "Analyze / Refresh"}
        </button>
      </div>
    </header>
  );
}
