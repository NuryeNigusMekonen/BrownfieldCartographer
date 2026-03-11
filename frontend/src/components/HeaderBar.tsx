import type { RepoSession, SummaryPayload } from "../types";

interface HeaderBarProps {
  session: RepoSession | undefined;
  summary: SummaryPayload | null;
  onRefresh: () => void;
  refreshPending: boolean;
}

export function HeaderBar({ session, summary, onRefresh, refreshPending }: HeaderBarProps) {
  const repository = summary?.repository;
  const displayName =
    repository?.display_name ?? session?.repo_display_name ?? (session ? `local/${session.repo_name}` : "No repository selected");
  const branch = repository?.branch ?? session?.repo_branch ?? "unknown";
  const lastAnalysis = summary?.last_analysis_timestamp ?? session?.last_analysis_timestamp ?? "Unknown";
  const artifactCount = summary?.artifact_count ?? summary?.artifacts?.length ?? session?.available_artifacts?.length ?? 0;
  const statusLabel = summary?.artifact_status ?? "Loaded";

  return (
    <header className="header-bar panel compact">
      <div className="header-main">
        <p className="eyebrow">Repository Dashboard</p>
        <h2 className="repo-display-name">{displayName}</h2>
        <div className="status-grid">
          <span className="status-pill">
            <strong>Repository</strong>
            {displayName}
          </span>
          <span className="status-pill">
            <strong>Branch</strong>
            {branch}
          </span>
          <span className="status-pill">
            <strong>Last Analysis</strong>
            {lastAnalysis}
          </span>
          <span className="status-pill">
            <strong>Artifacts</strong>
            {artifactCount}
          </span>
          <span className="status-pill good">
            <strong>Status</strong>
            {statusLabel}
          </span>
        </div>
      </div>

      <div className="header-right">
        <button className="primary-action" onClick={onRefresh} disabled={!session || refreshPending}>
          {refreshPending ? "Analyzing..." : "Analyze / Refresh"}
        </button>
      </div>
    </header>
  );
}
