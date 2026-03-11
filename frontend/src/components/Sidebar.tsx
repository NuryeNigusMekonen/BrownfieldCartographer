import type { RepoSession, WorkspaceView } from "../types";

const NAV_ITEMS: Array<{ id: WorkspaceView; label: string; badge: string }> = [
  { id: "overview", label: "Overview", badge: "O" },
  { id: "surveyor", label: "Surveyor", badge: "S" },
  { id: "hydrologist", label: "Hydrologist", badge: "H" },
  { id: "semanticist", label: "Semanticist", badge: "SE" },
  { id: "archivist", label: "Archivist", badge: "AR" },
  { id: "navigator", label: "Navigator", badge: "N" },
];

interface SidebarProps {
  sessions: RepoSession[];
  activeRepoId: string;
  view: WorkspaceView;
  onViewChange: (view: WorkspaceView) => void;
  onSessionSelect: (repoId: string) => void;
  onOpenIntake: () => void;
}

export function Sidebar({
  sessions,
  activeRepoId,
  view,
  onViewChange,
  onSessionSelect,
  onOpenIntake,
}: SidebarProps) {
  const active = sessions.find((session) => session.repo_id === activeRepoId);

  return (
    <div className="sidebar-content">
      <div className="brand">
        <p className="eyebrow">Brownfield Cartographer</p>
        <h1>Onboarding Workspace</h1>
        <p className="muted">Developer intelligence for unfamiliar repositories</p>
      </div>

      <section className="panel compact repo-switcher">
        <p className="eyebrow">Repository</p>
        <select value={activeRepoId} onChange={(event) => onSessionSelect(event.target.value)}>
          {sessions.map((session) => (
            <option value={session.repo_id} key={session.repo_id}>
              {session.repo_display_name ?? `local/${session.repo_name}`}
            </option>
          ))}
        </select>
        <p className="muted tiny">Branch: {active?.repo_branch ?? "unknown"}</p>
        <button className="primary-action" onClick={onOpenIntake}>
          Analyze New Repository
        </button>
      </section>

      <nav className="panel compact view-nav nav-panel">
        <p className="eyebrow">Workspace</p>
        {NAV_ITEMS.map((item) => (
          <button
            key={item.id}
            onClick={() => onViewChange(item.id)}
            className={item.id === view ? "active" : ""}
          >
            <span className="nav-item-badge">{item.badge}</span>
            <span className="nav-item-label">{item.label}</span>
          </button>
        ))}
      </nav>

      <section className="panel compact recent-panel">
        <p className="eyebrow">Recent Sessions</p>
        <ul className="recent-list">
          {sessions.slice(0, 8).map((session) => (
            <li key={session.repo_id}>
              <button
                className={session.repo_id === activeRepoId ? "active" : ""}
                onClick={() => onSessionSelect(session.repo_id)}
              >
                <span>{session.repo_display_name ?? `local/${session.repo_name}`}</span>
                <small>{session.last_analysis_timestamp}</small>
              </button>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
