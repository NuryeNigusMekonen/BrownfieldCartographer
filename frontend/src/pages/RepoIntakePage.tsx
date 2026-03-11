import { useMemo, useState } from "react";
import type { RepoSession } from "../types";

interface RepoIntakePageProps {
  sessions: RepoSession[];
  analyzing: boolean;
  analyzeError: string;
  onAnalyze: (repoInput: string) => Promise<void>;
  onOpenSession: (repoId: string) => void;
}

function isValidInput(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return false;
  if (trimmed.startsWith("https://github.com/")) return true;
  if (trimmed.startsWith("git@github.com:")) return true;
  return true;
}

export function RepoIntakePage({ sessions, analyzing, analyzeError, onAnalyze, onOpenSession }: RepoIntakePageProps) {
  const [repoInput, setRepoInput] = useState("");
  const valid = useMemo(() => isValidInput(repoInput), [repoInput]);

  async function submit() {
    if (!valid || analyzing) {
      return;
    }
    await onAnalyze(repoInput.trim());
  }

  return (
    <div className="intake-shell">
      <section className="panel intake-panel">
        <p className="eyebrow">Repository Intake</p>
        <h2>Start a Brownfield Analysis</h2>
        <p className="muted">
          Enter a local repository path or GitHub URL, run analysis, and open a multi-agent onboarding dashboard.
        </p>

        <label htmlFor="repo-input">Repository path or GitHub URL</label>
        <input
          id="repo-input"
          value={repoInput}
          onChange={(event) => setRepoInput(event.target.value)}
          placeholder="/path/to/repo or https://github.com/org/repo"
        />

        <div className="status-row">
          <span className={`status-badge ${valid ? "ok" : "warn"}`}>{valid ? "Input looks valid" : "Enter a repository path or URL"}</span>
          {analyzing ? <span className="status-badge">Running analysis...</span> : null}
        </div>

        {analyzeError ? <p className="error-text">{analyzeError}</p> : null}

        <button className="primary-action" onClick={submit} disabled={!valid || analyzing}>
          {analyzing ? "Analyzing Repository..." : "Analyze Repository"}
        </button>
      </section>

      <section className="panel intake-panel">
        <p className="eyebrow">Recent Repositories</p>
        <h3>Open Existing Session</h3>
        {!sessions.length ? (
          <p className="muted">No analyzed repositories yet.</p>
        ) : (
          <ul className="intake-recent-list">
            {sessions.slice(0, 12).map((session) => (
              <li key={session.repo_id}>
                <button onClick={() => onOpenSession(session.repo_id)}>
                  <strong>{session.repo_display_name ?? `local/${session.repo_name}`}</strong>
                  <span>Branch: {session.repo_branch ?? "unknown"}</span>
                  <small>{session.last_analysis_timestamp}</small>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
