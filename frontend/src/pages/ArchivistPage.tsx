import { useState } from "react";
import { MarkdownViewer } from "../components/MarkdownViewer";
import type { ArchivistPayload } from "../types";

interface ArchivistPageProps {
  payload: ArchivistPayload;
  onInspectTrace: (event: Record<string, unknown>) => void;
}

export function ArchivistPage({ payload, onInspectTrace }: ArchivistPageProps) {
  const [tab, setTab] = useState<"codebase" | "onboarding" | "trace" | "state">("codebase");

  return (
    <div className="stack">
      <section className="panel">
        <p className="eyebrow">Archivist</p>
        <h3>Living Documentation</h3>
        <p className="muted">
          Archivist solves contextual amnesia by preserving architecture docs, onboarding context, trace evidence, and runtime state.
        </p>
        <div className="pill-row">
          <button onClick={() => setTab("codebase")} className={tab === "codebase" ? "active" : ""}>
            CODEBASE
          </button>
          <button onClick={() => setTab("onboarding")} className={tab === "onboarding" ? "active" : ""}>
            Onboarding Brief
          </button>
          <button onClick={() => setTab("trace")} className={tab === "trace" ? "active" : ""}>
            Trace Timeline
          </button>
          <button onClick={() => setTab("state")} className={tab === "state" ? "active" : ""}>
            State Summary
          </button>
        </div>
      </section>

      {tab === "codebase" ? (
        <section className="panel doc-panel">
          <MarkdownViewer markdown={payload.codebase.markdown} />
        </section>
      ) : null}

      {tab === "onboarding" ? (
        <section className="panel doc-panel">
          <MarkdownViewer markdown={payload.onboarding.markdown} />
        </section>
      ) : null}

      {tab === "trace" ? (
        <section className="panel">
          <ul className="timeline">
            {payload.trace.map((item, index) => (
              <li key={`${item.agent ?? "agent"}-${item.action ?? "action"}-${index}`}>
                <button onClick={() => onInspectTrace(item)}>
                  <strong>{String(item.agent ?? "agent")}</strong>
                  <span>{String(item.action ?? "action")}</span>
                  <small>{String(item.confidence ?? "")}</small>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {tab === "state" ? (
        <section className="panel">
          <pre>{JSON.stringify(payload.state, null, 2)}</pre>
        </section>
      ) : null}
    </div>
  );
}
