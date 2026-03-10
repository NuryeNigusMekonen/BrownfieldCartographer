import { QueryInput } from "../components/QueryInput";
import type { QueryPayload } from "../types";
import { formatValue, parseNavigatorQuery } from "../utils";

interface QueryHistoryItem {
  query: string;
  ok: boolean;
  timestamp: string;
}

interface NavigatorPageProps {
  running: boolean;
  latest: QueryPayload | null;
  history: QueryHistoryItem[];
  examples: string[];
  onRun: (query: string) => Promise<void>;
  onInspect: (title: string, payload: Record<string, unknown>) => void;
  onUseHistory: (query: string) => Promise<void>;
}

function normalizeResults(payload: QueryPayload | null): Array<Record<string, unknown>> {
  if (!payload?.result) return [];
  if (Array.isArray(payload.result)) {
    return payload.result
      .map((item, index) => {
        if (typeof item === "object" && item !== null) return item as Record<string, unknown>;
        return { index, value: item };
      })
      .slice(0, 50);
  }
  if (typeof payload.result === "object") {
    return [payload.result as Record<string, unknown>];
  }
  return [{ value: payload.result }];
}

export function NavigatorPage({
  running,
  latest,
  history,
  examples,
  onRun,
  onInspect,
  onUseHistory,
}: NavigatorPageProps) {
  const parsed = latest ? parseNavigatorQuery(latest.query) : { tool: "", arg: "", direction: "upstream" };
  const items = normalizeResults(latest);

  return (
    <div className="stack">
      <section className="panel">
        <p className="eyebrow">Navigator</p>
        <h3>Repository Query Engine</h3>
        <p className="muted">
          Supports exact syntax and friendly text: explain_module, blast_radius, find_implementation, trace_lineage.
        </p>
      </section>

      <QueryInput disabled={running} examples={examples} onSubmit={onRun} />

      <section className="panel">
        <p className="eyebrow">Parsed Query</p>
        <p className="muted">
          Tool: <strong>{latest?.tool || parsed.tool || "-"}</strong> | Argument: <strong>{latest?.arg || parsed.arg || "-"}</strong>
        </p>
        {latest?.error ? <p className="error-text">{latest.error}</p> : null}
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Results</p>
            <h3>Structured Output</h3>
          </div>
        </div>

        {!latest ? (
          <p className="muted">Run a query to inspect implementation, blast radius, or lineage evidence.</p>
        ) : items.length === 0 ? (
          <p className="muted">No result entries returned.</p>
        ) : (
          <div className="card-grid">
            {items.map((item, index) => (
              <article className="mini-card" key={`result-${index}`}>
                <h4>{String(item.module ?? item.path ?? item.dataset ?? item.node ?? `Result ${index + 1}`)}</h4>
                <p>{formatValue(item.reason ?? item.summary ?? item.value ?? item)}</p>
                <button onClick={() => onInspect(`Query Result ${index + 1}`, item)}>Inspect Evidence</button>
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <p className="eyebrow">Query History</p>
        {history.length === 0 ? (
          <p className="muted">No previous queries for this repository.</p>
        ) : (
          <ul className="history-list">
            {history.map((item) => (
              <li key={`${item.timestamp}-${item.query}`}>
                <button onClick={() => onUseHistory(item.query)}>
                  <strong>{item.query}</strong>
                  <span>{item.ok ? "success" : "failed"}</span>
                  <small>{item.timestamp}</small>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
