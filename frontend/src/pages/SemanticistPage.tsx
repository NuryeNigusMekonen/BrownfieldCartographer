import type { SemanticPayload } from "../types";

interface SemanticistPageProps {
  payload: SemanticPayload;
  query: string;
  results: Array<Record<string, unknown>>;
  onQueryChange: (query: string) => void;
  onSearch: () => Promise<void>;
  onSelectModule: (modulePath: string, moduleData: Record<string, unknown>) => void;
}

export function SemanticistPage({
  payload,
  query,
  results,
  onQueryChange,
  onSearch,
  onSelectModule,
}: SemanticistPageProps) {
  const modules = (results.length ? results : payload.modules) as Array<Record<string, unknown>>;

  return (
    <div className="stack">
      <section className="panel">
        <p className="eyebrow">Semanticist</p>
        <h3>Semantic Knowledge Index</h3>
        <p className="muted">
          Semanticist solves silent debt by surfacing module purpose, domain clusters, and documentation drift signals.
        </p>
        <div className="query-row">
          <input
            value={query}
            onChange={(event) => onQueryChange(event.target.value)}
            placeholder="Search module purpose or concept"
          />
          <button className="primary-action" onClick={onSearch}>
            Search
          </button>
        </div>
      </section>

      <section className="panel">
        <p className="eyebrow">Domain Clusters</p>
        <div className="chip-row">
          {payload.domain_clusters.map((cluster) => (
            <span className="chip" key={cluster.cluster}>
              {cluster.cluster} ({cluster.count})
            </span>
          ))}
        </div>
      </section>

      <section className="panel">
        <p className="eyebrow">Module Purpose</p>
        <div className="card-grid">
          {modules.slice(0, 80).map((module) => {
            const path = String(module.path ?? "unknown");
            const purpose = String(module.purpose_statement ?? "No purpose statement available.");
            return (
              <article className="mini-card" key={path}>
                <h4>{path}</h4>
                <p>{purpose}</p>
                <button onClick={() => onSelectModule(path, module)}>Inspect</button>
              </article>
            );
          })}
        </div>
      </section>

      <section className="panel">
        <p className="eyebrow">Doc Drift Flags</p>
        {!payload.drift_flags.length ? (
          <p className="muted">No major drift flags reported.</p>
        ) : (
          <ul className="drift-list">
            {payload.drift_flags.map((flag) => (
              <li key={`${flag.path}-${flag.flag}`}>
                <strong>{flag.path}</strong>
                <span>{flag.flag}</span>
                <em>{flag.severity}</em>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
