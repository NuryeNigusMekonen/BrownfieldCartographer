import type { InspectorState } from "../types";
import { formatValue, toTitle } from "../utils";

interface InspectorPanelProps {
  inspector: InspectorState | null;
}

export function InspectorPanel({ inspector }: InspectorPanelProps) {
  if (!inspector) {
    return (
      <div className="inspector-sticky">
        <div className="inspector-heading">
          <p className="eyebrow">Inspector</p>
          <h3>Select a node or run a query</h3>
          <p className="muted">Details, evidence, and metadata appear here.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="inspector-sticky">
      <div className="inspector-heading">
        <p className="eyebrow">Inspector</p>
        <h3>{inspector.title}</h3>
        {inspector.subtitle ? <p className="muted">{inspector.subtitle}</p> : null}
      </div>

      {inspector.data ? (
        <section className="panel compact inspector-section">
          <p className="eyebrow">Metadata</p>
          <dl className="inspector-list">
            {Object.entries(inspector.data).map(([key, value]) => (
              <div key={key}>
                <dt>{toTitle(key)}</dt>
                <dd>{formatValue(value)}</dd>
              </div>
            ))}
          </dl>
        </section>
      ) : null}

      {inspector.evidence !== undefined ? (
        <section className="panel compact inspector-section">
          <p className="eyebrow">Evidence</p>
          <pre>{formatValue(inspector.evidence)}</pre>
        </section>
      ) : null}
    </div>
  );
}
