import { GraphPanel } from "../components/GraphPanel";
import type { GraphPayload } from "../types";

interface SurveyorPageProps {
  graph: GraphPayload;
  onSelectNode: (nodeId: string) => void;
}

export function SurveyorPage({ graph, onSelectNode }: SurveyorPageProps) {
  return (
    <div className="stack">
      <section className="panel">
        <p className="eyebrow">Surveyor</p>
        <h3>Architecture Graph</h3>
        <p className="muted">
          Surveyor solves navigation blindness by exposing module topology, centrality, and dead-code hotspots.
        </p>
      </section>

      <GraphPanel
        title="Module Dependency Graph"
        graphKind="module"
        nodes={graph.nodes}
        edges={graph.edges}
        onNodeSelect={onSelectNode}
      />

      <section className="panel">
        <p className="eyebrow">High Centrality</p>
        <div className="chip-row">
          {(graph.hubs ?? []).map((hub) => (
            <button key={hub} onClick={() => onSelectNode(hub)}>
              {hub}
            </button>
          ))}
        </div>
      </section>
    </div>
  );
}
