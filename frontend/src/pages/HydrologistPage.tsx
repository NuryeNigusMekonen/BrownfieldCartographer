import { GraphPanel } from "../components/GraphPanel";
import type { GraphPayload } from "../types";

interface HydrologistPageProps {
  graph: GraphPayload;
  onSelectNode: (nodeId: string) => void;
}

export function HydrologistPage({ graph, onSelectNode }: HydrologistPageProps) {
  return (
    <div className="stack">
      <section className="panel">
        <p className="eyebrow">Hydrologist</p>
        <h3>Lineage DAG</h3>
        <p className="muted">
          Hydrologist solves dependency opacity by tracing upstream/downstream lineage across datasets and transformations.
        </p>
      </section>

      <GraphPanel
        title="Dataset and Transformation Lineage"
        graphKind="lineage"
        nodes={graph.nodes}
        edges={graph.edges}
        onNodeSelect={onSelectNode}
      />
    </div>
  );
}
