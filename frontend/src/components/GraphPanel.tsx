import { useEffect, useMemo, useRef, useState } from "react";
import type { GraphEdge, GraphNode } from "../types";

interface GraphPanelProps {
  title: string;
  graphKind: "module" | "lineage";
  nodes: GraphNode[];
  edges: GraphEdge[];
  onNodeSelect: (nodeId: string) => void;
}

function nodeColor(node: GraphNode, graphKind: "module" | "lineage"): string {
  if (graphKind === "module") {
    if (node.dead_code) return "#f2b6a0";
    if (node.important) return "#6a95d8";
    return "#97a8b7";
  }
  if (node.node_type === "dataset") return "#3f9f92";
  if (node.node_type === "transformation") return "#dfad4d";
  return "#8a9bab";
}

export function GraphPanel({ title, graphKind, nodes, edges, onNodeSelect }: GraphPanelProps) {
  const mountRef = useRef<HTMLDivElement | null>(null);
  const networkRef = useRef<any>(null);
  const onNodeSelectRef = useRef(onNodeSelect);
  const [searchTerm, setSearchTerm] = useState("");
  const [showLabels, setShowLabels] = useState(true);
  const [performanceMode, setPerformanceMode] = useState(nodes.length > 260 || edges.length > 900);
  const [graphError, setGraphError] = useState("");

  useEffect(() => {
    setPerformanceMode(nodes.length > 260 || edges.length > 900);
  }, [nodes.length, edges.length]);

  useEffect(() => {
    setGraphError("");
  }, [nodes.length, edges.length, graphKind]);

  useEffect(() => {
    onNodeSelectRef.current = onNodeSelect;
  }, [onNodeSelect]);

  const visData = useMemo(() => {
    const mappedNodes = nodes.map((node) => ({
      id: node.id,
      label: showLabels ? node.label ?? node.id : "",
      title: `${node.id}`,
      value: Number(node.size ?? 12),
      color: {
        background: nodeColor(node, graphKind),
        border: "#24323f",
      },
      font: {
        color: "#182433",
        size: 11,
      },
    }));

    const mappedEdges = edges.map((edge) => ({
      id: `${edge.from}->${edge.to}`,
      from: edge.from,
      to: edge.to,
      arrows: graphKind === "lineage" ? "to" : "",
      color: { color: "rgba(63, 92, 122, 0.35)", highlight: "#2c6ecb" },
      width: 1,
    }));

    return { mappedNodes, mappedEdges };
  }, [edges, graphKind, nodes, showLabels]);

  useEffect(() => {
    const mount = mountRef.current;
    if (!mount) {
      return;
    }

    let disposed = false;

    const loadAndRender = async () => {
      try {
        const vis = await ensureVisAvailable();
        if (disposed || !mountRef.current) return;
        mountRef.current.innerHTML = "";

        const hasDataSet = typeof vis.DataSet === "function";
        const data = {
          nodes: hasDataSet ? new vis.DataSet(visData.mappedNodes) : visData.mappedNodes,
          edges: hasDataSet ? new vis.DataSet(visData.mappedEdges) : visData.mappedEdges,
        };

        const network = new vis.Network(mountRef.current, data, {
          autoResize: true,
          interaction: {
            hover: true,
            zoomView: true,
            dragView: true,
            navigationButtons: true,
            keyboard: {
              enabled: true,
              bindToWindow: false,
            },
          },
          physics: {
            enabled: true,
            stabilization: { iterations: performanceMode ? 80 : 180, fit: true },
            barnesHut: {
              gravitationalConstant: performanceMode ? -2600 : -6200,
              centralGravity: 0.12,
              springLength: performanceMode ? 120 : 90,
              damping: performanceMode ? 0.88 : 0.4,
            },
          },
          nodes: {
            shape: "dot",
          },
          edges: {
            smooth: {
              enabled: true,
              type: "dynamic",
            },
          },
          layout: {
            improvedLayout: !performanceMode,
            randomSeed: 7,
          },
        });

        network.on("click", (params: { nodes: string[] }) => {
          const selected = params.nodes?.[0];
          if (selected) {
            onNodeSelectRef.current(selected);
          }
        });

        network.once("stabilizationIterationsDone", () => {
          network.fit({ animation: true });
          if (performanceMode) {
            network.setOptions({ physics: { enabled: false } });
          }
        });

        networkRef.current = network;
        setGraphError("");
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unable to initialize graph renderer.";
        setGraphError(message);
      }
    };

    void loadAndRender();

    return () => {
      disposed = true;
      networkRef.current?.destroy();
      networkRef.current = null;
      if (mountRef.current) {
        mountRef.current.innerHTML = "";
      }
    };
  }, [graphKind, performanceMode, visData]);

  function fitGraph() {
    networkRef.current?.fit({ animation: true });
  }

  function zoomIn() {
    const network = networkRef.current;
    if (!network) return;
    const current = Number(network.getScale?.() ?? 1);
    network.moveTo({ scale: Math.min(4, current * 1.2), animation: true });
  }

  function zoomOut() {
    const network = networkRef.current;
    if (!network) return;
    const current = Number(network.getScale?.() ?? 1);
    network.moveTo({ scale: Math.max(0.08, current / 1.2), animation: true });
  }

  function resetView() {
    networkRef.current?.moveTo({
      scale: 1,
      position: { x: 0, y: 0 },
      animation: true,
    });
  }

  function searchNode() {
    const term = searchTerm.trim().toLowerCase();
    if (!term) return;
    const match = nodes.find(
      (node) => node.id.toLowerCase().includes(term) || String(node.label || "").toLowerCase().includes(term),
    );
    if (!match) return;
    networkRef.current?.selectNodes([match.id]);
    networkRef.current?.focus(match.id, { scale: 1.2, animation: true });
    onNodeSelect(match.id);
  }

  return (
    <section className="panel graph-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Interactive Graph</p>
          <h3>{title}</h3>
        </div>
        <div className="graph-controls">
          <input
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}
            placeholder="Search node"
            aria-label="Search node"
          />
          <button className="primary-action" onClick={searchNode}>
            Search
          </button>
          <button onClick={zoomIn}>Zoom +</button>
          <button onClick={zoomOut}>Zoom -</button>
          <button onClick={fitGraph}>Fit</button>
          <button onClick={resetView}>Reset</button>
          <button onClick={() => setShowLabels((value) => !value)}>{showLabels ? "Hide labels" : "Show labels"}</button>
          <button onClick={() => setPerformanceMode((value) => !value)}>
            {performanceMode ? "Performance on" : "Performance off"}
          </button>
        </div>
      </div>
      <div className="graph-meta">
        <span>{nodes.length} nodes</span>
        <span>{edges.length} edges</span>
        <span>{performanceMode ? "Performance mode enabled" : "Full detail mode"}</span>
      </div>
      {graphError ? (
        <div className="graph-canvas graph-canvas-error">
          <p>{graphError}</p>
          <p className="muted">Check `/vendor/vis-9.1.2/vis-network.min.js` is accessible, then refresh.</p>
        </div>
      ) : (
        <div ref={mountRef} className="graph-canvas" />
      )}
    </section>
  );
}

async function ensureVisAvailable(): Promise<any> {
  const existing = (window as any).vis;
  if (existing?.Network) {
    return existing;
  }

  const selector = 'script[data-vis-network-loader="true"]';
  const found = document.querySelector(selector) as HTMLScriptElement | null;
  if (found) {
    await waitForScript(found);
    const loaded = (window as any).vis;
    if (loaded?.Network) return loaded;
    throw new Error("vis-network loaded but window.vis.Network is missing.");
  }

  const script = document.createElement("script");
  script.src = "/vendor/vis-9.1.2/vis-network.min.js";
  script.async = true;
  script.setAttribute("data-vis-network-loader", "true");
  document.head.appendChild(script);
  await waitForScript(script);

  const loaded = (window as any).vis;
  if (!loaded?.Network) {
    throw new Error("vis-network script did not expose window.vis.Network.");
  }
  return loaded;
}

function waitForScript(script: HTMLScriptElement): Promise<void> {
  return new Promise((resolve, reject) => {
    if ((script as any)._loaded) {
      resolve();
      return;
    }
    script.addEventListener("load", () => {
      (script as any)._loaded = true;
      resolve();
    });
    script.addEventListener("error", () => {
      reject(new Error("Failed to load graph library from /vendor/vis-9.1.2/vis-network.min.js"));
    });
  });
}
