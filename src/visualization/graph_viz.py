from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import networkx as nx
from pyvis.network import Network


def render_module_graph(input_path: Path | str, output_html: Path | str) -> dict[str, Any]:
    graph = _load_graph(input_path)
    sanitized, warnings = _preprocess_module_graph(graph)
    report = _build_report(sanitized, warnings)
    report["graph_type"] = "module"

    if sanitized.number_of_nodes() == 0:
        _write_empty_html(output_html, "No module graph data found.")
        report["labeled_nodes"] = 0
        return report

    pagerank = nx.pagerank(sanitized) if sanitized.number_of_edges() > 0 else {}
    degrees = dict(sanitized.degree())
    ranked_nodes = _rank_nodes(sanitized, pagerank)
    labeled_nodes = set(ranked_nodes[: min(18, max(8, int(math.sqrt(sanitized.number_of_nodes()))))])

    network = Network(height="900px", width="100%", directed=True, notebook=False, bgcolor="#fbfbf7")
    network.set_options(_module_options())

    for node_id, attrs in sanitized.nodes(data=True):
        score = pagerank.get(node_id, 0.0)
        degree = degrees.get(node_id, 0)
        is_dead = bool(attrs.get("is_dead_code_candidate", False))
        is_key = node_id in labeled_nodes
        color = _module_color(is_dead=is_dead, is_key=is_key)
        size = _scale(degree + (score * 500.0), lower=10, upper=42)
        font = {"size": 18 if is_key else 10, "color": "#1f2933", "face": "Georgia"}
        label = _module_label(node_id) if is_key else ""
        title = _module_tooltip(node_id, attrs, degree, score)
        mass = 2.2 if is_key else 0.8
        network.add_node(
            node_id,
            label=label,
            title=title,
            color=color,
            size=size,
            mass=mass,
            font=font,
        )

    for source, target, attrs in sanitized.edges(data=True):
        edge_type = str(attrs.get("edge_type", "IMPORTS"))
        width = 2 if source in labeled_nodes or target in labeled_nodes else 1
        color = "#adb5bd" if width == 1 else "#6c757d"
        network.add_edge(source, target, title=edge_type, arrows="to", width=width, color=color)

    report["labeled_nodes"] = len(labeled_nodes)
    report["top_nodes_by_degree"] = _top_nodes_by_degree(sanitized)
    _write_network_html(network, output_html, _legend_html("module"))
    return report


def render_lineage_graph(input_path: Path | str, output_html: Path | str) -> dict[str, Any]:
    graph = _load_graph(input_path)
    sanitized, warnings = _preprocess_lineage_graph(graph)
    report = _build_report(sanitized, warnings)
    report["graph_type"] = "lineage"

    if sanitized.number_of_nodes() == 0:
        _write_empty_html(output_html, "No lineage graph data found.")
        report["labeled_nodes"] = 0
        return report

    degrees = dict(sanitized.degree())
    betweenness = nx.betweenness_centrality(sanitized) if sanitized.number_of_nodes() <= 250 else {}
    ranked_nodes = _rank_nodes(sanitized, betweenness)
    labeled_nodes = {
        node
        for node in ranked_nodes[: min(14, max(6, int(math.sqrt(sanitized.number_of_nodes()))))]
        if degrees.get(node, 0) > 0
    }

    network = Network(height="900px", width="100%", directed=True, notebook=False, bgcolor="#fffdfa")
    network.set_options(_lineage_options(sanitized.number_of_nodes()))

    for node_id, attrs in sanitized.nodes(data=True):
        node_type = _infer_node_type(node_id, attrs)
        degree = degrees.get(node_id, 0)
        is_key = node_id in labeled_nodes or degree >= 2
        size = _scale(degree, lower=14, upper=34)
        label = _lineage_label(node_id, attrs) if is_key else ""
        title = _lineage_tooltip(node_id, attrs, degree)
        level = _lineage_level(node_type)
        network.add_node(
            node_id,
            label=label,
            title=title,
            color=_lineage_color(node_type),
            size=size,
            level=level,
            font={"size": 18 if is_key else 10, "color": "#1f2933", "face": "Georgia"},
            shape="dot",
        )

    for source, target, attrs in sanitized.edges(data=True):
        edge_type = str(attrs.get("edge_type", ""))
        width = 2 if source in labeled_nodes or target in labeled_nodes else 1
        network.add_edge(
            source,
            target,
            arrows="to",
            title=edge_type,
            width=width,
            color="#7f8c8d",
            smooth=False,
        )

    report["labeled_nodes"] = len(labeled_nodes)
    report["top_nodes_by_degree"] = _top_nodes_by_degree(sanitized)
    _write_network_html(network, output_html, _legend_html("lineage"))
    return report


def write_visualization_debug(output_path: Path | str, module_report: dict[str, Any], lineage_report: dict[str, Any]) -> None:
    warnings = list(module_report.get("warnings", [])) + list(lineage_report.get("warnings", []))
    payload = {
        "module_graph_node_count": module_report.get("node_count", 0),
        "module_graph_edge_count": module_report.get("edge_count", 0),
        "lineage_graph_node_count": lineage_report.get("node_count", 0),
        "lineage_graph_edge_count": lineage_report.get("edge_count", 0),
        "module_graph_labeled_nodes": module_report.get("labeled_nodes", 0),
        "lineage_graph_labeled_nodes": lineage_report.get("labeled_nodes", 0),
        "top_module_nodes_by_degree": module_report.get("top_nodes_by_degree", []),
        "top_lineage_nodes_by_degree": lineage_report.get("top_nodes_by_degree", []),
        "suspicious_hub_detected": bool(
            module_report.get("suspicious_hub_detected", False) or lineage_report.get("suspicious_hub_detected", False)
        ),
        "warnings": warnings,
    }
    Path(output_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_graph(path: Path | str) -> nx.DiGraph:
    input_path = Path(path)
    data = json.loads(input_path.read_text(encoding="utf-8"))
    edge_key = "edges" if "edges" in data else "links"
    return nx.node_link_graph(data, directed=True, edges=edge_key)


def _preprocess_module_graph(graph: nx.DiGraph) -> tuple[nx.DiGraph, list[str]]:
    clean = nx.DiGraph()
    warnings: list[str] = []

    for node_id, attrs in graph.nodes(data=True):
        if not node_id:
            continue
        node_type = _infer_node_type(str(node_id), attrs)
        if node_type != "module":
            continue
        node_attrs = dict(attrs)
        node_attrs["node_type"] = "module"
        clean.add_node(str(node_id), **node_attrs)

    for source, target, attrs in graph.edges(data=True):
        if not source or not target:
            continue
        if source not in clean or target not in clean:
            continue
        edge_type = str(attrs.get("edge_type", ""))
        if edge_type != "IMPORTS":
            continue
        clean.add_edge(str(source), str(target), **attrs)

    if clean.number_of_edges() == 0:
        warnings.append("Module graph contains no module-to-module import edges; visualization may be sparse.")

    return clean, warnings + _suspicious_hub_warnings(clean, "module graph")


def _preprocess_lineage_graph(graph: nx.DiGraph) -> tuple[nx.DiGraph, list[str]]:
    clean = nx.DiGraph()

    for node_id, attrs in graph.nodes(data=True):
        if not node_id:
            continue
        node_type = _infer_node_type(str(node_id), attrs)
        node_attrs = dict(attrs)
        node_attrs["node_type"] = node_type
        clean.add_node(str(node_id), **node_attrs)

    for source, target, attrs in graph.edges(data=True):
        if not source or not target:
            continue
        if source not in clean or target not in clean:
            continue
        clean.add_edge(str(source), str(target), **attrs)

    warnings = _suspicious_hub_warnings(clean, "lineage graph")
    return clean, warnings


def _infer_node_type(node_id: str, attrs: dict[str, Any]) -> str:
    node_type = str(attrs.get("node_type") or attrs.get("type") or "").strip().lower()
    if node_type:
        return node_type
    if node_id.startswith("dataset::"):
        return "dataset"
    if node_id.startswith("transform::"):
        return "transformation"
    if node_id.startswith("config::"):
        return "config"
    if node_id.startswith("pipeline::"):
        return "pipeline"
    if "::" in node_id:
        return "function"
    return "module"


def _rank_nodes(graph: nx.DiGraph, score_map: dict[str, float]) -> list[str]:
    degrees = dict(graph.degree())
    return [
        node
        for node, _ in sorted(
            graph.nodes(data=True),
            key=lambda item: (
                -(score_map.get(item[0], 0.0)),
                -(degrees.get(item[0], 0)),
                item[0],
            ),
        )
    ]


def _top_nodes_by_degree(graph: nx.DiGraph, limit: int = 10) -> list[dict[str, Any]]:
    top = sorted(graph.degree(), key=lambda item: (-item[1], item[0]))[:limit]
    return [{"id": node_id, "degree": degree} for node_id, degree in top]


def _suspicious_hub_warnings(graph: nx.DiGraph, graph_name: str) -> list[str]:
    if graph.number_of_nodes() <= 2:
        return []
    top_node, top_degree = max(graph.degree(), key=lambda item: item[1], default=("", 0))
    ratio = top_degree / max(1, graph.number_of_nodes() - 1)
    if top_degree >= 10 and ratio >= 0.6:
        return [f"Suspicious hub detected in {graph_name}: {top_node} connects to {top_degree} neighbors."]
    return []


def _build_report(graph: nx.DiGraph, warnings: list[str]) -> dict[str, Any]:
    return {
        "node_count": graph.number_of_nodes(),
        "edge_count": graph.number_of_edges(),
        "warnings": warnings,
        "suspicious_hub_detected": any("Suspicious hub detected" in warning for warning in warnings),
    }


def _module_label(node_id: str) -> str:
    return Path(node_id).name or node_id


def _module_tooltip(node_id: str, attrs: dict[str, Any], degree: int, score: float) -> str:
    return (
        f"{node_id}<br>"
        f"language: {attrs.get('language', 'unknown')}<br>"
        f"degree: {degree}<br>"
        f"pagerank: {score:.4f}<br>"
        f"complexity: {attrs.get('complexity_score', 0)}<br>"
        f"loc: {attrs.get('loc', 0)}<br>"
        f"velocity_30d: {attrs.get('change_velocity_30d', 0)}<br>"
        f"dead_code_candidate: {bool(attrs.get('is_dead_code_candidate', False))}"
    )


def _module_color(*, is_dead: bool, is_key: bool) -> str:
    if is_dead:
        return "#b0b7bf"
    if is_key:
        return "#d95f02"
    return "#9ecae1"


def _lineage_label(node_id: str, attrs: dict[str, Any]) -> str:
    if "name" in attrs and attrs["name"]:
        return str(attrs["name"])
    if node_id.startswith("dataset::"):
        return node_id.replace("dataset::", "", 1)
    if node_id.startswith("transform::"):
        return Path(node_id.replace("transform::", "", 1)).name or node_id
    return node_id


def _lineage_tooltip(node_id: str, attrs: dict[str, Any], degree: int) -> str:
    return (
        f"{node_id}<br>"
        f"type: {_infer_node_type(node_id, attrs)}<br>"
        f"degree: {degree}<br>"
        f"source_file: {attrs.get('source_file', '')}<br>"
        f"storage_type: {attrs.get('storage_type', '')}"
    )


def _lineage_color(node_type: str) -> str:
    if node_type == "dataset":
        return "#2b8cbe"
    if node_type == "transformation":
        return "#f28e2b"
    return "#9aa5b1"


def _lineage_level(node_type: str) -> int:
    if node_type == "dataset":
        return 1
    if node_type == "transformation":
        return 2
    return 3


def _scale(value: float, *, lower: int, upper: int) -> int:
    bounded = max(0.0, min(value, 50.0))
    return int(lower + (upper - lower) * (bounded / 50.0))


def _module_options() -> str:
    return """
    const options = {
      "layout": {
        "improvedLayout": true
      },
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "keyboard": true
      },
      "physics": {
        "enabled": true,
        "barnesHut": {
          "gravitationalConstant": -6000,
          "centralGravity": 0.08,
          "springLength": 170,
          "springConstant": 0.02,
          "damping": 0.2
        },
        "stabilization": {
          "enabled": true,
          "iterations": 250
        }
      },
      "edges": {
        "smooth": {
          "type": "dynamic"
        }
      }
    }
    """


def _lineage_options(node_count: int) -> str:
    physics = "true" if node_count > 80 else "false"
    return f"""
    const options = {{
      "layout": {{
        "hierarchical": {{
          "enabled": true,
          "direction": "LR",
          "sortMethod": "directed",
          "nodeSpacing": 180,
          "levelSeparation": 220,
          "treeSpacing": 260,
          "blockShifting": true,
          "edgeMinimization": true,
          "parentCentralization": true
        }}
      }},
      "interaction": {{
        "hover": true,
        "navigationButtons": true,
        "keyboard": true,
        "dragNodes": true,
        "dragView": true,
        "zoomView": true
      }},
      "physics": {{
        "enabled": {physics},
        "hierarchicalRepulsion": {{
          "nodeDistance": 180,
          "avoidOverlap": 1
        }},
        "stabilization": {{
          "enabled": true,
          "iterations": 150
        }}
      }},
      "edges": {{
        "smooth": false
      }}
    }}
    """


def _legend_html(graph_type: str) -> str:
    if graph_type == "lineage":
        items = [
            ("#2b8cbe", "Dataset"),
            ("#f28e2b", "Transformation"),
            ("#9aa5b1", "Unknown / Other"),
        ]
        title = "Lineage Graph Legend"
    else:
        items = [
            ("#d95f02", "High-centrality module"),
            ("#9ecae1", "Other module"),
            ("#b0b7bf", "Dead code candidate"),
        ]
        title = "Module Graph Legend"

    entries = "".join(
        f'<div style="display:flex;align-items:center;gap:8px;margin:6px 0;">'
        f'<span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:{color};"></span>'
        f'<span>{label}</span></div>'
        for color, label in items
    )
    return (
        '<div style="position:fixed;top:16px;right:16px;z-index:9999;'
        'background:rgba(255,255,255,0.94);border:1px solid #d0d7de;border-radius:12px;'
        'padding:14px 16px;font-family:Georgia,serif;box-shadow:0 8px 24px rgba(0,0,0,0.12);">'
        f'<div style="font-weight:700;margin-bottom:8px;">{title}</div>{entries}</div>'
    )


def _write_network_html(network: Network, output_html: Path | str, legend_html: str) -> None:
    html = network.generate_html(notebook=False)
    injected = html.replace("<body>", f"<body>{legend_html}", 1)
    Path(output_html).write_text(injected, encoding="utf-8")


def _write_empty_html(output_html: Path | str, message: str) -> None:
    html = (
        "<html><head><meta charset='utf-8'><title>Cartography Graph</title></head>"
        "<body style='font-family:Georgia,serif;background:#fbfbf7;color:#1f2933;"
        "display:flex;align-items:center;justify-content:center;height:100vh;'>"
        f"<div style='padding:24px 32px;border:1px solid #d0d7de;border-radius:12px;background:#ffffff;'>{message}</div>"
        "</body></html>"
    )
    Path(output_html).write_text(html, encoding="utf-8")
