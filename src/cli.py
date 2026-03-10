from __future__ import annotations

import builtins
import json
import webbrowser
from pathlib import Path

import typer
from rich import print

from src.agents.hydrologist import HydrologistAgent
from src.agents.navigator import NavigatorAgent, NavigatorLangGraphAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.orchestrator import CartographyOrchestrator
from src.repo import DEFAULT_WORKSPACE_REPO_ROOT, resolve_repo_input
from backend.workspace_api import serve_workspace
from src.visualization.graph_viz import render_lineage_graph, render_module_graph, write_visualization_debug

app = typer.Typer(help="Brownfield Cartographer CLI")

HYDRO_TOOL_ALIASES = {
    "feeds": "what_feeds_table",
    "what_feeds_table": "what_feeds_table",
    "depends_on": "what_depends_on_output",
    "what_depends_on_output": "what_depends_on_output",
    "upstream": "upstream",
    "downstream": "downstream",
    "trace_lineage": "trace_lineage",
    "blast_radius": "blast_radius",
}

NAV_TOOL_ALIASES = {
    "find_implementation": "find_implementation",
    "explain_module": "explain_module",
}


def _resolve_output_dir(repo_path: Path, output: str) -> Path:
    output_path = Path(output)
    if output_path.is_absolute():
        return output_path
    if output_path.parent == Path("."):
        return (repo_path / output_path).resolve()
    return output_path.resolve()


def _resolve_cartography_dir(repo_path: Path, output: str) -> Path:
    cartography_dir = _resolve_output_dir(repo_path, output)
    if not cartography_dir.exists():
        raise typer.BadParameter(f"Cartography directory not found: {cartography_dir}")
    return cartography_dir


def _resolve_checkout_root(checkout_root: str | None) -> Path | None:
    if not checkout_root:
        return None
    return Path(checkout_root).expanduser().resolve()


@app.command()
def analyze(
    repo: str = typer.Argument(".", help="Local repo path"),
    output: str = typer.Option(".cartography", "--output", "-o"),
    checkout_root: str | None = typer.Option(
        None,
        "--checkout-root",
        help=f"Workspace root for normalized repos (default: {DEFAULT_WORKSPACE_REPO_ROOT})",
    ),
    incremental: bool = typer.Option(True, "--incremental/--no-incremental"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=_resolve_checkout_root(checkout_root))
    out_dir = _resolve_output_dir(repo_path, output)
    orchestrator = CartographyOrchestrator(repo_path=repo_path, out_dir=out_dir)
    if incremental:
        changed = orchestrator.changed_files_since_last_run()
        if changed:
            print(f"[yellow]Incremental hint:[/yellow] {len(changed)} changed files since last run.")
    artifacts = orchestrator.analyze(incremental=incremental)
    print("[green]Cartography analysis complete.[/green]")
    print(json.dumps(artifacts, indent=2))


@app.command()
def query(
    repo: str = typer.Argument(".", help="Local repo path"),
    tool: str = typer.Argument(
        ...,
        help="find_implementation|trace_lineage|blast_radius|explain_module|what_feeds_table|what_depends_on_output",
    ),
    arg: str = typer.Argument(..., help="Tool argument"),
    checkout_root: str | None = typer.Option(
        None,
        "--checkout-root",
        help=f"Workspace root for normalized repos (default: {DEFAULT_WORKSPACE_REPO_ROOT})",
    ),
    direction: str = typer.Option("upstream", "--direction"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=_resolve_checkout_root(checkout_root))
    cartography = repo_path / ".cartography"
    module_graph_file = cartography / "module_graph.json"
    lineage_graph_file = cartography / "lineage_graph.json"
    if not module_graph_file.exists() or not lineage_graph_file.exists():
        raise typer.Exit(code=1)

    module_graph = _load_graph(module_graph_file)
    lineage_graph = _load_graph(lineage_graph_file)
    normalized_tool = _normalize_query_tool(tool)

    try:
        result = _run_query_tool(
            normalized_tool=normalized_tool,
            arg=arg,
            direction=direction,
            module_graph=module_graph,
            lineage_graph=lineage_graph,
        )
        payload = {
            "ok": True,
            "tool": normalized_tool,
            "arg": arg,
            "result": result,
            "error": None,
        }
        builtins.print(json.dumps(payload, indent=2))
    except ValueError as exc:
        payload = {
            "ok": False,
            "tool": normalized_tool or tool,
            "arg": arg,
            "result": None,
            "error": str(exc),
        }
        builtins.print(json.dumps(payload, indent=2))
        raise typer.Exit(code=2)


@app.command()
def visualize(
    repo: str = typer.Argument(".", help="Local repo path or GitHub URL"),
    checkout_root: str | None = typer.Option(
        None,
        "--checkout-root",
        help=f"Workspace root for normalized repos (default: {DEFAULT_WORKSPACE_REPO_ROOT})",
    ),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=_resolve_checkout_root(checkout_root))
    cartography = repo_path / ".cartography"
    module_graph_file = cartography / "module_graph.json"
    lineage_graph_file = cartography / "lineage_graph.json"
    if not module_graph_file.exists() or not lineage_graph_file.exists():
        print("[red]Missing graph JSON artifacts. Run analyze first.[/red]")
        raise typer.Exit(code=1)

    module_html = cartography / "module_graph.html"
    lineage_html = cartography / "lineage_graph.html"
    debug_json = cartography / "visualization_debug.json"

    module_report = render_module_graph(module_graph_file, module_html)
    lineage_report = render_lineage_graph(lineage_graph_file, lineage_html)
    write_visualization_debug(debug_json, module_report, lineage_report)

    print("[green]Visualization generated.[/green]")
    print(
        f"Module graph: {module_report['node_count']} nodes, {module_report['edge_count']} edges, "
        f"{module_report['labeled_nodes']} labeled nodes shown."
    )
    print(
        f"Lineage graph: {lineage_report['node_count']} nodes, {lineage_report['edge_count']} edges, "
        f"{lineage_report['labeled_nodes']} labeled nodes shown."
    )
    for warning in [*module_report.get("warnings", []), *lineage_report.get("warnings", [])]:
        print(f"[yellow]Warning:[/yellow] {warning}")
    print(str(module_html))
    print(str(lineage_html))
    print(str(debug_json))

    if open_browser:
        webbrowser.open_new_tab(module_html.resolve().as_uri())
        webbrowser.open_new_tab(lineage_html.resolve().as_uri())


@app.command()
def workspace(
    repo: str = typer.Argument(".", help="Local repo path or GitHub URL"),
    output: str = typer.Option(".cartography", "--output", "-o"),
    checkout_root: str | None = typer.Option(
        None,
        "--checkout-root",
        help=f"Workspace root for normalized repos (default: {DEFAULT_WORKSPACE_REPO_ROOT})",
    ),
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8765, "--port"),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=_resolve_checkout_root(checkout_root))
    candidate = _resolve_output_dir(repo_path, output)
    cartography_dir = candidate if candidate.exists() else None
    serve_workspace(cartography_dir=cartography_dir, host=host, port=port, open_browser=open_browser)


def _load_graph(path: Path) -> KnowledgeGraph:
    return KnowledgeGraph.load(path)


def _normalize_query_tool(tool: str) -> str:
    normalized = tool.strip().lower()
    if normalized in HYDRO_TOOL_ALIASES:
        return HYDRO_TOOL_ALIASES[normalized]
    if normalized in NAV_TOOL_ALIASES:
        return NAV_TOOL_ALIASES[normalized]
    return normalized


def _run_query_tool(
    *,
    normalized_tool: str,
    arg: str,
    direction: str,
    module_graph: KnowledgeGraph,
    lineage_graph: KnowledgeGraph,
) -> object:
    hydrologist = HydrologistAgent(lineage_graph=lineage_graph)
    nav = NavigatorAgent(module_graph=module_graph, lineage_graph=lineage_graph)
    lang_nav = NavigatorLangGraphAgent(nav)

    if normalized_tool == "upstream":
        return hydrologist.get_upstream(arg)
    if normalized_tool == "downstream":
        return hydrologist.get_downstream(arg)
    if normalized_tool == "trace_lineage":
        direction = direction.strip().lower()
        if direction == "downstream":
            return hydrologist.get_downstream(arg)
        return hydrologist.get_upstream(arg)
    if normalized_tool == "what_feeds_table":
        return hydrologist.what_feeds_table(arg)
    if normalized_tool == "what_depends_on_output":
        return hydrologist.what_depends_on_output(arg)
    if normalized_tool == "blast_radius":
        hydro_result = hydrologist.blast_radius(arg)
        # Compatibility: if lineage blast radius is empty and arg is a module path, use Navigator behavior.
        if (
            isinstance(hydro_result, dict)
            and int(hydro_result.get("impact_count", 0)) == 0
            and arg in module_graph.graph
        ):
            module_impacted = nav.blast_radius(arg)
            return {
                "target": arg,
                "impacted_nodes": module_impacted,
                "impact_count": len(module_impacted),
                "evidence": [entry.get("evidence", {}) for entry in module_impacted],
            }
        return hydro_result

    result = lang_nav.run(tool=normalized_tool, arg=arg, direction=direction)
    if isinstance(result, dict) and "error" in result:
        raise ValueError(
            f"Unsupported tool '{normalized_tool}'. Supported tools: "
            "trace_lineage, what_feeds_table, what_depends_on_output, blast_radius, "
            "upstream, downstream, feeds, depends_on, find_implementation, explain_module."
        )
    return result


if __name__ == "__main__":
    app()
