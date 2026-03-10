from __future__ import annotations

import json
import webbrowser
from pathlib import Path

import typer
from rich import print

from src.agents.navigator import NavigatorAgent, NavigatorLangGraphAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.orchestrator import CartographyOrchestrator
from src.repo import resolve_repo_input
from src.visualization.graph_viz import render_lineage_graph, render_module_graph, write_visualization_debug

app = typer.Typer(help="Brownfield Cartographer CLI")


@app.command()
def analyze(
    repo: str = typer.Argument(".", help="Local repo path"),
    output: str = typer.Option(".cartography", "--output", "-o"),
    checkout_root: str = typer.Option("/tmp/cartographer_repos", "--checkout-root"),
    incremental: bool = typer.Option(True, "--incremental/--no-incremental"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=Path(checkout_root))
    out_dir = (repo_path / output).resolve() if not Path(output).is_absolute() else Path(output)
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
    tool: str = typer.Argument(..., help="find_implementation|trace_lineage|blast_radius|explain_module"),
    arg: str = typer.Argument(..., help="Tool argument"),
    checkout_root: str = typer.Option("/tmp/cartographer_repos", "--checkout-root"),
    direction: str = typer.Option("upstream", "--direction"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=Path(checkout_root))
    cartography = repo_path / ".cartography"
    module_graph_file = cartography / "module_graph.json"
    lineage_graph_file = cartography / "lineage_graph.json"
    if not module_graph_file.exists() or not lineage_graph_file.exists():
        raise typer.Exit(code=1)

    module_graph = _load_graph(module_graph_file)
    lineage_graph = _load_graph(lineage_graph_file)
    nav = NavigatorAgent(module_graph=module_graph, lineage_graph=lineage_graph)
    lang_nav = NavigatorLangGraphAgent(nav)
    result = lang_nav.run(tool=tool, arg=arg, direction=direction)
    if isinstance(result, dict) and "error" in result:
        print(f"Unknown tool: {tool}")
        raise typer.Exit(code=2)
    print(result)


@app.command()
def visualize(
    repo: str = typer.Argument(".", help="Local repo path or GitHub URL"),
    checkout_root: str = typer.Option("/tmp/cartographer_repos", "--checkout-root"),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser"),
) -> None:
    repo_path = resolve_repo_input(repo, checkout_root=Path(checkout_root))
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


def _load_graph(path: Path) -> KnowledgeGraph:
    return KnowledgeGraph.load(path)


if __name__ == "__main__":
    app()
