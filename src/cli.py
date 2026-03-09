from __future__ import annotations

import json
from pathlib import Path

import typer
from rich import print

from src.agents.navigator import NavigatorAgent, NavigatorLangGraphAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.orchestrator import CartographyOrchestrator
from src.repo import resolve_repo_input

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


def _load_graph(path: Path) -> KnowledgeGraph:
    return KnowledgeGraph.load(path)


if __name__ == "__main__":
    app()
