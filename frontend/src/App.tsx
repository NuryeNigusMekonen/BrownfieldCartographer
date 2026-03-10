import { useEffect, useMemo, useState } from "react";
import {
  analyzeRepository,
  fetchArchivist,
  fetchLineageGraph,
  fetchModuleGraph,
  fetchNodeDetails,
  fetchSemanticist,
  fetchSummary,
  listSessions,
  runNavigatorQuery,
  selectSession,
  semanticSearch,
} from "./api";
import { AppShell } from "./components/AppShell";
import { ArtifactLoader } from "./components/ArtifactLoader";
import { HeaderBar } from "./components/HeaderBar";
import { InspectorPanel } from "./components/InspectorPanel";
import { Sidebar } from "./components/Sidebar";
import { ArchivistPage } from "./pages/ArchivistPage";
import { HydrologistPage } from "./pages/HydrologistPage";
import { NavigatorPage } from "./pages/NavigatorPage";
import { OverviewPage } from "./pages/OverviewPage";
import { RepoIntakePage } from "./pages/RepoIntakePage";
import { SemanticistPage } from "./pages/SemanticistPage";
import { SurveyorPage } from "./pages/SurveyorPage";
import type {
  ArchivistPayload,
  GraphPayload,
  InspectorState,
  QueryPayload,
  RepoSession,
  SemanticPayload,
  SummaryPayload,
  WorkspaceView,
} from "./types";
import { toToolQuery } from "./utils";

interface QueryHistoryItem {
  query: string;
  ok: boolean;
  timestamp: string;
}

function historyKey(repoId: string): string {
  return `cartographer-history:${repoId}`;
}

function loadQueryHistory(repoId: string): QueryHistoryItem[] {
  try {
    const raw = localStorage.getItem(historyKey(repoId));
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((item) => item && typeof item.query === "string").slice(0, 20);
  } catch {
    return [];
  }
}

function saveQueryHistory(repoId: string, items: QueryHistoryItem[]): void {
  localStorage.setItem(historyKey(repoId), JSON.stringify(items.slice(0, 20)));
}

export default function App() {
  const [sessions, setSessions] = useState<RepoSession[]>([]);
  const [activeRepoId, setActiveRepoId] = useState("");
  const [view, setView] = useState<WorkspaceView>("overview");
  const [showIntake, setShowIntake] = useState(false);

  const [summary, setSummary] = useState<SummaryPayload | null>(null);
  const [moduleGraph, setModuleGraph] = useState<GraphPayload | null>(null);
  const [lineageGraph, setLineageGraph] = useState<GraphPayload | null>(null);
  const [semanticist, setSemanticist] = useState<SemanticPayload | null>(null);
  const [archivist, setArchivist] = useState<ArchivistPayload | null>(null);

  const [loading, setLoading] = useState(false);
  const [loadingError, setLoadingError] = useState("");

  const [analyzing, setAnalyzing] = useState(false);
  const [analyzeError, setAnalyzeError] = useState("");

  const [inspector, setInspector] = useState<InspectorState | null>(null);

  const [semanticQuery, setSemanticQuery] = useState("");
  const [semanticResults, setSemanticResults] = useState<Array<Record<string, unknown>>>([]);

  const [latestQuery, setLatestQuery] = useState<QueryPayload | null>(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryItem[]>([]);

  const activeSession = sessions.find((session) => session.repo_id === activeRepoId);

  useEffect(() => {
    void bootstrap();
  }, []);

  useEffect(() => {
    if (!activeRepoId) return;
    setQueryHistory(loadQueryHistory(activeRepoId));
  }, [activeRepoId]);

  useEffect(() => {
    if (!activeRepoId) return;

    if (view === "surveyor" && !moduleGraph) {
      void loadModuleGraph(activeRepoId);
    }
    if (view === "hydrologist" && !lineageGraph) {
      void loadLineageGraph(activeRepoId);
    }
    if ((view === "semanticist" || view === "navigator") && !semanticist) {
      void loadSemanticist(activeRepoId);
    }
    if (view === "archivist" && !archivist) {
      void loadArchivist(activeRepoId);
    }
  }, [activeRepoId, archivist, lineageGraph, moduleGraph, semanticist, view]);

  const queryExamples = useMemo(() => {
    const moduleSample = moduleGraph?.nodes?.[0]?.id ?? "src/orchestrator.py";
    const concept = String(semanticist?.modules?.[0]?.domain_cluster ?? "pipeline");
    const datasetSample =
      lineageGraph?.nodes?.find((node) => String(node.id).startsWith("dataset::"))?.id ?? "dataset::orders";
    return [
      `explain module ${moduleSample}`,
      `blast radius ${moduleSample}`,
      `find implementation ${concept}`,
      `trace lineage ${datasetSample}`,
    ];
  }, [lineageGraph, moduleGraph, semanticist]);

  async function bootstrap() {
    try {
      const payload = await listSessions();
      setSessions(payload.sessions);
      const nextRepo = payload.active_repo_id || payload.sessions[0]?.repo_id || "";
      if (!nextRepo) {
        setShowIntake(true);
        return;
      }
      await switchSession(nextRepo, false);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load sessions");
      setShowIntake(true);
    }
  }

  async function refreshSessions() {
    const payload = await listSessions();
    setSessions(payload.sessions);
    return payload;
  }

  function clearArtifacts() {
    setSummary(null);
    setModuleGraph(null);
    setLineageGraph(null);
    setSemanticist(null);
    setArchivist(null);
    setSemanticResults([]);
    setSemanticQuery("");
    setLatestQuery(null);
    setInspector(null);
  }

  async function switchSession(repoId: string, callSelect = true) {
    if (!repoId) return;
    setLoading(true);
    setLoadingError("");

    try {
      if (callSelect) {
        await selectSession(repoId);
      }

      setActiveRepoId(repoId);
      setShowIntake(false);
      setView("overview");
      clearArtifacts();

      const summaryPayload = await fetchSummary(repoId);
      setSummary(summaryPayload);
      await refreshSessions();
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to switch repository");
    } finally {
      setLoading(false);
    }
  }

  async function runAnalysis(repoInput: string) {
    setAnalyzing(true);
    setAnalyzeError("");
    try {
      const result = await analyzeRepository({ repo_input: repoInput, incremental: true });
      await refreshSessions();
      await switchSession(result.session.repo_id, false);
      setShowIntake(false);
    } catch (error) {
      setAnalyzeError(error instanceof Error ? error.message : "Analysis failed");
    } finally {
      setAnalyzing(false);
    }
  }

  async function refreshCurrentRepo() {
    if (!activeSession) return;
    await runAnalysis(activeSession.repo_input);
  }

  async function loadSummary(repoId: string) {
    const payload = await fetchSummary(repoId);
    setSummary(payload);
  }

  async function loadModuleGraph(repoId: string) {
    try {
      const payload = await fetchModuleGraph(repoId);
      setModuleGraph(payload);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load module graph");
    }
  }

  async function loadLineageGraph(repoId: string) {
    try {
      const payload = await fetchLineageGraph(repoId);
      setLineageGraph(payload);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load lineage graph");
    }
  }

  async function loadSemanticist(repoId: string) {
    try {
      const payload = await fetchSemanticist(repoId);
      setSemanticist(payload);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load semantic index");
    }
  }

  async function loadArchivist(repoId: string) {
    try {
      const payload = await fetchArchivist(repoId);
      setArchivist(payload);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load archivist artifacts");
    }
  }

  async function inspectNode(graph: "module" | "lineage", nodeId: string) {
    if (!activeRepoId) return;
    try {
      const details = await fetchNodeDetails(activeRepoId, graph, nodeId);
      setInspector({
        title: nodeId,
        subtitle: graph === "module" ? "Architecture node" : "Lineage node",
        data: details,
      });
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Failed to load node details");
    }
  }

  async function searchSemantic() {
    if (!activeRepoId) return;
    try {
      const payload = await semanticSearch(activeRepoId, semanticQuery);
      setSemanticResults(payload.results);
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Semantic search failed");
    }
  }

  async function runNavigator(rawQuery: string) {
    if (!activeRepoId) return;
    setQueryRunning(true);
    try {
      const outbound = toToolQuery(rawQuery);
      const payload = await runNavigatorQuery(activeRepoId, outbound);
      setLatestQuery(payload);
      const item: QueryHistoryItem = {
        query: rawQuery,
        ok: payload.ok,
        timestamp: new Date().toISOString(),
      };
      const next = [item, ...queryHistory.filter((entry) => entry.query !== rawQuery)].slice(0, 20);
      setQueryHistory(next);
      saveQueryHistory(activeRepoId, next);

      setInspector({
        title: payload.tool ? `Navigator: ${payload.tool}` : "Navigator",
        subtitle: payload.arg,
        evidence: payload.result,
      });
    } catch (error) {
      setLoadingError(error instanceof Error ? error.message : "Navigator query failed");
    } finally {
      setQueryRunning(false);
    }
  }

  async function reuseQuery(query: string) {
    await runNavigator(query);
  }

  if (!activeRepoId) {
    return (
      <div className="page-root page-root-intake">
        <RepoIntakePage
          sessions={sessions}
          analyzing={analyzing}
          analyzeError={analyzeError || loadingError}
          onAnalyze={runAnalysis}
          onOpenSession={(repoId) => void switchSession(repoId)}
        />
      </div>
    );
  }

  let content: JSX.Element = <section className="panel">Choose a workspace view.</section>;

  if (showIntake) {
    content = (
      <RepoIntakePage
        sessions={sessions}
        analyzing={analyzing}
        analyzeError={analyzeError || loadingError}
        onAnalyze={runAnalysis}
        onOpenSession={(repoId) => void switchSession(repoId)}
      />
    );
  } else if (view === "overview") {
    content = (
      <ArtifactLoader loading={loading} error={loadingError} hasData={Boolean(summary)}>
        {summary ? <OverviewPage summary={summary} onNavigate={setView} /> : null}
      </ArtifactLoader>
    );
  } else if (view === "surveyor") {
    content = (
      <ArtifactLoader
        loading={loading}
        error={loadingError}
        hasData={Boolean(moduleGraph)}
        emptyMessage="Module graph artifact is not available."
      >
        {moduleGraph ? <SurveyorPage graph={moduleGraph} onSelectNode={(id) => void inspectNode("module", id)} /> : null}
      </ArtifactLoader>
    );
  } else if (view === "hydrologist") {
    content = (
      <ArtifactLoader
        loading={loading}
        error={loadingError}
        hasData={Boolean(lineageGraph)}
        emptyMessage="Lineage graph artifact is not available."
      >
        {lineageGraph ? <HydrologistPage graph={lineageGraph} onSelectNode={(id) => void inspectNode("lineage", id)} /> : null}
      </ArtifactLoader>
    );
  } else if (view === "semanticist") {
    content = (
      <ArtifactLoader
        loading={loading}
        error={loadingError}
        hasData={Boolean(semanticist)}
        emptyMessage="Semantic index artifact is not available."
      >
        {semanticist ? (
          <SemanticistPage
            payload={semanticist}
            query={semanticQuery}
            results={semanticResults}
            onQueryChange={setSemanticQuery}
            onSearch={searchSemantic}
            onSelectModule={(path, moduleData) =>
              setInspector({
                title: path,
                subtitle: "Semantic module purpose",
                data: moduleData,
              })
            }
          />
        ) : null}
      </ArtifactLoader>
    );
  } else if (view === "archivist") {
    content = (
      <ArtifactLoader
        loading={loading}
        error={loadingError}
        hasData={Boolean(archivist)}
        emptyMessage="Documentation artifacts are not available."
      >
        {archivist ? (
          <ArchivistPage
            payload={archivist}
            onInspectTrace={(event) =>
              setInspector({
                title: String(event.action ?? "Trace event"),
                subtitle: String(event.agent ?? ""),
                data: event,
              })
            }
          />
        ) : null}
      </ArtifactLoader>
    );
  } else if (view === "navigator") {
    content = (
      <NavigatorPage
        running={queryRunning}
        latest={latestQuery}
        history={queryHistory}
        examples={queryExamples}
        onRun={runNavigator}
        onUseHistory={reuseQuery}
        onInspect={(title, payload) => setInspector({ title, data: payload })}
      />
    );
  }

  return (
    <div className="page-root page-root-shell">
      <AppShell
        sidebar={
          <Sidebar
            sessions={sessions}
            activeRepoId={activeRepoId}
            view={view}
            onViewChange={setView}
            onSessionSelect={(repoId) => void switchSession(repoId)}
            onOpenIntake={() => setShowIntake(true)}
          />
        }
        header={<HeaderBar session={activeSession} summary={summary} onRefresh={() => void refreshCurrentRepo()} refreshPending={analyzing} />}
        inspector={<InspectorPanel inspector={inspector} />}
      >
        {content}
      </AppShell>
    </div>
  );
}
