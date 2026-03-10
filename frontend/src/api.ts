import type {
  ArchivistPayload,
  GraphPayload,
  QueryPayload,
  RepoSession,
  SemanticPayload,
  SemanticSearchPayload,
  SummaryPayload,
} from "./types";

interface AnalyzeResponse {
  ok: boolean;
  session: RepoSession;
  artifacts: Record<string, string>;
  duration_seconds: number;
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      if (payload?.error) {
        message = String(payload.error);
      }
    } catch {
      const raw = await response.text();
      if (raw.trim()) {
        message = raw.trim();
      }
    }
    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

function withRepoId(path: string, repoId: string): string {
  const connector = path.includes("?") ? "&" : "?";
  return `${path}${connector}repo_id=${encodeURIComponent(repoId)}`;
}

export async function listSessions(): Promise<{ sessions: RepoSession[]; active_repo_id: string }> {
  return request<{ sessions: RepoSession[]; active_repo_id: string }>("/api/sessions");
}

export async function selectSession(repoId: string): Promise<{ session: RepoSession; active_repo_id: string }> {
  return request<{ session: RepoSession; active_repo_id: string }>("/api/session/select", {
    method: "POST",
    body: JSON.stringify({ repo_id: repoId }),
  });
}

export async function analyzeRepository(input: {
  repo_input: string;
  output?: string;
  checkout_root?: string;
  incremental?: boolean;
}): Promise<AnalyzeResponse> {
  return request<AnalyzeResponse>("/api/analyze", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export async function fetchSummary(repoId: string): Promise<SummaryPayload> {
  return request<SummaryPayload>(withRepoId("/api/summary", repoId));
}

export async function fetchModuleGraph(repoId: string): Promise<GraphPayload> {
  return request<GraphPayload>(withRepoId("/api/module-graph", repoId));
}

export async function fetchLineageGraph(repoId: string): Promise<GraphPayload> {
  return request<GraphPayload>(withRepoId("/api/lineage-graph", repoId));
}

export async function fetchSemanticist(repoId: string): Promise<SemanticPayload> {
  return request<SemanticPayload>(withRepoId("/api/semanticist", repoId));
}

export async function semanticSearch(repoId: string, query: string): Promise<SemanticSearchPayload> {
  const base = withRepoId("/api/semantic-search", repoId);
  return request<SemanticSearchPayload>(`${base}&q=${encodeURIComponent(query)}`);
}

export async function fetchArchivist(repoId: string): Promise<ArchivistPayload> {
  return request<ArchivistPayload>(withRepoId("/api/archivist", repoId));
}

export async function runNavigatorQuery(repoId: string, query: string): Promise<QueryPayload> {
  const base = withRepoId("/api/query", repoId);
  return request<QueryPayload>(`${base}&q=${encodeURIComponent(query)}`);
}

export async function fetchNodeDetails(
  repoId: string,
  graph: "module" | "lineage",
  id: string,
): Promise<Record<string, unknown>> {
  const base = withRepoId("/api/node-details", repoId);
  return request<Record<string, unknown>>(`${base}&graph=${encodeURIComponent(graph)}&id=${encodeURIComponent(id)}`);
}

export async function fetchArtifactMetadata(repoId: string): Promise<{ artifacts: Array<Record<string, unknown>> }> {
  return request<{ artifacts: Array<Record<string, unknown>> }>(withRepoId("/api/artifacts", repoId));
}
