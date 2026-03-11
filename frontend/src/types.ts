export type WorkspaceView =
  | "overview"
  | "surveyor"
  | "hydrologist"
  | "semanticist"
  | "archivist"
  | "navigator";

export interface RepositoryMetadata {
  owner: string;
  repo_name: string;
  branch: string;
  display_name: string;
  url?: string;
}

export interface ArtifactInfo {
  name: string;
  exists: boolean;
  size_bytes: number;
}

export interface RepoSession {
  repo_id: string;
  repo_name: string;
  repo_owner?: string;
  repo_branch?: string;
  repo_display_name?: string;
  repo_url?: string;
  repo_input: string;
  repo_path: string;
  cartography_dir: string;
  last_analysis_epoch: number;
  last_analysis_timestamp: string;
  artifact_location: string;
  available_artifacts: string[];
  artifacts: ArtifactInfo[];
}

export interface SummaryPayload {
  repo_name: string;
  cartography_dir: string;
  repository?: RepositoryMetadata;
  last_analysis_timestamp: string;
  artifact_status: string;
  artifact_count?: number;
  artifacts: string[];
  metrics: {
    modules: number;
    functions: number;
    datasets: number;
    transformations: number;
    lineage_edges: number;
    dead_code_candidates: number;
  };
  top_modules: Array<{ id: string; label: string; centrality: number }>;
  critical_path_modules: Array<{ id: string; label: string; centrality: number }>;
  quick_links: Array<{ view: string; label: string }>;
  agent_stories: Array<{ agent: string; problem: string; artifact: string; summary: string }>;
}

export interface GraphNode {
  id: string;
  label: string;
  [key: string]: unknown;
}

export interface GraphEdge {
  from: string;
  to: string;
  [key: string]: unknown;
}

export interface GraphPayload {
  nodes: GraphNode[];
  edges: GraphEdge[];
  hubs?: string[];
}

export interface SemanticPayload {
  modules: Array<Record<string, unknown>>;
  domain_clusters: Array<{ cluster: string; count: number }>;
  drift_flags: Array<{ path: string; flag: string; severity: string }>;
}

export interface SemanticSearchPayload {
  query: string;
  results: Array<Record<string, unknown>>;
}

export interface ArchivistPayload {
  codebase: {
    markdown: string;
    sections: Array<{ title: string; body: string }>;
  };
  onboarding: {
    markdown: string;
    questions: Array<Record<string, unknown>>;
  };
  trace: Array<Record<string, unknown>>;
  state: Record<string, unknown>;
}

export interface QueryPayload {
  ok: boolean;
  query: string;
  tool: string;
  arg: string;
  direction: string;
  result: unknown;
  error?: string;
}

export interface InspectorState {
  title: string;
  subtitle?: string;
  module_profile?: {
    filename: string;
    module_type: string;
    folder_path: string;
  };
  data?: Record<string, unknown>;
  evidence?: unknown;
}
