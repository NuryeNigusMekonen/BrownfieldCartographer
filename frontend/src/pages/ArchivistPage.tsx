import { useEffect, useMemo, useRef, useState } from "react";
import { MarkdownViewer } from "../components/MarkdownViewer";
import type { ArchivistPayload, RepoSession } from "../types";

interface ArchivistPageProps {
  payload: ArchivistPayload;
  session?: RepoSession | null;
  onInspectTrace: (event: Record<string, unknown>) => void;
}

interface ParsedOnboardingAnswer {
  explanation: string;
  keyLabel: string;
  keyItems: string[];
}

type ArchivistTab = "codebase" | "onboarding" | "trace" | "state";
type TraceAgentFilter = "all" | "surveyor" | "hydrologist" | "semanticist" | "archivist" | "orchestrator";

interface TraceEntry {
  id: string;
  timestamp: string;
  agent: string;
  action: string;
  confidence: string;
  evidence: unknown;
  raw: Record<string, unknown>;
}

const TAB_STORAGE_KEY = "archivist-active-tab";
const CODEBASE_HIGHLIGHTS = new Set([
  "architecture overview",
  "critical path",
  "data sources",
  "data sinks",
  "known debt",
  "high-velocity files",
  "raw git high-velocity files",
  "module purpose index",
]);
const CODEBASE_TITLE_ALIASES: Record<string, string> = {
  architectureoverview: "Architecture Overview",
  criticalpath: "Critical Path",
  datasources: "Data Sources",
  datasinks: "Data Sinks",
  knowndebt: "Known Debt",
  highvelocityfiles: "High-Velocity Files",
  rawgithighvelocityfiles: "Raw Git High-Velocity Files",
  modulepurposeindex: "Module Purpose Index",
};

const WHY_THIS_MATTERS: Record<string, string> = {
  "1": "This tells you where upstream dependencies enter the system and where ingestion risk starts.",
  "2": "These outputs are what downstream consumers depend on, so failures here are user-visible quickly.",
  "3": "Blast radius highlights where one module failure can cascade into multiple dependent modules.",
  "4": "This points you to files where core business rules and transformations are concentrated.",
  "5": "High-velocity areas change frequently and are the most likely to break assumptions during onboarding.",
};

function normalizeConfidence(value: unknown): "low" | "medium" | "high" {
  const normalized = String(value ?? "medium").toLowerCase();
  if (normalized === "high") {
    return "high";
  }
  if (normalized === "low") {
    return "low";
  }
  return "medium";
}

function confidenceLabel(value: unknown): string {
  const normalized = normalizeConfidence(value);
  if (normalized === "high") {
    return "High";
  }
  if (normalized === "low") {
    return "Low";
  }
  return "Medium";
}

function confidenceScoreLabel(value: unknown): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "";
  }
  return ` (${value.toFixed(2)})`;
}

function formatBytes(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return "Unknown";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatRelativeTime(epochSeconds: number | null): string {
  if (typeof epochSeconds !== "number" || !Number.isFinite(epochSeconds) || epochSeconds <= 0) {
    return "Unknown";
  }
  const deltaSeconds = Math.max(0, Math.floor(Date.now() / 1000 - epochSeconds));
  if (deltaSeconds < 60) {
    return "Just now";
  }
  if (deltaSeconds < 3600) {
    const minutes = Math.floor(deltaSeconds / 60);
    return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
  }
  if (deltaSeconds < 86400) {
    const hours = Math.floor(deltaSeconds / 3600);
    return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  }
  const days = Math.floor(deltaSeconds / 86400);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}

function syntaxHighlightJson(value: unknown): string {
  const safeJson = JSON.stringify(value, null, 2)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  return safeJson.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)/g,
    (match) => {
      let cls = "json-number";
      if (match.startsWith('"')) {
        cls = match.endsWith(":") ? "json-key" : "json-string";
      } else if (match === "true" || match === "false") {
        cls = "json-boolean";
      } else if (match === "null") {
        cls = "json-null";
      }
      return `<span class="${cls}">${match}</span>`;
    }
  );
}

function formatTimestamp(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    const asDate = new Date(value * 1000);
    return Number.isNaN(asDate.getTime()) ? "Unknown time" : asDate.toLocaleString();
  }
  if (typeof value === "string" && value.trim()) {
    const asDate = new Date(value);
    return Number.isNaN(asDate.getTime()) ? value : asDate.toLocaleString();
  }
  return "Unknown time";
}

function summarizeEvidence(value: unknown): string {
  if (!value) {
    return "No evidence payload recorded.";
  }
  if (Array.isArray(value)) {
    return value.length ? `${value.length} evidence entries captured.` : "No evidence entries captured.";
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (!entries.length) {
      return "No evidence payload recorded.";
    }
    return entries
      .slice(0, 3)
      .map(([key, entry]) => {
        if (typeof entry === "string" || typeof entry === "number" || typeof entry === "boolean") {
          return `${key}: ${String(entry)}`;
        }
        if (Array.isArray(entry)) {
          return `${key}: ${entry.length} items`;
        }
        if (entry && typeof entry === "object") {
          return `${key}: ${Object.keys(entry as Record<string, unknown>).length} fields`;
        }
        return `${key}: value`;
      })
      .join(" | ");
  }
  return String(value);
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-");
}

function firstTextLine(value: string): string {
  const lines = String(value ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  return lines[0] ?? "";
}

function normalizeCodebaseSectionTitle(value: string): string {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "Untitled Section";
  }
  const aliasKey = raw.replace(/[^a-z0-9]/gi, "").toLowerCase();
  if (CODEBASE_TITLE_ALIASES[aliasKey]) {
    return CODEBASE_TITLE_ALIASES[aliasKey];
  }
  if (raw.includes(" ")) {
    return raw;
  }
  const fromCamel = raw.replace(/([a-z])([A-Z])/g, "$1 $2").trim();
  if (fromCamel.includes(" ")) {
    return fromCamel;
  }
  return raw;
}

function parseOnboardingAnswer(answer: string): ParsedOnboardingAnswer {
  const lines = String(answer ?? "").split("\n").map((line) => line.replace(/\s+$/, ""));
  const nonEmpty = lines.map((line) => line.trim()).filter(Boolean);
  const explanation = nonEmpty[0] ?? "";

  let keyLabel = "Key Entities";
  let keyItems: string[] = [];
  const labelIndex = lines.findIndex((line) => /^key\s+.+:\s*$/i.test(line.trim()));
  if (labelIndex >= 0) {
    keyLabel = lines[labelIndex].trim().replace(/:\s*$/, "");
    for (let i = labelIndex + 1; i < lines.length; i += 1) {
      const text = lines[i].trim();
      if (!text) {
        if (keyItems.length > 0) {
          break;
        }
        continue;
      }
      if (text.startsWith("- ")) {
        keyItems.push(text.slice(2).trim());
        continue;
      }
      if (keyItems.length === 0) {
        keyItems.push(text);
      }
    }
  }
  return { explanation, keyLabel, keyItems };
}

export function ArchivistPage({ payload, session, onInspectTrace }: ArchivistPageProps) {
  const initialTab = useMemo<ArchivistTab>(() => {
    try {
      const stored = window.sessionStorage.getItem(TAB_STORAGE_KEY);
      if (stored === "codebase" || stored === "onboarding" || stored === "trace" || stored === "state") {
        return stored;
      }
    } catch {
      return "codebase";
    }
    return "codebase";
  }, []);

  const [tab, setTab] = useState<ArchivistTab>(initialTab);
  const [codeCollapsed, setCodeCollapsed] = useState<Record<string, boolean>>({});
  const [codeJump, setCodeJump] = useState<string>("");
  const [copyStatus, setCopyStatus] = useState<string>("");
  const [onboardingEvidenceOpen, setOnboardingEvidenceOpen] = useState<Record<string, boolean>>({});
  const [traceFilter, setTraceFilter] = useState<TraceAgentFilter>("all");
  const [stateCopyStatus, setStateCopyStatus] = useState<string>("");
  const [showStateJsonModal, setShowStateJsonModal] = useState<boolean>(false);

  const tabPanels = useRef<Record<ArchivistTab, HTMLDivElement | null>>({
    codebase: null,
    onboarding: null,
    trace: null,
    state: null,
  });
  const tabScrollPositions = useRef<Record<ArchivistTab, number>>({
    codebase: 0,
    onboarding: 0,
    trace: 0,
    state: 0,
  });
  const codeSectionRefs = useRef<Record<string, HTMLDivElement | null>>({});

  useEffect(() => {
    try {
      window.sessionStorage.setItem(TAB_STORAGE_KEY, tab);
    } catch {
      // Ignore storage errors in restricted environments.
    }
  }, [tab]);

  useEffect(() => {
    const panel = tabPanels.current[tab];
    if (panel) {
      panel.scrollTop = tabScrollPositions.current[tab] ?? 0;
    }
  }, [tab]);

  useEffect(() => {
    if (!showStateJsonModal) {
      return;
    }
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setShowStateJsonModal(false);
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [showStateJsonModal]);

  const codeSections = useMemo(() => payload.codebase.sections ?? [], [payload.codebase.sections]);
  const traceEntries = useMemo<TraceEntry[]>(() => {
    return (payload.trace ?? []).map((item, index) => {
      const agent = String(item.agent ?? "unknown").toLowerCase();
      const action = String(item.action ?? "action");
      const confidence = String(item.confidence ?? "medium");
      const timestamp = String(item.timestamp ?? "");
      const evidence = item.evidence ?? {};
      return {
        id: `${agent}-${action}-${index}`,
        timestamp,
        agent,
        action,
        confidence,
        evidence,
        raw: item,
      };
    });
  }, [payload.trace]);

  const filteredTrace = useMemo(() => {
    if (traceFilter === "all") {
      return traceEntries;
    }
    return traceEntries.filter((entry) => entry.agent === traceFilter);
  }, [traceEntries, traceFilter]);

  useEffect(() => {
    if (!codeSections.length) {
      setCodeCollapsed({});
      return;
    }
    setCodeCollapsed((previous) => {
      if (Object.keys(previous).length) {
        return previous;
      }
      const next: Record<string, boolean> = {};
      codeSections.forEach((section, index) => {
        const id = `${slugify(section.title)}-${index + 1}`;
        const lowered = section.title.toLowerCase();
        next[id] = lowered.includes("module purpose index") || section.body.length > 2600;
      });
      return next;
    });
  }, [codeSections]);

  useEffect(() => {
    if (!payload.onboarding.questions.length) {
      setOnboardingEvidenceOpen({});
      return;
    }
    setOnboardingEvidenceOpen((previous) => {
      if (Object.keys(previous).length) {
        return previous;
      }
      const next: Record<string, boolean> = {};
      payload.onboarding.questions.forEach((question) => {
        next[question.id] = false;
      });
      return next;
    });
  }, [payload.onboarding.questions]);

  const handleTabSwitch = (nextTab: ArchivistTab) => {
    const current = tabPanels.current[tab];
    if (current) {
      tabScrollPositions.current[tab] = current.scrollTop;
    }
    setTab(nextTab);
  };

  const confidenceTooltip = (question: {
    confidence_reason?: string;
    confidence_score?: number | null;
    confidence_factors?: Record<string, number>;
    confidence_components?: Record<string, number>;
  }): string | undefined => {
    const score = typeof question.confidence_score === "number" ? question.confidence_score : null;
    const factors = question.confidence_factors ?? {};
    const components = question.confidence_components ?? {};
    const evidenceCount =
      typeof factors.evidence_count === "number"
        ? factors.evidence_count
        : typeof components.evidence_count_score === "number"
          ? components.evidence_count_score
          : null;
    const evidenceDiversity =
      typeof factors.evidence_diversity === "number"
        ? factors.evidence_diversity
        : typeof components.evidence_diversity_score === "number"
          ? components.evidence_diversity_score
          : null;
    const graphCoverage =
      typeof factors.graph_coverage === "number"
        ? factors.graph_coverage
        : typeof components.graph_coverage_score === "number"
          ? components.graph_coverage_score
          : null;
    const heuristicReliability =
      typeof factors.heuristic_reliability === "number"
        ? factors.heuristic_reliability
        : typeof components.heuristic_reliability_score === "number"
          ? components.heuristic_reliability_score
          : null;
    const signalAgreement =
      typeof factors.signal_agreement === "number"
        ? factors.signal_agreement
        : typeof components.signal_agreement_score === "number"
          ? components.signal_agreement_score
          : null;
    const repoTypeFit =
      typeof factors.repo_type_fit === "number"
        ? factors.repo_type_fit
        : typeof components.repo_type_fit_score === "number"
          ? components.repo_type_fit_score
          : null;
    const reason = typeof question.confidence_reason === "string" ? question.confidence_reason.trim() : "";
    if (
      score === null &&
      evidenceCount === null &&
      evidenceDiversity === null &&
      graphCoverage === null &&
      heuristicReliability === null &&
      signalAgreement === null &&
      repoTypeFit === null &&
      !reason
    ) {
      return undefined;
    }
    const format = (value: number | null): string => (value === null ? "n/a" : value.toFixed(2));
    return [
      "confidence_score = 0.25*evidence_count + 0.20*evidence_diversity + 0.20*graph_coverage + 0.15*heuristic_reliability + 0.10*signal_agreement + 0.10*repo_type_fit",
      `score=${format(score)}`,
      `evidence_count=${format(evidenceCount)}`,
      `evidence_diversity=${format(evidenceDiversity)}`,
      `graph_coverage=${format(graphCoverage)}`,
      `heuristic_reliability=${format(heuristicReliability)}`,
      `signal_agreement=${format(signalAgreement)}`,
      `repo_type_fit=${format(repoTypeFit)}`,
      reason ? `reason=${reason}` : null,
    ]
      .filter((line): line is string => Boolean(line))
      .join("\n");
  };

  const repository = payload.state?.repository && typeof payload.state.repository === "object" ? payload.state.repository : {};
  const repoOwner = String((repository as Record<string, unknown>).owner ?? "unknown");
  const repoShortName = String((repository as Record<string, unknown>).repo_name ?? "unknown");
  const repoSlug = `${repoOwner}/${repoShortName}`;
  const repoName = String(
    (repository as Record<string, unknown>).display_name ||
      (repository as Record<string, unknown>).repo_name ||
      (payload.state?.repo_name as string) ||
      "Repository"
  );
  const showRepoSlug = repoSlug.toLowerCase() !== repoName.toLowerCase();
  const analysisTimestamp = formatTimestamp((payload.state as Record<string, unknown>)?.analyzed_at_epoch);
  const analysisEpoch =
    typeof (payload.state as Record<string, unknown>)?.analyzed_at_epoch === "number"
      ? ((payload.state as Record<string, unknown>).analyzed_at_epoch as number)
      : typeof session?.last_analysis_epoch === "number"
        ? session.last_analysis_epoch
        : null;
  const analysisRelativeTime = formatRelativeTime(analysisEpoch);
  const repoUrlRaw = String((repository as Record<string, unknown>).url ?? "").trim();
  const repoUrl = repoUrlRaw && repoUrlRaw !== "unknown" ? repoUrlRaw : "";
  const repoPath = session?.repo_path || String((repository as Record<string, unknown>).path ?? "Unknown");
  const repoIdentifier = session?.repo_id || `${repoOwner}:${repoShortName}`;
  const sessionArtifacts = Array.isArray(session?.artifacts) ? session.artifacts : [];
  const artifactMetadataByName = new Map(sessionArtifacts.map((artifact) => [artifact.name, artifact]));
  const artifactStates = [
    {
      label: "CODEBASE",
      fileName: "CODEBASE.md",
      available: Boolean(payload.codebase.markdown?.trim()),
      openAction: () => handleTabSwitch("codebase"),
    },
    {
      label: "Onboarding Brief",
      fileName: "onboarding_brief.md",
      available: Boolean(payload.onboarding.markdown?.trim()) || payload.onboarding.questions.length > 0,
      openAction: () => handleTabSwitch("onboarding"),
    },
    {
      label: "Trace Timeline",
      fileName: "cartography_trace.jsonl",
      available: payload.trace.length > 0,
      openAction: () => handleTabSwitch("trace"),
    },
    {
      label: "State",
      fileName: "state.json",
      available: Object.keys(payload.state ?? {}).length > 0,
      openAction: () => setShowStateJsonModal(true),
    },
  ];
  const availableArtifactCount = artifactStates.filter((artifact) => artifact.available).length;
  const generatedArtifacts = session?.available_artifacts?.length ?? availableArtifactCount;
  const totalArtifacts = sessionArtifacts.length || artifactStates.length;
  const analysisStatus =
    availableArtifactCount === artifactStates.length ? "completed" : availableArtifactCount > 0 ? "partial" : "failed";

  const codebaseSummaryCopy = async () => {
    const summaryLines = codeSections.length
      ? codeSections.map((section) => `- ${section.title}: ${firstTextLine(section.body) || "Summary unavailable."}`)
      : [payload.codebase.markdown || "CODEBASE.md is empty or missing."];
    const text = ["# CODEBASE Summary", ...summaryLines].join("\n");
    try {
      await navigator.clipboard.writeText(text);
      setCopyStatus("Summary copied.");
    } catch {
      setCopyStatus("Clipboard unavailable.");
    }
    window.setTimeout(() => {
      setCopyStatus("");
    }, 1800);
  };

  const codebaseSectionEntries = codeSections.map((section, index) => {
    const id = `${slugify(section.title)}-${index + 1}`;
    const title = normalizeCodebaseSectionTitle(section.title);
    const highlighted = CODEBASE_HIGHLIGHTS.has(title.toLowerCase());
    return { ...section, title, id, highlighted };
  });

  const toggleAllCodeSections = (expand: boolean) => {
    const next: Record<string, boolean> = {};
    codebaseSectionEntries.forEach((entry) => {
      next[entry.id] = !expand;
    });
    setCodeCollapsed(next);
  };

  const toggleAllOnboardingEvidence = (expand: boolean) => {
    const next: Record<string, boolean> = {};
    payload.onboarding.questions.forEach((question) => {
      next[question.id] = expand;
    });
    setOnboardingEvidenceOpen(next);
  };

  const jumpToCodebaseSection = (sectionId: string) => {
    setCodeJump(sectionId);
    const target = codeSectionRefs.current[sectionId];
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const stateData = payload.state ?? {};
  const stateRecord = stateData as Record<string, unknown>;
  const branch = String((repository as Record<string, unknown>).branch ?? "unknown");
  const durationSecondsRaw =
    (typeof stateRecord.analysis_duration_seconds === "number" && stateRecord.analysis_duration_seconds) ||
    (typeof stateRecord.duration_seconds === "number" && stateRecord.duration_seconds) ||
    (typeof stateRecord.run_duration_seconds === "number" && stateRecord.run_duration_seconds) ||
    null;
  const analysisDuration =
    typeof durationSecondsRaw === "number" && Number.isFinite(durationSecondsRaw) && durationSecondsRaw > 0
      ? durationSecondsRaw
      : null;
  const analysisDurationLabel =
    analysisDuration === null
      ? "Not recorded"
      : analysisDuration < 90
        ? `${analysisDuration.toFixed(1)}s`
        : `${(analysisDuration / 60).toFixed(1)}m`;
  const incrementalMode =
    typeof stateRecord.incremental === "boolean" ? (stateRecord.incremental ? "enabled" : "disabled") : "unknown";
  const gitHead = String(stateRecord.head ?? "unknown");
  const gitHeadShort = gitHead !== "unknown" && gitHead.length > 14 ? `${gitHead.slice(0, 14)}…` : gitHead;
  const copyGitHead = async () => {
    if (!gitHead || gitHead === "unknown") {
      return;
    }
    try {
      await navigator.clipboard.writeText(gitHead);
      setStateCopyStatus("Git head copied.");
    } catch {
      setStateCopyStatus("Clipboard unavailable.");
    }
    window.setTimeout(() => setStateCopyStatus(""), 1800);
  };
  const knownStateFields = new Set(["repository", "analyzed_at_epoch", "head", "duration_seconds", "analysis_duration_seconds"]);
  const extraStateEntries = Object.entries(stateRecord).filter(([key]) => !knownStateFields.has(key));
  const graphMissing = ["module_graph.json", "lineage_graph.json"].some(
    (name) => artifactMetadataByName.get(name)?.exists === false
  );
  const systemHealth =
    analysisStatus === "completed" && !graphMissing
      ? {
          level: "ok",
          message: "All artifacts generated successfully.",
        }
      : availableArtifactCount > 0 && graphMissing
        ? {
            level: "warn",
            message: "Partial analysis results detected. Graph generation is incomplete.",
          }
        : availableArtifactCount > 0
          ? {
              level: "warn",
              message: "Partial analysis results detected.",
            }
          : {
              level: "error",
              message: "Analysis failed or no artifacts were generated.",
            };

  return (
    <div className="stack archivist-workspace">
      <section className="panel archivist-summary-strip">
        <div className="archivist-summary-grid">
          <article className="archivist-metric-card">
            <p className="archivist-metric-label">Repository</p>
            <strong className="archivist-metric-value">{repoName}</strong>
          </article>
          <article className="archivist-metric-card">
            <p className="archivist-metric-label">Last Analysis</p>
            <strong className="archivist-metric-value">{analysisTimestamp}</strong>
          </article>
          <article className="archivist-metric-card">
            <p className="archivist-metric-label">Artifacts</p>
            <strong className="archivist-metric-value">{availableArtifactCount} available</strong>
          </article>
          <article className="archivist-metric-card">
            <p className="archivist-metric-label">Status</p>
            <strong
              className={`archivist-status-chip ${
                analysisStatus === "completed" ? "ok" : analysisStatus === "partial" ? "warn" : "error"
              }`}
            >
              {analysisStatus === "completed" ? "Completed" : analysisStatus === "partial" ? "Partial" : "Failed"}
            </strong>
          </article>
        </div>
      </section>

      <section className="panel archivist-header-panel">
        <p className="eyebrow">Archivist</p>
        <h3>Long-Lived Project Understanding</h3>
        <p className="muted">
          Archivist preserves what this repository is, what was analyzed, which outputs matter, and what to read first for onboarding.
        </p>
        <div className="pill-row archivist-tab-row">
          <button onClick={() => handleTabSwitch("codebase")} className={tab === "codebase" ? "active" : ""}>
            CODEBASE
          </button>
          <button onClick={() => handleTabSwitch("onboarding")} className={tab === "onboarding" ? "active" : ""}>
            Onboarding Brief
          </button>
          <button onClick={() => handleTabSwitch("trace")} className={tab === "trace" ? "active" : ""}>
            Trace Timeline
          </button>
          <button onClick={() => handleTabSwitch("state")} className={tab === "state" ? "active" : ""}>
            State Summary
          </button>
        </div>
      </section>

      {tab === "codebase" ? (
        <section ref={(node) => (tabPanels.current.codebase = node)} className="panel doc-panel archivist-tab-panel">
          {!payload.codebase.markdown.trim() ? (
            <div className="archivist-empty-state">
              <h4>CODEBASE.md is not available</h4>
              <p className="muted">
                Run a fresh analysis to generate CODEBASE context before using this onboarding workspace.
              </p>
            </div>
          ) : (
            <div className="codebase-layout">
              <aside className="codebase-toc">
                <p className="codebase-toc-title">Jump to Section</p>
                <select
                  value={codeJump}
                  onChange={(event) => jumpToCodebaseSection(event.target.value)}
                  className="codebase-jump-select"
                >
                  <option value="">Select section</option>
                  {codebaseSectionEntries.map((section) => (
                    <option key={section.id} value={section.id}>
                      {section.title}
                    </option>
                  ))}
                </select>
                <div className="codebase-action-row">
                  <button onClick={() => toggleAllCodeSections(true)}>Expand All</button>
                  <button onClick={() => toggleAllCodeSections(false)}>Collapse All</button>
                </div>
                <button className="primary-action codebase-copy-btn" onClick={codebaseSummaryCopy}>
                  Copy Summary
                </button>
                {copyStatus ? <p className="tiny muted">{copyStatus}</p> : null}
                <nav className="codebase-nav-list">
                  {codebaseSectionEntries.map((section) => (
                    <button
                      key={section.id}
                      className={section.highlighted ? "highlight" : ""}
                      onClick={() => jumpToCodebaseSection(section.id)}
                    >
                      {section.title}
                    </button>
                  ))}
                </nav>
              </aside>

              <div className="codebase-sections-scroll">
                {codebaseSectionEntries.length ? (
                  codebaseSectionEntries.map((section) => {
                    const collapsed = Boolean(codeCollapsed[section.id]);
                    return (
                      <article
                        key={section.id}
                        ref={(node) => {
                          codeSectionRefs.current[section.id] = node;
                        }}
                        className={`codebase-section-card ${section.highlighted ? "is-highlight" : ""}`}
                      >
                        <header className="codebase-section-header">
                          <h4>{section.title}</h4>
                          <button onClick={() => setCodeCollapsed((prev) => ({ ...prev, [section.id]: !collapsed }))}>
                            {collapsed ? "Expand" : "Collapse"}
                          </button>
                        </header>
                        {!collapsed ? <MarkdownViewer markdown={section.body} /> : null}
                      </article>
                    );
                  })
                ) : (
                  <article className="codebase-section-card">
                    <MarkdownViewer markdown={payload.codebase.markdown} />
                  </article>
                )}
              </div>
            </div>
          )}
        </section>
      ) : null}

      {tab === "onboarding" ? (
        <section ref={(node) => (tabPanels.current.onboarding = node)} className="panel doc-panel archivist-tab-panel">
          <div className="onboarding-toolbar">
            <p className="eyebrow">Day-One Questions</p>
            <div className="onboarding-toolbar-actions">
              <button onClick={() => toggleAllOnboardingEvidence(true)}>Expand All Evidence</button>
              <button onClick={() => toggleAllOnboardingEvidence(false)}>Collapse All Evidence</button>
            </div>
          </div>
          {payload.onboarding.questions.length ? (
            <div className="onboarding-question-list">
              {payload.onboarding.questions.map((question) => {
                const parsed = parseOnboardingAnswer(question.answer);
                const confidence = question.confidence_label ?? question.confidence;
                const helper = WHY_THIS_MATTERS[String(question.id)] ?? "This section captures a critical onboarding signal.";
                return (
                  <article key={question.id} className="onboarding-question-card">
                    <div className="onboarding-question-heading">
                      <h4 className="onboarding-question-title">
                        {question.id}) {question.title}
                      </h4>
                      <span className={`confidence-badge ${normalizeConfidence(confidence)}`} title={confidenceTooltip(question)}>
                        {confidenceLabel(confidence)}
                        {confidenceScoreLabel(question.confidence_score)}
                      </span>
                    </div>

                    {parsed.explanation ? <p className="onboarding-explanation">{parsed.explanation}</p> : null}
                    <p className="onboarding-helper">{helper}</p>

                    {parsed.keyItems.length ? (
                      <div className="onboarding-key-block">
                        <p className="onboarding-key-label">{parsed.keyLabel}</p>
                        <ul className="onboarding-key-list">
                          {parsed.keyItems.map((item) => (
                            <li key={item}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}

                    <details
                      className="onboarding-evidence"
                      open={Boolean(onboardingEvidenceOpen[question.id])}
                      onToggle={(event) => {
                        const target = event.currentTarget;
                        setOnboardingEvidenceOpen((prev) => ({ ...prev, [question.id]: target.open }));
                      }}
                    >
                      <summary>Evidence ({question.evidence.length})</summary>
                      <pre className="onboarding-evidence-code">{JSON.stringify(question.evidence, null, 2)}</pre>
                    </details>
                  </article>
                );
              })}
            </div>
          ) : (
            <div className="archivist-empty-state">
              <h4>Onboarding brief is empty</h4>
              <p className="muted">No Day-One question cards were parsed from onboarding artifacts.</p>
            </div>
          )}
        </section>
      ) : null}

      {tab === "trace" ? (
        <section ref={(node) => (tabPanels.current.trace = node)} className="panel doc-panel archivist-tab-panel">
          <div className="trace-toolbar">
            <p className="eyebrow">Execution History</p>
            <div className="trace-filter-row">
              {(["all", "surveyor", "hydrologist", "semanticist", "archivist", "orchestrator"] as TraceAgentFilter[]).map(
                (value) => (
                  <button
                    key={value}
                    className={traceFilter === value ? "active" : ""}
                    onClick={() => setTraceFilter(value)}
                  >
                    {value === "all" ? "All Agents" : value}
                  </button>
                )
              )}
            </div>
          </div>
          {filteredTrace.length ? (
            <ol className="trace-timeline">
              {filteredTrace.map((entry) => (
                <li key={entry.id} className="trace-item">
                  <div className="trace-item-marker" />
                  <article className="trace-item-card">
                    <div className="trace-item-header">
                      <span className={`agent-chip agent-${entry.agent || "unknown"}`}>{entry.agent || "unknown"}</span>
                      <span className="trace-time">{formatTimestamp(entry.timestamp)}</span>
                      <span className={`confidence-badge ${normalizeConfidence(entry.confidence)}`}>
                        {confidenceLabel(entry.confidence)}
                      </span>
                    </div>
                    <h4 className="trace-action">{entry.action}</h4>
                    <p className="trace-summary">{summarizeEvidence(entry.evidence)}</p>
                    <details className="trace-evidence">
                      <summary>View full evidence</summary>
                      <pre className="trace-evidence-code">{JSON.stringify(entry.evidence, null, 2)}</pre>
                    </details>
                    <button onClick={() => onInspectTrace(entry.raw)}>Inspect in side panel</button>
                  </article>
                </li>
              ))}
            </ol>
          ) : (
            <div className="archivist-empty-state">
              <h4>No trace events available</h4>
              <p className="muted">Trace artifacts are missing or no events matched the selected agent filter.</p>
            </div>
          )}
        </section>
      ) : null}

      {tab === "state" ? (
        <section ref={(node) => (tabPanels.current.state = node)} className="panel doc-panel archivist-tab-panel">
          <section className="state-status-strip">
            <article className="state-status-card">
              <p className="state-status-label">
                <span className="state-icon repo" />
                Repository
              </p>
              <strong>{repoName}</strong>
            </article>
            <article className="state-status-card">
              <p className="state-status-label">
                <span className="state-icon branch" />
                Branch
              </p>
              <strong>{branch}</strong>
            </article>
            <article className="state-status-card">
              <p className="state-status-label">
                <span className="state-icon clock" />
                Last Analysis
              </p>
              <strong>{analysisRelativeTime}</strong>
              <span className="muted tiny">{analysisTimestamp}</span>
            </article>
            <article className="state-status-card">
              <p className="state-status-label">
                <span className="state-icon artifact" />
                Artifacts Generated
              </p>
              <strong>
                {generatedArtifacts}/{totalArtifacts}
              </strong>
            </article>
            <article className="state-status-card">
              <p className="state-status-label">
                <span className="state-icon status" />
                Analysis Status
              </p>
              <span className={`state-health-badge ${analysisStatus}`}>
                {analysisStatus === "completed" ? "Completed" : analysisStatus === "partial" ? "Partial" : "Failed"}
              </span>
            </article>
          </section>

          <section className={`state-health-banner ${systemHealth.level}`}>
            <p className="state-health-title">Analysis Health</p>
            <p className="state-health-message">{systemHealth.message}</p>
          </section>

          <section className="state-runtime-section">
            <div className="state-toolbar">
              <p className="eyebrow">Runtime State</p>
              <details className="state-action-menu">
                <summary>Actions</summary>
                <div className="state-action-menu-popover">
                  <button onClick={() => setShowStateJsonModal(true)}>View Raw JSON</button>
                  <button onClick={copyGitHead} disabled={gitHead === "unknown"}>
                    Copy Git Head
                  </button>
                </div>
              </details>
            </div>

            <div className="state-summary-grid state-runtime-grid">
              <article className="state-card state-runtime-card">
                <h4>Repository</h4>
                <dl className="state-runtime-list">
                  <div>
                    <dt>Name</dt>
                    <dd className="state-repo-name">{repoName}</dd>
                  </div>
                  <div>
                    <dt>Repository Path</dt>
                    <dd className="state-mono">{repoPath}</dd>
                  </div>
                  <div>
                    <dt>Identifier</dt>
                    <dd className="state-mono">{repoIdentifier}</dd>
                  </div>
                  {showRepoSlug ? (
                    <div>
                      <dt>Slug</dt>
                      <dd>{repoSlug}</dd>
                    </div>
                  ) : null}
                  {repoUrl ? (
                    <div>
                      <dt>Remote</dt>
                      <dd>
                        <a href={repoUrl} target="_blank" rel="noreferrer">
                          Open remote
                        </a>
                      </dd>
                    </div>
                  ) : null}
                </dl>
              </article>

              <article className="state-card state-runtime-card">
                <h4>Analysis Runtime</h4>
                <dl className="state-runtime-list">
                  <div>
                    <dt>Last Updated</dt>
                    <dd>{analysisTimestamp}</dd>
                  </div>
                  <div>
                    <dt>Updated</dt>
                    <dd>{analysisRelativeTime}</dd>
                  </div>
                  <div>
                    <dt>Duration</dt>
                    <dd>{analysisDurationLabel}</dd>
                  </div>
                  <div>
                    <dt>Branch</dt>
                    <dd>{branch}</dd>
                  </div>
                </dl>
              </article>

              <article className="state-card state-runtime-card">
                <h4>Git / Execution</h4>
                <dl className="state-runtime-list">
                  <div>
                    <dt>Git Head</dt>
                    <dd className="state-mono" title={gitHead}>
                      {gitHeadShort}
                    </dd>
                  </div>
                  <div>
                    <dt>Incremental Mode</dt>
                    <dd>
                      <span className={`state-mode-chip ${incrementalMode}`}>{incrementalMode}</span>
                    </dd>
                  </div>
                  <div>
                    <dt>Copy Commit</dt>
                    <dd>
                      <button onClick={copyGitHead} disabled={gitHead === "unknown"}>
                        Copy
                      </button>
                    </dd>
                  </div>
                </dl>
              </article>
            </div>

            {stateCopyStatus ? <p className="tiny muted">{stateCopyStatus}</p> : null}
          </section>

          <section className="state-artifacts">
            <div className="state-artifact-heading">
              <h4>Artifact Availability</h4>
              <span className={`state-artifact-count ${availableArtifactCount >= 3 ? "ok" : "warn"}`}>
                {availableArtifactCount}/4 available
              </span>
            </div>
            <div className="state-artifact-grid">
              {artifactStates.map((artifact) => {
                const metadata = artifactMetadataByName.get(artifact.fileName);
                const statusAvailable = artifact.available || Boolean(metadata?.exists);
                return (
                  <article key={artifact.label} className={`state-artifact-card ${statusAvailable ? "ok" : "warn"}`}>
                    <div className="state-artifact-card-header">
                      <p>
                        <span className="state-icon file" />
                        {artifact.label}
                      </p>
                      <span className={`state-artifact-status ${statusAvailable ? "ok" : "warn"}`}>
                        <i />
                        {statusAvailable ? "Available" : "Missing"}
                      </span>
                    </div>
                    <dl className="state-artifact-meta">
                      <div>
                        <dt>Size</dt>
                        <dd>{statusAvailable ? formatBytes(metadata?.size_bytes) : "--"}</dd>
                      </div>
                      <div>
                        <dt>Updated</dt>
                        <dd>{statusAvailable ? analysisRelativeTime : "--"}</dd>
                      </div>
                      <div>
                        <dt>Generation</dt>
                        <dd>{statusAvailable ? "Generated" : "Not generated"}</dd>
                      </div>
                    </dl>
                    <button onClick={artifact.openAction} disabled={!statusAvailable}>
                      Open
                    </button>
                  </article>
                );
              })}
            </div>
          </section>

          {extraStateEntries.length ? (
            <section className="state-extra">
              <h4>Additional State Fields</h4>
              <div className="state-extra-grid">
                {extraStateEntries.map(([key, value]) => (
                  <article key={key} className="state-extra-card">
                    <p className="state-label">{key}</p>
                    <span className="state-extra-value">{typeof value === "string" ? value : JSON.stringify(value)}</span>
                  </article>
                ))}
              </div>
            </section>
          ) : null}

          {showStateJsonModal ? (
            <div className="state-json-modal-backdrop" onClick={() => setShowStateJsonModal(false)} role="presentation">
              <section
                className="state-json-modal"
                role="dialog"
                aria-modal="true"
                aria-label="Raw state JSON"
                onClick={(event) => event.stopPropagation()}
              >
                <header className="state-json-modal-header">
                  <h4>state.json</h4>
                  <button onClick={() => setShowStateJsonModal(false)}>Close</button>
                </header>
                <pre className="state-json-modal-code">
                  <code dangerouslySetInnerHTML={{ __html: syntaxHighlightJson(payload.state) }} />
                </pre>
              </section>
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
