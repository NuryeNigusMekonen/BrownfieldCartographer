export interface ParsedNavigatorQuery {
  tool: string;
  arg: string;
  direction: "upstream" | "downstream";
}

export function parseNavigatorQuery(input: string): ParsedNavigatorQuery {
  const text = input.trim();
  if (!text) {
    return { tool: "", arg: "", direction: "upstream" };
  }

  const lowered = text.toLowerCase();
  const direct = text.match(/^(explain_module|blast_radius|find_implementation|trace_lineage)\s+(.+)$/i);
  if (direct) {
    return {
      tool: direct[1].toLowerCase(),
      arg: direct[2].trim(),
      direction: "upstream",
    };
  }

  if (lowered.startsWith("find implementation of ")) {
    return { tool: "find_implementation", arg: text.slice(23).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("find implementation ")) {
    return { tool: "find_implementation", arg: text.slice(20).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("trace downstream of ")) {
    return { tool: "trace_lineage", arg: text.slice(20).trim(), direction: "downstream" };
  }
  if (lowered.startsWith("trace downstream ")) {
    return { tool: "trace_lineage", arg: text.slice(17).trim(), direction: "downstream" };
  }
  if (lowered.startsWith("trace lineage of ")) {
    return { tool: "trace_lineage", arg: text.slice(17).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("trace lineage ")) {
    return { tool: "trace_lineage", arg: text.slice(14).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("compute blast radius of ")) {
    return { tool: "blast_radius", arg: text.slice(24).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("blast radius ")) {
    return { tool: "blast_radius", arg: text.slice(13).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("explain module ")) {
    return { tool: "explain_module", arg: text.slice(15).trim(), direction: "upstream" };
  }
  if (lowered.startsWith("explain ")) {
    return { tool: "explain_module", arg: text.slice(8).trim(), direction: "upstream" };
  }

  return { tool: "", arg: "", direction: "upstream" };
}

export function toToolQuery(input: string): string {
  const parsed = parseNavigatorQuery(input);
  if (!parsed.tool || !parsed.arg) {
    return input;
  }
  if (parsed.tool === "trace_lineage" && parsed.direction === "downstream") {
    return `trace downstream ${parsed.arg}`;
  }
  return `${parsed.tool} ${parsed.arg}`;
}

export function formatValue(value: unknown): string {
  if (value == null) {
    return "-";
  }
  if (Array.isArray(value)) {
    return value.map((item) => (typeof item === "string" ? item : JSON.stringify(item))).join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
}

export function toTitle(value: string): string {
  return value
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(" ");
}
