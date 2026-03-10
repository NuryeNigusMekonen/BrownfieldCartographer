import { useMemo, useState } from "react";
import { parseNavigatorQuery } from "../utils";

interface QueryInputProps {
  disabled?: boolean;
  examples: string[];
  onSubmit: (query: string) => Promise<void>;
}

export function QueryInput({ disabled, examples, onSubmit }: QueryInputProps) {
  const [query, setQuery] = useState(examples[0] ?? "");
  const [running, setRunning] = useState(false);
  const parsed = useMemo(() => parseNavigatorQuery(query), [query]);

  async function run() {
    const trimmed = query.trim();
    if (!trimmed || running || disabled) {
      return;
    }
    setRunning(true);
    try {
      await onSubmit(trimmed);
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Navigator</p>
          <h3>Query Console</h3>
        </div>
      </div>
      <div className="query-row">
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          disabled={disabled || running}
          placeholder="explain_module src/path.py"
        />
        <button className="primary-action" onClick={run} disabled={disabled || running}>
          {running ? "Running..." : "Run Query"}
        </button>
      </div>
      <p className="muted query-parse">
        Parsed: <strong>{parsed.tool || "-"}</strong> <span>{parsed.arg || "-"}</span>
      </p>
      <div className="pill-row">
        {examples.map((example) => (
          <button key={example} onClick={() => setQuery(example)} disabled={disabled || running}>
            {example}
          </button>
        ))}
      </div>
    </section>
  );
}
