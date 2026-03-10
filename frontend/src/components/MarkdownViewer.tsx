function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function inlineMarkdown(value: string): string {
  return value
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*]+)\*/g, "<em>$1</em>");
}

function markdownToHtml(source: string): string {
  const lines = source.split(/\r?\n/);
  const out: string[] = [];
  let inList = false;
  let inCode = false;

  for (const rawLine of lines) {
    const escaped = escapeHtml(rawLine);

    if (escaped.startsWith("```")) {
      if (!inCode) {
        out.push("<pre><code>");
      } else {
        out.push("</code></pre>");
      }
      inCode = !inCode;
      continue;
    }

    if (inCode) {
      out.push(`${escaped}\n`);
      continue;
    }

    if (escaped.startsWith("### ")) {
      if (inList) {
        out.push("</ul>");
        inList = false;
      }
      out.push(`<h3>${inlineMarkdown(escaped.slice(4))}</h3>`);
      continue;
    }

    if (escaped.startsWith("## ")) {
      if (inList) {
        out.push("</ul>");
        inList = false;
      }
      out.push(`<h2>${inlineMarkdown(escaped.slice(3))}</h2>`);
      continue;
    }

    if (escaped.startsWith("# ")) {
      if (inList) {
        out.push("</ul>");
        inList = false;
      }
      out.push(`<h1>${inlineMarkdown(escaped.slice(2))}</h1>`);
      continue;
    }

    if (escaped.startsWith("- ")) {
      if (!inList) {
        out.push("<ul>");
        inList = true;
      }
      out.push(`<li>${inlineMarkdown(escaped.slice(2))}</li>`);
      continue;
    }

    if (inList) {
      out.push("</ul>");
      inList = false;
    }

    if (escaped.trim()) {
      out.push(`<p>${inlineMarkdown(escaped)}</p>`);
    }
  }

  if (inList) {
    out.push("</ul>");
  }
  if (inCode) {
    out.push("</code></pre>");
  }

  return out.join("\n");
}

interface MarkdownViewerProps {
  markdown: string;
}

export function MarkdownViewer({ markdown }: MarkdownViewerProps) {
  const html = markdownToHtml(markdown || "");
  return <article className="markdown" dangerouslySetInnerHTML={{ __html: html }} />;
}
