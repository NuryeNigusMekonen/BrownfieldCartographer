import type { ReactNode } from "react";

interface AppShellProps {
  sidebar: ReactNode;
  header: ReactNode;
  children: ReactNode;
  inspector: ReactNode;
}

export function AppShell({ sidebar, header, children, inspector }: AppShellProps) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-scroll">{sidebar}</div>
      </aside>
      <main className="workspace">
        <div className="workspace-header">{header}</div>
        <section className="workspace-content">{children}</section>
      </main>
      <aside className="inspector">
        <div className="inspector-scroll">{inspector}</div>
      </aside>
    </div>
  );
}
