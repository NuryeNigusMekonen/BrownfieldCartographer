import type { ReactNode } from "react";

interface ArtifactLoaderProps {
  loading: boolean;
  error: string;
  emptyMessage?: string;
  hasData: boolean;
  children: ReactNode;
}

export function ArtifactLoader({ loading, error, emptyMessage, hasData, children }: ArtifactLoaderProps) {
  if (loading) {
    return <section className="panel loading">Loading artifacts...</section>;
  }
  if (error) {
    return <section className="panel error">{error}</section>;
  }
  if (!hasData) {
    return <section className="panel empty">{emptyMessage ?? "No artifacts found for this repository."}</section>;
  }
  return <>{children}</>;
}
