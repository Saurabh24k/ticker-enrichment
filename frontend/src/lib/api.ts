import type { PreviewRow } from "./types";

const BASE = (import.meta as any).env?.VITE_API_BASE ?? "http://localhost:8000";

export async function previewFile(file: File): Promise<PreviewRow[]> {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${BASE}/files/preview-file`, { method: "POST", body: fd });
  if (!res.ok) throw new Error(`Preview failed: ${res.status}`);
  return res.json();
}

export async function commitFile(file: File, overrides: Record<number, string>): Promise<Blob> {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("overrides_json", new Blob([JSON.stringify(overrides)], { type: "application/json" }));
  const res = await fetch(`${BASE}/files/commit-file`, { method: "POST", body: fd });
  if (!res.ok) throw new Error(`Commit failed: ${res.status}`);
  return res.blob();
}
