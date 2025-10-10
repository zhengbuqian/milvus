import { getBasePath } from './state';

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { cache: 'no-cache' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url, { cache: 'no-cache' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.text();
}

export function buildAssetUrl(path: string): string {
  const base = getBasePath().replace(/\/+$/, '');
  const rel = String(path || '').replace(/^\/+/, '');
  return `${base}/${rel}`;
}

export async function assetExists(path: string): Promise<boolean> {
  const url = buildAssetUrl(path);
  try {
    const res = await fetch(url, { method: 'HEAD', cache: 'no-cache' });
    if (res.ok) return true;
    // Some servers may not support HEAD; fall back to GET for small SVGs
    if (res.status === 405) {
      const getRes = await fetch(url, { method: 'GET', cache: 'no-cache' });
      return getRes.ok;
    }
    return false;
  } catch {
    return false;
  }
}

export async function getIndex(): Promise<any> {
  return fetchJson(buildAssetUrl('index.json'));
}

export async function getRunMeta(runId: string | number): Promise<any> {
  return fetchJson(buildAssetUrl(`${runId}/meta.json`));
}

export async function getRunMetrics(runId: string | number): Promise<any> {
  return fetchJson(buildAssetUrl(`${runId}/metrics.json`));
}

export async function getRunSummary(runId: string | number): Promise<string> {
  return fetchText(buildAssetUrl(`${runId}/run_summary.txt`));
}


