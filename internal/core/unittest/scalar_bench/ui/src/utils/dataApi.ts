import { IndexJson, MetricsJson, RunMeta } from './types'

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { cache: 'no-store' })
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
  return response.json() as Promise<T>
}

export async function fetchIndex(): Promise<IndexJson> {
  // index.json is expected at the same directory level as the app root
  return getJson<IndexJson>(`./index.json`)
}

export async function fetchRunMeta(runId: string): Promise<RunMeta> {
  return getJson<RunMeta>(`./runs/${encodeURIComponent(runId)}/meta.json`)
}

export async function fetchRunMetrics(runId: string): Promise<MetricsJson> {
  return getJson<MetricsJson>(`./runs/${encodeURIComponent(runId)}/metrics.json`)
}

export function resolveRunAssetUrl(runId: string, subPath: string): string {
  return `./runs/${encodeURIComponent(runId)}/${subPath}`
}

