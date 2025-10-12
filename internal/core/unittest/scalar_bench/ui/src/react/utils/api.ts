import { getBasePath } from './state';
import type {
  IndexData,
  BundleInfo,
  BundleMeta,
  CaseMeta,
  CaseMetrics,
} from '../types/bundle';

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

// ============================================================================
// Bundle-Case API functions
// ============================================================================

/**
 * Get all bundles from index.json
 * @returns Array of bundle information
 */
export async function getBundles(): Promise<BundleInfo[]> {
  const indexData = await fetchJson<IndexData>(buildAssetUrl('index.json'));
  return indexData.bundles || [];
}

/**
 * Get bundle metadata
 * @param bundleId - The bundle ID (timestamp)
 * @returns Bundle metadata including config, test params, and cases
 */
export async function getBundleMeta(bundleId: string): Promise<BundleMeta> {
  return fetchJson<BundleMeta>(buildAssetUrl(`${bundleId}/bundle_meta.json`));
}

/**
 * Get bundle summary text
 * @param bundleId - The bundle ID (timestamp)
 * @returns Plain text summary of the bundle
 */
export async function getBundleSummary(bundleId: string): Promise<string> {
  return fetchText(buildAssetUrl(`${bundleId}/bundle_summary.txt`));
}

/**
 * Get case metadata
 * @param bundleId - The bundle ID (timestamp)
 * @param caseId - The case ID (format: bundleId_index)
 * @returns Case metadata including suites and test counts
 */
export async function getCaseMeta(bundleId: string, caseId: string): Promise<CaseMeta> {
  return fetchJson<CaseMeta>(buildAssetUrl(`${bundleId}/cases/${caseId}/case_meta.json`));
}

/**
 * Get case metrics (all test results)
 * @param bundleId - The bundle ID (timestamp)
 * @param caseId - The case ID (format: bundleId_index)
 * @returns Case metrics containing all test results
 */
export async function getCaseMetrics(bundleId: string, caseId: string): Promise<CaseMetrics> {
  return fetchJson<CaseMetrics>(buildAssetUrl(`${bundleId}/cases/${caseId}/case_metrics.json`));
}

/**
 * Get case summary text
 * @param bundleId - The bundle ID (timestamp)
 * @param caseId - The case ID (format: bundleId_index)
 * @returns Plain text summary of the case
 */
export async function getCaseSummary(bundleId: string, caseId: string): Promise<string> {
  return fetchText(buildAssetUrl(`${bundleId}/cases/${caseId}/case_summary.txt`));
}

/**
 * Build URL for case-level assets (flamegraphs, etc.)
 * @param bundleId - The bundle ID (timestamp)
 * @param caseId - The case ID (format: bundleId_index)
 * @param assetPath - Relative path within the case directory (e.g., "flamegraphs/0001.svg")
 * @returns Full URL to the asset
 */
export function buildCaseAssetUrl(bundleId: string, caseId: string, assetPath: string): string {
  const path = `${bundleId}/cases/${caseId}/${assetPath}`;
  return buildAssetUrl(path);
}


