import { getBasePath } from './state.js';

export async function fetchJSON(relativePath) {
  const base = getBasePath();
  const url = new URL(relativePath, window.location.origin);
  url.pathname = normalizePath(base, relativePath);

  try {
    const response = await fetch(url.pathname, { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return await response.json();
  } catch (error) {
    throw new Error(`Failed to load ${relativePath}: ${error.message}`);
  }
}

export async function fetchText(relativePath) {
  const base = getBasePath();
  const url = new URL(relativePath, window.location.origin);
  url.pathname = normalizePath(base, relativePath);
  try {
    const response = await fetch(url.pathname, { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return await response.text();
  } catch (error) {
    throw new Error(`Failed to load ${relativePath}: ${error.message}`);
  }
}

export function buildAssetUrl(relativePath) {
  return normalizePath(getBasePath(), relativePath);
}

export async function getIndex() {
  return fetchJSON('index.json');
}

export async function getRunMeta(runId) {
  return fetchJSON(`${runId}/meta.json`);
}

export async function getRunMetrics(runId) {
  return fetchJSON(`${runId}/metrics.json`);
}

export async function getRunSummary(runId) {
  return fetchText(`${runId}/run_summary.txt`);
}

export async function getRunConfig(runId) {
  return fetchJSON(`${runId}/run_config.json`);
}

export function normalizePath(base, relative) {
  const cleanBase = base.endsWith('/') ? base : `${base}/`;
  const trimmed = relative.startsWith('/') ? relative.slice(1) : relative;
  const link = document.createElement('a');
  link.href = cleanBase + trimmed;
  return link.pathname;
}