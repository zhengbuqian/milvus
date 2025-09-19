const BASE_PATH_KEY = 'scalarBenchBasePath';
const SELECTED_RUNS_KEY = 'scalarBenchSelectedRuns';
const SELECTED_CASES_KEY = 'scalarBenchSelectedCases'; // stored as array of strings: "<runId>:<caseId>"

const DEFAULT_BASE_PATH = '../_artifacts/results/';

let basePath = ensureTrailingSlash(
  window.localStorage.getItem(BASE_PATH_KEY) || DEFAULT_BASE_PATH,
);

let selectedRuns = new Set();
try {
  const stored = JSON.parse(window.localStorage.getItem(SELECTED_RUNS_KEY) || '[]');
  if (Array.isArray(stored)) {
    stored.filter(Boolean).forEach((id) => selectedRuns.add(String(id)));
  }
} catch (err) {
  console.warn('Failed to restore selected runs:', err);
}

function ensureTrailingSlash(value) {
  if (!value) return '';
  return value.endsWith('/') ? value : `${value}/`;
}

export function getBasePath() {
  return basePath;
}

export function setBasePath(newPath) {
  basePath = ensureTrailingSlash(newPath || '');
  window.localStorage.setItem(BASE_PATH_KEY, basePath);
  document.dispatchEvent(new CustomEvent('scalar-bench:base-path-changed', { detail: basePath }));
}

export function getSelectedRuns() {
  return Array.from(selectedRuns);
}

export function toggleRunSelection(runId, force) {
  const id = String(runId);
  const shouldSelect = typeof force === 'boolean' ? force : !selectedRuns.has(id);
  if (shouldSelect) {
    selectedRuns.add(id);
  } else {
    selectedRuns.delete(id);
  }
  persistSelection();
  document.dispatchEvent(new CustomEvent('scalar-bench:selected-runs-changed', { detail: getSelectedRuns() }));
}

export function setSelectedRuns(runIds) {
  selectedRuns = new Set((runIds || []).map(String));
  persistSelection();
  document.dispatchEvent(new CustomEvent('scalar-bench:selected-runs-changed', { detail: getSelectedRuns() }));
}

function persistSelection() {
  window.localStorage.setItem(SELECTED_RUNS_KEY, JSON.stringify(Array.from(selectedRuns)));
}

// Case-level selection
let selectedCases = new Set();
try {
  const stored = JSON.parse(window.localStorage.getItem(SELECTED_CASES_KEY) || '[]');
  if (Array.isArray(stored)) {
    stored.filter(Boolean).forEach((id) => selectedCases.add(String(id)));
  }
} catch (err) {
  console.warn('Failed to restore selected cases:', err);
}

export function getSelectedCases() {
  return Array.from(selectedCases);
}

export function toggleCaseSelection(runId, caseId, force) {
  const key = `${runId}:${caseId}`;
  const shouldSelect = typeof force === 'boolean' ? force : !selectedCases.has(key);
  if (shouldSelect) {
    selectedCases.add(key);
  } else {
    selectedCases.delete(key);
  }
  persistCaseSelection();
  document.dispatchEvent(new CustomEvent('scalar-bench:selected-cases-changed', { detail: getSelectedCases() }));
}

export function setSelectedCases(keys) {
  selectedCases = new Set((keys || []).map(String));
  persistCaseSelection();
  document.dispatchEvent(new CustomEvent('scalar-bench:selected-cases-changed', { detail: getSelectedCases() }));
}

function persistCaseSelection() {
  window.localStorage.setItem(SELECTED_CASES_KEY, JSON.stringify(Array.from(selectedCases)));
}

export function formatTimestamp(ms) {
  if (!ms) return '—';
  const date = new Date(Number(ms));
  if (Number.isNaN(date.getTime())) {
    return String(ms);
  }
  return `${date.toISOString().replace('T', ' ').replace('Z', ' UTC')}`;
}

export function clearState() {
  setSelectedRuns([]);
}

export const defaults = {
  basePath: DEFAULT_BASE_PATH,
};