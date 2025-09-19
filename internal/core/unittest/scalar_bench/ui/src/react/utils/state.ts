export const DEFAULT_BASE_PATH = '/_artifacts/results/';

const BASE_PATH_KEY = 'scalar-bench:base-path';
const SELECTED_RUNS_KEY = 'scalar-bench:selected-runs';
const SELECTED_CASES_KEY = 'scalar-bench:selected-cases';

function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function writeJson<T>(key: string, value: T): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // ignore
  }
}

export function getBasePath(): string {
  const v = readJson<string | null>(BASE_PATH_KEY, null);
  return v || DEFAULT_BASE_PATH;
}

export function setBasePath(path: string): void {
  writeJson(BASE_PATH_KEY, path);
  const evt = new CustomEvent<string>('scalar-bench:base-path-changed', { detail: path });
  document.dispatchEvent(evt);
}

export function getSelectedRuns(): string[] {
  return readJson<string[]>(SELECTED_RUNS_KEY, []);
}

export function toggleRunSelection(runId: string | number, selected: boolean): void {
  const id = String(runId);
  const set = new Set(getSelectedRuns());
  if (selected) set.add(id); else set.delete(id);
  const next = Array.from(set);
  writeJson(SELECTED_RUNS_KEY, next);
  document.dispatchEvent(new Event('scalar-bench:selected-runs-changed'));
}

export function getSelectedCases(): string[] {
  return readJson<string[]>(SELECTED_CASES_KEY, []);
}

export function toggleCaseSelection(runId: string | number, caseId: string | number, selected: boolean): void {
  const key = `${runId}:${caseId}`;
  const set = new Set(getSelectedCases());
  if (selected) set.add(key); else set.delete(key);
  const next = Array.from(set);
  writeJson(SELECTED_CASES_KEY, next);
  document.dispatchEvent(new Event('scalar-bench:selected-cases-changed'));
}

export { formatTimestamp } from './format';


