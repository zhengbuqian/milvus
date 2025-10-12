export function getRelativeConfigPath(configFile: string): string {
  const parts = configFile.split('/');
  const benchCasesIndex = parts.indexOf('bench_cases');
  if (benchCasesIndex >= 0) {
    return parts.slice(benchCasesIndex).join('/');
  }
  return configFile;
}

export function getNestedValue(obj: any, path: string): any {
  return path ? path.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj) : undefined;
}

export interface SummaryStats {
  count: number;
  avgQps: number;
  avgLatency: number;
  maxQps: number;
  minLatency: number;
}

export function calculateStats(tests: Array<{ qps: number; latency_ms: { avg: number } }>): SummaryStats | null {
  if (!tests || tests.length === 0) return null;

  const avgQps = tests.reduce((sum, t) => sum + t.qps, 0) / tests.length;
  const avgLatency = tests.reduce((sum, t) => sum + t.latency_ms.avg, 0) / tests.length;
  const maxQps = Math.max(...tests.map(t => t.qps));
  const minLatency = Math.min(...tests.map(t => t.latency_ms.avg));

  return { avgQps, avgLatency, maxQps, minLatency, count: tests.length };
}
