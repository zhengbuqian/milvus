import type { MetricDef } from '../components/ExecutionsTable';
import { formatNumber, formatPercentage } from './format';

export const METRIC_KEYS: MetricDef[] = [
  { key: 'qps', label: 'QPS', formatter: formatNumber, better: 'higher' },
  { key: 'latency_ms.avg', label: 'Avg ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p50', label: 'P50 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p90', label: 'P90 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p99', label: 'P99 ms', formatter: formatNumber, better: 'lower' },
  { key: 'selectivity', label: 'Selectivity', formatter: formatPercentage },
  { key: 'index_build_ms', label: 'Index build ms', formatter: formatNumber, better: 'lower' },
];

export const COMPARE_METRIC_KEYS: MetricDef[] = [
  ...METRIC_KEYS,
  { key: 'memory.index_mb', label: 'Index MB', formatter: formatNumber, better: 'lower' },
  { key: 'memory.exec_peak_mb', label: 'Exec peak MB', formatter: formatNumber, better: 'lower' },
  { key: 'cpu_pct', label: 'CPU %', formatter: formatNumber, better: 'lower' },
];
