import { StatCard } from './StatCard';
import type { SummaryStats as StatsType } from '../utils/helpers';

export interface SummaryStatsProps {
  stats: StatsType;
}

export function SummaryStats({ stats }: SummaryStatsProps): JSX.Element {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem' }}>
      <StatCard label="Total Tests" value={stats.count} />
      <StatCard label="Avg QPS" value={stats.avgQps.toFixed(2)} />
      <StatCard label="Max QPS" value={stats.maxQps.toFixed(2)} color="#4CAF50" />
      <StatCard label="Avg Latency" value={`${stats.avgLatency.toFixed(3)} ms`} />
      <StatCard label="Min Latency" value={`${stats.minLatency.toFixed(3)} ms`} color="#2196F3" />
    </div>
  );
}
