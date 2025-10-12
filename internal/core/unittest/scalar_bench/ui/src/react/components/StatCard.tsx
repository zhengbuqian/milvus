export interface StatCardProps {
  label: string;
  value: string | number;
  color?: string;
}

export function StatCard({ label, value, color }: StatCardProps): JSX.Element {
  return (
    <div className="stat-card" style={{ padding: '1rem', background: '#f8f9fa', borderRadius: '4px' }}>
      <div style={{ fontSize: '0.85em', color: '#666', marginBottom: '0.25rem' }}>{label}</div>
      <div style={{ fontSize: '1.5em', fontWeight: 'bold', color }}>{value}</div>
    </div>
  );
}
