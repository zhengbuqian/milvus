export function escapeHtml(input: unknown): string {
  const text = String(input ?? '');
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function formatNumber(value: unknown): string {
  const num = Number(value);
  if (!Number.isFinite(num)) return '—';
  if (Math.abs(num) >= 1000) return num.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (Math.abs(num) >= 100) return num.toFixed(1);
  if (Math.abs(num) >= 10) return num.toFixed(2);
  if (Math.abs(num) >= 1) return num.toFixed(3);
  return num.toPrecision(3);
}

export function formatPercentage(value: unknown): string {
  const num = Number(value);
  if (!Number.isFinite(num)) return '—';
  return `${(num * 100).toFixed(2)}%`;
}

export function formatTimestamp(value: unknown): string {
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value ?? '—');
  const date = new Date(num);
  if (Number.isNaN(date.getTime())) return String(value ?? '—');
  return date.toLocaleString();
}


