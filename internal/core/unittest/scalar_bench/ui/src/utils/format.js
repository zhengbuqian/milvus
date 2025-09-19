export function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function formatNumber(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return '—';
  if (typeof value !== 'number') value = Number(value);
  if (!Number.isFinite(value)) return String(value);
  if (Math.abs(value) >= 100) {
    return value.toFixed(0);
  }
  return value.toFixed(2);
}

export function formatPercentage(value) {
  if (value === undefined || value === null) return '—';
  if (typeof value !== 'number') value = Number(value);
  if (!Number.isFinite(value)) return String(value);
  return `${(value * 100).toFixed(2)}%`;
}


