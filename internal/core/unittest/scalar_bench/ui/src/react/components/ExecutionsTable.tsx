import React, { useMemo, useState } from 'react';
import { escapeHtml } from '../utils/format';
import { getNestedValue } from '../utils/helpers';

export type MetricDef = {
  key: string;
  label: string;
  formatter?: (v: unknown) => string;
  better?: 'higher' | 'lower';
};

export type ExecutionRow = {
  runId?: string;
  testId: string;
  dataConfig?: string;
  indexConfig?: string;
  expression?: string;
  metrics: Record<string, any>;
  timestamp?: string | number;
};

export function ExecutionsTable({
  rows,
  metricKeys,
  showRunId = false,
  allowSelection = false,
  isSelected = () => false,
  getSelectionKey = (row: ExecutionRow) => String(row.testId),
  onToggleSelect = () => {},
  showFlamegraphLink = true,
  buildFlamegraphUrl = () => null,
}: {
  rows: ExecutionRow[];
  metricKeys: MetricDef[];
  showRunId?: boolean;
  allowSelection?: boolean;
  isSelected?: (key: string) => boolean;
  getSelectionKey?: (row: ExecutionRow) => string;
  onToggleSelect?: (key: string, checked: boolean, row: ExecutionRow) => void;
  showFlamegraphLink?: boolean;
  buildFlamegraphUrl?: (row: ExecutionRow) => string | null;
}): JSX.Element {
  const [currentMetric, setCurrentMetric] = useState<string>('');
  const [currentOrder, setCurrentOrder] = useState<'asc' | 'desc'>('desc');

  const sorted = useMemo(() => sortExecutionsGrouped(rows, currentMetric, currentOrder), [rows, currentMetric, currentOrder]);

  const colorScales = useMemo(() => buildColorScales(rows, metricKeys), [rows, metricKeys]);

  return (
    <div className="table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            {allowSelection && <th></th>}
            {showRunId && <th>Run</th>}
            <th>Test</th>
            <th>Dataset</th>
            <th>Expression</th>
            <th>Index</th>
            {metricKeys.map((m) => {
              const isActive = m.key === currentMetric;
              const arrow = isActive ? (currentOrder === 'asc' ? ' ↑' : ' ↓') : '';
              return (
                <th
                  key={m.key}
                  className="numeric"
                  data-metric-key={m.key}
                  style={{ cursor: 'pointer', background: isActive ? 'rgba(56,189,248,0.12)' : undefined }}
                  title={m.key}
                  onClick={() => {
                    if (isActive) setCurrentOrder(currentOrder === 'asc' ? 'desc' : 'asc');
                    else { setCurrentMetric(m.key); setCurrentOrder('desc'); }
                  }}
                >
                  {m.label}{arrow}
                </th>
              );
            })}
            {showFlamegraphLink && <th>Flamegraph</th>}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, idx) => {
            const key = getSelectionKey(row);
            const selected = isSelected(key);
            return (
              <tr
                key={idx}
                onClick={() => {
                  if (allowSelection) onToggleSelect(key, !selected, row);
                }}
                style={allowSelection ? { cursor: 'pointer' } : undefined}
              >
                {allowSelection && (
                  <td onClick={(e) => { e.stopPropagation(); onToggleSelect(key, !selected, row); }}>
                    <input
                      type="checkbox"
                      checked={selected}
                      onClick={(e) => e.stopPropagation()}
                      onChange={(e) => onToggleSelect(key, e.target.checked, row)}
                    />
                  </td>
                )}
                {showRunId && <td>{row.runId}</td>}
                <td>{row.testId}</td>
                <td>{row.dataConfig ?? '—'}</td>
                <td>
                  <code dangerouslySetInnerHTML={{ __html: escapeHtml(row.expression ?? '—') }} />
                </td>
                <td>{row.indexConfig ?? '—'}</td>
                {metricKeys.map((m) => {
                  const value = getNestedValue(row.metrics, m.key);
                  const formatted = m.formatter ? m.formatter(value) : String(value);
                  return (
                    <td
                      className="numeric"
                      key={m.key + idx}
                      style={computeCellStyle(value, m, colorScales)}
                      title={typeof value === 'number' ? String(value) : undefined}
                    >{formatted}</td>
                  );
                })}
                {showFlamegraphLink && (
                  <td>
                    {(() => {
                      const url = buildFlamegraphUrl(row);
                      return url ? (
                        <a href={url} target="_blank" onClick={(e) => e.stopPropagation()}>View</a>
                      ) : (
                        '—'
                      );
                    })()}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function sortExecutionsGrouped(rows: ExecutionRow[], metricKey: string, order: 'asc' | 'desc'): ExecutionRow[] {
  const groups = new Map<string, { dataset: string; expression: string; items: ExecutionRow[] }>();
  rows.forEach((r) => {
    const dataset = r.dataConfig || '';
    const expr = r.expression || '';
    const key = `${dataset}\u0000${expr}`;
    if (!groups.has(key)) groups.set(key, { dataset, expression: expr, items: [] });
    groups.get(key)!.items.push(r);
  });

  const groupList = Array.from(groups.values());
  groupList.sort((a, b) => {
    const ds = String(a.dataset).localeCompare(String(b.dataset));
    if (ds !== 0) return ds;
    return String(a.expression).localeCompare(String(b.expression));
  });

  const dir = order === 'asc' ? 1 : -1;
  const result: ExecutionRow[] = [];
  groupList.forEach((g) => {
    if (metricKey) {
      g.items.sort((a, b) => {
        const av = Number(getNestedValue(a.metrics, metricKey));
        const bv = Number(getNestedValue(b.metrics, metricKey));
        if (Number.isNaN(av) && Number.isNaN(bv)) return 0;
        if (Number.isNaN(av)) return 1;
        if (Number.isNaN(bv)) return -1;
        return dir * (av - bv);
      });
    } else {
      g.items.sort((a, b) => Number(b.testId) - Number(a.testId));
    }
    result.push(...g.items);
  });
  return result;
}

function buildColorScales(rows: ExecutionRow[], metricDefs: MetricDef[]): Record<string, { min: number; max: number; better: 'higher' | 'lower' } | null> {
  const map: Record<string, { min: number; max: number; better: 'higher' | 'lower' } | null> = {};
  metricDefs.forEach((m) => {
    if (!m.better) { map[m.key] = null; return; }
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    rows.forEach((r) => {
      const v = Number(getNestedValue(r.metrics, m.key));
      if (Number.isFinite(v)) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
    });
    if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) {
      map[m.key] = null;
    } else {
      map[m.key] = { min, max, better: m.better };
    }
  });
  return map;
}

function computeCellStyle(value: unknown, m: MetricDef, scales: Record<string, { min: number; max: number; better: 'higher' | 'lower' } | null>): React.CSSProperties | undefined {
  const scale = scales[m.key];
  const num = Number(value);
  if (!scale || !Number.isFinite(num)) return undefined;
  const { min, max, better } = scale;
  const t = (num - min) / (max - min);
  const score = better === 'higher' ? 1 - t : t;
  const alpha = Math.max(0.08, Math.min(0.40, 0.08 + score * 0.32));
  return { background: `rgba(56, 189, 248, ${alpha})` };
}

