import React, { useMemo, useState } from 'react';
import { escapeHtml } from '../lib/format';

export type MetricDef = { key: string; label: string; formatter?: (v: unknown) => string };

export type CaseRow = {
  runId?: string;
  caseId: string;
  dataConfig?: string;
  indexConfig?: string;
  expression?: string;
  metrics: Record<string, any>;
  timestamp?: string | number;
};

export function CasesTable({
  rows,
  metricKeys,
  showRunId = false,
  allowSelection = false,
  isSelected = () => false,
  getSelectionKey = (row: CaseRow) => String(row.caseId),
  onToggleSelect = () => {},
  showFlamegraphLink = true,
  buildFlamegraphUrl = () => null,
}: {
  rows: CaseRow[];
  metricKeys: MetricDef[];
  showRunId?: boolean;
  allowSelection?: boolean;
  isSelected?: (key: string) => boolean;
  getSelectionKey?: (row: CaseRow) => string;
  onToggleSelect?: (key: string, checked: boolean, row: CaseRow) => void;
  showFlamegraphLink?: boolean;
  buildFlamegraphUrl?: (row: CaseRow) => string | null;
}): JSX.Element {
  const [currentMetric, setCurrentMetric] = useState<string>('');
  const [currentOrder, setCurrentOrder] = useState<'asc' | 'desc'>('desc');

  const sorted = useMemo(() => sortRowsGrouped(rows, currentMetric, currentOrder), [rows, currentMetric, currentOrder]);

  return (
    <div className="table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            {allowSelection && <th></th>}
            {showRunId && <th>Run</th>}
            <th>Case</th>
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
            return (
              <tr key={idx}>
                {allowSelection && (
                  <td>
                    <input
                      type="checkbox"
                      checked={isSelected(key)}
                      onChange={(e) => onToggleSelect(key, e.target.checked, row)}
                    />
                  </td>
                )}
                {showRunId && <td>{row.runId}</td>}
                <td>{row.caseId}</td>
                <td>{row.dataConfig ?? '—'}</td>
                <td>
                  <code dangerouslySetInnerHTML={{ __html: escapeHtml(row.expression ?? '—') }} />
                </td>
                <td>{row.indexConfig ?? '—'}</td>
                {metricKeys.map((m) => {
                  const value = getMetricValue(row.metrics, m.key);
                  const formatted = m.formatter ? m.formatter(value) : String(value);
                  return (
                    <td className="numeric" key={m.key + idx}>{formatted}</td>
                  );
                })}
                {showFlamegraphLink && (
                  <td>
                    {(() => {
                      const url = buildFlamegraphUrl(row);
                      return url ? (
                        <a href={url} target="_blank">View</a>
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

function getMetricValue(obj: any, path: string): any {
  return path ? path.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj) : undefined;
}

function sortRowsGrouped(rows: CaseRow[], metricKey: string, order: 'asc' | 'desc'): CaseRow[] {
  const groups = new Map<string, { dataset: string; expression: string; items: CaseRow[] }>();
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
  const result: CaseRow[] = [];
  groupList.forEach((g) => {
    if (metricKey) {
      g.items.sort((a, b) => {
        const av = Number(getMetricValue(a.metrics, metricKey));
        const bv = Number(getMetricValue(b.metrics, metricKey));
        if (Number.isNaN(av) && Number.isNaN(bv)) return 0;
        if (Number.isNaN(av)) return 1;
        if (Number.isNaN(bv)) return -1;
        return dir * (av - bv);
      });
    } else {
      g.items.sort((a, b) => Number(b.caseId) - Number(a.caseId));
    }
    result.push(...g.items);
  });
  return result;
}

