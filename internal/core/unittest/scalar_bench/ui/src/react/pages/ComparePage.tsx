import React, { useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getIndex, getRunMeta, getRunMetrics, buildAssetUrl } from '../utils/api';
import { CasesTable, CaseRow, MetricDef } from '../components/CasesTable';
import { formatNumber, formatPercentage } from '../utils/format';
import { FlamegraphSection } from '../components/Flamegraphs';

const METRIC_KEYS: MetricDef[] = [
  { key: 'qps', label: 'QPS', formatter: formatNumber, better: 'higher' },
  { key: 'latency_ms.avg', label: 'Avg ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p50', label: 'P50 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p90', label: 'P90 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p99', label: 'P99 ms', formatter: formatNumber, better: 'lower' },
  { key: 'selectivity', label: 'Selectivity', formatter: formatPercentage },
  { key: 'index_build_ms', label: 'Index build ms', formatter: formatNumber, better: 'lower' },
  { key: 'memory.index_mb', label: 'Index MB', formatter: formatNumber, better: 'lower' },
  { key: 'memory.exec_peak_mb', label: 'Exec peak MB', formatter: formatNumber, better: 'lower' },
  { key: 'cpu_pct', label: 'CPU %', formatter: formatNumber, better: 'lower' },
];

export default function ComparePage(): JSX.Element {
  const location = useLocation();
  const navigate = useNavigate();
  const params = new URLSearchParams(location.search);
  const runsParam = params.get('runs') || '';
  const casesParam = params.get('cases') || '';
  const colsParam = params.get('cols') || '';

  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [cols, setCols] = useState<number>(Math.max(1, Math.min(5, Number(colsParam) || 2)));

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const indexData = await getIndex();
        const allRuns = Array.isArray(indexData?.runs) ? indexData.runs : [];
        const selectedRunIds = deriveInitialRuns(runsParam, allRuns);
        const data = await Promise.all(selectedRunIds.map(async (runId) => {
          const [meta, metrics] = await Promise.all([getRunMeta(runId), getRunMetrics(runId)]);
          return { runId, meta, metrics };
        }));
        if (!mounted) return;
        setRuns(data);
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        setError(err?.message || String(err));
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [runsParam]);

  const cards = useMemo(() => buildCards(runs, casesParam), [runs, casesParam]);
  const tableRows: CaseRow[] = useMemo(() => buildRows(runs, casesParam), [runs, casesParam]);

  if (loading) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to runs</button></div>
      </div>
      <div className="loading">Preparing comparison…</div>
    </div>
  );
  if (error) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to runs</button></div>
      </div>
      <div className="alert">{error}</div>
    </div>
  );

  const runSelector = (
    <RunSelector
      allRuns={runs}
      activeRunIds={runs.map((r) => String(r.runId))}
      onChange={(selected) => {
        navigate(`/compare?runs=${selected.join(',')}${cols ? `&cols=${cols}` : ''}${casesParam ? `&cases=${encodeURIComponent(casesParam)}` : ''}`);
      }}
    />
  );

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to runs</button></div>
      </div>
      {runSelector}
      <div>
        <CasesTable
          rows={tableRows}
          metricKeys={METRIC_KEYS}
          showRunId
          allowSelection={false}
          showFlamegraphLink
          buildFlamegraphUrl={(row) => (row.metrics as any).flamegraph ? buildAssetUrl(`${row.runId}/${(row.metrics as any).flamegraph}`) : null}
        />
      </div>

      {cards.length > 0 && (
        <FlamegraphSection cards={cards as any} cols={cols} setCols={setCols} />
      )}
    </div>
  );
}

function deriveInitialRuns(paramRuns: string, runs: any[]): string[] {
  if (paramRuns) {
    const ids = paramRuns
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id && runs.find((run) => String(run.id) === id || String(run.runId) === id));
    if (ids.length) {
      return ids;
    }
  }
  if (runs.length) return runs.slice(0, 2).map((r) => String(r.id || r.runId));
  return [];
}

function buildRows(data: any[], casesParam: string): CaseRow[] {
  const active = casesParam ? new Set(casesParam.split(',').map(String)) : null;
  const rows: CaseRow[] = [];
  data.forEach(({ runId, meta, metrics }) => {
    const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
    cases.forEach(([caseId, values]: any) => {
      const key = `${runId}:${caseId}`;
      if (active && !active.has(key)) return;
      rows.push({
        runId: String(runId),
        caseId: String(caseId),
        dataConfig: values.data_config,
        indexConfig: values.index_config,
        expression: values.expression,
        metrics: values,
        timestamp: meta?.timestamp_ms || runId,
      });
    });
  });
  return rows;
}

function buildCards(data: any[], casesParam: string) {
  const active = casesParam ? new Set(casesParam.split(',').map(String)) : null;
  const cards: { runId: string; caseId: string; values: any }[] = [];
  data.forEach(({ runId, metrics }) => {
    const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
    cases.forEach(([caseId, values]: any) => {
      if (!values.flamegraph) return;
      const key = `${runId}:${caseId}`;
      if (active && !active.has(key)) return;
      cards.push({ runId: String(runId), caseId: String(caseId), values });
    });
  });
  return cards;
}

function RunSelector({ allRuns, activeRunIds, onChange }: { allRuns: any[]; activeRunIds: string[]; onChange: (selected: string[]) => void }): JSX.Element {
  return (
    <div className="section-card">
      <h3 style={{ marginTop: 0 }}>Runs</h3>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
        {allRuns
          .slice()
          .sort((a, b) => Number((b.meta?.timestamp_ms || b.runId || 0)) - Number((a.meta?.timestamp_ms || a.runId || 0)))
          .map((run) => {
            const id = String(run.runId || run.id);
            const checked = activeRunIds.includes(id);
            return (
              <label key={id} className="badge" style={{ justifyContent: 'space-between', cursor: 'pointer' }}>
                <span>
                  <input type="checkbox" checked={checked} onChange={(e) => {
                    const next = new Set(activeRunIds);
                    if (e.target.checked) next.add(id); else next.delete(id);
                    onChange(Array.from(next));
                  }} />
                  <strong style={{ marginLeft: '0.35rem' }}>{id}</strong>
                </span>
                <span className="text-muted small">{String(run.meta?.timestamp_ms || id)}</span>
              </label>
            );
          })}
      </div>
    </div>
  );
}

