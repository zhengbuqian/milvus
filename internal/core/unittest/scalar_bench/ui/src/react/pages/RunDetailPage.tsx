import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { getRunMeta, getRunMetrics, getRunSummary, buildAssetUrl } from '../utils/api';
import { formatTimestamp, getSelectedRuns, toggleRunSelection } from '../utils/state';
import { CasesTable, CaseRow, MetricDef } from '../components/CasesTable';
import { formatNumber, formatPercentage, escapeHtml } from '../utils/format';
import { FlamegraphSection } from '../components/Flamegraphs';

const METRIC_KEYS: MetricDef[] = [
  { key: 'qps', label: 'QPS', formatter: formatNumber, better: 'higher' },
  { key: 'latency_ms.avg', label: 'Avg ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p50', label: 'P50 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p90', label: 'P90 ms', formatter: formatNumber, better: 'lower' },
  { key: 'latency_ms.p99', label: 'P99 ms', formatter: formatNumber, better: 'lower' },
  { key: 'selectivity', label: 'Selectivity', formatter: formatPercentage },
  { key: 'index_build_ms', label: 'Index build ms', formatter: formatNumber, better: 'lower' },
];

export default function RunDetailPage(): JSX.Element {
  const { runId = '' } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [meta, setMeta] = useState<any>(null);
  const [metrics, setMetrics] = useState<any>(null);
  const [summaryText, setSummaryText] = useState<string>('');
  const [cols, setCols] = useState<number>(2);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    (async () => {
      try {
        const [m, mt, st] = await Promise.all([
          getRunMeta(runId),
          getRunMetrics(runId),
          getRunSummary(runId).catch(() => null),
        ]);
        if (!mounted) return;
        setMeta(m);
        setMetrics(mt);
        setSummaryText(st || 'run_summary.txt not found.');
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        setError(err?.message || String(err));
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [runId]);

  const rows: CaseRow[] = useMemo(() => {
    if (!metrics?.cases) return [];
    return Object.entries(metrics.cases).map(([caseId, data]: any) => ({
      runId,
      caseId: String(caseId),
      dataConfig: data.data_config,
      indexConfig: data.index_config,
      expression: data.expression,
      metrics: data,
    }));
  }, [metrics, runId]);

  const flameCards = useMemo(() => {
    if (!metrics?.cases) return [] as any[];
    return Object.entries(metrics.cases)
      .filter(([, data]: any) => data.flamegraph)
      .map(([caseId, data]: any) => ({ runId, caseId: String(caseId), values: data }));
  }, [metrics, runId]);

  if (loading) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All runs</button>
          <h2 style={{ margin: 0 }}>Run {runId}</h2>
        </div>
      </div>
      <div className="loading">Loading run details…</div>
    </div>
  );
  if (error) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All runs</button>
          <h2 style={{ margin: 0 }}>Run {runId}</h2>
        </div>
      </div>
      <div className="alert">{error}</div>
    </div>
  );

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All runs</button>
          <h2 style={{ margin: 0 }}>Run {runId}</h2>
        </div>
        <div className="right">
          <label className="badge" style={{ cursor: 'pointer' }}>
            <input type="checkbox" style={{ marginRight: '0.35rem' }} defaultChecked={getSelectedRuns().includes(String(runId))} onChange={(e) => toggleRunSelection(runId, e.target.checked)} />
            Select for compare
          </label>
        </div>
      </div>

      <div className="meta-grid">
        {renderMetaItem('Timestamp', formatTimestamp(meta.timestamp_ms || runId))}
        {renderMetaItem('Total cases', meta.summary?.total_cases ?? '—')}
        {renderMetaItem('Datasets', (meta.data_configs || []).join(', ') || '—')}
        {renderMetaItem('Indexes', (meta.index_configs || []).join(', ') || '—')}
        {renderMetaItem('Expressions', (meta.expressions || []).join(', ') || '—')}
        {renderMetaItem('Label', meta.label || '—')}
      </div>

      <div style={{ marginTop: '1.5rem' }}>
        <h3>Artifacts</h3>
        <div className="tag-list">
          {[
            { label: 'benchmark_results.csv', path: `${runId}/benchmark_results.csv` },
            { label: 'run_summary.txt', path: `${runId}/run_summary.txt` },
            { label: 'run_config.json', path: `${runId}/run_config.json` },
            { label: 'metrics.json', path: `${runId}/metrics.json` },
            { label: 'meta.json', path: `${runId}/meta.json` },
          ].map((f) => (
            <a key={f.label} href={buildAssetUrl(f.path)} className="tag" download>{f.label}</a>
          ))}
        </div>
      </div>

      <div style={{ marginTop: '1.5rem' }}>
        <h3>Run summary</h3>
        <div className="code-block"><pre>{summaryText}</pre></div>
      </div>

      <div style={{ marginTop: '1.5rem' }}>
        <h3>Case metrics</h3>
        <CasesTable
          rows={rows}
          metricKeys={METRIC_KEYS}
          showRunId={false}
          allowSelection={false}
          showFlamegraphLink
          buildFlamegraphUrl={(row) => (row.metrics as any).flamegraph ? buildAssetUrl(`${runId}/${(row.metrics as any).flamegraph}`) : null}
        />
      </div>

      {flameCards.length > 0 && (
        <FlamegraphSection cards={flameCards as any} cols={cols} setCols={setCols} />
      )}
    </div>
  );
}

function renderMetaItem(label: string, value: string): JSX.Element {
  return (
    <div className="meta-item">
      <span className="label">{label}</span>
      <span className="value" dangerouslySetInnerHTML={{ __html: escapeHtml(String(value)) }} />
    </div>
  );
}

