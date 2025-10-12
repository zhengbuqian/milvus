import React, { useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getBundles, getBundleMeta, getCaseMetrics, buildCaseAssetUrl } from '../utils/api';
import { ExecutionsTable, ExecutionRow } from '../components/ExecutionsTable';
import { FlamegraphSection, FlameCard } from '../components/Flamegraphs';
import DatasetCharts from '../components/DatasetCharts';
import { COMPARE_METRIC_KEYS } from '../utils/constants';
import type { BundleInfo, BundleMeta, CaseMetrics } from '../types/bundle';

type BundleData = {
  bundleId: string;
  meta: BundleMeta;
  allCaseMetrics: Map<string, CaseMetrics>;
};

export default function ComparePage(): JSX.Element {
  const location = useLocation();
  const navigate = useNavigate();
  const params = new URLSearchParams(location.search);
  const runsParam = params.get('runs') || '';
  const casesParam = params.get('cases') || '';
  const colsParam = params.get('cols') || '';

  const [bundles, setBundles] = useState<BundleData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [cols, setCols] = useState<number>(Math.max(1, Math.min(5, Number(colsParam) || 2)));

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const allBundles = await getBundles();
        const selectedBundleIds = deriveInitialBundles(runsParam, allBundles);
        const data = await Promise.all(selectedBundleIds.map(async (bundleId) => {
          const meta = await getBundleMeta(bundleId);

          // Load all case metrics for this bundle
          const metricsMap = new Map<string, CaseMetrics>();
          await Promise.all(
            meta.cases.map(async (caseInfo) => {
              try {
                const metrics = await getCaseMetrics(bundleId, caseInfo.case_id);
                metricsMap.set(caseInfo.case_id, metrics);
              } catch (err) {
                console.error(`Failed to load metrics for case ${caseInfo.case_id}:`, err);
              }
            })
          );

          return { bundleId, meta, allCaseMetrics: metricsMap };
        }));
        if (!mounted) return;
        setBundles(data);
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        setError(err?.message || String(err));
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [runsParam]);

  const cards: FlameCard[] = useMemo(() => buildCards(bundles, casesParam), [bundles, casesParam]);
  const tableRows: ExecutionRow[] = useMemo(() => buildRows(bundles, casesParam), [bundles, casesParam]);
  const hasFlameByMeta = useMemo(() => bundles.some((b) => b.meta.test_params.enable_flame_graph), [bundles]);
  const hasAnyFlamegraph = useMemo(() => tableRows.some((r) => Boolean((r.metrics as any)?.flamegraph)), [tableRows]);

  if (loading) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to bundles</button></div>
      </div>
      <div className="loading">Preparing comparison…</div>
    </div>
  );
  if (error) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to bundles</button></div>
      </div>
      <div className="alert">{error}</div>
    </div>
  );

  const bundleSelector = (
    <BundleSelector
      allBundles={bundles}
      activeBundleIds={bundles.map((b) => b.bundleId)}
      onChange={(selected) => {
        navigate(`/compare?runs=${selected.join(',')}${cols ? `&cols=${cols}` : ''}${casesParam ? `&cases=${encodeURIComponent(casesParam)}` : ''}`);
      }}
    />
  );

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left"><h2 style={{ margin: 0 }}>Compare</h2></div>
        <div className="right"><button className="ghost" onClick={() => navigate('/runs')}>← Back to bundles</button></div>
      </div>
      {bundleSelector}
      <div style={{ marginTop: '1.5rem' }}>
        <h3>Charts</h3>
        <DatasetCharts rows={tableRows} />
      </div>
      <div>
        <ExecutionsTable
          rows={tableRows}
          metricKeys={COMPARE_METRIC_KEYS}
          showRunId
          allowSelection={false}
          showFlamegraphLink={hasFlameByMeta && hasAnyFlamegraph}
          buildFlamegraphUrl={(row) => {
            const flamegraph = (row.metrics as any).flamegraph;
            if (!flamegraph) return null;
            const match = flamegraph.match(/cases\/([^/]+)\//);
            const caseId = match ? match[1] : '';
            return buildCaseAssetUrl(row.runId || '', caseId, flamegraph.replace(`cases/${caseId}/`, ''));
          }}
        />
      </div>

      {hasFlameByMeta && cards.length > 0 && (
        <FlamegraphSection cards={cards} cols={cols} setCols={setCols} />
      )}
    </div>
  );
}

function deriveInitialBundles(paramRuns: string, bundles: BundleInfo[]): string[] {
  if (paramRuns) {
    const ids = paramRuns
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id && bundles.find((b) => b.bundle_id === id));
    if (ids.length) {
      return ids;
    }
  }
  if (bundles.length) return bundles.slice(0, 2).map((b) => b.bundle_id);
  return [];
}

function buildRows(data: BundleData[], casesParam: string): ExecutionRow[] {
  const active = casesParam ? new Set(casesParam.split(',').map(String)) : null;
  const rows: ExecutionRow[] = [];
  data.forEach(({ bundleId, meta, allCaseMetrics }) => {
    meta.cases.forEach((caseInfo) => {
      const metrics = allCaseMetrics.get(caseInfo.case_id);
      if (metrics?.tests) {
        metrics.tests.forEach((test) => {
          const key = `${bundleId}:${test.test_id}`;
          if (active && !active.has(key)) return;
          rows.push({
            runId: bundleId,
            testId: test.test_id,
            dataConfig: test.data_config,
            indexConfig: test.index_config,
            expression: test.expression,
            metrics: {
              qps: test.qps,
              latency_ms: test.latency_ms,
              selectivity: test.selectivity,
              index_build_ms: test.index_build_ms,
              suite_name: test.suite_name,
              case_name: caseInfo.case_name,
              flamegraph: test.flamegraph,
              memory: test.memory,
              cpu_pct: test.cpu_pct,
            },
            timestamp: meta.timestamp_ms,
          });
        });
      }
    });
  });
  return rows;
}

function buildCards(data: BundleData[], casesParam: string): FlameCard[] {
  const active = casesParam ? new Set(casesParam.split(',').map(String)) : null;
  const cards: FlameCard[] = [];
  data.forEach(({ bundleId, meta, allCaseMetrics }) => {
    meta.cases.forEach((caseInfo) => {
      const metrics = allCaseMetrics.get(caseInfo.case_id);
      if (metrics?.tests) {
        metrics.tests.forEach((test) => {
          if (!test.flamegraph) return;
          const key = `${bundleId}:${test.test_id}`;
          if (active && !active.has(key)) return;
          cards.push({
            runId: bundleId,
            caseId: test.test_id,
            values: {
              flamegraph: test.flamegraph,
              data_config: test.data_config,
              index_config: test.index_config,
            },
          });
        });
      }
    });
  });
  return cards;
}

function BundleSelector({ allBundles, activeBundleIds, onChange }: { allBundles: BundleData[]; activeBundleIds: string[]; onChange: (selected: string[]) => void }): JSX.Element {
  return (
    <div className="section-card">
      <h3 style={{ marginTop: 0 }}>Bundles</h3>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
        {allBundles
          .slice()
          .sort((a, b) => Number(b.meta.timestamp_ms) - Number(a.meta.timestamp_ms))
          .map((bundle) => {
            const id = bundle.bundleId;
            const checked = activeBundleIds.includes(id);
            return (
              <label key={id} className="badge" style={{ justifyContent: 'space-between', cursor: 'pointer' }}>
                <span>
                  <input type="checkbox" checked={checked} onChange={(e) => {
                    const next = new Set(activeBundleIds);
                    if (e.target.checked) next.add(id); else next.delete(id);
                    onChange(Array.from(next));
                  }} />
                  <strong style={{ marginLeft: '0.35rem' }}>{id}</strong>
                </span>
                <span className="text-muted small">{bundle.meta.config_file}</span>
              </label>
            );
          })}
      </div>
    </div>
  );
}
