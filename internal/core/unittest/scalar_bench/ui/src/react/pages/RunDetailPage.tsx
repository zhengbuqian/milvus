import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { getBundleMeta, getCaseMetrics, buildCaseAssetUrl } from '../utils/api';
import { formatTimestamp, getSelectedRuns, toggleRunSelection } from '../utils/state';
import { ExecutionsTable, ExecutionRow } from '../components/ExecutionsTable';
import { FlamegraphSection, FlameCard } from '../components/Flamegraphs';
import { MetaItem } from '../components/MetaItem';
import { SummaryStats } from '../components/SummaryStats';
import DatasetCharts from '../components/DatasetCharts';
import { CasesList } from '../components/CasesList';
import { METRIC_KEYS } from '../utils/constants';
import { getRelativeConfigPath, calculateStats } from '../utils/helpers';
import type { BundleMeta, CaseMetrics, CaseInfo } from '../types/bundle';

export default function RunDetailPage(): JSX.Element {
  const { bundleId = '' } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [bundleMeta, setBundleMeta] = useState<BundleMeta | null>(null);
  const [allCaseMetrics, setAllCaseMetrics] = useState<Map<string, CaseMetrics>>(new Map());

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    (async () => {
      try {
        const meta = await getBundleMeta(bundleId);
        if (!mounted) return;
        setBundleMeta(meta);

        // Load all case metrics
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

        if (!mounted) return;
        setAllCaseMetrics(metricsMap);
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        setError(err?.message || String(err));
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [bundleId]);

  if (loading) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All bundles</button>
          <h2 style={{ margin: 0 }}>Bundle {bundleId}</h2>
        </div>
      </div>
      <div className="loading">Loading bundle details…</div>
    </div>
  );

  if (error) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All bundles</button>
          <h2 style={{ margin: 0 }}>Bundle {bundleId}</h2>
        </div>
      </div>
      <div className="alert">{error}</div>
    </div>
  );

  if (!bundleMeta) return <div></div>;

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate('/runs')}>← All bundles</button>
          <h2 style={{ margin: 0 }}>Bundle {bundleId}</h2>
        </div>
        <div className="right">
          <label className="badge" style={{ cursor: 'pointer' }}>
            <input
              type="checkbox"
              style={{ marginRight: '0.35rem' }}
              defaultChecked={getSelectedRuns().includes(bundleId)}
              onChange={(e) => toggleRunSelection(bundleId, e.target.checked)}
            />
            Select for compare
          </label>
        </div>
      </div>

      <div className="meta-grid">
        <MetaItem label="Bundle ID" value={bundleId} />
        <MetaItem label="Timestamp" value={formatTimestamp(bundleMeta.timestamp_ms)} />
        <MetaItem label="Config File" value={getRelativeConfigPath(bundleMeta.config_file)} />
        <MetaItem label="Total Cases" value={String(bundleMeta.cases.length)} />
        <MetaItem label="Total Tests" value={String(bundleMeta.cases.reduce((sum, c) => sum + c.total_tests, 0))} />
        <MetaItem label="Warmup Iterations" value={String(bundleMeta.test_params.warmup_iterations)} />
        <MetaItem label="Test Iterations" value={String(bundleMeta.test_params.test_iterations)} />
        <MetaItem label="Collect Memory" value={bundleMeta.test_params.collect_memory_stats ? 'Yes' : 'No'} />
        <MetaItem label="Enable Flamegraph" value={bundleMeta.test_params.enable_flame_graph ? 'Yes' : 'No'} />
      </div>

      <div style={{ marginTop: '1.5rem' }}>
        <h3>Cases ({bundleMeta.cases.length})</h3>
        <CasesList cases={bundleMeta.cases} bundleId={bundleId} />
      </div>

      {bundleMeta.cases.map((caseInfo) => (
        <CaseDetailSection
          key={caseInfo.case_id}
          bundleId={bundleId}
          caseInfo={caseInfo}
          caseMetrics={allCaseMetrics.get(caseInfo.case_id)}
          enableFlamegraph={bundleMeta.test_params.enable_flame_graph}
        />
      ))}

      {/* Config File Content */}
      <div style={{ marginTop: '1.5rem' }}>
        <h3>Config File Content</h3>
        <div className="code-block"><pre>{bundleMeta.config_content}</pre></div>
      </div>
    </div>
  );
}

interface CaseDetailSectionProps {
  bundleId: string;
  caseInfo: CaseInfo;
  caseMetrics?: CaseMetrics;
  enableFlamegraph: boolean;
}

function CaseDetailSection({ bundleId, caseInfo, caseMetrics, enableFlamegraph }: CaseDetailSectionProps): JSX.Element {
  const [expanded, setExpanded] = useState(false);
  const [cols, setCols] = useState<number>(2);

  const rows: ExecutionRow[] = useMemo(() => {
    if (!caseMetrics?.tests) return [];
    return caseMetrics.tests.map((test) => ({
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
      },
    }));
  }, [caseMetrics, bundleId, caseInfo.case_name]);

  const flameCards: FlameCard[] = useMemo(() => {
    if (!caseMetrics?.tests) return [];
    return caseMetrics.tests
      .filter((test) => test.flamegraph)
      .map((test) => ({
        runId: bundleId,
        caseId: test.test_id,
        values: {
          flamegraph: test.flamegraph || undefined,
          data_config: test.data_config,
          index_config: test.index_config,
        },
      }));
  }, [caseMetrics, bundleId]);

  const hasAnyFlamegraph = useMemo(() => {
    return rows.some((r) => Boolean((r.metrics as any)?.flamegraph));
  }, [rows]);

  const stats = useMemo(() => {
    return caseMetrics?.tests ? calculateStats(caseMetrics.tests) : null;
  }, [caseMetrics]);

  if (!caseMetrics) {
    return (
      <div id={`case-${caseInfo.case_id}`} style={{ marginTop: '2rem', paddingTop: '1rem', borderTop: '2px solid #e0e0e0' }}>
        <h3>{caseInfo.case_name}</h3>
        <div className="alert">Failed to load case metrics</div>
      </div>
    );
  }

  return (
    <div id={`case-${caseInfo.case_id}`} style={{ marginTop: '2rem', paddingTop: '1rem', borderTop: '2px solid #e0e0e0' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 style={{ margin: 0 }}>
          {caseInfo.case_name} <span style={{ color: '#666', fontSize: '0.9em' }}>({caseInfo.total_tests} tests)</span>
        </h3>
        <button
          className="ghost"
          onClick={() => setExpanded(!expanded)}
          style={{ fontSize: '0.9em' }}
        >
          {expanded ? '▼ Collapse' : '▶ Expand Details'}
        </button>
      </div>

      {stats && (
        <>
          <div style={{ marginTop: '1rem' }}>
            <SummaryStats stats={stats} />
          </div>

          {/* Charts and Table - Collapsible */}
          {expanded && (
            <>
              <div style={{ marginTop: '1.5rem' }}>
                <DatasetCharts rows={rows} />
              </div>

              <div style={{ marginTop: '1.5rem' }}>
                <h4>Test Results</h4>
                <ExecutionsTable
                  rows={rows}
                  metricKeys={METRIC_KEYS}
                  showRunId={false}
                  allowSelection={false}
                  showFlamegraphLink={enableFlamegraph && hasAnyFlamegraph}
                  buildFlamegraphUrl={(row) => {
                    const flamegraph = (row.metrics as any).flamegraph;
                    if (!flamegraph) return null;
                    const match = flamegraph.match(/cases\/([^/]+)\//);
                    const caseId = match ? match[1] : '';
                    return buildCaseAssetUrl(bundleId, caseId, flamegraph.replace(`cases/${caseId}/`, ''));
                  }}
                />
              </div>

              {enableFlamegraph && flameCards.length > 0 && (
                <FlamegraphSection cards={flameCards} cols={cols} setCols={setCols} title={`Flamegraphs for ${caseInfo.case_name}`} />
              )}
            </>
          )}
        </>
      )}
    </div>
  );
}
