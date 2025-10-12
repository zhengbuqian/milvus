import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { getBundleMeta, getCaseMetrics, buildCaseAssetUrl } from '../utils/api';
import { ExecutionsTable, ExecutionRow } from '../components/ExecutionsTable';
import { formatTimestamp } from '../utils/format';
import { FlamegraphSection, FlameCard } from '../components/Flamegraphs';
import { MetaItem } from '../components/MetaItem';
import { SummaryStats } from '../components/SummaryStats';
import DatasetCharts from '../components/DatasetCharts';
import { METRIC_KEYS } from '../utils/constants';
import { getRelativeConfigPath, calculateStats } from '../utils/helpers';
import type { BundleMeta, CaseMetrics, CaseInfo } from '../types/bundle';

export default function CaseDetailPage(): JSX.Element {
  const { bundleId = '', caseId = '' } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [bundleMeta, setBundleMeta] = useState<BundleMeta | null>(null);
  const [caseMetrics, setCaseMetrics] = useState<CaseMetrics | null>(null);
  const [cols, setCols] = useState<number>(2);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    (async () => {
      try {
        const [meta, metrics] = await Promise.all([
          getBundleMeta(bundleId),
          getCaseMetrics(bundleId, caseId),
        ]);
        if (!mounted) return;
        setBundleMeta(meta);
        setCaseMetrics(metrics);
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        setError(err?.message || String(err));
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [bundleId, caseId]);

  const caseInfo: CaseInfo | undefined = useMemo(() => {
    return bundleMeta?.cases.find((c) => c.case_id === caseId);
  }, [bundleMeta, caseId]);

  const rows: ExecutionRow[] = useMemo(() => {
    if (!caseMetrics?.tests || !caseInfo) return [];
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
  }, [caseMetrics, bundleId, caseInfo]);

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

  if (loading) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate(`/bundle/${bundleId}`)}>← Back to bundle</button>
          <h2 style={{ margin: 0 }}>Case Detail</h2>
        </div>
      </div>
      <div className="loading">Loading case details…</div>
    </div>
  );

  if (error) return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate(`/bundle/${bundleId}`)}>← Back to bundle</button>
          <h2 style={{ margin: 0 }}>Case Detail</h2>
        </div>
      </div>
      <div className="alert">{error}</div>
    </div>
  );

  if (!bundleMeta || !caseInfo) return <div></div>;

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <button className="ghost" onClick={() => navigate(`/bundle/${bundleId}`)}>← Back to bundle</button>
          <h2 style={{ margin: 0 }}>{caseInfo.case_name}</h2>
        </div>
      </div>

      <div className="meta-grid">
        <MetaItem label="Case Name" value={caseInfo.case_name} />
        <MetaItem label="Case ID" value={caseId} />
        <MetaItem label="Bundle ID" value={bundleId} />
        <MetaItem label="Timestamp" value={formatTimestamp(bundleMeta.timestamp_ms)} />
        <MetaItem label="Config File" value={getRelativeConfigPath(bundleMeta.config_file)} />
        <MetaItem label="Suites" value={caseInfo.suites.join(', ')} />
        <MetaItem label="Total Tests" value={String(caseInfo.total_tests)} />
        <MetaItem label="Has Flamegraphs" value={caseInfo.has_flamegraphs ? 'Yes' : 'No'} />
      </div>

      {stats && (
        <>
          <div style={{ marginTop: '1.5rem' }}>
            <h3>Summary Statistics</h3>
            <SummaryStats stats={stats} />
          </div>

          {/* Charts */}
          <div style={{ marginTop: '1.5rem' }}>
            <DatasetCharts rows={rows} />
          </div>

          {/* Test Results Table */}
          <div style={{ marginTop: '1.5rem' }}>
            <h3>Test Results</h3>
            <ExecutionsTable
              rows={rows}
              metricKeys={METRIC_KEYS}
              showRunId={false}
              allowSelection={false}
              showFlamegraphLink={bundleMeta.test_params.enable_flame_graph && hasAnyFlamegraph}
              buildFlamegraphUrl={(row) => {
                const flamegraph = (row.metrics as any).flamegraph;
                if (!flamegraph) return null;
                const match = flamegraph.match(/cases\/([^/]+)\//);
                const caseIdFromPath = match ? match[1] : '';
                return buildCaseAssetUrl(bundleId, caseIdFromPath, flamegraph.replace(`cases/${caseIdFromPath}/`, ''));
              }}
            />
          </div>

          {/* Flamegraphs */}
          {bundleMeta.test_params.enable_flame_graph && flameCards.length > 0 && (
            <FlamegraphSection cards={flameCards} cols={cols} setCols={setCols} title={`Flamegraphs for ${caseInfo.case_name}`} />
          )}
        </>
      )}
    </div>
  );
}
