import React, { useEffect, useMemo, useState } from 'react';
import { getBundles, getBundleMeta, getCaseMetrics } from '../utils/api';
import { formatTimestamp, getSelectedRuns, toggleRunSelection, getSelectedCases } from '../utils/state';
import { useNavigate } from 'react-router-dom';
import { escapeHtml } from '../utils/format';
import { StatCard } from '../components/StatCard';
import { calculateStats } from '../utils/helpers';
import type { BundleInfo, BundleMeta } from '../types/bundle';

type BundleRow = BundleInfo & {
  meta?: BundleMeta;
  error?: string;
};

export default function RunsPage(): JSX.Element {
  const [bundles, setBundles] = useState<BundleRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const [selectedRunsCount, setSelectedRunsCount] = useState<number>(getSelectedRuns().length);
  const [selectedCasesCount, setSelectedCasesCount] = useState<number>(getSelectedCases().length);
  const [openIds, setOpenIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const bundleList = await getBundles();
        const bundlesWithMeta = await Promise.all(
          bundleList.map(async (bundle) => {
            try {
              const meta = await getBundleMeta(bundle.bundle_id);
              return { ...bundle, meta };
            } catch (err: any) {
              return { ...bundle, meta: undefined, error: err?.message || String(err) };
            }
          }),
        );
        if (mounted) setBundles(bundlesWithMeta);
      } catch (err: any) {
        if (mounted) setError(err?.message || String(err));
      }
    })();
    const onRunSel = () => setSelectedRunsCount(getSelectedRuns().length);
    const onCaseSel = () => setSelectedCasesCount(getSelectedCases().length);
    document.addEventListener('scalar-bench:selected-runs-changed', onRunSel as EventListener);
    document.addEventListener('scalar-bench:selected-cases-changed', onCaseSel as EventListener);
    return () => {
      mounted = false;
      document.removeEventListener('scalar-bench:selected-runs-changed', onRunSel as EventListener);
      document.removeEventListener('scalar-bench:selected-cases-changed', onCaseSel as EventListener);
    };
  }, []);

  const filters = useMemo(() => buildFilters(bundles || []), [bundles]);
  const [caseName, setCaseName] = useState('');
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    const list = (bundles || [])
      .slice()
      .sort((a, b) => Number(b.timestamp_ms) - Number(a.timestamp_ms))
      .filter((bundle) => {
        if (caseName && !(bundle.cases || []).includes(caseName)) return false;
        if (search) {
          const text = [bundle.bundle_id, bundle.label, bundle.config_file, bundle.cases?.join(' ')]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();
          if (!text.includes(search.toLowerCase())) return false;
        }
        return true;
      });
    return list;
  }, [bundles, caseName, search]);

  if (error) return <div className="section-card"><div className="alert">{error}</div></div>;

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <h2 style={{ margin: 0 }}>Benchmark Bundles</h2>
          <span className="badge">Selected bundles: <span id="selected-count">{selectedRunsCount}</span></span>
          <span className="badge" style={{ marginLeft: '0.5rem' }}>Selected cases: <span id="selected-cases-count">{selectedCasesCount}</span></span>
        </div>
        <div className="right">
          <button className="primary" id="compare-runs-btn" disabled={selectedRunsCount < 2} onClick={() => {
            const selected = getSelectedRuns();
            if (selected.length >= 2) navigate(`/compare?runs=${selected.join(',')}`);
          }}>Compare bundles</button>
          <button className="secondary" id="compare-cases-btn" disabled={selectedCasesCount < 2} onClick={() => {
            const cases = getSelectedCases();
            if (cases.length >= 2) navigate(`/compare?cases=${encodeURIComponent(cases.join(','))}`);
          }}>Compare cases</button>
          <button className="ghost" id="refresh-btn" onClick={() => window.location.reload()}>Refresh</button>
        </div>
      </div>
      <p className="caption">Results are read from <code>index.json</code> and per-bundle folders in the configured results directory.</p>

      {!bundles ? (
        <div className="loading">Loading bundles…</div>
      ) : bundles.length === 0 ? (
        <div className="empty-state">No bundles found. Execute the benchmark to populate results.</div>
      ) : (
        <>
          <div className="filter-bar">
            <label>Case Name
              <select value={caseName} onChange={(e) => setCaseName(e.target.value)}>
                <option value="">All</option>
                {filters.caseNames.map((c) => <option value={c} key={c}>{c}</option>)}
              </select>
            </label>
            <label>Search
              <input type="search" placeholder="Bundle ID, config file…" value={search} onChange={(e) => setSearch(e.target.value)} />
            </label>
          </div>

          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th></th>
                  <th>Bundle ID</th>
                  <th>Timestamp</th>
                  <th>Config File</th>
                  <th>Cases</th>
                  <th>Total Tests</th>
                  <th>Label</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((bundle) => {
                  const isSelected = getSelectedRuns().includes(bundle.bundle_id);
                  const isOpen = openIds.has(bundle.bundle_id);
                  return (
                    <React.Fragment key={bundle.bundle_id}>
                      <tr
                        onClick={() => {
                          setOpenIds((prev) => {
                            const s = new Set(Array.from(prev));
                            if (s.has(bundle.bundle_id)) s.delete(bundle.bundle_id);
                            else s.add(bundle.bundle_id);
                            return s;
                          });
                        }}
                        style={{ cursor: 'pointer' }}
                      >
                        <td>
                          <input type="checkbox" checked={isSelected} onClick={(e) => e.stopPropagation()} onChange={(e) => {
                            toggleRunSelection(bundle.bundle_id, e.target.checked);
                            setSelectedRunsCount(getSelectedRuns().length);
                          }} />
                        </td>
                        <td>{bundle.bundle_id}</td>
                        <td>{formatTimestamp(bundle.timestamp_ms)}</td>
                        <td><code style={{ fontSize: '0.85em' }}>{bundle.config_file.split('/').pop()}</code></td>
                        <td>{renderTagList(bundle.cases)}</td>
                        <td className="numeric">{bundle.total_tests}</td>
                        <td>{bundle.label || '—'}</td>
                        <td>
                          <div className="table-actions">
                            <button className="secondary" onClick={(e) => {
                              e.stopPropagation();
                              navigate(`/bundle/${bundle.bundle_id}`);
                            }}>Details</button>
                            {bundle.error ? <span className="badge danger" title={bundle.error}>bundle_meta.json failed</span> : null}
                          </div>
                        </td>
                      </tr>
                      {isOpen && bundle.meta && (
                        <tr>
                          <td colSpan={8} style={{ padding: 0 }}>
                            <InlineCases bundleId={bundle.bundle_id} bundleMeta={bundle.meta} />
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

function InlineCases({ bundleId, bundleMeta }: { bundleId: string; bundleMeta: BundleMeta }): JSX.Element {
  const navigate = useNavigate();
  const [allMetrics, setAllMetrics] = useState<Map<string, any>>(new Map());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!bundleMeta?.cases || bundleMeta.cases.length === 0) {
      setLoading(false);
      return;
    }

    let mounted = true;
    (async () => {
      try {
        // Load metrics for all cases
        const metricsData = await Promise.all(
          bundleMeta.cases.map(async (caseInfo) => {
            try {
              const metrics = await getCaseMetrics(bundleId, caseInfo.case_id);
              return { caseInfo, metrics };
            } catch (err) {
              console.error(`Failed to load metrics for case ${caseInfo.case_id}:`, err);
              return { caseInfo, metrics: null };
            }
          })
        );

        if (!mounted) return;

        const metricsMap = new Map();
        metricsData.forEach(({ caseInfo, metrics }) => {
          metricsMap.set(caseInfo.case_id, { caseInfo, metrics });
        });

        setAllMetrics(metricsMap);
        setLoading(false);
      } catch (err) {
        console.error('Error loading case metrics:', err);
        setLoading(false);
      }
    })();

    return () => { mounted = false; };
  }, [bundleId, bundleMeta]);

  if (!bundleMeta?.cases || bundleMeta.cases.length === 0) {
    return <div className="empty-state" style={{ margin: '0.5rem 1rem' }}>No cases found in bundle metadata.</div>;
  }

  if (loading) {
    return <div className="loading" style={{ margin: '0.5rem 1rem' }}>Loading case metrics…</div>;
  }

  return (
    <div style={{ padding: '1rem', backgroundColor: '#f9fafb' }}>
      {bundleMeta.cases.map((caseInfo) => {
        const data = allMetrics.get(caseInfo.case_id);
        const metrics = data?.metrics;
        const stats = metrics?.tests ? calculateStats(metrics.tests) : null;

        return (
          <div key={caseInfo.case_id} style={{ marginBottom: '1rem', padding: '1rem', background: 'white', borderRadius: '6px', border: '1px solid #e5e7eb' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <h4 style={{ margin: 0 }}>
                <strong>{caseInfo.case_name}</strong>
                <span style={{ color: '#666', fontSize: '0.85em', marginLeft: '0.5rem' }}>({caseInfo.total_tests} tests)</span>
              </h4>
              <button
                className="secondary"
                style={{ padding: '0.35rem 0.75rem', fontSize: '0.85em' }}
                onClick={() => navigate(`/bundle/${bundleId}/case/${caseInfo.case_id}`)}
              >
                View Details
              </button>
            </div>

            {stats && (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '0.75rem' }}>
                <StatCard label="Avg QPS" value={stats.avgQps.toFixed(2)} />
                <StatCard label="Max QPS" value={stats.maxQps.toFixed(2)} color="#4CAF50" />
                <StatCard label="Avg Latency" value={`${stats.avgLatency.toFixed(3)} ms`} />
                <StatCard label="Min Latency" value={`${stats.minLatency.toFixed(3)} ms`} color="#2196F3" />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function buildFilters(bundles: BundleRow[]): { caseNames: string[] } {
  const caseNames = new Set<string>();
  bundles.forEach((bundle) => {
    (bundle.cases || []).forEach((caseName: string) => caseNames.add(caseName));
  });
  return {
    caseNames: Array.from(caseNames).sort(),
  };
}

function renderTagList(items?: string[]): JSX.Element {
  if (!items || !items.length) return <span className="text-muted">—</span>;
  return (
    <div className="tag-list">
      {items.map((item) => (
        <span className="tag" key={item} dangerouslySetInnerHTML={{ __html: escapeHtml(String(item)) }} />
      ))}
    </div>
  );
}
