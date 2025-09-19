import React, { useEffect, useMemo, useState } from 'react';
import { getIndex, getRunMeta, getRunMetrics } from '../utils/api';
import { formatTimestamp, getSelectedRuns, toggleRunSelection, getSelectedCases, toggleCaseSelection } from '../utils/state';
import { useNavigate } from 'react-router-dom';
import { escapeHtml, formatNumber } from '../utils/format';
import { CasesTable, CaseRow } from '../components/CasesTable';

type RunRow = any;

export default function RunsPage(): JSX.Element {
  const [runs, setRuns] = useState<RunRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const [selectedRunsCount, setSelectedRunsCount] = useState<number>(getSelectedRuns().length);
  const [selectedCasesCount, setSelectedCasesCount] = useState<number>(getSelectedCases().length);
  const [openIds, setOpenIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const indexData = await getIndex();
        const list = Array.isArray(indexData?.runs) ? indexData.runs : [];
        const runsWithMeta = await Promise.all(
          list.map(async (run: any) => {
            try {
              const [meta, metrics] = await Promise.all([
                getRunMeta(run.id),
                getRunMetrics(run.id).catch(() => null),
              ]);
              return { ...run, meta, metrics };
            } catch (err: any) {
              return { ...run, meta: null, metrics: null, error: err?.message || String(err) };
            }
          }),
        );
        if (mounted) setRuns(runsWithMeta);
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

  const filters = useMemo(() => buildFilters(runs || []), [runs]);
  const [dataset, setDataset] = useState('');
  const [expr, setExpr] = useState('');
  const [index, setIndex] = useState('');
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    const list = (runs || [])
      .slice()
      .sort((a, b) => Number(b.timestamp_ms || b.id || 0) - Number(a.timestamp_ms || a.id || 0))
      .filter((run) => {
        const meta = run.meta || {};
        if (dataset && !(meta.data_configs || []).includes(dataset)) return false;
        if (index && !(meta.index_configs || []).includes(index)) return false;
        if (expr && !(meta.expressions || []).includes(expr)) return false;
        if (search) {
          const text = [run.id, run.label, meta.label, meta.summary?.total_cases]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();
          if (!text.includes(search.toLowerCase())) return false;
        }
        return true;
      });
    return list;
  }, [runs, dataset, index, expr, search]);

  if (error) return <div className="section-card"><div className="alert">{error}</div></div>;

  return (
    <div className="section-card">
      <div className="page-actions">
        <div className="left">
          <h2 style={{ margin: 0 }}>Benchmark Runs</h2>
          <span className="badge">Selected runs: <span id="selected-count">{selectedRunsCount}</span></span>
          <span className="badge" style={{ marginLeft: '0.5rem' }}>Selected cases: <span id="selected-cases-count">{selectedCasesCount}</span></span>
        </div>
        <div className="right">
          <button className="primary" id="compare-runs-btn" disabled={selectedRunsCount < 2} onClick={() => {
            const selected = getSelectedRuns();
            if (selected.length >= 2) navigate(`/compare?runs=${selected.join(',')}`);
          }}>Compare runs</button>
          <button className="secondary" id="compare-cases-btn" disabled={selectedCasesCount < 2} onClick={() => {
            const cases = getSelectedCases();
            if (cases.length >= 2) navigate(`/compare?cases=${encodeURIComponent(cases.join(','))}`);
          }}>Compare cases</button>
          <button className="ghost" id="refresh-btn" onClick={() => window.location.reload()}>Refresh</button>
        </div>
      </div>
      <p className="caption">Results are read from <code>index.json</code> and per-run folders in the configured results directory.</p>

      {!runs ? (
        <div className="loading">Loading runs…</div>
      ) : runs.length === 0 ? (
        <div className="empty-state">No runs found. Execute the benchmark to populate results.</div>
      ) : (
        <>
          <div className="filter-bar">
            <label>Dataset
              <select value={dataset} onChange={(e) => setDataset(e.target.value)}>
                <option value="">All</option>
                {filters.datasets.map((d) => <option value={d} key={d}>{d}</option>)}
              </select>
            </label>
            <label>Expression
              <select value={expr} onChange={(e) => setExpr(e.target.value)}>
                <option value="">All</option>
                {filters.expressions.map((d) => <option value={d} key={d}>{d}</option>)}
              </select>
            </label>
            <label>Index
              <select value={index} onChange={(e) => setIndex(e.target.value)}>
                <option value="">All</option>
                {filters.indexes.map((d) => <option value={d} key={d}>{d}</option>)}
              </select>
            </label>
            <label>Search
              <input type="search" placeholder="Run id, label…" value={search} onChange={(e) => setSearch(e.target.value)} />
            </label>
          </div>

          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th></th>
                  <th>Run ID</th>
                  <th>Timestamp</th>
                  <th>Total cases</th>
                  <th>Datasets</th>
                  <th>Expressions</th>
                  <th>Indexes</th>
                  <th>Label</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((run) => {
                  const meta = run.meta;
                  const isSelected = getSelectedRuns().includes(String(run.id));
                  const isOpen = openIds.has(String(run.id));
                  return (
                    <React.Fragment key={run.id}>
                      <tr
                        onClick={() => {
                          setOpenIds((prev) => {
                            const s = new Set(Array.from(prev));
                            const id = String(run.id);
                            if (s.has(id)) s.delete(id); else s.add(id);
                            return s;
                          });
                        }}
                        style={{ cursor: 'pointer' }}
                      >
                        <td>
                          <InlineExpander open={isOpen} setOpen={(next) => {
                            setOpenIds((prev) => {
                              const s = new Set(Array.from(prev));
                              const id = String(run.id);
                              if (next) s.add(id); else s.delete(id);
                              return s;
                            });
                          }} />
                          <input type="checkbox" checked={isSelected} onClick={(e) => e.stopPropagation()} onChange={(e) => {
                            toggleRunSelection(run.id, e.target.checked);
                            setSelectedRunsCount(getSelectedRuns().length);
                          }} />
                        </td>
                        <td>{run.id}</td>
                        <td>{formatTimestamp(run.timestamp_ms || run.id)}</td>
                        <td className="numeric">{meta?.summary?.total_cases ?? '—'}</td>
                        <td>{renderTagList(meta?.data_configs)}</td>
                        <td>{renderTagList(meta?.expressions)}</td>
                        <td>{renderTagList(meta?.index_configs)}</td>
                        <td>{meta?.label || run.label || '—'}</td>
                        <td>
                          <div className="table-actions">
                            <button className="secondary" onClick={() => navigate(`/run/${run.id}`)}>Details</button>
                            {run.error ? <span className="badge danger" title={run.error}>meta.json failed</span> : null}
                          </div>
                        </td>
                      </tr>
                      {isOpen && (
                        <tr>
                          <td colSpan={9} style={{ padding: 0 }}>
                            <InlineCases runId={run.id} metrics={run.metrics} onCasesChange={() => setSelectedCasesCount(getSelectedCases().length)} />
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

function InlineExpander({ open, setOpen }: { open: boolean; setOpen: (v: boolean) => void }): JSX.Element {
  return (
    <button className="ghost" title={open ? 'Collapse cases' : 'Expand cases'} onClick={() => setOpen(!open)}>
      {open ? '−' : '+'}
    </button>
  );
}

function InlineCases({ runId, metrics, onCasesChange }: { runId: string; metrics: any; onCasesChange: () => void }): JSX.Element {
  if (!metrics?.cases) return <div className="empty-state" style={{ margin: '0.5rem 1rem' }}>No cases found in metrics.json.</div>;

  const rows: CaseRow[] = Object.entries(metrics.cases).map(([caseId, data]: any) => ({
    runId,
    caseId: String(caseId),
    dataConfig: data.data_config,
    indexConfig: data.index_config,
    expression: data.expression,
    metrics: data,
  }));

  return (
    <div style={{ display: 'block' }}>
      <CasesTable
        rows={rows}
        metricKeys={[
          { key: 'qps', label: 'QPS', formatter: formatNumber, better: 'higher' },
          { key: 'latency_ms.p99', label: 'P99', formatter: formatNumber, better: 'lower' },
        ]}
        showRunId={false}
        allowSelection
        isSelected={(key) => getSelectedCases().includes(key)}
        getSelectionKey={(row) => `${row.runId}:${row.caseId}`}
        onToggleSelect={(key, checked, row) => {
          const [rid, cid] = key.split(':');
          toggleCaseSelection(rid, cid, checked);
          onCasesChange();
        }}
        showFlamegraphLink={false}
      />
    </div>
  );
}

function buildFilters(runs: any[]): { datasets: string[]; indexes: string[]; expressions: string[] } {
  const datasets = new Set<string>();
  const indexes = new Set<string>();
  const expressions = new Set<string>();
  runs.forEach((run) => {
    (run.meta?.data_configs || []).forEach((item: string) => datasets.add(item));
    (run.meta?.index_configs || []).forEach((item: string) => indexes.add(item));
    (run.meta?.expressions || []).forEach((item: string) => expressions.add(item));
  });
  return {
    datasets: Array.from(datasets).sort(),
    indexes: Array.from(indexes).sort(),
    expressions: Array.from(expressions).sort(),
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

