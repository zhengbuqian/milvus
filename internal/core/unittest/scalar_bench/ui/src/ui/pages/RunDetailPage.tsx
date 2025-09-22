import React from 'react'
import { useParams } from 'react-router-dom'
import { fetchRunMeta, fetchRunMetrics, resolveRunAssetUrl } from '../../utils/dataApi'
import { RunMeta, MetricsJson } from '../../utils/types'

export function RunDetailPage(): JSX.Element {
  const { runId = '' } = useParams()
  const [meta, setMeta] = React.useState<RunMeta | null>(null)
  const [metrics, setMetrics] = React.useState<MetricsJson | null>(null)
  const [loading, setLoading] = React.useState<boolean>(true)
  const [error, setError] = React.useState<string | null>(null)

  React.useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        setLoading(true)
        const [m, t] = await Promise.all([fetchRunMeta(runId), fetchRunMetrics(runId)])
        if (cancelled) return
        setMeta(m)
        setMetrics(t)
        setLoading(false)
      } catch (e) {
        if (cancelled) return
        setError(String(e))
        setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [runId])

  const cases = Object.entries(metrics?.cases ?? {})

  return (
    <div className="page run-detail-page">
      <h1>Run: {runId}</h1>
      {loading && <div>Loading…</div>}
      {error && <div className="error">{error}</div>}
      {meta && (
        <section className="meta">
          <h2>Meta</h2>
          <pre className="pre-box">{JSON.stringify(meta, null, 2)}</pre>
        </section>
      )}
      {cases.length > 0 && (
        <section>
          <h2>Metrics</h2>
          <table className="metrics-table">
            <thead>
              <tr>
                <th>Case</th>
                <th>QPS</th>
                <th>p50</th>
                <th>p95</th>
                <th>p99</th>
                <th>RSS(GB)</th>
                <th>CPU%</th>
                <th>IO Read MB/s</th>
              </tr>
            </thead>
            <tbody>
              {cases.map(([caseName, data]) => (
                <tr key={caseName}>
                  <td>{caseName}</td>
                  <td>{data.qps ?? ''}</td>
                  <td>{data.p50_ms ?? ''}</td>
                  <td>{data.p95_ms ?? ''}</td>
                  <td>{data.p99_ms ?? ''}</td>
                  <td>{data.rss_gb ?? ''}</td>
                  <td>{data.cpu_pct ?? ''}</td>
                  <td>{data.io_read_mb_s ?? ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
      <section>
        <h2>Flamegraphs</h2>
        <div className="flamegrid">
          {cases.map(([caseName]) => (
            <figure key={caseName} className="flameitem">
              <img src={resolveRunAssetUrl(runId, `flamegraphs/${encodeURIComponent(caseName)}.svg`)} alt={caseName} />
              <figcaption>{caseName}</figcaption>
            </figure>
          ))}
        </div>
      </section>
      <section>
        <h2>Assets</h2>
        <ul>
          <li>
            <a href={resolveRunAssetUrl(runId, 'config.yaml')} download>config.yaml</a>
          </li>
        </ul>
      </section>
    </div>
  )
}

