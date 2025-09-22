import React from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { fetchIndex } from '../../utils/dataApi'
import { RunIndexItem } from '../../utils/types'

export function RunsPage(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams()
  const [runs, setRuns] = React.useState<RunIndexItem[]>([])
  const [loading, setLoading] = React.useState<boolean>(true)
  const [error, setError] = React.useState<string | null>(null)

  const datasetParam = searchParams.get('dataset') ?? ''
  const indexParam = searchParams.get('index') ?? ''
  const exprParam = searchParams.get('expr') ?? ''

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetchIndex()
      .then((data) => {
        if (cancelled) return
        const list = data.runs.slice().sort((a, b) => (a.timestamp < b.timestamp ? 1 : -1))
        setRuns(list)
        setLoading(false)
      })
      .catch((e) => {
        if (cancelled) return
        setError(String(e))
        setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const filtered = runs.filter((r) => {
    if (datasetParam && String(r.config?.dataset ?? '').indexOf(datasetParam) < 0) return false
    if (indexParam && String(r.config?.index ?? '').indexOf(indexParam) < 0) return false
    if (exprParam && String(r.config?.expr ?? '').indexOf(exprParam) < 0) return false
    return true
  })

  function onFilterChange(key: string, value: string) {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    setSearchParams(next)
  }

  return (
    <div className="page runs-page">
      <h1>Runs</h1>
      <section className="filters">
        <input placeholder="dataset" value={datasetParam} onChange={(e) => onFilterChange('dataset', e.target.value)} />
        <input placeholder="index" value={indexParam} onChange={(e) => onFilterChange('index', e.target.value)} />
        <input placeholder="expr" value={exprParam} onChange={(e) => onFilterChange('expr', e.target.value)} />
      </section>
      {loading && <div>Loading…</div>}
      {error && <div className="error">{error}</div>}
      {!loading && !error && (
        <table className="runs-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Label</th>
              <th>Dataset</th>
              <th>Index</th>
              <th>QPS</th>
              <th>P95</th>
              <th>RSS</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => (
              <tr key={r.id}>
                <td>{new Date(r.timestamp).toLocaleString()}</td>
                <td>
                  <Link to={`/run/${encodeURIComponent(r.id)}`}>{r.label || r.id}</Link>
                </td>
                <td>{String(r.config?.dataset ?? '')}</td>
                <td>{String(r.config?.index ?? '')}</td>
                <td>{r.summary?.qps ?? ''}</td>
                <td>{r.summary?.p95_ms ?? ''}</td>
                <td>{r.summary?.rss_gb ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

