import React from 'react'
import { useSearchParams } from 'react-router-dom'
import { fetchIndex, fetchRunMetrics, resolveRunAssetUrl } from '../../utils/dataApi'
import { RunIndexItem, MetricsJson } from '../../utils/types'

export function ComparePage(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams()
  const [index, setIndex] = React.useState<RunIndexItem[]>([])
  const [metricsMap, setMetricsMap] = React.useState<Record<string, MetricsJson>>({})
  const [loading, setLoading] = React.useState<boolean>(true)
  const [error, setError] = React.useState<string | null>(null)

  const runsParam = (searchParams.get('runs') || '').split(',').filter(Boolean)
  const casesParam = (searchParams.get('cases') || '').split(',').filter(Boolean)
  const cols = Math.max(1, Math.min(4, Number(searchParams.get('cols') || 3)))

  React.useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        setLoading(true)
        const idx = await fetchIndex()
        if (cancelled) return
        setIndex(idx.runs)
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
  }, [])

  React.useEffect(() => {
    let cancelled = false
    async function loadRuns() {
      const entries = await Promise.all(
        runsParam.map(async (r) => {
          try {
            const m = await fetchRunMetrics(r)
            return [r, m] as const
          } catch (e) {
            return [r, { cases: {} }] as const
          }
        })
      )
      if (cancelled) return
      setMetricsMap(Object.fromEntries(entries))
    }
    if (runsParam.length > 0) loadRuns()
    return () => {
      cancelled = true
    }
  }, [searchParams])

  function toggleRun(runId: string) {
    const list = new Set(runsParam)
    if (list.has(runId)) list.delete(runId)
    else list.add(runId)
    const next = new URLSearchParams(searchParams)
    next.set('runs', Array.from(list).join(','))
    setSearchParams(next)
  }

  function setCases(value: string) {
    const next = new URLSearchParams(searchParams)
    next.set('cases', value)
    setSearchParams(next)
  }

  function setCols(value: number) {
    const next = new URLSearchParams(searchParams)
    next.set('cols', String(value))
    setSearchParams(next)
  }

  const allCases = Array.from(
    new Set(
      Object.values(metricsMap).flatMap((m) => Object.keys(m.cases || {}))
    )
  )
  const selectedCases = casesParam.length > 0 ? casesParam : allCases

  return (
    <div className="page compare-page">
      <h1>Compare</h1>
      {loading && <div>Loading…</div>}
      {error && <div className="error">{error}</div>}
      <section className="compare-controls">
        <div className="runs-picker">
          <strong>Select Runs:</strong>
          <div className="runs-chips">
            {index.map((r) => (
              <button
                key={r.id}
                className={runsParam.includes(r.id) ? 'chip active' : 'chip'}
                onClick={() => toggleRun(r.id)}
              >
                {r.label || r.id}
              </button>
            ))}
          </div>
        </div>
        <div>
          <strong>Cases:</strong>
          <input
            placeholder="comma separated case names"
            value={casesParam.join(',')}
            onChange={(e) => setCases(e.target.value)}
          />
        </div>
        <div>
          <strong>Columns:</strong>
          <input type="number" min={1} max={4} value={cols} onChange={(e) => setCols(Number(e.target.value))} />
        </div>
      </section>

      <section>
        <h2>Metrics</h2>
        <table className="compare-table">
          <thead>
            <tr>
              <th>Case</th>
              {runsParam.map((r) => (
                <th key={r}>{r}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {selectedCases.map((c) => (
              <tr key={c}>
                <td>{c}</td>
                {runsParam.map((r) => {
                  const m = metricsMap[r]?.cases?.[c]
                  return (
                    <td key={r + c}>
                      {m ? `qps=${m.qps ?? ''} p95=${m.p95_ms ?? ''}` : ''}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h2>Flamegraphs</h2>
        <div className="flamegrid" style={{ gridTemplateColumns: `repeat(${cols}, 1fr)` }}>
          {selectedCases.map((c) => (
            <React.Fragment key={c}>
              {runsParam.map((r) => (
                <figure key={r + c} className="flameitem">
                  <img src={resolveRunAssetUrl(r, `flamegraphs/${encodeURIComponent(c)}.svg`)} alt={`${r} ${c}`} />
                  <figcaption>{r} / {c}</figcaption>
                </figure>
              ))}
            </React.Fragment>
          ))}
        </div>
      </section>
    </div>
  )
}

