export interface RunIndexItem {
  id: string
  timestamp: string
  label?: string
  git_commit?: string
  machine?: Record<string, unknown>
  config?: {
    dataset?: string
    index?: string
    expr?: string
    mmap?: boolean
  }
  summary?: {
    qps?: number
    p95_ms?: number
    rss_gb?: number
  }
}

export interface IndexJson {
  runs: RunIndexItem[]
}

export interface RunMeta {
  [key: string]: unknown
}

export interface CaseMetrics {
  qps?: number
  p50_ms?: number
  p95_ms?: number
  p99_ms?: number
  rss_gb?: number
  cpu_pct?: number
  io_read_mb_s?: number
}

export interface MetricsJson {
  cases: Record<string, CaseMetrics>
}

