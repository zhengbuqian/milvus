// Bundle-related TypeScript types for the new hierarchical structure

/**
 * Test parameters configuration
 */
export interface TestParams {
  warmup_iterations: number;
  test_iterations: number;
  collect_memory_stats: boolean;
  enable_flame_graph: boolean;
  flamegraph_repo_path?: string;
}

/**
 * Latency metrics for a single test
 */
export interface LatencyMetrics {
  min_ms: number;
  max_ms: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  stddev_ms: number;
}

/**
 * Memory statistics for a test (optional)
 */
export interface MemoryStats {
  peak_memory_mb?: number;
  avg_memory_mb?: number;
}

/**
 * Basic bundle information from index.json
 */
export interface BundleInfo {
  bundle_id: string;
  config_file: string;
  timestamp_ms: number;
  label?: string;
  cases: string[];
  total_tests: number;
}

/**
 * Index file structure containing all bundles
 */
export interface IndexData {
  bundles: BundleInfo[];
}

/**
 * Case information within a bundle
 */
export interface CaseInfo {
  case_name: string;
  case_id: string;
  suites: string[];
  total_tests: number;
  has_flamegraphs: boolean;
}

/**
 * Complete bundle metadata
 */
export interface BundleMeta {
  bundle_id: string;
  config_file: string;
  config_content: string;
  timestamp_ms: number;
  test_params: TestParams;
  cases: CaseInfo[];
}

/**
 * Suite information within a case
 */
export interface SuiteInfo {
  suite_name: string;
  data_configs: string[];
  index_configs: string[];
  expr_templates: string[];
}

/**
 * Complete case metadata
 */
export interface CaseMeta {
  case_id: string;
  case_name: string;
  bundle_id: string;
  suites: SuiteInfo[];
  total_tests: number;
  has_flamegraphs: boolean;
}

/**
 * Individual test result
 */
export interface TestResult {
  test_id: string;
  suite_name: string;
  data_config: string;
  index_config: string;
  expression: string;
  actual_expression: string;
  qps: number;
  latency_ms: {
    avg: number;
    p50: number;
    p90: number;
    p99: number;
    p999: number;
    min: number;
    max: number;
  };
  matched_rows: number;
  total_rows: number;
  selectivity: number;
  index_build_ms: number;
  memory: {
    index_mb: number;
    exec_peak_mb: number;
  };
  cpu_pct: number;
  flamegraph: string | null;
}

/**
 * Case metrics containing all test results
 */
export interface CaseMetrics {
  tests: TestResult[];
}

/**
 * Statistics for a group of tests
 */
export interface TestGroupStats {
  count: number;
  avg_qps: number;
  avg_latency_ms: number;
  min_latency_ms: number;
  max_latency_ms: number;
  avg_p99_latency_ms: number;
}
