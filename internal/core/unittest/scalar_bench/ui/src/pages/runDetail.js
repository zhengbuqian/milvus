import { getRunMeta, getRunMetrics, getRunSummary, buildAssetUrl } from '../api.js';
import { buildFlamegraphGrid, updateFlamegraphGridColumns } from '../components/flamegraphs.js';
import { formatTimestamp, getSelectedRuns, toggleRunSelection } from '../state.js';
import { navigateTo } from '../router.js';
import { escapeHtml, formatNumber, formatPercentage } from '../utils/format.js';
import { buildCasesTable } from '../components/casesTable.js';

const METRIC_KEYS = [
  { key: 'qps', label: 'QPS', formatter: formatNumber },
  { key: 'latency_ms.avg', label: 'Avg ms', formatter: formatNumber },
  { key: 'latency_ms.p50', label: 'P50 ms', formatter: formatNumber },
  { key: 'latency_ms.p90', label: 'P90 ms', formatter: formatNumber },
  { key: 'latency_ms.p99', label: 'P99 ms', formatter: formatNumber },
  { key: 'selectivity', label: 'Selectivity', formatter: formatPercentage },
  { key: 'index_build_ms', label: 'Index build ms', formatter: formatNumber },
];

export async function renderRunDetailPage(root, runId) {
  root.innerHTML = '';
  const card = document.createElement('div');
  card.className = 'section-card';
  card.innerHTML = `
    <div class="page-actions">
      <div class="left">
        <button class="ghost" data-action="back">← All runs</button>
        <h2 style="margin:0">Run ${runId}</h2>
      </div>
      <div class="right">
        <label class="badge" style="cursor:pointer;">
          <input type="checkbox" style="margin-right:0.35rem;" ${getSelectedRuns().includes(String(runId)) ? 'checked' : ''} data-run-select />
          Select for compare
        </label>
      </div>
    </div>
    <div id="detail-content"><div class="loading">Loading run details…</div></div>
  `;
  root.appendChild(card);

  card.querySelector('[data-action="back"]').addEventListener('click', () => navigateTo('#/runs'));
  const selectToggle = card.querySelector('[data-run-select]');
  selectToggle.addEventListener('change', (event) => toggleRunSelection(runId, event.target.checked));

  try {
    const [meta, metrics, summaryText] = await Promise.all([
      getRunMeta(runId),
      getRunMetrics(runId),
      getRunSummary(runId).catch(() => null),
    ]);

    renderRunDetails(card.querySelector('#detail-content'), runId, meta, metrics, summaryText);
  } catch (error) {
    card.querySelector('#detail-content').innerHTML = `<div class="alert">${error.message}</div>`;
  }
}

function renderRunDetails(container, runId, meta, metrics, summaryText) {
  const files = [
    { label: 'benchmark_results.csv', path: `${runId}/benchmark_results.csv` },
    { label: 'run_summary.txt', path: `${runId}/run_summary.txt` },
    { label: 'run_config.json', path: `${runId}/run_config.json` },
    { label: 'metrics.json', path: `${runId}/metrics.json` },
    { label: 'meta.json', path: `${runId}/meta.json` },
  ];

  const metaGrid = document.createElement('div');
  metaGrid.className = 'meta-grid';
  metaGrid.innerHTML = `
    ${renderMetaItem('Timestamp', formatTimestamp(meta.timestamp_ms || runId))}
    ${renderMetaItem('Total cases', meta.summary?.total_cases ?? '—')}
    ${renderMetaItem('Datasets', (meta.data_configs || []).join(', ') || '—')}
    ${renderMetaItem('Indexes', (meta.index_configs || []).join(', ') || '—')}
    ${renderMetaItem('Expressions', (meta.expressions || []).join(', ') || '—')}
    ${renderMetaItem('Label', meta.label || '—')}
  `;

  const downloads = document.createElement('div');
  downloads.className = 'tag-list';
  files.forEach((file) => {
    const link = document.createElement('a');
    link.href = buildAssetUrl(file.path);
    link.textContent = file.label;
    link.className = 'tag';
    link.setAttribute('download', '');
    downloads.appendChild(link);
  });

  const summaryBlock = document.createElement('div');
  summaryBlock.className = 'code-block';
  const pre = document.createElement('pre');
  pre.textContent = summaryText || 'run_summary.txt not found.';
  summaryBlock.appendChild(pre);

  const tableContainer = document.createElement('div');

  const rows = [];
  const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
  cases.forEach(([caseId, data]) => {
    rows.push({
      runId,
      caseId,
      dataConfig: data.data_config,
      indexConfig: data.index_config,
      expression: data.expression,
      metrics: data,
    });
  });

  const casesTable = buildCasesTable(rows, {
    metricKeys: METRIC_KEYS,
    showRunId: false,
    allowSelection: false,
    showFlamegraphLink: true,
    buildFlamegraphUrl: (row) => row.metrics.flamegraph ? buildAssetUrl(`${runId}/${row.metrics.flamegraph}`) : null,
  });
  tableContainer.appendChild(casesTable);
  const flamegraphSection = buildFlamegraphSection(runId, metrics);

  container.innerHTML = '';
  container.appendChild(metaGrid);

  const downloadsWrapper = document.createElement('div');
  downloadsWrapper.style.marginTop = '1.5rem';
  downloadsWrapper.innerHTML = '<h3>Artifacts</h3>';
  downloadsWrapper.appendChild(downloads);
  container.appendChild(downloadsWrapper);

  const summaryWrapper = document.createElement('div');
  summaryWrapper.style.marginTop = '1.5rem';
  summaryWrapper.innerHTML = '<h3>Run summary</h3>';
  summaryWrapper.appendChild(summaryBlock);
  container.appendChild(summaryWrapper);

  const metricsWrapper = document.createElement('div');
  metricsWrapper.style.marginTop = '1.5rem';
  metricsWrapper.innerHTML = '<h3>Case metrics</h3>';
  metricsWrapper.appendChild(tableContainer);
  container.appendChild(metricsWrapper);

  if (flamegraphSection) {
    const flameWrapper = document.createElement('div');
    flameWrapper.style.marginTop = '1.5rem';
    flameWrapper.innerHTML = '<h3>Flamegraphs</h3>';

    const controlBar = document.createElement('div');
    controlBar.className = 'section-card';
    controlBar.innerHTML = '<h3 style="margin-top:0">Flamegraphs per row</h3>';
    const segmented = document.createElement('div');
    segmented.className = 'segmented';
    let currentCols = 2;
    for (let n = 1; n <= 5; n++) {
      const btn = document.createElement('button');
      btn.className = `segmented-btn${n === currentCols ? ' active' : ''}`;
      btn.textContent = String(n);
      btn.addEventListener('click', () => {
        currentCols = n;
        segmented.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        const grid = flameWrapper.querySelector('.flamegraph-grid');
        if (grid) updateFlamegraphGridColumns(grid, currentCols);
      });
      segmented.appendChild(btn);
    }
    controlBar.appendChild(segmented);

    flameWrapper.appendChild(controlBar);
    flameWrapper.appendChild(flamegraphSection);
    container.appendChild(flameWrapper);
  }
}

function renderMetaItem(label, value) {
  const safe = escapeHtml(String(value));
  return `
    <div class="meta-item">
      <span class="label">${escapeHtml(String(label))}</span>
      <span class="value">${safe}</span>
    </div>
  `;
}

function buildFlamegraphSection(runId, metrics) {
  const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
  const withFlamegraphs = cases.filter(([, data]) => data.flamegraph);
  if (!withFlamegraphs.length) {
    return null;
  }
  const cards = withFlamegraphs.map(([caseId, data]) => ({ runId, caseId, values: data }));
  return buildFlamegraphGrid(cards);
}

// formatting helpers moved to ../utils/format.js