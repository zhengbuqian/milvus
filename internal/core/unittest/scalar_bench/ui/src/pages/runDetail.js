import { getRunMeta, getRunMetrics, getRunSummary, buildAssetUrl } from '../api.js';
import { formatTimestamp, getSelectedRuns, toggleRunSelection } from '../state.js';
import { navigateTo } from '../router.js';

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
  summaryBlock.innerHTML = `<pre>${summaryText || 'run_summary.txt not found.'}</pre>`;

  const metricsTable = buildMetricsTable(runId, metrics);
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
  metricsWrapper.appendChild(metricsTable);
  container.appendChild(metricsWrapper);

  if (flamegraphSection) {
    const flameWrapper = document.createElement('div');
    flameWrapper.style.marginTop = '1.5rem';
    flameWrapper.innerHTML = '<h3>Flamegraphs</h3>';
    flameWrapper.appendChild(flamegraphSection);
    container.appendChild(flameWrapper);
  }
}

function renderMetaItem(label, value) {
  return `
    <div class="meta-item">
      <span class="label">${label}</span>
      <span class="value">${value}</span>
    </div>
  `;
}

function buildMetricsTable(runId, metrics) {
  const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
  if (!cases.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'metrics.json has no cases.';
    return empty;
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'data-table';
  table.innerHTML = `
    <thead>
      <tr>
        <th>Case ID</th>
        <th>Dataset</th>
        <th>Index</th>
        <th>Expression</th>
        <th class="numeric">QPS</th>
        <th class="numeric">Avg ms</th>
        <th class="numeric">P50</th>
        <th class="numeric">P90</th>
        <th class="numeric">P99</th>
        <th class="numeric">Selectivity</th>
        <th class="numeric">Index build (ms)</th>
        <th>Flamegraph</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;
  const tbody = table.querySelector('tbody');
  cases
    .sort((a, b) => Number(b[0]) - Number(a[0]))
    .forEach(([caseId, data]) => {
      const flamegraphPath = data.flamegraph ? buildAssetUrl(`${runId}/${data.flamegraph}`) : null;
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${caseId}</td>
        <td>${data.data_config ?? '—'}</td>
        <td>${data.index_config ?? '—'}</td>
        <td><code>${escapeHtml(data.expression ?? '—')}</code></td>
        <td class="numeric">${formatNumber(data.qps)}</td>
        <td class="numeric">${formatNumber(data.latency_ms?.avg)}</td>
        <td class="numeric">${formatNumber(data.latency_ms?.p50)}</td>
        <td class="numeric">${formatNumber(data.latency_ms?.p90)}</td>
        <td class="numeric">${formatNumber(data.latency_ms?.p99)}</td>
        <td class="numeric">${formatPercentage(data.selectivity)}</td>
        <td class="numeric">${formatNumber(data.index_build_ms)}</td>
        <td>${flamegraphPath ? `<a href="${flamegraphPath}" target="_blank">View</a>` : '—'}</td>
      `;
      tbody.appendChild(tr);
    });

  wrapper.appendChild(table);
  return wrapper;
}

function buildFlamegraphSection(runId, metrics) {
  const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
  const withFlamegraphs = cases.filter(([, data]) => data.flamegraph);
  if (!withFlamegraphs.length) {
    return null;
  }
  const grid = document.createElement('div');
  grid.className = 'flamegraph-grid';
  withFlamegraphs.forEach(([caseId, data]) => {
    const card = document.createElement('div');
    card.className = 'flamegraph-card';
    const src = buildAssetUrl(`${runId}/${data.flamegraph}`);
    card.innerHTML = `
      <div class="case-name">${caseId}</div>
      <div class="text-muted small">${data.data_config} • ${data.index_config}</div>
      <div class="svg-container"><object data="${src}" type="image/svg+xml" class="flamegraph-embed"></object></div>
      <div class="links">
        <a href="${src}" target="_blank">Open</a>
        <button class="ghost" data-action="reset" data-src="${src}">Reset view</button>
        ${data.expression ? `<span class="tag">${escapeHtml(data.expression).slice(0, 40)}${
          data.expression.length > 40 ? '…' : ''
        }</span>` : ''}
      </div>
    `;
    grid.appendChild(card);
  });
  // Reset buttons reload the object to reset zoom/pan
  grid.querySelectorAll('button[data-action="reset"]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const src = btn.getAttribute('data-src');
      const wrapper = btn.closest('.flamegraph-card');
      if (!wrapper) return;
      const obj = wrapper.querySelector('object.flamegraph-embed');
      if (!obj) return;
      obj.setAttribute('data', '');
      // force reflow then set back
      setTimeout(() => obj.setAttribute('data', src), 0);
    });
  });
  return grid;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatNumber(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return '—';
  if (typeof value !== 'number') value = Number(value);
  if (!Number.isFinite(value)) return String(value);
  if (Math.abs(value) >= 100) {
    return value.toFixed(0);
  }
  return value.toFixed(2);
}

function formatPercentage(value) {
  if (value === undefined || value === null) return '—';
  if (typeof value !== 'number') value = Number(value);
  if (!Number.isFinite(value)) return String(value);
  return `${(value * 100).toFixed(2)}%`;
}