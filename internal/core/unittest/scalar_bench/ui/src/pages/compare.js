import { getIndex, getRunMeta, getRunMetrics, buildAssetUrl } from '../api.js';
import { formatTimestamp, setSelectedRuns, getSelectedRuns, setSelectedCases, getSelectedCases } from '../state.js';
import { navigateTo } from '../router.js';

const METRIC_KEYS = [
  { key: 'qps', label: 'QPS', formatter: formatNumber },
  { key: 'latency_ms.avg', label: 'Avg ms', formatter: formatNumber },
  { key: 'latency_ms.p50', label: 'P50 ms', formatter: formatNumber },
  { key: 'latency_ms.p90', label: 'P90 ms', formatter: formatNumber },
  { key: 'latency_ms.p99', label: 'P99 ms', formatter: formatNumber },
  { key: 'selectivity', label: 'Selectivity', formatter: formatPercentage },
  { key: 'index_build_ms', label: 'Index build ms', formatter: formatNumber },
  { key: 'memory.index_mb', label: 'Index MB', formatter: formatNumber },
  { key: 'memory.exec_peak_mb', label: 'Exec peak MB', formatter: formatNumber },
  { key: 'cpu_pct', label: 'CPU %', formatter: formatNumber },
];

export async function renderComparePage(root, params) {
  root.innerHTML = '';
  const card = document.createElement('div');
  card.className = 'section-card';
  card.innerHTML = `
    <div class="page-actions">
      <div class="left">
        <h2 style="margin:0">Compare</h2>
      </div>
      <div class="right">
        <button class="ghost" data-action="back">← Back to runs</button>
      </div>
    </div>
    <div id="compare-content"><div class="loading">Preparing comparison…</div></div>
  `;
  root.appendChild(card);
  card.querySelector('[data-action="back"]').addEventListener('click', () => navigateTo('#/runs'));

  try {
    const indexData = await getIndex();
    const runs = Array.isArray(indexData?.runs) ? indexData.runs : [];
    const initialRuns = deriveInitialRuns(params.runs, runs);
    const initialCases = deriveInitialCases(params.cases);

    await renderComparison(card.querySelector('#compare-content'), runs, initialRuns, params.metrics, initialCases, params.cols);
  } catch (error) {
    card.querySelector('#compare-content').innerHTML = `<div class="alert">${error.message}</div>`;
  }
}

function deriveInitialRuns(paramRuns, runs) {
  if (paramRuns) {
    const ids = paramRuns
      .split(',')
      .map((id) => id.trim())
      .filter((id) => id && runs.find((run) => String(run.id) === id));
    if (ids.length) {
      setSelectedRuns(ids);
      return ids;
    }
  }
  const stored = getSelectedRuns();
  if (stored.length) {
    return stored.filter((id) => runs.find((run) => String(run.id) === id));
  }
  return runs.slice(0, 2).map((run) => String(run.id));
}

function deriveInitialCases(paramCases) {
  if (!paramCases) return getSelectedCases();
  const arr = paramCases
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  setSelectedCases(arr);
  return arr;
}

async function renderComparison(container, allRuns, activeRunIds, metricKey, activeCaseKeys, cols) {
  container.innerHTML = '';

  // Top controls panel
  const topPanel = document.createElement('div');
  topPanel.className = 'section-card';

  const runSelector = buildRunSelector(allRuns, activeRunIds, async (selected) => {
    setSelectedRuns(selected);
    navigateTo(`#/compare?runs=${selected.join(',')}${metricKey ? `&metrics=${metricKey}` : ''}${cols ? `&cols=${cols}` : ''}`);
  });

  topPanel.appendChild(runSelector);

  // Local state for buttons (no full page refresh)
  let currentMetric = metricKey || '';
  let currentOrder = 'desc';
  let currentCols = Math.max(1, Math.min(5, Number(cols) || 2));

  const metricButtons = buildMetricButtons(currentMetric, (newMetric, updateActive) => {
    currentMetric = newMetric || '';
    if (typeof updateActive === 'function') updateActive(newMetric);
    renderBodies();
  });
  topPanel.appendChild(metricButtons);

  const orderButtons = buildOrderButtons(currentOrder, (newOrder, updateActive) => {
    currentOrder = newOrder === 'asc' ? 'asc' : 'desc';
    if (typeof updateActive === 'function') updateActive(currentOrder);
    renderBodies();
  });
  topPanel.appendChild(orderButtons);

  const columnsButtons = buildColumnsButtons(currentCols, (newCols, updateActive) => {
    currentCols = Math.max(1, Math.min(5, Number(newCols) || 1));
    if (typeof updateActive === 'function') updateActive(currentCols);
    renderBodies();
  });
  topPanel.appendChild(columnsButtons);

  const data = await Promise.all(
    activeRunIds.map(async (runId) => {
      const meta = await getRunMeta(runId);
      const metrics = await getRunMetrics(runId);
      return { runId, meta, metrics };
    }),
  );

  const summary = buildSummaryPanel(data);
  topPanel.appendChild(summary);

  container.appendChild(topPanel);

  if (!activeRunIds.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'Select one or more runs to compare.';
    container.appendChild(empty);
    return;
  }

  const tableContainer = document.createElement('div');
  const flamesContainer = document.createElement('div');
  flamesContainer.style.marginTop = '1.5rem';
  container.appendChild(tableContainer);
  container.appendChild(flamesContainer);

  function renderBodies() {
    // Table
    const table = activeCaseKeys && activeCaseKeys.length
      ? buildCaseComparisonTable(data, activeCaseKeys, currentMetric, currentOrder)
      : buildComparisonTable(data, currentMetric, currentOrder);
    tableContainer.innerHTML = '';
    tableContainer.appendChild(table);

    // Flamegraphs
    const existingGrid = flamesContainer.querySelector('.flamegraph-grid');
    if (!existingGrid) {
      const flamegraphs = buildFlamegraphGallery(data, activeCaseKeys, currentCols);
      flamesContainer.innerHTML = '';
      if (flamegraphs) {
        const flameWrapper = document.createElement('div');
        flameWrapper.innerHTML = '<h3>Flamegraphs</h3>';
        flameWrapper.appendChild(flamegraphs);
        flamesContainer.appendChild(flameWrapper);
      }
    } else {
      const colNum = Math.max(1, Math.min(5, Number(currentCols) || 1));
      existingGrid.className = `flamegraph-grid columns-${colNum}`;
    }
  }

  renderBodies();
}

function buildCaseComparisonTable(data, activeCaseKeys, metricKey, order) {
  const rows = [];
  const active = new Set(activeCaseKeys.map(String));
  data.forEach(({ runId, meta, metrics }) => {
    const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
    cases.forEach(([caseId, values]) => {
      const key = `${runId}:${caseId}`;
      if (active.has(key)) {
        rows.push({
          runId,
          caseId,
          dataConfig: values.data_config,
          indexConfig: values.index_config,
          expression: values.expression,
          metrics: values,
          timestamp: meta.timestamp_ms || runId,
        });
      }
    });
  });

  if (!rows.length) {
    const div = document.createElement('div');
    div.className = 'empty-state';
    div.textContent = 'No selected cases to compare.';
    return div;
  }

  if (metricKey) {
    rows.sort((a, b) => {
      const av = getMetricValue(a.metrics, metricKey);
      const bv = getMetricValue(b.metrics, metricKey);
      return order === 'asc' ? av - bv : bv - av;
    });
  } else {
    rows.sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'data-table';
  table.innerHTML = `
    <thead>
      <tr>
        <th>Run</th>
        <th>Case</th>
        <th>Dataset</th>
        <th>Index</th>
        <th>Expression</th>
        ${METRIC_KEYS.map((metric) => `<th class="numeric">${metric.label}</th>`).join('')}
        <th>Flamegraph</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;
  const tbody = table.querySelector('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.runId}</td>
      <td>${row.caseId}</td>
      <td>${row.dataConfig ?? '—'}</td>
      <td>${row.indexConfig ?? '—'}</td>
      <td><code>${escapeHtml(row.expression ?? '—')}</code></td>
      ${METRIC_KEYS.map((metric) => {
        const value = getMetricValue(row.metrics, metric.key);
        const formatted = metric.formatter(value);
        const highlight = metricKey === metric.key ? ' style="background: rgba(56,189,248,0.12);"' : '';
        return `<td class="numeric"${highlight}>${formatted}</td>`;
      }).join('')}
      <td>${row.metrics.flamegraph ? `<a href="${buildAssetUrl(`${row.runId}/${row.metrics.flamegraph}`)}" target="_blank">View</a>` : '—'}</td>
    `;
    tbody.appendChild(tr);
  });

  wrapper.appendChild(table);
  return wrapper;
}

function buildRunSelector(allRuns, activeRunIds, onChange) {
  const wrapper = document.createElement('div');
  wrapper.className = 'section-card';
  wrapper.innerHTML = '<h3 style="margin-top:0">Runs</h3>';
  const list = document.createElement('div');
  list.style.display = 'flex';
  list.style.flexDirection = 'column';
  list.style.gap = '0.5rem';

  allRuns
    .slice()
    .sort((a, b) => Number(b.timestamp_ms || b.id || 0) - Number(a.timestamp_ms || a.id || 0))
    .forEach((run) => {
      const id = String(run.id);
      const item = document.createElement('label');
      item.className = 'badge';
      item.style.justifyContent = 'space-between';
      item.style.cursor = 'pointer';
      item.innerHTML = `
        <span>
          <input type="checkbox" ${activeRunIds.includes(id) ? 'checked' : ''} data-run="${id}" />
          <strong>${id}</strong>
        </span>
        <span class="text-muted small">${formatTimestamp(run.timestamp_ms || run.id)}</span>
      `;
      list.appendChild(item);
    });

  wrapper.appendChild(list);

  wrapper.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener('change', () => {
      const selected = Array.from(wrapper.querySelectorAll('input[type="checkbox"]'))
        .filter((el) => el.checked)
        .map((el) => el.dataset.run);
      onChange(selected);
    });
  });

  return wrapper;
}

function buildMetricButtons(selectedKey, onChange) {
  const wrapper = document.createElement('div');
  wrapper.className = 'section-card';
  wrapper.innerHTML = '<h3 style="margin-top:0">Sort metric</h3>';
  const bar = document.createElement('div');
  bar.className = 'segmented';

  const noneBtn = document.createElement('button');
  noneBtn.className = `segmented-btn${!selectedKey ? ' active' : ''}`;
  noneBtn.textContent = 'None';
  noneBtn.addEventListener('click', () => onChange('', (newKey) => {
    bar.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
    noneBtn.classList.add('active');
  }));
  bar.appendChild(noneBtn);

  METRIC_KEYS.forEach((m) => {
    const btn = document.createElement('button');
    btn.className = `segmented-btn${selectedKey === m.key ? ' active' : ''}`;
    btn.textContent = m.label;
    btn.title = m.key;
    btn.addEventListener('click', () => onChange(m.key, () => {
      bar.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
    }));
    bar.appendChild(btn);
  });

  wrapper.appendChild(bar);
  wrapper.appendChild(createCaption('Sort cases by metric (desc).'));
  return wrapper;
}

function buildOrderButtons(selectedOrder, onChange) {
  const wrapper = document.createElement('div');
  wrapper.className = 'section-card';
  wrapper.innerHTML = '<h3 style="margin-top:0">Order</h3>';
  const bar = document.createElement('div');
  bar.className = 'segmented';

  const descBtn = document.createElement('button');
  descBtn.className = `segmented-btn${selectedOrder !== 'asc' ? ' active' : ''}`;
  descBtn.textContent = 'Desc';
  descBtn.addEventListener('click', () => {
    onChange('desc', () => {
      bar.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
      descBtn.classList.add('active');
    });
  });
  bar.appendChild(descBtn);

  const ascBtn = document.createElement('button');
  ascBtn.className = `segmented-btn${selectedOrder === 'asc' ? ' active' : ''}`;
  ascBtn.textContent = 'Asc';
  ascBtn.addEventListener('click', () => {
    onChange('asc', () => {
      bar.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
      ascBtn.classList.add('active');
    });
  });
  bar.appendChild(ascBtn);

  wrapper.appendChild(bar);
  wrapper.appendChild(createCaption('Ascending or descending when sorting by metric.'));
  return wrapper;
}

function buildColumnsButtons(selectedCols, onChange) {
  const wrapper = document.createElement('div');
  wrapper.className = 'section-card';
  wrapper.innerHTML = '<h3 style="margin-top:0">Flamegraphs per row</h3>';
  const bar = document.createElement('div');
  bar.className = 'segmented';
  const current = Math.max(1, Math.min(5, Number(selectedCols) || 1));
  for (let n = 1; n <= 5; n++) {
    const btn = document.createElement('button');
    btn.className = `segmented-btn${n === current ? ' active' : ''}`;
    btn.textContent = String(n);
    btn.addEventListener('click', () => onChange(n, () => {
      bar.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
    }));
    bar.appendChild(btn);
  }
  wrapper.appendChild(bar);
  wrapper.appendChild(createCaption('Arrange 1-5 flamegraphs per row. Height fixed at 400px with vertical scroll.'));
  return wrapper;
}

function buildSummaryPanel(data) {
  const wrapper = document.createElement('div');
  wrapper.className = 'section-card';
  wrapper.innerHTML = '<h3 style="margin-top:0">Summary</h3>';

  const chips = document.createElement('div');
  chips.style.display = 'flex';
  chips.style.flexDirection = 'column';
  chips.style.gap = '0.5rem';

  data.forEach(({ runId, meta }) => {
    const chip = document.createElement('div');
    chip.className = 'run-chip';
    chip.innerHTML = `
      <strong>${runId}</strong>
      <span class="text-muted small">${meta.summary?.total_cases ?? '—'} cases</span>
    `;
    chips.appendChild(chip);
  });

  wrapper.appendChild(chips);
  return wrapper;
}

function buildComparisonTable(data, metricKey, order) {
  const rows = [];
  data.forEach(({ runId, meta, metrics }) => {
    const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
    cases.forEach(([caseId, values]) => {
      rows.push({
        runId,
        caseId,
        dataConfig: values.data_config,
        indexConfig: values.index_config,
        expression: values.expression,
        metrics: values,
        timestamp: meta.timestamp_ms || runId,
      });
    });
  });

  if (metricKey) {
    rows.sort((a, b) => {
      const av = getMetricValue(a.metrics, metricKey);
      const bv = getMetricValue(b.metrics, metricKey);
      return order === 'asc' ? av - bv : bv - av;
    });
  } else {
    rows.sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'data-table';
  table.innerHTML = `
    <thead>
      <tr>
        <th>Run</th>
        <th>Case</th>
        <th>Dataset</th>
        <th>Index</th>
        <th>Expression</th>
        ${METRIC_KEYS.map((metric) => `<th class="numeric">${metric.label}</th>`).join('')}
        <th>Flamegraph</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.runId}</td>
      <td>${row.caseId}</td>
      <td>${row.dataConfig ?? '—'}</td>
      <td>${row.indexConfig ?? '—'}</td>
      <td><code>${escapeHtml(row.expression ?? '—')}</code></td>
      ${METRIC_KEYS.map((metric) => {
        const value = getMetricValue(row.metrics, metric.key);
        const formatted = metric.formatter(value);
        const highlight = metricKey === metric.key ? ' style="background: rgba(56,189,248,0.12);"' : '';
        return `<td class="numeric"${highlight}>${formatted}</td>`;
      }).join('')}
      <td>${row.metrics.flamegraph ? `<a href="${buildAssetUrl(`${row.runId}/${row.metrics.flamegraph}`)}" target="_blank">View</a>` : '—'}</td>
    `;
    tbody.appendChild(tr);
  });

  wrapper.appendChild(table);
  return wrapper;
}

function buildFlamegraphGallery(data, activeCaseKeys, cols) {
  const cards = [];
  const active = activeCaseKeys && activeCaseKeys.length ? new Set(activeCaseKeys.map(String)) : null;
  data.forEach(({ runId, metrics }) => {
    const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
    cases
      .filter(([caseId, values]) => {
        if (!values.flamegraph) return false;
        if (!active) return true;
        const key = `${runId}:${caseId}`;
        return active.has(key);
      })
      .forEach(([caseId, values]) => {
        cards.push({ runId, caseId, values });
      });
  });
  if (!cards.length) return null;

  const grid = document.createElement('div');
  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  grid.className = `flamegraph-grid columns-${colNum}`;
  cards.forEach((cardData) => {
    const card = document.createElement('div');
    card.className = 'flamegraph-card';
    card.innerHTML = `
      <div class="svg-container limit-400"><object data="${buildAssetUrl(`${cardData.runId}/${cardData.values.flamegraph}`)}" type="image/svg+xml" class="flamegraph-embed tall-embed"></object></div>
      <div class="text-muted small">Run ${cardData.runId} • Case ${cardData.caseId}</div>
      <div class="links">
        <a href="${buildAssetUrl(`${cardData.runId}/${cardData.values.flamegraph}`)}" target="_blank">Open</a>
        <button class="ghost" data-action="reset" data-src="${buildAssetUrl(`${cardData.runId}/${cardData.values.flamegraph}`)}">Reset view</button>
        <span class="tag">${cardData.values.data_config ?? 'n/a'}</span>
        <span class="tag">${cardData.values.index_config ?? 'n/a'}</span>
      </div>
    `;
    // Default scroll to bottom after SVG loads
    const obj = card.querySelector('object.flamegraph-embed');
    obj.addEventListener('load', () => {
      const cont = card.querySelector('.svg-container');
      if (cont) {
        cont.scrollTop = cont.scrollHeight;
      }
    });
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
      setTimeout(() => obj.setAttribute('data', src), 0);
    });
  });
  return grid;
}

function getMetricValue(obj, path) {
  return path.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj);
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

function createCaption(text) {
  const p = document.createElement('p');
  p.className = 'caption';
  p.textContent = text;
  return p;
}