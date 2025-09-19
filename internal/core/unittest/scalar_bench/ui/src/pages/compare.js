import { getIndex, getRunMeta, getRunMetrics, buildAssetUrl } from '../api.js';
import { buildFlamegraphGrid, updateFlamegraphGridColumns } from '../components/flamegraphs.js';
import { buildCasesTable } from '../components/casesTable.js';
import { formatTimestamp, setSelectedRuns, getSelectedRuns, setSelectedCases, getSelectedCases } from '../state.js';
import { navigateTo } from '../router.js';
import { escapeHtml, formatNumber, formatPercentage } from '../utils/format.js';

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

  let currentCols = Math.max(1, Math.min(5, Number(cols) || 2));

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
    // Build rows for cases table
    const allRows = [];
    data.forEach(({ runId, meta, metrics }) => {
      const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
      cases.forEach(([caseId, values]) => {
        allRows.push({
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

    const rows = activeCaseKeys && activeCaseKeys.length
      ? allRows.filter((r) => activeCaseKeys.includes(`${r.runId}:${r.caseId}`))
      : allRows;

    const casesTable = buildCasesTable(rows, {
      metricKeys: METRIC_KEYS,
      showRunId: true,
      allowSelection: false,
      showFlamegraphLink: true,
      buildFlamegraphUrl: (row) => row.metrics.flamegraph ? buildAssetUrl(`${row.runId}/${row.metrics.flamegraph}`) : null,
    });
    tableContainer.innerHTML = '';
    tableContainer.appendChild(casesTable);

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
      updateFlamegraphGridColumns(existingGrid, currentCols);
    }
  }

  renderBodies();
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
  wrapper.appendChild(createCaption('Groups ordered by Dataset → Expression. Sort within groups by metric.'));
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

  const sorted = sortRowsGrouped(rows, metricKey, order);

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
        <th>Expression</th>
        <th>Index</th>
        ${METRIC_KEYS.map((metric) => `<th class="numeric">${metric.label}</th>`).join('')}
        <th>Flamegraph</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector('tbody');
  sorted.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.runId}</td>
      <td>${row.caseId}</td>
      <td>${row.dataConfig ?? '—'}</td>
      <td><code>${escapeHtml(row.expression ?? '—')}</code></td>
      <td>${row.indexConfig ?? '—'}</td>
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
  return buildFlamegraphGrid(cards, Math.max(1, Math.min(5, Number(cols) || 1)));
}

function getMetricValue(obj, path) {
  return path.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj);
}

function createCaption(text) {
  const p = document.createElement('p');
  p.className = 'caption';
  p.textContent = text;
  return p;
}