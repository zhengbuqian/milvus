import { getIndex, getRunMeta, getRunMetrics } from '../api.js';
import { formatTimestamp, getSelectedRuns, toggleRunSelection, getSelectedCases, toggleCaseSelection } from '../state.js';
import { navigateTo } from '../router.js';
import { escapeHtml, formatNumber } from '../utils/format.js';
import { buildCasesTable } from '../components/casesTable.js';

export async function renderRunsPage(root) {
  root.innerHTML = '';

  const container = document.createElement('div');
  container.className = 'section-card';
  container.innerHTML = `
    <div class="page-actions">
      <div class="left">
        <h2 style="margin:0">Benchmark Runs</h2>
        <span class="badge">Selected runs: <span id="selected-count">${getSelectedRuns().length}</span></span>
        <span class="badge" style="margin-left:0.5rem">Selected cases: <span id="selected-cases-count">${getSelectedCases().length}</span></span>
      </div>
      <div class="right">
        <button class="primary" id="compare-runs-btn" ${getSelectedRuns().length < 2 ? 'disabled' : ''}>Compare runs</button>
        <button class="secondary" id="compare-cases-btn" ${getSelectedCases().length < 2 ? 'disabled' : ''}>Compare cases</button>
        <button class="ghost" id="refresh-btn">Refresh</button>
      </div>
    </div>
    <p class="caption">Results are read from <code>index.json</code> and per-run folders in the configured results directory.</p>
    <div id="runs-content"></div>
  `;
  root.appendChild(container);

  const runsContent = container.querySelector('#runs-content');
  runsContent.innerHTML = '<div class="loading">Loading runs…</div>';

  container.querySelector('#refresh-btn').addEventListener('click', () => renderRunsPage(root));
  container.querySelector('#compare-runs-btn').addEventListener('click', () => {
    const selected = getSelectedRuns();
    if (selected.length >= 2) {
      navigateTo(`#/compare?runs=${selected.join(',')}`);
    }
  });
  container.querySelector('#compare-cases-btn').addEventListener('click', () => {
    const selectedCases = getSelectedCases();
    if (selectedCases.length >= 2) {
      navigateTo(`#/compare?cases=${encodeURIComponent(selectedCases.join(','))}`);
    }
  });

  try {
    const indexData = await getIndex();
    const runs = Array.isArray(indexData?.runs) ? indexData.runs : [];
    if (!runs.length) {
      runsContent.innerHTML = '<div class="empty-state">No runs found. Execute the benchmark to populate results.</div>';
      return;
    }

    const runsWithMeta = await Promise.all(
      runs.map(async (run) => {
        try {
          const [meta, metrics] = await Promise.all([
            getRunMeta(run.id),
            getRunMetrics(run.id).catch(() => null),
          ]);
          return { ...run, meta, metrics };
        } catch (error) {
          return { ...run, meta: null, metrics: null, error: error.message };
        }
      }),
    );

    renderRunsTable(runsContent, runsWithMeta);
  } catch (error) {
    runsContent.innerHTML = `<div class="alert">${error.message}</div>`;
  }
}

function renderRunsTable(container, runs) {
  const filters = buildFilters(runs);
  const controls = document.createElement('div');
  controls.className = 'filter-bar';
  controls.innerHTML = `
    <label>Dataset
      <select data-filter="dataset">
        <option value="">All</option>
        ${filters.datasets.map((d) => `<option value="${d}">${d}</option>`).join('')}
      </select>
    </label>
    <label>Expression
      <select data-filter="expression">
        <option value="">All</option>
        ${filters.expressions.map((d) => `<option value="${d}">${d}</option>`).join('')}
      </select>
    </label>
    <label>Index
      <select data-filter="index">
        <option value="">All</option>
        ${filters.indexes.map((d) => `<option value="${d}">${d}</option>`).join('')}
      </select>
    </label>
    <label>Search
      <input type="search" placeholder="Run id, label…" data-filter="search" />
    </label>
  `;

  const tableWrapper = document.createElement('div');
  tableWrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'data-table';
  table.innerHTML = `
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
    <tbody></tbody>
  `;
  tableWrapper.appendChild(table);

  container.innerHTML = '';
  container.appendChild(controls);
  container.appendChild(tableWrapper);

  const tbody = table.querySelector('tbody');
  const selectedCount = document.getElementById('selected-count');
  const selectedCasesCount = document.getElementById('selected-cases-count');

  function applyFilters() {
    const dataset = controls.querySelector('[data-filter="dataset"]').value;
    const index = controls.querySelector('[data-filter="index"]').value;
    const expression = controls.querySelector('[data-filter="expression"]').value;
    const search = controls.querySelector('[data-filter="search"]').value.toLowerCase();

    const filtered = runs
      .slice()
      .sort((a, b) => Number(b.timestamp_ms || b.id || 0) - Number(a.timestamp_ms || a.id || 0))
      .filter((run) => {
        const meta = run.meta || {};
        if (dataset && !(meta.data_configs || []).includes(dataset)) return false;
        if (index && !(meta.index_configs || []).includes(index)) return false;
        if (expression && !(meta.expressions || []).includes(expression)) return false;
        if (search) {
          const text = [run.id, run.label, meta.label, meta.summary?.total_cases]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();
          if (!text.includes(search)) return false;
        }
        return true;
      });

    tbody.innerHTML = '';
    filtered.forEach((run) => {
      const tr = document.createElement('tr');
      const meta = run.meta;
      const isSelected = getSelectedRuns().includes(String(run.id));
      tr.innerHTML = `
        <td>
          <button class="ghost" data-action="expand" data-run="${run.id}" title="Expand cases">+</button>
          <input type="checkbox" ${isSelected ? 'checked' : ''} data-run-select="${run.id}" />
        </td>
        <td>${run.id}</td>
        <td>${formatTimestamp(run.timestamp_ms || run.id)}</td>
        <td class="numeric">${meta?.summary?.total_cases ?? '—'}</td>
        <td>${renderTagList(meta?.data_configs)}</td>
        <td>${renderTagList(meta?.expressions)}</td>
        <td>${renderTagList(meta?.index_configs)}</td>
        <td>${meta?.label || run.label || '—'}</td>
        <td>
          <div class="table-actions">
            <button class="secondary" data-action="view" data-run="${run.id}">Details</button>
            ${run.error ? `<span class="badge danger" title="${run.error}">meta.json failed</span>` : ''}
          </div>
        </td>
      `;
      tbody.appendChild(tr);

      // Expandable row for cases
      const casesRow = document.createElement('tr');
      const casesCell = document.createElement('td');
      casesCell.colSpan = 9;
      casesCell.style.padding = '0';
      casesRow.appendChild(casesCell);
      casesRow.style.display = 'none';
      tbody.appendChild(casesRow);

      const expandBtn = tr.querySelector('button[data-action="expand"]');
      expandBtn.addEventListener('click', () => {
        if (casesRow.style.display === 'none') {
          casesRow.style.display = '';
          renderCasesList(casesCell, run.id, run.metrics);
          expandBtn.textContent = '−';
        } else {
          casesRow.style.display = 'none';
          casesCell.innerHTML = '';
          expandBtn.textContent = '+';
        }
      });
    });

    tbody.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
      checkbox.addEventListener('change', (event) => {
        if (event.target.dataset.runSelect) {
          toggleRunSelection(event.target.dataset.runSelect, event.target.checked);
          selectedCount.textContent = getSelectedRuns().length;
          const compareRunsBtn = document.getElementById('compare-runs-btn');
          if (compareRunsBtn) {
            compareRunsBtn.disabled = getSelectedRuns().length < 2;
          }
        }
      });
    });

    tbody.querySelectorAll('button[data-action="view"]').forEach((btn) => {
      btn.addEventListener('click', () => navigateTo(`#/run/${btn.dataset.run}`));
    });
  }

  controls.querySelectorAll('select, input').forEach((el) => {
    el.addEventListener('input', applyFilters);
  });

  applyFilters();
}

function renderCasesList(container, runId, metrics) {
  container.innerHTML = '';
  const cases = metrics?.cases ? Object.entries(metrics.cases) : [];
  if (!cases.length) {
    container.innerHTML = '<div class="empty-state" style="margin:0.5rem 1rem">No cases found in metrics.json.</div>';
    return;
  }

  const rows = cases.map(([caseId, data]) => ({
    runId,
    caseId,
    dataConfig: data.data_config,
    indexConfig: data.index_config,
    expression: data.expression,
    metrics: data,
  }));

  const casesTable = buildCasesTable(rows, {
    metricKeys: [
      { key: 'qps', label: 'QPS', formatter: formatNumber },
      { key: 'latency_ms.p99', label: 'P99', formatter: formatNumber },
    ],
    showRunId: false,
    allowSelection: true,
    isSelected: (key) => getSelectedCases().includes(key),
    getSelectionKey: (row) => `${row.runId}:${row.caseId}`,
    onToggleSelect: (key, checked, row) => {
      const [rid, cid] = key.split(':');
      toggleCaseSelection(rid, cid, checked);
      const selectedCasesCount = document.getElementById('selected-cases-count');
      if (selectedCasesCount) selectedCasesCount.textContent = getSelectedCases().length;
      const compareCasesBtn = document.getElementById('compare-cases-btn');
      if (compareCasesBtn) compareCasesBtn.disabled = getSelectedCases().length < 2;
    },
    showFlamegraphLink: false,
  });

  container.appendChild(casesTable);
}

function buildFilters(runs) {
  const datasets = new Set();
  const indexes = new Set();
  const expressions = new Set();
  runs.forEach((run) => {
    (run.meta?.data_configs || []).forEach((item) => datasets.add(item));
    (run.meta?.index_configs || []).forEach((item) => indexes.add(item));
    (run.meta?.expressions || []).forEach((item) => expressions.add(item));
  });
  return {
    datasets: Array.from(datasets).sort(),
    indexes: Array.from(indexes).sort(),
    expressions: Array.from(expressions).sort(),
  };
}

function renderTagList(items) {
  if (!items || !items.length) {
    return '<span class="text-muted">—</span>';
  }
  return `<div class="tag-list">${items
    .map((item) => `<span class="tag">${escapeHtml(String(item))}</span>`)
    .join('')}</div>`;
}