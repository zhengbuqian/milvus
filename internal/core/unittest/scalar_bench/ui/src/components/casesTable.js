import { escapeHtml } from '../utils/format.js';

// rows: Array<{ runId?, caseId, dataConfig, indexConfig, expression, metrics }>
// options: {
//   metricKeys: Array<{ key, label, formatter }>,
//   showRunId?: boolean,
//   allowSelection?: boolean,
//   isSelected?: (key: string) => boolean,
//   getSelectionKey?: (row) => string,
//   onToggleSelect?: (key: string, checked: boolean, row) => void,
//   showFlamegraphLink?: boolean,
//   buildFlamegraphUrl?: (row) => string | null
// }
export function buildCasesTable(rows, options) {
  const opts = {
    metricKeys: [],
    showRunId: false,
    allowSelection: false,
    isSelected: () => false,
    getSelectionKey: (row) => String(row.caseId),
    onToggleSelect: () => {},
    showFlamegraphLink: true,
    buildFlamegraphUrl: () => null,
    ...options,
  };

  let currentMetric = '';
  let currentOrder = 'desc';

  const wrapper = document.createElement('div');
  wrapper.className = 'table-scroll';
  const table = document.createElement('table');
  table.className = 'data-table';

  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  if (opts.allowSelection) {
    const th = document.createElement('th');
    headRow.appendChild(th);
  }
  if (opts.showRunId) {
    const th = document.createElement('th');
    th.textContent = 'Run';
    headRow.appendChild(th);
  }
  // Fixed info columns: Case, Dataset, Expression, Index
  addStaticHeader(headRow, 'Case');
  addStaticHeader(headRow, 'Dataset');
  addStaticHeader(headRow, 'Expression');
  addStaticHeader(headRow, 'Index');

  // Metric columns with click sorting
  opts.metricKeys.forEach((m) => {
    const th = document.createElement('th');
    th.className = 'numeric';
    th.textContent = m.label;
    th.style.cursor = 'pointer';
    th.setAttribute('data-metric-key', m.key);
    th.setAttribute('data-label', m.label);
    th.addEventListener('click', () => {
      const key = m.key;
      if (currentMetric === key) {
        currentOrder = currentOrder === 'asc' ? 'desc' : 'asc';
      } else {
        currentMetric = key;
        currentOrder = 'desc';
      }
      renderBody();
      updateHeaderActive();
    });
    headRow.appendChild(th);
  });

  if (opts.showFlamegraphLink) {
    addStaticHeader(headRow, 'Flamegraph');
  }

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  table.appendChild(tbody);
  wrapper.appendChild(table);

  function updateHeaderActive() {
    thead.querySelectorAll('th').forEach((th) => {
      const key = th.getAttribute('data-metric-key');
      if (!key) return;
      const label = th.getAttribute('data-label') || th.textContent || '';
      let arrow = '';
      if (key === currentMetric) {
        th.style.background = 'rgba(56,189,248,0.12)';
        th.title = key;
        arrow = currentOrder === 'asc' ? ' ↑' : ' ↓';
      } else {
        th.style.background = '';
        th.title = key;
      }
      th.textContent = label + arrow;
    });
  }

  function renderBody() {
    const sorted = sortRowsGrouped(rows, currentMetric, currentOrder);
    tbody.innerHTML = '';
    sorted.forEach((row) => {
      const tr = document.createElement('tr');
      if (opts.allowSelection) {
        const td = document.createElement('td');
        const key = opts.getSelectionKey(row);
        td.innerHTML = `<input type="checkbox" ${opts.isSelected(key) ? 'checked' : ''} />`;
        const cb = td.querySelector('input');
        cb.addEventListener('change', () => opts.onToggleSelect(key, cb.checked, row));
        tr.appendChild(td);
      }
      if (opts.showRunId) {
        addCell(tr, row.runId);
      }
      addCell(tr, row.caseId);
      addCell(tr, row.dataConfig ?? '—');
      addCell(tr, `<code>${escapeHtml(row.expression ?? '—')}</code>`, true);
      addCell(tr, row.indexConfig ?? '—');

      opts.metricKeys.forEach((m) => {
        const value = getMetricValue(row.metrics, m.key);
        const formatted = m.formatter ? m.formatter(value) : value;
        addNumericCell(tr, formatted);
      });

      if (opts.showFlamegraphLink) {
        const url = opts.buildFlamegraphUrl ? opts.buildFlamegraphUrl(row) : null;
        addCell(tr, url ? `<a href="${url}" target="_blank">View</a>` : '—', true);
      }

      tbody.appendChild(tr);
    });
  }

  renderBody();
  updateHeaderActive();
  return wrapper;
}

function addStaticHeader(headRow, label) {
  const th = document.createElement('th');
  th.textContent = label;
  headRow.appendChild(th);
}

function addCell(tr, content, isHtml = false) {
  const td = document.createElement('td');
  if (isHtml) td.innerHTML = content; else td.textContent = String(content);
  tr.appendChild(td);
}

function addNumericCell(tr, content) {
  const td = document.createElement('td');
  td.className = 'numeric';
  td.textContent = String(content);
  tr.appendChild(td);
}

function getMetricValue(obj, path) {
  return path ? path.split('.').reduce((acc, key) => (acc ? acc[key] : undefined), obj) : undefined;
}

function sortRowsGrouped(rows, metricKey, order) {
  const groups = new Map();
  rows.forEach((r) => {
    const dataset = r.dataConfig || '';
    const expr = r.expression || '';
    const key = `${dataset}\u0000${expr}`;
    if (!groups.has(key)) groups.set(key, { dataset, expression: expr, items: [] });
    groups.get(key).items.push(r);
  });

  const groupList = Array.from(groups.values());
  groupList.sort((a, b) => {
    const ds = String(a.dataset).localeCompare(String(b.dataset));
    if (ds !== 0) return ds;
    return String(a.expression).localeCompare(String(b.expression));
  });

  const dir = order === 'asc' ? 1 : -1;
  const result = [];
  groupList.forEach((g) => {
    if (metricKey) {
      g.items.sort((a, b) => {
        const av = Number(getMetricValue(a.metrics, metricKey));
        const bv = Number(getMetricValue(b.metrics, metricKey));
        if (Number.isNaN(av) && Number.isNaN(bv)) return 0;
        if (Number.isNaN(av)) return 1;
        if (Number.isNaN(bv)) return -1;
        return dir * (av - bv);
      });
    } else {
      // default within-group order by caseId desc
      g.items.sort((a, b) => Number(b.caseId) - Number(a.caseId));
    }
    result.push(...g.items);
  });
  return result;
}


