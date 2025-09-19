import { initRouter, registerRoute, navigateTo } from './router.js';
import { renderRunsPage } from './pages/runs.js';
import { renderRunDetailPage } from './pages/runDetail.js';
import { renderComparePage } from './pages/compare.js';
import { getBasePath, setBasePath, defaults } from './state.js';

const app = document.getElementById('app');

function renderLayout() {
  app.innerHTML = `
    <header class="app-header">
      <div class="title">Milvus Scalar Bench</div>
      <div class="subtitle">Visualise scalar filter benchmark results from _artifacts/results</div>
      <nav>
        <a href="#/runs">Runs</a>
        <a href="#/compare">Compare</a>
      </nav>
    </header>
    <main id="page-content"></main>
  `;

  renderSettingsBadge();
}

function renderSettingsBadge() {
  const subtitle = app.querySelector('.subtitle');
  if (!subtitle) return;
  const badge = document.createElement('span');
  badge.className = 'badge';
  badge.textContent = `Data root: ${getBasePath()}`;
  badge.style.marginLeft = '0.75rem';
  badge.style.cursor = 'pointer';
  badge.title = 'Click to configure results directory';
  subtitle.appendChild(badge);

  badge.addEventListener('click', () => {
    showSettingsPanel();
  });

  document.addEventListener('scalar-bench:base-path-changed', (event) => {
    badge.textContent = `Data root: ${event.detail}`;
  });
}

function showSettingsPanel() {
  const existing = document.getElementById('settings-panel');
  if (existing) {
    existing.remove();
  }
  const panel = document.createElement('div');
  panel.id = 'settings-panel';
  panel.className = 'settings-panel';
  panel.innerHTML = `
    <h3 style="margin:0">Results directory</h3>
    <p class="text-muted small">Provide the relative or absolute path that contains <code>index.json</code> and run folders. Default is <code>${defaults.basePath}</code>.</p>
    <input type="text" value="${getBasePath()}" placeholder="../_artifacts/results/" />
    <div class="actions">
      <button class="ghost" data-action="cancel">Cancel</button>
      <button class="primary" data-action="save">Save</button>
    </div>
  `;

  const main = document.querySelector('main');
  const container = document.createElement('div');
  container.style.position = 'fixed';
  container.style.top = '0';
  container.style.left = '0';
  container.style.right = '0';
  container.style.bottom = '0';
  container.style.backdropFilter = 'blur(6px)';
  container.style.background = 'rgba(2, 6, 23, 0.6)';
  container.style.display = 'flex';
  container.style.alignItems = 'flex-start';
  container.style.justifyContent = 'center';
  container.style.paddingTop = '10vh';
  container.style.zIndex = '100';
  container.appendChild(panel);
  document.body.appendChild(container);

  panel.querySelector('[data-action="cancel"]').addEventListener('click', () => {
    container.remove();
  });

  panel.querySelector('[data-action="save"]').addEventListener('click', () => {
    const input = panel.querySelector('input');
    setBasePath(input.value.trim() || defaults.basePath);
    container.remove();
    navigateTo(window.location.hash || '#/runs');
  });

  container.addEventListener('click', (event) => {
    if (event.target === container) {
      container.remove();
    }
  });
}

renderLayout();

registerRoute((hash) => {
  if (!hash || hash === '#/runs') {
    return { params: {} };
  }
  return null;
}, () => renderRunsPage(document.getElementById('page-content')));

registerRoute((hash) => {
  const match = hash.match(/^#\/run\/(\d+)/);
  if (match) {
    return { params: { runId: match[1] } };
  }
  return null;
}, (params) => renderRunDetailPage(document.getElementById('page-content'), params.runId));

registerRoute((hash) => {
  if (hash.startsWith('#/compare')) {
    const query = hash.includes('?') ? hash.split('?')[1] : '';
    const params = new URLSearchParams(query);
    return { params: { runs: params.get('runs') || '', metrics: params.get('metrics') || '', cases: params.get('cases') || '', cols: params.get('cols') || '' } };
  }
  return null;
}, (params) => renderComparePage(document.getElementById('page-content'), params));

initRouter();