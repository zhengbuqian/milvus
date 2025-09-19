import { buildAssetUrl } from '../api.js';

// cards: Array<{ runId, caseId, values: { flamegraph, data_config?, index_config? } }>
// cols: 1..5
export function buildFlamegraphGrid(cards, cols = 2) {
  if (!Array.isArray(cards) || !cards.length) return null;
  const grid = document.createElement('div');
  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  grid.className = `flamegraph-grid columns-${colNum}`;

  cards.forEach((cardData) => {
    if (!cardData?.values?.flamegraph) return;
    const card = document.createElement('div');
    card.className = 'flamegraph-card';
    const src = buildAssetUrl(`${cardData.runId}/${cardData.values.flamegraph}`);
    card.innerHTML = `
      <div class="svg-container limit-400"><object data="${src}" type="image/svg+xml" class="flamegraph-embed tall-embed"></object></div>
      <div class="text-muted small">Run ${cardData.runId} • Case ${cardData.caseId}</div>
      <div class="links">
        <a href="${src}" target="_blank">Open</a>
        <button class="ghost" data-action="reset" data-src="${src}">Reset view</button>
        ${cardData.values.data_config ? `<span class="tag">${cardData.values.data_config}</span>` : ''}
        ${cardData.values.index_config ? `<span class="tag">${cardData.values.index_config}</span>` : ''}
      </div>
    `;
    const obj = card.querySelector('object.flamegraph-embed');
    obj.addEventListener('load', () => {
      const cont = card.querySelector('.svg-container');
      if (cont) cont.scrollTop = cont.scrollHeight;
    });
    grid.appendChild(card);
  });

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

export function updateFlamegraphGridColumns(gridEl, cols = 2) {
  if (!gridEl) return;
  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  gridEl.className = `flamegraph-grid columns-${colNum}`;
}


