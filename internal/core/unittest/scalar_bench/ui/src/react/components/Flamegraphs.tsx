import React from 'react';
import { buildAssetUrl } from '../utils/api';

export type FlameCard = {
  runId: string;
  caseId: string;
  values: { flamegraph?: string; data_config?: string; index_config?: string };
};

export function FlamegraphGrid({ cards, cols = 2 }: { cards: FlameCard[]; cols?: number }): JSX.Element | null {
  if (!Array.isArray(cards) || !cards.length) return null;
  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  return (
    <div className={`flamegraph-grid columns-${colNum}`}>
      {cards.map((card, i) => {
        if (!card?.values?.flamegraph) return null;
        const src = buildAssetUrl(`${card.runId}/${card.values.flamegraph}`);
        return (
          <div className="flamegraph-card" key={i}>
            <div className="svg-container limit-400">
              <object
                data={src}
                type="image/svg+xml"
                className="flamegraph-embed tall-embed"
                onLoad={(e) => {
                  const container = (e.currentTarget as HTMLObjectElement).closest('.svg-container') as HTMLElement | null;
                  if (container) {
                    // Scroll to bottom so deepest stacks are visible by default
                    container.scrollTop = container.scrollHeight;
                  }
                }}
              ></object>
            </div>
            <div className="text-muted small">Run {card.runId} • Case {card.caseId}</div>
            <div className="links">
              <button className="ghost" onClick={() => window.open(src, '_blank')}>Open in new page</button>
              <button className="ghost" onClick={(e) => {
                const wrapper = (e.currentTarget as HTMLButtonElement).closest('.flamegraph-card');
                const obj = wrapper?.querySelector('object.flamegraph-embed');
                const container = wrapper?.querySelector('.svg-container') as HTMLElement | null;
                if (obj) {
                  obj.setAttribute('data', '');
                  setTimeout(() => {
                    obj.setAttribute('data', src);
                    if (container) {
                      container.scrollTop = container.scrollHeight;
                    }
                  }, 0);
                }
              }}>Reset view</button>
              {card.values.data_config ? <span className="tag">{card.values.data_config}</span> : null}
              {card.values.index_config ? <span className="tag">{card.values.index_config}</span> : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

export function FlamegraphSection({
  cards,
  cols,
  setCols,
  title = 'Flamegraphs',
}: {
  cards: FlameCard[];
  cols: number;
  setCols: (n: number) => void;
  title?: string;
}): JSX.Element | null {
  if (!Array.isArray(cards) || !cards.length) return null;
  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  return (
    <div style={{ marginTop: '1.5rem' }}>
      <h3>{title}</h3>
      <div className="section-card">
        <h3 style={{ marginTop: 0 }}>Flamegraphs per row</h3>
        <div className="segmented">
          {Array.from({ length: 5 }, (_, i) => i + 1).map((n) => (
            <button key={n} className={`segmented-btn${n === colNum ? ' active' : ''}`} onClick={() => setCols(n)}>{n}</button>
          ))}
        </div>
      </div>
      <FlamegraphGrid cards={cards} cols={colNum} />
    </div>
  );
}

