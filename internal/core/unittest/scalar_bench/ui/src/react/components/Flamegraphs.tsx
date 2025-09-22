import React from 'react';
import { buildAssetUrl } from '../lib/api';

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
              <object data={src} type="image/svg+xml" className="flamegraph-embed tall-embed"></object>
            </div>
            <div className="text-muted small">Run {card.runId} • Case {card.caseId}</div>
            <div className="links">
              <a href={src} target="_blank">Open</a>
              <button className="ghost" onClick={(e) => {
                const wrapper = (e.currentTarget as HTMLButtonElement).closest('.flamegraph-card');
                const obj = wrapper?.querySelector('object.flamegraph-embed');
                if (obj) { obj.setAttribute('data', ''); setTimeout(() => obj.setAttribute('data', src), 0); }
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

