import React, { useLayoutEffect, useMemo, useRef, useState } from 'react';
import * as am5 from '@amcharts/amcharts5';
import * as am5xy from '@amcharts/amcharts5/xy';
import am5themes_Animated from '@amcharts/amcharts5/themes/Animated';
import type { CaseRow } from './CasesTable';

type MetricKey = 'qps' | 'latency_ms.avg' | 'latency_ms.p50' | 'latency_ms.p90' | 'latency_ms.p99' | 'index_build_ms';

const METRICS: { key: MetricKey; label: string }[] = [
  { key: 'qps', label: 'QPS' },
  { key: 'latency_ms.avg', label: 'Avg ms' },
  { key: 'latency_ms.p50', label: 'P50 ms' },
  { key: 'latency_ms.p90', label: 'P90 ms' },
  { key: 'latency_ms.p99', label: 'P99 ms' },
  { key: 'index_build_ms', label: 'Index build ms' },
];

function get(obj: any, path: string): any {
  return path ? path.split('.').reduce((acc, k) => (acc ? acc[k] : undefined), obj) : undefined;
}

export function DatasetCharts({ rows }: { rows: CaseRow[] }): JSX.Element {
  const [metricKey, setMetricKey] = useState<MetricKey>('latency_ms.avg');
  const [cols, setCols] = useState<number>(4);

  const datasets = useMemo(() => {
    const map = new Map<string, Map<string, { x: number; y: number; expr: string; caseId: string }[]>>();
    rows.forEach((r) => {
      const dataset = r.dataConfig || '';
      const index = r.indexConfig || '';
      const x = Number(get(r.metrics, 'selectivity'));
      const y = Number(get(r.metrics, metricKey));
      if (!Number.isFinite(x) || !Number.isFinite(y)) return;
      if (!map.has(dataset)) map.set(dataset, new Map());
      const im = map.get(dataset)!;
      if (!im.has(index)) im.set(index, []);
      im.get(index)!.push({ x, y, expr: String(r.expression || ''), caseId: r.caseId });
    });
    // sort points by x
    for (const [, im] of map) {
      for (const [, arr] of im) {
        arr.sort((a, b) => a.x - b.x);
      }
    }
    return map;
  }, [rows, metricKey]);

  const datasetNames = useMemo(() => Array.from(datasets.keys()).sort(), [datasets]);

  const colNum = Math.max(1, Math.min(5, Number(cols) || 1));
  const rowsByLine = useMemo(() => {
    const chunks: string[][] = [];
    for (let i = 0; i < datasetNames.length; i += colNum) {
      chunks.push(datasetNames.slice(i, i + colNum));
    }
    return chunks;
  }, [datasetNames, colNum]);

  return (
    <div>
      <div className="page-actions" style={{ marginBottom: '0.5rem' }}>
        <div className="left"><h3 style={{ margin: 0 }}>Charts</h3></div>
        <div className="right" style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          <div>
            <span className="small text-muted" style={{ marginRight: '0.5rem' }}>Metric</span>
            <div className="segmented" style={{ display: 'inline-flex', flexWrap: 'wrap' }}>
              {METRICS.map((m) => (
                <button key={m.key} className={`segmented-btn${m.key === metricKey ? ' active' : ''}`} onClick={() => setMetricKey(m.key)}>{m.label}</button>
              ))}
            </div>
          </div>
          <div>
            <span className="small text-muted" style={{ marginRight: '0.5rem' }}>Charts per row</span>
            <div className="segmented" style={{ display: 'inline-flex' }}>
              {Array.from({ length: 5 }, (_, i) => i + 1).map((n) => (
                <button key={n} className={`segmented-btn${n === colNum ? ' active' : ''}`} onClick={() => setCols(n)}>{n}</button>
              ))}
            </div>
          </div>
        </div>
      </div>
      {rowsByLine.map((chunk, r) => (
        <div key={r} style={{ display: 'grid', gap: '0.75rem', gridTemplateColumns: `repeat(${chunk.length}, minmax(0, 1fr))` }}>
          {chunk.map((name) => (
            <div key={name}>
              <h4 style={{ margin: '0 0 0.25rem 0' }}>{name}</h4>
              <PerDatasetChart key={name + ':' + metricKey} dataset={name} seriesMap={datasets.get(name)!} metricLabel={METRICS.find((m) => m.key === metricKey)?.label || metricKey} />
            </div>
          ))}
        </div>
      ))}
      {datasetNames.length === 0 && <div className="text-muted">No chartable data.</div>}
    </div>
  );
}

function PerDatasetChart({ dataset, seriesMap, metricLabel }: { dataset: string; seriesMap: Map<string, { x: number; y: number; expr: string; caseId: string }[]>; metricLabel: string }): JSX.Element {
  const divRef = useRef<HTMLDivElement | null>(null);

  useLayoutEffect(() => {
    if (!divRef.current) return;
    const root = am5.Root.new(divRef.current);
    root.setThemes([am5themes_Animated.new(root)]);

    const textColor = am5.color(0xE5E7EB);
    const gridColor = am5.color(0x374151);
    root.interfaceColors.set('text', textColor);

    const chart = root.container.children.push(
      am5xy.XYChart.new(root, {
        layout: root.verticalLayout,
        wheelY: 'zoomX',
        pinchZoomX: true,
      })
    );
    const seriesNames = Array.from(seriesMap.keys());
    const maxNameLen = seriesNames.reduce((m, s) => Math.max(m, s.length), 0);
    chart.setAll({ paddingRight: 0, paddingLeft: 0 });
    (chart.plotContainer as any).set('mask', undefined);

    let minX = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    for (const [, arr] of seriesMap) {
      for (const p of arr) {
        if (Number.isFinite(p.x)) {
          if (p.x < minX) minX = p.x;
          if (p.x > maxX) maxX = p.x;
        }
      }
    }
    if (!Number.isFinite(minX) || !Number.isFinite(maxX)) { minX = 0; maxX = 1; }
    const range = Math.max(1e-6, maxX - minX);
    const pad = Math.max(range * 0.05, 0.01);
    const padRight = Math.max(range * 0.12, 0.02);

    const xAxis = chart.xAxes.push(
      am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererX.new(root, { minGridDistance: 40 }),
        min: minX - pad,
        max: maxX + padRight,
        strictMinMax: true,
        numberFormat: '0.00',
        tooltip: am5.Tooltip.new(root, {}),
      })
    );
    const xRenderer = xAxis.get('renderer') as am5xy.AxisRendererX;
    xRenderer.labels.template.setAll({ text: '{value.formatNumber(0.00)}', fill: textColor, fontSize: 11 });
    xRenderer.grid.template.setAll({ stroke: gridColor, strokeOpacity: 0.3 });
    xAxis.children.push(am5.Label.new(root, { text: 'Selectivity', fill: textColor, fontSize: 12, x: am5.p50, centerX: am5.p50, paddingTop: 6 }));

    const yAxis = chart.yAxes.push(
      am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererY.new(root, {}),
        tooltip: am5.Tooltip.new(root, {}),
      })
    );
    const yRenderer = yAxis.get('renderer') as am5xy.AxisRendererY;
    yRenderer.labels.template.setAll({ fill: textColor, fontSize: 11 });
    yRenderer.grid.template.setAll({ stroke: gridColor, strokeOpacity: 0.3 });
    yRenderer.setAll?.({ inside: true, paddingLeft: 0, paddingRight: 0 } as any);
    chart.plotContainer.children.unshift(
      am5.Label.new(root, { text: metricLabel, fill: textColor, fontSize: 12, rotation: -90, x: 2, centerX: am5.p50, y: am5.p50, centerY: am5.p50, opacity: 0.85 })
    );


    const COLORS: number[] = [
      0x60A5FA,
      0xF59E0B,
      0x34D399,
      0xF472B6,
      0xA78BFA,
      0xF87171,
      0x22D3EE,
      0xC084FC,
      0xFBBF24,
      0x4ADE80,
    ];

    const entries = Array.from(seriesMap.entries());
    const enriched = entries.map(([name, arr]) => ({
      name,
      data: arr,
      lastX: (arr && arr.length ? arr[arr.length - 1].x : Number.NaN),
      lastY: (arr && arr.length ? arr[arr.length - 1].y : Number.NaN),
    }));
    const orderByY = [...enriched].sort((a, b) => (Number(b.lastY) - Number(a.lastY)));
    const globalOrder = new Map<string, number>(orderByY.map((e, i) => [e.name, i]));

    let seriesIdx = 0;
    for (const [indexName, data] of entries) {
      if (!data || data.length === 0) continue;
      const labelIndex = globalOrder.get(indexName) ?? seriesIdx;
      const total = entries.length;
      const series = chart.series.push(
        am5xy.LineSeries.new(root, {
          name: indexName,
          xAxis,
          yAxis,
          valueXField: 'x',
          valueYField: 'y',
          tooltip: am5.Tooltip.new(root, { labelText: `{name}\nselectivity: {valueX.formatNumber(0.00)}\n${metricLabel}: {valueY.formatNumber(0.##)}`, pointerOrientation: 'horizontal' }),
        })
      );
      const tooltip = series.get('tooltip');
      if (tooltip) {
        tooltip.label.setAll({ fontSize: 11 });
        tooltip.setAll({ dx: 10 });
      }
      series.data.setAll(data);
      const color = am5.color(COLORS[seriesIdx % COLORS.length]);
      series.strokes.template.setAll({ strokeWidth: 2, stroke: color });
      series.setAll({ stroke: color, fill: color });
      // ensure end labels outside plot are not clipped
      series.set('maskBullets', false as any);
      series.bullets.push(() => {
        return am5.Bullet.new(root, {
          sprite: am5.Circle.new(root, { radius: 3, fill: color })
        });
      });
      series.bullets.push((rootArg, seriesArg, dataItem) => {
        if (!dataItem) return undefined as any;
        const idx = seriesArg.dataItems.indexOf(dataItem);
        if (idx !== seriesArg.dataItems.length - 1) return undefined as any;
        const lastX = data[data.length - 1]?.x;
        const cluster = enriched
          .filter((e) => e.data && e.data.length && e.lastX === lastX)
          .sort((a, b) => (Number(b.lastY) - Number(a.lastY)));
        const clusterIndex = Math.max(0, cluster.findIndex((e) => e.name === indexName));
        const clusterOffset = cluster.length > 1 ? (clusterIndex - (cluster.length - 1) / 2) : 0;
        return am5.Bullet.new(rootArg, {
          sprite: am5.Label.new(rootArg, {
            text: indexName,
            fill: color,
            fontSize: 11,
            centerY: am5.p50,
            centerX: am5.p100,
            dx: -4,
            dy: ((labelIndex - (total - 1) / 2) * 8) + (clusterOffset * 8),
          }),
          locationX: 1,
        });
      });
      seriesIdx += 1;
    }

    const cursor = chart.set('cursor', am5xy.XYCursor.new(root, { behavior: 'none' }));
    cursor.lineY.set('visible', false);

    return () => {
      root.dispose();
    };
  }, [dataset, seriesMap, metricLabel]);

  return <div ref={divRef} style={{ width: '100%', height: 320, border: '1px solid var(--border-color)', borderRadius: 6 }} />;
}

export default DatasetCharts;


