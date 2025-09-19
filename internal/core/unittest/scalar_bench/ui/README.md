# Scalar Bench UI - 功能与设计文档（MVP）

本 UI 为 `internal/core/unittest/scalar_bench` 的结果提供纯前端展示与对比界面。目标：零后端、本地或静态服务器直接打开。

## 一、信息架构

- 主页（Runs）
  - 列举所有 run（读取 `index.json`）
  - 显示关键配置/摘要指标（数据集、索引、expr、mmap、git、机器、时间）
  - 筛选/搜索：数据集、索引、expr 关键词、mmap、时间范围、git、机器
  - 点击跳转 Run 详情

- Run 详情页
  - 元信息：`meta.json`
  - 指标表：`metrics.json`（每个 case 指标）
  - 火焰图：`flamegraphs/*.svg`（缩略图/原图下载）
  - 资产下载：config、folded、perf.data（若存在）

- 对比页
  - 多 run、多 case 选择
  - 指标横向对比（可选指标集）
  - 火焰图并排展示（列数可选 2/3/4）

## 二、数据产物与目录结构

前端仅读取静态文件；不写入。

```
scalar_bench/
  runs/
    <run_id>/
      meta.json           // 运行元信息
      metrics.json        // case -> 指标
      config.yaml         // 原始配置
      flamegraphs/
        <case>.svg
      stacks/
        <case>.folded
      raw/
        <case>.perf.data  // 可选
  index.json               // 所有 run 摘要清单（供主页）
```

示例 `index.json`：
```json
{
  "runs": [
    {
      "id": "2025-09-19_12-30-00_hash123",
      "timestamp": "2025-09-19T12:30:00Z",
      "label": "mmap_on ivf_flat 10M",
      "git_commit": "hash123",
      "machine": { "host": "bench-01", "cpu": "AMD EPYC", "kernel": "5.15", "perf_version": "6.8" },
      "config": { "dataset": "sift-10M", "index": "ivf_flat", "expr": "x>10 && y<5", "mmap": true },
      "summary": { "qps": 120000, "p95_ms": 4.2, "rss_gb": 12.3 }
    }
  ]
}
```

`metrics.json` 示例：
```json
{
  "cases": {
    "caseA": { "qps": 123456, "p50_ms": 2.5, "p95_ms": 4.2, "p99_ms": 7.1, "rss_gb": 12.0, "cpu_pct": 320, "io_read_mb_s": 150 },
    "caseB": { "qps": 110000, "p50_ms": 2.8, "p95_ms": 4.8, "p99_ms": 7.9, "rss_gb": 13.5, "cpu_pct": 280, "io_read_mb_s": 90 }
  }
}
```

## 三、页面与交互（MVP）

- 主页：run 列表（按时间排序）、多选进入对比页
- 详情：顶部元信息、case 指标表（列可选：QPS、p50/p95/p99、RSS、CPU%、IO）、火焰图
- 对比：左侧多选器（run/case）、指标表横向对比、火焰图网格（列数可选）

## 四、实现与技术栈

- 纯静态前端：原生 TS + 简单 hash 路由 或 轻量 React + Vite
- MVP 推荐原生 TS + Web Components（零依赖）；如需提效可用 React
- 安全：仅本地静态读取；SVG 以 <img> 引入（避免内联执行）

## 五、URL 结构（可分享）

- `#/runs` 主页
- `#/run/<run_id>` 详情
- `#/compare?runs=a,b&cases=x,y&cols=3&metrics=qps,p95_ms`

## 六、后续增强（非 MVP）

- baseline 与 Δ 高亮；趋势图；同步 hover/缩放；深色模式；上传/权限；更强筛选与分页

## 七、落地计划（MVP）

1) 定义 `index.json/meta.json/metrics.json` 产物格式
2) 主页列表与过滤器
3) Run 详情表格 + 火焰图缩略图
4) 对比页（指标对比 + 火焰图并排）

> 完成后可将 `scalar_bench/ui` 作为静态根目录，将 `runs` 与 `index.json` 放同级或子级，使用相对路径即可。
