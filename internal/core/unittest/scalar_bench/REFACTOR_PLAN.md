# Scalar Benchmark 结果组织方式重构计划

## 文档信息
- **创建日期**: 2025-01-12
- **状态**: 计划中
- **目标版本**: 2.0

## 概述

本文档描述了 scalar benchmark 结果组织方式的重构计划，从当前的扁平"Run"结构改为层次化的"Bundle-Case-Suite"结构。目标是提供更好的测试结果组织方式，直接映射到 YAML 配置文件，并改进 UI 导航和对比功能。

---

## 1. 当前架构

### 1.1 当前结构

```
Run (基于时间戳)
└── Cases (扁平列表)
    ├── Case 1 (data_config, index_config, expression)
    ├── Case 2
    └── Case 3
```

### 1.2 当前文件组织

```
results/
├── index.json
└── <run_id>/
    ├── meta.json
    ├── metrics.json
    ├── run_summary.txt
    ├── benchmark_results.csv
    └── flamegraphs/
        └── <case_id>.svg
```

### 1.3 当前 UI 页面

1. **RunsPage** (`/runs`)：
   - 列出所有基准测试运行
   - 可展开的内联 case 表格
   - 允许选择 run 进行对比

2. **RunDetailPage** (`/run/:runId`)：
   - 显示单个 run 的元数据
   - 所有 cases 的 DatasetCharts
   - 统一的 case 指标表格
   - 所有火焰图的汇总展示

3. **ComparePage** (`/compare`)：
   - 并排对比选中的 runs 或 cases

### 1.4 当前架构的问题

1. **无 YAML-结果映射**：无法轻松看到哪个 YAML 配置产生了哪些结果
2. **扁平的 case 结构**：没有按 YAML 中定义的逻辑测试用例分组
3. **Suite 不可见**：一个 case 中的多个 suite 无法区分
4. **导航问题**：无法从配置结构导航到结果
5. **对比困难**：难以跨 bundle 对比特定的 cases

---

## 2. 目标架构

### 2.1 新的层次化结构

```
Bundle (YAML 配置文件)
├── Bundle 元数据
│   ├── config_file: "bool/equal.yaml"
│   ├── bundle_id: <时间戳>
│   ├── timestamp_ms: 1704067200000
│   └── cases: [case1, case2, ...]
│
└── Cases (YAML 中定义)
    ├── Case 1: "bool_equal"
    │   ├── Case 元数据
    │   │   ├── case_name: "bool_equal"
    │   │   ├── case_id: <唯一ID>
    │   │   └── suites: [suite1, suite2]
    │   │
    │   └── Suites (执行结果，在 UI 中合并)
    │       ├── Suite 1: "default"
    │       │   ├── 数据配置: [balanced, skewed_9010]
    │       │   ├── 索引配置: [no_index, bitmap_index]
    │       │   └── 表达式: [good_equal_true, good_not_equal_true]
    │       │
    │       └── Suite 2: "high_selectivity"
    │           ├── 数据配置: [most_true]
    │           ├── 索引配置: [inverted_index]
    │           └── 表达式: [good_equal_false]
    │
    └── Case 2: "bool_not_equal"
        └── ...
```

### 2.2 核心概念

#### Bundle（测试包）
- **定义**：单个 YAML 配置文件的完整测试执行
- **标识**：唯一的 bundle_id（时间戳）+ config_file 路径
- **包含**：YAML 文件中定义的多个 cases
- **用途**：结果组织和对比的顶层单元

#### Case（测试用例）
- **定义**：YAML `cases:[]` 数组中定义的逻辑测试用例
- **标识**：case_name（来自 YAML）+ case_id（bundle 内唯一）
- **包含**：多个 suites 及其测试结果
- **用途**：相关测试的中层分组，使用不同的数据/索引组合

#### Suite（测试套件）
- **定义**：YAML case 内 `suites:[]` 数组中定义的测试套件
- **标识**：suite_name（来自 YAML）
- **包含**：(data_configs × index_configs × expressions) 的笛卡尔积
- **用途**：执行单元，但在 UI 中结果会合并到 case 级别视图

### 2.3 新的文件组织

```
results/
├── index.json                          # 列出所有 bundles
│   {
│     "bundles": [
│       {
│         "bundle_id": "1704067200000",
│         "config_file": "bool/equal.yaml",
│         "timestamp_ms": 1704067200000,
│         "label": "布尔相等性测试",
│         "cases": ["bool_equal", "bool_not_equal"]
│       }
│     ]
│   }
│
└── <bundle_id>/
    ├── bundle_meta.json                # Bundle 级别元数据
    │   {
    │     "bundle_id": "1704067200000",
    │     "config_file": "bool/equal.yaml",
    │     "config_content": "<yaml 内容>",
    │     "timestamp_ms": 1704067200000,
    │     "test_params": {...},
    │     "cases": [
    │       {
    │         "case_name": "bool_equal",
    │         "case_id": "1704067200000_0",
    │         "suites": ["default", "high_selectivity"]
    │       }
    │     ]
    │   }
    │
    ├── bundle_summary.txt              # 整体摘要
    │
    ├── cases/
    │   ├── <case_id>/
    │   │   ├── case_meta.json          # Case 级别元数据
    │   │   │   {
    │   │   │     "case_id": "1704067200000_0",
    │   │   │     "case_name": "bool_equal",
    │   │   │     "bundle_id": "1704067200000",
    │   │   │     "suites": [
    │   │   │       {
    │   │   │         "suite_name": "default",
    │   │   │         "data_configs": ["balanced", "skewed_9010"],
    │   │   │         "index_configs": ["no_index", "bitmap_index"],
    │   │   │         "expr_templates": ["good_equal_true"]
    │   │   │       }
    │   │   │     ],
    │   │   │     "total_tests": 8,
    │   │   │     "has_flamegraphs": true
    │   │   │   }
    │   │   │
    │   │   ├── case_metrics.json       # 所有 suites 的合并指标
    │   │   │   {
    │   │   │     "tests": [
    │   │   │       {
    │   │   │         "test_id": "0001",
    │   │   │         "suite_name": "default",
    │   │   │         "data_config": "balanced",
    │   │   │         "index_config": "no_index",
    │   │   │         "expression": "good == true",
    │   │   │         "qps": 12345,
    │   │   │         "latency_ms": {...},
    │   │   │         "selectivity": 0.5,
    │   │   │         "flamegraph": "flamegraphs/0001.svg"
    │   │   │       }
    │   │   │     ]
    │   │   │   }
    │   │   │
    │   │   ├── case_summary.txt        # Case 特定的摘要
    │   │   │
    │   │   ├── case_results.csv        # 此 case 的 CSV 导出
    │   │   │
    │   │   └── flamegraphs/
    │   │       ├── 0001.svg
    │   │       └── 0002.svg
    │   │
    │   └── <case_id_2>/
    │       └── ...
    │
    └── all_results.csv                 # 整个 bundle 的合并 CSV
```

### 2.4 新的 UI 结构

#### 2.4.1 主页面：Bundles 列表 (`/bundles`)

**用途**：入口页面，显示所有测试 bundles

**布局**：
```
┌─────────────────────────────────────────────────────────────┐
│ Scalar 基准测试结果                            [刷新]       │
│                                                               │
│ ┌───────────────────────────────────────────────────────┐   │
│ │ [▼] bool/equal.yaml - 2025-01-01 10:00:00            │   │
│ │     Bundle ID: 1704067200000                          │   │
│ │     2 个用例 | 总计 15 个测试                          │   │
│ │                                        [查看 Bundle] │   │
│ │                                                        │   │
│ │     ┌──────────────────────────────────────────────┐  │   │
│ │     │ 用例: bool_equal                   [详情] │  │   │
│ │     │ 2 个套件 | 8 个测试                         │  │   │
│ │     │ QPS 范围: 10K - 50K                         │  │   │
│ │     └──────────────────────────────────────────────┘  │   │
│ │                                                        │   │
│ │     ┌──────────────────────────────────────────────┐  │   │
│ │     │ 用例: bool_not_equal               [详情] │  │   │
│ │     │ 1 个套件 | 7 个测试                          │  │   │
│ │     │ QPS 范围: 15K - 45K                         │  │   │
│ │     └──────────────────────────────────────────────┘  │   │
│ └───────────────────────────────────────────────────────┘   │
│                                                               │
│ ┌───────────────────────────────────────────────────────┐   │
│ │ [►] array/array.yaml - 2025-01-01 09:00:00            │   │
│ │     Bundle ID: 1704063600000                          │   │
│ │     4 个用例 | 总计 64 个测试                          │   │
│ │                                        [查看 Bundle] │   │
│ └───────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**功能特性**：
- 可展开的 bundle 卡片
- 显示配置文件路径和执行时间
- 嵌套的 case 列表，带快速指标
- 直接链接到 Bundle Detail 和 Case Detail 页面
- 按配置文件、日期范围、case 名称过滤

#### 2.4.2 Bundle 详情页 (`/bundle/:bundleId`)

**用途**：Bundle 内所有 cases 的概览

**布局**：
```
┌─────────────────────────────────────────────────────────────┐
│ ← 返回 Bundles                                              │
│                                                               │
│ Bundle: bool/equal.yaml                                       │
│ 执行时间: 2025-01-01 10:00:00                                │
│ Bundle ID: 1704067200000                                      │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Bundle 配置                                              │ │
│ │ ├─ 测试迭代: 100                                         │ │
│ │ ├─ 预热迭代: 30                                          │ │
│ │ ├─ 总用例数: 2                                           │ │
│ │ └─ 总测试数: 15                                          │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ [▼] 用例: bool_equal                        [打开用例] │ │
│ │                                                          │ │
│ │     Case ID: 1704067200000_0                            │ │
│ │     套件: default, high_selectivity                     │ │
│ │     总测试数: 8                                          │ │
│ │                                                          │ │
│ │     📊 图表                                              │ │
│ │     [按索引分组的 QPS] [延迟分布] [选择率]              │ │
│ │                                                          │ │
│ │     📋 测试结果 (默认折叠)                               │ │
│ │     [显示/隐藏表格]                                      │ │
│ │     ┌───────────────────────────────────────────────┐   │ │
│ │     │ 套件    │ 数据     │ 索引   │ QPS   │ P99  │   │ │
│ │     ├─────────┼──────────┼────────┼───────┼──────┤   │ │
│ │     │ default │ balanced │ no_idx │ 25.3K │ 0.8  │   │ │
│ │     │ default │ balanced │ bitmap │ 45.1K │ 0.4  │   │ │
│ │     └───────────────────────────────────────────────┘   │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ [►] 用例: bool_not_equal                    [打开用例] │ │
│ │     (已折叠)                                             │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**功能特性**：
- Bundle 配置概览
- 每个 case 显示为可展开的区块
- 每个 case 的图表和表格（默认折叠）
- 不显示火焰图（在 Case Detail 中查看）
- 快速导航到各个 Case Detail 页面
- 导出 bundle 级别的 CSV

#### 2.4.3 Case 详情页 (`/bundle/:bundleId/case/:caseId`)

**用途**：单个 case 的详细视图，包含所有测试结果

**布局**：
```
┌─────────────────────────────────────────────────────────────┐
│ ← 返回 Bundle                                               │
│                                                               │
│ 用例: bool_equal                                              │
│ Bundle: bool/equal.yaml                                       │
│ Case ID: 1704067200000_0                                      │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 用例配置                                                 │ │
│ │ ├─ 套件: default, high_selectivity                       │ │
│ │ ├─ 总测试数: 8                                           │ │
│ │ ├─ 数据配置: balanced, skewed_9010, most_true            │ │
│ │ ├─ 索引配置: no_index, bitmap_index, inverted_index     │ │
│ │ └─ 表达式: good == true, good != true                    │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ 📊 性能图表                                                   │
│ [按数据配置的 QPS] [按索引的延迟] [选择率]                   │
│                                                               │
│ 📋 测试结果                                                   │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 套件    │ 数据     │ 索引      │ 表达式      │ QPS   │...│ │
│ ├─────────┼──────────┼──────────┼─────────────┼───────┼───┤ │
│ │ default │ balanced │ no_index │ good==true  │ 25.3K │...│ │
│ │ default │ balanced │ bitmap   │ good==true  │ 45.1K │...│ │
│ │ default │ skewed   │ no_index │ good==true  │ 28.7K │...│ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ 🔥 火焰图 (如果可用)                                          │
│ ┌─────────────────────┐ ┌─────────────────────┐             │
│ │ 测试 0001           │ │ 测试 0002           │             │
│ │ [火焰图 SVG]        │ │ [火焰图 SVG]        │             │
│ └─────────────────────┘ └─────────────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

**功能特性**：
- 完整的 case 详情，包括所有 suites
- 该 case 的全面图表
- 包含所有指标的完整测试结果表格
- 火焰图区块（如果可用）
- 导出 case 级别的 CSV
- 表格中的 Suite 列（suites 不单独展开）

### 2.5 对比功能设计

#### 2.5.1 设计理念

**核心原则**：
- ✅ **有意义的对比**：只允许对比"相同"的测试（同一个 config_file + case_name）
- ✅ **历史追踪**：追踪性能回归和优化效果
- ✅ **自动化而非手动**：系统自动识别可对比的版本，避免用户错误对比
- ❌ **禁止跨 case 对比**：`bool_equal` vs `array_contains` 没有意义

**不支持的对比**：
- 跨不同 `config_file` 的对比（不同 YAML 文件）
- 跨不同 `case_name` 的对比（不同测试用例）
- 任意手动选择 bundles/cases 进行对比

#### 2.5.2 两层对比模式

##### 模式 1：Bundle 级别历史对比（粗粒度）

**用例**：
- "我修改了代码，想看看对整个 `bool/equal.yaml` 测试集的影响"
- "对比本周和上周的完整测试结果"
- "这次优化对所有 cases 都有效果吗？"

**功能特性**：
- 对比同一个 `config_file` 的不同执行（不同 bundle_id）
- 显示所有 cases 的性能变化概览
- 用颜色热力图展示变化趋势

**UI 示例**：
```
┌─────────────────────────────────────────────────────────────┐
│ Bundle 历史对比: bool/equal.yaml                    [关闭] │
│                                                               │
│ 选择对比版本（最近 10 次执行）：                             │
│ ☑ 2025-01-12 10:00 (最新) ← 基准                            │
│ □ 2025-01-10 15:30                                           │
│ □ 2025-01-08 09:00                                           │
│ ☑ 2025-01-05 14:20 ← 对比                                    │
│                                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 性能对比概览                                             │ │
│ │                                                          │ │
│ │ Case: bool_equal                        [查看详细对比] │ │
│ │   QPS (avg):     45.2K  vs  42.1K   ↑ +7.4%  🟢        │ │
│ │   P99 Latency:   0.82ms vs  0.95ms  ↓ -13.7% 🟢        │ │
│ │                                                          │ │
│ │ Case: bool_not_equal                    [查看详细对比] │ │
│ │   QPS (avg):     38.5K  vs  39.2K   ↓ -1.8%  🟡        │ │
│ │   P99 Latency:   1.02ms vs  0.88ms  ↑ +15.9% 🔴        │ │
│ │                                                          │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ [导出对比报告]                                               │
└─────────────────────────────────────────────────────────────┘
```

**入口点**：
- 从 Bundle Detail 页面：`[查看历史趋势]` 按钮
- 从 Bundles 列表页：每个 bundle 卡片的 `[对比]` 图标

##### 模式 2：Case 级别历史对比（细粒度）

**用例**：
- "这个 case 的性能为什么下降了？"
- "哪些数据配置/索引组合变快了，哪些变慢了？"
- "对比火焰图，找出性能瓶颈的变化"

**功能特性**：
- 对比同一个 `config_file` + `case_name` 的不同执行
- 详细的每个测试（data × index × expression）的性能变化
- 用颜色标注变化幅度
- 支持火焰图并排对比

**UI 示例**：
```
┌─────────────────────────────────────────────────────────────┐
│ Case 历史对比: bool/equal.yaml → bool_equal        [关闭] │
│                                                               │
│ 对比版本：                                                   │
│ 基准: 2025-01-12 10:00 (最新)                               │
│ 对比: 2025-01-05 14:20                                       │
│                                                               │
│ 📊 性能趋势图 (最近 10 次)                                   │
│ QPS                                                           │
│ 50K ┤                                    ●                   │
│ 45K ┤                          ●    ●                        │
│ 40K ┤              ●      ●                                  │
│ 35K ┤     ●   ●                                              │
│ 30K ┤ ●                                                      │
│     └──────────────────────────────────────→                │
│       1/1  1/3  1/5  1/7  1/9  1/11 1/12                     │
│                                                               │
│ 📋 详细对比                                                   │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │Suite │Data    │Index  │Expr       │QPS   │变化  │P99  │变化│ │
│ ├──────┼────────┼───────┼───────────┼──────┼──────┼─────┼───┤ │
│ │def.. │bal..   │no_idx │good==true │25.3K │↑+12%🟢│0.8ms│↓-8%🟢│ │
│ │def.. │bal..   │bitmap │good==true │45.1K │↑+5% 🟢│0.4ms│→0% ⚪│ │
│ │def.. │skewed  │no_idx │good==true │28.7K │↓-3% 🔴│0.9ms│↑+15%🔴│ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                               │
│ 🔥 火焰图对比                                                 │
│ ┌──────────────────────┐  ┌──────────────────────┐          │
│ │ 2025-01-12 (基准)   │  │ 2025-01-05 (对比)   │          │
│ │ balanced × bitmap    │  │ balanced × bitmap    │          │
│ │ [火焰图 SVG]         │  │ [火焰图 SVG]         │          │
│ │                      │  │                      │          │
│ │ filter_expr: 45%    │  │ filter_expr: 52%    │          │
│ │ index_scan: 30%     │  │ index_scan: 28%     │          │
│ └──────────────────────┘  └──────────────────────┘          │
│ 🔴 filter_expr 耗时增加了 7%                                 │
│ 🟢 index_scan 耗时减少了 2%                                  │
│                                                               │
│ [导出对比报告] [下载火焰图对比]                              │
└─────────────────────────────────────────────────────────────┘
```

**入口点**：
- 从 Case Detail 页面：`[对比历史版本]` 按钮
- 从 Bundle 对比页面：点击某个 case 的 `[查看详细对比]`

#### 2.5.3 颜色编码方案

**性能指标（越高越好：QPS、吞吐量）**：
- 🟢 绿色：提升 > 5%（好）
- 🟡 黄色：变化 1-5%（轻微）
- ⚪ 灰色：变化 < 1%（基本持平）
- 🟠 橙色：下降 1-5%（轻微下降）
- 🔴 红色：下降 > 5%（显著下降）

**性能指标（越低越好：延迟、内存）**：
- 🟢 绿色：下降 > 5%（变快了，好）
- 🟡 黄色：变化 1-5%（轻微）
- ⚪ 灰色：变化 < 1%（基本持平）
- 🟠 橙色：上升 1-5%（轻微变慢）
- 🔴 红色：上升 > 5%（显著变慢）

**颜色显示方式**：
```typescript
// 计算变化百分比和颜色
function getChangeInfo(current: number, previous: number, lowerIsBetter: boolean) {
  const change = ((current - previous) / previous) * 100;
  const absChange = Math.abs(change);

  let color: string;
  if (absChange < 1) {
    color = 'gray';  // ⚪
  } else if (absChange < 5) {
    color = (lowerIsBetter ? change < 0 : change > 0) ? 'yellow' : 'orange';  // 🟡/🟠
  } else {
    color = (lowerIsBetter ? change < 0 : change > 0) ? 'green' : 'red';  // 🟢/🔴
  }

  return { change, color };
}
```

#### 2.5.4 智能版本选择

**自动过滤可对比版本**：
```typescript
// 获取可对比的历史版本
function getComparableVersions(currentBundle: BundleInfo): BundleInfo[] {
  return allBundles
    .filter(b => b.config_file === currentBundle.config_file)  // 同一个 YAML 文件
    .filter(b => b.bundle_id !== currentBundle.bundle_id)      // 排除当前版本
    .sort((a, b) => b.timestamp_ms - a.timestamp_ms)           // 按时间倒序
    .slice(0, 10);                                              // 最多显示最近 10 次
}

// 判断两个 case 是否可对比
function isSameCase(case1: CaseInfo, case2: CaseInfo): boolean {
  return (
    case1.config_file === case2.config_file &&
    case1.case_name === case2.case_name
  );
}
```

**推荐对比版本**：
- 默认选中最新版本作为基准
- 推荐对比"上一次执行"（最常见的用例）
- 标记"最后一次正常"版本（如果检测到回归）

#### 2.5.5 性能回归检测

**自动检测**：
在 Case Detail 页面自动显示回归警告：

```
┌─────────────────────────────────────────────────────────────┐
│ ⚠️ 性能回归检测                                              │
│                                                               │
│ 检测到 2 个显著的性能下降：                                   │
│                                                               │
│ 1. balanced × bitmap_index × good==true                      │
│    QPS: 45.1K → 38.2K (↓ -15.3%) 🔴                          │
│    最后正常: 2025-01-10 15:30                                │
│    开始异常: 2025-01-11 09:00                                │
│    [查看对比] [查看火焰图]                                    │
│                                                               │
│ 2. skewed × no_index × good!=true                            │
│    P99 Latency: 0.9ms → 1.2ms (↑ +33.3%) 🔴                 │
│    最后正常: 2025-01-09 14:00                                │
│    开始异常: 2025-01-10 08:30                                │
│    [查看对比] [查看火焰图]                                    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

**检测算法**：
```typescript
// 简单的回归检测
function detectRegressions(currentCase: CaseMetrics, history: CaseMetrics[]): Regression[] {
  const regressions: Regression[] = [];

  // 遍历当前 case 的所有测试
  for (const currentTest of currentCase.tests) {
    // 找到历史中相同的测试
    const historicalTests = history
      .flatMap(h => h.tests)
      .filter(t =>
        t.data_config === currentTest.data_config &&
        t.index_config === currentTest.index_config &&
        t.expression === currentTest.expression
      )
      .sort((a, b) => b.timestamp_ms - a.timestamp_ms);

    if (historicalTests.length === 0) continue;

    // 与最近的历史对比
    const recentTest = historicalTests[0];
    const qpsChange = ((currentTest.qps - recentTest.qps) / recentTest.qps) * 100;

    // QPS 下降超过 10% 视为回归
    if (qpsChange < -10) {
      regressions.push({
        test: currentTest,
        metric: 'QPS',
        change: qpsChange,
        lastNormal: findLastNormalVersion(currentTest, historicalTests),
        firstAbnormal: currentTest.bundle_id,
      });
    }
  }

  return regressions;
}
```

#### 2.5.6 路由设计

**URL 结构**：
```
# Bundle 历史对比
/bundle/compare?config=bool/equal.yaml&bundles=1704067200000,1704063600000

# Case 历史对比
/case/compare?config=bool/equal.yaml&case=bool_equal&bundles=1704067200000,1704063600000

# 带火焰图高亮的对比
/case/compare?config=bool/equal.yaml&case=bool_equal&bundles=1704067200000,1704063600000&test=0001
```

**路由配置**：
```tsx
<Routes>
  <Route path="/" element={<Navigate to="/bundles" />} />
  <Route path="/bundles" element={<BundlesPage />} />
  <Route path="/bundle/:bundleId" element={<BundleDetailPage />} />
  <Route path="/bundle/:bundleId/case/:caseId" element={<CaseDetailPage />} />

  {/* 对比页面 */}
  <Route path="/bundle/compare" element={<BundleComparePage />} />
  <Route path="/case/compare" element={<CaseComparePage />} />
</Routes>
```

#### 2.5.7 趋势图可视化

在 Case 对比页面显示历史趋势：

```typescript
// 趋势图组件
function TrendChart({ metric, history }: TrendChartProps) {
  return (
    <div className="trend-chart">
      <h4>{metric} 历史趋势</h4>
      <LineChart data={history} xKey="timestamp" yKey={metric}>
        {/* 标记当前版本 */}
        <ReferenceLine x={currentTimestamp} stroke="blue" label="当前" />
        {/* 标记对比版本 */}
        <ReferenceLine x={compareTimestamp} stroke="green" label="对比" />
        {/* 标记回归区域 */}
        {regressions.map(r => (
          <ReferenceArea
            x1={r.startTimestamp}
            x2={r.endTimestamp}
            fill="red"
            fillOpacity={0.1}
          />
        ))}
      </LineChart>
    </div>
  );
}
```

#### 2.5.8 导出和分享

**导出功能**：
- CSV 格式：对比数据表格
- JSON 格式：完整对比数据
- PNG 格式：趋势图和对比图表
- HTML 报告：完整的对比报告，包含图表和表格

**分享链接**：
```
# 生成可分享的对比链接
https://benchmark-ui.example.com/case/compare?config=bool/equal.yaml&case=bool_equal&bundles=1704067200000,1704063600000

# 用户点击链接后直接看到对比结果
```

#### 2.5.9 实现优先级

**Phase 1（核心功能）**：
1. Case 级别历史对比（最重要）
2. 基本的颜色编码
3. 版本选择 UI
4. 详细对比表格

**Phase 2（增强功能）**：
1. Bundle 级别对比
2. 趋势图可视化
3. 性能回归检测
4. 导出功能

**Phase 3（高级功能）**：
1. 火焰图并排对比
2. 智能推荐对比版本
3. 自定义回归阈值
4. 对比报告模板

---

## 3. 实施计划

### 3.1 阶段 1：后端 - 文件结构 (第 1-2 周)

#### 3.1.1 更新基准测试配置和执行

**要修改的文件**：
- `scalar_filter_benchmark.h`
- `scalar_filter_benchmark.cpp`
- `main.cpp`

**变更内容**：

1. **在 BenchmarkResult 中添加 Bundle 概念**：
```cpp
struct BenchmarkResult {
    // Bundle 标识
    int64_t bundle_id;           // Bundle 时间戳
    std::string config_file;     // YAML 配置文件路径

    // Case 标识
    std::string case_name;       // 来自 YAML cases[].name
    int64_t case_id;             // Bundle 内的唯一 case ID

    // Suite 标识（保留现有）
    std::string suite_name;

    // 其余现有字段...
};
```

2. **更新输出目录结构**：
```cpp
// 在 ScalarFilterBenchmark::RunBenchmark() 中
std::string bundle_dir = CreateBundleDirectory(bundle_id, config_file);
for (每个 case) {
    std::string case_dir = CreateCaseDirectory(bundle_dir, case_id, case_name);
    // 将 case 结果写入 case_dir
}
```

3. **生成新的 JSON 文件**：
   - `bundle_meta.json`: Bundle 级别元数据
   - `cases/<case_id>/case_meta.json`: Case 级别元数据
   - `cases/<case_id>/case_metrics.json`: 合并的 suite 指标

#### 3.1.2 创建文件写入器

**新文件**：
- `result_writers.h`
- `result_writers.cpp`

**函数**：
```cpp
void WriteBundleMeta(const std::string& bundle_dir,
                     const BundleMetadata& meta);

void WriteCaseMeta(const std::string& case_dir,
                   const CaseMetadata& meta);

void WriteCaseMetrics(const std::string& case_dir,
                      const std::vector<BenchmarkResult>& results);

void WriteIndexJson(const std::string& results_base_dir,
                    const std::vector<BundleInfo>& bundles);
```

#### 3.1.3 更新 index.json 结构

**新格式**：
```json
{
  "bundles": [
    {
      "bundle_id": "1704067200000",
      "config_file": "bool/equal.yaml",
      "timestamp_ms": 1704067200000,
      "label": "布尔相等性测试",
      "cases": ["bool_equal", "bool_not_equal"],
      "total_tests": 15
    }
  ]
}
```

### 3.2 阶段 2：API 层 (第 2-3 周)

#### 3.2.1 更新 API 函数

**文件**: `ui/src/react/utils/api.ts`

**新函数**：
```typescript
// Bundle APIs
export async function getBundles(): Promise<BundleInfo[]>
export async function getBundleMeta(bundleId: string): Promise<BundleMeta>
export async function getBundleSummary(bundleId: string): Promise<string>

// Case APIs
export async function getCaseMeta(bundleId: string, caseId: string): Promise<CaseMeta>
export async function getCaseMetrics(bundleId: string, caseId: string): Promise<CaseMetrics>
export async function getCaseSummary(bundleId: string, caseId: string): Promise<string>

// 辅助函数
export function buildCaseAssetUrl(bundleId: string, caseId: string, path: string): string
```

#### 3.2.2 定义 TypeScript 类型

**文件**: `ui/src/react/types/bundle.ts` (新文件)

```typescript
export interface BundleInfo {
  bundle_id: string;
  config_file: string;
  timestamp_ms: number;
  label?: string;
  cases: string[];
  total_tests: number;
}

export interface BundleMeta {
  bundle_id: string;
  config_file: string;
  config_content: string;
  timestamp_ms: number;
  test_params: TestParams;
  cases: CaseInfo[];
}

export interface CaseInfo {
  case_name: string;
  case_id: string;
  suites: string[];
  total_tests: number;
  has_flamegraphs: boolean;
}

export interface CaseMeta {
  case_id: string;
  case_name: string;
  bundle_id: string;
  suites: SuiteInfo[];
  total_tests: number;
  has_flamegraphs: boolean;
}

export interface SuiteInfo {
  suite_name: string;
  data_configs: string[];
  index_configs: string[];
  expr_templates: string[];
}

export interface CaseMetrics {
  tests: TestResult[];
}

export interface TestResult {
  test_id: string;
  suite_name: string;
  data_config: string;
  index_config: string;
  expression: string;
  qps: number;
  latency_ms: LatencyMetrics;
  selectivity: number;
  flamegraph?: string;
}
```

### 3.3 阶段 3：UI 组件 (第 3-4 周)

#### 3.3.1 创建新页面

**新文件**：
1. `ui/src/react/pages/BundlesPage.tsx`
   - 替代当前 RunsPage 作为主入口
   - 列出所有 bundles，带可展开的 case 摘要

2. `ui/src/react/pages/BundleDetailPage.tsx`
   - 显示 bundle 内所有 cases 的概览
   - 每个 case 的图表和表格（可折叠）
   - 无火焰图

3. `ui/src/react/pages/CaseDetailPage.tsx`
   - 单个 case 的详细视图
   - 所有指标和图表
   - 火焰图区块

**更新文件**：
4. `ui/src/react/App.tsx`
   - 更新路由到新页面结构

#### 3.3.2 创建可复用组件

**新文件**：
1. `ui/src/react/components/BundleCard.tsx`
   - BundlesPage 的可展开 bundle 卡片

2. `ui/src/react/components/CaseSummary.tsx`
   - bundle 视图中 case 的摘要卡片

3. `ui/src/react/components/CaseCharts.tsx`
   - 单个 case 特定的图表
   - 在 Bundle Detail 和 Case Detail 中复用

4. `ui/src/react/components/CaseMetricsTable.tsx`
   - 显示 case 所有测试结果的表格
   - 包含 suite 列

#### 3.3.3 更新现有组件

**要修改的文件**：
1. `ui/src/react/components/DatasetCharts.tsx`
   - 适配 case 级别数据

2. `ui/src/react/components/Flamegraphs.tsx`
   - 更新以使用 case 级别的火焰图路径

### 3.4 阶段 4：路由和导航 (第 4 周)

#### 3.4.1 更新路由

**文件**: `ui/src/react/App.tsx`

```tsx
<Routes>
  <Route path="/" element={<Navigate to="/bundles" />} />
  <Route path="/bundles" element={<BundlesPage />} />
  <Route path="/bundle/:bundleId" element={<BundleDetailPage />} />
  <Route path="/bundle/:bundleId/case/:caseId" element={<CaseDetailPage />} />
  <Route path="/compare" element={<ComparePage />} />
</Routes>
```

#### 3.4.2 面包屑导航

添加显示层次结构的面包屑组件：
```
首页 > Bundles > bool/equal.yaml > bool_equal
```

### 3.5 阶段 5：功能验证和文档 (第 5 周)

#### 3.5.1 功能验证

- 使用新结构进行端到端基准测试运行
- 验证所有文件正确生成
- 验证 UI 导航流程正常工作

#### 3.5.2 文档

1. 更新 `README.md` 添加新结构说明
2. 更新 `data_generation_schema.md` 添加 bundle/case 概念
3. 创建 UI 导航用户指南
4. 为新端点添加 API 文档

---

## 4. 详细任务分解

### 4.1 后端任务

- [x] **任务 B1**: 在 BenchmarkResult 结构体中添加 bundle_id、case_name、case_id 字段
- [x] **任务 B2**: 更新目录创建以使用 bundle/case 层次结构
- [x] **任务 B3**: 实现 WriteBundleMeta() 函数
- [x] **任务 B4**: 实现 WriteCaseMeta() 函数
- [x] **任务 B5**: 实现 WriteCaseMetrics() 函数
- [x] **任务 B6**: 更新 WriteIndexJson() 以生成 bundles 数组
- [x] **任务 B7**: 更新 main.cpp 以跟踪 bundle 和 case 信息
- [x] **任务 B8**: 更新 RunBenchmark() 中的结果写入到新结构
- [x] **任务 B9**: 为每个 case 生成 case_summary.txt
- [x] **任务 B10**: 为整个 bundle 生成 bundle_summary.txt

### 4.2 API 任务

- [ ] **任务 A1**: 创建 bundle TypeScript 类型
- [ ] **任务 A2**: 实现 getBundles() API 函数
- [ ] **任务 A3**: 实现 getBundleMeta() API 函数
- [ ] **任务 A4**: 实现 getBundleSummary() API 函数
- [ ] **任务 A5**: 实现 getCaseMeta() API 函数
- [ ] **任务 A6**: 实现 getCaseMetrics() API 函数
- [ ] **任务 A7**: 实现 getCaseSummary() API 函数
- [ ] **任务 A8**: 实现 buildCaseAssetUrl() 辅助函数

### 4.3 UI 组件任务

- [ ] **任务 U1**: 创建 BundlesPage.tsx
- [ ] **任务 U2**: 创建 BundleDetailPage.tsx
- [ ] **任务 U3**: 创建 CaseDetailPage.tsx
- [ ] **任务 U4**: 创建 BundleCard.tsx 组件
- [ ] **任务 U5**: 创建 CaseSummary.tsx 组件
- [ ] **任务 U6**: 创建 CaseCharts.tsx 组件
- [ ] **任务 U7**: 创建 CaseMetricsTable.tsx 组件
- [ ] **任务 U8**: 更新 DatasetCharts.tsx 适配 case 级别数据
- [ ] **任务 U9**: 更新 Flamegraphs.tsx 适配 case 级别路径
- [ ] **任务 U10**: 创建 Breadcrumb.tsx 导航组件
- [ ] **任务 U11**: 更新 App.tsx 添加新路由
- [ ] **任务 U12**: 在 BundleDetailPage 中添加可折叠区块
- [ ] **任务 U13**: 在 Bundle/Case Detail 页面添加对比入口

### 4.4 对比功能任务

- [ ] **任务 C1**: 实现 getComparableVersions() API 函数（获取可对比的历史版本）
- [ ] **任务 C2**: 实现 getCaseHistory() API 函数（获取 case 的历史指标）
- [ ] **任务 C3**: 创建 BundleComparePage.tsx（Bundle 级别对比页面）
- [ ] **任务 C4**: 创建 CaseComparePage.tsx（Case 级别对比页面）
- [ ] **任务 C5**: 创建 CompareTable.tsx 组件（对比表格，带颜色编码）
- [ ] **任务 C6**: 创建 TrendChart.tsx 组件（历史趋势图）
- [ ] **任务 C7**: 创建 RegressionDetector.tsx 组件（回归检测显示）
- [ ] **任务 C8**: 实现颜色编码逻辑（getChangeInfo 函数）
- [ ] **任务 C9**: 实现性能回归检测算法（detectRegressions 函数）
- [ ] **任务 C10**: 实现火焰图并排对比组件
- [ ] **任务 C11**: 添加对比结果导出功能（CSV、JSON、PNG）
- [ ] **任务 C12**: 更新路由以支持对比页面
- [ ] **任务 C13**: 实现版本选择 UI（复选框列表）

### 4.5 文档任务

- [ ] **任务 D1**: 更新 README.md
- [ ] **任务 D2**: 更新 data_generation_schema.md
- [ ] **任务 D3**: 创建 UI 导航指南
- [ ] **任务 D4**: 记录新文件结构
- [ ] **任务 D5**: 记录 API 端点

---

## 5. 关键设计决策

### 5.1 Bundle 标识

**决策**：使用 `bundle_id`（时间戳）+ `config_file` 路径

**理由**：
- 时间戳确保唯一性
- 配置文件路径提供语义含义
- 允许同一配置文件的多次运行
- 易于按时间顺序排序

### 5.2 Case 标识

**决策**：使用 `case_id` = `bundle_id_<索引>` 格式

**理由**：
- bundle 内外都唯一
- 索引保留 YAML 顺序
- 简单生成和解析

### 5.3 UI 中 Suite 的合并

**决策**：在表格中显示 suite 列，但不创建单独的可展开区块

**理由**：
- 降低 UI 复杂度
- Suite 是执行细节，不是逻辑分组
- Case 是用户主要关注的单元
- 仍然允许按 suite 过滤/排序

### 5.4 火焰图位置

**决策**：
- Bundle Detail：无火焰图
- Case Detail：该 case 的所有火焰图

**理由**：
- Bundle Detail 用于概览和对比
- 火焰图是详细的性能分析产物
- Case Detail 是用户深入分析的地方
- 减少 Bundle Detail 的页面加载时间

### 5.5 目录结构

**决策**：在 bundle 目录下嵌套 cases

**理由**：
- 清晰的层次结构映射逻辑结构
- 易于删除整个 bundle
- 简化备份和归档
- 避免命名空间冲突

---

## 6. 风险评估和缓解

### 6.1 风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|----------|
| 性能回退 | 中 | 低 | 保持文件操作高效，添加缓存 |
| UI 复杂度 | 中 | 中 | 迭代设计，收集用户反馈 |
| 大型 bundle 加载时间 | 中 | 中 | 实现懒加载和分页 |

### 6.2 缓解策略

1. **性能**：
   - 懒加载 case 详情
   - 对大型测试表格分页
   - 缓存 API 响应
   - 对昂贵组件使用 React.memo

2. **用户体验**：
   - 清晰的文档和示例
   - 分阶段推出并收集反馈

---

## 7. 成功标准

### 7.1 功能要求

- [ ] 所有基准测试生成新的 bundle/case 结构
- [ ] UI 显示 bundles 列表，带可展开的 cases
- [ ] Bundle Detail 页面显示所有 cases 的图表和表格
- [ ] Case Detail 页面显示完整的 case 详情和火焰图
- [ ] 页面间导航流畅
- [ ] 对比功能适配新结构

### 7.2 非功能要求

- [ ] 典型 bundle（10 个 cases，100 个测试）的页面加载时间 < 2 秒
- [ ] UI 在移动设备上响应良好
- [ ] 所有现有功能继续工作
- [ ] 文档完整准确

### 7.3 用户验收

- [ ] 用户能轻松找到特定 YAML 配置的结果
- [ ] 从配置 → bundle → case 的导航直观
- [ ] 对比工作流比以前更清晰
- [ ] 火焰图访问更有组织
- [ ] 整体反馈积极

---

## 8. 时间线

| 周 | 阶段 | 交付成果 |
|----|------|----------|
| 1-2 | 后端 | 新文件结构、写入器、目录组织 |
| 2-3 | API | TypeScript 类型、API 函数、数据获取 |
| 3-4 | UI | 新页面、组件、图表 |
| 4 | 导航 | 路由、面包屑、链接 |
| 5 | 功能验证和文档 | 端到端验证、文档、用户指南 |

**总预计时间**：5 周

---

## 9. 未来增强 (v2.0 后)

### 9.1 对比增强

- 跨不同 bundles 对比特定 cases
- YAML 配置的可视化差异
- 并排火焰图对比

### 9.2 历史分析

- 跟踪同一配置文件随时间的变化
- 显示性能趋势
- 回归检测

### 9.3 高级过滤

- 按性能指标过滤 cases
- 按表达式模式搜索
- 用自定义标签标记 bundles

### 9.4 导出改进

- 导出对比报告
- 为每个 bundle/case 生成 PDF 报告
- 将火焰图导出为 PNG

---

## 10. 附录

### 10.1 YAML 配置 → Bundle 映射示例

**YAML**: `bool/equal.yaml`
```yaml
cases:
  - name: bool_equal
    suites:
      - name: default
        data_configs: [balanced, skewed_9010]
        index_configs: [no_index, bitmap_index]
        expr_templates:
          - name: good_equal_true
            expr_template: good == true
```

**生成的 Bundle 结构**：
```
results/1704067200000/
├── bundle_meta.json
├── bundle_summary.txt
├── all_results.csv
└── cases/
    └── 1704067200000_0/           # bool_equal case
        ├── case_meta.json
        ├── case_metrics.json
        ├── case_summary.txt
        ├── case_results.csv
        └── flamegraphs/
            ├── 0001.svg           # balanced × no_index × good==true
            ├── 0002.svg           # balanced × bitmap_index × good==true
            ├── 0003.svg           # skewed_9010 × no_index × good==true
            └── 0004.svg           # skewed_9010 × bitmap_index × good==true
```

### 10.2 API 端点摘要

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/index` | GET | 列出所有 bundles |
| `/api/bundle/:id/meta` | GET | Bundle 元数据 |
| `/api/bundle/:id/summary` | GET | Bundle 摘要文本 |
| `/api/bundle/:id/case/:cid/meta` | GET | Case 元数据 |
| `/api/bundle/:id/case/:cid/metrics` | GET | Case 指标 |
| `/api/bundle/:id/case/:cid/summary` | GET | Case 摘要文本 |
| `/api/bundle/:id/case/:cid/assets/*` | GET | Case 资源（CSV、火焰图） |

### 10.3 文件大小估算

对于一个典型的 bundle，包含 4 个 cases，每个 case 8 个测试（总计 32 个测试）：

| 文件 | 预计大小 |
|------|----------|
| bundle_meta.json | ~5 KB |
| bundle_summary.txt | ~2 KB |
| all_results.csv | ~10 KB |
| case_meta.json（每个 case） | ~2 KB |
| case_metrics.json（每个 case） | ~8 KB |
| case_summary.txt（每个 case） | ~1 KB |
| case_results.csv（每个 case） | ~3 KB |
| flamegraph.svg（每个测试） | ~100-500 KB |

**Bundle 总大小**：~50 KB + 火焰图（变化）

---

## 文档修订历史

| 版本 | 日期 | 作者 | 变更 |
|------|------|------|------|
| 1.0 | 2025-01-12 | Initial | 创建初始重构计划 |
| 2.0 | 2025-01-12 | Update | 改为中文，移除向后兼容性 |
