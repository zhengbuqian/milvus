# 阶段 0：基建（依赖 baseline、lint、建立组件 target、common hygiene）

> **状态：设计提案，讨论中。** 返回 [总览](README.md)。
> 现状事实基于 master `e255009e01`，数据由全 core include 扫描得出（仅生产文件，排除 `*Test*`/`*test*`）。

## 1. 为什么阶段 0 必须先做

阶段 0 不改任何业务逻辑，它产出的是后续所有阶段的验收工具与起点数据：

- 没有 baseline，"边界更清晰"无法验证，重构结束时无法回答"到底改善了多少"。
- 没有 lint，每个阶段修好的边界会在下个阶段被新代码破坏——递归 glob 让新文件可以无声进入任何组件。
- 没有 hygiene 轨道，L0 的 `common` 会持续阻塞每个阶段（它今天依赖了 L1/L3/L4 三层）。

阶段 0 的所有产出都是工具与数据，不含接口设计，因此可以与阶段 1 并行推进。

## 2. 现状核查

### 2.1 全 core 依赖违规 baseline

扫描全部组件生产文件的 `#include`，按[总览 §3 的目标分层](README.md#3-目标分层图)判定，得到 149 处违规，分三类：

**A. 跨层的违规依赖（Lk → L>k）：62 处** —— 这是重构的主要清理对象。

| 源 | 目标 | 处数 | 代表位置 | 所属阶段 |
|---|---|---|---|---|
| `common` (L0) | `storage` (L1) | 7 | `Common.{h,cpp}`→`ThreadPool.h`/`EntryStreamUtils.h`；`ChunkTarget.h`、`ChunkWriter.cpp`→`FileWriter.h` | 阶段 0 hygiene |
| `common` (L0) | `exec` (L4) | 5 | `ElementFilterIterator.{h,cpp}`→`QueryContext.h`/`Expr.h`/`EvalCtx.h`；`init_c.cpp`→`ExprCache.h` | 阶段 0 hygiene |
| `common` (L0) | `segcore` (L3) | 3 | `ArrayOffsets.cpp`→`SegmentInterface.h`；`init_c.cpp`→`memory_planner.h`/`GroupCTMeta.h` | 阶段 0 hygiene |
| `common` (L0) | `mmap` (L1) | 1 | `ArrayOffsets.cpp` | 阶段 0 hygiene |
| `futures` (L0) | `storage` (L1) | 1 | | 阶段 0 hygiene |
| `storage` (L1) | `index` (L2) | 5 | `FileManager.h`→`index/Meta.h`；`Mem/DiskFileManagerImpl.cpp`、`Util.cpp`→`index/Utils.h` | 阶段 1（随 FileSink/Source 倒置） |
| `storage` (L1) | `segcore` (L3) | 1 | `Util.cpp`→`memory_planner.h` | 阶段 2 |
| `mmap` (L1) | `segcore` (L3) | 3 | `ChunkVector.h`→`SegcoreConfig.h`；`ChunkedColumnGroup.h`→`GroupCTMeta.h`；`ChunkedColumn.h`→`storagev1translator/ChunkTranslator.h` | 阶段 1 并行项（columnar-format 独立的阻塞项） |
| `index` (L2) | `segcore` (L3) | 6 | `JsonKeyStats.cpp` ×5、`FMIndex.h` | 全部在阶段 1 内降到 0：`FMIndex.h` 走配置注入；`JsonKeyStats` 的 5 处随目录迁到 `segcore/json_stats/` 当场消失（断继承 + `git mv`，无行为变更，见 [01 §1 过渡处理](01-scalar-index.md#1-范围)） |
| `index` (L2) | `exec` (L4) | 1 | `NgramInvertedIndex.h` | 阶段 1 |
| `index` (L2) | `query` (L4) | 1 | | 阶段 1 |
| `segcore` (L3) | `query`/`exec`/`plan`/`rescores`/`expr` (L4) | 28 | `SegmentInterface.cpp` 构造 `ExecPlanNodeVisitor` 等 | 阶段 3 / 阶段 4（编排上移） |

**B. 同层内部边：3 处** —— `mmap` ↔ `storage`（各 1–2 处），需在 columnar-format 确定时定序。

**C. L4 内部未定序：84 处** —— `plan`/`expr`/`query`/`exec`/`rescores` 五者互相 include（`exec→plan` 25、`exec→expr` 23、`exec→query` 11、`query→exec` 7…）。它们同属 L4，当前没有内部次序定义，这正是 [阶段 4 query 三分](README.md#7-阶段计划) 要解决的问题。阶段 0 只记录，不判违规。

**19 对双向边**，最严重的几对（两个方向的 include 数）：`common ↔ segcore` 3/378、`common ↔ exec` 5/382、`segcore ↔ query` 21/14、`index ↔ segcore` 6/74、`storage ↔ index` 5/140、`mmap ↔ segcore` 3/20。

> **两条此前未被记录的发现**（早前的 segcore 域内清单只列了 5 组违反分层方向的依赖）：
> 1. **`common` 依赖了 L1/L3/L4 三层共 16 处**——不只是"3 处 segcore include"。`ElementFilterIterator` 让 L0 直接依赖 exec 的表达式内核，是全 core 最深的一条违反分层方向的依赖。
> 2. **`mmap` → `segcore` 3 处**。mmap 要独立成 columnar-format，这三条是硬阻塞：`SegcoreConfig`（配置错位，与 `FMIndex` 是同样的问题）、`GroupCTMeta`（cachinglayer 调优参数）、`ChunkTranslator`（translator 属于 segcore load）。

### 2.2 现有检查基建

| 事实 | 位置 | 对阶段 0 的意义 |
|---|---|---|
| **已有 bespoke 静态守卫先例**：allowlist + 每条注释说明豁免理由 + 退出码语义 | `scripts/check_segment_timestamp_usage.sh` | 阶段 0 照抄这个形态即可，不需要引入新框架 |
| 但该脚本未接入 Makefile 与 CI（grep 无调用点） | — | 阶段 0 必须显式接入 |
| `make cppcheck` 实际只做 clang-format 检查 | `Makefile:165`→`scripts/check_cpp_fmt.sh` | C++ 侧目前没有任何架构约束检查 |
| CI 入口已覆盖所需路径 | `.github/workflows/code-checker.yaml`（paths 含 `scripts/**`、`internal/**`） | 接入成本低 |

### 2.3 组件 target 现状

- 16 个组件 target 中 14 个使用 `add_source_at_current_directory_recursively()`；只有 `bitset` 和 `storage`（98 个文件）使用显式 source list——显式列表在本仓库已被证明可行。
- `mmap` 已由 [#51504](https://github.com/milvus-io/milvus/pull/51504) 建立正式 target `milvus_mmap`，但沿用了递归 glob。
- `expr/` 仍无 target（单文件 `ITypeExpr.h`），且它反向 include `exec/expression/function/FunctionFactory.h`；消费者横跨 plan/query/rescores/segcore/exec 五个组件。

## 3. 阶段 0 交付物

### 3.1 `internal/core/module_deps.yaml` —— 规则声明

全 core 版本的规则表（segcore 内部模块之间的更细规则留到阶段 3 定）：

```yaml
layers:                      # 组件 → 层号，唯一事实来源
  bitset: 0
  common: 0
  monitor: 0
  futures: 0
  pb: 0
  storage: 1
  mmap: 1                    # → columnar-format
  index: 2
  segcore: 3
  plan: 4
  exec: 4
  query: 4                   # 阶段 4 后消失
  rescores: 4                # 合并候选
  indexbuilder: 5
  clustering: 5

rules:
  - unidirectional: true     # 只允许 Lk → L(<k)
  - no_cycles: true          # 组件级 include 图无环
  - no_recursive_glob: true
  - no_inter_target_link: true   # 组件 target 间不得 target_link_libraries（#35610 纪律）

component_overrides:         # 阶段专属规则，随阶段收紧
  index:
    deny: [segcore, exec, query]
    deny_symbol: [knowhere]   # 仅标量索引类型与共享基类，阶段 1 生效
  common:
    deny: [storage, mmap, index, segcore, exec, query, plan]
  mmap:
    deny: [segcore, index]
```

### 3.2 `scripts/check_core_deps.py` —— lint 脚本

形态照抄 `check_segment_timestamp_usage.sh`（allowlist + 注释说明 + 退出码），实现用 Python（需要建图判环）。检查项：

1. **include 白/黑名单**：解析每个生产文件的 `#include`，按 `module_deps.yaml` 校验。
2. **无环**：组件级 include 图检测环——这条最重要，人工 review 无法稳定发现。
3. **无递归 glob**：`grep add_source_at_current_directory_recursively`。
4. **无组件间 target 边**：`target_link_libraries(<component> ...)` 实参只允许 INTERFACE 库；测试可执行文件（叶子）豁免。
5. **baseline 比对**：只对新增违规失败（§3.3）。

测试文件不在约束内（它们随 `all_tests` 链接完整 `milvus_core`），但新增的模块级测试目标受约束——这条在阶段 3 随 segcore 模块测试一起生效。

### 3.3 baseline 与渐进机制

`internal/core/module_deps_baseline.txt` 记录 §2.1 的 62 + 3 处（L4 内部 84 处单列为 `pending_layer_split`，阶段 4 前不计违规）。

- **新增违规 → CI 失败**；存量违规不阻塞。
- 每个阶段必须把其负责的 baseline 条目降到 0（见 §2.1 表的"所属阶段"列），降到 0 后从 baseline 删除。
- **baseline 文件在阶段 5 结束时删除**——如果它还在，说明重构没完成。

### 3.4 CI 接入

```makefile
core-deps-check:
	@python3 scripts/check_core_deps.py --config internal/core/module_deps.yaml \
	                                    --baseline internal/core/module_deps_baseline.txt
```

挂到 `code-checker.yaml`（paths 已覆盖），并把已存在但未接入的 `check_segment_timestamp_usage.sh` 一并接上。

### 3.5 `expr/` 建立正式 target

建 `milvus_expr` target（显式 source list）并进入 `milvus_core` 聚合。它的目标所属组件是 plan（L4），阶段 0 只做建 target 与建边记录，`ITypeExpr.h` → `FunctionFactory.h` 的违反分层方向的 include 留给阶段 4 的 query 三分处理。

`mmap` 的递归 glob 改显式列表（顺带）。其余 13 个组件的 glob 改造随各自阶段进行，阶段 0 只让 lint 规则记录。

### 3.6 common hygiene 轨道（与阶段 1 并行）

`common` 的 16 处反向依赖按所属组件分四组：

| 文件 | 现状 | 所属组件 | 理由 |
|---|---|---|---|
| `ElementFilterIterator.{h,cpp}` | → exec/QueryContext、Expr、EvalCtx（5 处） | **迁出 common → `exec/`** | 它是表达式求值的迭代器，本就是 exec 的东西；L0 不该有执行语义 |
| `ArrayOffsets.cpp` | → segcore/SegmentInterface、mmap（2 处）；`BuildFromSegment(const void*)` 把 `void*` 强转成 `SegmentInternalInterface*` | **迁出 common → columnar-format**；签名改 `BuildFromColumns(ColumnInterface&, ...)` | 数组列的 offset 是列内存表示的一部分；`void*` 强转是这条依赖不该存在的证据 |
| `Common.{h,cpp}`、`ChunkTarget.h`、`ChunkWriter.cpp` | → storage/ThreadPool、FileWriter、EntryStreamUtils（7 处） | 依赖倒置：ThreadPool/FileWriter 抽象下移到 `common/`，或这些文件上移 `storage/` | 逐个判定：`ChunkWriter` 写文件 → 归 storage；`Common.h` 的线程池句柄 → 抽象下移 |
| `init_c.cpp` | → storage、exec、segcore ×2（4 处） | 拆分：各组件自己的 init 归各组件；`memory_planner`/`GroupCTMeta` 随阶段 2 / 阶段 3 迁走后自动消失 | 全局 init 是天然的"什么都认识"，应当是最上层的 capi 而非 L0 |
| `futures` → storage（1 处） | | 同上判定 | |

> **`Types.h` 不在阶段 0 范围**。它 33 个 include、被 195 个生产文件 include，是巨型头文件，但拆它会触碰全仓——列为独立议题，阶段 3 后评估。阶段 0 只在 lint 中记录其扇出作为观测指标。

## 4. 阶段 0 验收标准

- [ ] `module_deps.yaml` 覆盖全部 18 个目录，每个组件有层号与职责定义（后者在[总览 §4](README.md#4-组件定义)）。
- [ ] `check_core_deps.py` 实现，五项检查全部完成，本地可跑。
- [ ] baseline 文件生成，数值与 §2.1 一致（62 + 3 违规，84 处 `pending_layer_split`）。
- [ ] CI 接入完成：新增违规导致 `code-checker` 失败；`check_segment_timestamp_usage.sh` 一并接上。
- [ ] `expr/` 建立 target `milvus_expr`；`mmap` 改显式 source list。
- [ ] common hygiene 至少完成 `ElementFilterIterator` 与 `ArrayOffsets` 两项（消灭 L0→L4 与 L0→L3 边，共 8 处），baseline 相应降到 0。

## 5. 非目标

- **不做接口设计**。阶段 0 不定义任何新接口。
- **不动 `Types.h`**（见 §3.6 注）。
- **不改 CMake 构建形态**（组件间不建 target 边这条纪律沿用 [#35610](https://github.com/milvus-io/milvus/pull/35610)，阶段 0 只是把它写进 lint）。
- **不追求 baseline 降到 0**。阶段 0 的成果是"违规不再增长"，降到 0 是各阶段的责任。
