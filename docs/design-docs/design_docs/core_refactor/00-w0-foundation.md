# W0：基建（依赖 baseline、lint、组件转正、common hygiene）

> **状态：设计提案，讨论中。** 返回 [总览](README.md)。
> 现状事实基于 master `e255009e01`，数据由全 core include 扫描得出（仅生产文件，排除 `*Test*`/`*test*`）。

## 1. 为什么 W0 必须先做

W0 **不改任何业务逻辑**，它产出的是后续所有波次的**验收工具与起点数据**：

- 没有 baseline，"边界更清晰"不可证伪，重构结束时无法回答"到底改善了多少"。
- 没有 lint，每一波修好的边界会在下一波被新代码悄悄破坏——递归 glob 让新文件可以无声进入任何组件。
- 没有 hygiene 轨道，L0 的 `common` 会持续绊住每一波（它今天依赖了 L1/L3/L4 三层）。

W0 的所有产出都是**工具与数据**，不含接口设计，因此可以与 W1 并行推进。

## 2. 现状核查

### 2.1 全 core 依赖违规 baseline

扫描全部组件生产文件的 `#include`，按[总览 §3 的目标分层](README.md#3-目标分层图)判定，得到 **149 处**违规，分三类：

**A. 真·跨层反向边（Lk → L>k）：62 处** —— 这是重构的主要清理对象。

| 源 | 目标 | 处数 | 代表位置 | 归属波次 |
|---|---|---|---|---|
| `common` (L0) | `storage` (L1) | 7 | `Common.{h,cpp}`→`ThreadPool.h`/`EntryStreamUtils.h`；`ChunkTarget.h`、`ChunkWriter.cpp`→`FileWriter.h` | W0 hygiene |
| `common` (L0) | `exec` (L4) | 5 | `ElementFilterIterator.{h,cpp}`→`QueryContext.h`/`Expr.h`/`EvalCtx.h`；`init_c.cpp`→`ExprCache.h` | W0 hygiene |
| `common` (L0) | `segcore` (L3) | 3 | `ArrayOffsets.cpp`→`SegmentInterface.h`；`init_c.cpp`→`memory_planner.h`/`GroupCTMeta.h` | W0 hygiene |
| `common` (L0) | `mmap` (L1) | 1 | `ArrayOffsets.cpp` | W0 hygiene |
| `futures` (L0) | `storage` (L1) | 1 | | W0 hygiene |
| `storage` (L1) | `index` (L2) | 5 | **`FileManager.h`→`index/Meta.h`**；`Mem/DiskFileManagerImpl.cpp`、`Util.cpp`→`index/Utils.h` | W1（随 FileSink/Source 倒置） |
| `storage` (L1) | `segcore` (L3) | 1 | `Util.cpp`→`memory_planner.h` | W2 |
| `mmap` (L1) | `segcore` (L3) | 3 | `ChunkVector.h`→`SegcoreConfig.h`；`ChunkedColumnGroup.h`→`GroupCTMeta.h`；`ChunkedColumn.h`→`storagev1translator/ChunkTranslator.h` | W1∥（columnar-format 独立的阻塞项） |
| `index` (L2) | `segcore` (L3) | 6 | `JsonKeyStats.cpp` ×5、`FMIndex.h` | **全部 W1 内清零**：`FMIndex.h` 走配置注入；`JsonKeyStats` 的 5 处随目录迁到 `segcore/json_stats/` 当场消失（断继承 + `git mv`，无行为变更，见 [01 §1 过渡处理](01-scalar-index.md#1-范围)） |
| `index` (L2) | `exec` (L4) | 1 | `NgramInvertedIndex.h` | W1 |
| `index` (L2) | `query` (L4) | 1 | | W1 |
| `segcore` (L3) | `query`/`exec`/`plan`/`rescores`/`expr` (L4) | 28 | `SegmentInterface.cpp` 构造 `ExecPlanNodeVisitor` 等 | W3/W4（编排上移） |

**B. 同层内部边：3 处** —— `mmap` ↔ `storage`（各 1–2 处），需在 columnar-format 定型时定序。

**C. L4 内部未定序：84 处** —— `plan`/`expr`/`query`/`exec`/`rescores` 五者互相 include（`exec→plan` 25、`exec→expr` 23、`exec→query` 11、`query→exec` 7…）。它们同属 L4，当前**没有内部次序定义**，这正是 [W4 query 三分](README.md#7-波次计划) 要解决的问题。W0 只记录，不判违规。

**19 对双向边**，最严重的几对（两个方向的 include 数）：`common ↔ segcore` 3/378、`common ↔ exec` 5/382、`segcore ↔ query` 21/14、`index ↔ segcore` 6/74、`storage ↔ index` 5/140、`mmap ↔ segcore` 3/20。

> **两条此前未被记录的发现**（[segcore 章 11-cross-cutting](../segcore_refactor/11-cross-cutting.md) 只列了 5 组边）：
> 1. **`common` 依赖了 L1/L3/L4 三层共 16 处**——不只是"3 处 segcore include"。`ElementFilterIterator` 让 L0 直接依赖 exec 的表达式内核，是全 core 最深的一条反向边。
> 2. **`mmap` → `segcore` 3 处**。mmap 要独立成 columnar-format，这三条是硬阻塞：`SegcoreConfig`（配置错位，与 `FMIndex` 同病）、`GroupCTMeta`（cachinglayer 调优参数）、`ChunkTranslator`（translator 属于 segcore load）。

### 2.2 现有检查基建

| 事实 | 位置 | 对 W0 的意义 |
|---|---|---|
| **已有 bespoke 静态守卫先例**：allowlist + 每条注释说明豁免理由 + 退出码语义 | `scripts/check_segment_timestamp_usage.sh` | **W0 照抄这个形态即可，不需要引入新框架** |
| 但该脚本**未接入** Makefile 与 CI（grep 无调用点） | — | W0 必须显式接线，否则 lint 形同虚设 |
| `make cppcheck` 实际只做 clang-format 检查 | `Makefile:165`→`scripts/check_cpp_fmt.sh` | C++ 侧目前**没有任何架构约束检查** |
| CI 入口已覆盖所需路径 | `.github/workflows/code-checker.yaml`（paths 含 `scripts/**`、`internal/**`） | 接线成本低 |

### 2.3 组件 target 现状

- 16 个组件 target 中 **14 个使用 `add_source_at_current_directory_recursively()`**；只有 `bitset` 和 **`storage`（98 个文件）** 使用显式 source list——**显式列表在本仓库已被证明可行**，不是理想主义。
- `mmap` 已由 [#51504](https://github.com/milvus-io/milvus/pull/51504) 转正为 `milvus_mmap`，但沿用了递归 glob。
- `expr/` 仍无 target（单文件 `ITypeExpr.h`），且它反向 include `exec/expression/function/FunctionFactory.h`；消费者横跨 plan/query/rescores/segcore/exec 五个组件。

## 3. W0 交付物

### 3.1 `internal/core/module_deps.yaml` —— 规则声明

全 core 版本的规则表（segcore 内部模块规则见 [segcore 章 11-cross-cutting §4.1](../segcore_refactor/11-cross-cutting.md#41-规则表述)，W3 时并入）：

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
  query: 4                   # W4 后消失
  rescores: 4                # 归并候选
  indexbuilder: 5
  clustering: 5

rules:
  - unidirectional: true     # 只允许 Lk → L(<k)
  - no_cycles: true          # 组件级 include 图无环
  - no_recursive_glob: true
  - no_inter_target_link: true   # 组件 target 间不得 target_link_libraries（#35610 纪律）

component_overrides:         # 波次专属规则，随波次收紧
  index:
    deny: [segcore, exec, query]
    deny_symbol: [knowhere]   # 仅标量族与共享根，W1 生效
  common:
    deny: [storage, mmap, index, segcore, exec, query, plan]
  mmap:
    deny: [segcore, index]
```

### 3.2 `scripts/check_core_deps.py` —— lint 脚本

形态照抄 `check_segment_timestamp_usage.sh`（allowlist + 注释说明 + 退出码），实现用 Python（需要建图判环）。检查项：

1. **include 白/黑名单**：解析每个生产文件的 `#include`，按 `module_deps.yaml` 校验。
2. **无环**：组件级 include 图检测环——**这条最重要**，人工 review 无法稳定发现。
3. **无递归 glob**：`grep add_source_at_current_directory_recursively`。
4. **无组件间 target 边**：`target_link_libraries(<component> ...)` 实参只允许 INTERFACE 库；测试可执行文件（叶子）豁免。
5. **baseline 比对**：只对**新增**违规失败（§3.3）。

**测试文件不在约束内**（它们随 `all_tests` 链接完整 `milvus_core`），但新增的模块级测试目标受约束——这条在 W3 随 segcore 模块测试一起生效。

### 3.3 baseline 与渐进机制

`internal/core/module_deps_baseline.txt` 记录 §2.1 的 62 + 3 处（L4 内部 84 处单列为 `pending_layer_split`，W4 前不计违规）。

- **新增违规 → CI 失败**；存量违规不阻塞。
- 每波次**必须归零**其负责的 baseline 条目（见 §2.1 表的"归属波次"列），归零后从 baseline 删除。
- **baseline 文件在 W5 结束时删除**——如果它还在，说明重构没完成。

### 3.4 CI 接线

```makefile
core-deps-check:
	@python3 scripts/check_core_deps.py --config internal/core/module_deps.yaml \
	                                    --baseline internal/core/module_deps_baseline.txt
```

挂到 `code-checker.yaml`（paths 已覆盖），并把已存在但未接线的 `check_segment_timestamp_usage.sh` 一并接上。

### 3.5 `expr/` 转正

建 `milvus_expr` target（显式 source list）并进入 `milvus_core` 聚合。它的目标归宿是 plan（L4），W0 只做转正与建边记录，`ITypeExpr.h` → `FunctionFactory.h` 的反向 include 留给 W4 的 query 三分处理。

`mmap` 的递归 glob 改显式列表（顺带）。其余 13 个组件的 glob 改造随各自波次进行，W0 只让 lint 规则**记账**。

### 3.6 common hygiene 轨道（与 W1 并行）

`common` 的 16 处反向依赖按去向分四组：

| 文件 | 现状 | 去向 | 理由 |
|---|---|---|---|
| `ElementFilterIterator.{h,cpp}` | → exec/QueryContext、Expr、EvalCtx（5 处） | **迁出 common → `exec/`** | 它是表达式求值的迭代器，本就是 exec 的东西；L0 不该有执行语义 |
| `ArrayOffsets.cpp` | → segcore/SegmentInterface、mmap（2 处）；`BuildFromSegment(const void*)` 把 `void*` 强转成 `SegmentInternalInterface*` | **迁出 common → columnar-format**；签名改 `BuildFromColumns(ColumnInterface&, ...)` | 数组列的 offset 是列内存表示的一部分；`void*` 强转是"这里有条不该存在的依赖"的自白 |
| `Common.{h,cpp}`、`ChunkTarget.h`、`ChunkWriter.cpp` | → storage/ThreadPool、FileWriter、EntryStreamUtils（7 处） | 依赖倒置：ThreadPool/FileWriter 抽象下沉 `common/`，或这些文件上移 `storage/` | 逐个判定：`ChunkWriter` 写文件 → 归 storage；`Common.h` 的线程池句柄 → 抽象下沉 |
| `init_c.cpp` | → storage、exec、segcore ×2（4 处） | 拆分：各组件自己的 init 归各组件；`memory_planner`/`GroupCTMeta` 随 W2/W3 迁走后自动消失 | 全局 init 是天然的"什么都认识"，应当是**最上层**的 capi 而非 L0 |
| `futures` → storage（1 处） | | 同上判定 | |

> **`Types.h` 不在 W0 范围**。它 33 个 include、被 195 个生产文件 include，是 god header 实锤，但拆它会触碰全仓——**列为独立议题，W3 后评估**。W0 只在 lint 中记录其扇出作为观测指标。

## 4. W0 出口标准

- [ ] `module_deps.yaml` 覆盖全部 18 个目录，每个组件有层号与一句话定义（后者在[总览 §4](README.md#4-组件定义)）。
- [ ] `check_core_deps.py` 落地，五项检查全部实现，本地可跑。
- [ ] baseline 文件生成，数值与 §2.1 一致（62 + 3 违规，84 处 `pending_layer_split`）。
- [ ] CI 接线完成：新增违规导致 `code-checker` 失败；`check_segment_timestamp_usage.sh` 一并接上。
- [ ] `expr/` 转正为 `milvus_expr`；`mmap` 改显式 source list。
- [ ] common hygiene 至少完成 `ElementFilterIterator` 与 `ArrayOffsets` 两项（消灭 L0→L4 与 L0→L3 边，共 8 处），baseline 相应归零。

## 5. 非目标

- **不做接口设计**。W0 不定义任何新契约。
- **不动 `Types.h`**（见 §3.6 注）。
- **不改 CMake 构建形态**（组件间不建 target 边这条纪律沿用 [#35610](https://github.com/milvus-io/milvus/pull/35610)，W0 只是把它写进 lint）。
- **不追求 baseline 归零**。W0 的成果是"违规不再增长"，归零是各波次的责任。
