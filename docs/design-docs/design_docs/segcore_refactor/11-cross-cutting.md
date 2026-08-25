# 跨模块事项：Utils 拆解、反向边修复、依赖 lint

> 设计提案。返回 [总览](README.md)。

本文档收录不属于任何单一模块、但必须完成的事项。**每一项都有明确的归属模块与阶段。**

## 1. `segcore/Utils.*` 的拆解与删除

`Utils.h`（290 行）+ `Utils.cpp`（1,839 行）是典型的跨域杂物间。生产代码中被 22 处 include：segcore 16、exec 3、query 2、index 1（连同测试文件共 29 处）。**query/exec 为了用一个 helper，被迫依赖整个 segcore utility surface。**

拆解后 `Utils.h` / `Utils.cpp` 删除，不留兼容头。

| 现符号（`Utils.h` 行号） | 去向 | 阶段 |
|---|---|---|
| `ParsePksFromFieldData` ×2、`ParsePksFromIDs`（`:33,36,41`） | `mvcc/PkCodec.h` | P1 |
| `GetSizeOfIdArray`（`:46`） | `mvcc/PkCodec.h` | P1 |
| `upper_bound(ConcurrentVector<Timestamp>&, ...)`（`:180`） | `mvcc/TimestampVisibility.cpp`，改接 `TimestampData` | P1 |
| `TimestampToPhysicalMs`（`:212`） | `common/Types.h`（本就与 segcore 无关） | P1 |
| `GetRawDataSizeOfDataArray`（`:49`） | `reduce/DataArrayBuilder.cpp` | P5 |
| `CreateEmptyScalarDataArray`、`CreateScalarDataArray`、`CreateEmptyVectorDataArray` ×2、`CreateScalarDataArrayFrom`、`CreateVectorDataArrayFrom` ×2、`CreateDataArrayFrom`、`SetUpScalarFieldData`（`:56-99`） | `reduce/DataArrayBuilder.*` | P5 |
| `MergeBase`、`MergeDataArray`（`:105,147`） | `reduce/DataArrayBuilder.*` | P5 |
| `SortEqualScoresByPks`（`:288`） | `reduce/ReduceHelper.cpp` | P5 |
| `bulk_script_field_data`（`:217`） | `reduce/Materializer.cpp` | P5 |
| `ReverseDataFromIndex`（`:151`） | `indexing/ScalarValueSource.cpp`（成为 `IScalarValueSource::Gather` 的实现） | P3 |
| `LoadArrowReaderFromRemote`、`LoadArrowReaderForJsonStatsFromRemote`、`LoadFieldDatasFromRemote`、`LoadIndexData`（`:156-198`） | `load/RemoteLoad.*` | P4 |
| `getCacheWarmupPolicy`、`getCellDataType`（`:189,195`） | `load/WarmupPolicy.*` | P4 |
| `CheckCancellation` ×2（`:237,257`） | `common/OpContext.h`（取消是通用机制，不是 segcore 概念） | P0 |
| `GetEffectiveSearchTopk`（`:271`） | `common/QueryInfo.h` | P0 |

> `CheckCancellation` 与 `GetEffectiveSearchTopk` 放在 P0 先动：它们是 exec/query 依赖 `segcore/Utils.h` 的主要原因，迁到 `common/` 之后可以立刻减少 6 个跨层 include。

## 2. 反向边修复清单

五组已确认的反向边，每组指定负责模块与修复方式。

### 2.1 `storage → segcore`

**现状：** `storage/Util.cpp:68` include `segcore/memory_planner.h`；`:1504` 使用 `segcore::ParallelDegreeSplitStrategy`。

**修复：** `memory_planner.{h,cpp}` 整体迁到 `storage/read_planning/`。它的内容（`RowGroupSplitStrategy`、`MemoryBasedSplitStrategy`、`ParallelDegreeSplitStrategy`、`CellSpec`、`BatchReaderFactory`、`CellFinalizeFunc`）本质是存储层读取规划，与 segment 概念无关。`segcore_load` 变成它的消费者。

**负责：** [load](07-load.md)。**阶段：** P4 第一步（独立可合入）。

### 2.2 `common → segcore`

**现状：** `common/ArrayOffsets.cpp:36` include `SegmentInterface.h`；`:382` `ArrayOffsetsSealed::BuildFromSegment(const void* segment, ...)` 把 `void*` 强转成 `SegmentInternalInterface*`。

（`common/init_c.cpp` 的另外两条 include 属于配置错位，见 [§2.5](#25-配置与-cache-元数据的错位)。）

**修复：** 签名改为 `BuildFromColumns(const IColumnSource&, FieldId, ...)`。`void*` 强转本身就是"这里有个不该存在的依赖"的自白——用 `void*` 绕过 include 并不能消除依赖，只是把它藏进运行时。

**负责：** [columnar](03-columnar.md)。**阶段：** P2。

### 2.3 `index → segcore`

**现状：** `index/json_stats/JsonKeyStats.cpp` include `segcore/ChunkedSegmentSealedImpl.h`（`:61`）、`segcore/storagev1translator/BsonInvertedIndexTranslator.h`（`:60`）、`segcore/storagev2translator/ManifestGroupTranslator.h`（`:62`）、`segcore/Utils.h`（`:63`）、`segcore/default_fs.h`（`:27`）。

**修复：** 依赖倒置。JsonKeyStats 需要的是"读一批 JSON 值"和"拿一个文件系统"，不是"一个 sealed segment"。

```cpp
// index/json_stats/JsonStatsSource.h —— 定义在 index 侧
namespace milvus::index {

class IJsonStatsSource {
 public:
    virtual ~IJsonStatsSource() = default;
    virtual std::unique_ptr<segcore::IColumnChunkCursor> OpenJsonColumn(FieldId) = 0;
    virtual std::shared_ptr<arrow::fs::FileSystem>       FileSystem() = 0;
};

}  // namespace milvus::index
```

segcore 侧提供实现并注入。`default_fs` 迁到 `storage/`。

**负责：** [indexing](04-indexing.md) + [load](07-load.md)。**阶段：** P4。

### 2.4 `index → exec`（构成 `segcore → index → exec → segcore` 环）

**现状：** `index/NgramInvertedIndex.h:68,93` 直接接受 `exec::SegmentExpr*` 参数。

**修复：** 改为接受 `IColumnChunkCursor` 或一个窄回调（ngram 需要的是"对候选行取原值做二次校验"，不是整个表达式对象）。

```cpp
// index/NgramInvertedIndex.h —— 修复后
class NgramInvertedIndex {
 public:
    // 原签名：ExecuteQuery(..., exec::SegmentExpr* segment, ...)
    // 新签名：只要一个按 offset 取值的回调
    using ValueFetcher = std::function<std::string_view(int64_t offset)>;

    TargetBitmap ExecuteQuery(const std::string& literal, proto::plan::OpType,
                              const ValueFetcher&);
};
```

**负责：** [indexing](04-indexing.md)。**阶段：** P3。

### 2.5 配置与 cache 元数据的错位

**现状（两条独立的小边）：**

- `index/FMIndex.h:30` include `segcore/SegcoreConfig.h`，`:227` 读 `SegcoreConfig::default_config()` 的某项配置。
- `common/init_c.cpp:35,36` include `segcore/memory_planner.h` 与 `segcore/storagev2translator/GroupCTMeta.h`，`:222` 调 `segcore::storagev2translator::SetCellTargetSizeBytes()` 把 paramtable 的值写进一个 segcore 里的全局 atomic。

**共同病因：** 全局可变配置放在了 segcore 里，于是所有需要读配置的模块都被迫依赖 segcore。

**修复：**

- `FMIndex` 需要的那项配置作为构造参数传入，或迁到 `common/Config`。这与[未决问题 1](#6-未决问题)（`SegcoreConfig` 的归属）是同一个决策。
- `GroupCTMeta` 的 `cell_target_size_bytes` 是 **cachinglayer 的调优参数**，不是 segcore 的；随 storage-v2 cache 配置一起迁到 `cachinglayer/` 或 `storage/`。
- `memory_planner` 随 [§2.1](#21-storage--segcore) 一起迁到 `storage/`，`init_c.cpp` 的 include 自动消失。

**负责：** `FMIndex` 归 [indexing](04-indexing.md)（P3）；`GroupCTMeta` 与 `memory_planner` 归 [load](07-load.md)（P4）。

### 2.6 汇总

所有数字均为**生产代码**（排除 `*Test.cpp`）。

| 边 | 当前 include 数 | 目标 | 阶段 |
|---|---|---|---|
| `storage → segcore` | 1（`storage/Util.cpp:68`） | 0 | P4 |
| `common → segcore` | 3（`ArrayOffsets.cpp:36`、`init_c.cpp:35,36`） | 0 | P2（`ArrayOffsets`）/ P4（`init_c`） |
| `index → segcore` | 6（`JsonKeyStats.cpp` 5 + `FMIndex.h:30`） | 0 | P3（`FMIndex`）/ P4（`JsonKeyStats`） |
| `index → exec` | 2 处签名（`NgramInvertedIndex.h:68,93`） | 0 | P3 |
| `query/exec → segcore`（非 contracts） | 74，分布在 50 个文件 | 0 | P2/P3/P6 |

> 测试文件中的反向 include（common 15 处、index 32 处等）不在此列——它们随 `all_tests` 一起链接完整 `milvus_core`，本就不受分层约束。但**新增的模块级测试必须遵守规则**，见 §4.2 第 4 条。

## 3. 全局构建改动

### 3.0 不引入模块间 target 边

`segcore/**/CMakeLists.txt` 中，模块 target 只允许 `target_link_libraries(<module> PUBLIC milvus_conan_deps)`。这与 `src/` 下现有 16 个组件 target 的形态一致，也是 [#35610](https://github.com/milvus-io/milvus/pull/35610) 建立的纪律。

原因是实测结论（CMake 4.2）：OBJECT 库 A `target_link_libraries` B 时，Ninja 只生成 phony 的 `cmake_object_order_depends_target_A: || cmake_object_order_depends_target_B`，A 的 `.o` 不依赖 B 的 `.o`；但 Unix Makefiles 生成 `CMakeFiles/A.dir/all: CMakeFiles/B.dir/all`，A 的全部编译等 B 的全部完成。`scripts/core_build.sh:203` 会在无 ninja 时 fallback 到 make，因此这条边不能写。

依赖方向由 §4 的 lint 强制，不由 CMake 强制。

### 3.1 移除递归 glob

`segcore/CMakeLists.txt:13` 的 `add_source_at_current_directory_recursively()` 删除，改为每个模块显式列出。

副作用：新增文件必须同时改 CMakeLists——这是**期望的**行为。递归 glob 让一个文件可以悄无声息地进入构建，模块归属无从审查。

### 3.2 `--allow-multiple-definition`

`unittest/CMakeLists.txt:141` 的全局 `LINKER:--allow-multiple-definition` 在模块级测试目标上**不开启**。它当前存在是因为 `all_tests` 链接完整 `milvus_core` 加上一些静态库，产生了符号冲突。模块测试链接面小得多，不应该需要它。

**如果某个模块测试需要这个 flag 才能链接，说明它的依赖集合仍然过大**——这是一个有用的诊断信号，不要顺手加上。

### 3.3 PCH

`src/CMakeLists.txt` 的 `MILVUS_PCH_HEADERS` 列表需要按新 target 更新。10 个 segcore 子 target 各自生成 PCH 会增加构建开销；建议只对 ≥ 10 个 `.cpp` 的 target 启用（预计是 `columnar`、`indexing`、`load`、`segment`、`app`）。

## 4. 依赖 lint

P0 交付，此后每个阶段收紧一次规则。

### 4.1 规则表述

规则以数据形式声明，脚本读取后校验：

```yaml
# internal/core/module_deps.yaml
modules:
  segcore_contracts:
    path: segcore/contracts
    allow: [common, bitset]
    max_includes_per_header: 15
    max_virtuals_per_class: 15

  segcore_mvcc:
    path: segcore/mvcc
    allow: [segcore_contracts, common, bitset, pb/plan.pb.h]

  segcore_columnar:
    path: segcore/columnar
    allow: [segcore_contracts, common, bitset, mmap, cachinglayer, index/SkipIndex.h]

  segcore_indexing:
    path: segcore/indexing
    allow: [segcore_contracts, segcore_columnar, common, bitset, index, cachinglayer]

  segcore_text:
    path: segcore/text
    allow: [segcore_contracts, segcore_columnar, common, milvus-storage, arrow]
    deny:  [index]                      # TEXT 值 ≠ TEXT 索引

  segcore_reduce:
    path: segcore/reduce
    allow: [segcore_contracts, common, pb, query, knowhere]

  segcore_load:
    path: segcore/load
    allow: [segcore_contracts, segcore_columnar, segcore_indexing, segcore_text,
            segcore_mvcc, storage, milvus-storage, index, common]
    proto_allowlist: [load/LoadInfoProtoAdapter.cpp, load/LoadInfoProtoAdapter.h]

  segcore_segment:
    path: segcore/segment
    allow: [segcore_contracts, segcore_load, segcore_columnar, segcore_indexing,
            segcore_mvcc, segcore_text, common, folly]
    deny:  [query, exec, pb]

  segcore_app:
    path: segcore/app
    allow: [segcore_segment, segcore_reduce, segcore_load, segcore_contracts,
            query, exec, futures, folly, storage, pb, common]
    deny:  [segcore_columnar, segcore_indexing, segcore_mvcc, segcore_text]

  segcore_capi:
    path: segcore/capi
    allow: [segcore_app, segcore_contracts, common]
    deny:  [query, exec, folly, pb, milvus-storage]
    max_function_lines: 30

external_rules:
  - { from: query,   deny_include: "segcore/", except: "segcore/contracts/" }
  - { from: exec,    deny_include: "segcore/", except: "segcore/contracts/" }
  - { from: index,   deny_include: "segcore/", except: "segcore/contracts/" }
  - { from: storage, deny_include: "segcore/" }
  - { from: common,  deny_include: "segcore/" }
```

### 4.2 检查项

1. **include 白名单**：解析每个 `.h`/`.cpp` 的 `#include`，比对 `allow`/`deny`。
2. **无环**：把模块间 include 建成有向图，检测环。任何环即失败——**这是最重要的一条**，因为它无法靠人工 review 稳定发现。
3. **无递归 glob**：`grep add_source_at_current_directory_recursively segcore/**/CMakeLists.txt` 必须为空。
3b. **无模块间 target 边**：`segcore/**/CMakeLists.txt` 中 `target_link_libraries(<module> ...)` 的实参只允许是 INTERFACE 库（`milvus_conan_deps`、`segcore_contracts`）。测试可执行文件不受此限（叶子节点）。理由见 [README §6](README.md#关键约束模块间不建立-cmake-target-边)。
4. **测试链接面**：解析每个 `test_segcore_*` 的 `target_link_libraries`，断言不含 `milvus_core`。
5. **规模约束**：contracts 的 include/virtual 上限、capi 的函数行数上限、segment impl 的文件行数上限。
6. **proto 隔离**：`proto::segcore::SegmentLoadInfo` 只允许出现在 `proto_allowlist` 与 `capi/`。

### 4.3 渐进启用

lint 支持 `baseline` 文件记录当前违规数，只禁止**新增**违规，并要求每个阶段把对应条目的 baseline 归零：

| 阶段 | 归零的 baseline 条目 |
|---|---|
| P0 | 递归 glob；`common`/`storage` → segcore 中的 `CheckCancellation` 相关引用 |
| P1 | `segcore_mvcc` 的全部规则 |
| P2 | `common → segcore`；`exec → segcore` 的列访问部分 |
| P3 | `index → exec`；`exec → segcore` 的索引访问部分 |
| P4 | `storage → segcore`；`index → segcore`；proto 隔离 |
| P5 | `segcore_text` 的 `deny: index`；reduce 规则 |
| P6 | capi 全部规则；`segment` 的 `deny: query/exec` |
| P7 | 全部；删除 baseline 文件 |

**baseline 文件在 P7 被删除**——如果它还在，说明重构没完成。

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 热路径性能回归（游标引入间接层） | P2 合入前必须跑 search/retrieve 基准，差异 > 3% 则回退设计；虚调用锁定在 chunk 粒度（[03 §5](03-columnar.md#5-性能约束)） |
| 迁移期双实现共存导致行为漂移 | 每个能力接口落地时，旧实现改为转发到新实现（而非并行两套逻辑）；转发适配器在 P7 强制删除 |
| `ApplyLoadDiff` 迁移（P4 最大一步）引入加载 bug | 按 plan 字段分批（索引 → column group → binlog → 默认值 → text/json），每批独立 commit；`PlanReopen` 纯函数先行落地并被表驱动测试覆盖 |
| 模块数过多导致构建变慢 | 10 个 OBJECT target 不改变编译单元总数；PCH 按 §3.3 只对大 target 启用；预期头文件传染减少反而加快构建 |
| 重构期间与其他 feature 分支冲突 | 每个阶段独立可合入且保持 `all_tests` 全绿；优先合入纯迁移 commit（无逻辑改动），减少长期分叉 |

## 6. 未决问题

以下问题在设计阶段没有定论，需要在对应阶段决策：

1. **`SegcoreConfig` 的归属。** 它被几乎所有模块读取（chunk rows、interim index 开关、mmap 配置）。选项：(a) 放 `common/`；(b) 每个模块只接受自己需要的字段作为构造参数。倾向 (b)——全局配置对象是另一种形式的 God Object，但需要评估构造参数膨胀。**P1 决策。**
2. **`OpContext` 的传递方式。** 当前几乎每个方法都带 `OpContext*` 参数（取消 + tracing）。是否改为 thread-local 或 executor-bound？影响所有契约签名。**P0 决策**（决定后契约签名就定型了）。
3. **growing 与 sealed 是否共享同一套 `IColumnSource` 实现。** 二者的 chunk 语义差异较大（`ConcurrentVector` vs `ChunkedColumnInterface`），当前设计是两个实现。若差异可归一化，可合并。**P2 决策。**
4. **`IScalarPredicateIndex` 是否需要模板化。** 当前 `index::ScalarIndex<T>` 是模板；契约用 `ScalarValue` variant 抹平类型会有装箱开销。需在 P3 用基准数据决定：variant 还是 CRTP 还是保留模板 + 类型擦除的双层。**P3 决策，需基准支撑。**
