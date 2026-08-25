# L3 `segcore_load` —— 加载、reopen 与物化

> 设计提案。返回 [总览](README.md)。

## 1. 职责

把"外部声明的 segment 应该长什么样"变成"内存里实际存在的资源"。

- **声明**：`LoadSpec` —— 不可变的加载声明（字段、索引、binlog、column group、manifest、TEXT/JSON 索引）。
- **现状**：`RuntimeInventory` —— 当前实际持有哪些列、索引、reader。
- **规划**：`ReopenPlanner` —— 纯函数 `diff(inventory, spec) → LoadPlan`。
- **执行**：`LoadExecutor` —— 按 plan 拉取数据、构造 translator、产出新的 inventory。
- **协议适配**：`LoadInfoProtoAdapter` —— proto ↔ `LoadSpec`，是整个 segcore 里**唯一**认识 `proto::segcore::SegmentLoadInfo` 的地方。
- storage v1 / v2 translator。

## 2. 边界

**不属于本模块：**

- 加载完成后的**发布**（copy-on-write 换代、读写门）。属于 [L4 segment](08-segment.md)。本模块产出不可变的 inventory，谁来把它换上去是 segment 的事。
- 索引与列的**使用**。属于 [indexing](04-indexing.md) / [columnar](03-columnar.md)。
- 加载的触发与并发编排（异步 executor、取消源）。属于 [L5 app](09-application.md)。
- 内存预算与 IO 并行度规划。**`memory_planner` 迁出 segcore，落到 `storage/`**，见 §6。

## 3. 现状来源与问题

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/SegmentLoadInfo.h:413` `SegmentLoadInfo`（1,324 行头 + 1,078 行实现） | 拆成 `load/LoadSpec.h`、`load/LoadInfoProtoAdapter.*` | 见 §3.1 |
| `segcore/SegmentLoadInfo.h:52` `LoadDiff` | `load/LoadPlan.h` | 结构基本保留，改名以表达"计划"而非"差异" |
| `SegmentLoadInfo::ComputeDiff*`（7 个方法，`SegmentLoadInfo.cpp:251-1021`） | `load/ReopenPlanner.cpp` | 改为纯函数，见 §4.3 |
| `ChunkedSegmentSealedImpl::ApplyLoadDiff` / `PrepareLoadDiffForReopen` / `LoadBatchIndexes` / `StageLoadColumnGroup*`（`ChunkedSegmentSealedImpl.h:2123` 附近一组） | `load/LoadExecutor.cpp` | 从 sealed impl 中析出，是 9,949 行里最大的一块 |
| `RuntimeResourceState`（`ChunkedSegmentSealedImpl.h:340`，22 个字段） | 拆成 `columnar::ColumnInventory` + `indexing::IndexInventory` + `text::TextLobInventory` + `load/RuntimeInventory.h` | 见 §4.2 |
| `segcore/storagev1translator/*`（7 个 translator） | `load/v1/*` | 原样迁移 |
| `segcore/storagev2translator/*`（4 个 translator） | `load/v2/*` | 原样迁移 |
| `segcore/Utils.{h,cpp}` 的 `LoadFieldDatasFromRemote` / `LoadArrowReaderFromRemote` / `LoadArrowReaderForJsonStatsFromRemote` / `LoadIndexData` / `getCacheWarmupPolicy` / `getCellDataType` | `load/RemoteLoad.*` | |
| `segcore/memory_planner.{h,cpp}` | **迁出到 `storage/read_planning/`** | 见 §6 |

### 3.1 `SegmentLoadInfo` 混了四件事

当前这个类同时是：

1. proto 的持有者与解析缓存（`converted_field_index_cache_`、`field_binlog_cache_`、`json_index_path_cache_` 等 8 个 cache 成员）
2. 声明的查询接口（"这个字段有没有索引"）
3. diff 算法的宿主（7 个 `ComputeDiff*` 方法）
4. 运行时状态的一部分（作为 `PublishedSegmentState::load_info` 被发布）

两个具体症状：

- `ComputeDiff(SegmentLoadInfo& new_info)`（`SegmentLoadInfo.h:1026`）的参数是**非 const 引用**——一个"计算差异"的操作会修改输入，说明它其实混了状态迁移。
- 拷贝构造函数需要手写 8 个成员并调 `BuildFieldBinlogCache()`（`SegmentLoadInfo.h:442`），因为缓存里存着指向自己 proto 的裸指针。这是"声明"与"派生缓存"没分开的直接代价。

## 4. 公开接口

### 4.1 `LoadSpec` —— 不可变声明

```cpp
// segcore/load/LoadSpec.h
namespace milvus::segcore::load {

struct FieldSource {
    FieldId                  field_id;
    std::vector<std::string> binlog_paths;
    std::optional<int>       column_group_index;
    bool                     lazy_load       = false;
    bool                     fill_default    = false;
    std::string              warmup_policy;
};

struct IndexSource {
    FieldId     field_id;
    int64_t     index_id = 0;
    IndexKind   kind     = IndexKind::Scalar;
    std::string nested_path;          // JSON / ngram-on-json
    bool        has_raw_data = false;
    std::vector<std::string> index_files;
};

// 不可变。构造后所有查询都是 O(1) 或 O(log n)，无内部可变缓存。
class LoadSpec {
 public:
    LoadSpec(std::vector<FieldSource>, std::vector<IndexSource>,
             ManifestSource, SchemaPtr, LoadSpecFlags);

    const FieldSource* Field(FieldId) const;
    const IndexSource* Index(FieldId) const;
    const IndexSource* NamedIndex(FieldId, IndexKind, std::string_view path) const;

    std::span<const FieldSource> Fields() const;
    std::span<const IndexSource> Indexes() const;

    const ManifestSource& Manifest() const;
    SchemaPtr             Schema() const;
    bool                  IsSortedByPk() const;
    int64_t               RowCount() const;
};

}  // namespace milvus::segcore::load
```

> **没有 mutable 缓存**是这里的关键约束。所有派生索引在构造时一次算好，因此拷贝是平凡的，也不再需要手写拷贝构造。

### 4.2 `RuntimeInventory` —— 不可变现状

```cpp
// segcore/load/RuntimeInventory.h
namespace milvus::segcore::load {

// 三个子 inventory 各自归属对应模块，这里只做组合。
struct RuntimeInventory {
    std::shared_ptr<const columnar::ColumnInventory> columns;
    std::shared_ptr<const indexing::IndexInventory>  indexes;
    std::shared_ptr<const indexing::IndexReadiness>  readiness;
    std::shared_ptr<const text::TextLobInventory>    text;
    std::shared_ptr<const mvcc::PkIndex>             pk_index;
    std::shared_ptr<const mvcc::TimestampVisibility> timestamps;
    std::shared_ptr<milvus_storage::api::Reader>     reader;
    int64_t row_count = 0;
};

}  // namespace milvus::segcore::load
```

> 对比当前 `RuntimeResourceState` 的 22 个平铺字段：拆开之后每个子 inventory 都能被它所属模块单独构造和测试，而组合体只在 load 与 segment 两处出现。

### 4.3 `ReopenPlanner` —— 纯函数

```cpp
// segcore/load/ReopenPlanner.h
namespace milvus::segcore::load {

struct LoadPlan {
    std::vector<IndexSource>  indexes_to_load;
    std::vector<IndexSource>  indexes_to_replace;
    std::set<FieldId>         indexes_to_drop;
    std::unordered_map<FieldId, std::unordered_set<std::string>> json_indexes_to_drop;

    std::vector<ColumnGroupTask> column_groups_to_load;
    std::vector<ColumnGroupTask> column_groups_to_replace;
    std::vector<ColumnGroupTask> column_groups_to_lazyload;

    std::vector<BinlogTask>   binlogs_to_load;
    std::vector<BinlogTask>   binlogs_to_replace;
    std::unordered_set<FieldId> field_data_to_drop;
    std::vector<FieldId>      fields_to_reload;
    std::vector<FieldId>      fields_to_fill_default;

    std::unordered_map<FieldId, TextIndexTask> text_indexes_to_load;
    std::unordered_map<FieldId, JsonStatsTask> json_stats_to_load;

    bool Empty() const;
};

// 纯函数：无 IO、无锁、无全局状态、参数全 const。
// 这是本模块最重要的可测试性产出。
LoadPlan
PlanReopen(const RuntimeInventory& current,
           const LoadSpec& target,
           const Schema& schema);

// 首次加载是 reopen 的特例：current 为空 inventory。
LoadPlan
PlanInitialLoad(const LoadSpec& target, const Schema& schema);

}  // namespace milvus::segcore::load
```

> **把 `ComputeDiff` 变成纯函数是 P4 的核心价值。** 当前它是 `SegmentLoadInfo` 的成员、接受非 const 引用、依赖对象内部缓存；改成纯函数后，reopen 逻辑（schema evolution、索引换代、column group 迁移、lazy load 降级）可以用表驱动测试穷举，而不需要起一个 sealed segment。

### 4.4 `LoadExecutor`

```cpp
// segcore/load/LoadExecutor.h
namespace milvus::segcore::load {

struct LoadResult {
    RuntimeInventory inventory;      // 新的不可变现状
    ResourceEstimate consumed;
};

class LoadExecutor {
 public:
    LoadExecutor(int64_t segment_id, const SegcoreConfig&,
                 std::shared_ptr<storage::ReadPlanner>);

    // 执行 plan，产出新 inventory。不发布 —— 发布是 segment 的职责。
    LoadResult Execute(OpContext*, tracer::TraceContext&,
                       const RuntimeInventory& base,
                       const LoadPlan&, const LoadSpec&, SchemaPtr);

    // 只估算不执行
    static ResourceEstimate Estimate(const LoadPlan&, const LoadSpec&, const Schema&);
};

}  // namespace milvus::segcore::load
```

### 4.5 `LoadInfoProtoAdapter`

```cpp
// segcore/load/LoadInfoProtoAdapter.h
namespace milvus::segcore::load {

// segcore 里唯一认识 proto::segcore::SegmentLoadInfo 的地方。
LoadSpec FromProto(const proto::segcore::SegmentLoadInfo&, SchemaPtr);
LoadSpec FromLegacyLoadFieldDataInfo(const LoadFieldDataInfo&, SchemaPtr);
LoadSpec FromLegacyLoadIndexInfo(const LoadIndexInfo&, SchemaPtr);

}  // namespace milvus::segcore::load
```

> lint 规则：`grep -rn "proto::segcore::SegmentLoadInfo" internal/core/src/segcore/` 的结果必须全部落在 `load/LoadInfoProtoAdapter.*` 与 `capi/` 内。当前它出现在 `SegmentInterface.h`（`load_info_` 成员 + `SetLoadInfo` + 两个 `Reopen` 重载）——一个纯 proto 类型出现在最底层的读接口上，是分层错误的典型症状。

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | `LoadSpec` 被 `ISegmentLifecycle` 前置声明引用 |
| `segcore_columnar` / `segcore_indexing` / `segcore_text` / `segcore_mvcc` | 构造各自的 inventory |
| `storage/` | chunk manager、read planner、mmap manager |
| `milvus-storage` | manifest、column group、packed reader |
| `index/` | 索引反序列化 |
| `pb/segcore.pb.h`、`pb/index_cgo_msg.pb.h` | **仅 `LoadInfoProtoAdapter.cpp`** |

**禁止依赖：** `segment`、`app`、`capi`、`query/`、`exec/`、`reduce`。

## 6. `memory_planner` 迁出

当前 `storage/Util.cpp:68` include `segcore/memory_planner.h`，并在 `:1504` 使用 `segcore::ParallelDegreeSplitStrategy`——这是一条 `storage → segcore` 的反向边。

`memory_planner` 的内容（`RowGroupSplitStrategy`、`MemoryBasedSplitStrategy`、`ParallelDegreeSplitStrategy`、`CellSpec`、`BatchReaderFactory`）本质是**存储层的读取规划**，与 segment 概念无关。方案：整体迁到 `storage/read_planning/`，segcore_load 作为消费者。

迁移后：`storage/` 对 `segcore/` 的 include 数为 0。

## 7. 测试

`test_segcore_load`，链接 `segcore_load` + 四个 L1/L2 模块 + `milvus_storage` + `milvus-storage`，**不链接 `milvus_core`**。

现有可迁移：`SegmentLoadInfoTest.cpp`（3,955 行，是 segcore 目前最大的单测文件——它的存在本身说明 load 逻辑已经复杂到需要独立测试，只是还没有独立 target）、`MemoryPlannerTest.cpp`（随 planner 迁到 storage）、`GroupChunkTranslatorTest.cpp`、`ManifestGroupTranslatorTest.cpp`、`DefaultValueChunkTranslatorTest.cpp`。

新增覆盖（`PlanReopen` 是纯函数，可表驱动穷举）：

- schema evolution：加字段 / 删字段 / 字段改类型 / 字段从有索引变无索引
- 索引换代：同字段新旧 index id、JSON 多路径下只换其中一条
- column group 迁移：字段在 group 间移动、group 内容变化
- lazy load 与 eager load 的相互降级
- binlog ↔ manifest 模式切换
- 空 plan（幂等 reopen）必须 `Empty() == true`

最后一条是当前**没有任何测试覆盖**的重要不变量：对同一个 load info 重复 reopen 不应产生任何加载动作。

## 8. 迁移步骤（P4）

1. `memory_planner` 迁到 `storage/read_planning/`，修 `storage/Util.cpp` 的 include。**这一步独立可合入**，先消掉一条反向边。
2. 建 `load/` 与 target；迁 translator（无逻辑改动）。
3. `LoadSpec` + `LoadInfoProtoAdapter` 落地，`SegmentLoadInfo` 保留为 adapter 的薄壳。
4. `ComputeDiff*` → `PlanReopen` 纯函数；`SegmentLoadInfoTest.cpp` 改造成纯函数测试（去掉 segment 依赖）。
5. `ApplyLoadDiff` 系列从 `ChunkedSegmentSealedImpl` 迁入 `LoadExecutor`。**这是 P4 最大的一步**，建议按 plan 的字段分批：索引 → column group → binlog → 默认值 → text/json。
6. `RuntimeResourceState` 拆成四个子 inventory + `RuntimeInventory` 组合体。
7. 删除 `SegmentLoadInfo` 与 `SegmentInterface::load_info_` / `SetLoadInfo`。

出口标准：`PlanReopen` 被表驱动测试覆盖；`storage/` 对 `segcore/` 的 include 为 0；`proto::segcore::SegmentLoadInfo` 只出现在 adapter 与 capi。
