# L2 `segcore_indexing` —— 索引访问与 growing interim index

> 设计提案。返回 [总览](README.md)。
> **本模块是重构的 pilot（P3）**，理由见 [§8](#8-为什么-scalar-index-是第一刀)。

## 1. 职责

提供 segment 的**索引执行通道**，并把"哪个字段有什么索引、能不能下推"这件事收敛成一个描述符。

- sealed 索引清单的持有、pin 与发布（scalar / vector / ngram / text / json）。
- growing interim index 的创建、append、与原始数据的同步。
- 索引能力计算：把"有没有索引、索引能不能反查原值、能不能 pattern match"折算成 `FieldIndexCapability`。
- 索引构建参数生成（`IndexConfigGenerator`）。

## 2. 边界

**不属于本模块：**

- 索引算法本身。所有 `index::*` 实现留在 `internal/core/src/index/`，本模块只是它们的**清单与 pin 管理者**。
- 从远端加载索引文件。属于 [L3 load](07-load.md)（`LoadIndexData` 等）。
- 原始列访问与回退。属于 [L1 columnar](03-columnar.md)。**"没有索引就回原始列"这个决策不在本模块**，在调用方，因为它需要同时看两个通道。
- 查询表达式的求值策略。属于 `exec/`。本模块只回答"能力是什么"，不回答"该怎么执行"。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/SealedIndexingRecord.h` | `indexing/SealedIndexInventory.h` | vector index 清单 |
| `RuntimeResourceState` 的 `scalar_indexings` / `vector_indexings` / `ngram_indexings` / `json_indices` / `vec_binlog_config` / `ngram_fields` | `indexing/IndexInventory.h` | 见 §4.4 |
| `PublishedSegmentState` 的 `index_ready_bitset` / `binlog_index_bitset` / `index_has_raw_data` 等 6 个字段 | `indexing/IndexReadiness.h` | 当前散在 sealed impl 的发布状态里 |
| `segcore/FieldIndexing.{h,cpp}` | `indexing/growing/*` | growing interim index，含 `CreateIndex` 工厂（`FieldIndexing.cpp:820`） |
| `segcore/IndexConfigGenerator.{h,cpp}` | `indexing/IndexConfigGenerator.*` | 原样迁移 |
| `SegmentInternalInterface` 的 `PinIndex` / `PinJsonIndex` / `GetNgramIndex*` / `GetTextIndex` / `GetJsonStats` / `HasIndex` / `HasJsonIndex` / `CalcDistByIDs` / `IsIndexRefineEnabled` | `indexing/IndexProvider.cpp` | 9 组 virtual，全部收进本模块 |
| `exec/expression/Expr.h:396-409` `EnsurePinnedIndex()` | `indexing/IndexProvider.cpp` | pin 逻辑从 exec 迁回 |

## 4. 公开接口

### 4.1 scalar index 的三条窄合同

这是 pilot 的核心产出。当前 `index::ScalarIndex<T>`（`index/ScalarIndex.h:102`）把查询、构建、序列化、上传下载、mmap、资源计费、planner capability 混在一个接口上，消费者只要用其中一个能力就得依赖全部。三条合同按**调用方**切分：

```cpp
// segcore/contracts/IScalarPredicateIndex.h   （契约在 L0，实现在本模块）
namespace milvus::segcore {

// 合同 1：谓词下推。exec 的表达式求值只见这个。
class IScalarPredicateIndex {
 public:
    virtual ~IScalarPredicateIndex() = default;

    virtual DataType ValueType() const = 0;
    virtual int64_t  Count() const = 0;

    // bitset 语义：1 = 命中
    virtual TargetBitmap In(const ScalarValueList& values) const = 0;
    virtual TargetBitmap NotIn(const ScalarValueList& values) const = 0;
    virtual TargetBitmap Range(const ScalarValue& v, proto::plan::OpType) const = 0;
    virtual TargetBitmap Range(const ScalarValue& lo, bool lo_inc,
                               const ScalarValue& hi, bool hi_inc) const = 0;
    virtual TargetBitmap IsNull() const = 0;
    virtual TargetBitmap IsNotNull() const = 0;

    // 能力自述。不支持时调用方走原始列通道，而不是接异常。
    virtual bool SupportsPatternMatch() const { return false; }
    virtual TargetBitmap PatternMatch(std::string_view pattern,
                                      proto::plan::OpType) const = 0;

    // 带过滤的 In，避免物化完整 bitmap
    virtual bool SupportsFilteredIn() const { return false; }
    virtual TargetBitmap InWithFilter(const ScalarValueList&,
                                      const std::function<bool(int64_t)>& keep) const = 0;
};

// 合同 2：取值。隐藏"从 index 反查"与"回原始列"的差异。
class IScalarValueSource {
 public:
    virtual ~IScalarValueSource() = default;

    virtual DataType ValueType() const = 0;

    // 反查成本是否可接受（对应现 ScalarIndex::SupportFastReverseLookup()）。
    // false 时调用方应改走 IColumnSource。
    virtual bool CheapPerRowLookup() const = 0;

    virtual void Gather(const int64_t* offsets, int64_t count,
                        ColumnSink& sink) const = 0;
};

// 合同 3：growing 增量索引
class IGrowingScalarIndex {
 public:
    virtual ~IGrowingScalarIndex() = default;

    virtual void    Append(int64_t reserved_offset, int64_t size,
                           const FieldDataPtr&) = 0;
    virtual int64_t BuildThreshold() const = 0;
    virtual bool    InSyncWithRawData() const = 0;

    // 已建成部分的只读视图；未达阈值时返回空 Pinned
    virtual Pinned<IScalarPredicateIndex> Pin() const = 0;
};

}  // namespace milvus::segcore
```

**第一阶段范围**（严格控制，避免 pilot 变成大爆炸）：

| 纳入 | 排除 |
|---|---|
| bool / 整型 / 浮点 / VARCHAR 的谓词 | Geometry / RTree |
| `In` / `NotIn` / `Range` / `IsNull` / `IsNotNull` | JSON stats / JSON path index |
| pattern match（作为能力自述，可回退） | Ngram |
| reverse lookup（合同 2） | TextMatch |
| growing 的 append / sync / factory | 所有 vector index |
| | storage translator |

排除项在 P3 之后**保持现状不动**，通过 `IIndexProvider::PinNamedIndex()` 的通用出口访问。

### 4.2 `IndexProvider`

```cpp
// segcore/indexing/IndexProvider.h
namespace milvus::segcore::indexing {

class IndexProvider final : public IIndexProvider {
 public:
    IndexProvider(std::shared_ptr<const IndexInventory>,
                  std::shared_ptr<const IndexReadiness>);

    // 纯元数据，不触发冷取
    FieldIndexCapability Capability(FieldId) const override;
    FieldIndexCapability CapabilityForJsonPath(FieldId, std::string_view) const override;

    Pinned<IScalarPredicateIndex> PinScalarPredicate(OpContext*, FieldId) const override;
    Pinned<IScalarValueSource>    PinScalarValues(OpContext*, FieldId) const override;
    Pinned<index::IndexBase>      PinVectorIndex(OpContext*, FieldId) const override;
    Pinned<index::IndexBase>      PinNamedIndex(OpContext*, FieldId, IndexKind,
                                                std::string_view path) const override;

    std::shared_ptr<const SkipIndex> Skip() const override;
};

}  // namespace milvus::segcore::indexing
```

> **`Capability()` 不 pin，`Pin*()` 才 pin。** 当前 `SegmentInterface.h:239` 的 `GetJsonFlatIndexNestedPath()` 用一段注释解释"不要在这里触发 tiered-storage 冷取"——重构把这个约定变成两个方法的类型区分。

### 4.3 growing interim index

```cpp
// segcore/indexing/growing/GrowingIndexSet.h
namespace milvus::segcore::indexing {

// 取代当前 IndexingRecord（FieldIndexing.h:371）
class GrowingIndexSet {
 public:
    GrowingIndexSet(const Schema&, IndexMetaPtr, const SegcoreConfig&,
                    const GrowingIndexSourceResolver&);

    void Append(FieldId, int64_t reserved_offset, int64_t size,
                const FieldDataPtr&);

    bool Has(FieldId) const;
    Pinned<IScalarPredicateIndex> PinScalar(FieldId) const;
    Pinned<index::IndexBase>      PinVector(FieldId) const;

    bool InSyncWithRawData(FieldId) const;
};

// 工厂。取代 FieldIndexing.cpp:820 的 CreateIndex —— 那里把
// vector / scalar / geometry 三种完全不同的东西塞进同一个返回类型。
std::unique_ptr<IGrowingScalarIndex>
MakeGrowingScalarIndex(const FieldMeta&, const SegcoreConfig&);

std::unique_ptr<IGrowingVectorIndex>
MakeGrowingVectorIndex(const FieldMeta&, const FieldIndexMeta&,
                       int64_t max_rows, const SegcoreConfig&);

}  // namespace milvus::segcore::indexing
```

> `GrowingIndexSourceResolver` 取代当前 `FieldIndexing` 直接持有 `const VectorBase*`（`FieldIndexing.h:69-92`）。growing 索引需要读原始列来建索引，但不应该直接认识 `ConcurrentVector` 的类型——改为从 [columnar](03-columnar.md) 取一个游标。

### 4.4 `IndexInventory` / `IndexReadiness`

不可变快照，由 [load](07-load.md) 构造并发布。

```cpp
// segcore/indexing/IndexInventory.h
namespace milvus::segcore::indexing {

struct IndexEntry {
    index::CacheIndexBasePtr index;
    IndexKind                kind = IndexKind::Scalar;
    std::string              nested_path;      // JSON / ngram-on-json 用
    MetricType               metric_type;      // vector 用
    bool                     has_raw_data = false;
};

class IndexInventory {
 public:
    const IndexEntry* FindScalar(FieldId) const;
    const IndexEntry* FindVector(FieldId) const;
    const IndexEntry* FindNamed(FieldId, IndexKind, std::string_view path) const;
    std::string       ResolveJsonFlatPath(FieldId, std::string_view query_path) const;

    class Builder;   // 只有 load 能构造下一个版本
};

// 发布态：哪些索引"已就绪可查"。当前散在 PublishedSegmentState 的 6 个 bitset。
class IndexReadiness {
 public:
    bool Ready(FieldId) const;
    bool BinlogIndexReady(FieldId) const;
    bool HasRawData(FieldId) const;

    class Builder;
};

}  // namespace milvus::segcore::indexing
```

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | 实现 `IIndexProvider` 等 |
| `segcore_columnar` | growing 索引建索引时读原始列；`ColumnSink` |
| `index/` | 全部索引实现类型 |
| `cachinglayer/` | index pin。**不得越出本模块** |
| `common/`、`bitset/` | 基础类型 |

**禁止依赖：** `mvcc`、`load`、`text`、`segment`、`query/`、`exec/`、`storage/`。

**需要修复的反向边（本模块负责）：** `index/NgramInvertedIndex.h:68,93` 当前直接接受 `exec::SegmentExpr*`。修复方向是改为接受 `IColumnChunkCursor` 或一个窄回调，使 `index → exec → segcore` 这条环消失。详见 [11-cross-cutting.md](11-cross-cutting.md#2-反向边修复清单)。

## 6. 测试

`test_segcore_indexing`，链接 `segcore_indexing` + `segcore_columnar` + `milvus_index` + `milvus_common`，**不链接 `milvus_core`**。

现有可迁移：`SegmentGrowingIndexTest.cpp` 中不依赖 segment 的部分。

新增覆盖：

- `Capability()` 在各种 inventory 组合下的正确性（有 scalar 无 raw data、有 binlog index、json 多路径）
- `Capability()` **不触发 pin** —— 用计数型 fake `CacheSlot` 断言 pin 次数为 0。这是一个当前完全没被测到、但线上会造成冷取放大的行为。
- `IScalarPredicateIndex` 三种实现（inverted / bitmap / trie）在 `In`/`Range`/null 上的一致性（同一份数据，三种索引结果必须逐位相同）
- `CheapPerRowLookup() == false` 时调用方回退路径被触发
- growing：append 到阈值前后 `InSyncWithRawData()` 与 `Pin()` 的状态迁移

## 7. 迁移步骤（P3）

1. 建 `indexing/` 与 target；先迁 `IndexConfigGenerator`、`SealedIndexingRecord`（无争议的叶子）。
2. 落地 `FieldIndexCapability` + `IndexProvider::Capability()`，让 exec 的 `DetermineExecPath()` 改用它。**这一步不动执行路径**，只换决策输入。
3. 落地 `IScalarPredicateIndex` 与适配器（包住现有 `index::ScalarIndex<T>`），exec 的 scalar 谓词改用它，删掉 `Expr.h:678`/`:745` 的 `dynamic_cast<const Index*>`。
4. pin 逻辑从 `Expr.h:396-409` 迁入 `IndexProvider::PinScalarPredicate()`，删掉 `num_index_chunk_ == 1` 断言（`Expr.h:672`/`:2075`）。
5. 落地 `IScalarValueSource`，合并现 `segcore/Utils.h:151` `ReverseDataFromIndex` 与原始列回退。
6. growing：`IndexingRecord` → `GrowingIndexSet`，`CreateIndex` 工厂按返回类型拆成三个。
7. `IndexInventory` / `IndexReadiness` 从 `RuntimeResourceState` / `PublishedSegmentState` 析出（与 P4 协同）。

出口标准：`exec/` 下 `dynamic_cast` 到具体 index 类型的次数为 0；`exec/` 不 include 任何 `index/` 具体索引头；`test_segcore_indexing` 不链接 `milvus_core`。

## 8. 为什么 scalar index 是第一刀

- **调用方最集中**：scalar 谓词的消费者基本只有 `exec/expression/`，改造面可控。
- **语义最清晰**：`In`/`Range`/`IsNull` 是纯函数式的，能力边界容易画准。
- **验证力最强**：它同时暴露了两个 P0 问题——God Interface（`PinIndex` 在 `SegmentInterface` 上）与 segcore↔exec 双向依赖（exec 直接 `dynamic_cast` 具体索引）。切开它就同时验证了两条修复路径。
- **回退成本低**：适配器包住现有 `ScalarIndex<T>`，任何一步出问题都能单独 revert，不影响其他索引类型。

反过来，**不适合做第一刀的**：vector index（knowhere 边界复杂、性能敏感）、JSON stats（本身还有反向依赖要先修）、TextMatch（跨 tantivy FFI）。
