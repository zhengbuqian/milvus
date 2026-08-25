# L0 `segcore_contracts` —— 能力契约

> 设计提案。返回 [总览](README.md)。

## 1. 职责

定义 segcore 对外发布的**全部**接口。这是唯一允许被 `query/`、`exec/` include 的 segcore 目录。

- 能力接口（capability interface）：把当前 `SegmentInterface` 的 89 个 virtual 按调用方向切成若干窄接口。
- 描述符（descriptor）：让消费者**在不接触实现的前提下**做执行策略决策的纯数据结构。
- 值类型：`ChunkSpan`、`RowRange`、`Pinned<T>` 等跨模块传递的轻量视图。

**本模块没有 `.cpp`**，是 header-only 的 `INTERFACE` target。

## 2. 边界

**不属于本模块：**

- 任何实现。包括默认实现、超过三行的 inline 便利函数、工厂函数。
- 任何 proto 类型。`proto::segcore::SegmentLoadInfo` 不得出现在契约里——转换发生在 [L3 load](07-load.md) 的 `ProtoAdapter`。
- `query::Plan` / `query::PlaceholderGroup` / `SearchInfo`。契约层不认识查询计划；`Search`/`Retrieve` 属于 [L5 app](09-application.md)。
- 具体 index 类型（`index::NgramInvertedIndex`、`index::TextMatchIndex`、`index::JsonKeyStats`）。契约只暴露 `index::IndexBase` 与能力描述符。
- `cachinglayer::CacheSlot` / `PinWrapper`。pin 是实现细节，契约层用 RAII 句柄表达（见 §3.3）。

**允许 include：** `common/`（`Types.h`、`Schema.h`、`FieldMeta.h`、`BitsetView.h`、`Span.h`、`OpContext.h`）、`bitset/`、STL。**上限 15 个 include，每个接口上限 15 个 virtual。**

## 3. 公开接口

### 3.1 身份与所有权：`ISegment`

`ISegment` 只回答"你是谁"，不提供任何数据访问。能力通过**投影**取得。

```cpp
// segcore/contracts/ISegment.h
namespace milvus::segcore {

class IColumnSource;
class IIndexProvider;
class IMvccView;
class ISegmentLifecycle;
class IGrowingSink;

class ISegment {
 public:
    virtual ~ISegment() = default;

    virtual int64_t     id() const = 0;
    virtual SegmentType type() const = 0;
    virtual SchemaPtr   schema() const = 0;

    virtual int64_t row_count() const = 0;
    virtual int64_t deleted_count() const = 0;
    virtual int64_t real_count() const = 0;
    virtual size_t  memory_usage() const = 0;

    // 能力投影。返回的引用/指针生命周期不超过 *this。
    // 指针形式返回 nullptr 表示该 segment 类型不提供此能力。
    virtual const IColumnSource&  columns() const = 0;
    virtual const IIndexProvider& indexes() const = 0;
    virtual const IMvccView&      mvcc() const = 0;
    virtual ISegmentLifecycle*    lifecycle() = 0;      // growing 返回 nullptr
    virtual IGrowingSink*         growing_sink() = 0;   // sealed  返回 nullptr
};

using SegmentPtr = std::shared_ptr<ISegment>;

}  // namespace milvus::segcore
```

> **设计说明。** 这里刻意不做成"`ISegment` 继承所有能力接口"。继承会让消费者重新拿到全量 surface，编译器无法约束；投影强制调用方在签名上写出它需要哪个能力，依赖关系因此变成可 grep 的事实。

### 3.2 列数据访问：`IColumnSource` / `IColumnChunkCursor`

这是 exec 的**原始数据通道**，取代当前 `SegmentInternalInterface` 的 `chunk_data_impl` / `chunk_view` / `bulk_subscript` / `num_chunk` 一族。

```cpp
// segcore/contracts/IColumnSource.h
namespace milvus::segcore {

// 一个 chunk 的只读视图。POD，可平凡拷贝，不持有资源。
struct ChunkSpan {
    const void* data        = nullptr;  // 定长类型：连续数组；变长类型用 view()
    const bool* validity    = nullptr;  // nullptr 表示该 chunk 全部 valid
    int64_t     base_offset = 0;        // 本 chunk 第 0 行的 segment 行号
    int64_t     row_count   = 0;

    template <typename T> Span<T>        as()   const;
    template <typename V> std::vector<V> view() const;  // string_view / ArrayView / ...
};

struct ColumnLayout {
    int64_t  chunk_count    = 0;
    int64_t  rows_per_chunk = 0;     // 0 表示不等长，需逐 chunk 查询
    bool     single_chunk   = false;
    bool     mmap           = false;
    bool     has_raw_data   = false;
    bool     nullable       = false;
    DataType data_type      = DataType::NONE;
};

struct RowRange {
    int64_t begin = 0;
    int64_t end   = kAllRows;   // 半开区间
};

// 游标持有 pin。Next() 之后上一个 ChunkSpan 失效。
// 调用方永远不接触 cachinglayer。
class IColumnChunkCursor {
 public:
    virtual ~IColumnChunkCursor() = default;

    virtual bool      Next() = 0;          // 推进到下一 chunk；false 表示耗尽
    virtual ChunkSpan Current() const = 0;
    virtual int64_t   ChunkId() const = 0;
    virtual void      SeekToRow(int64_t segment_offset) = 0;
};

// 按行号乱序取值的接收器（取代 bulk_subscript 的 void* 输出）。
class ColumnSink {
 public:
    virtual ~ColumnSink() = default;
    virtual void Accept(int64_t index, ChunkSpan span, int64_t offset_in_chunk) = 0;
    virtual void AcceptNull(int64_t index) = 0;
};

class IColumnSource {
 public:
    virtual ~IColumnSource() = default;

    virtual bool         Exists(FieldId) const = 0;
    virtual ColumnLayout Layout(FieldId) const = 0;

    // 顺序扫描通道
    virtual std::unique_ptr<IColumnChunkCursor>
    OpenCursor(OpContext*, FieldId, RowRange range = {}) const = 0;

    // 随机访问通道。offsets 为 segment 行号。
    virtual void
    Gather(OpContext*, FieldId, const int64_t* offsets, int64_t count,
           ColumnSink& sink) const = 0;

    // 行有效性（nullable 字段）。只清除无效位，不改动有效位。
    virtual void
    ApplyValidity(OpContext*, FieldId, RowRange, TargetBitmapView out) const = 0;

    virtual std::shared_ptr<const IArrayOffsets> ArrayOffsets(FieldId) const = 0;
    virtual void Prefetch(OpContext*, FieldId, RowRange) const = 0;
};

}  // namespace milvus::segcore
```

> **这是解决 segcore ↔ exec 耦合的核心。** 当前 `exec/expression/Expr.h` 自己维护 `ProcessDataChunksForSingleChunk`（`:1630`）与 `ForMultipleChunk`（`:1747`）两套遍历、自己管 pin 生命周期（`EnsurePinnedIndex()`，`:396-409`）、自己断言 chunk 布局（`num_index_chunk_ == 1`，`:672`/`:2075`）。这些都是 segment 存储布局的知识泄漏。游标把它们收进 segcore 侧，exec 只剩下"对一个 `ChunkSpan` 求值"。

### 3.3 索引访问：`IIndexProvider`

```cpp
// segcore/contracts/IIndexProvider.h
namespace milvus::segcore {

// RAII pin 句柄。析构即释放，禁止拷贝。
template <typename T>
class Pinned {
 public:
    Pinned() = default;
    Pinned(Pinned&&) noexcept;
    Pinned& operator=(Pinned&&) noexcept;
    ~Pinned();

    explicit operator bool() const noexcept { return ptr_ != nullptr; }
    const T* operator->() const noexcept { return ptr_; }
    const T& operator*()  const noexcept { return *ptr_; }

 private:
    friend class IndexPinFactory;    // 只有 L2 indexing 能构造
    const T*              ptr_ = nullptr;
    std::shared_ptr<void> keep_alive_;
};

// 消费者据此选择执行形态，且**不接触任何 index 实现**。
struct FieldIndexCapability {
    bool has_scalar_predicate = false;  // In/NotIn/Range/Null 可下推
    bool has_vector_index     = false;
    bool has_ngram            = false;
    bool has_text_match       = false;
    bool has_json_path_index  = false;
    bool has_skip_index       = false;
    bool index_has_raw_data   = false;  // 可从 index 反查原值，无需回原始列
    bool index_refine_enabled = false;
    int64_t index_chunk_count = 0;      // 当前为 0 或 1，保留给分段索引
};

class IScalarPredicateIndex;   // 见 04-indexing.md
class IScalarValueSource;      // 见 04-indexing.md
enum class IndexKind { Ngram, TextMatch, JsonPath, JsonStats };

class IIndexProvider {
 public:
    virtual ~IIndexProvider() = default;

    // 纯元数据查询，**不触发 tiered-storage 冷取**
    virtual FieldIndexCapability Capability(FieldId) const = 0;
    virtual FieldIndexCapability CapabilityForJsonPath(FieldId,
                                                       std::string_view path) const = 0;

    virtual Pinned<IScalarPredicateIndex> PinScalarPredicate(OpContext*, FieldId) const = 0;
    virtual Pinned<IScalarValueSource>    PinScalarValues(OpContext*, FieldId) const = 0;
    virtual Pinned<index::IndexBase>      PinVectorIndex(OpContext*, FieldId) const = 0;
    virtual Pinned<index::IndexBase>      PinNamedIndex(OpContext*, FieldId, IndexKind,
                                                        std::string_view path) const = 0;

    virtual std::shared_ptr<const SkipIndex> Skip() const = 0;
};

}  // namespace milvus::segcore
```

> **`Capability()` 与 `Pin*()` 的分离是有意的。** exec 的 `DetermineExecPath()` 只需要前者，不应触发冷取。当前 `GetJsonFlatIndexNestedPath()`（`SegmentInterface.h:239`）已经在用一段注释解释这件事——契约里应该把它变成类型上的区分，而不是靠注释维持。

### 3.4 可见性：`IMvccView`

```cpp
// segcore/contracts/IMvccView.h
namespace milvus::segcore {

struct PkLookupResult {
    std::vector<int64_t> offsets;
    bool                 has_more = false;
};

class IMvccView {
 public:
    virtual ~IMvccView() = default;

    virtual int64_t   ActiveCount(Timestamp ts) const = 0;
    virtual Timestamp MaxTimestamp() const = 0;

    // bitset 语义：1 = 不可见（被过滤掉）
    virtual void MaskDeleted(BitsetTypeView&, int64_t insert_barrier, Timestamp) const = 0;
    virtual void MaskExpired(BitsetTypeView&, Timestamp, Timestamp collection_ttl) const = 0;

    virtual bool Contains(const PkType&) const = 0;
    virtual void SeekByPk(BitsetType& out, const IdArray&) const = 0;

    virtual PkLookupResult FindFirstN(int64_t limit, const BitsetTypeView&) const = 0;

    // 仅 sorted-by-pk 的 sealed segment 支持；其余返回 false 且不修改 bitset。
    virtual bool PkRange(proto::plan::OpType, const PkType&, BitsetTypeView&) const = 0;
    virtual bool PkBinaryRange(const PkType& lo, bool lo_inc,
                               const PkType& hi, bool hi_inc,
                               BitsetTypeView&) const = 0;
};

}  // namespace milvus::segcore
```

> `PkRange` 从"growing 抛 `Unsupported`"（当前 `SegmentGrowing.h`）改为返回 `false`。用异常表达能力缺失会迫使调用方先判断 `type()`，那正是类型泄漏。

### 3.5 生命周期与写入

```cpp
// segcore/contracts/ISegmentLifecycle.h
namespace milvus::segcore {

struct LoadSpec;    // 见 07-load.md，此处只需前置声明

struct ResourceEstimate {
    int64_t memory_bytes = 0;
    int64_t disk_bytes   = 0;
};

class ISegmentLifecycle {
 public:
    virtual ~ISegmentLifecycle() = default;

    virtual void Load(const LoadSpec&, OpContext*) = 0;
    virtual void Reopen(const LoadSpec&, SchemaPtr new_schema, OpContext*) = 0;
    virtual void DropField(FieldId) = 0;
    virtual void Clear() = 0;

    virtual ResourceEstimate Estimate(const LoadSpec&) const = 0;
    virtual void SetCommitTimestamp(Timestamp) = 0;
};

class IGrowingSink {
 public:
    virtual ~IGrowingSink() = default;

    virtual int64_t PreInsert(int64_t size) = 0;
    virtual void    Insert(int64_t reserved_offset, int64_t size,
                           const int64_t* row_ids, const Timestamp*,
                           const InsertRecordProto*) = 0;
    virtual SegcoreError Delete(int64_t size, const IdArray* pks, const Timestamp*) = 0;
};

}  // namespace milvus::segcore
```

## 4. 依赖

| 方向 | 允许 |
|---|---|
| 本模块依赖 | `common/`、`bitset/`、STL。**仅此** |
| 依赖本模块 | 所有 segcore 模块；`query/`、`exec/`；`index/` 的游标适配 |

`index/` 依赖 contracts 是允许的（用 `IColumnChunkCursor` 替换 `exec::SegmentExpr*`，见 [11-cross-cutting.md](11-cross-cutting.md)），但 `index/` 不得依赖 contracts 以外的任何 segcore 头。

## 5. 测试

契约层没有实现，测试内容是**契约本身的可用性**：

- `test_segcore_contracts` 提供一组 fake 实现（`FakeColumnSource`、`FakeIndexProvider`、`FakeMvccView`），全部 in-memory、零依赖。
- 这些 fake **同时是其他模块和 exec 的测试夹具**，以 `segcore_contracts_testing` target 导出。这是"exec 单测不再需要真 segment"的前提，也是本重构最直接的收益。
- 编译期断言：`static_assert` 检查每个接口的 virtual 数量上限；lint 检查 include 集合。

## 6. 迁移步骤

1. 新建 `segcore/contracts/` 与 `segcore_contracts` INTERFACE target（P0，可以只有空头文件即合入）。
2. 逐个接口落地。每落地一个，就在 `SegmentInternalInterface` 上加一个转发适配器（能力投影返回包装 `*this` 的对象），保证旧路径不动。
3. 消费者按模块切换到窄接口：exec → `IColumnSource`/`IIndexProvider`（P2/P3），load path → `ISegmentLifecycle`（P4），C ABI → `ISegment`（P6）。
4. 全部消费者切换完成后删除 `SegmentInterface.h` 与 `SegmentInternalInterface`（P7）。

**转发适配器是过渡产物**，必须在 P7 删除。P7 起 lint 禁止 `contracts/` 之外的类同时继承两个以上能力接口——否则 God Interface 会以另一个名字复活。
