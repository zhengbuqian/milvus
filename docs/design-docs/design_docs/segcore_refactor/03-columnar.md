# L1 `segcore_columnar` —— 列与 chunk 数据访问

> 设计提案。返回 [总览](README.md)。

## 1. 职责

提供 segment 的**原始数据执行通道**，并把 chunk 布局、mmap、cachinglayer pin 三件事全部封在模块内部。

- 顺序扫描：`IColumnChunkCursor` 的实现，覆盖 sealed 的 chunked 列与 growing 的 `ConcurrentVector`。
- 随机访问：按 segment 行号 gather（取代 `bulk_subscript`）。
- 列布局描述：`ColumnLayout` 的计算（chunk 数、每 chunk 行数、是否单 chunk、是否 mmap）。
- 行有效性（nullable）位图的应用。
- 列级 skip 统计的接入（`SkipIndex` 的持有与查询；实现类型仍在 `index/`）。

## 2. 边界

**不属于本模块：**

- 从哪里把数据读进来。binlog 解析、manifest、column group、translator 属于 [L3 load](07-load.md)。本模块接受**已构造好**的 `ChunkedColumnInterface` / `ConcurrentVector`。
- 索引访问。任何 `index::IndexBase` 的 pin 与查询属于 [L2 indexing](04-indexing.md)。
- PK 与时间戳的**语义**。PK 列与 ts 列的物理存储在本模块，但"哪些行可见"由 [L1 mvcc](02-mvcc.md) 回答。
- 结果物化成 `DataArray` / proto。属于 [L2 reduce](06-reduce.md)。

**关键边界规则：`PinWrapper` / `CacheSlot` 不得越出本模块。** 出口一律是 `Pinned<T>`（contracts）或游标持有的 `ChunkSpan`。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/ConcurrentVector.{h,cpp}` | `columnar/ConcurrentVector.*` | 原样迁移，growing 的列容器 |
| `segcore/SegmentChunkReader.{h,cpp}` | **删除**，能力并入 `ChunkCursor` | 见 §4.2 |
| `segcore/InsertRecord.h` 中 growing 的列容器部分 | `columnar/GrowingColumnSet.h` | 从 `InsertRecordGrowing` 中剥离 |
| `ChunkedSegmentSealedImpl` 的 `chunk_data_impl` / `chunk_*_view_impl` / `chunk_*_views_by_offsets` | `columnar/SealedColumnSource.cpp` | 当前是 sealed impl 的 protected 虚函数 |
| `ChunkedSegmentSealedImpl` 的 `bulk_subscript` 系列（4 个公开重载 `:565,571,662,669` + 8 个私有 `bulk_subscript_*_impl` `:1203-1262`） | `columnar/Gather.cpp` | 输出格式由调用方注入，见 §4.3 |
| `SegmentInternalInterface::skip_index_` + `LoadSkipIndex*` | `columnar/ColumnSkipStats.h` | 持有权迁入；`index::SkipIndex` 类型不动 |
| `RuntimeResourceState::fields` / `mmap_field_ids` / `variable_fields_avg_size` | `columnar/ColumnInventory.h` | 见 §4.4 |

外部依赖 `mmap/ChunkedColumnInterface.h` 保持不动——它已经是一个合理的窄接口，本模块是它的消费者而非拥有者。

## 4. 公开接口

### 4.1 `IColumnSource` 的两个实现

```cpp
// segcore/columnar/SealedColumnSource.h
namespace milvus::segcore::columnar {

class SealedColumnSource final : public IColumnSource {
 public:
    explicit SealedColumnSource(std::shared_ptr<const ColumnInventory>);

    bool         Exists(FieldId) const override;
    ColumnLayout Layout(FieldId) const override;

    std::unique_ptr<IColumnChunkCursor>
    OpenCursor(OpContext*, FieldId, RowRange) const override;

    void Gather(OpContext*, FieldId, const int64_t* offsets, int64_t count,
                ColumnSink&) const override;

    void ApplyValidity(OpContext*, FieldId, RowRange, TargetBitmapView) const override;

    std::shared_ptr<const IArrayOffsets> ArrayOffsets(FieldId) const override;
    void Prefetch(OpContext*, FieldId, RowRange) const override;
};

// segcore/columnar/GrowingColumnSource.h
class GrowingColumnSource final : public IColumnSource {
 public:
    explicit GrowingColumnSource(const GrowingColumnSet&, const SegcoreConfig&);
    // 同上；OpenCursor 返回按 size_per_chunk 切分的 ConcurrentVector 游标
};

}  // namespace milvus::segcore::columnar
```

### 4.2 游标：`ChunkCursor`

这是本模块最重要的产出，也是 [问题 2 的实现耦合](README.md#12-已确认的反向依赖边) 的解药。

```cpp
// segcore/columnar/ChunkCursor.h
namespace milvus::segcore::columnar {

// sealed 侧：包住 ChunkedColumnInterface + PinWrapper 生命周期
class ChunkedColumnCursor final : public IColumnChunkCursor {
 public:
    ChunkedColumnCursor(OpContext*, std::shared_ptr<ChunkedColumnInterface>,
                        RowRange, DataType);

    bool      Next() override;        // 释放上一个 pin，pin 下一个 chunk
    ChunkSpan Current() const override;
    int64_t   ChunkId() const override;
    void      SeekToRow(int64_t segment_offset) override;

 private:
    // pin 只存在于这里。析构或 Next() 时释放。
    cachinglayer::PinWrapper<Chunk*> pinned_;
};

// growing 侧：ConcurrentVector 的分段视图，不涉及 pin
class ConcurrentVectorCursor final : public IColumnChunkCursor { /* ... */ };

}  // namespace milvus::segcore::columnar
```

调用方形态对比：

```cpp
// 现状（exec/expression/Expr.h:1747 ProcessDataChunksForMultipleChunk 的骨架）
//   exec 自己算 chunk 边界、自己判断 single/multiple chunk、自己 pin、
//   自己处理 batch_size_ 与 chunk 边界不对齐

// 重构后（exec 侧）
auto cursor = columns.OpenCursor(op_ctx, field_id, {row_begin, row_end});
while (cursor->Next()) {
    ChunkSpan c = cursor->Current();
    kernel(c.as<T>(), c.validity, c.base_offset, c.row_count, out);
}
```

### 4.3 `ColumnSink`

`bulk_subscript` 当前有 4 个公开重载 + 8 个 `*_impl` 私有实现，输出写进 `void*` 或 `DataArray`，因此 segcore 必须认识结果的物化格式。`ColumnSink` 把这个方向反过来：

```cpp
// segcore/columnar/Sinks.h
namespace milvus::segcore::columnar {

// 定长类型：直接写进调用方缓冲
template <typename T>
class TypedBufferSink final : public ColumnSink { /* ... */ };

// 变长类型：调用方给一个 append 回调
class CallbackSink final : public ColumnSink {
 public:
    using Appender = std::function<void(int64_t index, std::string_view value, bool valid)>;
    explicit CallbackSink(Appender);
};

}  // namespace milvus::segcore::columnar
```

`DataArray` 形态的 sink 定义在 [reduce](06-reduce.md)，本模块不认识 proto。

### 4.4 `ColumnInventory`

不可变快照，由 [load](07-load.md) 构造并发布，columnar 只读。对应当前 `RuntimeResourceState` 中列相关的字段。

```cpp
// segcore/columnar/ColumnInventory.h
namespace milvus::segcore::columnar {

struct ColumnEntry {
    std::shared_ptr<ChunkedColumnInterface> column;
    bool     mmap        = false;
    bool     nullable    = false;
    DataType data_type   = DataType::NONE;
    std::pair<int64_t, int64_t> avg_size{0, 0};   // {num_rows, avg_bytes}
};

class ColumnInventory {
 public:
    const ColumnEntry* Find(FieldId) const;
    int64_t RowCount() const;
    std::shared_ptr<const ColumnSkipStats> Skip() const;
    std::shared_ptr<const IArrayOffsets>   ArrayOffsets(FieldId) const;

    // 只有 load 能构造下一个版本
    class Builder;
};

}  // namespace milvus::segcore::columnar
```

## 5. 性能约束

重构必须性能中性。三条硬约束：

1. **虚调用只发生在 chunk 粒度**。`Next()` / `Current()` 每 chunk 各一次；chunk 内的逐行计算由调用方的模板 kernel 完成，不经过任何虚函数。当前 `ProcessDataChunks` 已经是这个形状，本次是把遍历代码搬家，不是新增抽象层。
2. **`ChunkSpan` 必须是 POD 且可平凡拷贝**，`Current()` 返回值而非引用，允许编译器完全内联。
3. **单 chunk 快路径保留**。`ColumnLayout::single_chunk == true` 时，调用方可以直接 `OpenCursor` + 一次 `Next()`，与当前 `ProcessDataChunksForSingleChunk`（`Expr.h:1630`）等价。

验收：`all_tests` 中的 search/retrieve 基准与重构前差异 ≤ 3%。**这一条必须在 P2 合入前跑，不能推迟到 P7。**

## 6. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | 实现 `IColumnSource` / `IColumnChunkCursor` |
| `common/` | `Types.h`、`Span.h`、`Array.h`、`Json.h`、`FieldMeta.h` |
| `mmap/` | `ChunkedColumnInterface`、`Chunk` |
| `cachinglayer/` | `PinWrapper`、`CacheSlot`。**仅本模块内部可见** |
| `bitset/` | validity 位图 |
| `index/SkipIndex.h` | 仅类型，不含其他 index 头 |

**禁止依赖：** `mvcc`、`indexing`、`load`、`segment`、`query/`、`exec/`、`storage/`、任何 proto。

## 7. 测试

`test_segcore_columnar`，链接 `segcore_columnar` + `milvus_common` + `mmap` + `cachinglayer`，**不链接 `milvus_core`**。

现有可迁移：`ConcurrentVectorTest.cpp`、`SegmentChunkReaderTest.cpp`。

新增覆盖：

- 游标在 chunk 边界、`RowRange` 跨 chunk、`SeekToRow` 落在 chunk 中部时的正确性
- `Next()` 之后上一个 `ChunkSpan` 失效（用 ASAN 断言，防止调用方持有悬垂视图）
- 游标析构时 pin 全部释放（构造一个计数型 fake `ChunkedColumnInterface` 断言 pin/unpin 配对）
- 变长类型（string / array / vector array / json）的 `view()` 路径
- nullable 字段全 null / 全非 null / 混合时的 `ApplyValidity`
- `Gather` 的乱序 offsets、重复 offsets、越界 offsets

第三条尤其重要：**pin 泄漏当前没有任何单测能覆盖**，因为 pin 生命周期分散在 exec 与 sealed impl 两处。

## 8. 迁移步骤（P2）

1. 建 `columnar/` 与 `segcore_columnar` target；迁 `ConcurrentVector`。
2. 落地 `ChunkedColumnCursor` + `ConcurrentVectorCursor`，与现有 `chunk_data_impl` 并存。
3. `exec/expression/Expr.h` 改用游标：先替换 `ProcessDataChunksForSingleChunk`，跑基准；再替换 `ForMultipleChunk`；最后删掉 `ProcessDataChunks` 的分派。
4. 把 `EnsurePinnedIndex()`（`Expr.h:396-409`）的 pin 逻辑移出 exec —— 索引侧的 pin 归 [indexing](04-indexing.md)，本步只处理原始列的 pin。
5. `bulk_subscript` 系列 → `Gather` + `ColumnSink`，逐个重载迁移。
6. 删除 `SegmentChunkReader`。
7. `ColumnInventory` 从 `RuntimeResourceState` 中析出（与 P4 load 阶段协同）。

出口标准：`exec/` 下 `PinWrapper` 出现次数为 0；`num_chunk`/`chunk_size`/`get_chunk_by_offset` 不再出现在 exec；基准无回归。
