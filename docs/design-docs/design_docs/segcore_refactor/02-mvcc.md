# L1 `segcore_mvcc` —— 主键、时间戳与删除可见性

> 设计提案。返回 [总览](README.md)。

## 1. 职责

回答一个与 Segment 概念无关的问题：**给定一批行、一批删除记录、一个时间戳，哪些行可见？**

- 主键映射：`PkType → segment offset`，含 growing 的增量插入与 sealed 的有序/虚拟形态。
- 时间戳可见性：MVCC 上界查找、collection TTL、entity TTL、load-time commit ts 覆盖。
- 删除记录：delete bitmap 的累积、快照、dump 与查询。
- 插入屏障（insert barrier）与 ack 序列。

这是**整个重构里最容易独立、最容易测试**的模块，因此排在第一个迁移阶段（P1）。

## 2. 边界

**不属于本模块：**

- 原始列数据的存储与访问 —— 属于 [L1 columnar](03-columnar.md)。PK 列的**物理存储**在 columnar，本模块只持有 `offset → pk` 的索引结构。
- 从 binlog / manifest 加载 PK 与 timestamp —— 属于 [L3 load](07-load.md)，本模块只接受已解析好的数组。
- delete 消息的接收、去重、C ABI —— 属于 [L5 app](09-application.md)。
- 任何 `ISegment` 的知识。本模块**不得** include `contracts/ISegment.h`。

**判据检验：** "给定 pk 数组、ts 数组和一个查询 ts，算出 bitmap" 这句话不需要提到 Segment ⟹ 归本模块。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/InsertRecord.h:53` `CompressedInt64PkArray` | `mvcc/CompressedInt64PkArray.h` | 原样迁移 |
| `segcore/InsertRecord.h:245` `OffsetMap` 及 4 个实现 | `mvcc/PkIndex.h` + 4 个 `.cpp` | 接口重命名，见 §4.1 |
| `segcore/InsertRecord.h:1123/1448` 的 PK/ts 部分 | `mvcc/PkIndexSealed.cpp` / `PkIndexGrowing.cpp` | **拆分**：`InsertRecordSealed`/`Growing` 目前同时持有 PK、timestamp 与原始列容器，原始列部分归 columnar |
| `segcore/DeletedRecord.h` | `mvcc/DeletedRecord.h` | 去掉 `is_sealed` 模板参数，见 §4.3 |
| `segcore/TimestampIndex.{h,cpp}` | `mvcc/TimestampIndex.*` | 原样迁移 |
| `segcore/TimestampData.h` | `mvcc/TimestampData.h` | 原样迁移 |
| `segcore/AckResponder.h` | `mvcc/AckResponder.h` | 原样迁移 |
| `segcore/Utils.h:180` `upper_bound(ConcurrentVector<Timestamp>&, ...)` | `mvcc/TimestampVisibility.cpp` | 改为接受 `TimestampData` 而非 `ConcurrentVector` |

`InsertRecord.h` 当前 1,984 行，混装了 PK 编码、多种 OffsetMap、sealed/growing 容器、schema 解释、mmap 与 interim-index 策略。**本模块只取前三项**，后两项分别归 [columnar](03-columnar.md) 与 [indexing](04-indexing.md)。

## 4. 公开接口

### 4.1 `PkIndex`

由现有 `OffsetMap`（`InsertRecord.h:245`）演化而来。改动有三处：去掉 `insert`/`seal` 与查询混在一个接口（拆成 `PkIndexBuilder`）、`find_range` 的 `Condition` 参数内联进语义、显式区分 growing/sealed 能力。

```cpp
// segcore/mvcc/PkIndex.h
namespace milvus::segcore::mvcc {

using OffsetType = int64_t;

// 只读查询面。exec 与 reduce 只见这个。
class PkIndex {
 public:
    virtual ~PkIndex() = default;

    virtual bool                     Contains(const PkType&) const = 0;
    virtual std::vector<OffsetType>  Find(const PkType&) const = 0;
    virtual bool                     Empty() const = 0;
    virtual size_t                   MemorySize() const = 0;

    // 范围查询。不支持时返回 false 且不修改 bitset（取代当前的抛异常）。
    virtual bool FindRange(const PkType&, proto::plan::OpType,
                           BitsetTypeView&) const = 0;

    virtual std::pair<std::vector<OffsetType>, bool>
    FindFirstN(int64_t limit, const BitsetTypeView&) const = 0;

    virtual std::tuple<std::vector<int64_t>,
                       std::vector<std::vector<int32_t>>, bool>
    FindFirstNElement(int64_t limit, const BitsetTypeView& element_bitset,
                      const IArrayOffsets*,
                      const std::optional<QueryIteratorCursor>&) const = 0;

    // 实现不做逐行存储（如 VirtualPKOffsetMap 通过位抽取推导）。
    // 调用方据此决定是否走全列扫描回退路径。
    virtual bool IsZeroStorage() const { return false; }
};

// 写入面。仅 load path 与 growing insert 持有，查询侧拿不到。
class PkIndexBuilder {
 public:
    virtual ~PkIndexBuilder() = default;
    virtual void Insert(const PkType&, OffsetType) = 0;
    virtual void Seal() = 0;
    virtual void Clear() = 0;
    virtual std::unique_ptr<PkIndex> Build() && = 0;
};

// 工厂：由 schema 的 PK 类型 + 是否 sorted-by-pk + 是否 virtual pk 决定实现
std::unique_ptr<PkIndexBuilder>
MakePkIndexBuilder(DataType pk_type, PkIndexLayout layout);

}  // namespace milvus::segcore::mvcc
```

> **查询面与写入面分离**是本模块的核心改动。当前 `OffsetMap` 把 `insert`/`seal` 与 `find`/`find_first_n` 放在一起，任何拿到 segment 的代码都能往 PK 索引里插数据。分离后写入句柄只在 load 与 insert 路径存在。

### 4.2 `TimestampVisibility`

```cpp
// segcore/mvcc/TimestampVisibility.h
namespace milvus::segcore::mvcc {

// 不可变快照。构造后线程安全，可无锁并发读。
class TimestampVisibility {
 public:
    TimestampVisibility(std::shared_ptr<const TimestampData>,
                        std::shared_ptr<const TimestampIndex>,
                        std::optional<Timestamp> commit_ts_override);

    // 满足 ts(row) <= query_ts 的行数（行按 ts 单调时可二分）
    int64_t ActiveCount(Timestamp query_ts) const;
    Timestamp MaxTimestamp() const;

    // bitset 语义：1 = 不可见
    void MaskAfter(BitsetTypeView&, Timestamp query_ts) const;
    void MaskExpired(BitsetTypeView&, Timestamp query_ts,
                     Timestamp collection_ttl,
                     int64_t entity_ttl_physical_time_us) const;

    Timestamp RowTimestamp(int64_t offset) const;
};

}  // namespace milvus::segcore::mvcc
```

> `commit_ts_override` 对应当前 `ChunkedSegmentSealedImpl::EffectiveCommitTs()`。现在它是 impl 的私有方法、被多个消费点分别调用（注释里写着"All timestamp consumers must route through this"）——把它作为构造参数收进快照，是把注释变成类型约束。

### 4.3 `DeletedRecord`

```cpp
// segcore/mvcc/DeletedRecord.h
namespace milvus::segcore::mvcc {

struct DeleteSnapshot {
    Timestamp  max_ts = 0;
    BitsetType mask;
};

// 与 sealed/growing 无关：当前的 template <bool is_sealed> 参数消失，
// 因为差异只在"pk → offset 怎么查"，由注入的 PkIndex 决定。
class DeletedRecord {
 public:
    DeletedRecord(const PkIndex& pk_index,
                  const TimestampVisibility& ts_view,
                  int64_t segment_id);

    void Push(const std::vector<PkType>& pks, const Timestamp* timestamps);
    void LoadPush(const std::vector<PkType>& pks, const Timestamp* timestamps);

    // bitset 语义：1 = 已删除
    void Query(BitsetTypeView& bitset, int64_t insert_barrier, Timestamp) const;

    std::shared_ptr<const DeleteSnapshot> LatestSnapshot() const;

    int64_t DeletedCount() const;
    int64_t MemorySize() const;
};

}  // namespace milvus::segcore::mvcc
```

> **`template <bool is_sealed>` 消失**是一个具体的简化信号。当前 `DeletedRecord<is_sealed>`（`DeletedRecord.h:68`）持有 `std::conditional_t<is_sealed, InsertRecordSealed, InsertRecordGrowing>*` 反向指针加一个 `std::function` 回调（`:422-427`），用来把 pk 翻成 offset。注入 `const PkIndex&` 之后，两个特化合并成一个类，且不再需要回调间接层。

### 4.4 组合视图

`IMvccView`（[contracts §3.4](01-contracts.md#34-可见性imvccview)）的实现就在本模块，是上述三者的组合：

```cpp
// segcore/mvcc/MvccView.h
class MvccView final : public IMvccView {
 public:
    MvccView(std::shared_ptr<const PkIndex>,
             std::shared_ptr<const TimestampVisibility>,
             const DeletedRecord&);
    // ... IMvccView 的全部实现，均为转发 + 组合，无独立状态
};
```

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | 实现 `IMvccView` |
| `common/` | `Types.h`、`FieldMeta.h`、`Schema.h` |
| `bitset/` | `BitsetType` / `TargetBitmap` |
| `pb/plan.pb.h` | 仅 `proto::plan::OpType` 枚举 |

**禁止依赖：** `columnar`、`indexing`、`load`、`segment`、`storage/`、`index/`、`query/`、`exec/`、`cachinglayer/`。

## 6. 测试

`test_segcore_mvcc`，链接 `segcore_mvcc` + `milvus_common` + `gtest`，**不链接 `milvus_core`**。

现有可直接迁移的测试：`InsertRecordOffsetOrderedArrayTest.cpp`、`InsertRecordOffsetOrderedMapTest.cpp`、`DeletedRecordTest.cpp`、`TimestampIndexTest.cpp`、`TimestampDataTest.cpp`、`CompressedInt64PkArray_test.cpp`。

新增覆盖（当前不存在，因为需要真 segment 才能触发）：

- delete 与 insert 交错、乱序 ts 的 bitmap 正确性
- `commit_ts_override` 生效时的 TTL 计算
- `VirtualPKOffsetMap` 的 `IsZeroStorage()` 回退路径
- `FindFirstN` 在 bitset 全 0 / 全 1 / 稀疏时的边界

**这些测试当前无法写**——不是因为难，而是因为要构造 `InsertRecordSealed` 就得先构造一个 sealed segment。这正是"边界缺失"的代价的直接证据。

## 7. 迁移步骤（P1）

1. 建 `mvcc/` 目录与 `segcore_mvcc` OBJECT target（显式 source list）。
2. 迁移无依赖的叶子：`TimestampData`、`TimestampIndex`、`AckResponder`、`CompressedInt64PkArray`。
3. `OffsetMap` → `PkIndex` + `PkIndexBuilder`，四个实现逐个迁移；`InsertRecord.h` 中保留 `using` 别名做过渡。
4. `DeletedRecord` 去模板化，构造函数改注入 `PkIndex`。**这一步会触及 sealed 与 growing 两个 impl 的构造顺序**，是本阶段风险最高的改动，需单独 commit。
5. 落地 `MvccView`，`SegmentInternalInterface` 的相关 virtual 改为转发到它。
6. 建 `test_segcore_mvcc`，补齐 §6 的新增覆盖。
7. 从 `InsertRecord.h` 删除已迁走的部分。

出口标准：`test_segcore_mvcc` 的 link 行不含 `milvus_core`，且新增覆盖全部通过。
