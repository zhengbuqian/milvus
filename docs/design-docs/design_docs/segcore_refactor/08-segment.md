# L4 `segcore_segment` —— Segment orchestrator

> 设计提案。返回 [总览](README.md)。

## 1. 职责

Segment 仍然是 segcore 的核心实体。本模块**保留这个实体**，但把它的实现降级为编排：

- 持有 segment 的身份（id、type、schema、commit ts）与所有权。
- 持有当前发布态（`RuntimeInventory` 的当前版本）并实现 copy-on-write 换代。
- 读写并发控制：读租约、发布屏障（`SegmentReadGate`）。
- 把 [contracts](01-contracts.md) 的能力投影接到 L1/L2 的实现上。
- 按 schema 把请求路由到某个 column source / index provider。
- growing 的 insert/delete 编排（PreInsert → 写列 → 写 PK → 追加索引 → ack）。

## 2. 边界

**本模块不实现任何算法。** 具体禁止清单：

| 禁止出现在本模块 | 应在哪 |
|---|---|
| chunk 遍历、pin 管理、列取值 | [columnar](03-columnar.md) |
| index 选择、index pin、能力计算 | [indexing](04-indexing.md) |
| delete bitmap 计算、pk 查找、ts 比较 | [mvcc](02-mvcc.md) |
| load diff 计算、binlog/manifest 解析、translator | [load](07-load.md) |
| LOB 读取、spillover | [text](05-text.md) |
| 结果物化、reduce | [reduce](06-reduce.md) |
| `query::Plan` / executor 构造 / Search / Retrieve | [app](09-application.md) |

**判据：** `segment/` 下任何一个函数，如果它的函数体里出现了循环遍历数据的代码，就说明它放错了地方。

### 2.1 规模约束

这是本模块唯一的量化约束，写进验收标准：

| 文件 | 当前 | 目标 |
|---|---|---|
| `SegmentSealedImpl.cpp` | 9,949 行（`ChunkedSegmentSealedImpl.cpp`） | ≤ 1,500 行 |
| `SegmentGrowingImpl.cpp` | 3,378 行 | ≤ 1,000 行 |
| `SegmentSealedImpl.h` | 2,729 行 | ≤ 400 行 |

行数不是目的，是**实现是否真的下放了**的代理指标。如果 P7 结束时 sealed impl 仍有 5,000 行，说明某个模块的边界只是名义上的。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/ChunkedSegmentSealedImpl.{h,cpp}` | `segment/SegmentSealedImpl.*` | **主体被拆走**，只留编排 |
| `segcore/SegmentGrowingImpl.{h,cpp}` | `segment/SegmentGrowingImpl.*` | 同上 |
| `segcore/SegmentReadLease.h` | `segment/ReadGate.h` | 原样迁移 |
| `PublishedSegmentState` + `StateDelta` + `CloneMutableRuntimeResourceState` / `PublishRuntimeStateLocked` / `CapturePublishedState` 一族 | `segment/PublishedState.h` | 泛化成模板，见 §4.2 |
| `segcore/SegmentInterface.{h,cpp}` | **删除** | 能力投影到 contracts |
| `segcore/SegmentSealed.h` / `SegmentGrowing.h` | **删除** | 同上 |
| `segcore/Collection.{h,cpp}` | `segment/Collection.*` | schema 与 index meta 持有者 |
| `segcore/SegcoreConfig.{h,cpp}` | `segment/SegcoreConfig.*` | |
| `segcore/InsertRecord.h` 剩余的 growing 编排部分 | `segment/GrowingInsertPath.cpp` | PK/列/索引三路写入的顺序编排 |

## 4. 公开接口

### 4.1 `SegmentSealedImpl` / `SegmentGrowingImpl`

```cpp
// segcore/segment/SegmentSealedImpl.h
namespace milvus::segcore::segment {

class SegmentSealedImpl final : public ISegment,
                                private ISegmentLifecycle {
 public:
    SegmentSealedImpl(SchemaPtr, IndexMetaPtr, const SegcoreConfig&,
                      int64_t segment_id, bool is_sorted_by_pk);

    // --- ISegment：身份 ---
    int64_t     id() const override;
    SegmentType type() const override { return SegmentType::Sealed; }
    SchemaPtr   schema() const override;
    int64_t     row_count() const override;
    int64_t     deleted_count() const override;
    int64_t     real_count() const override;
    size_t      memory_usage() const override;

    // --- ISegment：能力投影 ---
    const IColumnSource&  columns() const override;
    const IIndexProvider& indexes() const override;
    const IMvccView&      mvcc() const override;
    ISegmentLifecycle*    lifecycle() override { return this; }
    IGrowingSink*         growing_sink() override { return nullptr; }

 private:
    // --- ISegmentLifecycle：控制面，只有 app 通过 lifecycle() 拿得到 ---
    void Load(const LoadSpec&, OpContext*) override;
    void Reopen(const LoadSpec&, SchemaPtr, OpContext*) override;
    void DropField(FieldId) override;
    void Clear() override;
    ResourceEstimate Estimate(const LoadSpec&) const override;
    void SetCommitTimestamp(Timestamp) override;

    // 编排状态。注意：没有任何列、索引、bitmap 的直接持有。
    PublishedState<SegmentView> published_;
    mutable ReadGate            gate_;
    std::mutex                  reopen_mutex_;
    load::LoadExecutor          loader_;
    const SegcoreConfig&        config_;
    int64_t                     id_;
};

}  // namespace milvus::segcore::segment
```

> **`ISegmentLifecycle` 用私有继承**：只有通过 `lifecycle()` 才能拿到控制面。exec 拿到 `const ISegment&` 后无法调用 `Reopen()`——这是"依赖方向由编译器保证"的具体落点。

```cpp
// segcore/segment/SegmentGrowingImpl.h
class SegmentGrowingImpl final : public ISegment,
                                 private IGrowingSink {
 public:
    SegmentGrowingImpl(SchemaPtr, IndexMetaPtr, const SegcoreConfig&,
                       int64_t segment_id);

    // ISegment 同上
    ISegmentLifecycle* lifecycle() override { return nullptr; }
    IGrowingSink*      growing_sink() override { return this; }

 private:
    int64_t PreInsert(int64_t size) override;
    void    Insert(int64_t reserved_offset, int64_t size,
                   const int64_t* row_ids, const Timestamp*,
                   const InsertRecordProto*) override;
    SegcoreError Delete(int64_t, const IdArray*, const Timestamp*) override;
};
```

### 4.2 `PublishedState<T>` —— COW 换代

当前 sealed impl 里有一整套 `CapturePublishedState` / `CloneMutable*` / `Freeze*` / `PublishRuntimeStateLocked` / `StateDelta` / `StagedStateCommitter`（`ChunkedSegmentSealedImpl.h:340-400` 的三个 state struct，与 `:1333,1412,1427,1495` 的 commit/capture/clone 一族）。这套机制是通用的，应该泛化：

```cpp
// segcore/segment/PublishedState.h
namespace milvus::segcore::segment {

// 单写多读的 COW 状态槽。读者拿到 shared_ptr 快照，
// 写者构造新版本后原子换代。读者持有的快照在其生命周期内保持有效。
template <typename T>
class PublishedState {
 public:
    explicit PublishedState(std::shared_ptr<const T> initial);

    std::shared_ptr<const T> Capture() const noexcept;

    // 只有持有 PublishLease 才能换代
    void Publish(PublishLease&, std::shared_ptr<const T> next);

    uint64_t Generation() const noexcept;
};

// segment 的完整发布态
struct SegmentView {
    SchemaPtr                            schema;
    std::shared_ptr<const load::LoadSpec> spec;
    load::RuntimeInventory                inventory;
    Timestamp                             commit_ts = 0;
    bool                                  system_field_ready = false;
};

}  // namespace milvus::segcore::segment
```

> `PublishedSegmentState` 当前有 13 个字段，其中 6 个是 index readiness 的 bitset。拆走之后 `SegmentView` 只剩 5 个字段，且每个字段都是一个别处定义好的不可变类型。

### 4.3 `ReadGate`

接口保持现状（`SegmentReadLease.h:163`），仅迁移与改名：

```cpp
// segcore/segment/ReadGate.h
namespace milvus::segcore::segment {

class ReadGate {
 public:
    explicit ReadGate(std::chrono::milliseconds publish_drain_timeout);

    std::optional<SegmentReadLease>
    AcquireRead(const folly::CancellationToken&, OpContext*, int64_t segment_id) const;

    PublishLease AcquirePublish(OpContext*, int64_t segment_id) const;
    std::optional<PublishLease>
    AcquirePublishFailFast(OpContext*, int64_t segment_id) const;

    bool     CanAcquirePublishImmediately() const;
    uint64_t ActiveReaders() const;
    uint64_t BlockedReadersTotal() const;
    uint64_t PublishedGeneration() const;
    bool     WriterPending() const;
};

}  // namespace milvus::segcore::segment
```

### 4.4 工厂

```cpp
// segcore/segment/SegmentFactory.h
namespace milvus::segcore::segment {

SegmentPtr MakeSealed(SchemaPtr, IndexMetaPtr, const SegcoreConfig&,
                      int64_t segment_id, bool is_sorted_by_pk);

SegmentPtr MakeGrowing(SchemaPtr, IndexMetaPtr, const SegcoreConfig&,
                       int64_t segment_id);

}  // namespace milvus::segcore::segment
```

> 工厂放在这里而不是 capi。当前 `NewSegment` / `NewSegmentWithLoadInfo`（`segment_c.h:33,54`）在 C 层里同时做 proto 解析、schema 构造、segment 创建三件事。

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | 实现 `ISegment` 等 |
| `segcore_load` | `LoadExecutor`、`RuntimeInventory`、`LoadSpec` |
| `segcore_columnar` / `segcore_indexing` / `segcore_mvcc` / `segcore_text` | 构造能力实现 |
| `common/`、`folly` | `CancellationToken`、`OpContext` |

**禁止依赖：** `reduce`、`app`、`capi`、`query/`、`exec/`、任何 proto（`LoadSpec` 已经是 proto-free）。

> **`segment` 不依赖 `query/exec` 是本重构最重要的单条规则。** 当前 `SegmentInterface.cpp:154` 在 segment 内部构造 `ExecPlanNodeVisitor`，而 `query/ExecPlanNodeVisitor.h:23,38` 反过来 include `segcore/Utils.h` 与 `SegmentInterface.h`——这是 P0 环的核心。移除方式：`Search`/`Retrieve` 整体上移到 [app](09-application.md)。

## 6. 测试

`test_segcore_segment`，链接本模块 + L1/L2/L3，**不链接 `milvus_core`**。

现有可迁移：`ChunkedSegmentSealedTest.cpp`、`SegmentGrowingTest.cpp`、`SegmentSealedRetrieveTest.cpp`、`ChunkedSegmentSealedBinlogIndexTest.cpp`、`ChunkedSegmentSealedStorageV2Test.cpp`、`SegmentGrowingStorageV2Test.cpp`、`LoadCancellationTest.cpp`、`flush_growing_segment_test.cpp` 中不涉及 Search/Retrieve 的部分。

> 涉及 Search/Retrieve 的测试上移到 `test_segcore_app`——因为在新分层里查询编排不属于 segment。

新增覆盖：

- COW 换代：换代期间已 Capture 的旧快照仍然有效且内容不变
- 读租约与发布屏障：发布等待 reader 排空、`AcquirePublishFailFast` 在有读者时返回空
- reopen 幂等：同一 `LoadSpec` 连续 reopen 两次，第二次 `LoadPlan::Empty()` 且 generation 不变
- growing insert 三路写入的原子性：任一路失败时 PK 索引不应留下部分状态

## 7. 迁移步骤（P6 之前逐步进行，P7 收尾）

本模块没有独立的迁移阶段——它是**其他模块迁移的残差**。每完成一个 L1/L2/L3 模块，sealed/growing impl 就相应变薄。收尾动作：

1. `PublishedState<T>` 泛化，替换 `PublishedSegmentState` 的手写 COW（可在 P4 期间并行做）。
2. `Search` / `Retrieve` / `FillTargetEntry` 上移到 app（P5/P6）。
3. `SegmentInterface` / `SegmentSealed` / `SegmentGrowing` 删除，能力投影落地（P7）。
4. 检查规模约束 §2.1；未达标则回头找哪个模块没真正接走职责。

出口标准：§2.1 的三条行数约束达成；`segment/` 不 include `query/`、`exec/`、任何 proto；`test_segcore_segment` 不链接 `milvus_core`。
