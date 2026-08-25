# L5 `segcore_app` —— 应用服务

> 设计提案。返回 [总览](README.md)。

## 1. 职责

编排层。它是**唯一**同时认识 Segment 与查询执行器的地方，也是当前 `*_c.cpp` 里业务逻辑的去处。

- `SegmentService`：创建、加载、reopen、释放、资源估算。
- `QueryService`：Search / Retrieve / RetrieveByOffsets 的完整编排——绑定 plan、构造 executor、驱动 reduce、处理取消与 tracing。
- `FlushService`：growing segment flush、Storage V3 manifest 事务、物化字段导出。
- `AsyncDispatcher`：folly executor 接入、future 生命周期、取消源管理。

## 2. 为什么这一层必须存在

当前 `Search` / `Retrieve` 是 `SegmentInterface` 的虚函数，实现在 `SegmentInterface.cpp:154` 处构造 `ExecPlanNodeVisitor`。同时 `query/ExecPlanNodeVisitor.h:23,38` 反向 include `segcore/Utils.h` 与 `SegmentInterface.h`，`query/SearchOnGrowing.h:23` 直接依赖 `SegmentGrowingImpl`。这就是 P0 的 `segcore ↔ query/exec` 双向环。

环无法通过"把接口变窄"消除——只要**查询编排是 Segment 的方法**，Segment 就必须认识 executor。唯一的解法是把编排**上移一层**：

```text
现状：  segment → query executor → concrete segment      （环）

重构后：app → query executor → segcore_contracts          （DAG）
        app → segment ────────→ segcore_contracts
```

`query/exec` 只依赖 L0 契约，`app` 在 L5 同时依赖两者。**这一层的存在就是环的解**。

## 3. 边界

**不属于本模块：**

- 任何数据结构与算法。本模块只做顺序编排、错误处理、并发调度。
- C 类型转换与 `CStatus` 构造。属于 [L6 capi](10-capi.md)。
- proto 解析。`LoadSpec` 的构造属于 [load](07-load.md) 的 adapter；plan proto 的解析属于 `plan/`。**唯一例外**是 app 需要把已解析好的 proto 结果对象交给 capi，那是引用传递，不是解析。

**规模约束：** 每个 service ≤ 600 行。超过说明有算法混进来了。

## 4. 现状来源

| 现位置 | 迁入 | 说明 |
|---|---|---|
| `segment_c.cpp`（2,727 行）中的 segment 创建、schema 解析、load 编排 | `app/SegmentService.cpp` | |
| `segment_c.cpp` 中的 `AsyncSearch` / `AsyncRetrieve` / `AsyncRetrieveByOffsets` 编排体 | `app/QueryService.cpp` | |
| `segment_c.cpp` 中的 `FlushGrowingSegmentData` / `GetGrowingSegmentMaterializedFieldIDs` / `GetGrowingSegmentPrimaryKeys` | `app/FlushService.cpp` | |
| `segment_c.cpp` 中的 folly executor / promise / cancellation source 管理 | `app/AsyncDispatcher.cpp` | |
| `SegmentInterface::Search` / `Retrieve`（`SegmentInterface.cpp`，1,000 行） | `app/QueryService.cpp` | 含 `check_search` |
| `load_index_c.cpp` / `load_field_data_c.cpp` 的编排部分 | `app/SegmentService.cpp` | |
| `search_result_export_c.cpp`（1,223 行）的编排部分 | `app/QueryService.cpp` | 物化本身在 reduce |
| `plan_c.cpp` 的 plan 生命周期管理 | `app/PlanRegistry.cpp` | |

## 5. 公开接口

### 5.1 `SegmentService`

```cpp
// segcore/app/SegmentService.h
namespace milvus::segcore::app {

class SegmentService {
 public:
    explicit SegmentService(const SegcoreConfig&);

    SegmentPtr CreateSealed(SchemaPtr, IndexMetaPtr, int64_t segment_id,
                            bool is_sorted_by_pk);
    SegmentPtr CreateGrowing(SchemaPtr, IndexMetaPtr, int64_t segment_id);

    void Load(ISegment&, const load::LoadSpec&, OpContext*, tracer::TraceContext&);
    void Reopen(ISegment&, const load::LoadSpec&, SchemaPtr, OpContext*);

    void DropField(ISegment&, FieldId);
    void Release(SegmentPtr);

    ResourceEstimate Estimate(const ISegment&, const load::LoadSpec&) const;
};

}  // namespace milvus::segcore::app
```

### 5.2 `QueryService`

```cpp
// segcore/app/QueryService.h
namespace milvus::segcore::app {

struct SearchRequest {
    const query::Plan*             plan = nullptr;
    const query::PlaceholderGroup* placeholder_group = nullptr;
    Timestamp                      mvcc_timestamp = 0;
    Timestamp                      collection_ttl = 0;
    int64_t                        entity_ttl_physical_time_us = 0;
    int32_t                        consistency_level = 0;
    bool                           filter_only = false;
    bool                           enable_expr_cache = false;
    folly::CancellationToken       cancel_token;
    tracer::SpanPtr                trace_span;
};

struct RetrieveRequest {
    const query::RetrievePlan* plan = nullptr;
    Timestamp                  mvcc_timestamp = 0;
    int64_t                    limit_size = 0;
    bool                       ignore_non_pk = false;
    const int64_t*             offsets = nullptr;   // by-offsets 路径
    int64_t                    offset_count = 0;
    Timestamp                  collection_ttl = 0;
    int64_t                    entity_ttl_physical_time_us = 0;
    folly::CancellationToken   cancel_token;
};

class QueryService {
 public:
    QueryService(std::shared_ptr<exec::ExecutorFactory>,
                 std::shared_ptr<reduce::MaterializerFactory>);

    // 编排步骤：取读租约 → 校验 plan → 构造 executor →
    //           执行 → 应用 mvcc/delete → 物化 → 返回
    std::unique_ptr<SearchResult>
    Search(const ISegment&, const SearchRequest&, OpContext*, tracer::TraceContext*);

    std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(const ISegment&, const RetrieveRequest&, OpContext*, tracer::TraceContext*);
};

}  // namespace milvus::segcore::app
```

> `exec::ExecutorFactory` 是 exec 侧新增的窄工厂，取代当前 segment 内部直接 `new ExecPlanNodeVisitor`。它接受 `const IColumnSource&` / `const IIndexProvider&` / `const IMvccView&`，不接受 `ISegment`——这样 exec 连 Segment 这个类型都不需要认识。

### 5.3 `FlushService`

```cpp
// segcore/app/FlushService.h
namespace milvus::segcore::app {

struct FlushResult {
    std::vector<std::string> manifest_paths;
    int64_t                  row_count = 0;
    int64_t                  flushed_bytes = 0;
};

class FlushService {
 public:
    explicit FlushService(std::shared_ptr<storage::FileSystemProvider>);

    FlushResult FlushGrowing(ISegment&, const FlushSpec&, OpContext*);

    std::vector<FieldId> MaterializedFieldIds(const ISegment&) const;
    std::vector<PkType>  PrimaryKeys(const ISegment&) const;
};

}  // namespace milvus::segcore::app
```

### 5.4 `AsyncDispatcher`

```cpp
// segcore/app/AsyncDispatcher.h
namespace milvus::segcore::app {

// 把同步 service 调用包成 folly future，并统一处理取消与异常。
// 这是当前散在 segment_c.cpp 里的 promise/executor 样板的唯一去处。
class AsyncDispatcher {
 public:
    explicit AsyncDispatcher(std::shared_ptr<folly::Executor>);

    template <typename T>
    futures::Future<T> Submit(std::function<T(OpContext*)> work,
                              const folly::CancellationToken&,
                              tracer::TraceContext);

    CancellationSourcePtr NewCancellationSource();
};

}  // namespace milvus::segcore::app
```

## 6. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_segment` | 创建与生命周期 |
| `segcore_reduce` | 归并与物化 |
| `segcore_load` | `LoadSpec` 与 adapter |
| `segcore_contracts` | 能力接口 |
| `query/`、`exec/` | **本层是唯一允许依赖它们的 segcore 模块** |
| `futures/`、`folly` | 异步 |
| `storage/`、`milvus-storage` | flush 路径 |
| `pb/` | 结果 proto（不解析，只传递） |

**禁止依赖：** `capi`、`columnar`、`indexing`、`mvcc`、`text`（这些应通过 `ISegment` 的能力投影访问，不直接依赖实现模块）。

## 7. 测试

`test_segcore_app`，链接 app + segment + reduce + load + query + exec，**不链接 `milvus_core`**（这是层级最高的模块，链接面最大，但仍应能独立成目标）。

从 `segment_c_test.cpp` 迁入的测试将从"通过 C ABI 测业务逻辑"变成"直接测 service"——这是本模块最直接的收益：**当前 `segment_c.cpp` 里 2,700 行业务逻辑只能通过 C 接口或 Go 端 e2e 触发。**

新增覆盖：

- Search 编排：读租约获取失败、plan 校验失败、executor 抛异常时的资源释放
- 取消：在 executor 执行中途取消，断言 pin 与租约都被释放
- Retrieve by offsets 的越界 offset 处理
- flush 中途失败时不产生半成品 manifest
- `AsyncDispatcher` 的异常传播与取消传播

## 8. 迁移步骤（P6）

1. 建 `app/` 与 target。先落 `AsyncDispatcher`，把 `segment_c.cpp` 的 promise/executor 样板搬过来（纯机械，风险最低）。
2. `SegmentService`：把 `NewSegment` / `NewSegmentWithLoadInfo` / `SegmentLoad` / `AsyncReopenSegment` 的编排体搬过来，C 层只剩转发。
3. `QueryService`：把 `SegmentInterface::Search` / `Retrieve` 整体上移。**这是打开 P0 环的关键一步**，需要同时给 exec 加 `ExecutorFactory`。
4. 修 `query/ExecPlanNodeVisitor.h:23` 与 `query/SearchOnGrowing.h:23`，改为依赖 contracts。
5. `FlushService`：搬 `FlushGrowingSegmentData` 一族。
6. 检查规模约束（每个 service ≤ 600 行）。

出口标准：`query/` + `exec/` 对 `segcore/` 的 include 全部指向 `contracts/`；`segment/` 不 include `query/`、`exec/`；`test_segcore_app` 中存在不经 C ABI 的 service 级测试。
