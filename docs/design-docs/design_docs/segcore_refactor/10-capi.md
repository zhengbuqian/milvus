# L6 `segcore_capi` —— C ABI

> 设计提案。返回 [总览](README.md)。

## 1. 职责

Go ↔ C++ 的边界。**只做三件事**：

1. C 类型 ↔ C++ 类型的转换（`CSegmentInterface` ↔ `ISegment*`、`CTraceContext` ↔ `tracer::TraceContext`、裸指针 ↔ `shared_ptr`）。
2. 调用一个 [L5 app](09-application.md) service。
3. 异常 → `CStatus` 的转换，以及结果对象的所有权移交与释放函数。

## 2. 边界

**硬性禁令**（可 grep，写进 lint）：

| 禁止出现在 `capi/*.cpp` | 原因 |
|---|---|
| `#include "query/` / `#include "exec/` | 查询编排属于 app |
| `#include "segcore/segment/` 以外的 segcore 实现头 | 只允许通过 `ISegment` 与 app service |
| `folly::` executor / `Promise` / `Future` 的直接构造 | 异步调度属于 `app::AsyncDispatcher` |
| proto 的 `ParseFromArray` / 字段遍历 | 解析属于 `load::LoadInfoProtoAdapter` 或 `plan/` |
| `milvus_storage::` / manifest / 事务 | 属于 `app::FlushService` |
| 任何循环遍历数据 | 属于下层 |

**规模约束：** 每个 C 函数 ≤ 30 行。超过说明有逻辑没上移。

对照当前状态：`segment_c.cpp` 2,727 行，include 了 `query/PlanImpl.h`、`query/PlanNode.h`、`exec/expression/ExprCache.h`、`folly/executors/CPUThreadPoolExecutor.h`、`pb/schema.pb.h`、`pb/segcore.pb.h`、`milvus-storage/filesystem/fs.h`、`storage/loon_ffi/property_singleton.h`——**上表七条禁令当前条条命中**。

## 3. 现状来源

| 现文件 | 行数 | 处理 |
|---|---|---|
| `segment_c.{h,cpp}` | 2,727 | 业务逻辑迁 app，保留薄壳（目标 ≤ 500 行） |
| `search_result_export_c.{h,cpp}` | 1,223 | 物化迁 reduce、编排迁 app（目标 ≤ 250 行） |
| `load_index_c.{h,cpp}` | 406 | proto 解析迁 `load::LoadInfoProtoAdapter` |
| `external_utils_c.{h,cpp}` | 359 | 按内容分流 |
| `segcore_init_c.{h,cpp}` | 343 | 全局初始化，保留在 capi（这是它的合法职责） |
| `packed_writer_c` / `packed_reader_c` / `column_groups_c` / `arrow_fs_c` / `default_fs` | 900+ | **迁出 segcore**，见 §5 |
| `plan_c.{h,cpp}` | 230 | plan 生命周期迁 `app::PlanRegistry` |
| `load_field_data_c` / `collection_c` / `vector_index_c` / `check_vec_index_c` / `metrics_c` | ~600 | 已经较薄，原样迁入 `capi/` |
| `tokenizer_c` / `token_stream_c` / `phrase_match_c` / `minhash_c` | ~500 | 与 segment 无关，见 §5 |

## 4. 公开接口形态

C ABI 本身保持不变（Go 侧不改）。改的是**实现形态**：

```cpp
// segcore/capi/segment_c.cpp —— 重构后的典型形态
CStatus
SegmentLoad(CTraceContext c_trace, CSegmentInterface c_segment,
            const uint8_t* serialized_load_info, const int64_t len,
            CLoadCancellationSource c_source) {
    try {
        auto* segment = ToSegment(c_segment);                    // ① 类型转换
        auto  spec    = load::LoadSpecFromSerialized(             // ② adapter
                            serialized_load_info, len, segment->schema());
        auto  ctx     = ToOpContext(c_source);
        auto  trace   = ToTraceContext(c_trace);

        SegmentServiceSingleton().Load(*segment, spec, ctx.get(), trace);  // ③ 调 service
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);                        // ④ 异常转换
    }
}
```

四步之外不允许出现任何代码。当前同名函数包含 schema 解析、warmup policy 决策、cancellation 注册、executor 提交与 tracing span 管理。

### 转换工具

```cpp
// segcore/capi/Conversions.h
namespace milvus::segcore::capi {

ISegment*                 ToSegment(CSegmentInterface);
CSegmentInterface         FromSegment(SegmentPtr);       // 移交所有权
tracer::TraceContext      ToTraceContext(CTraceContext);
std::shared_ptr<OpContext> ToOpContext(CLoadCancellationSource);

// 结果对象所有权移交
CSearchResult   FromSearchResult(std::unique_ptr<SearchResult>);
CRetrieveResult FromRetrieveResult(std::unique_ptr<proto::segcore::RetrieveResults>);

}  // namespace milvus::segcore::capi
```

## 5. 与 segment 无关的 C 接口应迁出 segcore

`segcore/` 下有一批 `*_c.cpp` 与 Segment 毫无关系，只是历史上放在这里：

| 文件 | 实际归属 |
|---|---|
| `packed_reader_c` / `packed_writer_c` / `column_groups_c` | `storage/capi/` |
| `arrow_fs_c` / `default_fs` | `storage/capi/` |
| `tokenizer_c` / `token_stream_c` / `phrase_match_c` | `index/capi/`（tantivy binding 的门面） |
| `minhash_c` | `minhash/capi/` |
| `check_vec_index_c` / `vector_index_c` | `index/capi/` |
| `metrics_c` | `monitor/capi/` |

这些迁移与 segcore 的分层无关，但会让 `segcore/` 的边界更诚实——**"segcore 有 46 个生产 cpp"这个数字里，有相当一部分根本不是 segcore 的内容**。建议在 P6 一并处理，每个文件一个独立 commit。

## 6. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_app` | 全部 service |
| `segcore_contracts` | `ISegment` 类型 |
| `common/type_c.h`、`common/common_type_c.h` | C 类型定义 |
| `common/EasyAssert.h` | `CStatus` 构造 |

**禁止依赖：** 其余一切。

## 7. 错误处理约束

C ABI 是 `SegcoreError` 逃逸到 Go 的最后一站，本模块必须遵守 [CLAUDE.md 的 C++ 侧错误处理规则](../../../dev/error_handling_guide.md)：

- `FailureCStatus` 需要一个**真正的 `SegcoreError`**。`ExecOperatorException` 或 `throw std::runtime_error(status.ToString())` 会摧毁错误码。
- 错误分类在**构造点**（`ThrowInfo` / `AssertInfo` / `SegcoreError`）决定，不在这里。本模块**不得**做任何错误码重写、归并或降级。
- 新增 catch 分支时，必须确认 catch 到的类型携带了正确的错误码，而不是在这里补一个 `UnexpectedError` 兜底。

**lint 规则：** `capi/` 下不得出现 `catch (...)` 后直接构造固定错误码的模式。

## 8. 测试

C ABI 本身的测试（`segment_c_test.cpp`、`load_index_c_test.cpp`、`plan_c_test.cpp` 等）保留在 `all_tests`——它们测的是边界契约，需要完整链接。

但**业务逻辑的测试全部下沉到 `test_segcore_app`**。判据：一个 C ABI 测试如果在验证"某个 load diff 计算正确"，它就放错了层。

新增覆盖：

- 异常路径：service 抛 `SegcoreError` 时 `CStatus.error_code` 与原始码一致（对应 [G1 验证门](../../../../CLAUDE.md)）
- 所有权：`FromSegment` 之后 `DeleteSegment` 前后的引用计数
- 空指针 / 非法 handle 输入不崩溃

## 9. 迁移步骤（P6）

1. 建 `capi/` 与 target；先迁已经很薄的文件（`collection_c`、`load_field_data_c`、`vector_index_c`）验证形态。
2. `segment_c.cpp` 按函数逐个改造：每个函数的逻辑上移到 app service，C 侧收敛成四步形态。按 §4 的模板逐函数 review。
3. `search_result_export_c.cpp` 的物化部分下沉 reduce。
4. §5 的迁出，每个文件一个 commit。
5. 打开 §2 的 lint 禁令。

出口标准：§2 的七条禁令全部通过；每个 C 函数 ≤ 30 行；`segment_c.cpp` ≤ 500 行。
