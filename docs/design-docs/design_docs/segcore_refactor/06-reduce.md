# L2 `segcore_reduce` —— 结果归并与物化

> 设计提案。返回 [总览](README.md)。

## 1. 职责

把执行层产出的 `SearchResult` / `RetrieveResult` 变成可以交给上层的结果：

- 多 segment / 多 NQ 的 topK 归并与切片（`ReduceHelper`）。
- 精排（refine）：用向量索引重算精确距离，并按新距离重排。
- 结果物化：按 offset 从列/索引取出输出字段，填成 `DataArray` / `proto::segcore::RetrieveResults`。
- PK 回填与 ORDER BY 结果整理。
- 结果导出（Arrow RecordBatch 形态）。

## 2. 边界

**不属于本模块：**

- 执行计划的求值。属于 `exec/`。
- 结果的跨 segment 分发与并发调度。属于 [L5 app](09-application.md)。
- C ABI 与内存所有权移交。属于 [L6 capi](10-capi.md)。
- 列数据的读取机制。本模块通过 `IColumnSource::Gather` 取值，不认识 chunk 与 pin。

**关键边界改动：物化不再是 Segment 的方法。** 当前 `FillPrimaryKeys` / `FillTargetEntry` / `FillTargetEntryDirectly` / `FillOrderByResult` 是 `SegmentInterface` 与 `SegmentInternalInterface` 的成员（`SegmentInterface.h:87,92,505,510,722,730,736`）。它们的本质是"给定一批 offset 和一个输出 schema，产出结果 proto"——不需要是 Segment 的方法，只需要一个 `IColumnSource` 和一个 `IIndexProvider`。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/reduce/Reduce.{h,cpp}` | `reduce/ReduceHelper.*` | 原样迁移 |
| `segcore/ReduceStructure.h` | `reduce/ReduceStructure.h` | 原样迁移 |
| `SegmentInternalInterface::FillTargetEntry` 系列（4 个重载） | `reduce/Materializer.cpp` | 从 Segment 移出 |
| `SegmentInternalInterface::FillPrimaryKeys` | `reduce/Materializer.cpp` | 同上 |
| `SegmentInternalInterface::FillOrderByResult` | `reduce/OrderByMaterializer.cpp` | 同上 |
| `SegmentInternalInterface::bulk_subscript_not_exist_field` | `reduce/Materializer.cpp` | 缺字段的默认值填充 |
| `segcore/Utils.{h,cpp}` 的 DataArray 构造族（`CreateEmptyScalarDataArray`、`CreateDataArrayFrom`、`MergeDataArray` 等 10 个函数） | `reduce/DataArrayBuilder.*` | 见 [11-cross-cutting.md](11-cross-cutting.md) |
| `segcore/Utils.h:288` `SortEqualScoresByPks` | `reduce/ReduceHelper.cpp` | |
| `segcore/search_result_export_c.{h,cpp}` 的非 C 部分 | `reduce/SearchResultExport.cpp` | C 壳留在 capi |

## 4. 公开接口

### 4.1 `Materializer`

```cpp
// segcore/reduce/Materializer.h
namespace milvus::segcore::reduce {

// 物化所需的全部能力，显式列出。这个结构体本身就是
// "物化需要什么"的文档 —— 当前这个答案藏在 SegmentInterface 里。
struct MaterializeSource {
    const IColumnSource&  columns;
    const IIndexProvider& indexes;
    const IMvccView&      mvcc;
    SchemaPtr             schema;
    int64_t               segment_id = 0;
};

struct MaterializeRequest {
    const int64_t*             offsets = nullptr;
    int64_t                    count   = 0;
    std::vector<FieldId>       output_fields;
    std::vector<std::string>   dynamic_field_names;
    bool                       fill_pk_ids  = false;
    bool                       ignore_non_pk = false;
};

class Materializer {
 public:
    explicit Materializer(MaterializeSource);

    // Retrieve 路径
    void Fill(OpContext*, const MaterializeRequest&,
              proto::segcore::RetrieveResults& out) const;

    // Search 路径：只回填 PK
    void FillPrimaryKeys(OpContext*, const int64_t* offsets, int64_t count,
                         SearchResult& out) const;

    // ORDER BY 路径：已排序列搬移 + 延迟字段晚物化
    void FillOrdered(OpContext*, const MaterializeRequest&,
                     const OrderBySpec&,
                     proto::segcore::RetrieveResults& out) const;
};

}  // namespace milvus::segcore::reduce
```

> 取值策略（"该从 index 反查还是回原始列"）在 `Materializer` 内部实现，依据是 `IIndexProvider::Capability(f).index_has_raw_data` 与 `IScalarValueSource::CheapPerRowLookup()`。当前这段策略散在 `Utils.h:151` `ReverseDataFromIndex` 与 sealed impl 的多个 `bulk_subscript` 重载里。

### 4.2 `ReduceHelper`

接口基本保持现状，改动只有两处：不再持有 `SegmentInterface*`，改为持有一组 `MaterializeSource`；`RefineDistances` 通过 `IIndexProvider` 而非 `segment->CalcDistByIDs`。

```cpp
// segcore/reduce/ReduceHelper.h
namespace milvus::segcore::reduce {

class ReduceHelper {
 public:
    ReduceHelper(std::vector<SearchResult*>& search_results,
                 milvus::query::Plan* plan,
                 const milvus::query::PlaceholderGroup* placeholder_group,
                 std::vector<MaterializeSource> sources,   // 每 segment 一个
                 const int64_t* slice_nqs, const int64_t* slice_topKs,
                 int64_t slice_num,
                 tracer::TraceContext*, OpContext*);

    void Reduce();
    void Marshal();

    int64_t GetAllSearchCount() const;
};

}  // namespace milvus::segcore::reduce
```

### 4.3 `DataArrayBuilder`

`Utils` 中散落的 10 个 `Create*DataArray*` 函数收敛成一个 builder，并作为 `ColumnSink` 的实现，与 [columnar](03-columnar.md#43-columnsink) 对接：

```cpp
// segcore/reduce/DataArrayBuilder.h
namespace milvus::segcore::reduce {

class DataArrayBuilder final : public ColumnSink {
 public:
    DataArrayBuilder(const FieldMeta&, int64_t count);

    void Accept(int64_t index, ChunkSpan, int64_t offset_in_chunk) override;
    void AcceptNull(int64_t index) override;

    std::unique_ptr<DataArray> Build() &&;

    static std::unique_ptr<DataArray> Empty(const FieldMeta&, int64_t count);
    static std::unique_ptr<DataArray> DefaultFilled(const FieldMeta&, int64_t count);
};

std::unique_ptr<DataArray>
MergeDataArray(std::vector<MergeBase>&, const FieldMeta&);

}  // namespace milvus::segcore::reduce
```

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | `IColumnSource` / `IIndexProvider` / `IMvccView` / `ColumnSink` |
| `common/` | `QueryResult.h`、`FieldMeta.h`、`Schema.h` |
| `pb/segcore.pb.h`、`pb/schema.pb.h` | 结果 proto |
| `query/` | `Plan`、`PlaceholderGroup`、`RetrievePlan`（只读消费） |
| `knowhere` | refine 时的 `DataSetPtr` |

**禁止依赖：** `columnar`、`indexing`、`mvcc`、`text`、`load`、`segment`、`exec/`、`storage/`。

> 依赖 `query/` 不构成环：`query/` 依赖 `segcore_contracts`（L0），`reduce` 在 L2，`app` 在 L5 依赖两者。DAG 成立。

## 6. 测试

`test_segcore_reduce`，链接 `segcore_reduce` + `segcore_contracts_testing` + `milvus_common` + `milvus_query`，**不链接 `milvus_core`**。

现有可迁移：`ReduceStructureTest.cpp`、`search_result_export_c_test.cpp` 的非 C 部分。

新增覆盖（用 [contracts 的 fake](01-contracts.md#5-测试) 构造输入，无需真 segment）：

- 多 segment topK 归并：并列分数、topK 大于实际结果数、某 segment 结果为空
- 切片边界：`slice_nqs` 与 `slice_topKs` 不整除时的 prefix sum
- `Materializer` 在 `index_has_raw_data` 为 true/false 两条取值路径上产出一致
- 缺字段（schema evolution）时的默认值填充
- ORDER BY 的延迟字段晚物化：断言未被选中的行**没有**触发 `Gather`

最后一条当前无法测——因为需要观测 segment 的取值次数，而 segment 是真的。用 fake `IColumnSource` 计数即可。

## 7. 迁移步骤（P5）

1. 建 `reduce/` 与 target；迁 `Reduce.*`、`ReduceStructure.h`（已在子目录，改动小）。
2. 从 `Utils` 抽出 `DataArrayBuilder`，同时让它实现 `ColumnSink`。
3. `FillTargetEntry` 系列从 `SegmentInternalInterface` 迁入 `Materializer`；Segment 侧保留转发以便分步。
4. `ReduceHelper` 构造函数改接 `MaterializeSource`，删除对 `SegmentInterface*` 的持有。
5. `CalcDistByIDs` / `IsIndexRefineEnabled` 从 `SegmentInterface` 移到 `IIndexProvider`，refine 改走后者。
6. 删除 Segment 上的转发。

出口标准：`SegmentInterface` 上不再有任何 `Fill*` 方法；`test_segcore_reduce` 不链接 `milvus_core`。
