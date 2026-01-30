# IWYU (Include What You Use) Progress Report

## Summary

| Category | Count |
|----------|-------|
| Modified .cpp files | 196 |
| Modified .h files | 103 |
| Unmodified .h files | 244 |
| Total files changed | 301 |
| Lines added | +3953 |
| Lines removed | -1046 |

## Commit History

| Commit | Description |
|--------|-------------|
| ca3683804c | fix: add missing StringIndexMarisa.h include to test files |
| e98ccc2bd3 | style: apply clang-format to load_field_data_c.h |
| eed0f5d5ad | enhance: apply IWYU fixes to C++ header files - batch 5-12 (final) |
| a971654bd5 | enhance: apply IWYU fixes to header files - batch 4 |
| 69bf899fc7 | enhance: apply IWYU fixes to header files - batch 3 |
| 47727f3588 | enhance: apply IWYU fixes to header files - batch 2 |
| 637ae753b3 | enhance: apply IWYU fixes to header files - batch 1 |
| 7b9ffdac3e | enhance: add --no_fwd_decls to IWYU script to prefer includes over forward declarations |
| 944a9ea05d | enhance: apply IWYU fixes to C++ source files - batch 10 (final) |
| 06d34e9ae8 | enhance: apply IWYU fixes to C++ source files - batch 9 |
| b977b49be0 | enhance: apply IWYU fixes to C++ source files - batch 8 |
| a6f483dd2a | enhance: apply IWYU fixes to C++ source files - batch 7 |
| 5e10b5d572 | enhance: apply IWYU fixes to C++ source files - batch 6 |
| 3c76ae4e1e | enhance: apply IWYU fixes to C++ source files - batch 5 |
| 9b2b909361 | enhance: apply IWYU fixes to C++ source files - batch 4 |
| 3463f76069 | enhance: apply IWYU fixes to C++ source files - batch 3 |
| ec4b247edc | enhance: apply IWYU fixes to C++ source files - batch 2 |
| 47ed224578 | enhance: apply IWYU fixes to C++ source files - batch 1 |
| 718bdef177 | iwyu attempt, cpp files only |

## Skipped Files (Reverted due to compilation issues)

The following 2 header files were intentionally NOT modified because IWYU suggestions caused compilation failures:

1. `internal/core/src/storage/DiskFileManagerImpl.h`
2. `internal/core/src/storage/MinioChunkManager.h`

## Quality Assessment

**Not perfect IWYU** - Conservative strategy was used:

1. **Pragmatic approach**: Some IWYU suggestions were rejected to ensure compilation success
2. **Some redundant includes preserved**: Where removing them might affect other dependent files
3. **Forward declaration strategy**: Used `--no_fwd_decls` flag, preferring full includes over forward declarations
4. **Test file patches**: Some test files needed additional includes added later

**Current status**:
- Compilation passes (exit code 0)
- All changes committed

---

## Modified .cpp Files (196 files)

```
internal/core/src/clustering/analyze_c.cpp
internal/core/src/clustering/KmeansClustering.cpp
internal/core/src/common/ArrayOffsets.cpp
internal/core/src/common/binary_set_c.cpp
internal/core/src/common/Chunk.cpp
internal/core/src/common/ChunkTarget.cpp
internal/core/src/common/ChunkWriter.cpp
internal/core/src/common/Common.cpp
internal/core/src/common/ComplexVector.cpp
internal/core/src/common/ElementFilterIterator.cpp
internal/core/src/common/FieldData.cpp
internal/core/src/common/FieldMeta.cpp
internal/core/src/common/IndexMeta.cpp
internal/core/src/common/init_c.cpp
internal/core/src/common/JsonCastFunction.cpp
internal/core/src/common/JsonCastType.cpp
internal/core/src/common/JsonUtils.cpp
internal/core/src/common/logging_c.cpp
internal/core/src/common/OffsetMapping.cpp
internal/core/src/common/RangeSearchHelper.cpp
internal/core/src/common/RegexQuery.cpp
internal/core/src/common/Schema.cpp
internal/core/src/common/Slice.cpp
internal/core/src/common/SystemProperty.cpp
internal/core/src/common/Types.cpp
internal/core/src/config/ConfigKnowhere.cpp
internal/core/src/exec/Driver.cpp
internal/core/src/exec/expression/AlwaysTrueExpr.cpp
internal/core/src/exec/expression/BinaryArithOpEvalRangeExpr.cpp
internal/core/src/exec/expression/BinaryRangeExpr.cpp
internal/core/src/exec/expression/CallExpr.cpp
internal/core/src/exec/expression/ColumnExpr.cpp
internal/core/src/exec/expression/CompareExpr.cpp
internal/core/src/exec/expression/ConjunctExpr.cpp
internal/core/src/exec/expression/ExistsExpr.cpp
internal/core/src/exec/expression/Expr.cpp
internal/core/src/exec/expression/ExprTest.cpp
internal/core/src/exec/expression/function/FunctionFactory.cpp
internal/core/src/exec/expression/function/FunctionImplUtils.cpp
internal/core/src/exec/expression/function/impl/Empty.cpp
internal/core/src/exec/expression/function/impl/StartsWith.cpp
internal/core/src/exec/expression/GISFunctionFilterExpr.cpp
internal/core/src/exec/expression/JsonContainsExpr.cpp
internal/core/src/exec/expression/LikeConjunctExpr.cpp
internal/core/src/exec/expression/LogicalBinaryExpr.cpp
internal/core/src/exec/expression/LogicalUnaryExpr.cpp
internal/core/src/exec/expression/MatchExpr.cpp
internal/core/src/exec/expression/NullExpr.cpp
internal/core/src/exec/expression/TermExpr.cpp
internal/core/src/exec/expression/TimestamptzArithCompareExpr.cpp
internal/core/src/exec/expression/UnaryExpr.cpp
internal/core/src/exec/expression/ValueExpr.cpp
internal/core/src/exec/HashTable.cpp
internal/core/src/exec/operator/AggregationNode.cpp
internal/core/src/exec/operator/ElementFilterBitsNode.cpp
internal/core/src/exec/operator/ElementFilterNode.cpp
internal/core/src/exec/operator/FilterBitsNode.cpp
internal/core/src/exec/operator/IterativeFilterNode.cpp
internal/core/src/exec/operator/MvccNode.cpp
internal/core/src/exec/operator/ProjectNode.cpp
internal/core/src/exec/operator/query-agg/Aggregate.cpp
internal/core/src/exec/operator/query-agg/AggregateInfo.cpp
internal/core/src/exec/operator/query-agg/CountAggregateBase.cpp
internal/core/src/exec/operator/query-agg/GroupingSet.cpp
internal/core/src/exec/operator/query-agg/MaxAggregate.cpp
internal/core/src/exec/operator/query-agg/MinAggregate.cpp
internal/core/src/exec/operator/query-agg/RowContainer.cpp
internal/core/src/exec/operator/query-agg/SumAggregate.cpp
internal/core/src/exec/operator/RandomSampleNode.cpp
internal/core/src/exec/operator/RescoresNode.cpp
internal/core/src/exec/operator/SearchGroupByNode.cpp
internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.cpp
internal/core/src/exec/operator/VectorSearchNode.cpp
internal/core/src/exec/Task.cpp
internal/core/src/exec/VectorHasher.cpp
internal/core/src/futures/Executor.cpp
internal/core/src/futures/future_c.cpp
internal/core/src/futures/future_test_case_c.cpp
internal/core/src/index/HybridScalarIndex.cpp
internal/core/src/index/IndexFactory.cpp
internal/core/src/index/IndexStats.cpp
internal/core/src/index/InvertedIndexTantivy.cpp
internal/core/src/index/json_stats/bson_builder.cpp
internal/core/src/index/json_stats/bson_inverted.cpp
internal/core/src/index/json_stats/JsonKeyStats.cpp
internal/core/src/index/json_stats/parquet_writer.cpp
internal/core/src/index/json_stats/utils.cpp
internal/core/src/index/JsonFlatIndex.cpp
internal/core/src/index/JsonIndexBuilder.cpp
internal/core/src/index/JsonInvertedIndex.cpp
internal/core/src/index/RTreeIndex.cpp
internal/core/src/index/RTreeIndexWrapper.cpp
internal/core/src/index/ScalarIndex.cpp
internal/core/src/index/ScalarIndexSort.cpp
internal/core/src/index/ScalarIndexTest.cpp
internal/core/src/index/skipindex_stats/SkipIndexStats.cpp
internal/core/src/index/SkipIndex.cpp
internal/core/src/index/StringIndexMarisa.cpp
internal/core/src/index/StringIndexSort.cpp
internal/core/src/index/Utils.cpp
internal/core/src/index/VectorDiskIndex.cpp
internal/core/src/index/VectorMemIndex.cpp
internal/core/src/indexbuilder/index_c.cpp
internal/core/src/indexbuilder/init_c.cpp
internal/core/src/indexbuilder/ScalarIndexCreator.cpp
internal/core/src/indexbuilder/VecIndexCreator.cpp
internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx2.cpp
internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx512.cpp
internal/core/src/minhash/MinHashComputer.cpp
internal/core/src/minhash/MinHashHook.cpp
internal/core/src/monitor/Monitor.cpp
internal/core/src/monitor/monitor_c.cpp
internal/core/src/monitor/scope_metric.cpp
internal/core/src/plan/PlanNode.cpp
internal/core/src/query/CachedSearchIterator.cpp
internal/core/src/query/ExecPlanNodeVisitor.cpp
internal/core/src/query/Plan.cpp
internal/core/src/query/PlanProto.cpp
internal/core/src/query/SearchBruteForce.cpp
internal/core/src/query/SearchOnGrowing.cpp
internal/core/src/query/SearchOnIndex.cpp
internal/core/src/query/SearchOnSealed.cpp
internal/core/src/query/SubSearchResult.cpp
internal/core/src/rescores/Scorer.cpp
internal/core/src/segcore/arrow_fs_c.cpp
internal/core/src/segcore/check_vec_index_c.cpp
internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp
internal/core/src/segcore/Collection.cpp
internal/core/src/segcore/collection_c.cpp
internal/core/src/segcore/column_groups_c.cpp
internal/core/src/segcore/ConcurrentVector.cpp
internal/core/src/segcore/FieldIndexing.cpp
internal/core/src/segcore/IndexConfigGenerator.cpp
internal/core/src/segcore/load_field_data_c.cpp
internal/core/src/segcore/load_index_c.cpp
internal/core/src/segcore/memory_planner.cpp
internal/core/src/segcore/metrics_c.cpp
internal/core/src/segcore/minhash_c.cpp
internal/core/src/segcore/packed_reader_c.cpp
internal/core/src/segcore/packed_writer_c.cpp
internal/core/src/segcore/phrase_match_c.cpp
internal/core/src/segcore/plan_c.cpp
internal/core/src/segcore/reduce/GroupReduce.cpp
internal/core/src/segcore/reduce/Reduce.cpp
internal/core/src/segcore/reduce/StreamReduce.cpp
internal/core/src/segcore/reduce_c.cpp
internal/core/src/segcore/ReduceUtils.cpp
internal/core/src/segcore/segcore_init_c.cpp
internal/core/src/segcore/segment_c.cpp
internal/core/src/segcore/SegmentChunkReader.cpp
internal/core/src/segcore/SegmentGrowingImpl.cpp
internal/core/src/segcore/SegmentInterface.cpp
internal/core/src/segcore/SegmentLoadInfo.cpp
internal/core/src/segcore/storagev1translator/BsonInvertedIndexTranslator.cpp
internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.cpp
internal/core/src/segcore/storagev1translator/InterimSealedIndexTranslator.cpp
internal/core/src/segcore/storagev1translator/SealedIndexTranslator.cpp
internal/core/src/segcore/storagev1translator/TextMatchIndexTranslator.cpp
internal/core/src/segcore/storagev1translator/V1SealedIndexTranslator.cpp
internal/core/src/segcore/storagev2translator/GroupChunkTranslator.cpp
internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp
internal/core/src/segcore/TimestampIndex.cpp
internal/core/src/segcore/token_stream_c.cpp
internal/core/src/segcore/tokenizer_c.cpp
internal/core/src/segcore/Utils.cpp
internal/core/src/segcore/vector_index_c.cpp
internal/core/src/storage/aliyun/AliyunCredentialsProvider.cpp
internal/core/src/storage/aliyun/AliyunSTSClient.cpp
internal/core/src/storage/azure/AzureBlobChunkManager.cpp
internal/core/src/storage/azure/AzureChunkManager.cpp
internal/core/src/storage/BinlogReader.cpp
internal/core/src/storage/DataCodec.cpp
internal/core/src/storage/DiskFileManagerImpl.cpp
internal/core/src/storage/Event.cpp
internal/core/src/storage/FileWriter.cpp
internal/core/src/storage/huawei/HuaweiCloudCredentialsProvider.cpp
internal/core/src/storage/huawei/HuaweiCloudSTSClient.cpp
internal/core/src/storage/IndexData.cpp
internal/core/src/storage/InsertData.cpp
internal/core/src/storage/KeyRetriever.cpp
internal/core/src/storage/loon_ffi/ffi_reader_c.cpp
internal/core/src/storage/loon_ffi/ffi_writer_c.cpp
internal/core/src/storage/loon_ffi/util.cpp
internal/core/src/storage/MemFileManagerImpl.cpp
internal/core/src/storage/minio/MinioChunkManager.cpp
internal/core/src/storage/MmapChunkManager.cpp
internal/core/src/storage/PayloadReader.cpp
internal/core/src/storage/PayloadStream.cpp
internal/core/src/storage/PayloadWriter.cpp
internal/core/src/storage/RemoteInputStream.cpp
internal/core/src/storage/RemoteOutputStream.cpp
internal/core/src/storage/storage_c.cpp
internal/core/src/storage/tencent/TencentCloudCredentialsProvider.cpp
internal/core/src/storage/tencent/TencentCloudSTSClient.cpp
internal/core/src/storage/ThreadPool.cpp
internal/core/src/storage/ThreadPools.cpp
```

## Modified .h Files (103 files)

```
internal/core/src/clustering/analyze_c.h
internal/core/src/clustering/KmeansClustering.h
internal/core/src/common/ArrayOffsets.h
internal/core/src/common/binary_set_c.h
internal/core/src/common/ChunkWriter.h
internal/core/src/common/ElementFilterIterator.h
internal/core/src/common/FieldMeta.h
internal/core/src/common/IndexMeta.h
internal/core/src/common/init_c.h
internal/core/src/common/JsonCastType.h
internal/core/src/common/JsonUtils.h
internal/core/src/common/OffsetMapping.h
internal/core/src/common/RangeSearchHelper.h
internal/core/src/common/Schema.h
internal/core/src/common/Slice.h
internal/core/src/exec/Driver.h
internal/core/src/exec/HashTable.h
internal/core/src/exec/Task.h
internal/core/src/exec/VectorHasher.h
internal/core/src/exec/expression/AlwaysTrueExpr.h
internal/core/src/exec/expression/BinaryRangeExpr.h
internal/core/src/exec/expression/CallExpr.h
internal/core/src/exec/expression/ColumnExpr.h
internal/core/src/exec/expression/CompareExpr.h
internal/core/src/exec/expression/ConjunctExpr.h
internal/core/src/exec/expression/ExistsExpr.h
internal/core/src/exec/expression/GISFunctionFilterExpr.h
internal/core/src/exec/expression/function/FunctionFactory.h
internal/core/src/exec/expression/function/FunctionImplUtils.h
internal/core/src/exec/operator/AggregationNode.h
internal/core/src/exec/operator/ElementFilterNode.h
internal/core/src/exec/operator/IterativeFilterNode.h
internal/core/src/exec/operator/RescoresNode.h
internal/core/src/exec/operator/SearchGroupByNode.h
internal/core/src/exec/operator/VectorSearchNode.h
internal/core/src/exec/operator/query-agg/Aggregate.h
internal/core/src/exec/operator/query-agg/AggregateInfo.h
internal/core/src/exec/operator/query-agg/CountAggregateBase.h
internal/core/src/exec/operator/query-agg/GroupingSet.h
internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.h
internal/core/src/futures/future_c.h
internal/core/src/index/HybridScalarIndex.h
internal/core/src/index/IndexFactory.h
internal/core/src/index/IndexStats.h
internal/core/src/index/InvertedIndexTantivy.h
internal/core/src/index/JsonIndexBuilder.h
internal/core/src/index/RTreeIndex.h
internal/core/src/index/RTreeIndexWrapper.h
internal/core/src/index/ScalarIndexSort.h
internal/core/src/index/VectorMemIndex.h
internal/core/src/index/json_stats/bson_builder.h
internal/core/src/index/json_stats/parquet_writer.h
internal/core/src/index/skipindex_stats/SkipIndexStats.h
internal/core/src/indexbuilder/ScalarIndexCreator.h
internal/core/src/indexbuilder/VecIndexCreator.h
internal/core/src/indexbuilder/index_c.h
internal/core/src/minhash/MinHashComputer.h
internal/core/src/minhash/MinHashHook.h
internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx2.h
internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx512.h
internal/core/src/query/CachedSearchIterator.h
internal/core/src/query/SearchBruteForce.h
internal/core/src/segcore/Collection.h
internal/core/src/segcore/FieldIndexing.h
internal/core/src/segcore/IndexConfigGenerator.h
internal/core/src/segcore/ReduceUtils.h
internal/core/src/segcore/SegmentLoadInfo.h
internal/core/src/segcore/arrow_fs_c.h
internal/core/src/segcore/collection_c.h
internal/core/src/segcore/column_groups_c.h
internal/core/src/segcore/load_field_data_c.h
internal/core/src/segcore/metrics_c.h
internal/core/src/segcore/packed_reader_c.h
internal/core/src/segcore/packed_writer_c.h
internal/core/src/segcore/phrase_match_c.h
internal/core/src/segcore/plan_c.h
internal/core/src/segcore/reduce/GroupReduce.h
internal/core/src/segcore/segcore_init_c.h
internal/core/src/segcore/token_stream_c.h
internal/core/src/segcore/tokenizer_c.h
internal/core/src/segcore/vector_index_c.h
internal/core/src/storage/BinlogReader.h
internal/core/src/storage/DataCodec.h
internal/core/src/storage/Event.h
internal/core/src/storage/FileWriter.h
internal/core/src/storage/IndexData.h
internal/core/src/storage/InsertData.h
internal/core/src/storage/MmapChunkManager.h
internal/core/src/storage/PayloadReader.h
internal/core/src/storage/PayloadStream.h
internal/core/src/storage/PayloadWriter.h
internal/core/src/storage/ThreadPool.h
internal/core/src/storage/aliyun/AliyunCredentialsProvider.h
internal/core/src/storage/aliyun/AliyunSTSClient.h
internal/core/src/storage/azure/AzureBlobChunkManager.h
internal/core/src/storage/azure/AzureChunkManager.h
internal/core/src/storage/huawei/HuaweiCloudCredentialsProvider.h
internal/core/src/storage/huawei/HuaweiCloudSTSClient.h
internal/core/src/storage/loon_ffi/ffi_reader_c.h
internal/core/src/storage/loon_ffi/ffi_writer_c.h
internal/core/src/storage/storage_c.h
internal/core/src/storage/tencent/TencentCloudCredentialsProvider.h
internal/core/src/storage/tencent/TencentCloudSTSClient.h
```

## Unmodified .h Files (244 files)

These files were not processed by IWYU. Some may not need changes, others may benefit from future IWYU passes.

```
internal/core/src/bitset/bitset.h
internal/core/src/bitset/common.h
internal/core/src/bitset/detail/bit_wise.h
internal/core/src/bitset/detail/ctz.h
internal/core/src/bitset/detail/element_vectorized.h
internal/core/src/bitset/detail/element_wise.h
internal/core/src/bitset/detail/maybe_vector.h
internal/core/src/bitset/detail/platform/arm/instruction_set.h
internal/core/src/bitset/detail/platform/arm/neon-decl.h
internal/core/src/bitset/detail/platform/arm/neon-impl.h
internal/core/src/bitset/detail/platform/arm/neon.h
internal/core/src/bitset/detail/platform/arm/sve-decl.h
internal/core/src/bitset/detail/platform/arm/sve-impl.h
internal/core/src/bitset/detail/platform/arm/sve.h
internal/core/src/bitset/detail/platform/dynamic.h
internal/core/src/bitset/detail/platform/vectorized_ref.h
internal/core/src/bitset/detail/platform/x86/avx2-decl.h
internal/core/src/bitset/detail/platform/x86/avx2-impl.h
internal/core/src/bitset/detail/platform/x86/avx2.h
internal/core/src/bitset/detail/platform/x86/avx512-decl.h
internal/core/src/bitset/detail/platform/x86/avx512-impl.h
internal/core/src/bitset/detail/platform/x86/avx512.h
internal/core/src/bitset/detail/platform/x86/common.h
internal/core/src/bitset/detail/platform/x86/instruction_set.h
internal/core/src/bitset/detail/popcount.h
internal/core/src/bitset/detail/proxy.h
internal/core/src/clustering/file_utils.h
internal/core/src/clustering/type_c.h
internal/core/src/clustering/types.h
internal/core/src/common/Array.h
internal/core/src/common/ArrowDataWrapper.h
internal/core/src/common/BitUtil.h
internal/core/src/common/BitsetView.h
internal/core/src/common/BloomFilter.h
internal/core/src/common/CDataType.h
internal/core/src/common/Channel.h
internal/core/src/common/Chunk.h
internal/core/src/common/ChunkTarget.h
internal/core/src/common/Common.h
internal/core/src/common/Consts.h
internal/core/src/common/CustomBitset.h
internal/core/src/common/Exception.h
internal/core/src/common/FieldData.h
internal/core/src/common/FieldDataInterface.h
internal/core/src/common/File.h
internal/core/src/common/Geometry.h
internal/core/src/common/GeometryCache.h
internal/core/src/common/GroupChunk.h
internal/core/src/common/Json.h
internal/core/src/common/JsonCastFunction.h
internal/core/src/common/LoadInfo.h
internal/core/src/common/PreparedGeometry.h
internal/core/src/common/Promise.h
internal/core/src/common/QueryInfo.h
internal/core/src/common/QueryResult.h
internal/core/src/common/RegexQuery.h
internal/core/src/common/ScopedTimer.h
internal/core/src/common/SimdUtil.h
internal/core/src/common/Span.h
internal/core/src/common/SystemProperty.h
internal/core/src/common/TypeTraits.h
internal/core/src/common/Types.h
internal/core/src/common/Utils.h
internal/core/src/common/ValueOp.h
internal/core/src/common/Vector.h
internal/core/src/common/VectorArray.h
internal/core/src/common/VectorTrait.h
internal/core/src/common/bson_view.h
internal/core/src/common/float_util_c.h
internal/core/src/common/jsmn.h
internal/core/src/common/logging_c.h
internal/core/src/common/protobuf_utils.h
internal/core/src/common/protobuf_utils_c.h
internal/core/src/common/resource_c.h
internal/core/src/common/type_c.h
internal/core/src/config/ConfigKnowhere.h
internal/core/src/exec/QueryContext.h
internal/core/src/exec/expression/BinaryArithOpEvalRangeExpr.h
internal/core/src/exec/expression/Element.h
internal/core/src/exec/expression/EvalCtx.h
internal/core/src/exec/expression/Expr.h
internal/core/src/exec/expression/ExprCache.h
internal/core/src/exec/expression/JsonContainsExpr.h
internal/core/src/exec/expression/LikeConjunctExpr.h
internal/core/src/exec/expression/LogicalBinaryExpr.h
internal/core/src/exec/expression/LogicalUnaryExpr.h
internal/core/src/exec/expression/MatchExpr.h
internal/core/src/exec/expression/NullExpr.h
internal/core/src/exec/expression/TermExpr.h
internal/core/src/exec/expression/TimestamptzArithCompareExpr.h
internal/core/src/exec/expression/UnaryExpr.h
internal/core/src/exec/expression/Utils.h
internal/core/src/exec/expression/ValueExpr.h
internal/core/src/exec/expression/function/impl/StringFunctions.h
internal/core/src/exec/expression/function/init_c.h
internal/core/src/exec/operator/CallbackSink.h
internal/core/src/exec/operator/ElementFilterBitsNode.h
internal/core/src/exec/operator/FilterBitsNode.h
internal/core/src/exec/operator/MvccNode.h
internal/core/src/exec/operator/Operator.h
internal/core/src/exec/operator/ProjectNode.h
internal/core/src/exec/operator/RandomSampleNode.h
internal/core/src/exec/operator/Utils.h
internal/core/src/exec/operator/query-agg/AggregateUtil.h
internal/core/src/exec/operator/query-agg/MaxAggregateBase.h
internal/core/src/exec/operator/query-agg/MinAggregateBase.h
internal/core/src/exec/operator/query-agg/RowContainer.h
internal/core/src/exec/operator/query-agg/SimpleNumericAggregate.h
internal/core/src/exec/operator/query-agg/SumAggregateBase.h
internal/core/src/expr/ITypeExpr.h
internal/core/src/futures/Executor.h
internal/core/src/futures/Future.h
internal/core/src/futures/LeakyResult.h
internal/core/src/futures/Ready.h
internal/core/src/futures/future_c_types.h
internal/core/src/index/BitmapIndex.h
internal/core/src/index/BoolIndex.h
internal/core/src/index/Index.h
internal/core/src/index/IndexInfo.h
internal/core/src/index/IndexStructure.h
internal/core/src/index/InvertedIndexUtil.h
internal/core/src/index/JsonFlatIndex.h
internal/core/src/index/JsonInvertedIndex.h
internal/core/src/index/Meta.h
internal/core/src/index/NgramInvertedIndex.h
internal/core/src/index/RTreeIndexSerialization.h
internal/core/src/index/ScalarIndex.h
internal/core/src/index/SkipIndex.h
internal/core/src/index/StringIndex.h
internal/core/src/index/StringIndexMarisa.h
internal/core/src/index/StringIndexSort.h
internal/core/src/index/TextMatchIndex.h
internal/core/src/index/Utils.h
internal/core/src/index/VectorDiskIndex.h
internal/core/src/index/VectorIndex.h
internal/core/src/index/json_stats/JsonKeyStats.h
internal/core/src/index/json_stats/bson_inverted.h
internal/core/src/index/json_stats/utils.h
internal/core/src/index/skipindex_stats/utils.h
internal/core/src/indexbuilder/IndexCreatorBase.h
internal/core/src/indexbuilder/IndexFactory.h
internal/core/src/indexbuilder/init_c.h
internal/core/src/indexbuilder/type_c.h
internal/core/src/indexbuilder/types.h
internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.h
internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.h
internal/core/src/minhash/fusion_compute/fusion_compute_native.h
internal/core/src/mmap/ChunkData.h
internal/core/src/mmap/ChunkVector.h
internal/core/src/mmap/ChunkedColumn.h
internal/core/src/mmap/ChunkedColumnGroup.h
internal/core/src/mmap/ChunkedColumnInterface.h
internal/core/src/mmap/Types.h
internal/core/src/monitor/Monitor.h
internal/core/src/monitor/jemalloc_stats_c.h
internal/core/src/monitor/monitor_c.h
internal/core/src/monitor/scope_metric.h
internal/core/src/pb/cgo_msg.pb.h
internal/core/src/pb/clustering.pb.h
internal/core/src/pb/common.pb.h
internal/core/src/pb/index_cgo_msg.pb.h
internal/core/src/pb/plan.pb.h
internal/core/src/pb/schema.pb.h
internal/core/src/pb/segcore.pb.h
internal/core/src/plan/PlanNode.h
internal/core/src/plan/PlanNodeIdGenerator.h
internal/core/src/query/ExecPlanNodeVisitor.h
internal/core/src/query/Plan.h
internal/core/src/query/PlanImpl.h
internal/core/src/query/PlanNode.h
internal/core/src/query/PlanNodeVisitor.h
internal/core/src/query/PlanProto.h
internal/core/src/query/Relational.h
internal/core/src/query/ScalarIndex.h
internal/core/src/query/SearchOnGrowing.h
internal/core/src/query/SearchOnIndex.h
internal/core/src/query/SearchOnSealed.h
internal/core/src/query/SubSearchResult.h
internal/core/src/query/Utils.h
internal/core/src/query/helper.h
internal/core/src/rescores/Murmur3.h
internal/core/src/rescores/Scorer.h
internal/core/src/rescores/Utils.h
internal/core/src/segcore/AckResponder.h
internal/core/src/segcore/ChunkedSegmentSealedImpl.h
internal/core/src/segcore/ConcurrentVector.h
internal/core/src/segcore/DeletedRecord.h
internal/core/src/segcore/InsertRecord.h
internal/core/src/segcore/Record.h
internal/core/src/segcore/ReduceStructure.h
internal/core/src/segcore/SealedIndexingRecord.h
internal/core/src/segcore/SegcoreConfig.h
internal/core/src/segcore/SegmentChunkReader.h
internal/core/src/segcore/SegmentGrowing.h
internal/core/src/segcore/SegmentGrowingImpl.h
internal/core/src/segcore/SegmentInterface.h
internal/core/src/segcore/SegmentSealed.h
internal/core/src/segcore/TimestampIndex.h
internal/core/src/segcore/Types.h
internal/core/src/segcore/Utils.h
internal/core/src/segcore/check_vec_index_c.h
internal/core/src/segcore/load_index_c.h
internal/core/src/segcore/memory_planner.h
internal/core/src/segcore/minhash_c.h
internal/core/src/segcore/pkVisitor.h
internal/core/src/segcore/reduce/Reduce.h
internal/core/src/segcore/reduce/StreamReduce.h
internal/core/src/segcore/reduce_c.h
internal/core/src/segcore/segment_c.h
internal/core/src/segcore/storagev1translator/BsonInvertedIndexTranslator.h
internal/core/src/segcore/storagev1translator/ChunkTranslator.h
internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.h
internal/core/src/segcore/storagev1translator/InterimSealedIndexTranslator.h
internal/core/src/segcore/storagev1translator/SealedIndexTranslator.h
internal/core/src/segcore/storagev1translator/TextMatchIndexTranslator.h
internal/core/src/segcore/storagev1translator/V1SealedIndexTranslator.h
internal/core/src/segcore/storagev2translator/GroupCTMeta.h
internal/core/src/segcore/storagev2translator/GroupChunkTranslator.h
internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.h
internal/core/src/storage/ChunkManager.h
internal/core/src/storage/DiskFileManagerImpl.h
internal/core/src/storage/FileManager.h
internal/core/src/storage/KeyRetriever.h
internal/core/src/storage/LocalChunkManager.h
internal/core/src/storage/LocalChunkManagerSingleton.h
internal/core/src/storage/MemFileManagerImpl.h
internal/core/src/storage/MmapManager.h
internal/core/src/storage/PluginLoader.h
internal/core/src/storage/RemoteChunkManagerSingleton.h
internal/core/src/storage/RemoteInputStream.h
internal/core/src/storage/RemoteOutputStream.h
internal/core/src/storage/SafeQueue.h
internal/core/src/storage/StorageV2FSCache.h
internal/core/src/storage/ThreadPools.h
internal/core/src/storage/Types.h
internal/core/src/storage/Util.h
internal/core/src/storage/gcp-native-storage/GcpNativeChunkManager.h
internal/core/src/storage/gcp-native-storage/GcpNativeClientManager.h
internal/core/src/storage/loon_ffi/property_singleton.h
internal/core/src/storage/loon_ffi/util.h
internal/core/src/storage/minio/MinioChunkManager.h
internal/core/src/storage/opendal/OpenDALChunkManager.h
internal/core/src/storage/parquet_c.h
internal/core/src/storage/plugin/PluginInterface.h
```

## Other Modified Files

```
internal/core/milvus.imp
internal/core/run_iwyu.sh
```
