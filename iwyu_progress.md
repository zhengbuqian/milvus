# IWYU Progress Tracking

This file tracks the progress of Include-What-You-Use (IWYU) cleanup for the Milvus C++ codebase.

Legend:
- `[ ]` - Not processed yet
- `[x]` - Processed and completed
- `[!]` - Has issues / needs attention

---

## C++ Source Files (.cpp)

### bitset
- [x] /home/zilliz/milvus/internal/core/src/bitset/BitsetTest.cpp
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/instruction_set.cpp (not in compile_commands.json - ARM only)
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/neon-inst.cpp (not in compile_commands.json - ARM only)
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/sve-inst.cpp (not in compile_commands.json - ARM only)
- [x] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/dynamic.cpp
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx2-inst.cpp (IWYU suggestions cause compilation errors - template instantiation requires includes)
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx512-inst.cpp (IWYU suggestions cause compilation errors - template instantiation requires includes)
- [x] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/instruction_set.cpp

### clustering
- [x] /home/zilliz/milvus/internal/core/src/clustering/analyze_c.cpp
- [x] /home/zilliz/milvus/internal/core/src/clustering/KmeansClustering.cpp
- [!] /home/zilliz/milvus/internal/core/src/clustering/KmeansClusteringTest.cpp (not in compile_commands.json)

### common
- [x] /home/zilliz/milvus/internal/core/src/common/ArrayOffsets.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ArrayOffsetsTest.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ArrayTest.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/binary_set_c.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/BitmapTest.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/BloomFilterTest.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/Chunk.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ChunkTarget.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ChunkTest.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ChunkWriter.cpp
- [x] /home/zilliz/milvus/internal/core/src/common/ChunkWriterTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/Common.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/ComplexVector.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/ElementFilterIterator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/FieldData.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/FieldMeta.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/IndexMeta.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/init_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonCastFunction.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonCastType.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonUtils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/logging_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/OffsetMapping.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/PreparedGeometryTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/protobuf_utils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/ProtobufUtilsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/RangeSearchHelper.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/RangeSearchHelperTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/RegexQuery.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/RegexQueryTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/RegexQueryUtilTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/Schema.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/SchemaTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/Slice.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/SpanTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/SystemProperty.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/ThreeValuedLogicOpTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/Types.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/TypesTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/UtilsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorArrayChunkTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorArrayStorageV2Test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorArrayTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorTest.cpp

### config
- [ ] /home/zilliz/milvus/internal/core/src/config/ConfigKnowhere.cpp

### exec
- [ ] /home/zilliz/milvus/internal/core/src/exec/Driver.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/HashTable.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/Task.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/VectorHasher.cpp

### exec/expression
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/AlwaysTrueExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/AlwaysTrueExprTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/BinaryArithOpEvalRangeExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/BinaryRangeExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/CallExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ColumnExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/CompareExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ConjunctExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExistsExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/Expr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprArithMiscTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprArithOpTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprArrayTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprCache.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprCacheTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprCompareTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprGISMiscTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprJsonAdvancedTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprJsonContainsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprJsonIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprJsonRangeTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprRangeTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprTermTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/GISFunctionFilterExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/JsonContainsByStatsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/JsonContainsExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LikeConjunctExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LikeConjunctExprTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LogicalBinaryExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LogicalUnaryExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/MatchExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/MatchExprTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/NullExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/TermExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/TimestamptzArithCompareExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/UnaryExpr.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ValueExpr.cpp

### exec/expression/function
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/FunctionFactory.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/FunctionImplUtils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/FunctionTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/impl/Empty.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/impl/StartsWith.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/init_c.cpp

### exec/operator
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/AggregationNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ElementFilterBitsNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ElementFilterNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/FilterBitsNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/IterativeFilterNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/MvccNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/Operator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ProjectNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RandomSampleNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RandomSampleNodeTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RescoresNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RescoresNodeTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/SearchGroupByNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/VectorSearchNode.cpp

### exec/operator/query-agg
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/Aggregate.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/AggregateInfo.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/CountAggregateBase.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/GroupingSet.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/MaxAggregate.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/MinAggregate.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/RowContainer.cpp
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/SumAggregate.cpp

### exec/operator/search-groupby
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.cpp

### futures
- [ ] /home/zilliz/milvus/internal/core/src/futures/Executor.cpp
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_test_case_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/futures/FutureTest.cpp

### index
- [ ] /home/zilliz/milvus/internal/core/src/index/BitmapIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/BitmapIndexArrayTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/BitmapIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/BoolIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/HybridScalarIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/HybridScalarIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexFactory.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexStats.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/InvertedIndexArrayTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/InvertedIndexTantivy.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/InvertedIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonFlatIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonFlatIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonIndexBuilder.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonInvertedIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonInvertedIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/NgramInvertedIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/NgramInvertedIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndexWrapper.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndexWrapperTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndexSort.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndexSortTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/SkipIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexMarisa.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexSort.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexSortTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/TextMatchIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/TextMatchIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/Utils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/UtilsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/VectorDiskIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/VectorMemIndex.cpp

### index/json_stats
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/bson_builder.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/bson_inverted.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/JsonKeyStats.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/parquet_writer.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/utils.cpp

### index/skipindex_stats
- [ ] /home/zilliz/milvus/internal/core/src/index/skipindex_stats/SkipIndexStats.cpp
- [ ] /home/zilliz/milvus/internal/core/src/index/skipindex_stats/SkipIndexStatsTest.cpp

### indexbuilder
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/index_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/init_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/ScalarIndexCreator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/ScalarIndexCreatorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/VecIndexCreator.cpp

### minhash
- [ ] /home/zilliz/milvus/internal/core/src/minhash/MinHashComputer.cpp
- [ ] /home/zilliz/milvus/internal/core/src/minhash/MinHashHook.cpp
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.cpp
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.cpp
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx2.cpp
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx512.cpp

### mmap
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnGroupTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkVectorTest.cpp

### monitor
- [ ] /home/zilliz/milvus/internal/core/src/monitor/jemalloc_stats_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/monitor/monitor_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/monitor/Monitor.cpp
- [ ] /home/zilliz/milvus/internal/core/src/monitor/MonitorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/monitor/scope_metric.cpp

### plan
- [ ] /home/zilliz/milvus/internal/core/src/plan/PlanNode.cpp

### query
- [ ] /home/zilliz/milvus/internal/core/src/query/CachedSearchIterator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/CachedSearchIteratorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/ExecPlanNodeVisitor.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/Plan.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanNode.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanProto.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanProtoTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/RelationalTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchBruteForce.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchBruteForceSparseTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchBruteForceTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnGrowing.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnSealed.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SubSearchResult.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/SubSearchResultTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/query/UtilsTest.cpp

### rescores
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Scorer.cpp

### segcore
- [ ] /home/zilliz/milvus/internal/core/src/segcore/arrow_fs_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ArrowFsCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/check_vec_index_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedBinlogIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedStorageV2Test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/collection_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Collection.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/collection_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/column_groups_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ColumnGroupsCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ConcurrentVector.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ConcurrentVectorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/DeletedRecordTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/FieldIndexing.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/IndexConfigGenerator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/IndexCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/InsertRecordOffsetOrderedArrayTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/InsertRecordOffsetOrderedMapTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/LoadCancellationTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_field_data_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_field_data_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_index_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_index_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/memory_planner.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/MemoryPlannerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/metrics_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/minhash_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_reader_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/PackedReaderWriterTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_writer_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/phrase_match_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/plan_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/plan_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceStructureTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceUtils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegcoreConfig.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segcore_init_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegcoreInitCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segment_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentChunkReader.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segment_c_test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingImpl.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingStorageV2Test.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentInterface.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentLoadInfoTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentSealedRetrieveTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/TimestampIndex.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/TimestampIndexTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/tokenizer_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/TokenizerCTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/token_stream_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Utils.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/UtilsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/vector_index_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/vector_index_c_test.cpp

### segcore/reduce
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/GroupReduce.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/Reduce.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/StreamReduce.cpp

### segcore/storagev1translator
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/BsonInvertedIndexTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/ChunkTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslatorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/InterimSealedIndexTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/SealedIndexTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/TextMatchIndexTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/V1SealedIndexTranslator.cpp

### segcore/storagev2translator
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupChunkTranslator.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupChunkTranslatorTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp

### storage
- [ ] /home/zilliz/milvus/internal/core/src/storage/BinlogReader.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/ChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/DataCodec.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/DataCodecTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/DiskFileManagerImpl.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/DiskFileManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/Event.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/FileWriter.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/FileWriterTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/IndexData.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/InsertData.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/KeyRetriever.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/MemFileManagerImpl.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/MmapChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/MmapChunkManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadReader.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadStream.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadWriter.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteChunkManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteInputStream.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteOutputStream.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/storage_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/StorageV2FSCache.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPool.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPools.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPoolsTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPoolTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/Util.cpp

### storage/aliyun
- [ ] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunCredentialsProvider.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunSTSClient.cpp

### storage/azure
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureBlobChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureBlobChunkManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureChunkManagerTest.cpp

### storage/gcp-native-storage
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManagerTest.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeClientManager.cpp

### storage/huawei
- [ ] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudCredentialsProvider.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudSTSClient.cpp

### storage/loon_ffi
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_reader_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_writer_c.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/util.cpp

### storage/minio
- [ ] /home/zilliz/milvus/internal/core/src/storage/minio/MinioChunkManager.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/minio/MinioChunkManagerTest.cpp

### storage/opendal
- [ ] /home/zilliz/milvus/internal/core/src/storage/opendal/OpenDALChunkManager.cpp

### storage/tencent
- [ ] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudCredentialsProvider.cpp
- [ ] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudSTSClient.cpp

### unittest
- [ ] /home/zilliz/milvus/internal/core/unittest/init_gtest.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_element_filter.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_exec.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_expr_materialized_view.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_float16.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_group_by_json.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_indexing.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_index_wrapper.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_integer_overflow.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_iterative_filter.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_loading.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_minhash.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_query.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_query_group_by.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_rust_result.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_schema_reopen.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_scorer.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_sealed.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_search_group_by.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_storage.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_storage_v2_index_raw_data.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_string_chunk_writer.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_string_expr.cpp

### unittest/bench
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_applyhits.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_filewrite.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_findfirst.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_indexbuilder.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_naive.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_prepared_geometry.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/bench/bench_search.cpp

### unittest/test_json_stats
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_bson_builder.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_bson_view.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_json_key_stats.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_parquet_writer.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_traverse_json_for_build_stats.cpp
- [ ] /home/zilliz/milvus/internal/core/unittest/test_json_stats/test_utils.cpp

---

## Header Files (.h / .hpp)

### bitset
- [ ] /home/zilliz/milvus/internal/core/src/bitset/bitset.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/common.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/bit_wise.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/ctz.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/element_vectorized.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/element_wise.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/maybe_vector.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/instruction_set.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/neon-decl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/neon.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/neon-impl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/sve-decl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/sve.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/sve-impl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/dynamic.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/vectorized_ref.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx2-decl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx2.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx2-impl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx512-decl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx512.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/avx512-impl.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/common.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/x86/instruction_set.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/popcount.h
- [ ] /home/zilliz/milvus/internal/core/src/bitset/detail/proxy.h

### clustering
- [ ] /home/zilliz/milvus/internal/core/src/clustering/analyze_c.h
- [ ] /home/zilliz/milvus/internal/core/src/clustering/file_utils.h
- [ ] /home/zilliz/milvus/internal/core/src/clustering/KmeansClustering.h
- [ ] /home/zilliz/milvus/internal/core/src/clustering/type_c.h
- [ ] /home/zilliz/milvus/internal/core/src/clustering/types.h

### common
- [ ] /home/zilliz/milvus/internal/core/src/common/Array.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ArrayOffsets.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ArrowDataWrapper.h
- [ ] /home/zilliz/milvus/internal/core/src/common/binary_set_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/BitsetView.h
- [ ] /home/zilliz/milvus/internal/core/src/common/BitUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/common/BloomFilter.h
- [ ] /home/zilliz/milvus/internal/core/src/common/bson_view.h
- [ ] /home/zilliz/milvus/internal/core/src/common/CDataType.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Channel.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Chunk.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ChunkTarget.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ChunkWriter.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Common.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Consts.h
- [ ] /home/zilliz/milvus/internal/core/src/common/CustomBitset.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ElementFilterIterator.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Exception.h
- [ ] /home/zilliz/milvus/internal/core/src/common/FieldData.h
- [ ] /home/zilliz/milvus/internal/core/src/common/FieldDataInterface.h
- [ ] /home/zilliz/milvus/internal/core/src/common/FieldMeta.h
- [ ] /home/zilliz/milvus/internal/core/src/common/File.h
- [ ] /home/zilliz/milvus/internal/core/src/common/float_util_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/GeometryCache.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Geometry.h
- [ ] /home/zilliz/milvus/internal/core/src/common/GroupChunk.h
- [ ] /home/zilliz/milvus/internal/core/src/common/IndexMeta.h
- [ ] /home/zilliz/milvus/internal/core/src/common/init_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/jsmn.h
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonCastFunction.h
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonCastType.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Json.h
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonUtils.h
- [ ] /home/zilliz/milvus/internal/core/src/common/LoadInfo.h
- [ ] /home/zilliz/milvus/internal/core/src/common/logging_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/OffsetMapping.h
- [ ] /home/zilliz/milvus/internal/core/src/common/PreparedGeometry.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Promise.h
- [ ] /home/zilliz/milvus/internal/core/src/common/protobuf_utils_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/protobuf_utils.h
- [ ] /home/zilliz/milvus/internal/core/src/common/QueryInfo.h
- [ ] /home/zilliz/milvus/internal/core/src/common/QueryResult.h
- [ ] /home/zilliz/milvus/internal/core/src/common/RangeSearchHelper.h
- [ ] /home/zilliz/milvus/internal/core/src/common/RegexQuery.h
- [ ] /home/zilliz/milvus/internal/core/src/common/resource_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Schema.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ScopedTimer.h
- [ ] /home/zilliz/milvus/internal/core/src/common/SimdUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Slice.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Span.h
- [ ] /home/zilliz/milvus/internal/core/src/common/SystemProperty.h
- [ ] /home/zilliz/milvus/internal/core/src/common/type_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Types.h
- [ ] /home/zilliz/milvus/internal/core/src/common/TypeTraits.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Utils.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ValueOp.h
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorArray.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Vector.h
- [ ] /home/zilliz/milvus/internal/core/src/common/VectorTrait.h

### config
- [ ] /home/zilliz/milvus/internal/core/src/config/ConfigKnowhere.h

### exec
- [ ] /home/zilliz/milvus/internal/core/src/exec/Driver.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/HashTable.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/QueryContext.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/Task.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/VectorHasher.h

### exec/expression
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/AlwaysTrueExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/BinaryArithOpEvalRangeExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/BinaryRangeExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/CallExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ColumnExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/CompareExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ConjunctExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/Element.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/EvalCtx.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExistsExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprCache.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/Expr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ExprTestBase.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/GISFunctionFilterExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/JsonContainsExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LikeConjunctExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LogicalBinaryExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/LogicalUnaryExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/MatchExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/NullExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/TermExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/TimestamptzArithCompareExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/UnaryExpr.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/Utils.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/ValueExpr.h

### exec/expression/function
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/FunctionFactory.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/FunctionImplUtils.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/impl/StringFunctions.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/expression/function/init_c.h

### exec/operator
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/AggregationNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/CallbackSink.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ElementFilterBitsNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ElementFilterNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/FilterBitsNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/IterativeFilterNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/MvccNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/Operator.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/ProjectNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RandomSampleNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/RescoresNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/SearchGroupByNode.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/Utils.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/VectorSearchNode.h

### exec/operator/query-agg
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/Aggregate.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/AggregateInfo.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/AggregateUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/CountAggregateBase.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/GroupingSet.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/MaxAggregateBase.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/MinAggregateBase.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/RowContainer.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/SimpleNumericAggregate.h
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/query-agg/SumAggregateBase.h

### exec/operator/search-groupby
- [ ] /home/zilliz/milvus/internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.h

### expr
- [ ] /home/zilliz/milvus/internal/core/src/expr/ITypeExpr.h

### futures
- [ ] /home/zilliz/milvus/internal/core/src/futures/Executor.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c_types.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/Future.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/LeakyResult.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/Ready.h

### index
- [ ] /home/zilliz/milvus/internal/core/src/index/BitmapIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/BoolIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/HybridScalarIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexFactory.h
- [ ] /home/zilliz/milvus/internal/core/src/index/Index.h
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexInfo.h
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexStats.h
- [ ] /home/zilliz/milvus/internal/core/src/index/IndexStructure.h
- [ ] /home/zilliz/milvus/internal/core/src/index/InvertedIndexTantivy.h
- [ ] /home/zilliz/milvus/internal/core/src/index/InvertedIndexUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonFlatIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonIndexBuilder.h
- [ ] /home/zilliz/milvus/internal/core/src/index/JsonInvertedIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/Meta.h
- [ ] /home/zilliz/milvus/internal/core/src/index/NgramInvertedIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndexSerialization.h
- [ ] /home/zilliz/milvus/internal/core/src/index/RTreeIndexWrapper.h
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/ScalarIndexSort.h
- [ ] /home/zilliz/milvus/internal/core/src/index/SkipIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexMarisa.h
- [ ] /home/zilliz/milvus/internal/core/src/index/StringIndexSort.h
- [ ] /home/zilliz/milvus/internal/core/src/index/TextMatchIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/Utils.h
- [ ] /home/zilliz/milvus/internal/core/src/index/VectorDiskIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/VectorIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/index/VectorMemIndex.h

### index/json_stats
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/bson_builder.h
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/bson_inverted.h
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/JsonKeyStats.h
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/parquet_writer.h
- [ ] /home/zilliz/milvus/internal/core/src/index/json_stats/utils.h

### index/skipindex_stats
- [ ] /home/zilliz/milvus/internal/core/src/index/skipindex_stats/SkipIndexStats.h
- [ ] /home/zilliz/milvus/internal/core/src/index/skipindex_stats/utils.h

### indexbuilder
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/index_c.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/IndexCreatorBase.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/IndexFactory.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/init_c.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/ScalarIndexCreator.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/type_c.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/types.h
- [ ] /home/zilliz/milvus/internal/core/src/indexbuilder/VecIndexCreator.h

### minhash
- [ ] /home/zilliz/milvus/internal/core/src/minhash/MinHashComputer.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/MinHashHook.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/fusion_compute_native.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx2.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/x86/fusion_compute_avx512.h

### mmap
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkData.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnGroup.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumn.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnInterface.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkVector.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/Types.h

### monitor
- [ ] /home/zilliz/milvus/internal/core/src/monitor/jemalloc_stats_c.h
- [ ] /home/zilliz/milvus/internal/core/src/monitor/monitor_c.h
- [ ] /home/zilliz/milvus/internal/core/src/monitor/Monitor.h
- [ ] /home/zilliz/milvus/internal/core/src/monitor/scope_metric.h

### plan
- [ ] /home/zilliz/milvus/internal/core/src/plan/PlanNode.h
- [ ] /home/zilliz/milvus/internal/core/src/plan/PlanNodeIdGenerator.h

### query
- [ ] /home/zilliz/milvus/internal/core/src/query/CachedSearchIterator.h
- [ ] /home/zilliz/milvus/internal/core/src/query/ExecPlanNodeVisitor.h
- [ ] /home/zilliz/milvus/internal/core/src/query/helper.h
- [ ] /home/zilliz/milvus/internal/core/src/query/Plan.h
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanNode.h
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanNodeVisitor.h
- [ ] /home/zilliz/milvus/internal/core/src/query/PlanProto.h
- [ ] /home/zilliz/milvus/internal/core/src/query/Relational.h
- [ ] /home/zilliz/milvus/internal/core/src/query/ScalarIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchBruteForce.h
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnGrowing.h
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnSealed.h
- [ ] /home/zilliz/milvus/internal/core/src/query/SubSearchResult.h
- [ ] /home/zilliz/milvus/internal/core/src/query/Utils.h

### rescores
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Murmur3.h
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Scorer.h
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Utils.h

### segcore
- [ ] /home/zilliz/milvus/internal/core/src/segcore/AckResponder.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/arrow_fs_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/check_vec_index_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/collection_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Collection.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/column_groups_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ConcurrentVector.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/DeletedRecord.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/FieldIndexing.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/IndexConfigGenerator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/InsertRecord.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_field_data_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_index_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/memory_planner.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/metrics_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/minhash_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_reader_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_writer_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/phrase_match_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/pkVisitor.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/plan_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Record.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceStructure.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceUtils.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SealedIndexingRecord.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegcoreConfig.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segcore_init_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segment_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentChunkReader.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowing.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentInterface.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentSealed.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/TimestampIndex.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/tokenizer_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/token_stream_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Types.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Utils.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/vector_index_c.h

### segcore/reduce
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/GroupReduce.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/Reduce.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/StreamReduce.h

### segcore/storagev1translator
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/BsonInvertedIndexTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/ChunkTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/InterimSealedIndexTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/SealedIndexTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/TextMatchIndexTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/V1SealedIndexTranslator.h

### segcore/storagev2translator
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupChunkTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupCTMeta.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.h

### storage
- [ ] /home/zilliz/milvus/internal/core/src/storage/BinlogReader.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/ChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/DataCodec.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/DiskFileManagerImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/Event.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/FileManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/FileWriter.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/IndexData.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/InsertData.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/KeyRetriever.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManagerSingleton.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/MemFileManagerImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/MmapChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/MmapManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/parquet_c.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadReader.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadWriter.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PluginLoader.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteChunkManagerSingleton.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteInputStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteOutputStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/SafeQueue.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/storage_c.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/StorageV2FSCache.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPool.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPools.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/Types.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/Util.h

### storage/aliyun
- [ ] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunCredentialsProvider.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunSTSClient.h

### storage/azure
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureBlobChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/azure/AzureChunkManager.h

### storage/gcp-native-storage
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeClientManager.h

### storage/huawei
- [ ] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudCredentialsProvider.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudSTSClient.h

### storage/loon_ffi
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_reader_c.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_writer_c.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/property_singleton.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/util.h

### storage/minio
- [ ] /home/zilliz/milvus/internal/core/src/storage/minio/MinioChunkManager.h

### storage/opendal
- [ ] /home/zilliz/milvus/internal/core/src/storage/opendal/OpenDALChunkManager.h

### storage/plugin
- [ ] /home/zilliz/milvus/internal/core/src/storage/plugin/PluginInterface.h

### storage/tencent
- [ ] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudCredentialsProvider.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudSTSClient.h

### unittest/test_utils
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/AssertUtils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/cachinglayer_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/c_api_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Constants.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/DataGen.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Distance.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/GenExprProto.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/indexbuilder_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/PbHelper.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/storage_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Timer.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/TmpPath.h

---

## Summary

- **Total .cpp files**: 374
- **Total .h/.hpp files**: 353
- **Grand Total**: 727 files
