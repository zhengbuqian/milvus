# IWYU Progress Tracking

This file tracks the progress of header file Include-What-You-Use (IWYU) cleanup for the Milvus C++ codebase.

Legend:
- `[ ]` - Not processed yet
- `[x]` - Processed and completed
- `[!]` - Has issues / needs attention

---

## C++ Source Files (.cpp)

### bitset
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/instruction_set.cpp (not in compile_commands.json - ARM only)
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/neon-inst.cpp (not in compile_commands.json - ARM only)
- [!] /home/zilliz/milvus/internal/core/src/bitset/detail/platform/arm/sve-inst.cpp (not in compile_commands.json - ARM only)

### clustering
- [!] /home/zilliz/milvus/internal/core/src/clustering/KmeansClusteringTest.cpp (not in compile_commands.json)

### minhash
- [!] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.cpp (not in compile_commands.json - ARM only)
- [!] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.cpp (not in compile_commands.json - ARM only)

### mmap
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnGroupTest.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnTest.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkVectorTest.cpp (not in compile_commands.json)

### monitor
- [!] /home/zilliz/milvus/internal/core/src/monitor/MonitorTest.cpp (not in compile_commands.json)

### plan

### query
- [!] /home/zilliz/milvus/internal/core/src/query/CachedSearchIteratorTest.cpp (not in compile_commands.json)

### storage
- [!] /home/zilliz/milvus/internal/core/src/storage/ChunkManager.cpp (IWYU adds duplicate HuaweiCloudCredentialsProvider.h include causing redefinition error)
- [!] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManager.cpp (IWYU removes sstream causing incomplete type error for std::stringstream)
- [!] /home/zilliz/milvus/internal/core/src/storage/RemoteChunkManagerTest.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/src/storage/StorageV2FSCache.cpp (IWYU generates incorrect include path for TBB)

### storage/gcp-native-storage
- [!] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManager.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManagerTest.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeClientManager.cpp (not in compile_commands.json)

### storage/minio
- [!] /home/zilliz/milvus/internal/core/src/storage/minio/MinioChunkManagerTest.cpp (not in compile_commands.json)

### storage/opendal
- [!] /home/zilliz/milvus/internal/core/src/storage/opendal/OpenDALChunkManager.cpp (not in compile_commands.json)

### unittest
- [!] /home/zilliz/milvus/internal/core/unittest/test_schema_reopen.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/unittest/test_string_chunk_writer.cpp (not in compile_commands.json)

### unittest/bench
- [!] /home/zilliz/milvus/internal/core/unittest/bench/bench_applyhits.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/unittest/bench/bench_filewrite.cpp (not in compile_commands.json)
- [!] /home/zilliz/milvus/internal/core/unittest/bench/bench_findfirst.cpp (not in compile_commands.json)

### unittest/test_json_stats

---

## Header Files (.h / .hpp)

### bitset

### clustering
- [x] /home/zilliz/milvus/internal/core/src/clustering/file_utils.h
- [!] /home/zilliz/milvus/internal/core/src/clustering/type_c.h (C header, no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/clustering/types.h

### common
- [!] /home/zilliz/milvus/internal/core/src/common/BitsetView.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/common/BitUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/common/BloomFilter.h
- [x] /home/zilliz/milvus/internal/core/src/common/bson_view.h
- [!] /home/zilliz/milvus/internal/core/src/common/CDataType.h (C header, no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/common/CustomBitset.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ElementFilterIterator.h
- [!] /home/zilliz/milvus/internal/core/src/common/File.h (not analyzed - no direct cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/common/float_util_c.h
- [!] /home/zilliz/milvus/internal/core/src/common/Geometry.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/common/init_c.h
- [!] /home/zilliz/milvus/internal/core/src/common/jsmn.h (C header with extern "C", no direct analysis needed)
- [!] /home/zilliz/milvus/internal/core/src/common/JsonCastFunction.h (not analyzed - no direct cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/common/JsonUtils.h (no direct cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/common/logging_c.h (C header, no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/common/PreparedGeometry.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Promise.h
- [ ] /home/zilliz/milvus/internal/core/src/common/protobuf_utils_c.h
- [x] /home/zilliz/milvus/internal/core/src/common/protobuf_utils.h
- [x] /home/zilliz/milvus/internal/core/src/common/QueryResult.h (analyzed via test_sealed.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/common/RangeSearchHelper.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/common/resource_c.h (C header with only struct definition, no includes needed)
- [!] /home/zilliz/milvus/internal/core/src/common/ScopedTimer.h (no direct cpp entry in this batch)
- [ ] /home/zilliz/milvus/internal/core/src/common/SimdUtil.h
- [!] /home/zilliz/milvus/internal/core/src/common/Slice.h (not analyzed - no direct cpp entry)
- [x] /home/zilliz/milvus/internal/core/src/common/type_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ValueOp.h
- [x] /home/zilliz/milvus/internal/core/src/common/Vector.h (analyzed via test_sealed.cpp entry)

### config
- [x] /home/zilliz/milvus/internal/core/src/config/ConfigKnowhere.h (no changes suggested by IWYU)

### futures
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c_types.h
- [!] /home/zilliz/milvus/internal/core/src/futures/Future.h (no direct cpp entry in this batch)
- [ ] /home/zilliz/milvus/internal/core/src/futures/LeakyResult.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/Ready.h

### index
- [x] /home/zilliz/milvus/internal/core/src/index/VectorMemIndex.h (IWYU changes applied)
- [x] /home/zilliz/milvus/internal/core/src/index/json_stats/JsonKeyStats.h (IWYU added explicit includes)

### minhash
- [x] /home/zilliz/milvus/internal/core/src/minhash/MinHashComputer.h (analyzed via test_minhash.cpp entry)
- [x] /home/zilliz/milvus/internal/core/src/minhash/MinHashHook.h (analyzed via test_minhash.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.h
- [x] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/fusion_compute_native.h (analyzed via test_minhash.cpp entry)

### mmap
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkData.h
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnGroup.h (no direct cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumn.h (ChunkedSegmentSealedImpl.cpp has IWYU analysis errors)
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnInterface.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkVector.h
- [!] /home/zilliz/milvus/internal/core/src/mmap/Types.h (no changes suggested by IWYU)

### monitor
- [x] /home/zilliz/milvus/internal/core/src/monitor/jemalloc_stats_c.h (removed unused stdbool.h)
- [!] /home/zilliz/milvus/internal/core/src/monitor/monitor_c.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/monitor/Monitor.h

### plan
- [x] /home/zilliz/milvus/internal/core/src/plan/PlanNode.h
- [x] /home/zilliz/milvus/internal/core/src/plan/PlanNodeIdGenerator.h

### query
- [!] /home/zilliz/milvus/internal/core/src/query/CachedSearchIterator.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/query/helper.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/query/Plan.h (IWYU removes necessary forward declarations, causing compilation errors)
- [!] /home/zilliz/milvus/internal/core/src/query/PlanImpl.h (header not directly analyzed, depends on Plan.h)
- [x] /home/zilliz/milvus/internal/core/src/query/PlanNode.h
- [!] /home/zilliz/milvus/internal/core/src/query/PlanNodeVisitor.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/query/PlanProto.h (added expr/ITypeExpr.h and rescores/Scorer.h to fix build)
- [!] /home/zilliz/milvus/internal/core/src/query/Relational.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/query/SearchBruteForce.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/query/SearchOnGrowing.h
- [x] /home/zilliz/milvus/internal/core/src/query/SearchOnIndex.h
- [x] /home/zilliz/milvus/internal/core/src/query/SearchOnSealed.h
- [x] /home/zilliz/milvus/internal/core/src/query/ExecPlanNodeVisitor.h
- [!] /home/zilliz/milvus/internal/core/src/query/SubSearchResult.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/query/Utils.h (no changes suggested by IWYU)

### rescores
- [!] /home/zilliz/milvus/internal/core/src/rescores/Murmur3.h (no direct cpp entry)
- [x] /home/zilliz/milvus/internal/core/src/rescores/Scorer.h (updated with additional includes via IWYU)
- [!] /home/zilliz/milvus/internal/core/src/rescores/Utils.h (no direct cpp entry)

### segcore
- [!] /home/zilliz/milvus/internal/core/src/segcore/AckResponder.h (no direct cpp entry in this batch)
- [!] /home/zilliz/milvus/internal/core/src/segcore/arrow_fs_c.h (ChunkedSegmentSealedImpl.cpp has IWYU analysis errors)
- [x] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedImpl.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/collection_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/column_groups_c.h (C header, no direct analysis needed)
- [!] /home/zilliz/milvus/internal/core/src/segcore/ConcurrentVector.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/DeletedRecord.h (ChunkedSegmentSealedImpl.cpp has IWYU analysis errors)
- [x] /home/zilliz/milvus/internal/core/src/segcore/InsertRecord.h (analyzed via test_sealed.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/segcore/load_field_data_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/memory_planner.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/metrics_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/minhash_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/packed_reader_c.h (C header, no direct analysis needed)
- [x] /home/zilliz/milvus/internal/core/src/segcore/packed_writer_c.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/phrase_match_c.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/pkVisitor.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/plan_c.h (C header, no direct analysis needed)
- [!] /home/zilliz/milvus/internal/core/src/segcore/Record.h (no direct cpp entry in this batch)
- [x] /home/zilliz/milvus/internal/core/src/segcore/reduce_c.h (IWYU changes applied: added stdint.h and common/common_type_c.h)
- [x] /home/zilliz/milvus/internal/core/src/segcore/ReduceStructure.h (analyzed via test_search_group_by.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/segcore/ReduceUtils.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/SegcoreConfig.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/segcore_init_c.h (no changes suggested by IWYU via SegcoreInitCTest.cpp)
- [x] /home/zilliz/milvus/internal/core/src/segcore/segment_c.h (reorganized includes via IWYU analysis)
- [x] /home/zilliz/milvus/internal/core/src/segcore/SegmentChunkReader.h (IWYU changes applied)
- [!] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowing.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/SegmentSealed.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/tokenizer_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/token_stream_c.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/Types.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/Utils.h (no changes suggested by IWYU)

### segcore/reduce
- [!] /home/zilliz/milvus/internal/core/src/segcore/reduce/GroupReduce.h (no direct cpp entry in this batch)
- [x] /home/zilliz/milvus/internal/core/src/segcore/reduce/Reduce.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/reduce/StreamReduce.h (no direct cpp entry in this batch)

### segcore/storagev1translator
- [x] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/ChunkTranslator.h (analyzed via test_sealed.cpp entry)
- [x] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.h (IWYU changes applied)

### segcore/storagev2translator
- [!] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupChunkTranslator.h (no direct cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupCTMeta.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.h (IWYU changes applied)

### storage
- [x] /home/zilliz/milvus/internal/core/src/storage/BinlogReader.h
- [x] /home/zilliz/milvus/internal/core/src/storage/ChunkManager.h
- [!] /home/zilliz/milvus/internal/core/src/storage/DataCodec.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/DiskFileManagerImpl.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/Event.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/FileManager.h
- [!] /home/zilliz/milvus/internal/core/src/storage/FileWriter.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/InsertData.h (analyzed via test_sealed.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/storage/KeyRetriever.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManager.h
- [x] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManagerSingleton.h
- [!] /home/zilliz/milvus/internal/core/src/storage/MemFileManagerImpl.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/MmapChunkManager.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/MmapManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/parquet_c.h
- [x] /home/zilliz/milvus/internal/core/src/storage/PayloadReader.h (analyzed via test_sealed.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/storage/PayloadStream.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/PayloadWriter.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/PluginLoader.h (header-only file, no corresponding cpp, includes appear reasonable)
- [x] /home/zilliz/milvus/internal/core/src/storage/RemoteChunkManagerSingleton.h
- [x] /home/zilliz/milvus/internal/core/src/storage/RemoteInputStream.h
- [x] /home/zilliz/milvus/internal/core/src/storage/RemoteOutputStream.h
- [!] /home/zilliz/milvus/internal/core/src/storage/SafeQueue.h (no changes suggested by IWYU - template header)
- [x] /home/zilliz/milvus/internal/core/src/storage/storage_c.h
- [!] /home/zilliz/milvus/internal/core/src/storage/StorageV2FSCache.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/ThreadPool.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/ThreadPools.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/Types.h
- [x] /home/zilliz/milvus/internal/core/src/storage/Util.h

### storage/aliyun
- [x] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunCredentialsProvider.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/storage/aliyun/AliyunSTSClient.h (not analyzed - no direct cpp entry)

### storage/azure
- [!] /home/zilliz/milvus/internal/core/src/storage/azure/AzureBlobChunkManager.h (not analyzed - AZURE_BUILD_DIR flag dependent)
- [!] /home/zilliz/milvus/internal/core/src/storage/azure/AzureChunkManager.h (not analyzed - AZURE_BUILD_DIR flag dependent)

### storage/gcp-native-storage
- [!] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeChunkManager.h (not analyzed - ENABLE_GCP_NATIVE flag dependent)
- [ ] /home/zilliz/milvus/internal/core/src/storage/gcp-native-storage/GcpNativeClientManager.h

### storage/huawei
- [x] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudCredentialsProvider.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/huawei/HuaweiCloudSTSClient.h (no changes suggested by IWYU)

### storage/loon_ffi
- [!] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_reader_c.h (C header, no direct analysis needed)
- [x] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/ffi_writer_c.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/property_singleton.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/loon_ffi/util.h (IWYU changes applied)

### storage/minio
- [x] /home/zilliz/milvus/internal/core/src/storage/minio/MinioChunkManager.h (IWYU changes applied)

### storage/opendal
- [!] /home/zilliz/milvus/internal/core/src/storage/opendal/OpenDALChunkManager.h (not analyzed - USE_OPENDAL flag dependent)

### storage/plugin
- [x] /home/zilliz/milvus/internal/core/src/storage/plugin/PluginInterface.h (no changes suggested by IWYU)

### storage/tencent
- [x] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudCredentialsProvider.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/tencent/TencentCloudSTSClient.h (no changes suggested by IWYU)

### unittest/test_utils
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/AssertUtils.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/cachinglayer_test_utils.h (analyzed via test_sealed.cpp entry)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/c_api_test_utils.h (analyzed via test_exec.cpp entry)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/Constants.h
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/DataGen.h (large file, not directly analyzed)
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/Distance.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/PbHelper.h (no direct cpp entry)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/storage_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Timer.h
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/TmpPath.h (no direct cpp entry)
