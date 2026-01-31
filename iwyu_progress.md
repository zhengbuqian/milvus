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
- [ ] /home/zilliz/milvus/internal/core/src/common/CDataType.h
- [ ] /home/zilliz/milvus/internal/core/src/common/CustomBitset.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ElementFilterIterator.h
- [ ] /home/zilliz/milvus/internal/core/src/common/File.h
- [ ] /home/zilliz/milvus/internal/core/src/common/float_util_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Geometry.h
- [ ] /home/zilliz/milvus/internal/core/src/common/init_c.h
- [!] /home/zilliz/milvus/internal/core/src/common/jsmn.h (C header with extern "C", no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonCastFunction.h
- [ ] /home/zilliz/milvus/internal/core/src/common/JsonUtils.h
- [ ] /home/zilliz/milvus/internal/core/src/common/logging_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/PreparedGeometry.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Promise.h
- [ ] /home/zilliz/milvus/internal/core/src/common/protobuf_utils_c.h
- [x] /home/zilliz/milvus/internal/core/src/common/protobuf_utils.h
- [x] /home/zilliz/milvus/internal/core/src/common/QueryResult.h (analyzed via test_sealed.cpp entry)
- [!] /home/zilliz/milvus/internal/core/src/common/RangeSearchHelper.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/common/resource_c.h (C header with only struct definition, no includes needed)
- [ ] /home/zilliz/milvus/internal/core/src/common/ScopedTimer.h
- [ ] /home/zilliz/milvus/internal/core/src/common/SimdUtil.h
- [ ] /home/zilliz/milvus/internal/core/src/common/Slice.h
- [x] /home/zilliz/milvus/internal/core/src/common/type_c.h
- [ ] /home/zilliz/milvus/internal/core/src/common/ValueOp.h
- [x] /home/zilliz/milvus/internal/core/src/common/Vector.h (analyzed via test_sealed.cpp entry)

### config
- [ ] /home/zilliz/milvus/internal/core/src/config/ConfigKnowhere.h

### futures
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/future_c_types.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/Future.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/LeakyResult.h
- [ ] /home/zilliz/milvus/internal/core/src/futures/Ready.h

### minhash
- [x] /home/zilliz/milvus/internal/core/src/minhash/MinHashComputer.h (analyzed via test_minhash.cpp entry)
- [x] /home/zilliz/milvus/internal/core/src/minhash/MinHashHook.h (analyzed via test_minhash.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_neon.h
- [ ] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/arm/fusion_compute_sve.h
- [x] /home/zilliz/milvus/internal/core/src/minhash/fusion_compute/fusion_compute_native.h (analyzed via test_minhash.cpp entry)

### mmap
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkData.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnGroup.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumn.h
- [!] /home/zilliz/milvus/internal/core/src/mmap/ChunkedColumnInterface.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/mmap/ChunkVector.h
- [ ] /home/zilliz/milvus/internal/core/src/mmap/Types.h

### monitor
- [ ] /home/zilliz/milvus/internal/core/src/monitor/jemalloc_stats_c.h
- [ ] /home/zilliz/milvus/internal/core/src/monitor/monitor_c.h
- [ ] /home/zilliz/milvus/internal/core/src/monitor/Monitor.h

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
- [x] /home/zilliz/milvus/internal/core/src/query/PlanProto.h
- [ ] /home/zilliz/milvus/internal/core/src/query/Relational.h
- [!] /home/zilliz/milvus/internal/core/src/query/SearchBruteForce.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/query/SearchOnGrowing.h
- [x] /home/zilliz/milvus/internal/core/src/query/SearchOnIndex.h
- [x] /home/zilliz/milvus/internal/core/src/query/SearchOnSealed.h
- [x] /home/zilliz/milvus/internal/core/src/query/ExecPlanNodeVisitor.h
- [!] /home/zilliz/milvus/internal/core/src/query/SubSearchResult.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/query/Utils.h (no changes suggested by IWYU)

### rescores
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Murmur3.h
- [x] /home/zilliz/milvus/internal/core/src/rescores/Scorer.h
- [ ] /home/zilliz/milvus/internal/core/src/rescores/Utils.h

### segcore
- [ ] /home/zilliz/milvus/internal/core/src/segcore/AckResponder.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/arrow_fs_c.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/collection_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/column_groups_c.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/ConcurrentVector.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/DeletedRecord.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/InsertRecord.h (analyzed via test_sealed.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/load_field_data_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/memory_planner.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/metrics_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/minhash_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_reader_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/packed_writer_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/phrase_match_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/pkVisitor.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/plan_c.h (C header, no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/Record.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/reduce_c.h (C header, no direct analysis needed)
- [x] /home/zilliz/milvus/internal/core/src/segcore/ReduceStructure.h (analyzed via test_search_group_by.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/ReduceUtils.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/SegcoreConfig.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/segcore_init_c.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/segment_c.h (C header, no direct analysis needed)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentChunkReader.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowing.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/segcore/SegmentGrowingImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/SegmentSealed.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/tokenizer_c.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/token_stream_c.h
- [!] /home/zilliz/milvus/internal/core/src/segcore/Types.h (no changes suggested by IWYU)
- [!] /home/zilliz/milvus/internal/core/src/segcore/Utils.h (no changes suggested by IWYU)

### segcore/reduce
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/GroupReduce.h
- [x] /home/zilliz/milvus/internal/core/src/segcore/reduce/Reduce.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/reduce/StreamReduce.h

### segcore/storagev1translator
- [x] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/ChunkTranslator.h (analyzed via test_sealed.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev1translator/DefaultValueChunkTranslator.h

### segcore/storagev2translator
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupChunkTranslator.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/GroupCTMeta.h
- [ ] /home/zilliz/milvus/internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.h

### storage
- [x] /home/zilliz/milvus/internal/core/src/storage/BinlogReader.h
- [x] /home/zilliz/milvus/internal/core/src/storage/ChunkManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/DataCodec.h
- [!] /home/zilliz/milvus/internal/core/src/storage/DiskFileManagerImpl.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/storage/Event.h
- [x] /home/zilliz/milvus/internal/core/src/storage/FileManager.h
- [!] /home/zilliz/milvus/internal/core/src/storage/FileWriter.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/InsertData.h (analyzed via test_sealed.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/storage/KeyRetriever.h
- [x] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManager.h
- [x] /home/zilliz/milvus/internal/core/src/storage/LocalChunkManagerSingleton.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/MemFileManagerImpl.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/MmapChunkManager.h
- [x] /home/zilliz/milvus/internal/core/src/storage/MmapManager.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/parquet_c.h
- [x] /home/zilliz/milvus/internal/core/src/storage/PayloadReader.h (analyzed via test_sealed.cpp entry)
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PayloadWriter.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/PluginLoader.h
- [x] /home/zilliz/milvus/internal/core/src/storage/RemoteChunkManagerSingleton.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteInputStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/RemoteOutputStream.h
- [ ] /home/zilliz/milvus/internal/core/src/storage/SafeQueue.h
- [x] /home/zilliz/milvus/internal/core/src/storage/storage_c.h
- [!] /home/zilliz/milvus/internal/core/src/storage/StorageV2FSCache.h (no changes suggested by IWYU)
- [ ] /home/zilliz/milvus/internal/core/src/storage/ThreadPool.h
- [!] /home/zilliz/milvus/internal/core/src/storage/ThreadPools.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/src/storage/Types.h
- [x] /home/zilliz/milvus/internal/core/src/storage/Util.h

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
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/AssertUtils.h (no changes suggested by IWYU)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/cachinglayer_test_utils.h (analyzed via test_sealed.cpp entry)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/c_api_test_utils.h (analyzed via test_exec.cpp entry)
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/Constants.h
- [!] /home/zilliz/milvus/internal/core/unittest/test_utils/DataGen.h (large file, not directly analyzed)
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Distance.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/PbHelper.h
- [x] /home/zilliz/milvus/internal/core/unittest/test_utils/storage_test_utils.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/Timer.h
- [ ] /home/zilliz/milvus/internal/core/unittest/test_utils/TmpPath.h
