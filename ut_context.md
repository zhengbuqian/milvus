# V3 Index Format Unit Test Context

## 1. 项目概述

V3 标量索引格式：将所有 entry 打包到单文件，保留 V2 的并发上传/下载/加密优势。

## 2. V3 文件格式

```
┌─────────────────────────────────────────┐
│ Magic Number (8 bytes): "MVSIDXV3"       │
├──────────── Data Region ────────────────┤
│ Entry 0 data                             │
│ Entry 1 data                             │
│ ...                                      │
│ Entry N data                             │
├──────────── Directory Table (JSON) ─────┤
│ { "version": 3, "entries": [...] }       │
├──────────── Footer (8 bytes) ───────────┤
│ directory_table_size (uint64)            │
└─────────────────────────────────────────┘
```

非加密 Directory Table JSON:
```json
{
  "version": 3,
  "entries": [
    {"name": "SORT_INDEX_META", "offset": 0, "size": 48},
    {"name": "index_data", "offset": 48, "size": 100000000}
  ]
}
```

加密 Directory Table JSON 会额外包含 `__edek__`, `__ez_id__`, `slice_size`, 每个 entry 用 `original_size` + `slices[]` 替代 `offset` + `size`.

## 3. 核心新增文件

### 3.1 IndexWriter.h (`internal/core/src/storage/IndexWriter.h`)
- 命名空间: `milvus::storage`
- 常量: `MILVUS_V3_MAGIC = "MVSIDXV3"`, `MILVUS_V3_MAGIC_SIZE = 8`
- 结构体: `DirectoryEntry{name, offset, size}`, `SliceMeta{offset, size}`, `EncryptedDirectoryEntry{name, original_size, slices}`
- 抽象基类 `IndexWriter`:
  - 纯虚方法: `WriteEntry(name, data, size)`, `WriteEntry(name, fd, size)`, `Finish()`, `GetTotalBytesWritten()`
  - 保护方法: `CheckDuplicateName(name)` - 用 `unordered_set` 检测重复名，抛 AssertInfo

### 3.2 DirectStreamWriter (`internal/core/src/storage/DirectStreamWriter.cpp/.h`)
- 继承 `IndexWriter`
- 构造时写入 magic number 到 OutputStream
- `WriteEntry(name, data, size)`: 检查已 Finish、检查重名，直接写 output_，记录 dir_entry
- `WriteEntry(name, fd, size)`: 同上，从 fd 分块读 (read_buf_) 写入 output_
- `Finish()`: 生成 JSON directory table，写入 output_，写 uint64 dir_size footer，Close output_
- 成员: `output_` (shared_ptr<OutputStream>), `read_buf_` (vector<char>), `dir_entries_`, `current_offset_`, `total_bytes_written_`, `finished_`

### 3.3 EncryptedLocalWriter (`internal/core/src/storage/EncryptedLocalWriter.cpp/.h`)
- 继承 `IndexWriter`
- 构造时: 获取 edek、创建本地临时文件、写 magic
- 使用线程池 `ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE)` 做滑动窗口加密
- `Finish()`: 写 directory table + footer，close fd，上传本地文件到远端
- 析构: close fd, unlink 临时文件
- 需要 `ICipherPlugin`, `ArrowFileSystemPtr`, `ez_id`, `collection_id` 参数

### 3.4 EntryReader (`internal/core/src/storage/EntryReader.cpp/.h`)
- 静态工厂方法 `Open(input, file_size, collection_id, priority)`
- 构造时: ValidateMagic, ReadFooterAndDirectory (1-2 次 IO 读尾部)
- 根据 `__edek__` 判断加密/非加密格式
- `GetEntryNames()`: 返回所有 entry 名称
- `ReadEntry(name)`: 小 entry 有缓存 (kSmallEntryCacheThreshold = 1MB)
  - 非加密: 直接 ReadAt，>16MB 时用线程池并发 ReadAt
  - 加密: 线程池并发读取各 slice + 解密
- `ReadEntryToFile(name, path)`: 读 entry 到本地文件 (pwrite)
- `ReadEntriesToFiles(pairs)`: 批量读到文件
- 加密模式需要 `PluginLoader::GetInstance().getCipherPlugin()` 全局单例

## 4. 索引类修改

### 4.1 ScalarIndex<T> 基类 (`internal/core/src/index/ScalarIndex.h/.cpp`)
- 新增非虚方法: `UploadV3(config)`, `LoadV3(config)`
- 新增虚方法: `WriteEntries(writer)`, `LoadEntries(reader, config)` - 默认抛 Unsupported
- `file_manager_`: `MemFileManagerImplPtr`
- `is_index_file_`: bool，控制远端路径前缀 (默认 true = index_files/)

### 4.2 ScalarIndexSort<T> (`internal/core/src/index/ScalarIndexSort.cpp`)
- `WriteEntries`: 写 "SORT_INDEX_META" (JSON: index_length, num_rows, is_nested) + "index_data"
- `LoadEntries`: 读 meta, 读 data, 支持 mmap 模式

### 4.3 BitmapIndex<T> (`internal/core/src/index/BitmapIndex.cpp`)
- `WriteEntries`: 写 BITMAP_INDEX_META + BITMAP_INDEX_DATA
- `LoadEntries`: 读 meta + data, 支持 mmap

### 4.4 StringIndexMarisa (`internal/core/src/index/StringIndexMarisa.cpp`)
- `WriteEntries`: 写 MARISA_TRIE_INDEX (fd) + MARISA_STR_IDS (内存)
- `LoadEntries`: 读 trie 到临时文件 + 读 str_ids

### 4.5 StringIndexSort (`internal/core/src/index/StringIndexSort.cpp`)
- `WriteEntries`: 写 STRING_SORT_META + string_sort_data + valid_bitset

### 4.6 InvertedIndexTantivy<T> (`internal/core/src/index/InvertedIndexTantivy.cpp`)
- 提供 hook 模式: `BuildTantivyMeta()`, `WriteExtraEntries()`, `LoadExtraEntries()`
- `WriteEntries`: 写 TANTIVY_META + N 个 tantivy 文件 (fd) + null_offsets + extra
- `LoadEntries`: 读 meta, ReadEntriesToFiles, 读 null_offsets + extra

### 4.7 JsonInvertedIndex
- 通过 hook 扩展 tantivy: 在 meta 中追加 has_non_exist, 额外写/读 non_exist_offsets

### 4.8 HybridScalarIndex<T>
- `WriteEntries`: 写 HYBRID_INDEX_META + 委托 internal_index_->WriteEntries
- `LoadEntries`: 读 meta, 创建 internal_index_, 委托 LoadEntries

### 4.9 RTreeIndex<T>
- `WriteEntries`: 写 RTREE_META + N 个 rtree 文件 (fd) + null_offsets
- `LoadEntries`: 读 meta, ReadEntriesToFiles + null_offsets

### 4.10 NgramInvertedIndex
- `WriteEntries`: 调用父类 + 写 avg_row_size
- `LoadEntries`: 调用父类 + 读 avg_row_size

## 5. 关键接口

### 5.1 OutputStream (`filemanager/OutputStream.h`, 在 output/include/ 下)
```cpp
class OutputStream {
    virtual size_t Tell() const = 0;
    virtual size_t Write(const void* ptr, size_t size) = 0;
    virtual size_t Write(int fd, size_t size) = 0;
    virtual void Close() = 0;
};
```

### 5.2 InputStream (`filemanager/InputStream.h`, 在 output/include/ 下)
```cpp
class InputStream {
    virtual size_t Size() const = 0;
    virtual size_t Read(void* ptr, size_t size) = 0;
    virtual size_t ReadAt(void* ptr, size_t offset, size_t size) = 0;
    virtual void Close() = 0;
    // ... 等
};
```

### 5.3 RemoteOutputStream / RemoteInputStream
- `RemoteOutputStream`: 包装 `arrow::io::OutputStream`，在 `storage/RemoteOutputStream.h`
- `RemoteInputStream`: 包装 `arrow::io::RandomAccessFile`，在 `storage/RemoteInputStream.h`

### 5.4 ICipherPlugin / IEncryptor / IDecryptor (`storage/plugin/PluginInterface.h`)
```cpp
class ICipherPlugin : public IPlugin {
    virtual pair<shared_ptr<IEncryptor>, string> GetEncryptor(ez_id, coll_id) = 0;
    virtual shared_ptr<IDecryptor> GetDecryptor(ez_id, coll_id, edek) = 0;
};
class IEncryptor {
    virtual string Encrypt(const string& plaintext) const = 0;
    virtual string Encrypt(const void* data, size_t len) const = 0;
};
class IDecryptor {
    virtual string Decrypt(const string& ciphertext) const = 0;
    virtual string Decrypt(const void* data, size_t len) const = 0;
};
```

## 6. 测试基础设施

### 6.1 测试文件位置和自动发现
- 测试文件放在 `internal/core/src/` 目录树下
- CMakeLists (`internal/core/unittest/CMakeLists.txt`) 通过 GLOB 自动发现:
  ```cmake
  file(GLOB_RECURSE SOURCE_TEST_FILES
      "${CMAKE_HOME_DIRECTORY}/src/**/*Test.cpp"
      "${CMAKE_HOME_DIRECTORY}/src/**/*_test.cpp"
  )
  ```
- 所以新测试文件命名为 `*Test.cpp` 或 `*_test.cpp` 放在 src/ 下即可被自动编译

### 6.2 本地存储测试模式 (参考 DiskFileManagerTest.cpp)
```cpp
auto storage_config = get_default_local_storage_config();
cm_ = storage::CreateChunkManager(storage_config);
fs_ = storage::InitArrowFileSystem(storage_config);
```
或:
```cpp
auto conf = milvus_storage::ArrowFileSystemConfig();
conf.storage_type = "local";
conf.root_path = "/tmp/v3test";
auto result = milvus_storage::CreateArrowFileSystem(conf);
auto fs = result.ValueOrDie();
```

### 6.3 创建 OutputStream/InputStream
```cpp
// 写
auto output_result = fs->OpenOutputStream(path);
auto output = std::make_shared<RemoteOutputStream>(std::move(output_result.ValueOrDie()));

// 读
auto input_result = fs->OpenInputFile(path);
auto input = std::make_shared<RemoteInputStream>(std::move(input_result.ValueOrDie()));
```

### 6.4 FileManagerContext 构造
```cpp
FieldDataMeta filed_data_meta = {collection_id, partition_id, segment_id, field_id};
IndexMeta index_meta = {segment_id, field_id, build_id, index_version, "index"};
auto context = storage::FileManagerContext(filed_data_meta, index_meta, cm, fs);
```

### 6.5 ScalarIndexSort 构造和使用
```cpp
milvus::index::ScalarIndexSort<int64_t> index(context);
std::vector<int64_t> values = {1, 2, 3};
index.Build(values.size(), values.data());
auto stats = index.UploadV3({});
```

### 6.6 ThreadPools
ThreadPools 是全局单例，在测试环境中通常已初始化 (init_gtest.cpp)。
EntryReader 的并发读取使用 `ThreadPools::GetThreadPool(priority)`。

### 6.7 已有的 V3 测试
`DiskFileManagerTest.cpp` 中的 `V3PackedIndexPathMismatch`:
- 测试了 ScalarIndexSort<int64_t> 的 UploadV3
- 验证远端路径格式
- 但没有测试 LoadV3

### 6.8 test_utils
- `storage_test_utils.h` 在 `internal/core/unittest/test_utils/`
- `get_default_local_storage_config()`: 返回 local storage config, root_path = TestRemotePath
- `gen_field_data_meta()`, `gen_index_meta()`: 生成测试用元数据

## 7. 编译和运行测试

- 编译: `./compile.sh compile` (使用已有缓存)
- 运行 C++ 测试: `./compile.sh runcpput "TestFilter"` (带 gtest filter)
- 不要用 `make clean`！
- 测试二进制: `internal/core/output/unittest/all_tests`

## 8. 需要实现的测试

### 8.1 IndexWriterTest.cpp (新文件: `internal/core/src/storage/IndexWriterTest.cpp`)
DirectStreamWriter + EntryReader 非加密路径往返测试:
1. 小内存 entry 往返
2. 大内存 entry (>16MB) 往返 (触发 EntryReader 并发读取)
3. fd 写入 entry 往返
4. 多 entry 往返
5. 重复名检测
6. ReadEntryToFile
7. ReadEntriesToFiles
8. GetEntryNames
9. entry 未找到错误
10. 小 entry 缓存
11. GetTotalBytesWritten
12. Finish 后写入报错

### 8.2 DiskFileManagerTest.cpp 追加 (修改已有文件)
ScalarIndexSort V3 完整往返测试:
- Build → UploadV3 → LoadV3 → 验证索引正确性 (Count, In, Range, Reverse_Lookup)

### 8.3 额外 Scalar Index V3 往返测试 (均追加到 DiskFileManagerTest.cpp)

所有测试使用 DiskAnnFileManagerTest fixture。模式统一为: 构造索引 → Build → UploadV3 → 新实例 LoadV3 → 验证查询正确性。

#### 8.3.1 BitmapIndex<int64_t>
- 头文件: `index/BitmapIndex.h`
- Build: `Build(n, values)` — 直接传入 int64_t 数组
- WriteEntries 写 "BITMAP_INDEX_META" + "BITMAP_INDEX_DATA"
- 验证: Count, In 查询

#### 8.3.2 StringIndexMarisa
- 头文件: `index/StringIndexMarisa.h`
- Build: `Build(n, strs.data())` — 传入 std::string 数组
- WriteEntries 写 "MARISA_TRIE_INDEX" (fd) + "MARISA_STR_IDS"
- 验证: Count, In 查询, Reverse_Lookup
- 参考: `internal/core/unittest/StringIndexTest.cpp`

#### 8.3.3 StringIndexSort
- 头文件: `index/StringIndexSort.h`
- Build: `Build(n, strs.data())` — 传入 std::string 数组
- WriteEntries 写 "STRING_SORT_META" + "string_sort_data" + "valid_bitset"
- 验证: Count, In 查询, Range, Reverse_Lookup
- 参考: `internal/core/src/index/StringIndexSortTest.cpp`

#### 8.3.4 InvertedIndexTantivy<int64_t>
- 头文件: `index/InvertedIndexTantivy.h`
- Build: `BuildWithRawDataForUT(n, values, config)` — config 需要 "is_primary_key"=false, "tantivy_index_path"=临时目录
- WriteEntries 写 "TANTIVY_META" + N 个 tantivy 文件 (fd) + "null_offsets"
- 验证: Count, In 查询
- 注意: 需要创建临时目录给 tantivy 用

#### 8.3.5 RTreeIndex
- 头文件: `index/RTreeIndex.h`
- Build: `BuildWithRawDataForUT(n, wkbs.data())` — 传入 WKB 格式的几何数据
- WriteEntries 写 "RTREE_META" + N 个文件 (fd) + "null_offsets"
- 验证: Count
- 参考: `internal/core/src/index/RTreeIndexTest.cpp` — 有 CreatePointWKB 等辅助函数

#### 8.3.6 HybridScalarIndex<int64_t>
- 头文件: `index/HybridScalarIndex.h`
- Build: `Build(n, values)` — 根据基数自动选择内部索引 (Bitmap/STLSORT/Inverted)
- WriteEntries 写 "HYBRID_INDEX_META" + 委托内部索引写入
- 验证: Count, In 查询
- 注意: 需要设置 config 中的 "bitmap_cardinality_limit" 等参数
