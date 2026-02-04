# Milvus 标量索引构建完整指南

## 1. 概述

Milvus 的标量索引用于支持对标量字段（如整数、浮点数、字符串等）的高效过滤查询。与向量索引不同，标量索引返回的是满足条件的行位置位图（TargetBitmap），而非相似度排序的结果。

### 1.1 支持的标量索引类型

| 索引类型 | 枚举值 | 适用场景 |
|---------|--------|---------|
| STL排序索引 | `STLSORT` | 默认类型，适合范围查询 |
| 位图索引 | `BITMAP` | 低基数数据，精确匹配 |
| 倒排索引 | `INVERTED` | 基于 Tantivy，全文搜索 |
| 混合索引 | `HYBRID` | 动态选择最优策略 |
| Marisa Trie | `MARISA` | 字符串前缀匹配 |
| R树索引 | `RTREE` | 几何数据 |
| 文本匹配索引 | `TEXT_MATCH` | 全文搜索 |

## 2. 核心类结构

### 2.1 类继承体系

```
IndexBase (internal/core/src/index/Index.h)
    ↓
ScalarIndex<T> (internal/core/src/index/ScalarIndex.h)
    ├── ScalarIndexSort<T>      - 排序实现
    ├── BitmapIndex<T>          - 位图实现
    ├── InvertedIndexTantivy<T> - Tantivy 倒排实现
    ├── HybridScalarIndex<T>    - 混合策略
    ├── StringIndexMarisa       - 字符串前缀树
    ├── RTreeIndex<T>           - R树（几何）
    └── TextMatchIndex          - 文本匹配
```

### 2.2 ScalarIndex 基类接口

**文件位置**: `internal/core/src/index/ScalarIndex.h`

```cpp
template <typename T>
class ScalarIndex : public IndexBase {
public:
    // 构建
    virtual void Build(size_t n, const T* values, const bool* valid_data = nullptr) = 0;

    // 查询
    virtual const TargetBitmap In(size_t n, const T* values) = 0;
    virtual const TargetBitmap Range(T value, OpType op) = 0;
    virtual const TargetBitmap Range(T lower, bool lb_inclusive,
                                     T upper, bool ub_inclusive) = 0;
};
```

### 2.3 IndexBase 核心接口

**文件位置**: `internal/core/src/index/Index.h`

```cpp
class IndexBase {
public:
    // 序列化：将索引转换为 BinarySet
    virtual BinarySet Serialize(const Config& config) = 0;

    // 加载：从 BinarySet 恢复索引
    virtual void Load(const BinarySet& binary_set, const Config& config = {}) = 0;

    // 上传：将索引上传到对象存储，返回统计信息
    virtual IndexStatsPtr Upload(const Config& config = {}) = 0;
};
```

## 3. 索引创建流程

### 3.1 创建流程

```
CreateIndexInfo { field_type, index_type, field_name }
                    ↓
        IndexFactory::CreateScalarIndex()
                    ↓
        根据 field_type 和 index_type 路由
                    ↓
        返回具体的 ScalarIndex<T> 实例
```

### 3.2 IndexFactory 工厂类

**文件位置**: `internal/core/src/index/IndexFactory.cpp`

```cpp
IndexBasePtr IndexFactory::CreateScalarIndex(
    const CreateIndexInfo& info,
    const storage::FileManagerContext& file_manager_context) {

    auto data_type = info.field_type;
    auto index_type = info.index_type;

    // 根据数据类型和索引类型创建具体实现
    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8/16/32/64:
        case DataType::FLOAT/DOUBLE:
        case DataType::VARCHAR:
            return CreatePrimitiveScalarIndex(data_type, info, ...);
        case DataType::ARRAY:
            return CreateCompositeScalarIndex(info, ...);
        case DataType::JSON:
            return CreateJsonIndex(info, ...);
        case DataType::GEOMETRY:
            return CreateGeometryIndex(info, ...);
    }
}
```

## 4. 索引生命周期的三个阶段

标量索引的生命周期分为三个阶段：

| 阶段 | 说明 | 存储位置 |
|-----|------|---------|
| **构建阶段** | 创建索引数据结构 | 内存 或 本地临时磁盘 |
| **上传阶段** | 将索引上传到对象存储 | 从内存/磁盘 → 对象存储 |
| **加载阶段** | 从对象存储加载索引 | 内存 或 mmap |

**重要**：`ENABLE_MMAP` 配置**仅影响加载阶段**，不影响构建和上传阶段。

## 5. 构建阶段

### 5.1 两种构建模式

根据索引实现的不同，构建时数据存储在不同位置：

| 构建模式 | 索引类型 | 说明 |
|---------|---------|------|
| **内存构建** | ScalarIndexSort, BitmapIndex, StringIndexMarisa | 索引数据结构直接在内存中构建 |
| **磁盘构建** | InvertedIndexTantivy, RTreeIndex, TextMatchIndex | 索引写入本地临时目录 |

### 5.2 内存构建示例

```cpp
// 索引数据在内存中的 vector 里构建
std::vector<IndexStructure<T>> data_;
data_.reserve(n);
for (size_t i = 0; i < n; ++i) {
    data_.emplace_back(values[i], i);
}
std::sort(data_.begin(), data_.end());
```

### 5.3 磁盘构建示例

```cpp
// 索引写入本地临时目录
path_ = disk_file_manager_->GetLocalTempIndexObjectPrefix();
boost::filesystem::create_directories(path_);
wrapper_ = std::make_shared<TantivyIndexWrapper>(path_.c_str(), ...);
// ... 写入数据 ...
wrapper_->finish();  // 数据持久化到磁盘
```

## 6. 上传阶段

### 6.1 上传模式分类

所有标量索引的上传最终归结为两种数据来源：

| 数据来源 | FileManager | 说明 |
|---------|-------------|------|
| **BinarySet（内存）** | MemFileManagerImpl | 索引序列化为 BinarySet 后上传 |
| **磁盘文件** | DiskFileManagerImpl | 直接上传本地磁盘上的索引文件 |

### 6.2 各索引的上传模式

| 索引类型 | 上传来源 | FileManager |
|---------|---------|-------------|
| ScalarIndexSort | BinarySet | Mem |
| BitmapIndex | BinarySet | Mem |
| StringIndexMarisa | BinarySet | Mem |
| InvertedIndexTantivy | 磁盘文件 + BinarySet | Disk + Mem |
| HybridScalarIndex | 委托给内部索引 | 取决于内部索引 |
| RTreeIndex | 磁盘文件 + BinarySet | Disk + Mem |
| TextMatchIndex | 磁盘文件 + BinarySet | Disk + Mem |

### 6.3 纯内存上传流程

适用于：ScalarIndexSort, BitmapIndex, StringIndexMarisa

```cpp
IndexStatsPtr Upload(const Config& config) {
    // 1. 序列化为 BinarySet
    auto binary_set = Serialize(config);

    // 2. 通过 MemFileManager 上传
    file_manager_->AddFile(binary_set);

    // 3. 返回统计信息
    return IndexStats::NewFromSizeMap(
        file_manager_->GetAddedTotalMemSize(),
        file_manager_->GetRemotePathsToFileSize());
}
```

### 6.4 混合上传流程

适用于：InvertedIndexTantivy, RTreeIndex, TextMatchIndex

```cpp
IndexStatsPtr Upload(const Config& config) {
    finish();  // 确保数据写入磁盘

    // 1. 从磁盘上传索引文件
    for (auto& file : directory_iterator(path_)) {
        disk_file_manager_->AddFile(file.path().string());
    }

    // 2. 从内存上传元数据（如 null_offset）
    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);

    // 3. 合并统计信息
    return IndexStats::New(mem_size + disk_size, all_files);
}
```

### 6.5 上传流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          纯内存上传                                      │
│                                                                         │
│   ScalarIndexSort / BitmapIndex / StringIndexMarisa                     │
│                          ↓                                              │
│                   Serialize() → BinarySet                               │
│                          ↓                                              │
│               MemFileManagerImpl::AddFile()                             │
│                          ↓                                              │
│                    PutIndexData()                                       │
│                          ↓                                              │
│                   对象存储 (MinIO/S3)                                    │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                       混合上传（磁盘+内存）                               │
│                                                                         │
│   InvertedIndexTantivy / RTreeIndex / TextMatchIndex                    │
│                          ↓                                              │
│           ┌──────────────┴──────────────┐                               │
│           ↓                             ↓                               │
│    本地磁盘文件                    BinarySet（元数据）                    │
│           ↓                             ↓                               │
│  DiskFileManagerImpl::AddFile()  MemFileManagerImpl::AddFile()          │
│           ↓                             ↓                               │
│           └──────────────┬──────────────┘                               │
│                          ↓                                              │
│                    PutIndexData()                                       │
│                          ↓                                              │
│                   对象存储 (MinIO/S3)                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

## 7. FileManager 上传机制

### 7.1 两种 FileManager

| 实现类 | 数据来源 | 用途 |
|-------|---------|------|
| `MemFileManagerImpl` | BinarySet（内存） | 上传内存中序列化的数据 |
| `DiskFileManagerImpl` | 本地磁盘文件 | 分片上传大文件 |

### 7.2 文件分片机制

两种 FileManager 的分片时机不同：

| FileManager | 分片时机 | 分片方式 |
|-------------|---------|---------|
| MemFileManagerImpl | **AddFile() 之前**，在 `Serialize()` 中调用 `Disassemble()` | 将 BinarySet 中 > 16MB 的项拆分 |
| DiskFileManagerImpl | **AddFile() 内部** | 按 16MB 分片本地文件 |

### 7.3 内存索引的分片：Disassemble()

**文件位置**: `internal/core/src/common/Slice.cpp`

在 `Serialize()` 返回前调用 `Disassemble()` 对 BinarySet 进行分片：

```cpp
// 在各索引的 Serialize() 末尾调用
BinarySet Serialize(const Config& config) {
    BinarySet res_set;
    // ... 填充数据 ...

    milvus::Disassemble(res_set);  // 分片大于 16MB 的项
    return res_set;
}

// Disassemble 实现
void Disassemble(BinarySet& binarySet) {
    for (auto& kv : binarySet.binary_map_) {
        if (kv.second->size > FILE_SLICE_SIZE) {  // 16MB
            // 将该项拆分为多个 16MB 的切片
            // 例如: "index_data" → "index_data_0", "index_data_1", ...
            Slice(key, data, FILE_SLICE_SIZE, binarySet, slice_meta);
        }
    }
    // 添加分片元数据 INDEX_FILE_SLICE_META
    AppendSliceMeta(binarySet, meta_info);
}
```

### 7.4 MemFileManagerImpl::AddFile()

接收已分片的 BinarySet，按 128MB 批量上传：

```cpp
bool AddFile(const BinarySet& binary_set) {
    // BinarySet 已经在 Serialize() 中被 Disassemble() 分片
    // 这里按 128MB 批量上传
    for (const auto& [name, binary] : binary_set) {
        data_slices.push_back(binary->data.get());
        slice_names.push_back(prefix + "/" + name);

        if (batch_size >= 128MB) {
            PutIndexData(rcm_, data_slices, slice_sizes, slice_names, ...);
            // 清空批次
        }
    }
    // 上传剩余数据
    PutIndexData(...);
}
```

### 7.5 DiskFileManagerImpl::AddFile()

在内部对本地文件进行分片：

```cpp
bool AddFile(const std::string& local_file_path) {
    // 在 AddFile 内部分片
    // 1. 按 16MB (FILE_SLICE_SIZE) 分片文件
    // 2. 最多 8 个分片并行上传（128MB / 16MB = 8）
    // 3. 调用 PutIndexData 上传
}
```

### 7.6 PutIndexData() - 统一上传入口

**文件位置**: `internal/core/src/storage/Util.cpp`

无论是 MemFileManager 还是 DiskFileManager，最终都通过 `PutIndexData()` 上传：

```cpp
std::map<std::string, int64_t> PutIndexData(
    ChunkManager* remote_chunk_manager,
    const std::vector<const uint8_t*>& data_slices,
    const std::vector<int64_t>& slice_sizes,
    const std::vector<std::string>& slice_names,
    FieldDataMeta& field_meta,
    IndexMeta& index_meta) {

    // 线程池并行上传每个切片
    for (size_t i = 0; i < data_slices.size(); ++i) {
        futures.push_back(pool.Submit(
            EncodeAndUploadIndexSlice, ...));
    }

    // 等待完成，返回路径到大小的映射
    return remote_paths_to_size;
}
```

### 7.7 远程路径格式

```
{root_path}/index_files/{build_id}/{index_version}/{partition_id}/{segment_id}/{filename}_{slice_num}
```

示例：
```
minio/milvus/index_files/12345/1/100/456/index_data_0
```

## 8. 索引文件存储格式

上传到对象存储的每个文件包含两部分：

```
┌────────────────────────────────────────┐
│          DescriptorEvent              │
│  ├─ collection_id                     │
│  ├─ partition_id                      │
│  ├─ segment_id                        │
│  ├─ field_id                          │
│  ├─ extras (JSON): original_size,     │
│  │                 index_build_id     │
│  └─ timestamp                         │
├────────────────────────────────────────┤
│           IndexEvent                  │
│  └─ payload (原始索引二进制数据)        │
└────────────────────────────────────────┘
```

**文件位置**: `internal/core/src/storage/IndexData.cpp`

```cpp
std::vector<uint8_t> IndexData::serialize_to_remote_file() {
    // 1. 创建 DescriptorEvent（元数据）
    DescriptorEvent descriptor_event;
    // ... 填充元数据 ...
    auto des_event_bytes = descriptor_event.Serialize();

    // 2. 创建 IndexEvent（索引数据）
    IndexEvent index_event;
    index_event.event_data.payload_reader = payload_reader_;
    auto index_event_bytes = index_event.Serialize();

    // 3. 拼接输出
    des_event_bytes.insert(des_event_bytes.end(),
                           index_event_bytes.begin(),
                           index_event_bytes.end());
    return des_event_bytes;
}
```

**旧版本标量索引的加密阶段与范围（本节描述的序列化/上传流程）**：

- **加密阶段**：在 `serialize_to_remote_file()` 内部、`IndexEvent` 序列化完成之后、与 `DescriptorEvent` 拼接之前进行。
- **加密范围**：仅对 `IndexEvent` 的序列化字节进行加密（包含 `IndexEvent` 头部和 payload/原始索引二进制数据），`DescriptorEvent` 保持明文。
- **密钥信息**：若启用了 cipher 插件，会将 `EDEK` 和 `EZID` 写入 `DescriptorEvent.extras`，以便解密阶段使用。

## 9. 加载阶段

### 9.1 加载模式

加载阶段受 `ENABLE_MMAP` 配置影响：

| 模式 | 配置 | 内存占用 | 访问速度 |
|-----|------|---------|---------|
| 内存加载 | `ENABLE_MMAP=false` | 高 | 快 |
| MMAP 加载 | `ENABLE_MMAP=true` | 低（按需） | 页面置换时较慢 |

### 9.2 加载流程

```cpp
void Load(const BinarySet& index_binary, const Config& config) {
    is_mmap_ = GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);

    if (is_mmap_) {
        // 1. 写入本地文件
        FileWriter file_writer(mmap_filepath_);
        file_writer.Write(index_data->data.get(), index_data->size);

        // 2. mmap 映射
        mmap_data_ = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    } else {
        // 直接加载到内存
        data_.resize(index_size);
        memcpy(data_.data(), index_data->data.get(), index_data->size);
    }
}
```

## 10. 关键文件索引

| 模块 | 文件路径 |
|-----|---------|
| 索引基类 | `internal/core/src/index/Index.h` |
| 标量索引基类 | `internal/core/src/index/ScalarIndex.h` |
| 工厂类 | `internal/core/src/index/IndexFactory.cpp` |
| 排序索引 | `internal/core/src/index/ScalarIndexSort.cpp` |
| 位图索引 | `internal/core/src/index/BitmapIndex.cpp` |
| 倒排索引 | `internal/core/src/index/InvertedIndexTantivy.cpp` |
| 混合索引 | `internal/core/src/index/HybridScalarIndex.cpp` |
| 字符串索引 | `internal/core/src/index/StringIndexMarisa.cpp` |
| R树索引 | `internal/core/src/index/RTreeIndex.cpp` |
| 文本匹配索引 | `internal/core/src/index/TextMatchIndex.cpp` |
| 内存文件管理器 | `internal/core/src/storage/MemFileManagerImpl.cpp` |
| 磁盘文件管理器 | `internal/core/src/storage/DiskFileManagerImpl.cpp` |
| 索引数据序列化 | `internal/core/src/storage/IndexData.cpp` |
| Event 定义 | `internal/core/src/storage/Event.cpp` |
| 上传工具函数 | `internal/core/src/storage/Util.cpp` |

