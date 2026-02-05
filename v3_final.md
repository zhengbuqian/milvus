# Scalar Index V3 设计文档

## 1. 背景与目标

V2 标量索引采用多文件存储（每个 entry 一个远端对象），天然具备并发上传/下载/加密/解密能力（切片级），但文件数量多、管理复杂。

V3 目标：**单文件打包**，同时保留 V2 的所有并发优势。

| 目标 | 说明 |
|------|------|
| 单文件 | 所有 entry 打包到一个远端对象 |
| 并发上传 | Multipart Upload 并行 |
| 并发加密/解密 | slice 级并行，与 V2 切片粒度一致 |
| 并发下载 | `ReadAt` 多线程并行 |
| 低内存峰值 | `O(W × slice_size)`，不随 entry 大小增长 |
| 加密透明 | 索引类不感知是否加密 |
| 减少小 IO | O(1) meta 打包为单个 entry |
| 兼容 V2 | Go 控制面通过 etcd 版本号判断；数据湖通过文件名格式判断 |

---

## 2. 文件格式

### 2.1 布局

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

Directory Table 放在文件末尾，上传时边写数据边记录 offset，一遍完成。

### 2.2 Slice — 仅加密场景

Slice 是**加密边界**，不是通用格式概念：

- **加密时**：每个 Entry 拆分为固定大小的 Slice（默认 16MB），每个 Slice 独立调用 `IEncryptor::Encrypt()`。Slice 边界记录在 Directory Table 中。
- **不加密时**：Entry 是连续明文，Directory Table 只记录 offset + size。下载时 EntryReader 自行按范围并发 `ReadAt`。

### 2.3 Directory Table

**不加密**：

```json
{
  "version": 3,
  "entries": [
    {"name": "SORT_INDEX_META", "offset": 0, "size": 48},
    {"name": "index_data", "offset": 48, "size": 100000000}
  ]
}
```

**加密**：

```json
{
  "version": 3,
  "slice_size": 16777216,
  "entries": [
    {
      "name": "SORT_INDEX_META",
      "original_size": 48,
      "slices": [
        {"offset": 0, "size": 76}
      ]
    },
    {
      "name": "index_data",
      "original_size": 100000000,
      "slices": [
        {"offset": 76, "size": 16777244},
        {"offset": 16777320, "size": 16777244},
        ...
      ]
    }
  ],
  "__edek__": "base64_encoded_encrypted_dek",
  "__ez_id__": "12345"
}
```

字段说明：
- 不加密时：`offset` + `size` 为 entry 在 Data Region 中的位置（相对于 Magic 之后）
- 加密时：所有 entry 统一使用 `original_size` + `slices[]` 格式，不存在 plain 模式字段
  - `original_size`：entry 的明文大小，用于预分配 buffer
  - `slices[]`：各 slice 在 Data Region 中的位置和密文大小（含 IV/Tag 开销）
  - 即使 entry 很小（只有一个 slice），也使用 `slices[]` 格式，保持 schema 统一
- `__edek__` / `__ez_id__`：加密元数据，仅加密时存在
- `slice_size`：明文 slice 大小，仅加密时存在

**判断逻辑**：存在 `__edek__` → 加密格式；不存在 → 非加密格式。

---

## 3. 上传路径

### 3.1 IndexWriter 接口

索引类只看到这个接口，不感知加密和传输策略：

```cpp
class IndexWriter {
public:
    // 写入内存 entry（name 必须全局唯一，重复则抛异常）
    void WriteEntry(const std::string& name, const void* data, size_t size);
    // 写入磁盘 entry（大文件流式读取，name 同样唯一）
    void WriteEntry(const std::string& name, int fd, size_t size);
    // 写 Directory Table + Footer，完成上传
    void Finish();
};
```

**Entry 名唯一性约束**：`WriteEntry` 内部维护 `std::unordered_set<std::string> written_names_`，写入前检查，重复则抛异常。这保证了 `EntryReader` 按名称随机访问的正确性——同名 entry 会导致 Directory Table 中 offset 冲突。此检查在基类 `IndexWriter` 中实现，两种子类自动继承。

### 3.2 两种实现

```
CreateIndexWriterV3(filename)
    ├─ 无加密 → DirectStreamWriter    → 直接写 RemoteOutputStream
    └─ 有加密 → EncryptedLocalWriter  → 本地临时文件 → 并行 Multipart Upload
```

**为什么分两条路径**：
- 不加密：数据大小可预知，直接写远端流，`background_writes` 自动并行上传，无多余磁盘 IO。
- 有加密：加密后大小不可预知，无法用 `pwrite` 并行写（预估偏差导致空洞或覆盖），必须顺序写。写本地文件（>1GB/s）不会因网络背压阻塞加密流水线。本地文件还提供任务级重试能力（断点重传，无需重新加密）。

### 3.3 DirectStreamWriter（不加密）

```
索引 → WriteEntry → RemoteOutputStream → background_writes → S3 Multipart
```

```cpp
class DirectStreamWriter : public IndexWriter {
public:
    DirectStreamWriter(std::shared_ptr<OutputStream> output,
                       size_t read_buf_size = 16 * 1024 * 1024)
        : output_(output), read_buf_(read_buf_size) {
        output_->Write("MVSIDXV3", kMagicSize);
    }

    void WriteEntry(const std::string& name, const void* data, size_t size) override {
        CheckDuplicateName(name);  // 基类方法，重复则抛异常
        output_->Write(data, size);
        dir_entries_.push_back({name, current_offset_, size});
        current_offset_ += size;
    }

    void WriteEntry(const std::string& name, int fd, size_t size) override {
        CheckDuplicateName(name);
        size_t entry_offset = current_offset_;
        size_t remaining = size;
        while (remaining > 0) {
            size_t to_read = std::min(remaining, read_buf_.size());
            ssize_t n = ::read(fd, read_buf_.data(), to_read);
            output_->Write(read_buf_.data(), n);
            current_offset_ += n;
            remaining -= n;
        }
        dir_entries_.push_back({name, entry_offset, size});
    }

    void Finish() override {
        // 写 Directory Table（JSON）+ Footer（uint64 dir_size）
        auto dir_str = BuildDirectoryJson().dump();
        output_->Write(dir_str.data(), dir_str.size());
        uint64_t dir_size = dir_str.size();
        output_->Write(&dir_size, sizeof(uint64_t));
        output_->Close();
    }

private:
    std::shared_ptr<OutputStream> output_;
    std::vector<char> read_buf_;              // 预分配，跨 WriteEntry 复用
    std::vector<DirectoryEntry> dir_entries_;
    size_t current_offset_ = 0;
};
```

### 3.4 EncryptedLocalWriter（加密）

```
索引 → WriteEntry → 加密线程池(并行) → 有序队列 → 顺序写本地文件
                                                       ↓ Finish()
                                            并行 Multipart Upload → S3
```

加密流水线采用滑动窗口：W 个 slice 并行加密，按提交顺序取出，顺序写入本地文件。offset 由实际 `encrypted.size()` 递增，永远正确。

**线程池选择**：复用 V2 的全局优先级线程池 `ThreadPools::GetThreadPool(MIDDLE)`（与 V2 `PutIndexData` 一致）。不自建线程池。

**线程安全**：每个 slice 的加密任务在 lambda 内部创建自己的 `IEncryptor`，用完即销毁。不共享、不缓存——全局线程池的线程生命周期比 Writer 长，缓存实例会残留错误的 edek。每个 slice 16MB，加密开销远大于创建 encryptor 的开销。

```cpp
class EncryptedLocalWriter : public IndexWriter {
public:
    // cipher_plugin: 全局加密插件，通过 PluginLoader 获取
    // ez_id / collection_id: 用于创建 encryptor/获取 edek
    EncryptedLocalWriter(const std::string& remote_path,
                         ArrowFileSystemPtr fs,
                         std::shared_ptr<ICipherPlugin> cipher_plugin,
                         int64_t ez_id, int64_t collection_id,
                         size_t slice_size = 16 * 1024 * 1024)
        : remote_path_(remote_path), fs_(fs),
          cipher_plugin_(cipher_plugin),
          ez_id_(ez_id), collection_id_(collection_id),
          slice_size_(slice_size),
          pool_(ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE)) {
        local_path_ = GenerateTempFilePath();
        local_fd_ = open(local_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        ::write(local_fd_, "MVSIDXV3", kMagicSize);
        // 构造时获取 edek，后续所有 slice 共享
        auto [enc, edek] = cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
        edek_ = edek;
    }

    // 内存 entry：滑动窗口并行加密
    void WriteEntry(const std::string& name, const void* data, size_t size) override {
        CheckDuplicateName(name);
        std::vector<SliceMeta> slices;
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        const size_t W = pool_.GetMaxThreadNum();
        std::deque<std::future<std::string>> pending;
        size_t remaining = size, read_offset = 0;

        while (remaining > 0 || !pending.empty()) {
            while (pending.size() < W && remaining > 0) {
                size_t len = std::min(remaining, slice_size_);
                auto slice = std::string(reinterpret_cast<const char*>(ptr + read_offset), len);
                pending.push_back(pool_.Submit(
                    [this, s = std::move(slice)]() {
                        // 每个任务创建自己的 encryptor，用完即销毁
                        auto [enc, unused] = cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
                        return enc->Encrypt(s);
                    }));
                read_offset += len;
                remaining -= len;
            }
            if (!pending.empty()) {
                auto encrypted = pending.front().get();
                pending.pop_front();
                ::write(local_fd_, encrypted.data(), encrypted.size());
                slices.push_back({current_offset_, encrypted.size()});
                current_offset_ += encrypted.size();
            }
        }
        dir_entries_.push_back({name, size, std::move(slices)});
    }

    // 磁盘 entry：流式逐 slice 读取 → 滑动窗口并行加密
    // 不会将整个文件读入内存，峰值为 W × slice_size
    void WriteEntry(const std::string& name, int fd, size_t size) override {
        CheckDuplicateName(name);
        std::vector<SliceMeta> slices;
        const size_t W = pool_.GetMaxThreadNum();
        std::deque<std::future<std::string>> pending;
        size_t remaining = size;

        while (remaining > 0 || !pending.empty()) {
            while (pending.size() < W && remaining > 0) {
                size_t len = std::min(remaining, slice_size_);
                auto slice_data = ReadExact(fd, len);  // 从 fd 精确读取 len 字节
                pending.push_back(pool_.Submit(
                    [this, s = std::move(slice_data)]() {
                        auto [enc, unused] = cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
                        return enc->Encrypt(s);
                    }));
                remaining -= len;
            }
            if (!pending.empty()) {
                auto encrypted = pending.front().get();
                pending.pop_front();
                ::write(local_fd_, encrypted.data(), encrypted.size());
                slices.push_back({current_offset_, encrypted.size()});
                current_offset_ += encrypted.size();
            }
        }
        dir_entries_.push_back({name, size, std::move(slices)});
    }

    void Finish() override {
        // 写 Directory Table (含 __edek__, __ez_id__, slice_size) + Footer
        auto dir_str = BuildDirectoryJson().dump();
        ::write(local_fd_, dir_str.data(), dir_str.size());
        uint64_t dir_size = dir_str.size();
        ::write(local_fd_, &dir_size, sizeof(uint64_t));
        close(local_fd_);

        // 从本地文件顺序读取 + 写 RemoteOutputStream 上传
        UploadLocalFile();
        unlink(local_path_.c_str());
    }

private:
    ThreadPool& pool_;  // 引用全局 MIDDLE 优先级线程池
    std::shared_ptr<ICipherPlugin> cipher_plugin_;
    int local_fd_;
    std::string local_path_, remote_path_, edek_;
    int64_t ez_id_, collection_id_;
    size_t current_offset_ = 0, slice_size_;
    std::vector<EncDirEntry> dir_entries_;
};
```

### 3.5 工厂方法

```cpp
std::unique_ptr<IndexWriter> FileManagerImpl::CreateIndexWriterV3(const std::string& filename) {
    auto remote_path = GetRemoteIndexObjectPrefixV2() + "/" + filename;
    auto cipher_plugin = PluginLoader::GetInstance().getCipherPlugin();
    if (cipher_plugin != nullptr) {
        return std::make_unique<EncryptedLocalWriter>(
            remote_path, fs_, cipher_plugin, GetEzId(), GetCollectionId());
    } else {
        return std::make_unique<DirectStreamWriter>(OpenOutputStream(remote_path));
    }
}
```

---

## 4. 加载路径

### 4.1 EntryReader 接口

```cpp
class EntryReader {
public:
    // collection_id: 用于解密时获取 decryptor
    // priority: 线程池优先级，与 V2 GetObjectData 一致（默认 HIGH）
    static std::unique_ptr<EntryReader> Open(
        std::shared_ptr<InputStream> input,   // 包装 RandomAccessFile
        int64_t file_size,
        int64_t collection_id = 0,
        ThreadPoolPriority priority = ThreadPoolPriority::HIGH);

    std::vector<std::string> GetEntryNames() const;

    // 按名称读 entry 到内存（小 entry 自动缓存，重复调用无额外 IO）
    Entry ReadEntry(const std::string& name);

    // 按名称读 entry 到本地文件（并发 ReadAt + pwrite）
    void ReadEntryToFile(const std::string& name, const std::string& local_path);

    // 批量读到文件
    void ReadEntriesToFiles(
        const std::vector<std::pair<std::string, std::string>>& name_path_pairs);
};
```

**加密插件获取**：`Open()` 不显式传入 `ICipherPlugin`。如果 Directory Table 中存在 `__edek__`，`EntryReader` 内部通过 `PluginLoader::GetInstance().getCipherPlugin()` 获取全局加密插件实例。解密时结合 `collection_id`、`ez_id`（从 Directory Table 读取）和 `edek` 创建 `IDecryptor`。

**线程池选择**：复用 V2 的全局优先级线程池 `ThreadPools::GetThreadPool(priority)`。V2 中 `GetObjectData` 下载/解密默认用 `HIGH`（`HIGH_SEGC_POOL`），调用方可按需传入 `MIDDLE` 或 `LOW`。V3 保持相同语义。

### 4.2 加载流程

1. **Open()**：读文件尾部（1-2 次 IO）获取 Footer + Directory Table，构建 `name → EntryMeta` 索引。获取 `ThreadPools::GetThreadPool(priority)` 的引用。若 Directory Table 中存在 `__edek__`，通过 `PluginLoader` 获取全局加密插件，并保存 `edek` + `ez_id` + `collection_id`。
2. **ReadEntry(name)**（小 entry）：一次 `ReadAt`，如需解密则当场创建 `IDecryptor` 解密，结果缓存。
3. **ReadEntry(name)**（大 entry）：
   - 加密时：线程池并发 `ReadAt` 各 slice（边界由 Directory Table 定义），每个任务在 lambda 内创建自己的 `IDecryptor` 解密，拼接到目标 buffer。
   - 不加密时：EntryReader 自行按固定大小（如 16MB）切分范围，并发 `ReadAt`，拼接到目标 buffer。
4. **ReadEntryToFile(name, path)**（磁盘索引）：与上同，输出改为 `pwrite` 到本地文件。`pwrite` 无锁（各范围写不同 offset）。

**线程安全**：每个任务在 lambda 内创建自己的 `IDecryptor`，用完即销毁。与加密路径同理——全局线程池的线程比 EntryReader 活得久，不缓存实例。

### 4.3 IO 次数

| 步骤 | IO 次数 |
|------|--------|
| 读 Directory Table | 1-2 次（尾部读取） |
| 读 meta entry | 1 次（所有 O(1) meta 已打包） |
| 读大 data entry | 并发，按 slice/range 数 |

---

## 5. 索引类接口

### 5.1 基类

```cpp
template <typename T>
class ScalarIndex : public IndexBase {
public:
    void UploadV3() override final {
        auto filename = FormatPackedIndexFileName(kPackedIndexTypeToken, 3);
        auto writer = file_manager_->CreateIndexWriterV3(filename);
        WriteEntries(writer.get());
        writer->Finish();
    }

    void LoadV3(EntryReader& reader, const Config& config) override final {
        LoadEntries(reader, config);
    }

protected:
    virtual void WriteEntries(IndexWriter* writer) = 0;
    virtual void LoadEntries(EntryReader& reader, const Config& config) = 0;
};
```

### 5.2 Meta 打包规范

每个索引将所有 O(1) 元数据打包到**单个 meta entry**（JSON 序列化），从设计上消除多次小 IO。

| 索引 | meta entry | data entries |
|------|-----------|-------------|
| ScalarIndexSort | `SORT_INDEX_META` | `index_data` |
| BitmapIndex | `BITMAP_INDEX_META` | `BITMAP_INDEX_DATA` |
| StringIndexMarisa | 无 | `MARISA_TRIE_INDEX`, `MARISA_STR_IDS` |
| InvertedIndexTantivy | `TANTIVY_META` | N 个 tantivy 文件, `null_offsets` |
| JsonInvertedIndex | `TANTIVY_META`（含 `has_non_exist`） | N 个 tantivy 文件, `null_offsets`, `non_exist_offsets` |
| HybridScalarIndex | `HYBRID_INDEX_META` | 委托内部索引 |

Meta 结构定义：

```cpp
// ScalarIndexSort: {"index_length": N, "num_rows": N, "is_nested": bool}
// BitmapIndex:     {"index_length": N, "num_rows": N}
// Tantivy:         {"file_names": [...], "has_null": bool}
// Tantivy(Json):   {"file_names": [...], "has_null": bool, "has_non_exist": bool}  (子类通过 hook 追加)
// Hybrid:          {"index_type": uint8}
```

### 5.3 索引实现示例

```cpp
// ===== ScalarIndexSort =====
void WriteEntries(IndexWriter* writer) override {
    auto meta = nlohmann::json{
        {"index_length", data_.size()}, {"num_rows", total_num_rows_},
        {"is_nested", is_nested_}
    }.dump();
    writer->WriteEntry("SORT_INDEX_META", meta.data(), meta.size());
    writer->WriteEntry("index_data", data_.data(), data_.size() * sizeof(IndexStructure<T>));
}

void LoadEntries(EntryReader& reader, const Config& config) override {
    auto mj = nlohmann::json::parse(reader.ReadEntry("SORT_INDEX_META").data);
    idx_data_len_ = mj["index_length"];
    total_num_rows_ = mj["num_rows"];
    is_nested_ = mj["is_nested"];
    auto de = reader.ReadEntry("index_data");
    data_.resize(idx_data_len_);
    memcpy(data_.data(), de.data.data(), de.data.size());
}
```

```cpp
// ===== InvertedIndexTantivy =====
// 父类提供三个 virtual hook，子类可选择性重写：
//   BuildTantivyMeta()   — 构造 TANTIVY_META JSON（子类可追加字段）
//   WriteExtraEntries()  — 写完父类 entry 后追加子类 entry
//   LoadExtraEntries()   — 加载完父类 entry 后加载子类 entry

virtual nlohmann::json BuildTantivyMeta(
    const std::vector<std::string>& file_names, bool has_null) {
    return {{"file_names", file_names}, {"has_null", has_null}};
}

void WriteEntries(IndexWriter* writer) override {
    auto file_names = GetTantivyFileNames();
    bool has_null = !null_offset_.empty();
    // 调用虚方法，子类可重写追加字段
    auto meta = BuildTantivyMeta(file_names, has_null).dump();
    writer->WriteEntry("TANTIVY_META", meta.data(), meta.size());
    for (const auto& f : tantivy_files_) {
        int fd = open(f.path.c_str(), O_RDONLY);
        writer->WriteEntry(f.name, fd, f.size);
        close(fd);
    }
    if (has_null)
        writer->WriteEntry("null_offsets", null_offset_.data(),
                           null_offset_.size() * sizeof(size_t));
    WriteExtraEntries(writer);  // hook: 子类追加 entry
}

void LoadEntries(EntryReader& reader, const Config& config) override {
    auto mj = nlohmann::json::parse(reader.ReadEntry("TANTIVY_META").data);
    auto fnames = mj["file_names"].get<std::vector<std::string>>();
    std::vector<std::pair<std::string, std::string>> pairs;
    for (const auto& fn : fnames)
        pairs.emplace_back(fn, local_index_path_ + "/" + fn);
    reader.ReadEntriesToFiles(pairs);
    if (mj["has_null"].get<bool>()) {
        auto e = reader.ReadEntry("null_offsets");
        null_offset_.resize(e.data.size() / sizeof(size_t));
        memcpy(null_offset_.data(), e.data.data(), e.data.size());
    }
    // ... 构造 TantivyIndexWrapper, ComputeByteSize ...
    LoadExtraEntries(reader, mj);  // hook: 子类加载额外 entry
}
```

```cpp
// ===== JsonInvertedIndex =====（继承 Tantivy，通过 hook 扩展）
// 不重写 WriteEntries / LoadEntries，仅重写三个 hook

nlohmann::json BuildTantivyMeta(
    const std::vector<std::string>& file_names, bool has_null) override {
    auto meta = InvertedIndexTantivy::BuildTantivyMeta(file_names, has_null);
    meta["has_non_exist"] = !non_exist_offsets_.empty();
    return meta;  // 单个 TANTIVY_META，一次 IO
}

void WriteExtraEntries(IndexWriter* writer) override {
    if (!non_exist_offsets_.empty())
        writer->WriteEntry("non_exist_offsets", non_exist_offsets_.data(),
                           non_exist_offsets_.size() * sizeof(size_t));
}

void LoadExtraEntries(EntryReader& reader, const nlohmann::json& meta) override {
    if (meta.value("has_non_exist", false)) {
        auto e = reader.ReadEntry("non_exist_offsets");
        non_exist_offsets_.resize(e.data.size() / sizeof(size_t));
        memcpy(non_exist_offsets_.data(), e.data.data(), e.data.size());
    }
}
```

```cpp
// ===== HybridScalarIndex =====
void WriteEntries(IndexWriter* writer) override {
    auto meta = nlohmann::json{{"index_type", static_cast<uint8_t>(type_)}}.dump();
    writer->WriteEntry("HYBRID_INDEX_META", meta.data(), meta.size());
    internal_index_->WriteEntries(writer);  // 委托
}

void LoadEntries(EntryReader& reader, const Config& config) override {
    auto mj = nlohmann::json::parse(reader.ReadEntry("HYBRID_INDEX_META").data);
    internal_index_ = CreateInternalIndex(
        static_cast<ScalarIndexType>(mj["index_type"].get<uint8_t>()));
    internal_index_->LoadEntries(reader, config);  // 委托
}
```

### 5.4 继承链处理

父类 `InvertedIndexTantivy` 提供三个 virtual hook，子类通过重写 hook 来扩展，而不是重写整个 `WriteEntries`/`LoadEntries`：

| Hook | 作用 | 默认行为 |
|------|------|---------|
| `BuildTantivyMeta()` | 构造 TANTIVY_META JSON | 返回 `{file_names, has_null}` |
| `WriteExtraEntries()` | 写完父类 entry 后追加 | 空操作 |
| `LoadExtraEntries()` | 加载完父类 entry 后加载 | 空操作 |

**优势**：子类 meta 字段（如 `has_non_exist`）合并到父类的 `TANTIVY_META` entry 中，只有一个 meta entry、一次 IO。子类不需要重复父类的写/读逻辑。

---

## 6. 并发模型总览

所有线程池复用 V2 的 `ThreadPools::GetThreadPool(priority)`，不自建。

| 路径 | 线程池 | 与 V2 对应 |
|------|--------|-----------|
| 加密上传 | `MIDDLE`（`MIDD_SEGC_POOL`） | V2 `PutIndexData` |
| 下载/解密 | 调用方指定，默认 `HIGH`（`HIGH_SEGC_POOL`） | V2 `GetObjectData` |
| 不加密上传 | 无线程池（`RemoteOutputStream` 内部 `background_writes`） | — |

### 上传——不加密

```
主线程: [Write to RemoteOutputStream] ──→ 顺序填充
RemoteOutputStream:  background_writes 并行上传 Part 0, 1, 2...  ──→ S3
```

### 上传——加密

```
MIDD_SEGC_POOL (W threads):  并行 Encrypt slice 0, 1, 2...
主线程:  按序 .get() → 顺序 write 到本地文件 (>1GB/s)
    ↓ Finish()
并行 Multipart Upload:  Part 0, 1, 2...  ──→ S3
```

### 下载/加载（统一）

```
HIGH_SEGC_POOL (N threads, 或调用方指定优先级):
  Thread i: ReadAt(range_i) → [Decrypt*] → 输出到内存 or pwrite 到文件
  * Decrypt 仅加密时执行
  "range" = 加密时为 slice 边界；不加密时为 EntryReader 自切分
```

---

## 7. 内存峰值

| 场景 | 峰值 |
|------|------|
| 上传，不加密，内存 entry | entry 本身 |
| 上传，不加密，磁盘 entry | 1 × `read_buf_size` (16MB) |
| 上传，加密，内存 entry | entry + W × `slice_size` |
| 上传，加密，磁盘 entry | W × `slice_size` × 2 |
| 下载，到内存 | N × `range_size` + `original_size` |
| 下载，到文件 | N × `range_size`（可复用） |

与 V2 一致：峰值由并发度 × slice 大小决定，不随 entry 大小增长。

---

## 8. 兼容性

### 版本判断策略

V3 **不**在加载时通过读文件 Magic Number 判断版本。版本判断完全在调用侧完成：

1. **标准场景（有 etcd）**：Go 控制面从 etcd 中读取索引构建时保存的 `CurrentScalarIndexEngineVersion`，据此决定调用 `Upload()`/`Load()`（V2）还是 `UploadV3()`/`LoadV3()`（V3）。
2. **数据湖场景（无 etcd）**：通过远端文件名格式判断。V3 索引为单文件，文件名格式为 `packed_index_v3`；V2 索引为多文件。

### 调用链落点

- **构建侧**：Go 层 `indexnode` 根据 `CurrentScalarIndexEngineVersion` 决定调用 `UploadV3()` 还是 `Upload()`。
- **加载侧**：Go 层 `querynode`/`datanode` 根据 etcd 中索引元数据的版本号决定调用 `LoadV3()` 还是 `Load()`。
- **当前状态**：索引层（C++ 侧）的 `WriteEntries`/`LoadEntries`/`UploadV3`/`LoadV3` 已实现；Go 层调用路径尚未接入（`CurrentScalarIndexEngineVersion` 仍为 2）。待 Go 层接入后，将版本号 bump 到 3 即可启用。

### 远端路径

V3 保持 V2 的远端根目录分类不变，避免影响外部系统读取数据：

| 根目录 | 索引类型 | V2 路径结构 | V3 路径 |
|--------|---------|-----------|---------|
| `index_files/` | 普通标量索引、InvertedIndexTantivy | `build_id/ver/part/seg/file_0` | `.../packed_index_v3` |
| `text_log/` | TextMatchIndex | `build_id/ver/coll/part/seg/field/file_0` | `.../packed_index_v3` |

`FileManagerImpl` 的 `OpenOutputStream` / `OpenInputStream` 通过 `bool is_index_file` 参数选择前缀：

```cpp
// is_index_file=true  (默认) → GetRemoteIndexObjectPrefixV2()  → index_files/...
// is_index_file=false         → GetRemoteTextLogPrefixV2()      → text_log/...

std::shared_ptr<OutputStream>
OpenOutputStream(const std::string& filename) override final {
    return OpenOutputStreamImpl(filename, /*is_index_file=*/true);
}

// 非虚方法，供需要自定义路径的索引类使用
std::shared_ptr<OutputStream>
OpenOutputStream(const std::string& filename, bool is_index_file);

std::shared_ptr<InputStream>
OpenInputStream(const std::string& filename, bool is_index_file);
```

默认 `is_index_file=true`，cardinal 等已有调用方无需改动。TextMatchIndex V3 传 `false`。`CreateIndexWriterV3` 同理增加 `bool is_index_file = true` 参数。

### 向后兼容

- V2 的 `Upload()` 和 `Load()` 方法保留，不删除。
- 回退策略：将 `CurrentScalarIndexEngineVersion` 改回 2 即可回退到 V2 路径，无需代码变更。

---

## 9. Open Questions / TODO

### 数据完整性校验

当前 V3 格式仅依赖 Magic Number + JSON Directory Table 结构校验。未设计 entry/slice 级别的完整性校验（如 CRC32/SHA256/HMAC）。

**潜在问题**：
- 对象存储局部损坏（bit-flip、截断）时，无法在加载阶段精确定位到损坏的 entry/slice
- 加密场景下，密文损坏会导致解密失败，但错误信息不够友好（加密库内部异常 vs. 明确的校验失败）

**可选方案**：
1. 在 Directory Table 的每个 entry 中增加可选字段 `checksum`（明文 CRC32 或 SHA256），加载时校验
2. 加密场景下可依赖 AEAD（如 AES-GCM）的内置认证能力，非加密场景增加 CRC
3. 仅对 Directory Table 本身做校验（footer 中增加 `dir_checksum`）

**当前决定**：暂不实现，待实际需求驱动。预留 Directory Table JSON 的可扩展性即可。

---

## 10. 实现步骤

- [ ] **Step 1: 基础设施**
  - [ ] `IndexWriter` 接口 + `DirectStreamWriter` + `EncryptedLocalWriter`
  - [ ] `EntryReader` 接口 + `Open()` / `ReadEntry()` / `ReadEntryToFile()`
  - [ ] Directory Table JSON 序列化/反序列化
  - [ ] `FileManager::CreateIndexWriterV3()` 工厂方法

- [ ] **Step 2: 迁移索引类 - Upload**
  - [ ] `ScalarIndex` 基类添加 `UploadV3()` + `WriteEntries()`
  - [ ] 迁移 `ScalarIndexSort`（打包 `SORT_INDEX_META`）
  - [ ] 迁移 `BitmapIndex`（已有 meta 打包）
  - [ ] 迁移 `StringIndexMarisa`
  - [ ] 迁移 `InvertedIndexTantivy`（打包 `TANTIVY_META` + fd 写入）
  - [ ] 迁移 `JsonInvertedIndex`（继承 + `has_non_exist`）
  - [ ] 迁移 `HybridScalarIndex`（委托）
  - [ ] 迁移其他标量索引

- [ ] **Step 3: 迁移索引类 - Load**
  - [ ] `ScalarIndex` 基类添加 `LoadV3()` + `LoadEntries()`
  - [ ] 迁移各索引的 `LoadEntries()`（`it.Next()` → `reader.ReadEntry("name")`）
  - [ ] Go 层版本判断逻辑（etcd 版本号 / 文件名格式）

- [ ] **Step 4: 测试**
  - [ ] 各索引 Upload/Load 单元测试
  - [ ] 加密场景测试
  - [ ] V2 兼容性测试
