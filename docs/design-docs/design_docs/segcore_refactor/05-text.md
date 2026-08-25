# L2 `segcore_text` —— TEXT 列与 LOB

> 设计提案。返回 [总览](README.md)。

## 1. 职责

TEXT 类型字段的**大对象（LOB）存取**这一条独立数据通路。它与普通列的差别在于值不内联在 chunk 里，而是一条引用（`TextLobRef`），真正的内容在外部 LOB 文件或本地 spillover 文件中。

- LOB 引用的编解码（`TextLobRef`，16 字节定长）。
- 远端 LOB 读取与 reader 缓存（`TextColumnCache`）。
- growing 段的本地 spillover 写入与回读（`TextLobSpillover`）。
- TEXT 字段的 LOB base path 解析。

## 2. 边界

**不属于本模块：**

- **全文检索索引**。`index::TextMatchIndex` 的 pin 与查询属于 [L2 indexing](04-indexing.md)。本模块只管**值**，不管**倒排**。这是当前最容易混淆的一条边界——`text_indexes_` 与 `text_lob_paths_` 现在都挂在同一个 `RuntimeResourceState` 上，但它们是两条完全不同的路径。
- 普通 VARCHAR 列。走 [columnar](03-columnar.md) 的常规通道。
- LOB 文件的加载编排（哪些 path、什么时候读）。属于 [L3 load](07-load.md)。
- 分词器（`tokenizer_c.cpp`、`token_stream_c.cpp`）。属于 [L6 capi](10-capi.md) 与 tantivy binding，与 LOB 无关。

## 3. 现状来源

| 现文件 | 迁入 | 说明 |
|---|---|---|
| `segcore/TextColumnCache.{h,cpp}` | `text/TextColumnCache.*` | 原样迁移 |
| `segcore/TextLobSpillover.h` | `text/TextLobSpillover.h` | 原样迁移 |
| `RuntimeResourceState::text_lob_paths` | `text/TextLobInventory.h` | 从 sealed 状态中析出 |
| `SegmentGrowingImpl` 的 `text_lob_spillovers_` / `text_lob_paths_` / `text_loaded_row_count_`（`SegmentGrowingImpl.h:934-946`） | `text/GrowingTextStore.h` | 从 growing impl 中析出 |
| `ChunkedSegmentSealedImpl` 中 TEXT 取值的分支 | `text/SealedTextStore.cpp` | 与 `bulk_subscript` 的 TEXT 分支合并 |

## 4. 公开接口

```cpp
// segcore/text/ITextStore.h
namespace milvus::segcore::text {

// LOB 引用。定长 16 字节，与 TextLobRef 现有编码兼容。
struct LobRef {
    static constexpr size_t kEncodedSize = 16;

    uint64_t offset = 0;
    uint32_t size   = 0;
    uint32_t flags  = 0;   // 0 = uncompressed

    std::string Encode() const;
    static LobRef Decode(std::string_view);
    static bool   IsValidEncoding(std::string_view);
};

// TEXT 值的读取面。sealed 与 growing 各一个实现。
class ITextStore {
 public:
    virtual ~ITextStore() = default;

    virtual bool Has(FieldId) const = 0;

    // 单值读取
    virtual std::string Read(OpContext*, FieldId, LobRef) const = 0;

    // 批量读取。实现负责按 LOB 文件聚簇以减少 IO。
    virtual std::vector<std::string>
    ReadBatch(OpContext*, FieldId, const std::vector<LobRef>&) const = 0;
};

// growing 的写入面。只有 insert 路径持有。
class IGrowingTextSink {
 public:
    virtual ~IGrowingTextSink() = default;

    // 返回写入后的引用；超过内联阈值时落 spillover 文件
    virtual LobRef Append(FieldId, std::string_view value) = 0;
    virtual void   Flush(FieldId) = 0;
};

}  // namespace milvus::segcore::text
```

```cpp
// segcore/text/TextColumnCache.h（迁移后，接口不变）
namespace milvus::segcore::text {

struct TextColumnCacheConfig { size_t max_file_readers = 64; };
struct TextColumnCacheStats  { size_t file_cache_hits, file_cache_misses,
                               current_file_cache_size; };

class TextColumnCache {
 public:
    explicit TextColumnCache(const TextColumnCacheConfig& = {});
    TextColumnCache(const TextColumnCacheConfig&, TextLobReaderFactory);

    std::shared_ptr<CachedTextLobReader>
    GetOrCreateReader(const std::string& lob_base_path,
                      std::shared_ptr<arrow::fs::FileSystem>,
                      const milvus_storage::api::Properties&);

    std::string ReadText(...);
    std::vector<std::string> ReadBatch(...);
    TextColumnCacheStats Stats() const;
};

}  // namespace milvus::segcore::text
```

`TextLobReaderFactory` 是已有的注入点（`TextColumnCache.h:55`），迁移后保留——它正是本模块能脱离真实 IO 单测的原因。

## 5. 依赖

| 允许依赖 | 说明 |
|---|---|
| `segcore_contracts` | `ColumnSink`（批量读取结果的出口） |
| `segcore_columnar` | 读取内联的 LOB 引用列 |
| `common/` | `Types.h`、`OpContext.h` |
| `milvus-storage` | `lob_column::LobColumnReader` |
| `arrow` | `fs::FileSystem` |

**禁止依赖：** `indexing`（尤其不得 include `index/TextMatchIndex.h`）、`mvcc`、`load`、`segment`、`query/`、`exec/`。

> 这条禁令是本模块存在的主要理由之一：当前 TEXT **值**路径与 TEXT **索引**路径在 sealed impl 里交织，导致任何一方的改动都要读另一方的代码。

## 6. 测试

`test_segcore_text`，链接 `segcore_text` + `segcore_columnar` + `milvus_common` + `milvus-storage`，**不链接 `milvus_core`**。

现有可迁移：`TextColumnCacheTest.cpp`。

新增覆盖：

- `LobRef` 编解码往返、非法长度、`kEncodedSize` 的 `static_assert`
- reader 缓存的淘汰（`max_file_readers` 边界）与并发访问（`CachedTextLobReader::mutex` 的串行保证）
- `ReadBatch` 的 IO 聚簇：注入计数型 `TextLobReaderFactory`，断言 N 个引用命中同一文件时只 open 一次
- growing spillover：写入 → flush → 回读的一致性；进程内文件句柄泄漏检查

## 7. 迁移步骤（P5）

1. 建 `text/` 与 target；迁 `TextColumnCache`、`TextLobSpillover`（无外部依赖，可直接搬）。
2. 从 `SegmentGrowingImpl` 析出 `GrowingTextStore`（`SegmentGrowingImpl.h:934-946` 的三个成员）。
3. 从 `ChunkedSegmentSealedImpl` 的 `bulk_subscript` TEXT 分支析出 `SealedTextStore`。
4. `RuntimeResourceState::text_lob_paths` → `TextLobInventory`，与 P4 的 inventory 拆分协同。
5. 在 lint 中加入禁令：`text/` 下不得出现 `#include "index/`。

出口标准：`text/` 与 `indexing/` 之间无 include 边；`test_segcore_text` 不链接 `milvus_core`。
