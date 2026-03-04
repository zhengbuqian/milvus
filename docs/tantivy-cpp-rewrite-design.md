# C++ 重写 Tantivy 倒排索引设计方案

## Context

Milvus 当前通过 Rust FFI 调用 Tantivy 实现所有倒排索引（标量、文本、JSON、Ngram）。经过审计发现，Milvus 仅使用了 Tantivy 极小一部分能力：

- **标量倒排**：等价于 `BTreeMap<Value, PostingList>` + 持久化，无评分、无频率、无位置信息
- **TextMatch**：Tokenizer + OR posting list union，`IndexRecordOption::Basic`
- **PhraseMatch**：唯一使用位置信息的场景，`IndexRecordOption::WithFreqsAndPositions` + `PhraseQuery`
- **JSON 倒排**：path 编码的 term + 与标量相同的查询模式
- **Ngram 倒排**：ngram 分词 + AND posting list intersection

目标：用 C++ 完整重写倒排索引引擎，只实现 Milvus 实际使用的部分。Tokenizer/Analyzer 保留 Tantivy Rust 实现（pure function，通过精简的 FFI 调用）。

## 前置依赖

本方案在 **scalar-index-unified-format (V3)** 合并后实施。V3 提供了统一的索引文件格式（单文件 + Directory Table + Footer），本方案的磁盘格式将作为 V3 format 的 entry 来序列化，复用 V3 的上传/下载/加密/mmap 基础设施。

## 版本路由与兼容性

通过 `CurrentScalarIndexEngineVersion` 控制：

| 版本 | 行为 |
|---|---|
| ≤ 2 | 旧版标量索引 |
| 3 | 旧版 Tantivy 倒排索引 + scalar-index-unified-format (V3 文件格式) |
| **4** | **新 C++ 倒排索引 + V3 文件格式** |

- **保留所有现有 Tantivy 倒排索引代码**，不删除，确保 version ≤ 3 的旧索引仍可正常加载
- version = 4 时，新建/重建索引使用 C++ 实现
- 在 `HybridScalarIndex::Build()` 和 `IndexFactory` 中根据 `scalar_index_version` 选择实现路径
- 回滚只需将 `CurrentScalarIndexEngineVersion` 从 4 改回 3

```go
// pkg/common/common.go
const (
    MinimalScalarIndexEngineVersion = int32(0)
    CurrentScalarIndexEngineVersion = int32(4)  // 从 2→3(V3 format)→4(C++ 倒排)
)
```

```cpp
// C++ 侧 index_c.cpp / HybridScalarIndex.cpp 中的路由逻辑
if (scalar_index_version >= 4) {
    // 使用新 C++ InvertedIndex 实现
    return CreateNativeInvertedIndex<T>(config);
} else {
    // 使用旧 Tantivy 实现 (InvertedIndexTantivy<T>)
    return CreateInvertedIndex<T>(config);
}
```

## 已有可复用基础设施

| 组件 | 路径 | 说明 |
|---|---|---|
| Roaring Bitmap | conan dep `roaring/3.0.0` | 已在 BitmapIndex 中使用 |
| BitmapIndex | `internal/core/src/index/BitmapIndex.h` | `std::map<T, roaring::Roaring>` 模式参考 |
| TargetBitmap | `internal/core/src/common/Types.h:587` | 查询结果标准类型 |
| ScalarIndex<T> | `internal/core/src/index/ScalarIndex.h` | 标量索引基类 |
| V3 IndexEntryWriter/Reader | scalar-index-unified-format | 统一的序列化/存储/加密层 |
| CustomBitset | `internal/core/src/common/CustomBitset.h` | 位图实现 |
| Tokenizer C API | `internal/core/src/segcore/tokenizer_c.h` | 已有的 tokenizer FFI |

---

## 总体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    High-Level Index Classes                       │
│  NativeInvertedIndex<T>   TextMatchIndex  JsonInvertedIndex<T>   │
│  NgramInvertedIndex                                              │
│  (version=4 时使用新实现，version≤3 保留旧 Tantivy 实现)            │
├──────────────────────────────────────────────────────────────────┤
│                    InvertedIndexEngine (新)                       │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │ IndexWriter  │  │ IndexReader  │  │ Query Operators        │  │
│  │  addDoc()    │  │  termQuery() │  │  TermQuery             │  │
│  │  commit()    │  │  rangeQuery()│  │  RangeQuery            │  │
│  │  finish()    │  │  prefixQ()   │  │  PrefixQuery           │  │
│  │             │  │  regexQ()    │  │  RegexQuery            │  │
│  │             │  │  matchQ()    │  │  PhraseQuery           │  │
│  │             │  │  phraseQ()   │  │  NgramQuery            │  │
│  └─────────────┘  └──────────────┘  └────────────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│                    Core Data Structures                           │
│  ┌─────────────────┐  ┌──────────────────────────────────────┐  │
│  │ TermDictionary   │  │ PostingList (roaring::Roaring)       │  │
│  │  In-mem: std::map│  │ PositionalPostingList (doc+positions)│  │
│  │  Mmap: sorted[]  │  │                                      │  │
│  │  + binary search │  │                                      │  │
│  └─────────────────┘  └──────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│                    Serialization (V3 Entry Format)                │
│  WriteEntries() → IndexEntryWriter                               │
│  LoadEntries() ← IndexEntryReader                                │
├──────────────────────────────────────────────────────────────────┤
│                    Tokenizer Bridge (保留 Rust FFI)               │
│  tokenize(params, text) → [(token, position), ...]               │
└──────────────────────────────────────────────────────────────────┘
```

---

## 模块详细设计

### Module 1: PostingList

**文件**: `internal/core/src/index/inverted_index/PostingList.h`

```cpp
// 非位置感知的 posting list，用于标量/TextMatch/Ngram
class PostingList {
public:
    void add(uint32_t doc_id);
    void addBatch(const uint32_t* doc_ids, size_t n);

    // 查询
    bool contains(uint32_t doc_id) const;
    uint64_t count() const;

    // 集合运算
    void unionWith(const PostingList& other);
    void intersectWith(const PostingList& other);

    // 结果输出
    void writeToBitset(TargetBitmap* bitset) const;

    // 序列化（用于写入 V3 entry）
    size_t serializedSize() const;
    void serialize(uint8_t* buf) const;
    static PostingList deserialize(const uint8_t* buf, size_t len);

    // 从 mmap 区域直接构建（零拷贝 view）
    static PostingList fromMmap(const uint8_t* buf, size_t len);

    const roaring::Roaring& bitmap() const { return bitmap_; }

private:
    roaring::Roaring bitmap_;  // 复用已有 conan 依赖
};
```

### Module 2: PositionalPostingList

**文件**: `internal/core/src/index/inverted_index/PositionalPostingList.h`

```cpp
// 位置感知的 posting list，用于 PhraseMatch
class PositionalPostingList {
public:
    void add(uint32_t doc_id, uint32_t position);

    // 获取某个 doc 的所有位置
    const std::vector<uint32_t>& getPositions(uint32_t doc_id) const;

    // 获取包含此 term 的所有 doc
    const roaring::Roaring& docIds() const;

    // 序列化：doc_ids + per-doc position arrays
    size_t serializedSize() const;
    void serialize(uint8_t* buf) const;
    static PositionalPostingList deserialize(const uint8_t* buf, size_t len);

private:
    roaring::Roaring doc_ids_;
    std::unordered_map<uint32_t, std::vector<uint32_t>> positions_;
};
```

**Per-term 序列化布局**：
```
[roaring_size: u32] [roaring_data: bytes]        ← doc_ids bitmap
[num_docs: u32]
For each doc (按 doc_id 升序):
  [doc_id: u32] [num_positions: u32] [positions: u32[]]
```

### Module 3: TermDictionary（双态设计）

**文件**: `internal/core/src/index/inverted_index/TermDictionary.h`

关键设计：**两种模式**——构建时用 `std::map`，mmap 加载时用 **排序数组 + 二分查找**。

```cpp
// === 构建时使用的可写 term dictionary ===
template <typename T>
class WritableTermDictionary {
public:
    void addTerm(const T& term, uint32_t doc_id);

    std::shared_ptr<PostingList> find(const T& term) const;

    // std::map 的迭代器，支持 range scan
    using Iterator = typename std::map<T, std::shared_ptr<PostingList>>::const_iterator;
    std::pair<Iterator, Iterator> range(
        const T& lower, bool lb_inclusive,
        const T& upper, bool ub_inclusive) const;
    Iterator lowerBound(const T& value) const;
    Iterator begin() const;
    Iterator end() const;
    size_t termCount() const;

private:
    std::map<T, std::shared_ptr<PostingList>> terms_;
};

// === Mmap 加载的只读 term dictionary ===
// 磁盘布局（排序数组，连续内存，mmap 友好）：
//
// 定长类型 (I64/F64/Bool):
//   [num_terms: u32]
//   [terms: T[num_terms]]                    ← 排序数组，直接 reinterpret_cast
//   [posting_offsets: u64[num_terms]]         ← 每个 term 的 posting list 在 data 区的偏移
//   [posting_sizes: u32[num_terms]]           ← 每个 posting list 的序列化大小
//   [posting_data: bytes...]                  ← 所有 posting list 连续存储
//
// 变长类型 (String):
//   [num_terms: u32]
//   [string_offsets: u32[num_terms]]          ← 每个 string 在 string_pool 中的偏移
//   [string_lengths: u32[num_terms]]          ← 每个 string 的长度
//   [string_pool: bytes...]                   ← 所有 string 连续存储（已排序）
//   [posting_offsets: u64[num_terms]]
//   [posting_sizes: u32[num_terms]]
//   [posting_data: bytes...]
//
// 查找: binary search on sorted terms → O(log n)
// Range: lower_bound + sequential scan → O(log n + k)

template <typename T>
class MmapTermDictionary {
public:
    MmapTermDictionary(const uint8_t* data, size_t size);

    // 精确查找 → binary search → O(log n)
    // 返回 PostingList view（不拷贝，直接引用 mmap 区域的 roaring 数据）
    std::optional<PostingList> find(const T& term) const;

    // Range scan → lower_bound + scan → O(log n + k)
    template <typename Func>
    void rangeForEach(const T* lower, bool lb_inclusive,
                      const T* upper, bool ub_inclusive,
                      Func&& callback) const;

    // Prefix scan (string only)
    template <typename Func>
    void prefixForEach(const std::string& prefix, Func&& callback) const;

    // Full scan (for regex)
    template <typename Func>
    void forEach(Func&& callback) const;

    size_t termCount() const;

private:
    // 定长类型：直接 cast mmap 指针
    const T* sorted_terms_;           // sorted_terms_[i] = i-th term
    const uint64_t* posting_offsets_; // posting_offsets_[i] = offset in posting_data
    const uint32_t* posting_sizes_;   // posting_sizes_[i] = size of posting list
    const uint8_t* posting_data_;     // raw posting list data
    uint32_t num_terms_;

    // 变长类型额外字段
    const uint32_t* string_offsets_;
    const uint32_t* string_lengths_;
    const char* string_pool_;

    // binary search 辅助
    size_t findIndex(const T& term) const;       // 精确匹配，返回 num_terms_ 表示未找到
    size_t lowerBoundIndex(const T& term) const;  // 第一个 >= term 的位置
    size_t upperBoundIndex(const T& term) const;  // 第一个 > term 的位置

    // 从 mmap 数据中获取第 i 个 term 的 PostingList view
    PostingList getPostingAt(size_t index) const;
    T getTermAt(size_t index) const;  // 定长直接读，变长从 string_pool 读
};
```

### Module 4: 序列化与 V3 Entry 格式

**文件**: `internal/core/src/index/inverted_index/InvertedIndexSerializer.h`

索引数据作为 V3 format 的 named entries 来序列化，通过 `IndexEntryWriter`/`IndexEntryReader` 交互。

```cpp
// V3 entry 命名规范
static constexpr const char* ENTRY_TERM_DICT = "INVERTED_TERM_DICT";
static constexpr const char* ENTRY_POSTING_DATA = "INVERTED_POSTING_DATA";
static constexpr const char* ENTRY_POSITION_DATA = "INVERTED_POSITION_DATA";  // phrase match only
static constexpr const char* ENTRY_NULL_BITMAP = "INVERTED_NULL_BITMAP";
static constexpr const char* ENTRY_META = "INVERTED_META";

struct InvertedIndexMeta {
    uint32_t version = 1;
    uint8_t field_type;          // I64, F64, Bool, Keyword, Text, JSON
    bool has_positions;          // true for text with phrase match
    uint32_t num_terms;
    uint32_t num_docs;
    std::string analyzer_params; // for text fields
};

class InvertedIndexSerializer {
public:
    // 序列化到 V3 entries
    template <typename T>
    static void writeEntries(IndexEntryWriter& writer,
                             const WritableTermDictionary<T>& dict,
                             const InvertedIndexMeta& meta);

    // 序列化位置感知索引
    static void writePositionalEntries(
        IndexEntryWriter& writer,
        const WritablePositionalTermDictionary& dict,
        const InvertedIndexMeta& meta);

    // 从 V3 entries 加载
    template <typename T>
    static std::unique_ptr<MmapTermDictionary<T>> loadEntries(
        IndexEntryReader& reader, bool use_mmap);
};
```

**在 V3 文件中的布局**：
```
V3 File:
  [MVSIDXV3 magic]
  [INVERTED_META entry]           ← JSON: {version, field_type, has_positions, ...}
  [INVERTED_TERM_DICT entry]      ← sorted terms + posting offsets (mmap 友好的排序数组)
  [INVERTED_POSTING_DATA entry]   ← serialized roaring bitmaps
  [INVERTED_POSITION_DATA entry]  ← (optional) per-doc position arrays
  [INVERTED_NULL_BITMAP entry]    ← null offset roaring bitmap
  [Directory Table]
  [Footer]
```

### Module 5: Query Operators

**文件**: `internal/core/src/index/inverted_index/QueryOperators.h`

```cpp
// 统一的查询接口，兼容 WritableTermDictionary 和 MmapTermDictionary
// 使用模板或虚函数多态

// IN 查询: 多 term 查找 → union
template <typename Dict>
void termsQuery(const Dict& dict, const auto* terms, size_t n, TargetBitmap* result);

// Range 查询: lower_bound → upper_bound 范围 scan → union
template <typename Dict>
void rangeQuery(const Dict& dict,
                const auto* lower, bool lb_inclusive,
                const auto* upper, bool ub_inclusive,
                TargetBitmap* result);

// Prefix 查询 (string only): lower_bound(prefix) → 连续 scan while starts_with
template <typename Dict>
void prefixQuery(const Dict& dict, const std::string& prefix, TargetBitmap* result);

// Regex 查询 (string only): full scan + regex_match
template <typename Dict>
void regexQuery(const Dict& dict, const std::string& pattern, TargetBitmap* result);

// Match 查询: tokenize → multi-term OR union
template <typename Dict>
void matchQuery(const Dict& dict,
                const std::vector<std::string>& tokens,
                size_t min_should_match,
                TargetBitmap* result);

// Phrase 查询: positional intersection with slop
void phraseQuery(const PositionalTermDictionary& dict,
                 const std::vector<std::pair<uint32_t, std::string>>& terms_with_positions,
                 uint32_t slop,
                 TargetBitmap* result);

// Ngram 查询: ngram tokenize → AND intersection
template <typename Dict>
void ngramMatchQuery(const Dict& dict,
                     const std::vector<std::string>& ngrams,
                     TargetBitmap* result);
```

### Module 6: PhraseQuery 算法

```cpp
// phraseQuery 核心算法
void phraseQuery(
    const PositionalTermDictionary& dict,
    const std::vector<std::pair<uint32_t/*query_pos*/, std::string>>& terms_with_positions,
    uint32_t slop,
    TargetBitmap* result)
{
    // 1. 获取每个 query term 的 positional posting list
    std::vector<const PositionalPostingList*> postings;
    for (auto& [qpos, term] : terms_with_positions) {
        auto pl = dict.find(term);
        if (!pl) return;  // term 不存在，整体无匹配
        postings.push_back(pl.get());
    }

    // 2. 计算 doc_id 交集（所有 query term 都必须出现在 doc 中）
    roaring::Roaring candidates = postings[0]->docIds();
    for (size_t i = 1; i < postings.size(); i++) {
        candidates &= postings[i]->docIds();
    }

    // 3. 对每个候选 doc，检查 position 约束
    for (auto doc_id : candidates) {
        if (checkPositions(postings, terms_with_positions, doc_id, slop)) {
            result->set(doc_id);
        }
    }
}

// Position 检查算法（与 Tantivy phrase_match_slop.rs 一致）：
// 1. 计算 max_query_pos = max(所有 query term 的 position)
// 2. 对每个 query term，构造 adjusted_positions:
//    adjusted_pos = data_pos + (max_query_pos - query_pos)
//    这样如果文档中有完美匹配，所有 adjusted_pos 会相等
// 3. 使用 min-heap 跟踪当前各 list 的最小值和全局最大值
// 4. 不断弹出最小值，推入该 list 的下一个元素，更新最大值
// 5. 当 (current_max - current_min) <= slop 时，匹配成功
bool checkPositions(
    const std::vector<const PositionalPostingList*>& postings,
    const std::vector<std::pair<uint32_t, std::string>>& terms_with_positions,
    uint32_t doc_id,
    uint32_t slop);
```

### Module 7: TokenizerBridge（精简 Tantivy FFI）

**文件**: `internal/core/src/index/inverted_index/TokenizerBridge.h`

保留 Tantivy 仅作为 tokenizer 使用。基于已有的 `tokenizer_c.h` API 封装：

```cpp
struct Token {
    std::string text;
    uint32_t position;
};

class TokenizerBridge {
public:
    explicit TokenizerBridge(const std::string& analyzer_params,
                             const std::string& extra_info = "");
    ~TokenizerBridge();

    // 分词，返回 (token_text, position) 列表
    std::vector<Token> tokenize(const std::string& text) const;

    // Ngram 分词（纯 C++ 实现，不需要 Rust）
    static std::vector<std::string> ngramTokenize(
        const std::string& text, size_t min_gram, size_t max_gram);

private:
    CTokenizer tokenizer_;
};
```

### Module 8: IndexWriter / IndexReader

```cpp
// 新的 C++ 倒排索引（取代 InvertedIndexTantivy<T>）
template <typename T>
class NativeInvertedIndex : public ScalarIndex<T> {
public:
    // ScalarIndex<T> interface
    void Build(size_t n, const T* values, const bool* valid_data = nullptr) override;
    void Build(const Config& config = {}) override;

    BinarySet Serialize(const Config& config) override;
    void Load(const BinarySet& index_binary, const Config& config = {}) override;
    void Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    // V3 format integration
    void WriteEntries(IndexEntryWriter& writer) override;
    void LoadEntries(IndexEntryReader& reader) override;

    const TargetBitmap In(size_t n, const T* values) override;
    const TargetBitmap NotIn(size_t n, const T* values) override;
    const TargetBitmap Range(const T& value, OpType op) override;
    const TargetBitmap Range(const T& lower, bool lb_inc,
                             const T& upper, bool ub_inc) override;
    const TargetBitmap IsNull() override;
    TargetBitmap IsNotNull() override;

    // String-only
    const TargetBitmap RegexQuery(const std::string& pattern) override;
    const TargetBitmap PatternMatch(const std::string& pattern, OpType op) override;

private:
    // Build phase: writable
    WritableTermDictionary<T> writable_dict_;

    // Load phase: mmap (sealed) or writable (growing)
    std::variant<
        std::unique_ptr<MmapTermDictionary<T>>,
        std::unique_ptr<WritableTermDictionary<T>>
    > dict_;

    PostingList null_bitmap_;
    uint32_t num_docs_ = 0;
};
```

### Module 9: JSON Term 编码

```cpp
// JSON 字段的 term 编码 path + type + value
// 编码格式: path_bytes + '\0' + type_tag(1 byte) + value_bytes
// 按字典序可比较，同 path 的 terms 连续存储

enum class JsonValueType : uint8_t {
    Bool = 0,
    I64 = 1,
    F64 = 2,
    String = 3,
};

struct JsonTermKey {
    std::string path;
    JsonValueType type;
    std::string encoded_value;  // 按字典序可比较的编码

    bool operator<(const JsonTermKey& other) const;

    static JsonTermKey fromI64(const std::string& path, int64_t value);
    static JsonTermKey fromF64(const std::string& path, double value);
    static JsonTermKey fromBool(const std::string& path, bool value);
    static JsonTermKey fromString(const std::string& path, const std::string& value);
};

// JSON 倒排索引使用 WritableTermDictionary<JsonTermKey> / MmapTermDictionary<JsonTermKey>
```

---

## Mmap 设计详解

### 核心问题

mmap 的内存是连续映射的，无法使用 `std::map`（红黑树依赖散乱的指针节点）。

### 解决方案：排序数组 + 二分查找

| 操作 | std::map (构建时) | 排序数组 (mmap 时) |
|---|---|---|
| 精确查找 | O(log n) | O(log n) binary search |
| Range 查询 | O(log n + k) | O(log n + k) lower_bound + scan |
| 插入 | O(log n) | N/A (只读) |
| 内存布局 | 散乱指针 | 连续数组，mmap 友好 |
| Cache 性能 | 差 (pointer chasing) | 好 (sequential memory) |

定长类型（I64/F64/Bool）的 mmap 布局：
```
┌──────────────────────────────────────────────────────────┐
│ sorted_terms: [T, T, T, ...]        ← num_terms 个定长值  │ ← binary search 作用于此
│ posting_offsets: [u64, u64, ...]     ← 对应的 posting 偏移 │
│ posting_sizes: [u32, u32, ...]      ← 对应的 posting 大小  │
│ posting_data: [roaring0, roaring1, ...] ← 序列化的 bitmaps │ ← 按 offset 直接读取
└──────────────────────────────────────────────────────────┘
```

变长类型（String）的 mmap 布局：
```
┌──────────────────────────────────────────────────────────┐
│ string_offsets: [u32, u32, ...]      ← 字符串在 pool 中偏移│
│ string_lengths: [u32, u32, ...]      ← 字符串长度          │
│ string_pool: "aardvark\0apple\0..."  ← 所有字符串连续存储   │ ← binary search 时按
│ posting_offsets: [u64, u64, ...]                          │    offset+length 读取比较
│ posting_sizes: [u32, u32, ...]                            │
│ posting_data: [roaring0, roaring1, ...]                   │
└──────────────────────────────────────────────────────────┘
```

### 与 V3 Format 的集成

mmap 数据以 V3 entry 的形式存储。加载时：
1. V3 `IndexEntryReader` 读取 entry 到内存或 mmap 到本地文件
2. `MmapTermDictionary` 直接在 mmap'd memory 上操作（zero-copy）
3. Roaring bitmap 支持 `roaring::Roaring::portableDeserializeFrozen()` 从 mmap 区域直接构建视图

---

## 集成方案

### 替换策略

创建新的索引类（`NativeInvertedIndex<T>` 等），保留旧类不变：

| version ≤ 3 | version = 4 (新) |
|---|---|
| `InvertedIndexTantivy<T>` (保留) | `NativeInvertedIndex<T>` (新增) |
| `TextMatchIndex` (Tantivy 版, 保留) | `NativeTextMatchIndex` (新增) |
| `JsonInvertedIndex<T>` (Tantivy 版, 保留) | `NativeJsonInvertedIndex<T>` (新增) |
| `NgramInvertedIndex` (Tantivy 版, 保留) | `NativeNgramInvertedIndex` (新增) |

### 保留的 Rust FFI

仅保留 tokenizer 相关的 FFI 调用：
- `create_tokenizer` / `free_tokenizer`
- `create_token_stream` / token stream iteration
- `compute_phrase_match_slop`（可选，也可用 C++ 重写）

### 延迟清理

Tantivy 索引代码（`tantivy-wrapper.h`, index_reader/writer Rust 文件）在 **所有用户升级到 version ≥ 4 后** 再删除，不急。

---

## 关键文件清单

### 新增文件

```
internal/core/src/index/inverted_index/
├── PostingList.h                    # roaring bitmap wrapper
├── PostingList.cpp
├── PositionalPostingList.h          # position-aware posting list
├── PositionalPostingList.cpp
├── TermDictionary.h                 # WritableTermDictionary + MmapTermDictionary
├── InvertedIndexSerializer.h        # V3 entry 序列化
├── InvertedIndexSerializer.cpp
├── QueryOperators.h                 # term/range/prefix/regex/phrase/ngram queries
├── QueryOperators.cpp
├── PhraseQuery.h                    # phrase query algorithm
├── PhraseQuery.cpp
├── TokenizerBridge.h                # slim C++ wrapper over Rust tokenizer FFI
├── TokenizerBridge.cpp
├── NativeInvertedIndex.h            # 标量倒排索引（取代 InvertedIndexTantivy<T>）
├── NativeInvertedIndex.cpp
├── NativeTextMatchIndex.h           # 文本匹配索引
├── NativeTextMatchIndex.cpp
├── NativeJsonInvertedIndex.h        # JSON 倒排索引
├── NativeJsonInvertedIndex.cpp
├── NativeNgramInvertedIndex.h       # Ngram 倒排索引
├── NativeNgramInvertedIndex.cpp
├── JsonTermKey.h                    # JSON path+type+value encoding
└── JsonTermKey.cpp
```

### 修改文件

```
internal/core/src/index/IndexFactory.cpp     # 添加 version=4 的 factory 路由
internal/core/src/index/HybridScalarIndex.cpp # 添加 version=4 的 index type 选择
internal/core/CMakeLists.txt                  # 添加新文件编译
pkg/common/common.go                          # CurrentScalarIndexEngineVersion = 4
```

### 不删除（保留旧代码兼容）

```
internal/core/src/index/InvertedIndexTantivy.h/cpp     # 保留，version ≤ 3 使用
internal/core/src/index/TextMatchIndex.h/cpp            # 保留
internal/core/src/index/JsonInvertedIndex.h             # 保留
internal/core/src/index/NgramInvertedIndex.h/cpp        # 保留
internal/core/thirdparty/tantivy/tantivy-wrapper.h      # 保留
internal/core/thirdparty/tantivy/tantivy-binding/       # 保留所有
```

---

## 实现分阶段计划

**前置条件**: scalar-index-unified-format (V3) PR 已合并。

### Phase 1: 核心数据结构 + 标量查询
1. 实现 `PostingList`（roaring bitmap 封装）
2. 实现 `WritableTermDictionary<T>`（std::map 封装）
3. 实现标量查询：`termsQuery`, `rangeQuery`, `prefixQuery`, `regexQuery`
4. 单元测试

### Phase 2: 磁盘格式 + Mmap
1. 实现 `InvertedIndexSerializer`：通过 V3 `IndexEntryWriter` 序列化
2. 实现 `MmapTermDictionary<T>`：排序数组 + 二分查找，零拷贝
3. 支持 sealed segment 的 build → serialize → load 全流程
4. 单元测试

### Phase 3: 集成标量倒排
1. 实现 `NativeInvertedIndex<T>`，实现 `ScalarIndex<T>` 接口
2. 在 `IndexFactory` / `HybridScalarIndex` 中添加 version=4 路由
3. Null offset 处理
4. 运行已有单元测试 + 集成测试

### Phase 4: TokenizerBridge
1. 实现 `TokenizerBridge` C++ 封装（基于已有 `tokenizer_c.h`）
2. 验证所有 tokenizer 类型可正常工作

### Phase 5: TextMatch + PhraseMatch
1. 实现 `PositionalPostingList` + `WritablePositionalTermDictionary`
2. 实现 `matchQuery`（OR union）+ `matchQueryWithMinimum`
3. 实现 `phraseQuery` 算法（positional intersection with slop）
4. 实现 `NativeTextMatchIndex`
5. Growing segment 支持（in-memory dict + commit/reload）

### Phase 6: JSON 倒排
1. 实现 `JsonTermKey` 编码/比较
2. 实现 `NativeJsonInvertedIndex<T>`
3. 支持 json_exist_query（path 存在性检查）

### Phase 7: Ngram 倒排
1. 实现 C++ ngram tokenization
2. 实现 `NativeNgramInvertedIndex`（AND intersection）

### Phase 8: 版本切换
1. 更新 `pkg/common/common.go`: `CurrentScalarIndexEngineVersion = 4`
2. 端到端测试
3. 性能对比测试

---

## 验证方案

### 单元测试
- 每个 Phase 都包含对应模块的单元测试
- 重点测试：
  - range query 边界条件（inclusive/exclusive, unbounded）
  - phrase query slop 行为（exact match, slop=0, slop>0）
  - JSON path 编码正确性
  - mmap MmapTermDictionary binary search 边界条件
  - null handling

### 集成测试
- 运行已有的所有 scalar index 相关测试
- 构建整个 core 模块并确保编译通过
- version=3 和 version=4 的索引可以共存（不同 segment 不同版本）

### 性能验证
- 对比 Tantivy 版本和 C++ 版本在以下场景的性能：
  - 高基数 I64 字段的 range query
  - 大量 string IN 查询（>10K terms）
  - TextMatch 查询（中文 jieba 分词）
  - PhraseMatch 查询（不同 slop 值）
  - mmap 冷启动 + 热查询延迟
