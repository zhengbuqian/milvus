# Milvus Tantivy 使用情况全面审计报告

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [Tantivy Rust 依赖与版本](#2-tantivy-rust-依赖与版本)
3. [Rust Binding 层 — 核心功能实现](#3-rust-binding-层--核心功能实现)
4. [C FFI 接口层 — 70+ 导出函数](#4-c-ffi-接口层--70-导出函数)
5. [C++ Wrapper 层](#5-c-wrapper-层)
6. [Segcore C API 层 (Go CGO 入口)](#6-segcore-c-api-层-go-cgo-入口)
7. [Go 层使用方式](#7-go-层使用方式)
8. [使用场景一：标量倒排索引 (Inverted Index)](#8-使用场景一标量倒排索引-inverted-index)
9. [使用场景二：全文搜索 (Full-Text Search / BM25)](#9-使用场景二全文搜索-full-text-search--bm25)
10. [使用场景三：JSON 字段索引与查询](#10-使用场景三json-字段索引与查询)
11. [使用场景四：N-gram 索引 (子串匹配)](#11-使用场景四n-gram-索引-子串匹配)
12. [使用场景五：Tokenizer/Analyzer 体系](#12-使用场景五tokenizeranalyzer-体系)
13. [使用场景六：Growing Segment 实时索引](#13-使用场景六growing-segment-实时索引)
14. [数据类型支持矩阵](#14-数据类型支持矩阵)
15. [查询类型支持矩阵](#15-查询类型支持矩阵)
16. [关键数据流路径](#16-关键数据流路径)
17. [替换影响范围评估](#17-替换影响范围评估)
18. [文件索引](#18-文件索引)

---

## 1. 总体架构概览

Tantivy 在 Milvus 中的集成采用四层架构：

```
┌─────────────────────────────────────────────────────────────────┐
│  Go 层 (CGO)                                                    │
│  - analyzer/canalyzer   → Tokenizer 创建/验证/Token流           │
│  - indexcgowrapper      → BuildTextIndex / BuildJsonKeyIndex    │
│  - textmatch            → ComputePhraseMatchSlop                │
│  - function/bm25        → BM25 稀疏向量生成                     │
│  - querynodev2/segments → 索引加载                              │
└────────────────────────────┬────────────────────────────────────┘
                             │ CGO (C API)
┌────────────────────────────▼────────────────────────────────────┐
│  Segcore C API 层                                               │
│  - tokenizer_c.h/cpp        phrase_match_c.h/cpp                │
│  - token_stream_c.h/cpp     load_index_c.h/cpp                  │
│  - indexbuilder/index_c.h/cpp                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │ C++ 调用
┌────────────────────────────▼────────────────────────────────────┐
│  C++ Index Wrapper 层                                           │
│  - TantivyIndexWrapper      (tantivy-wrapper.h)                 │
│  - InvertedIndexTantivy<T>  (标量倒排索引)                      │
│  - TextMatchIndex           (全文搜索索引)                      │
│  - JsonInvertedIndex<T>     (JSON 路径索引)                     │
│  - NgramInvertedIndex       (N-gram 子串索引)                   │
│  - JsonKeyStats             (JSON Key 统计)                     │
│  - Tokenizer / TokenStream  (分词器 C++ 封装)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │ C FFI (extern "C")
┌────────────────────────────▼────────────────────────────────────┐
│  Rust Tantivy Binding 层 (libtantivy_binding.a)                 │
│  - 79 个 Rust 源文件, ~4625 行代码                              │
│  - 70+ 导出 C FFI 函数                                          │
│  - 支持 Tantivy V5 (0.21.1) 和 V7 (latest) 双版本              │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│  Tantivy Crate (zilliztech fork)                                │
│  - tantivy (latest/HEAD)  — V7                                  │
│  - tantivy-5 (0.21.1-fix4) — V5 (兼容 Milvus 2.4.x)           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Tantivy Rust 依赖与版本

**Cargo.toml**: `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml`

### 直接 Tantivy 依赖

| Crate | 版本/来源 | 用途 |
|---|---|---|
| `tantivy` (V7) | `git: zilliztech/tantivy.git` HEAD | 主版本，默认使用 |
| `tantivy-5` (V5) | `0.21.1-fix4` (tag) | 向后兼容 Milvus 2.4.x |

### 文本分析相关依赖

| Crate | 版本 | 用途 |
|---|---|---|
| `lindera` | `0.42.4` | 日语/韩语/中文分词 (5种词典) |
| `jieba-rs` | `0.6.8` | 中文分词 (结巴) |
| `icu_segmenter` | `2.0.0` | ICU 标准文本分段 |
| `lingua` | `1.7.1` | 语言检测 |
| `whatlang` | `0.16.4` | 语言识别 |
| `regex` | `1.11.1` | 正则表达式 |
| `fancy-regex` | `0.14.0` | 高级正则 (回溯支持) |
| `tonic` | `0.13.1` | gRPC (远程 tokenizer) |

### Lindera 词典特性

- `lindera-ipadic` — 日语 IPA 词典
- `lindera-ipadic-neologd` — 日语新词处理
- `lindera-unidic` — 日语统一词典
- `lindera-ko-dic` — 韩语词典
- `lindera-cc-cedict` — 中英词典

### 编译配置

- **Rust 工具链**: 1.89
- **输出**: `libtantivy_binding.a` (静态库)
- **C 头文件生成**: `cbindgen 0.27.0`
- **Proto 编译**: `tonic-build 0.13.0` (tokenizer.proto)
- **内存预算**: 每线程 15MB

---

## 3. Rust Binding 层 — 核心功能实现

**目录**: `internal/core/thirdparty/tantivy/tantivy-binding/src/` (79 文件, ~4625 行)

### 3.1 核心模块结构

```
src/
├── lib.rs                        # 入口, TantivyIndexVersion 枚举
├── data_type.rs                  # TantivyDataType: Text/Keyword/I64/F64/Bool/JSON
├── error.rs                      # 错误类型与转换
│
├── index_writer.rs               # IndexWriterWrapper 枚举(V5/V7)与 trait
├── index_writer_c.rs             # 写入 C FFI
├── index_writer_v5/              # V5 实现
│   ├── index_writer.rs           # IndexWriterWrapperImpl (V5)
│   ├── index_writer_text.rs      # 文本索引写入 (V5)
│   ├── index_writer_json_key_stats.rs
│   └── analyzer.rs               # V5 analyzer 适配
├── index_writer_v7/              # V7 实现
│   ├── index_writer.rs           # IndexWriterWrapperImpl (V7)
│   ├── index_writer_text.rs      # 文本索引写入 (V7)
│   └── index_writer_json_key_stats.rs
│
├── index_reader.rs               # IndexReaderWrapper: 查询主入口
├── index_reader_c.rs             # 读取 C FFI
├── index_reader_text.rs          # 文本查询实现 (match/phrase)
├── index_reader_text_c.rs        # 文本查询 C FFI
│
├── index_ngram_writer.rs         # N-gram 索引构建
├── index_ngram_writer_c.rs       # N-gram C FFI
├── index_json_key_stats_writer.rs # JSON Key 统计
├── index_json_key_stats_writer_c.rs
│
├── analyzer/                     # 分析器子系统
│   ├── analyzer.rs               # 分析器构建逻辑
│   ├── build_in_analyzer.rs      # 内置分析器定义
│   ├── tokenizers/               # 9 种 tokenizer 实现
│   ├── filter/                   # 8 种 filter 实现
│   ├── dict/                     # Lindera 词典支持
│   ├── options/                  # 配置与运行时选项
│   └── gen/                      # protobuf 生成代码
│
├── bitset_wrapper.rs             # Bitset 操作封装
├── docid_collector.rs            # DocID 收集器
├── milvus_id_collector.rs        # Milvus 专用 ID 收集
├── vec_collector.rs              # 向量结果收集
├── direct_bitset_collector.rs    # 直接 bitset 操作
│
├── phrase_match_slop.rs          # 短语匹配 slop 计算
├── token_stream_c.rs             # Token 流 C FFI
├── tokenizer_c.rs                # Tokenizer C FFI
├── array.rs                      # RustArray 定义
├── hashmap_c.rs                  # HashMap C FFI
├── string_c.rs                   # 字符串 C FFI
└── util.rs / util_c.rs           # 工具函数
```

### 3.2 使用的核心 Tantivy 类型

**索引与文档类:**
- `tantivy::Index` — 索引容器
- `tantivy::IndexWriter` — 索引写入器
- `tantivy::IndexReader` — 索引读取器
- `tantivy::Searcher` — 搜索器 (从 IndexReader 构建)
- `tantivy::Schema` / `SchemaBuilder` — 模式定义
- `tantivy::Document` / `TantivyDocument` — 文档表示
- `tantivy::Field` — 字段标识符
- `tantivy::Term` — 搜索词项
- `tantivy::Directory` — 索引文件存储

**查询类:**
- `tantivy::query::BooleanQuery` — 布尔组合查询
- `tantivy::query::TermQuery` — 单词项查询
- `tantivy::query::TermSetQuery` — 多词项批量查询
- `tantivy::query::RangeQuery` — 范围查询
- `tantivy::query::RegexQuery` — 正则查询
- `tantivy::query::ExistsQuery` — 字段存在性查询

**收集器与结果:**
- `tantivy::Collector` / `SegmentCollector` — 结果收集接口
- `tantivy::fastfield::Column` — 快速字段列访问

**分词:**
- `tantivy::tokenizer::TextAnalyzer` — 文本分析器
- `tantivy::tokenizer::Tokenizer` — 分词器接口
- `tantivy::tokenizer::NgramTokenizer` — N-gram 分词
- `tantivy::TokenStream` — Token 序列

### 3.3 V5 vs V7 差异

| 特性 | V5 (0.21.1) | V7 (latest) |
|---|---|---|
| Writer 类型 | `Either<IndexWriter, SingleSegmentIndexWriter>` | `IndexWriter` |
| Doc ID | 显式 `doc_id` 字段 (i64 fast field) | 支持原生 `user_specified_doc_id` |
| 用途 | 向后兼容 Milvus 2.4.x | 默认版本 |

### 3.4 结果回调机制

查询结果通过 C 函数指针回调返回：

```rust
pub(crate) type SetBitsetFn = extern "C" fn(*mut c_void, *const u32, usize);
```

Rust 侧执行查询 → 收集匹配的 doc_id → 通过 `SetBitsetFn` 回调将结果写入 C++ 侧的 bitset。

---

## 4. C FFI 接口层 — 70+ 导出函数

**头文件**: `internal/core/thirdparty/tantivy/tantivy-binding/include/tantivy-binding.h`

### 4.1 数据结构

```cpp
// 数据类型枚举
enum class TantivyDataType : uint8_t { Text, Keyword, I64, F64, Bool, JSON };

// Token 结构
struct TantivyToken {
    const char *token;
    int64_t start_offset, end_offset, position, position_length;
};

// 返回值类型
struct RustResult { bool success; Value value; const char *error; };

// Bitset 回调
using SetBitsetFn = void(*)(void*, const uint32_t*, uintptr_t);
```

### 4.2 完整 FFI 函数分类

#### A. 内存管理 (6 个)

| 函数 | 用途 |
|---|---|
| `free_rust_array` | 释放 uint32 数组 |
| `free_rust_array_i64` | 释放 int64 数组 |
| `free_rust_string_array` | 释放字符串数组 |
| `free_rust_result` | 释放 RustResult |
| `free_rust_error` | 释放错误字符串 |
| `free_rust_string` | 释放 Rust 字符串 |

#### B. 索引写入器创建与生命周期 (8 个)

| 函数 | 用途 |
|---|---|
| `tantivy_create_index` | 创建标量索引写入器 |
| `tantivy_create_index_with_single_segment` | 单 segment 标量写入器 (V5 兼容) |
| `tantivy_create_text_writer` | 创建文本索引写入器 (带 analyzer) |
| `tantivy_create_json_key_stats_writer` | 创建 JSON Key 统计写入器 |
| `tantivy_create_ngram_writer` | 创建 N-gram 写入器 |
| `tantivy_free_index_writer` | 释放写入器 |
| `tantivy_finish_index` | 完成索引构建 |
| `tantivy_commit_index` | 提交索引变更 |

#### C. 索引读取器操作 (6 个)

| 函数 | 用途 |
|---|---|
| `tantivy_load_index` | 从磁盘加载索引 (支持 mmap) |
| `tantivy_reload_index` | 重新加载索引 (growing segment 用) |
| `tantivy_free_index_reader` | 释放读取器 |
| `tantivy_index_count` | 获取文档数量 |
| `tantivy_index_size_bytes` | 获取索引大小 |
| `tantivy_create_reader_from_writer` | 从写入器创建读取器 |

#### D. Terms 查询 (5 个)

| 函数 | 数据类型 |
|---|---|
| `tantivy_terms_query_bool` | bool |
| `tantivy_terms_query_i64` | int64 |
| `tantivy_terms_query_f64` | double |
| `tantivy_terms_query_keyword` | string |
| `tantivy_term_query_keyword_i64` | keyword→i64 映射 |

#### E. Range 查询 (12 个, 4 类型 × 3 变体)

每种数据类型 (bool / i64 / f64 / keyword) 各有:
- `tantivy_lower_bound_range_query_*` — 下界范围
- `tantivy_upper_bound_range_query_*` — 上界范围
- `tantivy_range_query_*` — 双界范围

#### F. 模式查询 (4 个)

| 函数 | 用途 |
|---|---|
| `tantivy_prefix_query_keyword` | 前缀匹配 |
| `tantivy_regex_query` | 正则匹配 |
| `tantivy_match_query` | 全文匹配 (含 min_should_match) |
| `tantivy_phrase_match_query` | 短语匹配 (含 slop) |

#### G. JSON 查询 (12 个)

| 函数 | 用途 |
|---|---|
| `tantivy_json_term_query_i64/f64/bool/keyword` | JSON 路径词项查询 (4 个) |
| `tantivy_json_exist_query` | JSON 路径存在性 |
| `tantivy_json_range_query_i64/f64/bool/keyword` | JSON 路径范围查询 (4 个) |
| `tantivy_json_regex_query` | JSON 路径正则 |
| `tantivy_json_prefix_query` | JSON 路径前缀 |
| `tantivy_json_term_query_i64` | JSON 路径 i64 词项 |

#### H. N-gram 查询 (3 个)

| 函数 | 用途 |
|---|---|
| `tantivy_ngram_match_query` | N-gram 匹配查询 |
| `tantivy_ngram_tokenize` | N-gram 分词 (返回按 doc_freq 排序) |
| `tantivy_ngram_term_posting_list` | N-gram 词项 posting list |

#### I. 标量数据写入 (9 个 + 8 个单 segment 变体)

支持 `int8/int16/int32/int64/f32/f64/bool/string/json` 的 `tantivy_index_add_*` 系列。
单 segment 变体用于 V5 兼容模式。

#### J. 数组数据写入 (9 个 + 8 个单 segment 变体)

支持 `int8/int16/int32/int64/f32/f64/bool/keyword/json` 数组的 `tantivy_index_add_array_*` 系列。

#### K. JSON Key 统计写入 (1 个)

`tantivy_index_add_json_key_stats_data_by_batch` — 批量写入 JSON key 统计数据。

#### L. Tokenizer 操作 (9 个)

| 函数 | 用途 |
|---|---|
| `tantivy_create_analyzer` | 创建分析器 |
| `tantivy_validate_analyzer` | 验证分析器配置 |
| `tantivy_clone_analyzer` | 克隆分析器 |
| `tantivy_free_analyzer` | 释放分析器 |
| `tantivy_set_analyzer_options` | 设置全局分析器选项 |
| `tantivy_register_tokenizer` | 注册自定义 tokenizer |
| `tantivy_create_token_stream` | 创建 token 流 |
| `tantivy_token_stream_advance` | 推进 token 流 |
| `tantivy_token_stream_get_token` / `_get_detailed_token` | 获取 token |

#### M. 工具函数 (5 个)

| 函数 | 用途 |
|---|---|
| `tantivy_compute_phrase_match_slop` | 计算短语匹配 slop |
| `tantivy_index_exist` | 检查索引是否存在 |
| `create_hashmap` / `hashmap_set_value` / `free_hashmap` | HashMap 操作 |

---

## 5. C++ Wrapper 层

### 5.1 TantivyIndexWrapper (底层封装)

**文件**: `internal/core/thirdparty/tantivy/tantivy-wrapper.h`

直接封装所有 C FFI 函数，提供 C++ 模板方法：

```cpp
class TantivyIndexWrapper {
    void* writer_;       // Rust IndexWriter 指针
    void* reader_;       // Rust IndexReader 指针
    std::string path_;
    bool finished_;
    bool load_in_mmap_;
    std::string analyzer_extra_info_;

    // 模板方法 — 根据类型分发到不同 FFI 函数
    template<typename T> void add_data(const T* data, int64_t len, int64_t offset);
    template<typename T> void add_array_data(const T* data, int64_t len, int64_t offset);
    template<typename T> void terms_query(const T* terms, int len, void* bitset);
    template<typename T> void lower_bound_range_query(T bound, bool inclusive, void* bitset);
    template<typename T> void upper_bound_range_query(T bound, bool inclusive, void* bitset);
    template<typename T> void range_query(T lower, T upper, bool lb_inc, bool ub_inc, void* bitset);
    // ... 更多模板方法
};
```

### 5.2 InvertedIndexTantivy\<T\> (标量倒排索引)

**文件**: `internal/core/src/index/InvertedIndexTantivy.h/cpp`

继承 `ScalarIndex<T>`，支持类型：`bool, int8, int16, int32, int64, float, double, string`

```
关键方法:
├── Build()              → 从原始数据构建索引
├── BuildWithFieldData() → 从 FieldData 构建 (含 null 处理)
├── Load()               → 从磁盘加载 (支持 mmap)
├── Upload()             → 序列化并上传到存储
├── In()                 → terms_query (∈ 集合)
├── NotIn()              → 取反 terms_query
├── Range()              → range_query (比较运算)
├── PrefixMatch()        → prefix_query (仅 string)
├── RegexQuery()         → regex_query (仅 string)
├── IsNull() / IsNotNull() → null 值判断
└── build_index_for_array() → 数组字段索引
```

### 5.3 TextMatchIndex (全文搜索索引)

**文件**: `internal/core/src/index/TextMatchIndex.h/cpp`

继承 `InvertedIndexTantivy<string>`，扩展全文搜索能力：

```
关键方法:
├── AddTextSealed()        → 向 sealed segment 添加文本
├── AddTextsGrowing()      → 向 growing segment 批量添加
├── BuildIndexFromFieldData() → 从 FieldData 构建
├── Finish() / Commit()    → 完成/提交索引
├── Reload()               → 重新加载 (growing segment 同步)
├── CreateReader()         → 从 writer 创建 reader
├── RegisterAnalyzer()     → 注册自定义分词器
├── MatchQuery()           → 全文匹配查询 (BM25 相关)
└── PhraseMatchQuery()     → 短语匹配查询
```

**特殊功能:**
- 支持 Growing Segment 的并发读写 (读写锁)
- 基于时间的 commit 间隔机制
- 资源追踪 (`TextMatchIndexHolder`)

### 5.4 JsonInvertedIndex\<T\> (JSON 路径索引)

**文件**: `internal/core/src/index/JsonInvertedIndex.h/cpp`

继承 `InvertedIndexTantivy<T>`，支持 JSON 嵌套路径查询：

```
关键方法:
├── build_index_for_json() → 提取 JSON 路径值并索引
├── Exists()               → JSON 路径存在性 bitmap
└── 序列化: null_offset_ + non_exist_offsets_ 分别存储
```

### 5.5 NgramInvertedIndex (N-gram 子串索引)

**文件**: `internal/core/src/index/NgramInvertedIndex.h/cpp`

继承 `InvertedIndexTantivy<string>`，支持子串匹配：

```
关键方法:
├── BuildWithFieldData()   → 构建 + 计算平均行大小
├── BuildWithJsonFieldData() → JSON 路径 ngram 索引
├── ExecutePhase1()        → ngram 查询返回候选集
├── ExecutePhase2()        → 后过滤验证阶段
├── CanHandleLiteral()     → 检查字面量长度 >= min_gram
└── 策略选择: 迭代式 vs 批量式 (基于命中率和平均行大小)
```

### 5.6 JsonKeyStats (JSON Key 统计)

**文件**: `internal/core/src/index/json_stats/JsonKeyStats.h/cpp`

使用 Tantivy ngram writer 构建 JSON key 统计索引：
- 可配置 shredding columns 和 ratio 阈值
- Parquet writer 存储统计
- BSON 倒排索引支持

### 5.7 Bitset 回调机制

**文件**: `internal/core/src/index/Utils.h`

```cpp
// Sealed segment: 所有 doc_id < bitset.size()
void SetBitsetSealed(void* bitset, const uint32_t* doc_id, uintptr_t n);

// Growing segment: 可能有 doc_id >= bitset.size() (并发插入)
void SetBitsetGrowing(void* bitset, const uint32_t* doc_id, uintptr_t n);
```

---

## 6. Segcore C API 层 (Go CGO 入口)

### 6.1 Tokenizer C API

**文件**: `internal/core/src/segcore/tokenizer_c.h/cpp`

| C 函数 | 内部调用 |
|---|---|
| `set_tokenizer_option(params)` | `milvus::tantivy::set_tokenizer_options()` |
| `create_tokenizer(params, extra_info)` | `milvus::tantivy::Tokenizer(params, extra)` |
| `clone_tokenizer(ptr)` | `tokenizer->Clone()` |
| `validate_tokenizer(params, extra_info)` | `milvus::tantivy::validate_analyzer()` |
| `free_tokenizer(ptr)` | 析构 Tokenizer |

### 6.2 Token Stream C API

**文件**: `internal/core/src/segcore/token_stream_c.h/cpp`

| C 函数 | 内部调用 |
|---|---|
| `create_token_stream(tokenizer, text)` | `tokenizer->CreateTokenStream(text)` |
| `token_stream_advance(stream)` | `stream->Advance()` |
| `token_stream_get_token(stream)` | `stream->get_token_no_copy()` |
| `token_stream_get_detailed_token(stream)` | `stream->get_detailed_token()` |
| `free_token_stream(stream)` | 析构 TokenStream |

### 6.3 Phrase Match C API

**文件**: `internal/core/src/segcore/phrase_match_c.h/cpp`

| C 函数 | 内部调用 |
|---|---|
| `compute_phrase_match_slop_c(params, query, data, *slop)` | `milvus::tantivy::compute_phrase_match_slop()` |

### 6.4 Index Build C API

**文件**: `internal/core/src/indexbuilder/index_c.h/cpp`

| C 函数 | 内部调用 |
|---|---|
| `BuildTextIndex(result, serialized_info, len)` | 创建 `TextMatchIndex` → `Build()` → `Upload()` |
| `BuildJsonKeyIndex(result, serialized_info, len)` | 创建 `JsonKeyStats` → `Build()` → `Upload()` |

### 6.5 Load Index C API

**文件**: `internal/core/src/segcore/load_index_c.h/cpp`

| C 函数 | 内部调用 |
|---|---|
| `FinishLoadIndexInfo(info, serialized, len)` | 解析 Proto → 填充 LoadIndexInfo |
| `AppendIndexV2(trace, info)` | `LoadIndexData()` → 创建 `SealedIndexTranslator` → Cache |

---

## 7. Go 层使用方式

### 7.1 Analyzer CGO 封装

**目录**: `internal/util/analyzer/canalyzer/`

```go
// 创建分析器
analyzer, err := canalyzer.NewAnalyzer(analyzerParams, extraInfo)

// 获取 token 流
stream := analyzer.NewTokenStream(text)
defer stream.Destroy()
for stream.Advance() {
    token := stream.Token()          // 获取 token 文本
    detail := stream.DetailedToken() // 获取详细信息(偏移量、位置)
}
```

**关键方法:**
- `InitOptions()` — 初始化 tokenizer 全局选项 (Lindera 下载 URL、资源路径)
- `ValidateAnalyzer()` — 验证 analyzer 配置有效性
- `UpdateGlobalResourceInfo()` — 更新全局资源信息 (词典文件等)

### 7.2 BM25 函数

**文件**: `internal/util/function/bm25_function.go`

```go
type BM25FunctionRunner struct {
    tokenizer   analyzer.Analyzer  // Tantivy tokenizer
    // ...
}

func (v *BM25FunctionRunner) BatchRun(inputs ...any) ([]any, error) {
    // 1. 对每条文本创建 token stream
    // 2. 遍历 token, hash 化: typeutil.HashString2LessUint32(token)
    // 3. 构建稀疏向量 map[uint32]float32
    // 4. 返回 SparseFloatArray
}
```

### 7.3 Index Build (DataNode 侧)

**文件**: `internal/datanode/index/task_stats.go`

```go
func (st *statsTask) createTextIndex(ctx context.Context, ...) error {
    // 1. 下载分析器资源文件 (词典)
    // 2. 构建 BuildIndexInfo proto
    // 3. 调用 indexcgowrapper.CreateTextIndex()  → C.BuildTextIndex()
    // 4. 存储结果到 TextIndexStats
}
```

### 7.4 Index Load (QueryNode 侧)

**文件**: `internal/querynodev2/segments/segment_loader.go`

通过 `LoadIndexInfo` proto 传递索引加载参数，调用 `C.FinishLoadIndexInfo()` → `C.AppendIndexV2()`。

### 7.5 字段 Schema 配置

```go
// 用户侧 type_params:
"enable_match": "true"          // 启用文本匹配
"enable_analyzer": "true"       // 启用分析器
"analyzer_params": "{...}"      // 分析器 JSON 配置
"multi_analyzer_params": "{...}" // 多分析器配置
```

---

## 8. 使用场景一：标量倒排索引 (Inverted Index)

### 场景描述

对标量字段 (int, float, bool, string) 构建倒排索引，加速过滤查询。

### 数据流

```
用户创建 INVERTED 索引 → IndexFactory::CreateIndex()
    → InvertedIndexTantivy<T> 实例化
    → TantivyIndexWrapper::add_data<T>()
    → tantivy_index_add_*() [FFI]
    → Rust: IndexWriter::add_document()
    → tantivy_finish_index() [FFI]
    → 上传索引文件
```

### 查询流

```
表达式解析 (field > 10) → ScalarIndex<T>::Range()
    → InvertedIndexTantivy<T>::Range()
    → TantivyIndexWrapper::range_query<T>()
    → tantivy_range_query_i64() [FFI]
    → Rust: Searcher::search() + DocIdCollector
    → SetBitsetFn 回调写入结果 bitset
```

### 涉及的 Tantivy 组件

| Tantivy 组件 | 使用方式 |
|---|---|
| `Index` | 索引容器，每个标量字段一个 |
| `IndexWriter` | 写入文档 |
| `Schema` / `SchemaBuilder` | 定义 field (i64/f64/bool/keyword) + 可选 doc_id |
| `IndexReader` / `Searcher` | 读取与搜索 |
| `TermQuery` / `TermSetQuery` | terms 查询 (IN 操作) |
| `RangeQuery` | 范围查询 (>, <, >=, <=, BETWEEN) |
| `RegexQuery` | 正则查询 (string 字段) |
| `fastfield::Column` | 快速字段列访问 |
| `DocIdCollector` | 收集匹配文档 ID → bitset |
| `Directory` (FSDirectory) | 磁盘存储，支持 mmap |

### 支持的数据类型

int8, int16, int32, int64 (→ 映射为 Tantivy I64), float, double (→ F64), bool (→ Bool), string (→ Keyword)

---

## 9. 使用场景二：全文搜索 (Full-Text Search / BM25)

### 场景描述

对文本字段执行全文搜索，支持 TextMatch 和 PhraseMatch 查询。结合 BM25 稀疏向量实现相关性排序。

### 索引构建流

```
DataNode flush 时:
    → statsTask.createTextIndex()  [Go]
    → C.BuildTextIndex()           [CGO]
    → TextMatchIndex 创建          [C++]
        → TantivyIndexWrapper(text_writer + analyzer)
        → tantivy_create_text_writer(field, path, version, analyzer_name, params) [FFI]
        → Rust: 创建 Schema(TEXT field + 指定 analyzer) + IndexWriter
    → BuildIndexFromFieldData()
        → tantivy_index_add_string() for each document [FFI]
    → Finish() → Upload()
```

### 查询流 — TextMatch

```
用户: text_match(field, "query text")
    → Plan: UnaryRangeExpr(op=TextMatch)
    → TextMatchIndex::MatchQuery("query text", min_should_match=1)
    → TantivyIndexWrapper::match_query()
    → tantivy_match_query(ptr, query, min_should_match, bitset) [FFI]
    → Rust: 分词 query → 构建 BooleanQuery(OR) of TermQuery → Searcher::search()
```

### 查询流 — PhraseMatch

```
用户: phrase_match(field, "exact phrase", slop)
    → Plan: UnaryRangeExpr(op=PhraseMatch)
    → TextMatchIndex::PhraseMatchQuery("exact phrase", slop)
    → TantivyIndexWrapper::phrase_match_query()
    → tantivy_phrase_match_query(ptr, query, slop, bitset) [FFI]
    → Rust: 分词 → PhraseQuery with slop → Searcher::search()
```

### BM25 稀疏向量生成

```
Go 侧 BM25FunctionRunner:
    → analyzer.NewTokenStream(text)          → 调用 Tantivy tokenizer
    → stream.Advance() + stream.Token()      → 获取每个 token
    → HashString2LessUint32(token)           → hash 化
    → 构建 SparseFloatArray(hash → tf)       → 作为稀疏向量
    → SparseInvertedIndex(BM25) 索引检索
```

### Growing Segment 特殊处理

Growing segment 中的 TextMatchIndex 支持：
- 实时写入 (`AddTextsGrowing()`)
- 并发读写 (读写锁)
- 定期 commit (`Commit()`)
- Reload 同步 (`Reload()`)
- 从 writer 创建 reader (`CreateReader()`)

### 涉及的 Tantivy 组件

| Tantivy 组件 | 使用方式 |
|---|---|
| `Schema` + `TextFieldIndexing` | 文本字段配置 (analyzer, index_option) |
| `TextAnalyzer` | 文本分析 (tokenize + filter) |
| `IndexWriter` | 写入分词后的文档 |
| `BooleanQuery` | TextMatch: OR 组合多个 TermQuery |
| 短语查询 | PhraseMatch: 带 slop 的短语查询 |
| `ReloadPolicy` | Growing segment 索引重载 |

---

## 10. 使用场景三：JSON 字段索引与查询

### 场景描述

对 JSON 类型字段按路径建立倒排索引，支持嵌套路径查询。

### 索引构建

```
JsonInvertedIndex<T> 构建:
    → build_index_for_json()
    → 遍历 JSON 数据, 提取指定路径值
    → TantivyIndexWrapper::add_data<T>() 或 add_json()
    → 记录 null_offset_ 和 non_exist_offsets_
```

### 查询类型

| 查询 | FFI 函数 | 用途 |
|---|---|---|
| 词项查询 | `tantivy_json_term_query_i64/f64/bool/keyword` | JSON 路径值精确匹配 |
| 范围查询 | `tantivy_json_range_query_*` | JSON 路径值范围 |
| 存在性 | `tantivy_json_exist_query` | 检查 JSON 路径是否存在 |
| 正则 | `tantivy_json_regex_query` | JSON 路径值正则匹配 |
| 前缀 | `tantivy_json_prefix_query` | JSON 路径值前缀匹配 |

### JSON Key 统计

`JsonKeyStats` 使用 Tantivy 的 ngram writer 构建 JSON key 的统计索引，用于 shredding 列优化决策。

### 涉及的 Tantivy 组件

| Tantivy 组件 | 使用方式 |
|---|---|
| `Schema` + JSON field | JSON 字段定义 |
| `Term` with JSON path | 构建带路径的搜索词项 |
| `ExistsQuery` | 路径存在性检查 |
| `RangeQuery` | JSON 路径范围查询 |
| `RegexQuery` | JSON 路径正则 |

---

## 11. 使用场景四：N-gram 索引 (子串匹配)

### 场景描述

对字符串字段构建 N-gram 索引，实现高效的子串 (LIKE '%pattern%') 匹配。

### 两阶段执行策略

```
Phase 1: N-gram 候选集生成
    → 将查询字符串切分为 ngram
    → tantivy_ngram_tokenize() 获取按 doc_freq 排序的 ngram
    → 选择策略: 迭代式 vs 批量式 (基于命中率/平均行大小)
    → tantivy_ngram_term_posting_list() 或 tantivy_ngram_match_query()
    → 返回候选 doc_id 集合

Phase 2: 后过滤验证
    → 对候选集逐行验证是否真正包含目标子串
    → 消除 false positive (ngram 匹配不等于子串匹配)
```

### 涉及的 Tantivy 组件

| Tantivy 组件 | 使用方式 |
|---|---|
| `NgramTokenizer` | N-gram 分词 (min_gram, max_gram) |
| `IndexWriter` | 写入 ngram 分词后的文档 |
| `TermQuery` | 单个 ngram posting list 查询 |
| `BooleanQuery` | 多 ngram AND 组合 |

---

## 12. 使用场景五：Tokenizer/Analyzer 体系

### 内置 Tokenizer (9 种)

| Tokenizer | 实现 | 语言/用途 |
|---|---|---|
| Standard | Tantivy 内置 | 英文/通用 |
| Jieba | `jieba-rs` | 中文分词 |
| Lindera | `lindera` | 日/韩/中 (5 种词典) |
| ICU | `icu_segmenter` | Unicode 标准分段 |
| CharGroup | 自定义 | 基于字符分组 |
| LanguageIdent | `whatlang` | 语言识别后分词 |
| Ngram | Tantivy 内置 | N-gram 分词 |
| NgramWithChars | 自定义 | N-gram + 字符支持 |
| gRPC | `tonic` | 远程分词服务 |

### 内置 Analyzer (3 种预配置)

| Analyzer | Tokenizer | Filters |
|---|---|---|
| `standard_analyzer` | Whitespace | Lowercase + 可选 StopWords |
| `chinese_analyzer` | Jieba | CnAlphaNumOnly + StopWords |
| `english_analyzer` | Standard | Lowercase + Stemmer + English StopWords |

### 内置 Filter (8 种)

| Filter | 用途 |
|---|---|
| `LowerCaser` | 转小写 |
| `StopWordFilter` | 停用词过滤 (支持自定义列表) |
| `Stemmer` | 词干提取 |
| `CnAlphaNumOnlyFilter` | 中文字母数字过滤 |
| `DecompounderFilter` | 复合词分解 |
| `RegexFilter` | 正则过滤 |
| `RemovePunctFilter` | 去除标点 |
| `SynonymFilter` | 同义词扩展 |

### 分析器配置示例

```json
// 标准英文
{"tokenizer": "standard"}

// 中文 Jieba
{"tokenizer": "jieba"}

// 自定义分析器
{
  "tokenizer": "standard",
  "filter": [
    {"type": "lowercase"},
    {"type": "stop", "stop_words": ["the", "a", "an"]}
  ]
}

// 多分析器
{
  "by_field": "language_field",
  "alias": {"en": "english", "zh": "chinese"},
  "analyzers": {
    "english": {"tokenizer": "standard"},
    "chinese": {"tokenizer": "jieba"}
  }
}
```

---

## 13. 使用场景六：Growing Segment 实时索引

### 场景描述

Growing segment 是 Milvus 中的实时写入 segment，需要支持边写入边查询。

### 实现方式

TextMatchIndex 在 growing segment 中的工作方式：

1. **创建**: `TextMatchIndex(in_ram=true)` — 纯内存索引
2. **写入**: `AddTextsGrowing()` — 批量添加文本
3. **提交**: `Commit()` — 定期提交使新数据可查
4. **查询**: 通过读写锁保证并发安全
5. **重载**: `Reload()` — 重新加载 reader 获取最新数据
6. **从 Writer 创建 Reader**: `CreateReader()` — growing segment 特有

### 并发控制

```cpp
// Growing segment 中的 TextMatchIndex
std::shared_mutex mutex_;

// 写入: 独占锁
void AddTextsGrowing(...) { std::unique_lock lock(mutex_); ... }

// 查询: 共享锁
void MatchQuery(...) { std::shared_lock lock(mutex_); ... }
```

---

## 14. 数据类型支持矩阵

| Milvus 类型 | Tantivy 映射 | 倒排索引 | 文本索引 | JSON 索引 | N-gram 索引 |
|---|---|---|---|---|---|
| Bool | `Bool` | ✅ | - | ✅ | - |
| Int8 | `I64` | ✅ | - | ✅ | - |
| Int16 | `I64` | ✅ | - | ✅ | - |
| Int32 | `I64` | ✅ | - | ✅ | - |
| Int64 | `I64` | ✅ | - | ✅ | - |
| Float | `F64` | ✅ | - | ✅ | - |
| Double | `F64` | ✅ | - | ✅ | - |
| VarChar/String | `Keyword` | ✅ | - | ✅ | ✅ |
| VarChar (analyzed) | `Text` | - | ✅ | - | - |
| JSON | `JSON` | ✅ | - | ✅ | ✅ |
| Array(*) | 元素类型 | ✅ | - | - | - |

---

## 15. 查询类型支持矩阵

| 查询类型 | 标量倒排 | 文本索引 | JSON 索引 | N-gram | 对应 FFI 函数 |
|---|---|---|---|---|---|
| Terms (IN) | ✅ | - | ✅ | - | `tantivy_terms_query_*` |
| Range (>, <, BETWEEN) | ✅ | - | ✅ | - | `tantivy_range_query_*` |
| Prefix | ✅ (string) | - | ✅ | - | `tantivy_prefix_query_keyword` |
| Regex | ✅ (string) | - | ✅ | - | `tantivy_regex_query` |
| TextMatch | - | ✅ | - | - | `tantivy_match_query` |
| PhraseMatch | - | ✅ | - | - | `tantivy_phrase_match_query` |
| Exists | - | - | ✅ | - | `tantivy_json_exist_query` |
| N-gram Match | - | - | - | ✅ | `tantivy_ngram_match_query` |
| N-gram Posting | - | - | - | ✅ | `tantivy_ngram_term_posting_list` |

---

## 16. 关键数据流路径

### 16.1 索引构建 (Sealed Segment)

```
Go: DataNode flush
    → statsTask.createTextIndex()
    → indexcgowrapper.CreateTextIndex(BuildIndexInfo proto)
    → CGO: C.BuildTextIndex(serialized_proto)
    → C++: 解析 proto → TextMatchIndex / InvertedIndexTantivy
    → C++: TantivyIndexWrapper 构建
    → FFI: tantivy_create_text_writer / tantivy_create_index
    → Rust: 创建 Schema + IndexWriter
    → FFI: tantivy_index_add_string / tantivy_index_add_*
    → Rust: IndexWriter::add_document()
    → FFI: tantivy_finish_index
    → Rust: IndexWriter::finalize()
    → C++: Upload() → 写入存储
```

### 16.2 索引加载 (QueryNode)

```
Go: segment_loader.loadSegmentIndex()
    → LoadIndexInfo.appendLoadIndexInfo(LoadIndexInfo proto)
    → CGO: C.FinishLoadIndexInfo(serialized_proto)
    → CGO: C.AppendIndexV2()
    → C++: LoadIndexData()
    → C++: 创建 SealedIndexTranslator / TextMatchIndexTranslator
    → C++: 注册到 CacheLayer Manager
    → (延迟加载) IndexFactory::CreateIndex() → InvertedIndexTantivy / TextMatchIndex
    → FFI: tantivy_load_index(path, mmap)
    → Rust: Index::open_in_dir() + IndexReader::reload()
```

### 16.3 查询执行

```
Go: QueryNode 接收查询
    → Plan 解析生成表达式树
    → C++: ExecExprVisitor 遍历表达式
    → C++: ScalarIndex<T>::In/Range/PrefixMatch/...
    → C++: InvertedIndexTantivy::*() → TantivyIndexWrapper::*()
    → FFI: tantivy_terms_query_* / tantivy_range_query_* / ...
    → Rust: Searcher::search(query, DocIdCollector)
    → Rust: SetBitsetFn 回调写入 bitset
    → C++: TargetBitmap 返回给表达式求值
```

### 16.4 BM25 Tokenize (Go 侧独立使用)

```
Go: BM25FunctionRunner.BatchRun(texts)
    → analyzer.NewTokenStream(text)
    → CGO: C.create_token_stream(tokenizer_ptr, c_text)
    → C++: Tokenizer::CreateTokenStream()
    → FFI: tantivy_create_token_stream()
    → Rust: TextAnalyzer::token_stream()
    → Go: stream.Advance() / stream.Token()
    → CGO: C.token_stream_advance() / C.token_stream_get_token()
    → Rust: TokenStream::advance() / TokenStream::token().text
    → Go: HashString2LessUint32(token) → SparseFloatArray
```

---

## 17. 替换影响范围评估

### 17.1 必须替换的核心能力

| 能力 | 复杂度 | 说明 |
|---|---|---|
| **倒排索引存储引擎** | 极高 | Posting List, Term Dictionary, Fast Fields |
| **多类型 Schema** | 高 | I64/F64/Bool/Keyword/Text/JSON 6 种类型 |
| **文本分析器框架** | 高 | Tokenizer + Filter 可组合管线 |
| **查询引擎** | 高 | Term/Range/Boolean/Regex/Phrase 查询 |
| **索引 Writer/Reader** | 高 | 多线程写入、segment merge、mmap 读取 |
| **结果收集器** | 中 | DocId → Bitset 的高效收集 |
| **N-gram 分词与索引** | 中 | 子串匹配的两阶段查询 |
| **JSON 路径查询** | 中 | 嵌套路径的 Term/Range/Exists/Regex |
| **短语匹配** | 中 | Position-aware 查询 + Slop |
| **V5/V7 双版本兼容** | 低 | V5 可以在替换后逐步废弃 |

### 17.2 需要修改的文件清单

**Rust 层 (完全替换):**
- `internal/core/thirdparty/tantivy/tantivy-binding/` — 79 个文件全部

**C++ 层 (接口适配):**
- `internal/core/thirdparty/tantivy/tantivy-wrapper.h` — 主 Wrapper
- `internal/core/src/index/InvertedIndexTantivy.h/cpp` — 标量倒排
- `internal/core/src/index/TextMatchIndex.h/cpp` — 全文搜索
- `internal/core/src/index/JsonInvertedIndex.h/cpp` — JSON 索引
- `internal/core/src/index/NgramInvertedIndex.h/cpp` — N-gram
- `internal/core/src/index/json_stats/JsonKeyStats.h/cpp` — JSON 统计
- `internal/core/src/index/IndexFactory.cpp` — 索引工厂
- `internal/core/src/index/HybridScalarIndex.cpp` — 混合索引
- `internal/core/src/index/Utils.h` — Bitset 回调
- `internal/core/src/segcore/tokenizer_c.cpp` — Tokenizer C API
- `internal/core/src/segcore/token_stream_c.cpp` — TokenStream C API
- `internal/core/src/segcore/phrase_match_c.cpp` — Phrase Match
- `internal/core/src/segcore/Utils.cpp` — LoadIndexData
- `internal/core/src/indexbuilder/index_c.cpp` — BuildTextIndex
- `internal/core/src/segcore/storagev1translator/` — 索引加载翻译器

**Go 层 (若保持 C FFI 接口不变则无需修改):**
- `internal/util/analyzer/canalyzer/` — CGO 绑定 (接口不变则无需改)
- `internal/util/indexcgowrapper/` — 索引构建 (接口不变则无需改)
- `internal/util/textmatch/` — 短语匹配 (接口不变则无需改)

### 17.3 可复用的分词器依赖

如果自行实现索引引擎但保留分词能力，以下 crate 可继续使用：
- `jieba-rs` (中文分词)
- `lindera` (日韩中分词)
- `icu_segmenter` (ICU 分段)
- `whatlang` / `lingua` (语言检测)
- `regex` / `fancy-regex` (正则)

### 17.4 推荐替换策略

1. **保持 C FFI 接口不变** — 最小化上层修改，只替换 Rust 实现
2. **保持 `TantivyIndexWrapper` 接口不变** — C++ 层无需修改
3. **分阶段替换**:
   - Phase 1: 标量倒排索引 (最常用, 纯数值/string 类型)
   - Phase 2: 文本搜索 (TextMatch/PhraseMatch)
   - Phase 3: JSON 路径查询
   - Phase 4: N-gram 索引
4. **废弃 V5 支持** — 简化实现，减少维护负担

---

## 18. 文件索引

### Rust Binding 层
| 文件 | 说明 |
|---|---|
| `internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml` | Rust 依赖配置 |
| `internal/core/thirdparty/tantivy/tantivy-binding/build.rs` | 构建脚本 (cbindgen + tonic) |
| `internal/core/thirdparty/tantivy/tantivy-binding/src/lib.rs` | 库入口 |
| `internal/core/thirdparty/tantivy/tantivy-binding/src/index_writer.rs` | Writer 枚举与 trait |
| `internal/core/thirdparty/tantivy/tantivy-binding/src/index_reader.rs` | Reader 与查询实现 |
| `internal/core/thirdparty/tantivy/tantivy-binding/src/index_reader_text.rs` | 文本查询实现 |
| `internal/core/thirdparty/tantivy/tantivy-binding/src/analyzer/` | 分析器子系统 (9 tokenizer + 8 filter) |
| `internal/core/thirdparty/tantivy/tantivy-binding/include/tantivy-binding.h` | 生成的 C FFI 头文件 |

### C++ 层
| 文件 | 说明 |
|---|---|
| `internal/core/thirdparty/tantivy/tantivy-wrapper.h` | TantivyIndexWrapper 主封装 |
| `internal/core/thirdparty/tantivy/rust-array.h` | RustArray RAII 封装 |
| `internal/core/thirdparty/tantivy/rust-binding.h` | 编译辅助宏 |
| `internal/core/src/index/InvertedIndexTantivy.h/cpp` | 标量倒排索引 |
| `internal/core/src/index/TextMatchIndex.h/cpp` | 全文搜索索引 |
| `internal/core/src/index/JsonInvertedIndex.h/cpp` | JSON 路径索引 |
| `internal/core/src/index/NgramInvertedIndex.h/cpp` | N-gram 子串索引 |
| `internal/core/src/index/json_stats/JsonKeyStats.h/cpp` | JSON Key 统计 |
| `internal/core/src/index/IndexFactory.cpp` | 索引工厂 |
| `internal/core/src/index/HybridScalarIndex.cpp` | 混合索引选择 |
| `internal/core/src/index/Utils.h` | SetBitset 回调 |

### Segcore C API 层
| 文件 | 说明 |
|---|---|
| `internal/core/src/segcore/tokenizer_c.h/cpp` | Tokenizer C API |
| `internal/core/src/segcore/token_stream_c.h/cpp` | TokenStream C API |
| `internal/core/src/segcore/phrase_match_c.h/cpp` | PhraseMatch C API |
| `internal/core/src/segcore/load_index_c.h/cpp` | 索引加载 C API |
| `internal/core/src/segcore/Utils.cpp` | LoadIndexData 实现 |
| `internal/core/src/segcore/storagev1translator/` | 索引加载翻译器 |
| `internal/core/src/indexbuilder/index_c.h/cpp` | BuildTextIndex / BuildJsonKeyIndex |

### Go 层
| 文件 | 说明 |
|---|---|
| `internal/util/analyzer/canalyzer/c_analyzer.go` | Tokenizer CGO 绑定 |
| `internal/util/analyzer/canalyzer/c_analyzer_factory.go` | Tokenizer 工厂 |
| `internal/util/analyzer/canalyzer/c_token_stream.go` | TokenStream CGO 绑定 |
| `internal/util/function/bm25_function.go` | BM25 函数实现 |
| `internal/util/function/multi_analyzer_bm25_function.go` | 多分析器 BM25 |
| `internal/util/indexcgowrapper/index.go` | CreateTextIndex / CreateJsonKeyStats |
| `internal/util/textmatch/phrase_match.go` | ComputePhraseMatchSlop |
| `internal/datanode/index/task_stats.go` | 索引构建任务 |
| `internal/querynodev2/segments/segment_loader.go` | 索引加载 |
| `internal/querynodev2/segments/load_index_info.go` | LoadIndexInfo 封装 |

### 构建配置
| 文件 | 说明 |
|---|---|
| `internal/core/thirdparty/tantivy/CMakeLists.txt` | CMake 构建配置 |
| `internal/core/thirdparty/tantivy/tantivy-binding/cbindgen.toml` | C 头文件生成配置 |

### 测试文件
| 文件 | 说明 |
|---|---|
| `internal/core/thirdparty/tantivy/test.cpp` | C++ 单元测试 (多类型查询) |
| `internal/core/thirdparty/tantivy/bench.cpp` | C++ 性能测试 |
| `internal/core/thirdparty/tantivy/text_demo.cpp` | 全文搜索 demo |
| `internal/core/thirdparty/tantivy/jieba_demo.cpp` | 中文分词 demo |
| `internal/core/thirdparty/tantivy/tokenizer_demo.cpp` | Tokenizer demo |
| `tests/go_client/testcases/full_text_search_test.go` | Go 全文搜索测试 |
| `tests/go_client/testcases/phrase_match_test.go` | Go 短语匹配测试 |
| `tests/python_client/testcases/test_full_text_search.py` | Python 全文搜索测试 |
| `tests/python_client/testcases/test_phrase_match.py` | Python 短语匹配测试 |
| `tests/python_client/testcases/indexes/test_ngram.py` | Python N-gram 测试 |
