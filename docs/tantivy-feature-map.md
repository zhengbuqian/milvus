# Tantivy 功能大图：Milvus 使用 vs 抛弃

本文档基于对 Milvus 中 Tantivy 使用情况的全面审计，整理 Tantivy 作为搜索引擎提供的完整能力集合，并标注 Milvus 实际使用了哪些、抛弃了哪些。

## 总览

| 类别 | Tantivy 提供 | Milvus 使用 | 使用程度 |
|------|-------------|------------|---------|
| Schema & 字段类型 | 7 种字段类型 | 4 种 | 部分 |
| 索引选项 | 3 级索引记录 | 2 级 | 部分 |
| 查询类型 | 15+ 种查询 | 7 种 | 少量 |
| 评分/排序 | BM25/TF-IDF/自定义评分 | 0 | **完全不用** |
| 收集器 | TopDocs/Count/多种 | 自定义 bitset | 替代实现 |
| 多字段索引 | 支持 | 不用 | **完全不用** |
| 文档存储 | Stored Fields | 不用 | **完全不用** |
| Facet/聚合 | 支持 | 不用 | **完全不用** |
| 高亮/摘要 | Snippet Generator | 不用 | **完全不用** |
| 模糊搜索 | FuzzyTermQuery | 不用 | **完全不用** |
| 分词器 | 丰富的 pipeline | 深度使用 | **核心依赖** |

---

## 1. Schema 与字段类型

### Tantivy 提供的字段类型

| 字段类型 | Milvus 使用 | 说明 |
|---------|-----------|------|
| `I64` (i64) | **使用** | 标量倒排索引：INT8/16/32/64 → Tantivy I64 |
| `F64` (f64) | **使用** | 标量倒排索引：FLOAT/DOUBLE → Tantivy F64 |
| `Bool` | **使用** | 标量倒排索引：BOOL → Tantivy Bool |
| `Text` (String) | **使用** | 标量倒排索引(Keyword)、TextMatch、Ngram |
| `JSON` | **使用** | JSON 倒排索引 |
| `IpAddr` | **不用** | Tantivy 支持 IPv4/IPv6 字段类型，Milvus 不使用 |
| `Bytes` | **不用** | Tantivy 支持二进制字段类型，Milvus 不使用 |
| `DateTime` | **不用** | Tantivy 支持日期时间字段（含精度控制），Milvus 不使用 |
| `Facet` | **不用** | Tantivy 支持层级分面字段（如 `/category/sports/`），Milvus 不使用 |
| `U64` (u64) | **不用** | Tantivy 支持无符号整数字段，Milvus 统一用 I64 |

### 多字段索引

| 能力 | Tantivy | Milvus |
|------|---------|--------|
| 单个 Index 索引多个字段 | **支持** | **不用** — 每个 Tantivy 索引只包含 1 个业务字段 |
| 跨字段查询 | **支持** | **不用** |
| Schema 动态字段 | **支持** | **不用** |

**Milvus 的实际做法**：每个 segment 的每个字段创建独立的 Tantivy 索引。Schema 始终只有 1-2 个字段：
- 标量索引：`[field_name]`（一个字段）
- 文本索引：`[field_name, doc_id]`（业务字段 + fast field 存储 Milvus row_id）

```rust
// 标量索引 schema（V7）
schema_builder.add_i64_field(field_name, NumericOptions::default().set_indexed())  // 只有 1 个字段

// 文本索引 schema
let field = schema_builder.add_text_field(field_name, option);
let id_field = schema_builder.add_i64_field("doc_id", FAST);  // 仅用于 row_id 映射
```

---

## 2. 索引选项（Index Record Options）

### Tantivy 提供的索引记录级别

| 级别 | 存储内容 | Milvus 使用 | 场景 |
|------|---------|-----------|------|
| `Basic` | 仅 doc_id list | **使用** | 标量倒排、TextMatch(match_query)、JSON、Ngram |
| `WithFreqs` | doc_id + 词频 | **不用** | Tantivy 支持，Milvus 不使用 |
| `WithFreqsAndPositions` | doc_id + 词频 + 位置 | **使用** | 仅 PhraseMatch 场景 |

**关键发现**：Milvus 从不使用词频（term frequency），即使在 `WithFreqsAndPositions` 场景下，词频信息也被忽略，只使用位置信息。

### 其他索引选项

| 选项 | Tantivy | Milvus |
|------|---------|--------|
| Field Norms | 支持（用于 BM25 长度归一化） | **显式关闭** `set_fieldnorms(false)` |
| Stored Fields | 支持（原文存储） | **不用** |
| FAST (Columnar) | 支持 | **仅 `doc_id` 字段使用** — 用于 Tantivy internal doc_id → Milvus row_id 映射 |
| INDEXED | 支持 | **使用** — 所有字段 |

---

## 3. 查询类型

### Tantivy 提供的查询类型

| 查询类型 | 能力 | Milvus 使用 | 说明 |
|---------|------|-----------|------|
| `TermQuery` | 精确匹配单个 term | **使用** | IN 查询（少量 term 时逐个 TermQuery） |
| `TermSetQuery` | 批量 term 查询 | **使用** | IN 查询（大量 term 时用 TermSetQuery，阈值约 128） |
| `RangeQuery` | 范围查询 | **使用** | 标量 Range 查询（<, >, <=, >=, between） |
| `RegexQuery` | 正则匹配 | **使用** | PatternMatch (Like/Regex) |
| `BooleanQuery` | 布尔组合查询 | **使用** | TextMatch(intersection)、JSON ngram AND 查询 |
| `PhraseQuery` | 短语查询（位置匹配） | **使用** | PhraseMatch（带 slop） |
| `ExistsQuery` | 字段存在性查询 | **使用** | JSON `json_contains` 路径存在检查 |
| `FuzzyTermQuery` | 模糊/拼写纠错查询 | **不用** | Tantivy 支持 Levenshtein 距离模糊匹配 |
| `MoreLikeThisQuery` | 相似文档查询 | **不用** | Tantivy 支持 MLT 查询 |
| `PhrasePrefixQuery` | 短语前缀查询 | **不用** | Tantivy 支持，可匹配短语的最后一个 term 前缀 |
| `BoostQuery` | 权重提升查询 | **不用** | Tantivy 支持查询权重调整 |
| `ConstScoreQuery` | 固定分数查询 | **不用** | Tantivy 支持 |
| `DisjunctionMaxQuery` | 取最高分查询 | **不用** | Tantivy 支持 |
| `AllQuery` | 匹配所有文档 | **不用** | Tantivy 支持 |
| `EmptyQuery` | 空查询 | **不用** | Tantivy 支持 |

### 关键区别：Milvus 如何使用这些查询

Milvus 对 Tantivy 查询的使用方式与 Tantivy 的设计意图完全不同：

1. **不经过 Tantivy 的 query engine 评分流程**：Milvus 的 `DirectBitsetCollector` 直接读取 posting list 设置 bitset bit，完全绕过 Tantivy 的 Weight/Scorer 评分体系
2. **不使用 BooleanQuery 的评分语义**：`BooleanQuery::intersection()` 仅用于取交集，不关心各子查询的分数
3. **不使用 TermQuery 的 TF-IDF 评分**：所有 TermQuery 都在 `IndexRecordOption::Basic` 模式下使用

```rust
// Milvus 的 DirectBitsetCollector — 直接读 posting list，不评分
fn collect_segment(&self, _weight: &dyn Weight, _segment_ord: u32, reader: &SegmentReader) {
    for term in self.terms.iter() {
        let inv_index = reader.inverted_index(term.field())?;
        if let Some(mut posting) = inv_index.read_postings(term, IndexRecordOption::Basic)? {
            // 直接遍历 posting list 设置 bitset bit
            loop {
                let doc = posting.doc();
                self.bitset_wrapper.set(doc);
                if posting.advance() == TERMINATED { break; }
            }
        }
    }
}
```

---

## 4. 评分与排序

### Tantivy 提供的评分/排序能力（全部不用）

| 能力 | 说明 | Milvus 使用 |
|------|------|-----------|
| BM25 评分 | 内置 BM25 评分函数 | **不用** — Milvus 用自己的 BM25 实现生成稀疏向量 |
| TF-IDF | 内置 TF-IDF 评分 | **不用** |
| 自定义评分 | `CustomScorer` / `FieldBoost` | **不用** |
| TopDocs 收集器 | 按分数排序取 Top-K | **不用**（仅在单元测试中出现） |
| Score 传递 | `collect(doc, score)` 中的 score 参数 | **不用** — 所有 collector 忽略 score |
| `requires_scoring()` | 控制是否计算分数 | **始终返回 false** |
| Field Norms | BM25 的文档长度归一化因子 | **显式关闭** |

**核心洞察**：Milvus 将 Tantivy 当作纯粹的「倒排索引 + 查询执行器」使用，完全跳过评分层。所有查询结果都是布尔型（match / not match），不需要相关性排序。

Milvus 的 BM25 是在 Go 层独立实现的，将文本通过分词器转化为稀疏向量，然后走向量搜索流程，不经过 Tantivy 的评分系统。

---

## 5. 收集器（Collectors）

### Tantivy 内置收集器（全部不用）

| 收集器 | 说明 | Milvus 使用 |
|-------|------|-----------|
| `TopDocs` | 按分数排序收集 Top-K 文档 | **不用**（仅测试代码） |
| `Count` | 统计匹配文档数 | **不用** |
| `MultiCollector` | 多收集器组合 | **不用** |
| `FacetCollector` | 分面统计 | **不用** |
| `HistogramCollector` | 直方图统计 | **不用** |

### Milvus 自定义收集器（4 个）

| 收集器 | 用途 | 特点 |
|-------|------|------|
| `VecCollector` | 标量倒排索引查询 | doc_id == Milvus row_id，直接设 bitset；**检查只有 1 个 segment** |
| `MilvusIdCollector` | 标量倒排索引查询（V7 格式） | 同 VecCollector，使用 Tantivy internal doc_id |
| `DocIdCollector` | 文本索引查询 | 通过 fast field `doc_id` 列映射到 Milvus row_id 再设 bitset |
| `DirectBitsetCollector` | TextMatch 查询 | **完全绕过 Tantivy query engine**，直接读 posting list |

所有自定义收集器共同特点：
- `requires_scoring() = false`
- 忽略 `Score` 参数
- 输出到 `BitsetWrapper`（C++ 传入的 bitset 指针）
- 单线程安全警告（bitset 非线程安全）

---

## 6. 分词器/分析器（Tokenizer/Analyzer）

### 这是 Milvus 对 Tantivy 使用最深的部分

| 分词器 | 来源 | Milvus 使用 |
|-------|------|-----------|
| `standard` (SimpleTokenizer) | Tantivy 内置 | **使用** |
| `whitespace` | Tantivy 内置 | **使用** |
| `jieba` | tantivy-jieba crate | **使用** — 中文分词 |
| `lindera` | lindera crate | **使用** — 日文/韩文分词 |
| `icu` | icu_segmenter crate | **使用** — Unicode 分词 |
| `language_identifier` | 自定义 (whatlang/lingua) | **使用** — 多语言自动识别 |
| `grpc` | 自定义 (tonic) | **使用** — 远程分词服务 |
| `char_group` | 自定义 | **使用** — 按字符组切分 |
| `ngram` (NgramTokenizer) | Tantivy 内置 | **使用** — Ngram 索引 |

### Token Filters（全部使用）

| Filter | 来源 | 说明 |
|--------|------|------|
| `LowerCaser` | Tantivy 内置 | 转小写 |
| `AsciiFoldingFilter` | Tantivy 内置 | Unicode → ASCII 折叠 |
| `AlphaNumOnlyFilter` | Tantivy 内置 | 只保留字母数字 |
| `RemoveLongFilter` | Tantivy 内置 | 按长度过滤 |
| `StopWordFilter` | Tantivy 内置 | 停用词过滤 |
| `SplitCompoundWords` | Tantivy 内置 | 复合词拆分 |
| `Stemmer` | Tantivy 内置 | 词干提取 |
| `CnCharOnlyFilter` | 自定义 | 只保留中文字符 |
| `CnAlphaNumOnlyFilter` | 自定义 | 只保留中文+字母+数字 |
| `RemovePunctFilter` | 自定义 | 移除标点 |
| `RegexFilter` | 自定义 | 正则过滤 |
| `SynonymFilter` | 自定义 | 同义词扩展 |

**关键结论**：分词器/分析器是纯函数（text → tokens），与 Tantivy 的索引/查询引擎完全解耦。C++ 重写时保留 Tantivy 仅作为分词器使用是合理的。

---

## 7. 段管理与合并（Segment Management & Merge）

### Tantivy 提供的段管理能力

| 能力 | 说明 | Milvus 使用 |
|------|------|-----------|
| 多段写入 | 自动创建多个 segment | **使用但不期望** — V5 格式会产生多段 |
| 段合并 (Merge) | `LogMergePolicy` 等合并策略 | **使用** — 在 `finish()` 时强制合并为单段 |
| `wait_merging_threads()` | 等待合并完成 | **使用** |
| `garbage_collect_files()` | 清理过期段文件 | **使用** |
| 增量合并 | 后台异步合并 | **不用** — Milvus 只在 build 完成时一次性合并 |
| 自定义 Merge Policy | 可插拔的合并策略 | **不用** — 使用默认策略 |

**关键发现**：Milvus 的目标是每个 Tantivy 索引只有 **1 个 segment**。V5 格式的 `finish()` 显式合并所有段：

```rust
// V5: 显式合并为单段
let segment_ids = index_writer.index().searchable_segment_ids()?;
if segment_ids.len() > 1 {
    let _ = index_writer.merge(&segment_ids).wait();
}

// VecCollector: 如果出现多段会打警告
fn merge_fruits(&self, segment_fruits: Vec<()>) -> Result<()> {
    if segment_fruits.len() != 1 {
        warn!("inverted index should have only one segment, but got {} segments");
    }
}
```

V7 格式使用 `SingleSegmentIndexWriter`，从源头避免多段问题。

---

## 8. 索引读取与加载

### Tantivy 提供的读取能力

| 能力 | 说明 | Milvus 使用 |
|------|------|-----------|
| `IndexReader` | 带自动 reload 的查询入口 | **使用** |
| `ReloadPolicy::OnCommitWithDelay` | 自动检测新 commit 并 reload | **使用** — Growing segment 场景 |
| `ReloadPolicy::Manual` | 手动 reload | **使用** — 测试中 |
| `reader.reload()` | 手动触发 reload | **使用** — Growing segment 新增数据后 |
| `Searcher` | 线程安全的搜索执行器 | **使用** |
| Warm (预热) | 预加载索引数据到内存 | **不用** |
| Mmap directory | 内存映射方式打开索引 | **使用** — `MmapDirectory::open()` |
| RAM directory | 纯内存索引 | **不用** |

### 文档删除

| 能力 | 说明 | Milvus 使用 |
|------|------|-----------|
| `delete_term()` | 按 term 删除文档 | **不用** — Milvus 有自己的 delete bitmap 机制 |
| 软删除 + Merge 清理 | Tantivy 的删除模型 | **不用** |

---

## 9. Growing Segment 支持

| 能力 | 说明 | Milvus 使用 |
|------|------|-----------|
| 增量添加文档 | `add_document()` + `commit()` | **使用** — 每批数据写入后 commit |
| 自动 Reload | `OnCommitWithDelay` 策略 | **使用** — 查询时自动看到新数据 |
| 手动 Reload | `reader.reload()` | **使用** — `tantivy_reload_index()` FFI |
| `num_docs()` | 获取当前文档数 | **使用** — `tantivy_index_count()` FFI |
| `SingleSegmentIndexWriter` | V7 格式不支持增量 | **不用于 growing** — Growing 用标准 `IndexWriter` |

---

## 10. 高级功能（全部不用）

| 功能 | Tantivy 提供的能力 | Milvus 使用 |
|------|-------------------|-----------|
| **Faceted Search** | `Facet` 字段 + `FacetCollector`，支持层级分面计数 | **不用** |
| **Snippet/Highlighting** | `SnippetGenerator`，高亮搜索结果中的匹配片段 | **不用** |
| **Aggregation** | 各类聚合统计（桶、指标等） | **不用** |
| **Fuzzy Search** | `FuzzyTermQuery`，基于 Levenshtein 距离的模糊匹配 | **不用** |
| **MoreLikeThis** | `MoreLikeThisQuery`，查找与给定文档相似的文档 | **不用** |
| **Phrase Prefix** | `PhrasePrefixQuery`，短语查询 + 最后一词前缀匹配 | **不用** |
| **Query Parser** | 将人类可读查询字符串解析为查询树 | **不用** |
| **Column Block Accessor** | 批量读取列式数据 | **不用** |
| **Stored Fields** | 原文存储 + 检索 | **不用** |
| **多线程查询** | Tantivy 的多线程搜索执行 | **不用** — 所有 collector 强制单线程 |

---

## 11. 数据流对比：Tantivy 设计 vs Milvus 实际使用

### Tantivy 设计的典型数据流（全文搜索引擎）

```
用户查询 → QueryParser → Query Tree → BooleanQuery(子查询...)
    → 每个 segment 上:
        Weight.scorer(segment) → 遍历 posting list
        → 计算 BM25/TF-IDF 分数（需要 term freq, field norms, doc freq）
        → TopDocs 收集 Top-K（按分数排序）
    → merge_fruits: 合并多 segment 的 Top-K
    → 返回 [(doc_id, score), ...] 按相关性排序
```

### Milvus 实际的数据流（布尔过滤）

```
内部查询 → 构造 TermQuery/RangeQuery/RegexQuery
    → 单个 segment 上:
        → 遍历 posting list
        → 直接设置 bitset[doc_id] = 1（无评分）
    → 返回 TargetBitmap（位图，表示哪些行匹配）
```

### 差异总结

| 维度 | Tantivy 设计 | Milvus 使用 |
|------|-------------|-----------|
| 输出类型 | `Vec<(DocId, Score)>` | `BitsetWrapper`（位图） |
| 评分 | BM25/TF-IDF | 无 |
| 排序 | 按相关性排序 | 无排序 |
| Top-K | 支持 | 不需要 |
| 多段 | 设计支持 | 强制单段 |
| 多线程 | 支持并行搜索 | 强制单线程 |
| 文档存储 | 支持存储+检索原文 | 不使用 |
| 用途 | 全文搜索引擎 | 布尔过滤器 |

---

## 12. 依赖链分析

### Tantivy 引入的 Rust 依赖

```toml
# 核心 Tantivy（两个版本共存！）
tantivy = { git = "https://github.com/zilliztech/tantivy.git", ... }  # V7
tantivy-5 = { package = "tantivy", ... }                                # V5（兼容旧索引）

# 分词器相关（C++ 重写时需保留）
tantivy-jieba = "0.11"           # 中文分词（jieba）
lindera = "0.38"                 # 日文/韩文分词
icu_segmenter = "1.5.0"          # Unicode 分词
whatlang = "0.16"                # 语言检测
lingua = "1.6"                   # 语言检测（高精度）

# 序列化/网络（分词器相关）
serde_json, tonic, prost         # JSON 解析、gRPC 分词器

# Tantivy 内部依赖（C++ 重写后不再需要）
tantivy-common, tantivy-query-grammar, tantivy-tokenizer-api
tantivy-columnar, tantivy-stacker, tantivy-bitpacker
```

### C++ 重写后的依赖变化

| 依赖 | 当前 | 重写后 |
|------|------|-------|
| tantivy (核心) | 索引+查询+评分 全功能 | 仅保留分词器（~10% 代码） |
| tantivy-5 (兼容) | 旧格式兼容 | 保留（旧索引兼容）|
| jieba/lindera/icu | 分词器 | 保留 |
| roaring (C++ conan) | 已有 | 作为核心 posting list 实现 |

---

## 13. 为什么可以用 C++ 重写

基于以上分析，Milvus 实际使用的 Tantivy 能力可以精简为：

### 必须保留（Rust FFI）
1. **分词器 pipeline**：standard/whitespace/jieba/lindera/icu/language_identifier/grpc/char_group/ngram
2. **Token filters**：lowercase/asciifolding/length/stop/stemmer/decompounder/synonym/regex/cncharonly 等

### 可用 C++ 替代
1. **倒排索引存储**：`std::map<T, roaring::Roaring>` 或排序数组 + Roaring Bitmap
2. **Term 查询**：binary search on sorted terms
3. **Range 查询**：`lower_bound` / `upper_bound` on sorted terms
4. **Regex 查询**：`std::regex` 全扫描
5. **Phrase 查询**：位置交叉算法（已有 `phrase_match_slop.rs` 参考实现）
6. **Boolean AND/OR**：Roaring bitmap `&` / `|` 运算
7. **序列化/反序列化**：V3 unified format entries

### 完全不需要实现
1. BM25/TF-IDF 评分
2. TopDocs 排序收集器
3. 多字段 Schema
4. Stored Fields
5. Facet/Aggregation
6. Fuzzy/MoreLikeThis/PhrasePrefixQuery
7. 多段管理与合并策略
8. Query Parser
9. Field Norms
10. 多线程查询执行

---

## 14. 风险与注意事项

### 保留的复杂度
- **PhraseMatch**：位置信息的存储和交叉算法相对复杂，需要实现 `PositionalPostingList` 和 slop 匹配算法
- **JSON 路径编码**：需要复现 Tantivy 的 JSON path encoding（`path\0type_tag\0value`）
- **Growing Segment**：需要支持增量写入 + reload 语义

### 迁移安全
- 通过 `CurrentScalarIndexEngineVersion` 版本路由，新旧实现共存
- 旧 Tantivy 索引数据格式由旧代码读取，不需要格式转换
- 分词器始终使用 Tantivy Rust 实现，保证分词结果一致性
