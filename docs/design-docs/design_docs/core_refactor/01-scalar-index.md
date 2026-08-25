# W1：Index 四面化（标量各族 + 标量/向量共享面）

> **状态：设计提案，讨论中。** 返回 [总览](README.md)。
> 现状事实基于 master `e255009e01`。本文档定义 index 组件的目标接口：标量索引各族的完整合同，
> 以及标量/向量**共享面**（生命周期根、Loader、工厂、storage 管道）的处理。向量索引的**归位**
> 也在 W1 范围（§11），但其 knowhere 交互不做重设计。

## 1. 范围

- **纳入**：常规标量索引（inverted / bitmap / sort / marisa / hybrid）、模式匹配（FMIndex、inverted 的 LIKE 族）、TextMatch（全文）、Ngram、**Geometry/RTree（空间谓词，见 §5.6）**、JSON **path 索引**族（json path cast index、`JsonFlatIndex`）、growing 增量标量索引；**标量/向量共享面的清理与向量索引的四面归位**（§11）——共享生命周期根、Loader/IO、工厂拆族、`IndexBase` 波内退役。
- **排除**：knowhere 交互的重设计（vector 的搜索/构建逻辑**原样搬移**进新面，不改行为）；`SkipIndex`——它是列的 zone-map 统计，归 **columnar-format**（[#51504](https://github.com/milvus-io/milvus/pull/51504) 已将其用作 `CellSkipPredicate` 接进列扫描规划，证实了这个归属）；**JSON shredding（`JsonKeyStats`：typed 子列 + shared BSON 子列 + BSON 定位倒排）——它是 JSON 列的物理布局，整体归 columnar-format**，见下方裁决。

> **Geometry 属于标量索引**。`RTreeIndex : public ScalarIndex<T>` 本就在这棵树下，与其他标量族的差别只是**算子不同**（空间关系谓词 vs 点/范围谓词），生命周期、构建、持久化、pin 与其他标量族完全同构。按算子把它划成独立组件是错误分族——本文档的方法论正是"每族一个窄合同"，多一个空间族不构成例外。

> **JSON shredding 不是索引，是列布局**。判据用总览 §6.3 那条：*是否按数据形态选算法*。shredding 按数据形态选的是**存储布局**（高频 path 抽成 typed 子列、低频落 shared BSON），查询算法完全没变——仍是 exec 把比较 lambda 逐 chunk 跑在原始值上。三条代码事实：
>
> 1. `JsonKeyStats::ExecutorForShreddingData`（`JsonKeyStats.h:266-330`）持有的 `shredding_columns_` 是 `ChunkedColumnInterface`，循环 `column->Span(op_ctx, i)` / `StringViews()` 把 exec 传入的 functor 跑在原始值上，末尾断言 `processed_size == num_rows`——**永远全扫**，与 `SegmentExpr::ProcessDataChunks` 同形。它甚至自带一份 `SkipIndex skip_index_` 用于跳 chunk，而 zone-map 已裁给 columnar-format：同一个概念在两个组件里各存一份。
> 2. `BsonInvertedIndex`（`json_stats/bson_inverted.h:41`）的 posting 是 `path → [(row_id, offset)]`，`offset` 是**该值在这一行 BSON blob 里的字节偏移**——它索引的是物理位置，不是值。且 `ExecuteForSharedData` 从不把 posting list 返出来，拿到立刻 `shared_column_->BulkRawBsonAt(...)` 回调解码：**它不可能脱离那根列被独立消费**，这正是列内部布局结构（等价于 offsets/dictionary）的定义，不是一个索引面。连 `exists(path)` 都不能由 posting list 单独回答——`ExistsExpr.cpp:264-270` 拿到 (row, offset) 后仍要 `bson.IsBsonValueEmpty(offset)` 才敢置位。
> 3. 对照组：`JsonFlatIndex` / `JsonFlatIndexQueryExecutor`（`JsonFlatIndex.h:34,736`）的 tantivy 倒排建在**值**上，`TermBitset` / `json_exist_query` / range 直接出 bitmap，全程不碰列。这才是索引语义。
>
> 因此 `JsonKeyStats` 的 `NotImplemented` 泛滥（§2.2 第 1 行）不是"合同太宽"而是**站错了组件**：它今天只能靠继承 `ScalarIndex<std::string>` 来蹭索引的工厂、加载与 pin 机制。把它迁进 columnar-format 的代价是 columnar-format 要引入一个它目前没有的概念——**列的可选替代布局：异步构建、可以缺席、缺席时回退原始 JSON 列**（与 interim index 同构）。这个概念必须显式建模，否则只是把烂摊子换了个目录。归属属 [W1∥ columnar-format 收敛](README.md#7-波次计划)，不属本波。
> **反问：shared 侧确实复用了倒排，那它算不算索引？——复用的是引擎与生命周期管道，不是 `index` 组件。** 若让 columnar-format 持有一个索引对象、经 Reader 访问，就造出 **L1 → L2 的反向边**；而 index → columnar-format 已经存在且必需（[§5.8](#58-nested元素级索引坐标与投影) 的 `IArrayOffsets` 注入；shredded typed 子列本身就是列）。两条边合起来正是 [W0](00-w0-foundation.md) 明令禁止的双向组件对。实际被复用的只有两层东西，都在 `index` **之下**：
>
> - **引擎**：`TantivyIndexWrapper` 位于 `thirdparty/tantivy/tantivy-wrapper.h`，属外部底座 `tantivy_binding`，**不属 `src/index/`**。`BsonInvertedIndex` 今天已经直接组合它、完全不碰 `IndexBase`/`ScalarIndex`——它正是 §3 原则 2 引用的那个组合正例。组合外部引擎在任何层都合法（正如 vector 族组合 knowhere、text 族组合 tantivy）。
> - **生命周期管道**：构建→序列化→上传、下载→加载→pin→计费。这套东西**没有一处是索引专属的**，处理见 [§11.2 第 1 条](#112-处理决定)——它下沉到 L1，两边共用。
>
> 现状依赖数据支持这个判断：`bson_inverted.h` 的全部依赖是 `storage::DiskFileManagerImpl`（L1）+ tantivy wrapper（外部）+ `index/IndexStats.h`；而 `IndexStats` 只依赖 `common/protobuf_utils.h`，内容是 `(file_name, file_size)` 列表加内存尺寸，**零索引语义**。迁移的全部代价就是把这个类降层。
>
> 判定索引的可操作标准不是「用了什么引擎」，而是**能否被那根列以外的消费者当索引消费**。`ExecuteForSharedData` 从不把 posting list 返出来，`exists` 也必须回读 blob——答案是不能。一根字典编码列内部可能用哈希表甚至 FST，没人会因此说 dictionary column 属于索引模块。
> **再反问：那把 shredding 整体上提呢？让它既不属 index 也不属 columnar-format，而是一个同时持有列与索引的更高层组件。——不成立，三条理由。**
>
> 1. **shredded 列已经是 columnar-format 对象，不是"某种类似列的东西"。** `JsonKeyStats::LoadShreddingData`（`JsonKeyStats.cpp:1248-1283`）走的是 `ManifestGroupTranslator` → `ChunkedColumnGroup` → `ProxyChunkColumn`——与普通 storage-v2 列**完全同一条构造链**，连 `enable_mmap`/`warmup_policy`/`load_priority`/`size_estimate`/cachinglayer 计费都是同一套，唯一区别是一个 `GroupChunkType::JSON_KEY_STATS` 标签。上提等于按"来自哪个逻辑字段"把 storage-v2 column group 这一个概念劈成两个家。顺带这也解释了 index → segcore 那 5 处 include 的由来：index 组件得伸手到 L3 去拿 translator 才能造出这些列。
> 2. **"同时持有列与索引并在两者间选路"这个角色已经有人担任，是 exec。** 这件事对所有字段类型都一样（`DetermineExecPath`），不是 JSON 特有。为 JSON 单造一个持列又持索引的组件，等于造一个 mini-exec——而 [§5.4](#54-ngramreader二段执行的正确切分)（ngram）与 [§5.6](#56-spatialreader空间关系谓词)（spatial）的裁决刚把"索引内部做二段执行"赶回 exec，这是同一条原则的反面。
> 3. **上提的落点其实是 segcore（L3），不是新层**——它的一句话定义正是"列/索引/可见性三条读通道"。但那会让 segcore 拥有一个 JSON 专属数据结构（与 segcore 重构"甩掉类型特例"的方向相反），并且 JSON 字段将无法经统一 `ColumnInterface` 提供，把 [#51504](https://github.com/milvus-io/milvus/pull/51504) 刚统一起来的数据访问路径重新劈开。
>
> 那么"哪个 path 有 typed 子列、cast 成什么、没有时回退哪根列"这些内部知识放哪儿？——**列的自述（layout metadata）**，与列知道自己的 chunk 几何（`ColumnPlanner`）同类。exec 问"field F 的 path P 你能以什么形态给我"，拿到能力答案后自己选路。即 [§12.5](#12-未决问题) 那个 `ColumnCaps` 未决项，不需要新组件。
>
> **本裁决的失效条件（写死，便于将来复查）**：今天 `BsonInvertedIndex` 的 term 是 **path**、posting 是 `(row_id, blob 内字节偏移)`，是定位器。若将来 shared 侧改为索引**值**（例如支持 `a.b > 3` 直接出 bitmap 而不回读 blob），那么该布局内部就真的嵌了一个索引，"跨两层"的前提成立，本条裁决必须重开。
> **外部独立证实：[宽表建模设计文档](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)（2026-08 草稿）。** 该文档「需求 Scope」第 3 条写明：*在单个 Sealed Segment 内部，`Struct` 和 `JSON` 应在 schema 推断后，以相同的底层结构存储，即拆列*。`Struct` 拆列是无争议的存储/schema 概念（物理列、FieldID 分配、etcd schema）；要求 JSON 与之同构，等于宣告 **JSON shredding 是拆列的一个变体**——差别只在 layout 从哪儿来。既然 `Struct` 拆列显然不需要一个「同时持有列与索引」的新组件，JSON 更不需要。
>
> 但该文档同时**修正了本裁决的一处粒度**：JSON 一节写「拆列的结构无需保存到 etcd，就按现在只保存在 stats 里即可，每个 Segment 在 Segcore 里动态管理」。这不影响子列对象的归属，但把 **layout 目录** 的归属钉在了 segcore。三段划分：
>
> | 东西 | 归属 | 依据 |
> |---|---|---|
> | 子列对象（typed 子列、shared BSON 子列） | **columnar-format (L1)** | 与 `Struct` 子列同构（宽表建模 Scope 3）；今天已经是 `ManifestGroupTranslator`→`ChunkedColumnGroup`→`ProxyChunkColumn` |
> | layout 目录（path → 哪根子列 / cast type / 是否只在 shared 里） | **segcore (L3)** | JSON 的 layout 来自 per-segment stats、动态推断、每段不同；`Struct` 的 layout 来自 collection schema，走 schema 通道 |
> | 定位倒排（path → (row, byte offset)） | 随 shared 子列，归 columnar-format | 不能脱离该列被消费 |
> | 选路（走 path index 还是扫子列） | exec (L4) | 与所有字段类型一致 |
>
> 这比「列的自述」更准确：**JSON 的 layout 自述不是列自己知道的，是 segcore 从 stats 加载后装配给列的**；`Struct` 的 layout 则由 schema 直接给出。同一个数据访问接口、两个元数据来源——[§12.5](#12-未决问题) 的 `ColumnCaps` 必须按这个形状设计。
> **W1 内的过渡处理（宽表建模落地前）。** 上面的终局裁决依赖两个尚未定型的外部件（嵌套数据表达、Struct/JSON 拆列统一），但 W1 不能等——`JsonKeyStats` 挂在 `ScalarIndex<std::string>` 下会挡住 `IndexBase` 退役，5 处 `index → segcore` 反向边也是 W1 的出口判据。好消息是**代价几乎为零**：盘点后，它与索引机制的耦合全仓只剩一行。
>
> | 事实 | 证据 |
> |---|---|
> | 构建侧**不经 `IndexFactory`** | `indexbuilder/index_c.cpp:477` 直接 `make_unique<JsonKeyStats>`，只调 `Build(config)` + `Upload(config)` |
> | 加载侧**不经类型擦除** | `ChunkedSegmentSealedImpl::BuildJsonKeyStatsIndex` 直接构造，存为 `shared_ptr<JsonKeyStats>`（`ChunkedSegmentSealedImpl.h:350`、`SegmentInterface.h:941`）——**不是** `CacheIndexBasePtr`/`IndexBase` |
> | 查询侧**不经虚函数** | `segment->GetJsonStats()` 拿具体类型，调 `ExecutorForShreddingData` / `ExecuteForSharedData` |
> | 唯一耦合点 | `JsonKeyStats.h:76` 的 `: public ScalarIndex<std::string>`——**一个没有任何调用点使用的继承子句** |
>
> 因此过渡动作是两步，无行为变更、无格式变更、无 Go 侧改动：
>
> 1. **断继承**：`class JsonKeyStats : public ScalarIndex<std::string>` → `class JsonKeyStats`，删掉 `In`/`NotIn`/`Range`×2/`IsNull`/`IsNotNull`/`Reverse_Lookup`/`Build(n,…)`/`BuildWithDataset`/`BuildWithRawDataForUT`/`Load(BinarySet)` 这批 `NotImplemented` override。`Build(config)`/`Upload(config)`/`Load(TraceContext,Config)`/`Serialize` 保留为普通方法（真实调用点在）。零调用点改动。
> 2. **移目录** `index/json_stats/` → `segcore/json_stats/`。`index → segcore` 的 5 处反向边**当场归零**（[W0](00-w0-foundation.md) 表中该行清空），不是延后。`indexbuilder` 由此 include segcore——L5→L3 下行边，合法。
>
> **为什么落点是 segcore 而不是一个更"干净"的独立组件**：它需要 `ManifestGroupTranslator`（segcore），同时又被 segcore 的 `runtime.json_stats` 持有——独立组件必然成环，除非先把 translator 迁出 segcore，而那是 W2/W3 的量。segcore 内是当前唯一无环的落点，且**不是浪费**：宽表建模已裁定 JSON layout「每个 Segment 在 Segcore 里动态管理」，layout 目录终局就在 segcore。将来变的只是"它持有的子列升格为 columnar-format 一等对象"——那是 segcore 内部的替换，不再是跨组件搬家。
>
> **明确不做**（全部等宽表建模定型）：不把 typed 子列升格为 columnar-format 一等对象；不设计"列的可选替代布局"概念与 `ColumnCaps`；不改 exec 的 JSON 表达式调用形态（只改 include 路径）；不接 [§11.2](#112-处理决定) 的 L1 产物管道（它继续手写自己的 `Build`/`Serialize`/`Upload`/`Load`）；不改名（`JsonKeyStats` 这个名字跨到 proto 与 Go 侧，且宽表建模统一 Struct/JSON 时会自然重定）。
>
> **代价与退出条件**：过渡期内 `segcore/json_stats/` 是 segcore 里的一个 JSON 专属数据结构，与 segcore 重构"甩掉类型特例"的方向相反。这是**有意接受的临时状态**，退出条件写死：宽表建模「六、查询节点的数据表达」定型 + 子列升格为 columnar-format 对象。挂在 [README §8](README.md#8-segcore-章节文档的已知待修订项) 的 segcore 待修订项里，避免就地生根。

合同设计一次覆盖全部纳入族；**迁移可以分批**（先常规 scalar，再 ngram/text/json，最后 vector 归位），但接口不为"第一批"特化。

## 2. 现状与病理

### 2.1 结构事实

- `IndexBase`（`Index.h:37`）约 19 个 virtual，**构建、持久化、查询、缓存计费混装于根**：`Serialize`/`Load`×2/`Build`×3/`Upload`/`LoadUnified`/`UploadUnified`/`HasRawData`/`IsMmapSupported`/`GetCastType`/`Exists`/`CellByteSize`/`SetCellSize`/`ComputeByteSize`。
- `ScalarIndex<T>`（`ScalarIndex.h:102`）再叠约 24 个 virtual：`In`/`NotIn`/`Range`×2/`IsNull`/`IsNotNull`/`InApplyFilter`/`InApplyCallback`/`Reverse_Lookup`/`SupportFastReverseLookup`/`PatternMatch`/`SupportPatternMatch`/`Query(Dataset)`/`Build(n, values, valid)`……
- 14 个实现类挂在这棵树下，且存在**深实现继承**：`TextMatchIndex : InvertedIndexTantivy<std::string> : ScalarIndex : IndexBase`；`NgramInvertedIndex`、`JsonFlatIndex` 同样以继承 `InvertedIndexTantivy` 的方式复用 tantivy 封装。
- 10+ 个头文件 include `FileManager`/`DiskFileManagerImpl`——持久化 IO 长在索引类里。

### 2.2 病理证据（每条对应一个设计决定）

| 证据 | 位置 | 说明 | 对应设计决定 |
|---|---|---|---|
| `JsonKeyStats` 继承 `ScalarIndex<std::string>`，但 `In`/`NotIn`/`Range`/`IsNull`/`IsNotNull`/`Reverse_Lookup`/`Build` **全部 `ThrowInfo(NotImplemented)`** | `json_stats/JsonKeyStats.h:145-201` | 继承接口只为蹭工厂与加载机制，是教科书式 Liskov 违约。**根因是站错组件**：它是列布局不是索引（§1 裁决） | 双重结论：非常规索引不硬套 point-predicate 合同、每族一个窄合同（§4）；shredding 整体迁出 index（§1） |
| `TextMatchIndex` 有**四个构造函数对应四种生命周期**：growing 内存 writer（commit interval + background merge）/ sealed 加载时从原始数据建 / 构建服务（`FileManagerContext`）/ 加载已建索引 | `TextMatchIndex.h:31-52` | 四种生命周期挤在一个类，靠构造参数区分 | 四面切分：Reader / Builder / Appender / Loader（§3） |
| `NgramInvertedIndex::ExecutePhase2` 接 `exec::SegmentExpr*` | `NgramInvertedIndex.h:68,93` | 候选验证需要回读原始值，于是索引反向依赖 exec | 索引只做 Phase1（候选），验证归 exec 走列扫描（§5.4） |
| 能力缺失用异常表达：`PatternMatch`/`InApplyFilter` 默认 `ThrowInfo(Unsupported)` | `ScalarIndex.h:140,187` | 调用方要么 try、要么记住每个 `Support*` 方法 | 能力描述符自述，禁止 throw 探测（§4.1） |
| `indexbuilder::ScalarIndexCreator` 只用 `CreateIndex`/`Build`/`Serialize`/`Upload`；exec 只用 `In`/`Range`/`PatternMatch` 族；segcore load 只用 `Load`/cache 计费 | `indexbuilder/ScalarIndexCreator.cpp:188-243` | 三类调用方各用一面，却都拿到全量 surface | 按调用方切面（§3） |
| `HybridScalarIndex` 运行时包一层，按基数转发到 bitmap/inverted | `HybridScalarIndex.h` | 选型是构建期决策，被做成了运行时转发类 | 选型归 Builder 策略，加载返回具体 Reader（§6.3） |
| `FMIndex.h:30` include `segcore/SegcoreConfig.h` | `FMIndex.h:30,227` | 全局配置放 segcore，索引被迫反向依赖 | 配置作为构造参数注入（§8） |
| `RTreeIndex` 的真实查询面是 `QueryCandidates(GISOp, Geometry, vector<int64_t>&)`，却被迫实现 `In`/`NotIn`/`Range`×2/`InApplyFilter`/`InApplyCallback`，且 `Reverse_Lookup` throw `NotImplemented` | `RTreeIndex.h:111-189` | 空间索引硬套点谓词合同（与 `JsonKeyStats` 同病，只是程度轻些） | 独立空间族窄合同（§5.6） |
| **growing 侧的同构病例**：`FieldIndexing`（`FieldIndexing.h:51`）根上是两族接口的并集，5 个纯虚里 3 个 vector 专属、2 个 scalar 专属，**两个子类各自 throw 掉对方那一半**——`ScalarFieldIndexing` 的 `AppendSegmentIndexDense`/`Sparse`/`GetDataFromIndex` throw（`:152,161,183`），`VectorFieldIndexing` 的两个 `AppendSegmentIndex` 重载 throw（`:294,303`） | `segcore/FieldIndexing.h:51-131,152,161,183,294,303` | 与 `IndexBase` 一模一样的 Liskov 违约，只是发生在 growing 侧。它同时证明：**Appender 面两族共有**（都要 append），但 **append 的签名不该共享** | 四面统一、面内分族（§3、§7、§11） |
| `FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 返回 `PinWrapper<index::IndexBase*>` | `FieldIndexing.h:128,131` | growing 侧同样以 `IndexBase` 作类型擦除句柄——`IndexBase` 退役不只是 sealed 侧的事 | growing 句柄同波改为 `IndexReaderBase`（§7、§11.2 第 3 条） |
| **正例**：`RTreeIndex::QueryCandidates` 只出候选，精确验证在 exec（`PhyGISCoarseConjunctExpr` / `PhyGISRefineConjunctExpr`） | `RTreeIndex.h:184`、`GISFunctionFilterExpr.cpp:480-560` | "索引出候选、exec 验证"的**现成正确实践** | ngram 应向它看齐（§5.4/§5.6 候选族） |

## 3. 设计原则

1. **按调用方切面**。**四个面、四类持有者**：

   | 面 | 持有者 | 形态 | 生命周期 |
   |---|---|---|---|
   | **Reader** | exec（查询期） | 索引对象上的接口 | 不可变，长期存活 |
   | **Appender** | segcore growing insert 路径 | 索引对象上的接口 | 长期存活，与读并发 |
   | **Builder** | indexbuilder 服务、segcore load 就地建 | 独立协作者 | 一次性，`Seal()` 后终结 |
   | **Loader** | segcore load | 独立协作者（每族一个） | 无状态 |

   任何一类调用方拿不到其他面的方法。**Builder 与 Appender 是两个面而非一个**：前者一次性、独占、终结于产出 Artifact；后者长期存活、与读并发、只产出水位快照而不直接持久化（growing 的持久化走 flush 路径）。把两者合一正是 `TextMatchIndex` 四个构造函数挤在一个类的成因（§2.2）。
2. **组合替代继承**。tantivy 封装、marisa、FM 结构是被组合的**引擎**，不是基类。实现类之间禁止继承。

   > **仓库里已有正例**：`BsonInvertedIndex`（`json_stats/bson_inverted.h:42`）是个裸类，不继承 `IndexBase`/`ScalarIndex`/`InvertedIndexTantivy`，直接持有 `shared_ptr<TantivyIndexWrapper>` 并调 `term_query_i64`。同一个 tantivy 引擎，`TextMatchIndex`/`Ngram`/`JsonFlat` 靠继承复用、它靠组合——**后者才是本波要推广的形态**。代价是它把生命周期面（`AddRecord`/`BuildIndex`/`LoadIndex`/`UploadIndex`/`CellByteSize`）手写了一遍；四面化后这部分由共享的 Builder/Loader 承接，组合的好处保留、重复消失。
3. **能力自述，不许 throw**。每个 reader 携带能力描述符；不支持的操作在类型上就不存在，或经 `std::optional` 表达。
4. **模板留在热路径，类型擦除只在管理面**。exec 的表达式本就按值类型模板化，typed reader 无装箱开销；inventory 持类型擦除根，root→face 的 downcast **每个表达式节点一次**（随 pin 获取），不在 batch 或行的粒度上发生（§4.3）。
5. **不认识 segment 与 executor**。构建输入是 columnar-format 的扫描游标或纯数组；查询输出是 bitmap / 值，**坐标一律是行号**——元素级（nested）索引在 reader 内部完成元素→行的投影后才返回，见 [§5.8](#58-nested元素级索引坐标与投影)。
6. **IO 注入**。索引类不持有 `FileManagerContext`；文件读写经注入的 sink/source 接口，只在 Loader 与 Artifact 序列化的实现内出现。

## 4. 合同总览

```text
实现类（如 InvertedIndex<T>）
  ├── IndexReaderBase          身份与自述，类型擦除根；inventory 持它
  └── 若干查询 face（纯 mixin，彼此独立，不继承 root）：
        ScalarPredicateReader<T>   点/范围/空值谓词
        PatternMatchReader         LIKE 族（prefix/postfix/inner/match）
        TextMatchReader            分词匹配（match/phrase/fuzzy）
        NgramReader          ┐候选族：结果为超集，
        SpatialReader        ┘exec 侧精确验证
        ScalarValueReader<T>       取值面（反查）
        JsonIndexReader            path 路由：按 path 取到上面各族的谓词面

IndexBuilder<T>（两族共用一个面，各族一个实现）        → Seal() → IndexArtifact
GrowingScalarIndex<T> / GrowingTextIndex                → Append + 快照 Reader + 水位
IndexLoader（每族一个，只读方向）                        → 文件 → Reader，IO 注入
```

一个实现类可以同时提供多个查询面（inverted 同时是 `ScalarPredicateReader<T>` 和 `PatternMatchReader`），**通过接口多继承声明，而非通过实现继承获得**。

> **face 不继承 `IndexReaderBase`**。若每个 face 都 `virtual public IndexReaderBase`，多继承时为保证 root 子对象唯一必须用虚继承，代价是实现类访问 root 成员多一层间接、且 root→face 只能走虚基类的 `dynamic_cast`。改为纯 mixin 后：实现类非虚继承 root + 各 face，root→face 是一次 sibling cast，face 本身也不需要 root 的元数据（那些由 inventory 持有，见 §4.3）。

### 4.1 能力描述符

```cpp
// index/contracts/ReaderCaps.h
namespace milvus::index {

struct ReaderCaps {
    bool predicate          = false;  // In/NotIn/Range/Null
    bool pattern_match      = false;  // LIKE 族
    bool text_match         = false;
    bool ngram_candidates   = false;  // 候选族：结果为超集，需二次验证
    bool spatial            = false;  // 候选族：空间关系谓词（MBR 粗筛）
    bool nested             = false;  // 元素级索引：命中经存在量词投影到行（§5.8）
    bool value_lookup       = false;  // 可反查原值
    bool cheap_value_lookup = false;  // 逐行反查代价 O(1)/O(log n)
    bool json_paths         = false;  // path 寻址的复合索引
    bool exact              = true;   // false ⇒ 命中集是超集（ngram）
};

}  // namespace milvus::index
```

exec 的执行路径决策（`DetermineExecPath`）只消费这个结构，不 `dynamic_cast`、不 try-catch。segcore 的 `FieldIndexCapability`（segment 级"某 field 有什么索引"）由 inventory 聚合各索引的 `ReaderCaps` 得到——**单索引能力自述归 index，segment 级聚合归 segcore**。

> **`ReaderCaps` 必须能在不 pin 的前提下读到**，因此它由**加载期元数据**（索引族 + 构建参数，如"VARCHAR 上的 inverted"⇒ predicate + pattern_match + value_lookup）算出，作为**纯数据**存进 inventory 条目，**不是**必须持有索引对象才能调用的虚函数。理由见 §4.3：路径决策发生在 pin 之前，若读 caps 需要对象，分级存储里的冷索引会被无谓拉起。
>
> reader 上仍保留 `Caps()` 供自述（growing 快照、单测需要），但它是**一致性校验对象**而非查询期来源：pin 后的 `reader->Caps()` 必须等于 inventory 缓存的 caps，这条写成断言与测试。

### 4.2 类型擦除根

```cpp
// index/contracts/IndexReader.h
namespace milvus::index {

class IndexReaderBase {
 public:
    virtual ~IndexReaderBase() = default;

    // 自述面。查询期的路径决策**不读这里**（那条路走 inventory 缓存的 caps，
    // 见 §4.1/§4.3）；本方法用于 growing 快照、单测与一致性断言。
    virtual ReaderCaps  Caps() const = 0;

    virtual int64_t     Count() const = 0;
    virtual DataType    ValueType() const = 0;
    virtual int64_t     MemoryUsage() const = 0;   // 纯自述；缓存计费在 load 侧 translator
};

}  // namespace milvus::index
```

对比现状：`CellByteSize`/`SetCellSize`/`ComputeByteSize` 这组 cachinglayer 计费从索引根上**移除**——计费属于 segcore load 侧的 translator，reader 只报告自己的内存占用。`Upload` 移除——上传属于 indexbuilder 服务。

### 4.3 对象模型：谁创建、谁持有、何时 pin

> **术语**：本节及 §5 说的 **face = 查询 face**，即 [§4 合同树](#4-合同总览)里那批纯 mixin（`ScalarPredicateReader<T>`、`PatternMatchReader`、`TextMatchReader`、`NgramReader`、`SpatialReader`、`ScalarValueReader<T>`、`JsonIndexReader`）。它与 [§3 的「四面」](#3-设计原则)不是同一层：四面是按**调用方**切的四组职责（Reader / Appender / Builder / Loader），face 是 **Reader 这一面内部**按查询语义再切的接口，即 face ⊂ Reader 面。下文 `Pinned<Face>` 的 `Face` 就是上列类型之一。

**查询 face 不是每次查询新建的 proxy——它就是索引对象本身的一个接口视图。** 三类东西的创建时机完全不同：

| 对象 | 创建时机 | 生命周期 | 成本 |
|---|---|---|---|
| 实现类实例（`InvertedIndex<int64_t>` 等） | `Loader::Open()`，即索引加载时 | 长期存活；仅在被 cachinglayer 淘汰后重新加载时重建 | 一次加载 |
| `ReaderCaps` | inventory 构建时由加载期元数据算出 | 与 inventory 条目同寿 | 纯数据拷贝 |
| `Pinned<Face>` 句柄 | 取用时 | 栈上 RAII：cache pin + 类型化裸指针 | 两个指针 |

**inventory 持有的是 `CacheSlot`，不是索引对象。** 现状即 `CacheIndexBasePtr = shared_ptr<CacheSlot<IndexBase>>`（`Index.h:165`），W1 后为 `CacheSlot<IndexReaderBase>`。索引对象活在 slot 内、可被淘汰，因此取 face 必须**先 pin 后 cast**——pin 之前对象可能根本不存在。

查询期流程：

```text
1. caps = provider.Capability(field_id)              // 纯数据读：不 pin、不 cast
2. exec 按 caps 决定执行路径
3. pinned = provider.PinScalarPredicate<T>(op_ctx, field_id)     ← 每个表达式节点一次
       内部：slot->PinCells() → IndexReaderBase*
             sibling cast → ScalarPredicateReader<T>*
             返回 Pinned{ptr, keep_alive}
4. 每个 batch：pinned->In(...)                        // 直接虚调用，无 cast、无分配
```

**第 3 步的频率是每个表达式节点一次**，不是每 batch、更不是每行——cast 与 pin 的开销被整个表达式求值摊薄。这正是现状 `EnsurePinnedIndex()`（`Expr.h:395`，幂等、路径确定后才调）的形态，W1 只是把它从 exec 迁进 `IndexProvider`。

**第 1 步不得 pin 是硬约束。** 现状代码注释已写明理由："短路路径（TextIndex/PkIndex/JsonStats）与 RawData 路径永不调用它，标量索引 cell 在分级存储里保持冷态"。若把 caps 做成必须持有对象才能调的虚函数，路径决策就会把冷索引全部拉起——这是线上冷取放大的直接来源（[04-indexing §6](../segcore_refactor/04-indexing.md) 已把"`Capability()` 不触发 pin"列为必测项）。

**growing 侧的不对称**：appender 不是"每次写入去取"——`GrowingIndexSet` 长期持有 appender（每个建索引字段一个），insert 路径直接调 `Append`。读侧 `ReaderSnapshot()` 返回的是**另一个对象**：commit/reload 时产生的不可变快照，所有并发查询经 `shared_ptr` 共享同一份。**每次 commit 创建一次，不是每次查询**。

两侧因此都不存在"每查询一个 proxy"：sealed 是 pin 一个长期对象，growing 是共享一个 commit 期快照。

## 5. 查询面

所有 reader **不可变**：由 `Seal()` 或 `Loader::Open()` 产生后线程安全、可无锁并发读。bitmap 语义统一：1 = 命中；bitmap 尺寸 = `Count()`。

> **输出形态只有一种：`TargetBitmap`。** 谓词族、候选族、nested 投影一律如此，不提供稀疏 offsets 或回调变体。
>
> 理由：**选择率是查询的运行时属性，不是 reader 的静态属性**——同一个 `In` 查罕见 term 命中 10 行、查常见 term 命中 90%；常见三元组的 ngram 候选、覆盖全域的空间查询同样接近全表。既然形态无法按族静态决定，就只能选退化时不爆炸的那个：bitmap 大小恒为 `Count()/8`，与选择率无关；极稀疏时的代价是 `find_first`/`find_next` 的字跳扫描（1 亿行约 150 万次字读，亚毫秒级）。反过来稀疏 offsets 在稠密结果上是 8 字节/行 vs 1 比特/行的 **64 倍膨胀**——9000 万命中就是 720MB，不可接受。
>
> 因此 `SpatialReader::Candidates` 现状的 `std::vector<int64_t>` 属于 RTree 的历史写法，规整为 bitmap（消费者迭代置位即可，refine 逻辑不变）。

### 5.1 `ScalarPredicateReader<T>`

```cpp
// index/contracts/ScalarPredicateReader.h
template <typename T>
class ScalarPredicateReader {
 public:
    virtual TargetBitmap In(size_t n, const T* values) const = 0;
    virtual TargetBitmap NotIn(size_t n, const T* values) const = 0;
    virtual TargetBitmap Range(const T& value, OpType op) const = 0;
    virtual TargetBitmap Range(const T& lo, bool lo_inc,
                               const T& hi, bool hi_inc) const = 0;
    virtual TargetBitmap IsNull() const = 0;
    virtual TargetBitmap IsNotNull() const = 0;
};
```

与现状的差异：`Query(DatasetPtr)` 这个 knowhere 风格的万能入口删除；`Build`/`Size`/`GetIndexType` 不在查询面上；**`InApplyFilter` / `InApplyCallback` 一并删除**，理由见下。

> **`InApplyFilter` / `InApplyCallback` 为什么不进合同。** 盘点结果：`InApplyFilter` 生产代码**零调用点**（唯一引用是 `JsonFlatIndexTest.cpp:799`），`RTreeIndex` 的实现还是 throw 空壳；`InApplyCallback` 只有一个消费者——`PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex`（`UnaryExpr.cpp:804,807`），用于 ARRAY 整体相等走元素级索引时逐元素求交 + 1% 提前退出。
>
> 它以"避免物化完整 bitmap"为名，但两个实现都是 `TargetBitmap bitset(Count()); terms_query(...); apply_hits_with_callback(...)`——**照样物化完整 bitmap 再遍历**（`InvertedIndexTantivy.cpp:428` 处还留着 `todo: could push-down the callback to tantivy query`）。真实收益接近零，而 exec 侧那个提前退出用普通 `In()` + bitmap 求交同样能做，且比现在的 `unordered_set` 求交更快。
>
> 更根本的理由是**输出形态只保留一种**（见 §5 开头）：稀疏输出曾是这个接口最后的立足点，而稀疏与否是查询的运行时属性，不是合同层的关切。若将来 tantivy 真做了流式命中，那是**实现内部**省掉中间分配，不需要合同上预留形状。

### 5.2 `PatternMatchReader`

```cpp
class PatternMatchReader {
 public:
    // pattern 是 SQL LIKE 原文（非 regex），op ∈ {Match, PrefixMatch, PostfixMatch, InnerMatch}
    virtual TargetBitmap PatternMatch(std::string_view pattern, OpType op) const = 0;
};
```

提供者：tantivy inverted、marisa（prefix）、**FMIndex（它只有这一个面**——现在它作为 `ScalarIndex<std::string>` 背着 20 个不相关方法）。

### 5.3 `TextMatchReader`

```cpp
class TextMatchReader {
 public:
    virtual TargetBitmap MatchQuery(std::string_view query,
                                    uint32_t min_should_match) const = 0;
    virtual TargetBitmap PhraseMatchQuery(std::string_view query,
                                          uint32_t slop) const = 0;
    virtual TargetBitmap FuzzyMatchQuery(std::string_view query,
                                         uint32_t max_edit_distance) const = 0;
};
```

实现是对 tantivy reader 快照的组合封装。**不继承 inverted**——现在 `TextMatchIndex : InvertedIndexTantivy<std::string>` 顺带背上了 `In`/`Range` 等它永远不该被调用的方法。

### 5.4 `NgramReader`——二段执行的正确切分

```cpp
class NgramReader {
 public:
    virtual bool CanHandle(std::string_view literal, OpType op) const = 0;

    // Phase1：候选生成。结果 AND-merge 进 candidates；语义上是超集（caps.exact = false）。
    virtual void Candidates(std::string_view literal, OpType op,
                            TargetBitmap& candidates) const = 0;
};
```

**Phase2（验证）从索引中删除。** 现状 `ExecutePhase2(literal, op, exec::SegmentExpr*, ...)` 的本质是"对候选行取原值重新求值"——取值走 columnar-format 的 `Take`/`Scan`，求值本来就是 exec 的表达式内核。切分后：`index → exec` 反向边消失，且不需要引入回调（[11-cross-cutting §2.4](../segcore_refactor/11-cross-cutting.md) 原方案的 `ValueFetcher` 回调也不再需要）。

> **这不是新发明，是向 geometry 看齐。** `RTreeIndex::QueryCandidates` 出候选、exec 的 `PhyGISRefineConjunctExpr` 做精确验证——同一模式在空间族已经正确落地多时（§5.6）。ngram 是这个模式的**未完成实现**：它把验证留在了索引里，于是拖出一条 `index → exec` 反向边。

### 5.5 `ScalarValueReader<T>`——取值面

```cpp
template <typename T>
class ScalarValueReader {
 public:
    virtual std::optional<T> Lookup(int64_t offset) const = 0;
    // 批量反查；实现可按内部布局聚簇。输出对接 columnar-format 的 TakeResult 约定。
    virtual void Gather(const int64_t* offsets, int64_t count,
                        const std::function<void(int64_t i, const T*, bool valid)>& out) const = 0;
};
```

对应现状 `Reverse_Lookup` + `SupportFastReverseLookup` + `HasRawData`。"反查太贵就回原始列"的**决策不在这里**——`caps.cheap_value_lookup` 自述代价，选择权在消费者（reduce 的 Materializer / exec）。

### 5.6 `SpatialReader`——空间关系谓词

Geometry 与其他标量族的差别**只在算子**：谓词是空间关系（intersects / contains / within / …）而非点或范围比较。生命周期、构建、持久化、pin 完全同构，因此它是标量的一族，不是独立组件。

```cpp
// index/contracts/SpatialReader.h
class SpatialReader {
 public:
    // 候选生成：MBR 粗筛，结果是超集（caps.exact = false、caps.spatial = true）。
    // 精确的空间关系判定由 exec 对候选行取原值完成。
    virtual TargetBitmap Candidates(SpatialOp op, const Geometry& query_geom) const = 0;
};
```

- `SpatialOp` 是**契约层的 native 枚举**，不是 `proto::plan::GISFunctionFilterExpr_GISOp`——现状 `QueryCandidates` 直接吃 proto 枚举（`RTreeIndex.h:184`），违反"pb 只在 adapter"（[总览 §5 规则 2](README.md#5-全局硬规则)），plan→native 的映射发生在 plan/exec 侧。
- `RTreeIndex` 被迫实现的 `In`/`NotIn`/`Range`×2/`InApplyFilter`/`InApplyCallback` 与 throw 的 `Reverse_Lookup` 全部移除；`Query(DatasetPtr)` 万能入口一并删除。
- **候选族的共性**：`SpatialReader`、`NgramReader` 与 nested 索引上的 ARRAY 相等（§5.8）是同一模式的三个实例——索引给超集、exec 做精确验证。它们共享 `caps.exact = false` 的语义约定与统一的 bitmap 输出，消费者的处理骨架相同（取候选 → 按候选行取原值 → 重新求值）。

### 5.7 `JsonIndexReader`——path 寻址的谓词索引

**范围已收窄**：本族只覆盖建在**值**上的 JSON 索引——逐 path 的 cast index，以及 `JsonFlatIndex`（一个 tantivy 索引覆盖该 field 的全部 path）。shredding 已整体划归 columnar-format（[§1 裁决](#1-范围)），本族不再承担 shredded 列路由。

JSON 索引与常规标量索引的唯一结构差别是**多一层 path 寻址**：同一个 field 上，`a.b` 是 int64 谓词面、`a.c` 是 string 谓词面。所以本族不定义新的查询语义，只定义**路由**：

```cpp
// index/contracts/JsonIndexReader.h
class JsonIndexReader {
 public:
    virtual ~JsonIndexReader() = default;

    // 该 path 上是否有可用的谓词面；有则返回类型擦除根，
    // 消费者按 cast_type 做一次 sibling cast 得到 ScalarPredicateReader<T> / PatternMatchReader。
    // 返回 null = 该 path 无索引，exec 回退列扫描（可能落在 shredded 子列上，也可能落在
    // 原始 JSON 列上——那是 columnar-format 的事，本族不知道也不需要知道）。
    virtual std::shared_ptr<const IndexReaderBase>
    Resolve(std::string_view path, DataType cast_type) const = 0;

    // path 存在性。建在值上的倒排能独立回答（json_exist_query），无需回读列。
    virtual TargetBitmap Exists(std::string_view path,
                                JsonValueType type = JsonValueType::Any) const = 0;

    virtual std::vector<DataType> CastTypesOf(std::string_view path) const = 0;
};
```

- `JsonFlatIndex` 实现 `JsonIndexReader`：`Resolve` 返回一个绑定了 path 的 `JsonFlatIndexQueryExecutor<T>`——今天它已经是这个形态（`JsonFlatIndex.h:34`），只是靠继承 `InvertedIndexTantivy<T>` 拿到查询面；改为组合后它直接实现 `ScalarPredicateReader<T>`。
- 逐 path 的 cast index 不需要专门合同：它就是普通的 `ScalarPredicateReader<T>`，在 inventory 里以 `(field, path)` 为键注册；`JsonIndexReader` 对它退化为一层查表。
- `ReaderCaps::json_paths` 的含义随之收紧为「该索引对象按 path 寻址」，不再暗示 shredded 列的存在。「这个 path 有 typed 子列」是**列的能力自述**，由 columnar-format 提供（见 §12.5）。
- `IndexBase::GetCastType`/`Exists` 从根上移除，收进本族。
- `JsonKeyStats` 不出现在本节：它迁出 index，那批 `NotImplemented` 随迁移一并消失。

### 5.8 Nested（元素级）索引：坐标与投影

**nested 不是独立的族，是现有实现类上的一个模式位**：`BitmapIndex`（`BitmapIndex.h:88`）、`StringIndexSort`（`StringIndexSort.cpp:229`）都带 `is_nested_index_`，并**持久化在产物里**（`BinarySet` 的 `"is_nested_index"` 项、新 writer 的 `is_nested` meta，加载时做 `is_nested_index_ || loaded` 的兼容合并）。它表示：索引的对象是数组**元素**，而非行。

#### 投影到行由 reader 完成——这是正确性要求，不是风格选择

nested 索引内部命中的是元素坐标，但**对外必须返回行级 bitmap**。原因是元素级布尔组合会得出错误答案：

- `contains(1) AND contains(2)`：行 r 的元素 0 命中 1、元素 1 命中 2，两个元素级 bitmap 相与得到空（不同元素位），而行级正确答案是**真**。
- `NOT contains(1)`：元素级取反得到"不等于 1 的元素"，对行过滤无意义；正确语义是"不存在任何元素等于 1"，是行级存在量词的取反。

因此投影必须发生在组合之前，即在 reader 内部：**行 r 置位 ⟺ ∃ 元素命中**。

现状违反了这一点：元素 offset 被泄给 exec，由 `to_row_offset` lambda（`UnaryExpr.cpp:741`）经 `array_offsets->ElementIDToRowID()` 在消费者侧转换。目标态下该 lambda 消失，[§3 原则 5](#3-设计原则)（坐标一律是行号）随之成立——**当前实现是违例，不是该原则的例外**。

投影之后 `arr == [1,2,3]` 仍需 exec 侧 `is_same_array` 精确验证（存在量词 ≠ 精确相等），即候选族的常规形态，与 geometry / ngram 同构。

#### 元素→行映射存哪里

映射体就是 `ArrayOffsetsSealed` 持有的 `std::vector<int32_t> row_to_element_start`（前缀和，4 字节/行）。三个选项：

| 方案 | 评价 |
|---|---|
| **注入 segment 共享的 `IArrayOffsets`（推荐）** | 无重复；单一真相来源 |
| 索引产物内自带一份前缀和 | 与列各存一份：1000 万行的字段多 40MB；reopen / schema evolution 后两份存在不一致风险 |
| postings 里直接存 row id（构建期投影） | 最省，但把投影固化在构建期，丢失元素重数与位置信息，多层嵌套（见下）无回旋余地 |

推荐注入的关键依据：`array_offsets_map` 存放在 segment 的 runtime state 中，是 `shared_ptr<ArrayOffsetsSealed>`（`ChunkedSegmentSealedImpl.h:345`），**常驻元数据，不是可淘汰的 cache cell**。因此 reader 引用它**不会 pin 任何列数据**，不影响 index-only 执行——这是"自带一份以免拖起列"这个理由不成立的原因。

代价与约束：reader 不再能由 `Loader::Open` 单独构造完毕，需由 segcore 在加载时注入；持 `shared_ptr` 以保证 COW 换代后不悬垂。依赖方向合法：`IArrayOffsets` 随 [W0](00-w0-foundation.md#36-common-hygiene-轨道与-w1-并行) 迁入 columnar-format（L1），index 是 L2，L2→L1 是下行边，这条依赖由 reader 侧的注入建立（builder 侧不认识列，见 §6.1）。

#### 多层嵌套：不是 roadmap，是子列的固定形态

原先本节记作「roadmap，本轮只做单层」。[宽表建模设计文档](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「二、Schema 系统」把这件事变成了硬约束：

> 每一个具体的子列由一个物理列表示，**类型固定为 `Array<Array<...<T>>>`，深度由路径中的 `Array` 数量决定**，路径中的 `Struct` 不影响深度。**每层包括一个 `offsets`，每个 nullable 层包括一个 `validity` bitmap**。

即：多层不是将来要加的能力，而是嵌套子列的**通用形态**，单层只是深度 = 1 的特例。三条直接后果：

1. **元素→行的映射不是"一个 `IArrayOffsets`"，是一条 offsets 链**（每层一个）。投影是沿链**逐层折叠**的前缀和复合，而非单次 `ElementIDToRowID`。上表的注入方案随之升级为"注入该子列的 offsets 链"，其余评价不变。
2. **"注入共享实例"的推荐被强化**：该文档「五、存储层的更新」明确 *Array of struct 里的 offset 共享*。索引内自带一份前缀和会与存储层的共享方向正面冲突，且层数越多复制代价越大。
3. **本节查询面合同按 N 层写、N=1 退化**，不得出现单层假设。对外形态不变——reader 始终输出行级 bitmap，存在量词沿嵌套层级逐级投影到行；变化被限制在 builder 侧与精确验证的复杂度上。这也是不选"postings 直接存 row id"的原因：它在构建期抹掉层次信息。

**节奏依赖（不由本波决定）**：该文档「二十、后续功能」把**标量索引**列为嵌套建模的后续项。因此 W1 在嵌套面上**只定形状、不定实现**，实现节奏由宽表建模的推进牵引。

## 6. 构建面与加载面

两个面的分界是**输入不同**，不是阶段先后：

| | 输入 | 输出 | 谁调用 | 在哪个进程 |
|---|---|---|---|---|
| **Builder** | 列数据（数组或列游标） | Artifact | indexbuilder 服务 / segcore load 就地建 | 建索引任务 / querynode（interim） |
| **Loader** | 落盘字节 | Reader | segcore load | querynode |

看起来重叠的只有一处：序列化被放在 `Artifact::Serialize` 而不是持久化面自己身上。这是**有意的不对称**，三条理由：

1. **写方向没有独立调用点**。序列化永远紧接 `Seal()` 发生，从不单独发生；反序列化则发生在另一个时间、通常另一个进程，那时没有 Builder。一个同时提供 Serialize/Deserialize 的对称 Codec，将是一个**没有任何调用点同时用到两个方法**的类——对称只是形式上的。
2. **文件式族的写方向无法再分一层**。tantivy 系（inverted / text / bson）在构建过程中就直接往本地目录写字节，字节布局是 builder 的内部实现；事后再交给一个编解码器去"编码"是虚构的抽象，这些族的 `Serialize` 实质只是把已经落在本地的文件集交出去。
3. **Artifact 知道自己的物化形态**。它可能是内存结构（bitmap、marisa、有序数组），也可能是本地文件集（tantivy 系）；由它自己实现 `Serialize` 只需一次分派，由外部组件实现则要按族再分派一次。

所以这个面实际是**只读方向的组件**，命名为 `IndexLoader`：在没有 Builder 在场时，把落盘产物变回可读对象。它与 Builder 的唯一耦合是**格式约定**（同族的 Builder 写、Loader 读，必须一致），这个耦合靠同族同目录 + 往返测试约束，不靠共用一个类。

### 6.1 Builder

```cpp
// index/contracts/IndexBuilder.h
// 两族共用一个面。T 是**值类型**而非族：变长值用 view 类型表达
// （std::string_view / ArrayView / SparseRow view），dense vector 用 float
// 且 dim 在构造期已知。因此没有 Scalar/Vector 前缀。
template <typename T>
class IndexBuilder {
 public:
    virtual ~IndexBuilder() = default;

    // 静态自述：调用方据此决定怎么喂（§6.1.2）
    virtual BuilderInputSpec InputSpec() const = 0;

    // 唯一的数据输入面：push。调用方驱动。
    virtual void Add(size_t n, const T* values, const bool* valid) = 0;

    // 仅 form == LocalFile 的族（DiskANN）：数据已由物化器落到本地文件
    virtual void SetSourceFile(const std::string& path) {}

    virtual IndexArtifactPtr Seal() && = 0;
};

// 产物：内存 reader 或文件集合，由族自己决定物化形态
class IndexArtifact {
 public:
    virtual std::shared_ptr<IndexReaderBase> OpenReader() const = 0;   // 就地使用
    virtual void Serialize(storage::FileSink&) const = 0;              // 交给持久化
};
```

> **为什么没有 `Consume(ScanCursor&)`（pull 面）**。两条构建路径的源与模式本就不同：**离线构建**（indexbuilder，生产主路径）的源是远端 manifest/binlog，且**已经是 push**——`IterateFieldDataFromManifest(..., const std::function<void(FieldDataPtr)>& consumer, max_inflight_bytes)`（`storage/Util.h:352`）逐 batch 回调、后台池解码、按输入字节限流，那里根本没有 segment、没有列对象、没有 `ScanCursor`；**就地建**的源才是已加载的列（`generate_interim_index` 收 `ChunkedColumnInterface`，`ChunkedSegmentSealedImpl.h:1385`）。把 pull 摊成 push 只是调用方写个循环，把 push 包装成 pull 要加线程/协程或缓冲反转。所以统一到 push。
>
> 连带结论：**builder 的输入货币是裸数组，不是任何组件的对象**——它不认识列、不认识游标、不认识存储格式，`index → columnar-format` 在 builder 侧归零（reader 侧因 [§5.8](#58-nested元素级索引坐标与投影) 的 `IArrayOffsets` 注入仍在）。

各族的 builder 是这个面的**实现**而非新的面：text 族的分词器配置、json 族的 path 配置都是构造参数。


#### 6.1.1 输入形态：五档，两族交错

"标量流式、向量全量"是错的分界——**标量自己就横跨三档，向量落在其中两档**。实测：

| 档 | 输入形态 | 谁 | 证据 |
|---|---|---|---|
| **A** 真流式 | 逐片喂进引擎，索引类不缓冲 | tantivy 系（inverted / text / ngram / json flat / bson） | `wrapper_->add_data<T>(ptr, n, offset)` 逐 slice（`InvertedIndexTantivy.cpp:686`） |
| **B** 全量驻留 | 输入可单遍，但全量必须在内存里才能成型 | `ScalarIndexSort`、`BitmapIndex`、`StringIndexMarisa`、`FMIndex`、`RTreeIndex` | marisa 收满 keyset 再 `trie_.build()`，然后**再走一遍**回填 `str_ids_`（`StringIndexMarisa.cpp:173-205`）；FM 拼接全部 docs 交 libsais（`FMIndex.cpp:172`）；RTree `bulk_load_from_field_data`（`RTreeIndex.cpp:366`） |
| **B+** 连续缓冲 | 全量驻留**且要求单块连续内存** | knowhere 内存索引 | `CacheRawDataToMemory` → 拼成单个 tensor → 一次 `index_.Build(dataset)`（`VectorMemIndex.cpp:533-600`） |
| **C** 需先验统计 | 选型前要先看数据，**两遍** | `HybridScalarIndex` | `SelectBuildTypeForPrimitiveType` 先扫求基数（**有早退**，distinct 达上限即 `break`，`HybridScalarIndex.cpp:167`），再 `GetInternalIndex()->BuildWithFieldData` |
| **D** 本地文件 | 原始数据落成本地文件，按**路径**交付 | DiskANN | `CacheRawDataToDisk<T>` → `DISK_ANN_RAW_DATA_PATH`（`VectorDiskIndex.cpp:460-462`）；它明确**不要**数据在内存里 |

> **今天"看起来所有族都全量驻留"是入口造成的假象**：共同入口 `storage::CacheRawDataAndFillMissing` / `CacheRawDataToMemory` 先把整个字段拉进内存，与族本身的需求无关。A 档并不需要——改成游标输入后 A 档能真省内存，B/B+ 省不了。

#### 6.1.2 处理：面统一，差异收进声明

不给每族开一个 Builder 接口，也不硬塞进单一签名。把差异从**接口**挪到**数据**：

```cpp
struct BuilderInputSpec {
    enum Form { Streaming, Contiguous, LocalFile } form = Streaming;
    bool needs_second_pass = false;       // C 档：Seek(0) 重扫；第一遍可早退
    std::vector<FieldId> side_inputs;     // 非本列的构建输入，见下
};

// 每个族的 builder 静态自述，调用方据此决定怎么喂
virtual BuilderInputSpec InputSpec() const = 0;
```

- Builder 合同仍只有一个：`Add(n, values, valid)` + `Seal() → Artifact`。`Add` 表达的是**输入通道**，不是"能流式构建"；内部缓冲多少、要不要第二遍是实现细节。
- 三种物化方式（不物化 / 连续 buffer / 本地文件）由**一个共享物化器**实现一次，不是每族抄一遍。这与 [§4.1](#41-能力描述符) 的 `ReaderCaps` 是同一手法，只是在构建侧。
- **多遍必须写进合同**：C 档要求 `ScanCursor::Seek(0)` 重扫可用（[#51504](https://github.com/milvus-io/milvus/pull/51504) 的 `ScanCursor` 有 `Seek(position)`，表达得了），代价是冷列二次 IO。
- **`side_inputs` 不是预留**：`VectorMemIndex::Build` 读 `VEC_OPT_FIELDS` 并调 `CacheOptFieldToMemory`（`VectorMemIndex.cpp:539-547`），partition key isolation 需要**另一个字段**的数据作构建输入。单游标签名装不下——Builder 的输入不总是"本列"。

**连带收益**：今天三个几乎同义的入口 `CacheRawDataAndFillMissing`（标量）/ `CacheRawDataToMemory`（向量内存）/ `CacheRawDataToDisk`（DiskANN）散在各族里，且**都绑死 `FileManager`**。收敛成"声明 + 共享物化器"后，`FileManagerContext` 从 builder 上彻底消失——这正是 [§3 原则 6](#3-设计原则) 的落地路径。


### 6.2 Loader 与 IO 注入

```cpp
class IndexLoader {
 public:
    virtual ~IndexLoader() = default;
    virtual std::string Family() const = 0;   // "inverted" / "bitmap" / "text" / "json_stats" / ...

    // 与 IndexArtifact::OpenReader() 是通向 Reader 的两个入口：
    // 前者从落盘产物打开，后者从刚构建完的产物就地打开。
    virtual std::shared_ptr<IndexReaderBase>
    Open(storage::FileSource&, const LoadOptions& opts) = 0;   // opts: mmap、warmup
};
```

方法名用 `Open` 而非 `Deserialize`：mmap 形态下并不发生"反序列化"（不完整物化），且 `Open` 与 `Artifact::OpenReader()` 成对，读出两个入口的同构关系。

`FileSink`/`FileSource` 是 storage 提供的窄接口（本地暂存 + 远端读写），**取代索引类持有 `FileManagerContext`**。`Serialize`/`Load`/`Upload`/`LoadUnified`/`UploadUnified` 从索引类上全部移除：序列化在 Artifact，打开在 Loader，上传编排在 indexbuilder 服务，加载编排在 segcore load。

### 6.3 Hybrid 变为 Builder 策略

`HybridScalarIndex` 的"按基数选 bitmap 或 inverted"是**构建期决策**：Builder 在 `Seal()` 时选型并把选择记进 artifact 元数据，`Loader::Open` 直接返回选中的具体 reader。运行时转发类删除。

## 7. Growing 面

**growing 标量索引不是未来假设，而是已有生产事实**：`TextMatchIndex` 的 growing 构造（commit interval + background merge + `AddTextsGrowing` + `Commit`/`Reload` + reader 快照，`TextMatchIndex.h:31-37`）就是一个 growing 增量索引；segcore 的 `ScalarFieldIndexing<T>`（`FieldIndexing.h:141`）是另一套并行机制。本设计把这个模式提炼为统一合同：

```cpp
// index/contracts/GrowingIndex.h
template <typename T>
class GrowingScalarIndex {
 public:
    virtual ~GrowingScalarIndex() = default;

    // insert 路径独占持有；与读并发安全
    virtual void Append(int64_t reserved_offset, size_t n,
                        const T* values, const bool* valid) = 0;

    // 读侧拿快照。快照不可变，覆盖 [0, snapshot->Count())。
    // 返回空表示尚无可读快照（未达构建阈值）。
    virtual std::shared_ptr<const ScalarPredicateReader<T>>
    ReaderSnapshot() const = 0;

    // 水位：快照覆盖到哪一行。单调不减。
    virtual int64_t CommittedRows() const = 0;
};
```

三条关键语义：

1. **快照 + 水位，不承诺实时**。tantivy 的 commit/reload 天然是这个模型；今天 text match 的 commit 时滞是**隐式**的，合同把它变成显式水位。
2. **覆盖不足的桥接不在本合同内**。`[CommittedRows(), insert_barrier)` 这段未入索引的行怎么办（回退列扫描、还是等 commit）是 segcore/exec 的执行策略，索引只报告水位。这与 §5.5 取值面的"决策在消费者"是同一原则。
3. **"就地建"归 Builder 面，不归本面**。sealed 段加载时无索引就地建——今天有两处：`generate_interim_index`（`ChunkedSegmentSealedImpl.h:1385`，收 `ChunkedColumnInterface`，**vector 专属**：float/fp16/bf16/sparse 且无正式索引时建临时 binlog index）与 `CreateTextIndex`/`CreateTextIndexWithSchema`（`:252,2023`，text 从原始数据就地建）。它们走 [§6.1](#61-builder) 的 `IndexBuilder<T>::Add`，由调用方逐 chunk 从已加载的列喂进去，**不再是 growing 类的另一个构造分支**——`TextMatchIndex` 的四构造函数问题就此消解。

   > **就地建 ≠ growing**。两者都"在 segcore 里建索引"，但分属两个面：就地建是 **Builder**（一次性、独占、终结于 Artifact，输入是一根已加载的完整列）；growing 是 **Appender**（长期存活、与写并发、只产水位快照）。混淆这两者正是今天 `TextMatchIndex` 把 growing writer 和 sealed 就地建塞进同一个类的成因（§2.2）。

`GrowingTextIndex`（Append 文本、快照给出 `TextMatchReader`）同型。segcore 的 `GrowingIndexSet` 持有这些 appender，`FieldIndexing` 的散装机制退役。

### 7.1 vector 的 Appender 面：今天已存在，且带着与 `IndexBase` 同构的病

Appender **不是标量专属**。`segcore/FieldIndexing.h` 里两族今天都实现了 append：`VectorFieldIndexing::AppendSegmentIndexDense`/`Sparse`（`:281,287`）建 growing interim 向量索引，`ScalarFieldIndexing::AppendSegmentIndex`（`:171,177`）建 growing 标量索引。但共享根 `FieldIndexing`（`:51`）是**两族接口的并集**，5 个纯虚里 3 个 vector 专属、2 个 scalar 专属，两个子类各自 throw 掉对方那一半（§2.2）。

这给出四面化的第二份现场证据，并把面与族的关系钉死：

> **四个面对两族统一；每个面内部的接口按族分。** 面是按**调用方**切的（谁在用），族是按**数据与算法**切的（用什么结构）——两者正交。`FieldIndexing` 的错误正是把"两族共有一个面"误当成"两族共有一组方法"。

因此 growing 合同的形状是：`GrowingScalarIndex<T>` 与 `GrowingVectorIndex` **并列**，各自定义 `Append` 的签名（前者吃 `(values, valid)`，后者吃 dense/sparse 原始向量），共享的只有**语义**——快照 + 水位。两族在这三点上一致：

| 语义 | scalar | vector |
|---|---|---|
| 快照不可变、并发共享 | tantivy commit/reload | knowhere interim index 换代 |
| 水位（快照覆盖到哪一行） | `CommittedRows()` | 同；今天隐含在 `sync_data_with_index()` 里 |
| 阈值前无快照 | 无（`ScalarFieldIndexing::get_build_threshold()` 返回 0，`:197`） | 有（`VectorFieldIndexing::get_build_threshold()` 取配置，`:321`）——`ReaderSnapshot()` 返回空即表达 |

阈值差异**不构成分歧**：合同已用"返回空表示尚无可读快照"表达，scalar 只是阈值恒为 0 的退化情形。

**连带的 `IndexBase` 退役项**：`FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 返回 `PinWrapper<index::IndexBase*>`（`:128,131`）——growing 侧同样以 `IndexBase` 作类型擦除句柄。[§11.2 第 3 条](#112-处理决定)的"`IndexBase` 在 W1 内退役"必须把这两个出口一并算进去，否则 sealed 侧删干净了、growing 侧还留着一个引用。

## 8. 现有实现类 → 新合同映射

| 现类 | 查询面 | 构建/growing | 备注 |
|---|---|---|---|
| `InvertedIndexTantivy<T>` | `ScalarPredicateReader<T>` + `PatternMatchReader` | Builder | tantivy 封装降为内部引擎，不再是基类 |
| `BitmapIndex<T>` / `ScalarIndexSort<T>` / `StringIndexMarisa` | `ScalarPredicateReader<T>`（marisa 另 + `PatternMatchReader`） + `ScalarValueReader<T>` | Builder | `is_nested_index_` 模式位保留，投影移入 reader（§5.8） |
| `HybridScalarIndex<T>` | **消失** | Builder 选型策略 | §6.3 |
| `StringIndexSort` / `BoolIndex` | 同 sort/bitmap | Builder | 薄别名，随迁 |
| `FMIndex` | `PatternMatchReader` **仅此** | Builder | `SegcoreConfig` 依赖改构造参数注入（修复 [11-cross-cutting §2.5](../segcore_refactor/11-cross-cutting.md)） |
| `TextMatchIndex` | `TextMatchReader` | `IndexBuilder<std::string_view>` 的 text 实现 + `GrowingTextIndex` | 四构造函数拆到四个归属 |
| `NgramInvertedIndex` | `NgramReader` | Builder | Phase2 删除，`index → exec` 边消失 |
| `JsonFlatIndex` (+ QueryExecutor) | `JsonIndexReader` | `IndexBuilder<std::string_view>` 的 json 实现 | |
| json path cast index | `ScalarPredicateReader<T>` | Builder | inventory 按 (field, path) 注册 |
| `JsonKeyStats` | **迁出 index**。W1 内落到 `segcore/json_stats/`（断继承 + `git mv`）；终局子列升格 columnar-format、layout 目录留 segcore | 构建仍是离线任务，暂不接 L1 产物管道 | `NotImplemented` 泛滥消失；对 segcore 的 5 处 include 当场清零；`BsonInvertedIndex` 一并迁走。终局与过渡见 [§1](#1-范围) |
| `RTreeIndex` / geometry | `SpatialReader`（§5.6） | Builder | `QueryCandidates` 成为唯一查询面；点谓词重载与 throw 的 `Reverse_Lookup` 移除；GIS proto 枚举换 native `SpatialOp` |
| `SkipIndex` | **移出 index 合同** | | 归 columnar-format（zone-map/`CellSkipPredicate`） |
| `VectorMemIndex<T>` / `VectorDiskIndex<T>` | `VectorSearchReader` + `VectorValueReader`（§11.3） | `VectorIndexBuilder` + growing（接替 `VectorFieldIndexing`） | knowhere 交互原样内移，不重设计 |

## 9. 消费者对接

| 消费者 | 现状 | 目标 |
|---|---|---|
| exec 表达式 | `PinIndex` 拿 `IndexBase*` 后 `dynamic_cast` 到具体类型；能力探测靠 `Support*` + try | pin 出口给 typed reader；路径决策只读 `ReaderCaps`；`dynamic_cast` 清零（W1 出口标准） |
| exec ngram | `ExecutePhase1/2`，Phase2 传 `exec::SegmentExpr*` | Phase1 = `NgramReader::Candidates`；Phase2 = exec 用 columnar-format `Scan`/`Take` 取值后自行求值 |
| exec geometry | `QueryCandidates` + `PhyGISCoarseConjunctExpr`/`PhyGISRefineConjunctExpr`——**切分已正确** | 仅换合同：`SpatialReader::Candidates` + native `SpatialOp` + bitmap 输出；粗筛/精化的骨架不动，与 ngram 收敛为同一候选族处理流程 |
| exec ARRAY 相等 | `ExecArrayEqualForIndex`：`InApplyCallback` 逐元素回调 → `unordered_set` 求交 → `to_row_offset` 转坐标 → `is_same_array` 精确验证 | reader 内部投影到行（§5.8），exec 侧改为 `In()` 返回的行级 bitmap 逐元素 `inplace_and` + 1% 提前退出；`to_row_offset` lambda 与 `unordered_set` 一并消失 |
| indexbuilder | `ScalarIndexCreator` 调 `CreateIndex/Build/Serialize/Upload` | Builder + `Artifact::Serialize` + storage sink；Creator 变薄壳 |
| segcore load | `Load`/`LoadUnified` + cachinglayer 计费长在索引里 | `Loader::Open` + 计费在 load 侧 translator |
| segcore growing | `FieldIndexing`/`ScalarFieldIndexing` 散装机制 | `GrowingScalarIndex`/`GrowingTextIndex`，由 `GrowingIndexSet` 持有 |
| reduce 物化 | `ReverseDataFromIndex`（`segcore/Utils.h:151`） | `ScalarValueReader::Gather` |

## 10. 硬性规则（lint）

1. `index/` 不得 include `segcore/`、`exec/`、`query/`（现状违规：`NgramInvertedIndex` 2 处、`FMIndex` 1 处、`JsonKeyStats` 5 处——**全部本波清零**，其中 `JsonKeyStats` 随目录迁往 `segcore/json_stats/` 而消失，见 §1 过渡处理）。
2. `FileManagerContext`/`DiskFileManagerImpl` 不得出现在任何 Reader/Builder/Appender 的签名与成员中，只允许出现在各族 Loader/Artifact 的实现文件与 indexbuilder 上传编排内。
3. 实现类之间禁止继承（`grep ": public.*Index" `，只允许继承合同接口）；查询 face 不得继承 `IndexReaderBase`（纯 mixin，§4 注）。
3b. `ReaderCaps` 的查询期来源必须是 inventory 缓存的纯数据；lint 检查路径决策代码（`DetermineExecPath` 一族）不出现 pin 调用（§4.3）。
4. 能力缺失禁止用 `ThrowInfo(Unsupported)` 表达——lint 检查合同实现中不出现该模式。
5. cachinglayer 类型不得出现在任何合同签名（计费/pin 在 segcore load 侧）。
6. `knowhere` 头不得出现在共享根与标量族的合同及实现中，仅 vector 族及其 Loader/Artifact 可见（§11.2 第 5 条）。

## 11. 标量/向量共享面：盘点与处理

### 11.1 共享面积盘点

| 共享物 | 事实 | 性质 |
|---|---|---|
| 类型根 | `ScalarIndex<T>` 与 `VectorIndex` 都继承 `IndexBase`；两边都对它 Liskov 违约（vector 的 `BuildWithRawDataForUT` throw，scalar 的 `BuildWithDataset` throw）；根上还长着 json 专属的 `GetCastType`/`Exists`，vector 被迫继承 | 坏共享 |
| 统一句柄 | `CacheIndexBasePtr = CacheIndexPtr<IndexBase>`（`Index.h:167`）；segcore 的 `scalar_indexings` 与 `SealedIndexingRecord` 持同一句柄，load 路径对两族一视同仁 | 真共享（inventory 需要） |
| 生命周期/持久化面 | `Serialize→BinarySet`、`Load`×2、`Upload→IndexStatsPtr`、index_files 约定、mmap 标志、`CellByteSize` 计费——"构建→序列化→上传；下载→加载→pin→计费"两族完全同构 | 真共享（收益所在） |
| storage 管道 | `FileManagerContext`/`Mem`/`DiskFileManager` 被 17 个 index 头引用 | 真共享 |
| 工厂 | `IndexFactory::CreateIndex` 单点分派两族，`CreateIndexInfo` 参数袋混装 | 坏共享 |
| 被动头文件共享 | `Index.h` 根头 include knowhere 三个头 + `cachinglayer/CacheSlot.h`——**每个标量索引 TU 都在编译 knowhere**；`BinarySet = knowhere::BinarySet`（`common/Types.h:681`），标量索引的序列化货币也是 knowhere 类型 | 坏共享（纯历史） |
| 查询面 | `In/Range/bitmap` vs `Query(dataset, SearchInfo)/VectorIterators` | **零共享** |
| growing appender 根 | `segcore/FieldIndexing.h:51` 是两族接口的并集，两个子类各自 throw 掉对方那一半（§2.2、[§7.1](#71-vector-的-appender-面今天已存在且带着与-indexbase-同构的病)）；`get_chunk_indexing`/`get_segment_indexing` 又把 `IndexBase` 泄到 growing 侧（`:128,131`） | **坏共享**（与 `IndexBase` 同构，只是在 growing 侧） |
| growing 语义（快照 + 水位 + 阈值前无快照） | tantivy commit/reload 与 knowhere interim 换代是同一模型；阈值差异由"返回空快照"吸收 | **真共享**（语义共享，签名不共享） |

### 11.2 处理决定

1. **共享根收缩为生命周期根，且生命周期那半沉到 L1**。切成两段：

   | 段 | 内容 | 层 | 谁用 |
   |---|---|---|---|
   | 产物生命周期管道 | `Artifact`（`Serialize(FileSink&)`）、`ArtifactLoader`（`Open(FileSource&, LoadOptions&)`）、`FileSink`/`FileSource`/`LoadOptions`/`ArtifactStats`、极小根 `LoadedArtifact`（承载 `CellByteSize()` 计费） | **L1** | index 各族（下行边）、columnar-format 的 shredded 布局（同层） |
   | 索引查询根 | `IndexReaderBase : LoadedArtifact`（自述、`Count()`、类型擦除） | L2 | 仅 index |

   下沉的理由：这套管道**没有一处是索引专属的**。JSON shredded 布局（[§1](#1-范围)）同样是「离线构建、落盘、按需加载、参与缓存计费的派生产物」，需要同一套东西，而它在 L1；把管道留在 L2 就只剩两条出路——要么 L1→L2 反向边，要么 `BsonInvertedIndex` 那样把 `AddRecord`/`BuildIndex`/`LoadIndex`/`UploadIndex`/`CellByteSize` 再手写一遍。下沉后两者都不必。`ArtifactLoader::Open` 返回 `shared_ptr<LoadedArtifact>`，各层自己 downcast——与 §4.2 的类型擦除根同一手法，只是下移一层。

   设计验收标准：三种物化形态都装得下——knowhere `BinarySet`（内存 blob 集合）、DiskANN（本地大文件、流式）、mmap。**连带**：`IndexArtifact`/`IndexLoader`/`IndexStats` 的 `Index` 前缀随下沉失效，且 `CellByteSize` 涉及硬规则 4 的 cachinglayer 传染面——见 [§12.11](#12-未决问题)。
2. **查询面零共享写死**：vector 查询族与 scalar 各族并列（§11.3），不设计任何跨族查询合同。
3. **`IndexBase` 在 W1 内退役**：vector 同波迁移，**不需要 adapter 过渡**。顺序：立共享根与 Loader → scalar 各族迁移 → vector 归位 → 删 `IndexBase`。`CacheIndexBasePtr` 的句柄角色由 `IndexReaderBase` 接替。**出口清单必须含 growing 侧**：`FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 也返回 `PinWrapper<index::IndexBase*>`（`FieldIndexing.h:128,131`），见 [§7.1](#71-vector-的-appender-面今天已存在且带着与-indexbase-同构的病)。
4. **工厂拆族**：`CreateIndexInfo` 拆散，族级 loader/builder registry 取代 `IndexFactory` 的 God switch。
5. **knowhere 逐出标量路径**：标量族合同与实现零 knowhere include；`BinarySet` 只出现在 vector 族的 Loader/Artifact。收益：标量索引编译隔离、knowhere 升级不再重编全部标量索引。
6. **storage 管道收到 Loader/Artifact 边界之后**（两族同规则），17 个头的 `FileManager` 引用缩到各族 Loader/Artifact 实现文件内。

### 11.3 vector 的四面归位（不重设计）

vector 纳入 W1 的范围是**归位**：把现有 `VectorIndex` 的 virtual 面按四面重新安放，knowhere 交互逻辑原样搬进实现，不改行为，基准照跑。归位对象不止 sealed 侧的 `VectorIndex`，还包括 growing 侧的 `VectorFieldIndexing`（[§7.1](#71-vector-的-appender-面今天已存在且带着与-indexbase-同构的病)）。

```cpp
// 查询族（与 scalar 各族并列）
class VectorSearchReader {
 public:
    virtual void Search(const DatasetPtr&, const SearchInfo&,
                        const BitsetView&, OpContext*, SearchResult&) const = 0;
    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr&, const knowhere::Json&, const BitsetView&,
              OpContext*) const = 0;
    virtual bool RefineEnabled() const = 0;
};

// 取值族（GetVector / HasRawData 对应物）
class VectorValueReader { /* GetVector 系 */ };

// Appender 面（growing interim 索引）：与 GrowingScalarIndex<T> 并列，
// 语义相同（快照 + 水位），签名按族分。
class GrowingVectorIndex {
 public:
    virtual void AppendDense(int64_t reserved_offset, int64_t size,
                             const void* data) = 0;
    virtual void AppendSparse(int64_t reserved_offset, int64_t size,
                              int64_t new_dim, const void* data) = 0;
    virtual std::shared_ptr<const VectorSearchReader> ReaderSnapshot() const = 0;
    virtual int64_t CommittedRows() const = 0;   // 阈值未达时快照为空
};
```

四个面对两族**统一**（Reader / Appender / Builder / Loader），差别只在面内的接口按族分：

| 面 | scalar | vector | 关系 |
|---|---|---|---|
| Reader | §5 的各查询 face | `VectorSearchReader` / `VectorValueReader` | 形态同、内容**零共享**（§11.2 第 2 条） |
| Appender | `GrowingScalarIndex<T>` | `GrowingVectorIndex` | 语义共享（快照 + 水位），签名分族（[§7.1](#71-vector-的-appender-面今天已存在且带着与-indexbase-同构的病)） |
| Builder | `IndexBuilder<T>::Add` + `Seal()` | 同一个面；knowhere 内存索引落 B+ 档、DiskANN 落 D 档 | **面统一**，差异收进 `BuilderInputSpec`（[§6.1.1/6.1.2](#611-输入形态五档两族交错)） |
| Loader | 同一套 | 同一套 | 完全同构 |

> **Builder 面为什么可以共享，而 Reader 面不能**——两者不是双标。Reader 面零共享是因为查询**语义**完全不同（`In/Range/bitmap` vs `Search(dataset, SearchInfo)`），没有可共享的抽象；Builder 面共享是因为"喂数据 → 成型 → 产出 Artifact"两族确实同构，真实差异只在**输入形态**，而输入形态横切两族（[§6.1.1](#611-输入形态五档两族交错) 的五档里，标量自己就占三档），因此按族切是错的切法，按形态声明才是对的。
>
> 落地时先验一条：`BuilderInputSpec` 的三种 form（Streaming / Contiguous / LocalFile）加 `needs_second_pass` 与 `side_inputs`，是否装得下 knowhere 全部索引类型。装不下就补声明位，**而不是给 vector 开一个独立的 Builder 面**。

## 12. 未决问题

1. **segcore pin 出口的类型形态**：typed `Pin<ScalarPredicateReader<T>>`（无装箱，需要 inventory 做一次每-batch downcast）vs `ScalarValue` variant 抹平类型（契约简单，热路径装箱）。倾向 typed，需要基准确认 variant 的实际开销后才有资格翻案。
2. **growing 水位的补齐策略**是统一语义（一律回退列扫描）还是按索引族策略化（text match 允许滞后）。涉及正确性语义（文本查询结果是否包含最新写入），需要与产品语义对齐后定。
3. **`T = std::string` 是否改 `std::string_view`**：查询面全部只读，view 化可消除一批拷贝，但牵动 tantivy FFI 签名。
4. ~~**`InWithCallback` 优化口的去留**~~ **已结案：删除。** 盘点结果与理由见 [§5.1](#51-scalarpredicatereadert) 注。
5. **同一 path 既有 path index 又有 shredded 子列时怎么选**：shredding 迁出后这不再是两个索引之间的选择，而是**索引 vs 列扫描**的普通 `DetermineExecPath`——但 exec 必须知道「这个 path 有 typed 子列」（typed 子列扫描远快于原始 JSON 列逐行解析），而这是**列的能力自述**，index 侧的 `ReaderCaps` 给不出。需要 columnar-format 的列合同暴露等价物（#51504 的 `ColumnPlanner` 是自然落点），并在 exec 侧与 `FieldIndexCapability` 合流。**跨 W1 与 W1∥ 两波，必须两边同时定**。
6. **vector 查询族合同的细化**（`SearchInfo` 的归属、iterator 生命周期）：若 §11.3 的骨架在落地时不够，独立成 `02-vector-index.md`。~~growing vector interim index 的 appender 形态~~ **已结案**：Appender 面两族共有、签名按族分，语义（快照 + 水位）共享——见 [§7.1](#71-vector-的-appender-面今天已存在且带着与-indexbase-同构的病)。剩余待定的只有 `GrowingVectorIndex::Append` 的具体签名是否原样保留 dense/sparse 二分。
7. ~~**候选族的输出形态统一**~~ **已结案：统一 `TargetBitmap`。** 选择率是查询的运行时属性而非族的静态属性，理由见 [§5 开头](#5-查询面)。
8. **`RTreeIndex` 现有点谓词重载的真实消费者**：`In`/`NotIn`/`Range` 在 WKB 值上的语义是否有实际调用方（可能是 WKB 字节相等，而非空间关系）。若有，需保留一个 `ScalarPredicateReader<std::string>` 面而非直接删除——W1 开工前 grep 确认。
9. ~~**`IndexCodec` 是否改名**~~ **已结案：改名 `IndexLoader`，方法 `Deserialize` → `Open`。** 该面只承担读方向，`Codec` 承诺双向、与职责不符；理由见 [§6](#6-构建面与加载面)。
10. **L1 产物管道的精确落点与命名**（§11.2 第 1 条的两个尾巴）：① 落在 `storage` 内（它的一句话定义「字节与文件的世界，不认识 index、segment、查询」正好覆盖），还是新立一个 L1 小组件？倾向前者，但需确认不会把 storage 变成第二个杂物间。② 名字去掉 `Index` 前缀（`Artifact`/`ArtifactLoader`/`ArtifactStats`）——本文档暂仍写 `IndexArtifact`/`IndexLoader`，等落点定了一次改完，避免两轮改名。③ **与总览硬规则 4 冲突**：`CellByteSize()` 返回 `cachinglayer::ResourceUsage`，而硬规则 4 规定 cachinglayer 类型只允许出现在 columnar-format 与 segcore 内部。现状 `IndexBase::CellByteSize`/`SetCellSize` 已经违反了这条；管道下沉会把它固化到 L1。要么放宽规则（承认「参与分级缓存的产物」是一等概念），要么用 L0 的中性 `ResourceUsage` 类型在边界转换。**W1 开工前必须裁决**，否则 lint 规则自相矛盾。
11. **上游阻塞：嵌套结构的查询节点数据表达尚未设计**（[宽表建模](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「六、查询节点的数据表达」仍是 TODO：内存/mmap/Vortex 如何表达嵌套、仅单子列加载）。本文档 [§5.8](#58-nested元素级索引坐标与投影) 的逐层投影与下一条的注入点**都建立在这个未定项之上**。同时 [#51504](https://github.com/milvus-io/milvus/pull/51504) 的 `ColumnInterface`（`ScanCursor`/`ScanBatch` = values + validity + row_ids）目前是**平坦列**接口，没有嵌套层次的表达位置——**若在嵌套表达定型前把 `ColumnInterface` 冻结，之后加嵌套就是二次改接口**。W1∥ 与宽表建模第六章必须合并考虑，这是本重构对外部设计的一条硬依赖。
12. **shredded 子列上的 cast 类型面**：[宽表建模](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「二十、后续功能」第 6 条设想 *JSON Shredding 支持 path type cast*（cast 成 geo / timestamptz 甚至 ref-mode LOB）。若成立，空间谓词等会作用在**一根 shredded 子列**上而非顶层字段——[§5.6 `SpatialReader`](#56-spatialreader空间关系谓词) 的寻址单位必须是 `(field, path)` 而不只是 `field`（[§5.7](#57-jsonindexreaderpath-寻址的谓词索引) 的 `Resolve(path, cast_type)` 已是这个形状，空间族需对齐）。
13. **nested 投影所需 `IArrayOffsets` 的注入点**（[§5.8](#58-nested元素级索引坐标与投影) 推荐"注入共享实例"）：注入发生在 `Loader::Open` 的 `LoadOptions` 里，还是加载后由 segcore 二次装配？前者让 loader 认识 columnar-format 类型，后者让 reader 有一个"未装配"的中间态。W1 落地时定。
