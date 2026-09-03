# 阶段 1：Index 接口拆分（标量各索引类型 + 标量/向量公共接口）

> **状态：设计提案，讨论中。** 返回 [总览](README.md)。
> 现状事实基于 master `e255009e01`。本文档定义 index 组件的目标接口：标量索引各类型的完整接口定义，
> 以及标量/向量公共接口（生命周期基类、Loader、工厂、storage 流程）的处理。向量索引按接口重新划分
> 也在阶段 1 范围（§11），但其 knowhere 交互不做重设计。

## 1. 范围

- **纳入**：常规标量索引（inverted / bitmap / sort / marisa / hybrid）、模式匹配（FMIndex、inverted 的 LIKE 系列）、TextMatch（全文）、Ngram、Geometry/RTree（空间谓词，见 §5.6）、JSON path 索引类型（json path cast index、`JsonFlatIndex`）、growing 增量标量索引；标量/向量公共接口的清理与向量索引按四种接口重新划分（§11）——共享生命周期基类、Loader/IO、工厂按索引类型拆分、`IndexBase` 阶段内退役。
- **排除**：knowhere 交互的重设计（vector 的搜索/构建逻辑原样搬移进新接口，不改行为）；`SkipIndex`——它是列的 zone-map 统计，归 columnar-format（[#51504](https://github.com/milvus-io/milvus/pull/51504) 已将其用作 `CellSkipPredicate` 接进列扫描规划，证实了这个所属组件）；JSON shredding（`JsonKeyStats`：typed 子列 + shared BSON 子列 + BSON 定位倒排）——它是 JSON 列的物理布局，整体归 columnar-format，见下方判断。

> **Geometry 属于标量索引**。`RTreeIndex : public ScalarIndex<T>` 本就在这棵树下，与其他标量索引类型的差别只是算子不同（空间关系谓词 vs 点/范围谓词），生命周期、构建、持久化、pin 与其他标量索引类型完全同构。按算子把它划成独立组件是划分错误——本文档的方法论是"每个索引类型一个最小接口"。

> **JSON shredding 不是索引，是列布局**。判断标准用总览 §6.3 那条：_是否按数据形态选算法_。shredding 按数据形态选的是存储布局（高频 path 抽成 typed 子列、低频落 shared BSON），查询算法完全没变——仍是 exec 把比较 lambda 逐 chunk 跑在原始值上。三条代码事实：
>
> 1. `JsonKeyStats::ExecutorForShreddingData`（`JsonKeyStats.h:266-330`）持有的 `shredding_columns_` 是 `ChunkedColumnInterface`，循环 `column->Span(op_ctx, i)` / `StringViews()` 把 exec 传入的 functor 跑在原始值上，末尾断言 `processed_size == num_rows`——永远全扫，与 `SegmentExpr::ProcessDataChunks` 同形。它自带一份 `SkipIndex skip_index_` 用于跳 chunk，而 zone-map 已划归 columnar-format：同一个概念在两个组件里各存一份。
> 2. `BsonInvertedIndex`（`json_stats/bson_inverted.h:41`）的 posting 是 `path → [(row_id, offset)]`，`offset` 是该值在这一行 BSON blob 里的字节偏移——它索引的是物理位置，不是值。且 `ExecuteForSharedData` 从不把 posting list 返出来，拿到立刻 `shared_column_->BulkRawBsonAt(...)` 回调解码：它不可能脱离那根列被独立消费，这正是列内部布局结构（等价于 offsets/dictionary）的定义，不是一个索引接口。连 `exists(path)` 都不能由 posting list 单独回答——`ExistsExpr.cpp:264-270` 拿到 (row, offset) 后仍要 `bson.IsBsonValueEmpty(offset)` 才敢置位。
> 3. 对照组：`JsonFlatIndex` / `JsonFlatIndexQueryExecutor`（`JsonFlatIndex.h:34,736`）的 tantivy 倒排建在值上，`TermBitset` / `json_exist_query` / range 直接出 bitmap，全程不碰列。这才是索引语义。
>
> 因此 `JsonKeyStats` 的 `NotImplemented` 泛滥（§2.2 第 1 行）不是"接口太宽"而是站错了组件：它今天只能靠继承 `ScalarIndex<std::string>` 来借用索引的工厂、加载与 pin 机制。把它迁进 columnar-format 的代价是 columnar-format 要引入一个它目前没有的概念——列的可选替代布局：异步构建、可以缺席、缺席时回退原始 JSON 列（与 interim index 同构）。这个概念必须显式建模。这项工作属于 [阶段 1 并行项 columnar-format 统一](README.md#7-阶段计划)，不属本阶段。 shared 侧确实复用了倒排，但复用的是引擎与生命周期流程，不是 `index` 组件。若让 columnar-format 持有一个索引对象、经 Reader 访问，就造出 L1 → L2 的上层依赖——columnar-format 在 L1、index 在 L2，[阶段 0](00-w0-foundation.md) 的分层规则直接禁止，不需要再论证是否构成双向依赖。实际被复用的只有两层东西，都在 `index` 之下：
>
> - **引擎**：`TantivyIndexWrapper` 位于 `thirdparty/tantivy/tantivy-wrapper.h`，属外部依赖库 `tantivy_binding`，不属 `src/index/`。`BsonInvertedIndex` 今天已经直接组合它、完全不碰 `IndexBase`/`ScalarIndex`——它正是 §3 原则 2 引用的那个组合正例。组合外部引擎在任何层都合法（正如 vector 索引类型组合 knowhere、text 索引类型组合 tantivy）。
> - **生命周期流程**：构建→序列化→上传、下载→加载→pin→计费。这套东西没有一处是索引专属的，处理见 [§11.2 第 1 条](#112-处理决定)——它移到 L1，两边共用。
>
> 现状依赖数据支持这个判断：`bson_inverted.h` 的全部依赖是 `storage::DiskFileManagerImpl`（L1）+ tantivy wrapper（外部）+ `index/IndexStats.h`；而 `IndexStats` 只依赖 `common/protobuf_utils.h`，内容是 `(file_name, file_size)` 列表加内存尺寸，零索引语义。迁移的全部代价就是把这个类降层。
>
> 判定索引的可操作标准不是「用了什么引擎」，而是能否被那根列以外的消费者当索引消费。`ExecuteForSharedData` 从不把 posting list 返出来，`exists` 也必须回读 blob——答案是不能。一根字典编码列内部可能用哈希表甚至 FST，但 dictionary column 不属于索引模块。 把 shredding 整体上提、做成一个同时持有列与索引的更高层组件，同样不成立，理由：
>
> 1. **shredded 列已经是 columnar-format 对象，不是"某种类似列的东西"。** `JsonKeyStats::LoadShreddingData`（`JsonKeyStats.cpp:1248-1283`）走的是 `ManifestGroupTranslator` → `ChunkedColumnGroup` → `ProxyChunkColumn`——与普通 storage-v2 列完全同一条构造链，连 `enable_mmap`/`warmup_policy`/`load_priority`/`size_estimate`/cachinglayer 计费都是同一套，唯一区别是一个 `GroupChunkType::JSON_KEY_STATS` 标签。上提等于按"来自哪个逻辑字段"把 storage-v2 column group 这一个概念劈成两个。这也解释了 index → segcore 那 5 处 include 的由来：index 组件要到 L3 取 translator 才能造出这些列。
> 2. **"同时持有列与索引并在两者间做执行路径选择"这个角色已经有人担任，是 exec。** 这件事对所有字段类型都一样（`DetermineExecPath`），不是 JSON 特有。为 JSON 单造一个持列又持索引的组件，等于造一个 mini-exec——而 [§5.4](#54-ngramreader二段执行的正确切分)（ngram）与 [§5.6](#56-spatialreader空间关系谓词)（spatial）的判断刚把"索引内部做二段执行"归回 exec，这是同一条原则的反面。
> 3. **上提后的所属组件其实是 segcore（L3），不是新层**——它的职责定义正是"列/索引/可见性三条读取路径"。但那会让 segcore 拥有一个 JSON 专属数据结构（与 segcore 重构"甩掉类型特例"的方向相反），并且 JSON 字段将无法经统一 `ColumnInterface` 提供，把 [#51504](https://github.com/milvus-io/milvus/pull/51504) 刚统一起来的数据访问路径重新劈开。
>
> 那么"哪个 path 有 typed 子列、cast 成什么、没有时回退哪根列"这些内部知识放哪儿？——列的能力描述（layout metadata），与列知道自己的 chunk 几何（`ColumnPlanner`）同类。exec 问"field F 的 path P 你能以什么形态给我"，拿到能力答案后自己选执行路径。即列接口上的 `ColumnCaps`（归阶段 1 并行项），不需要新组件。
>
> **本判断的失效条件**：今天 `BsonInvertedIndex` 的 term 是 path、posting 是 `(row_id, blob 内字节偏移)`，是定位器。若将来 shared 侧改为索引值（例如支持 `a.b > 3` 直接出 bitmap 而不回读 blob），那么该布局内部就真的嵌了一个索引，"跨两层"的前提成立，本条判断必须重开。 外部独立证实：[宽表建模设计文档](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)（2026-08 草稿）。 该文档「需求 Scope」第 3 条写明：_在单个 Sealed Segment 内部，`Struct` 和 `JSON` 应在 schema 推断后，以相同的底层结构存储，即拆列_。`Struct` 拆列是无争议的存储/schema 概念（物理列、FieldID 分配、etcd schema）；要求 JSON 与之同构，等于宣告 JSON shredding 是拆列的一个变体——差别只在 layout 从哪儿来。既然 `Struct` 拆列不需要一个「同时持有列与索引」的新组件，JSON 也不需要。
>
> 但该文档修正了本判断的一处粒度：JSON 一节写「拆列的结构无需保存到 etcd，就按现在只保存在 stats 里即可，每个 Segment 在 Segcore 里动态管理」。这不影响子列对象的所属组件，但把 layout 目录的所属组件固定为 segcore。三段划分：
>
> | 东西                                                 | 所属组件                          | 依据                                                                                                    |
> | -------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------- |
> | 子列对象（typed 子列、shared BSON 子列）                      | columnar-format (L1)      | 与 `Struct` 子列同构（宽表建模 Scope 3）；今天已经是 `ManifestGroupTranslator`→`ChunkedColumnGroup`→`ProxyChunkColumn` |
> | layout 目录（path → 哪根子列 / cast type / 是否只在 shared 里） | segcore (L3)              | JSON 的 layout 来自 per-segment stats、动态推断、每段不同；`Struct` 的 layout 来自 collection schema，走 schema 路径       |
> | 定位倒排（path → (row, byte offset)）                    | 随 shared 子列，归 columnar-format | 不能脱离该列被消费                                                                                             |
> | 执行路径选择（走 path index 还是扫子列）                         | exec (L4)                     | 与所有字段类型一致                                                                                             |
>
> 这比「列的能力描述」更准确：JSON 的 layout 描述不是列自己知道的，是 segcore 从 stats 加载后装配给列的；`Struct` 的 layout 则由 schema 直接给出。同一个数据访问接口、两个元数据来源——`ColumnCaps` 必须按这个形状设计。 阶段 1 内的过渡处理（宽表建模实现前）：上面的终局判断依赖两个尚未确定的外部件（嵌套数据表示、Struct/JSON 拆列统一），但阶段 1 不能等——`JsonKeyStats` 挂在 `ScalarIndex<std::string>` 下会挡住 `IndexBase` 退役，5 处 `index → segcore` 违反分层方向的依赖也是阶段 1 的验收标准。代价几乎为零：统计后，它与索引机制的耦合全仓只剩一行。
>
> | 事实                       | 证据                                                                                                                                                                                       |
> | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
> | 构建侧不经 `IndexFactory` | `indexbuilder/index_c.cpp:477` 直接 `make_unique<JsonKeyStats>`，只调 `Build(config)` + `Upload(config)`                                                                                      |
> | 加载侧不经类型擦除              | `ChunkedSegmentSealedImpl::BuildJsonKeyStatsIndex` 直接构造，存为 `shared_ptr<JsonKeyStats>`（`ChunkedSegmentSealedImpl.h:350`、`SegmentInterface.h:941`）——不是 `CacheIndexBasePtr`/`IndexBase` |
> | 查询侧不经虚函数               | `segment->GetJsonStats()` 拿具体类型，调 `ExecutorForShreddingData` / `ExecuteForSharedData`                                                                                                    |
> | 唯一耦合点                    | `JsonKeyStats.h:76` 的 `: public ScalarIndex<std::string>`——一个没有任何调用点使用的继承子句                                                                                                          |
>
> 因此过渡动作是两步，无行为变更、无格式变更、无 Go 侧改动：
>
> 1. **断继承**：`class JsonKeyStats : public ScalarIndex<std::string>` → `class JsonKeyStats`，删掉 `In`/`NotIn`/`Range`×2/`IsNull`/`IsNotNull`/`Reverse_Lookup`/`Build(n,…)`/`BuildWithDataset`/`BuildWithRawDataForUT`/`Load(BinarySet)` 这批 `NotImplemented` override。`Build(config)`/`Upload(config)`/`Load(TraceContext,Config)`/`Serialize` 保留为普通方法（真实调用点在）。零调用点改动。
> 2. **移目录** `index/json_stats/` → `segcore/json_stats/`。`index → segcore` 的 5 处违反分层方向的依赖当场降到 0（[阶段 0](00-w0-foundation.md) 表中该行清空），不是延后。`indexbuilder` 由此 include segcore——L5→L3 下层依赖，合法。
>
> **为什么放在 segcore 而不是一个更"干净"的独立组件**：它需要 `ManifestGroupTranslator`（segcore），同时又被 segcore 的 `runtime.json_stats` 持有——独立组件必然成环，除非先把 translator 迁出 segcore，而那是阶段 2 / 阶段 3 的量。segcore 内是当前唯一无环的位置，且不是浪费：宽表建模已决定 JSON layout「每个 Segment 在 Segcore 里动态管理」，layout 目录终局就在 segcore。将来变的只是"它持有的子列升格为 columnar-format 一等对象"——那是 segcore 内部的替换，不再是跨组件迁移。
>
> **明确不做**（全部等宽表建模确定）：不把 typed 子列升格为 columnar-format 一等对象；不设计"列的可选替代布局"概念与 `ColumnCaps`；不改 exec 的 JSON 表达式调用形态（只改 include 路径）；不接 [§11.2](#112-处理决定) 的 L1 产物构建与加载流程（它继续手写自己的 `Build`/`Serialize`/`Upload`/`Load`）；不改名（`JsonKeyStats` 这个名字跨到 proto 与 Go 侧，且宽表建模统一 Struct/JSON 时会自然重定）。
>
> **代价与退出条件**：过渡期内 `segcore/json_stats/` 是 segcore 里的一个 JSON 专属数据结构，与 segcore 重构"甩掉类型特例"的方向相反。这是有意接受的临时状态，退出条件明确写出：宽表建模「六、查询节点的数据表达」确定 + 子列升格为 columnar-format 对象。挂在 [README §8](README.md#8-阶段-3-segcore-的已定判断) 的 segcore 待修订项里，避免固化。

接口设计一次覆盖全部纳入的索引类型；迁移可以分批（先常规 scalar，再 ngram/text/json，最后 vector 重新划分），但接口不为"第一批"特化。

## 2. 现状与问题

### 2.1 结构事实

- `IndexBase`（`Index.h:37`）约 19 个 virtual，构建、持久化、查询、缓存计费混在基类里：`Serialize`/`Load`×2/`Build`×3/`Upload`/`LoadUnified`/`UploadUnified`/`HasRawData`/`IsMmapSupported`/`GetCastType`/`Exists`/`CellByteSize`/`SetCellSize`/`ComputeByteSize`。
- `ScalarIndex<T>`（`ScalarIndex.h:102`）再叠约 24 个 virtual：`In`/`NotIn`/`Range`×2/`IsNull`/`IsNotNull`/`InApplyFilter`/`InApplyCallback`/`Reverse_Lookup`/`SupportFastReverseLookup`/`PatternMatch`/`SupportPatternMatch`/`Query(Dataset)`/`Build(n, values, valid)`……
- 14 个实现类挂在这棵树下，且存在深实现继承：`TextMatchIndex : InvertedIndexTantivy<std::string> : ScalarIndex : IndexBase`；`NgramInvertedIndex`、`JsonFlatIndex` 同样以继承 `InvertedIndexTantivy` 的方式复用 tantivy 封装。
- 10+ 个头文件 include `FileManager`/`DiskFileManagerImpl`——持久化 IO 实现在索引类里。

### 2.2 问题证据（每条对应一个设计决定）

| 证据 | 位置 | 说明 | 对应设计决定 |
|---|---|---|---|
| `JsonKeyStats` 继承 `ScalarIndex<std::string>`，但 `In`/`NotIn`/`Range`/`IsNull`/`IsNotNull`/`Reverse_Lookup`/`Build` 全部 `ThrowInfo(NotImplemented)` | `json_stats/JsonKeyStats.h:145-201` | 继承接口只为借用工厂与加载机制，是 Liskov 违约。根因是站错组件：它是列布局不是索引（§1 判断） | 双重结论：非常规索引不硬套 point-predicate 接口、每个索引类型一个最小接口（§4）；shredding 整体迁出 index（§1） |
| `TextMatchIndex` 有四个构造函数对应四种生命周期：growing 内存 writer（commit interval + background merge）/ sealed 加载时从原始数据建 / 构建服务（`FileManagerContext`）/ 加载已建索引 | `TextMatchIndex.h:31-52` | 四种生命周期挤在一个类，靠构造参数区分 | 四种接口切分：Reader / Builder / Appender / Loader（§3） |
| `NgramInvertedIndex::ExecutePhase2` 接 `exec::SegmentExpr*` | `NgramInvertedIndex.h:68,93` | 候选验证需要回读原始值，于是索引反向依赖 exec | 索引只做 Phase1（候选），验证归 exec 走列扫描（§5.4） |
| 能力缺失用异常表示：`PatternMatch`/`InApplyFilter` 默认 `ThrowInfo(Unsupported)` | `ScalarIndex.h:140,187` | 调用方要么 try、要么记住每个 `Support*` 方法 | 能力描述符声明，禁止 throw 探测（§4.1） |
| `indexbuilder::ScalarIndexCreator` 只用 `CreateIndex`/`Build`/`Serialize`/`Upload`；exec 只用 `In`/`Range`/`PatternMatch` 系列；segcore load 只用 `Load`/cache 计费 | `indexbuilder/ScalarIndexCreator.cpp:188-243` | 三类调用方各用一种接口，却都拿到全部公开接口 | 按调用方切分接口（§3） |
| `HybridScalarIndex` 运行时包一层，按基数转发到 bitmap/inverted | `HybridScalarIndex.h` | 选型是构建期决策，被做成了运行时转发类 | 选型归 Builder 策略，加载返回具体 Reader（§6.3） |
| `FMIndex.h:30` include `segcore/SegcoreConfig.h` | `FMIndex.h:30,227` | 全局配置放 segcore，索引被迫反向依赖 | 配置作为构造参数注入（§8） |
| `RTreeIndex` 的真实查询接口是 `QueryCandidates(GISOp, Geometry, vector<int64_t>&)`，却被迫实现 `In`/`NotIn`/`Range`×2/`InApplyFilter`/`InApplyCallback`，且 `Reverse_Lookup` throw `NotImplemented` | `RTreeIndex.h:111-189` | 空间索引硬套点谓词接口（与 `JsonKeyStats` 是同样的问题，只是程度轻些） | 独立的空间索引类型最小接口（§5.6） |
| **growing 侧的同类问题**：`FieldIndexing`（`FieldIndexing.h:51`）基类是两类接口的并集，5 个纯虚里 3 个 vector 专属、2 个 scalar 专属，两个子类各自 throw 掉对方那一半——`ScalarFieldIndexing` 的 `AppendSegmentIndexDense`/`Sparse`/`GetDataFromIndex` throw（`:152,161,183`），`VectorFieldIndexing` 的两个 `AppendSegmentIndex` 重载 throw（`:294,303`） | `segcore/FieldIndexing.h:51-131,152,161,183,294,303` | 与 `IndexBase` 一模一样的 Liskov 违约，只是发生在 growing 侧。它同时证明：Appender 接口两类索引共有（都要 append），但 append 的签名不该共享 | 四种接口统一、接口内按索引类型划分（§3、§7、§11） |
| `FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 返回 `PinWrapper<index::IndexBase*>` | `FieldIndexing.h:128,131` | growing 侧同样以 `IndexBase` 作类型擦除句柄——`IndexBase` 退役不只是 sealed 侧的事 | growing 句柄同阶段改为 `IndexReaderBase`（§7、§11.2 第 3 条） |
| **正例**：`RTreeIndex::QueryCandidates` 只出候选，精确验证在 exec（`PhyGISCoarseConjunctExpr` / `PhyGISRefineConjunctExpr`） | `RTreeIndex.h:184`、`GISFunctionFilterExpr.cpp:480-560` | "索引出候选、exec 验证"的现成正确实践 | ngram 应向它看齐（§5.4/§5.6 候选类型） |

## 3. 设计原则

1. **按调用方切分接口**。四种接口、四类持有者：

   | 接口 | 持有者 | 形态 | 生命周期 |
   |---|---|---|---|
   | **Reader** | exec（查询期） | 索引对象上的接口 | 不可变，长期存活 |
   | **Appender** | segcore growing insert 路径 | 索引对象上的接口 | 长期存活，与读并发 |
   | **Builder** | indexbuilder 服务、segcore 加载时构建 | 独立对象 | 一次性，`Seal()` 后结束 |
   | **Loader** | segcore load | 独立对象（每个索引类型一个） | 无状态 |

   任何一类调用方拿不到其他接口的方法。Builder 与 Appender 是两个接口而非一个：前者一次性、独占、以产出 Artifact 结束；后者长期存活、与读并发、只产出按已提交行数截取的快照而不直接持久化（growing 的持久化走 flush 路径）。把两者合一正是 `TextMatchIndex` 四个构造函数挤在一个类的原因（§2.2）。
2. **组合替代继承**。tantivy 封装、marisa、FM 结构是被组合的引擎，不是基类。实现类之间禁止继承。

   > **仓库里已有正例**：`BsonInvertedIndex`（`json_stats/bson_inverted.h:42`）是个裸类，不继承 `IndexBase`/`ScalarIndex`/`InvertedIndexTantivy`，直接持有 `shared_ptr<TantivyIndexWrapper>` 并调 `term_query_i64`。同一个 tantivy 引擎，`TextMatchIndex`/`Ngram`/`JsonFlat` 靠继承复用、它靠组合——后者才是本阶段要推广的形态。代价是它把生命周期接口（`AddRecord`/`BuildIndex`/`LoadIndex`/`UploadIndex`/`CellByteSize`）手写了一遍；接口拆分后这部分由共享的 Builder/Loader 承接，组合的好处保留、重复消失。
3. **能力描述，不许 throw**。每个 reader 携带能力描述符；不支持的操作在类型上就不存在，或经 `std::optional` 表示。
4. **模板留在热路径，类型擦除只在管理接口**。exec 的表达式本就按值类型模板化，typed reader 无装箱开销；索引清单持类型擦除基类，`IndexReaderBase` 到查询接口的 downcast 每个表达式节点一次（随 pin 获取），不在 batch 或行的粒度上发生（§4.3）。
5. **不认识 segment 与 executor**。构建输入是纯数组；查询输出是 bitmap / 值，坐标一律落在索引自己的坐标系里——行级索引给行号，元素级（nested）索引给元素号。元素→行的投影不属于索引：元素级到行级的聚合时机由查询语义决定，只有 plan 知道，见 [§5.8](#58-nested元素级索引坐标与投影)。
6. **IO 注入**。索引类不持有 `FileManagerContext`；文件读写经注入的 sink/source 接口，只在 Loader 与 Artifact 序列化的实现内出现。

## 4. 接口总览

```text
实现类（如 InvertedIndex<T>）
  ├── IndexReaderBase          身份与能力描述，类型擦除基类；索引清单持它
  └── 若干查询接口（纯 mixin，彼此独立，不继承基类）：
        ScalarPredicateReader<T>   点/范围/空值谓词
        PatternMatchReader         LIKE 系列（prefix/postfix/inner/match）
        TextMatchReader            分词匹配（match/phrase/fuzzy）
        NgramReader          ┐候选类型：结果为超集，
        SpatialReader        ┘exec 侧精确验证
        ScalarValueReader<T>       反查接口
        JsonIndexReader            path 路由：按 path 取到上面各类型的谓词接口

IndexBuilder<T>（两类索引共用一个接口，每类型一个实现）  → Seal() → IndexArtifact
GrowingScalarIndex<T> / GrowingTextIndex                → Append + 快照 Reader + 已提交行数
IndexLoader（每个索引类型一个，只读方向）                → 文件 → Reader，IO 注入
```

一个实现类可以同时提供多个查询接口（inverted 同时是 `ScalarPredicateReader<T>` 和 `PatternMatchReader`），通过接口多继承声明，而非通过实现继承获得。

> **查询接口不继承 `IndexReaderBase`**。若每个查询接口都 `virtual public IndexReaderBase`，多继承时为保证基类子对象唯一必须用虚继承，代价是实现类访问基类成员多一层间接、且 `IndexReaderBase` 到查询接口只能走虚基类的 `dynamic_cast`。改为纯 mixin 后：实现类非虚继承 `IndexReaderBase` + 各查询接口，`IndexReaderBase` 到查询接口是一次跨继承树的 `dynamic_cast`，查询接口本身也不需要基类的元数据（那些由索引清单持有，见 §4.3）。

### 4.1 能力描述符

```cpp
// index/contracts/ReaderCaps.h
namespace milvus::index {

struct ReaderCaps {
    bool predicate          = false;  // In/NotIn/Range（空值谓词见 NullReader，各索引类型都提供、不设位）
    bool pattern_match      = false;  // LIKE 系列
    bool text_match         = false;
    bool ngram_candidates   = false;  // 候选类型：结果为超集，需二次验证
    bool spatial            = false;  // 候选类型：空间关系谓词（MBR 粗筛）
    bool nested             = false;  // 元素级索引：命中是元素坐标，聚合到行由 exec 按 plan 决定（§5.8）
    bool value_lookup       = false;  // 可反查原值
    bool cheap_value_lookup = false;  // 逐行反查代价 O(1)/O(log n)
    bool json_paths         = false;  // path 寻址的复合索引
    bool exact              = true;   // false ⇒ 命中集是超集（ngram）
};

}  // namespace milvus::index
```

> **`ReaderCaps` 之外还有第二层判定：逐调用护栏。** `ReaderCaps` 是索引的静态属性（能不能做 LIKE），但还存在一类每次调用才能判定的属性：这个具体字面量值不值得走索引。现状里它是 `ScalarIndex<T>::ShouldUseOp(op, pattern)`（`ScalarIndex.h:228`），由 `exec/expression/Expr.h:2716` 消费，`FMIndex` 覆写它、用 O(|pattern|) 的出现次数统计拒掉退化字面量（`FMIndex.h:200`）。[§5.4](#54-ngramreader二段执行的正确切分) 的 `NgramReader::CanHandle` 是同一形状，通则如下。
>
> 两层分工明确写出：`ReaderCaps` 决定要不要 pin（静态、纯数据、pin 前可读），pin 到的查询接口回答这次调用用不用得上（动态、需要索引对象、只在已 pin 的前提下调）。因此 `PatternMatchReader` 需要一个与 `CanHandle` 同形的方法；不能把它塞进 `ReaderCaps` 位——`ReaderCaps` 是 pin 前读的，而这层判定必须看到字面量。

exec 的执行路径决策（`DetermineExecPath`）只消费这个结构，不 `dynamic_cast`、不 try-catch。segcore 的 `FieldIndexCapability`（segment 级"某 field 有什么索引"）由索引清单聚合各索引的 `ReaderCaps` 得到——单索引能力描述归 index，segment 级聚合归 segcore。

> **聚合是条目列表，不是按位 OR。** 同一 field 上可以同时存在 ngram（`exact = false`）与 inverted（`exact = true`），把两者的 `ReaderCaps` 按位或起来会得到一个不对应任何真实索引的描述符——exec 据此选执行路径就会拿着"精确"的假设去用候选类型的结果。正确形状是每个索引一条条目、各带自己的 `ReaderCaps`，选执行路径时先选条目再读 `ReaderCaps`。

> **执行路径选择规则：有可用索引就走索引，没有才回退列扫描。** shredding 迁出 index 后，"同一 path 既有 path 索引又有 shredded typed 子列"不再是两个索引之间的选择，而是索引 vs 列扫描的普通判定。typed 子列扫得快不快只影响 columnar-format 内部怎么扫，不上升为 exec 的执行路径选择输入——因此列侧 `ColumnCaps` 与 `FieldIndexCapability` 的合流不是阶段 1 的前置。
>
> 已知的次优情形：分级存储下拉起一个未加载的索引 cell 可能贵过扫一根已在内存的 typed 子列。这属于 cost model 层面的后续优化，不改执行路径选择的接口。

> **`ReaderCaps` 必须能在不 pin 的前提下读到**，因此它由加载期元数据（索引类型 + 构建参数，如"VARCHAR 上的 inverted"⇒ predicate + pattern_match + value_lookup）算出，作为纯数据存进索引清单条目，不是必须持有索引对象才能调用的虚函数。理由见 §4.3：路径决策发生在 pin 之前，若读 `ReaderCaps` 需要对象，分级存储里未加载的索引会被无谓拉起。
>
> reader 上仍保留 `Caps()` 用于能力描述（growing 快照、单测需要），但它是一致性校验对象而非查询期来源：pin 后的 `reader->Caps()` 必须等于索引清单缓存的 `ReaderCaps`，这条写成断言与测试。

### 4.2 类型擦除基类

```cpp
// index/contracts/IndexReader.h
namespace milvus::index {

// 索引结果所处的坐标系。行级索引给行号，元素级（nested）索引给元素号。
enum class Domain { Row, Element };

class IndexReaderBase {
 public:
    virtual ~IndexReaderBase() = default;

    // 能力描述接口。查询期的路径决策不读这里（那条路走索引清单缓存的 ReaderCaps，
    // 见 §4.1/§4.3）；本方法用于 growing 快照、单测与一致性断言。
    virtual ReaderCaps  Caps() const = 0;

    // 坐标系：Row（行级索引）或 Element（nested 索引）。Count() 是该坐标系的基数。
    virtual Domain      CoordDomain() const = 0;
    virtual int64_t     Count() const = 0;
    virtual DataType    ValueType() const = 0;
    virtual int64_t     MemoryUsage() const = 0;   // 纯能力描述；缓存计费在 load 侧 translator
};

}  // namespace milvus::index
```

对比现状：`CellByteSize`/`SetCellSize`/`ComputeByteSize` 这组 cachinglayer 计费从索引基类移除——计费属于 segcore load 侧的 translator，reader 只报告自己的内存占用。`Upload` 移除——上传属于 indexbuilder 服务。

### 4.3 对象模型：谁创建、谁持有、何时 pin

> Reader 接口按查询语义拆成若干查询接口（`ScalarPredicateReader<T>`、`PatternMatchReader` 等纯 mixin），下文 `Pinned<ReaderT>` 的 `ReaderT` 即其中之一。

查询接口不是每次查询新建的 proxy——它就是索引对象本身的一个接口视图。三类东西的创建时机完全不同：

| 对象 | 创建时机 | 生命周期 | 成本 |
|---|---|---|---|
| 实现类实例（`InvertedIndex<int64_t>` 等） | `Loader::Open()`，即索引加载时 | 长期存活；仅在被 cachinglayer 淘汰后重新加载时重建 | 一次加载 |
| `ReaderCaps` | 索引清单构建时由加载期元数据算出 | 与索引清单条目同寿 | 纯数据拷贝 |
| `Pinned<ReaderT>` 句柄 | 取用时 | 栈上 RAII：cache pin + 类型化裸指针 | 两个指针 |

索引清单持有的是 `CacheSlot`，不是索引对象。现状即 `CacheIndexBasePtr = shared_ptr<CacheSlot<IndexBase>>`（`Index.h:165`），阶段 1 后为 `CacheSlot<IndexReaderBase>`。索引对象活在 slot 内、可被淘汰，因此取查询接口必须先 pin 后 cast——pin 之前对象可能根本不存在。

查询期流程：

```text
1. caps = provider.Capability(field_id)              // 纯数据读：不 pin、不 cast
2. exec 按 caps 决定执行路径
3. pinned = provider.PinScalarPredicate<T>(op_ctx, field_id)     ← 每个表达式节点一次
       内部：slot->PinCells() → IndexReaderBase*
             跨继承树 cast → ScalarPredicateReader<T>*
             返回 Pinned{ptr, keep_alive}
4. 每个 batch：pinned->In(...)                        // 直接虚调用，无 cast、无分配
```

第 3 步的频率是每个表达式节点一次，不是每 batch、更不是每行——cast 与 pin 的开销被整个表达式求值分摊。这正是现状 `EnsurePinnedIndex()`（`Expr.h:395`，幂等、路径确定后才调）的形态，阶段 1 只是把它从 exec 迁进 `IndexProvider`。

> **typed pin 句柄比现状更省，不是更贵。** 索引查询整段只跑一次：`ProcessIndexChunksImpl` 用 `cached_index_chunk_id_` 加 `ExprCacheHelper::GetOrCompute`（`Expr.h:2077,2116`）算出全段一张 bitmap，之后每 batch 只做切片（`Expr.h:3072` 的注释即 "Populated once per segment, then sliced per batch"）。
>
> 今天真正每 batch 重做 `dynamic_cast` 的是 by-offsets 路径：非 source 表达式（`has_offset_input_`，上游已给候选 offsets）走 `ProcessIndexChunksByOffsets`（`Expr.h:686`）或 `ProcessIndexLookupByOffsetsImpl`（`Expr.h:754`），每批 cast 一次（`Expr.h:698,765`）。pin 出口给 typed reader 后这处开销一并消失。

**第 1 步不得 pin 是硬约束。** 现状代码注释已写明理由："短路路径（TextIndex/PkIndex/JsonStats）与 RawData 路径永不调用它，标量索引 cell 在分级存储里保持冷态"。若把 `ReaderCaps` 做成必须持有对象才能调的虚函数，路径决策就会把未加载的索引全部拉起——这是线上多余冷加载的直接来源。因此"`Capability()` 不触发 pin"是必测项：用计数型 fake `CacheSlot` 断言 pin 次数为 0——这是一个今天完全没被测到、而线上会造成多余冷加载的行为。

**growing 侧的不对称**：appender 不是"每次写入去取"——`GrowingIndexSet` 长期持有 appender（每个建索引字段一个），insert 路径直接调 `Append`。读侧 `ReaderSnapshot()` 返回的是另一个对象：commit/reload 时产生的不可变快照，所有并发查询经 `shared_ptr` 共享同一份。每次 commit 创建一次，不是每次查询。

两侧因此都不存在"每查询一个 proxy"：sealed 是 pin 一个长期对象，growing 是共享一个 commit 期快照。

## 5. 查询接口

所有 reader 不可变：由 `Seal()` 或 `Loader::Open()` 产生后线程安全、可无锁并发读。bitmap 语义统一：1 = 命中；bitmap 尺寸 = `Count()`。

**`Count()` 是 reader 自身坐标系的基数，不一定是行数。** 行级索引的坐标系是行，元素级（nested）索引的坐标系是元素，由 reader 经 [§4.2](#42-类型擦除基类) 的 `CoordDomain()` 描述；消费者据此决定拿到的 bitmap 按什么单位解读。索引不做跨坐标系的聚合，理由见 [§5.8](#58-nested元素级索引坐标与投影)。

> **输出形态只有一种：`TargetBitmap`。** 谓词类型、候选类型、nested 的元素级结果一律如此，不提供稀疏 offsets 或回调变体。
>
> 理由：选择率是查询的运行时属性，不是 reader 的静态属性——同一个 `In` 查罕见 term 命中 10 行、查常见 term 命中 90%；常见三元组的 ngram 候选、覆盖全域的空间查询同样接近全表。既然形态无法按索引类型静态决定，就选退化时代价不失控的那个：bitmap 大小恒为 `Count()/8`，与选择率无关；极稀疏时的代价是 `find_first`/`find_next` 的字跳扫描（1 亿行约 150 万次字读，亚毫秒级）。反过来稀疏 offsets 在稠密结果上是 8 字节/行 vs 1 比特/行的 64 倍膨胀——9000 万命中就是 720MB，不可接受。
>
> 因此 `SpatialReader::Candidates` 现状的 `std::vector<int64_t>` 属于 RTree 的历史写法，规整为 bitmap（消费者迭代置位即可，refine 逻辑不变）。

> **算子枚举一律 native，这是通则不是空间索引类型的特例。** [§5.6](#56-spatialreader空间关系谓词) 只对 GIS 算子写了这条，但 `milvus::OpType` 本身就是 `proto::plan::OpType`（`common/Types.h:106`），所以 §5.1 / §5.2 / §5.4 里凡出现 `OpType` 的签名都在违反[总览 §5 规则 2](README.md#5-全局硬规则)（pb 只在 adapter）。接口层需要三个 native 枚举：比较算子、模式匹配算子、空间算子；proto→native 的映射统一发生在 plan/exec 侧。
>
> **模式匹配算子集包含 `RegexMatch`**（见 [§5.2](#52-patternmatchreader)）。`RegexMatch` 今天走进 `NgramInvertedIndex::ExecutePhase1`（`NgramInvertedIndex.cpp:818,937,1084,1143`），`StringIndexMarisa.cpp:650-654` 的接受集里也显式列了它；漏掉它会删掉一条生产中在用的代码路径。
>
> **[§5.5](#55-scalarvaluereadert反查接口) 用到的 `owned_t<T>` 仓库里不存在**，需在接口层定义：`owned_t<std::string_view> = std::string`，其余 `= T`。

**跨索引类型的公共接口：`NullReader`。** 空值谓词与值类型无关，且七种标量索引全部真实现、零 throw（`BitmapIndex.cpp:816`、`FMIndex.cpp:392`、`RTreeIndex.cpp:469`、`InvertedIndexTantivy.cpp:349`、`ScalarIndexSort.cpp:508`、`StringIndexSort.cpp:462`、`StringIndexMarisa.cpp:406`），因此它单独成为一个接口：

```cpp
// index/contracts/NullReader.h
class NullReader {
 public:
    virtual TargetBitmap IsNull() const = 0;
    virtual TargetBitmap IsNotNull() const = 0;
};
```

> **为什么必须从 `ScalarPredicateReader<T>` 拆出来。** `RTreeIndex` 的 `In`/`NotIn`/`Range` 是未实现桩（全是 `ThrowInfo(NotImplemented)`，位置见 [§5.6](#56-spatialreader空间关系谓词)），删除后它不能再实现 `ScalarPredicateReader<T>`——否则 `T = std::string` 会把 WKB 上的点谓词重新拖回来；但 `geo_field IS NULL` 是生产中在用的代码路径：`PhyNullExpr` 经 `ProcessChunksForValid` → `ProcessIndexChunksForValid`（`Expr.h:2457`）确实路由到索引，而 `RTreeIndex::IsNull`/`IsNotNull`（`RTreeIndex.cpp:469,491`）是真实现。
>
> 信号在现状里早已存在：`IsNull`/`IsNotNull` 是 `ScalarIndex<T>`（`ScalarIndex.h:132,135`）里唯二不带 `T` 的方法。不带 `T` 的方法位于按 `T` 模板化的接口上，是接口拆分错误。
>
> `NullReader` 不设 `ReaderCaps` 位——标量各索引类型全员提供，是无条件可用的接口。同理它也不在 [§8 映射表](#8-现有实现类--新接口映射)里逐行重复。

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
};
```

与现状的差异：`Query(DatasetPtr)` 这个 knowhere 风格的万能入口删除；`Build`/`Size`/`GetIndexType` 不在查询接口上；`InApplyFilter` / `InApplyCallback` 一并删除（理由见下）；`IsNull`/`IsNotNull` 移出到跨索引类型的 `NullReader`（见 [§5 开头](#5-查询接口)）。

> **字符串索引类型的 `T` 取 `std::string_view`。** 本接口与 [§5.2](#52-patternmatchreader) 的入参全部只读、指向调用方内存，view 化无生命周期问题；tantivy FFI 本来就是 ptr+len，marisa 的 `predictive_search`、FMIndex 的 pattern 同理。
>
> **这条只适用于输入侧。** 反查接口 [`ScalarValueReader<T>::Lookup`](#55-scalarvaluereadert反查接口) 不能跟着 view 化——理由（marisa 的原值在反查时于运行时重建）见该节。

> **`InApplyFilter` / `InApplyCallback` 为什么不进接口。** `InApplyFilter` 生产代码零调用点（唯一引用是 `JsonFlatIndexTest.cpp:799`），`RTreeIndex` 的实现还是 throw 的未实现桩；`InApplyCallback` 只有一个消费者——`PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex`（`UnaryExpr.cpp:804,807`），用于 ARRAY 整体相等走元素级索引时逐元素求交 + 1% 提前退出。
>
> 它以"避免物化完整 bitmap"为名，但两个实现都是 `TargetBitmap bitset(Count()); terms_query(...); apply_hits_with_callback(...)`——照样物化完整 bitmap 再遍历（`InvertedIndexTantivy.cpp:428` 处还留着 `todo: could push-down the callback to tantivy query`）。真实收益接近零，而 exec 侧那个提前退出用普通 `In()` + bitmap 求交同样能做，且比现在的 `unordered_set` 求交更快。
>
> 另一个理由是输出形态只保留一种（见 §5 开头）：稀疏输出曾是这个接口最后的立足点，而稀疏与否是查询的运行时属性，不是接口层的关切。若将来 tantivy 真做了流式命中，那是实现内部省掉中间分配，不需要在接口上预留形状。

### 5.2 `PatternMatchReader`

```cpp
class PatternMatchReader {
 public:
    // pattern 是 SQL LIKE 原文（非 regex），op ∈ {Match, PrefixMatch, PostfixMatch, InnerMatch, RegexMatch}
    virtual TargetBitmap PatternMatch(std::string_view pattern, OpType op) const = 0;
};
```

提供者：tantivy inverted、marisa（prefix）、FMIndex（它只有这一个谓词接口——加上各索引类型共有的 `NullReader`（`FMIndex.cpp:392` 是真实现）就是它的全部；现在它作为 `ScalarIndex<std::string>` 背着 20 个不相关方法）。

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

实现是对 tantivy reader 快照的组合封装。不继承 inverted——现在 `TextMatchIndex : InvertedIndexTantivy<std::string>` 顺带背上了 `In`/`Range` 等它永远不该被调用的方法。

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

**Phase2（验证）从索引中删除。** 现状 `ExecutePhase2(literal, op, exec::SegmentExpr*, ...)` 的本质是"对候选行取原值重新求值"——取值走 columnar-format 的 `Take`/`Scan`，求值本来就是 exec 的表达式内核。切分后：`index → exec` 违反分层方向的依赖消失，且不需要引入回调（早期方案里为打断这条依赖而设想的 `ValueFetcher` 回调也不再需要——切分本身就消除了它）。

> `RTreeIndex::QueryCandidates` 出候选、exec 的 `PhyGISRefineConjunctExpr` 做精确验证——同一模式在空间索引类型上已经正确实现多时（§5.6）。ngram 是这个模式的未完成实现：它把验证留在了索引里，于是拖出一条 `index → exec` 违反分层方向的依赖。

### 5.5 `ScalarValueReader<T>`——反查接口

```cpp
template <typename T>
class ScalarValueReader {
 public:
    // 返回值持有所有权：owned_t<std::string_view> = std::string，其余 owned_t<T> = T。
    virtual std::optional<owned_t<T>> Lookup(int64_t offset) const = 0;
    // 批量反查；实现可按内部布局聚簇。输出对接 columnar-format 的 TakeResult 约定。
    // 回调期间 const T* 有效即可，作用域由实现方掌握，因此这里可以是 view。
    virtual void Gather(const int64_t* offsets, int64_t count,
                        const std::function<void(int64_t i, const T*, bool valid)>& out) const = 0;
};
```

对应现状 `Reverse_Lookup` + `SupportFastReverseLookup` + `HasRawData`。"反查太贵就回原始列"的决策不在这里——`ReaderCaps.cheap_value_lookup` 描述代价，选择权在消费者（reduce 的 Materializer / exec）。

> **`Lookup` 不能返回 view，`Gather` 可以——这是反查接口与谓词接口 `T` 不同步 view 化的原因**（[§5.1](#51-scalarpredicatereadert) 的 `string_view` 决定只覆盖输入侧）。
>
> `StringIndexMarisa::Reverse_Lookup`（`StringIndexMarisa.cpp:800-811`）的返回语句是 `std::string(agent.key().ptr(), agent.key().length())`：字节活在函数内的 `marisa::Agent` 局部里。trie 是压缩存储，原值在反查时于运行时重建，索引内部没有一段可指向的常驻缓冲——返回 view 必悬垂。任何 trie / 压缩类型都是这个形态，与 marisa 的实现无关。
>
> `Gather` 不受此限：实现可以让 agent 活到回调返回之后，view 在回调作用域内有效。这是本接口用回调而非返回容器的第二个理由（第一个是按内部布局聚簇）。

### 5.6 `SpatialReader`——空间关系谓词

Geometry 与其他标量索引类型的差别只在算子：谓词是空间关系（intersects / contains / within / …）而非点或范围比较。生命周期、构建、持久化、pin 完全同构，因此它是标量索引的一种类型，不是独立组件。

```cpp
// index/contracts/SpatialReader.h
class SpatialReader {
 public:
    // 候选生成：MBR 粗筛，结果是超集（caps.exact = false、caps.spatial = true）。
    // 精确的空间关系判定由 exec 对候选行取原值完成。
    virtual TargetBitmap Candidates(SpatialOp op, const Geometry& query_geom) const = 0;
};
```

- `SpatialOp` 是接口层的 native 枚举，不是 `proto::plan::GISFunctionFilterExpr_GISOp`——现状 `QueryCandidates` 直接吃 proto 枚举（`RTreeIndex.h:184`），违反"pb 只在 adapter"（[总览 §5 规则 2](README.md#5-全局硬规则)），plan→native 的映射发生在 plan/exec 侧。
- `RTreeIndex` 被迫实现的 `In`/`NotIn`/`Range`×2/`InApplyFilter`/`InApplyCallback` 与 `Reverse_Lookup` 全部移除。这批是 `ThrowInfo(NotImplemented)` 未实现桩，零生产调用方，删除安全——位置是 `RTreeIndex.cpp:412,420,427,464,474,481,489,500`。
- **`Query(DatasetPtr)` 不是未实现桩，它是今天 RTree 唯一的生产入口。** `RTreeIndex.cpp:521-543` 有完整实现（从 dataset 取出 op 与 geometry → `QueryCandidates` → 拼 bitmap），调用点是 `exec/expression/GISFunctionFilterExpr.cpp:466` 的 `idx_ptr->Query(ds)`——`QueryCandidates` 正是经由它才被走到。所以要删的是 `DatasetPtr` 这层包装（万能入口、参数靠字符串 key 取），行为整体搬进 `Candidates(SpatialOp, const Geometry&)`。按"未实现桩、删除安全"的字面理解去删会删掉在用的代码。
- **`IsNull`/`IsNotNull` 不在删除之列**：它们是真实现（`RTreeIndex.cpp:469,491`，由 `null_offset_` 生成），且 `geo_field IS NULL` 经 `PhyNullExpr` 走索引。因此 RTree 的接口是 `SpatialReader` + `NullReader`，后者是各索引类型共有的接口（见 [§5 开头](#5-查询接口)）——空值谓词不属于"geo 算子"，把它一起删掉会删掉在用的代码。
- **现状实现并不区分算子**：`RTreeIndexWrapper::query_candidates`（`RTreeIndexWrapper.cpp:253-281`）拿到 `op` 之后只用于 `LOG_DEBUG`，实际永远跑 `intersects(query_box)`。`op` 保留在接口上是因为 exec 必须能表示要哪种空间关系（精确验证在 exec 侧按 op 分支），索引层今天不按 op 走不同粗筛。`DWithin` 同理不带 distance 参数——距离在 exec 侧先转成 bbox（`GISFunctionFilterExpr.cpp:448-455`，注释原话 "Distance is not used for bounding box intersection query"）；`STIsValid` 明确不可用索引（`:201-202`），不进 `SpatialOp`。
- **候选类型的共性**：`SpatialReader`、`NgramReader` 与 nested 索引上的 ARRAY 相等（§5.8）是同一模式的三个实例——索引给超集、exec 做精确验证。它们共享 `ReaderCaps.exact = false` 的语义约定与统一的 bitmap 输出，消费者的处理流程相同（取候选 → 取原值 → 重新求值）。差别只在候选所处的坐标系：前两者是行，nested 是元素，需先经 exec 的聚合算子聚合到行。

### 5.7 `JsonIndexReader`——path 寻址的谓词索引

范围已缩小：本类型只覆盖建在值上的 JSON 索引——逐 path 的 cast index，以及 `JsonFlatIndex`（一个 tantivy 索引覆盖该 field 的全部 path）。shredding 已整体划归 columnar-format（[§1 判断](#1-范围)），本类型不再承担 shredded 列路由。

JSON 索引与常规标量索引的唯一结构差别是多一层 path 寻址：同一个 field 上，`a.b` 是 int64 谓词接口、`a.c` 是 string 谓词接口。所以本类型不定义新的查询语义，只定义路由：

```cpp
// index/contracts/JsonIndexReader.h
class JsonIndexReader {
 public:
    virtual ~JsonIndexReader() = default;

    // 该 path 上是否有可用的谓词接口；有则返回类型擦除基类，
    // 消费者按 cast_type 做一次跨继承树的 dynamic_cast 得到 ScalarPredicateReader<T> / PatternMatchReader。
    // 返回 null = 该 path 无索引，exec 回退列扫描（可能落在 shredded 子列上，也可能落在
    // 原始 JSON 列上——那是 columnar-format 的事，本类型不知道也不需要知道）。
    virtual std::shared_ptr<const IndexReaderBase>
    Resolve(std::string_view path, DataType cast_type) const = 0;

    // path 存在性。建在值上的倒排能独立回答（json_exist_query），无需回读列。
    virtual TargetBitmap Exists(std::string_view path,
                                JsonValueType type = JsonValueType::Any) const = 0;

    virtual std::vector<DataType> CastTypesOf(std::string_view path) const = 0;
};
```

- `JsonFlatIndex` 实现 `JsonIndexReader`：`Resolve` 返回一个绑定了 path 的 `JsonFlatIndexQueryExecutor<T>`——今天它已经是这个形态（`JsonFlatIndex.h:34`），只是靠继承 `InvertedIndexTantivy<T>` 拿到查询接口；改为组合后它直接实现 `ScalarPredicateReader<T>`。
- 逐 path 的 cast index 不需要专门接口：它就是普通的 `ScalarPredicateReader<T>`，在索引清单里以 `(field, path)` 为键注册；`JsonIndexReader` 对它退化为一层查表。
- `ReaderCaps::json_paths` 的含义随之收紧为「该索引对象按 path 寻址」，不再暗示 shredded 列的存在。「这个 path 有 typed 子列」是列的能力描述，由 columnar-format 提供。
- `IndexBase::GetCastType`/`Exists` 从基类移除，收进本类型。
- `JsonKeyStats` 不出现在本节：它迁出 index，那批 `NotImplemented` 随迁移一并消失。

### 5.8 Nested（元素级）索引：坐标与投影

**nested 不是独立的索引类型，是现有实现类上的一个模式位**：`BitmapIndex`（`BitmapIndex.h:88`）、`StringIndexSort`（`StringIndexSort.cpp:229`）都带 `is_nested_index_`，并持久化在产物里（`BinarySet` 的 `"is_nested_index"` 项、新 writer 的 `is_nested` meta，加载时做 `is_nested_index_ || loaded` 的兼容合并）。它表示：索引的对象是数组元素，而非行。

#### 投影不属于索引：聚合点由查询语义决定

nested 索引在元素坐标系里工作——命中的是数组元素，返回的 bitmap 尺寸是元素总数（`CoordDomain() == Domain::Element`）。元素级到行级的存在量词聚合由 exec 完成，且聚合发生在哪一步由 plan 决定，因为聚合得太早、太晚各有一类查询会算错：

- **聚合太早会错**（必须先在元素级组合）：struct array 上的相关元素谓词。`struct[*].a == 1 AND struct[*].b == 2` 的语义是 `∃i:(a[i]=1 ∧ b[i]=2)`。若两侧各自先聚合到行，得到的是 `∃i:a[i]=1 ∧ ∃j:b[j]=2`——两个元素可以不同，行会被错误命中。
- **聚合太晚会错**（必须先聚合到行）：同一数组上的不相关谓词。`contains(1) AND contains(2)` 的语义就是 `∃i:x[i]=1 ∧ ∃j:x[j]=2`，元素级相与等于要求"同一个元素既等于 1 又等于 2"，行会被错误漏掉。`NOT contains(1)` 同理：正确语义是 `¬∃i:x[i]=1`，元素级取反得到的是"存在不等于 1 的元素"，两者不等价。

同一个 nested 索引必须同时服务这两类查询，而区分它们的信息只在 plan 里。因此 reader 不能替 plan 决定聚合点，只能交付元素级结果。

现状代码：

```cpp
// exec/expression/JsonContainsExpr.cpp:2469
auto element_bitset = index_ptr->In(n, data);        // 索引 → 元素级
if (!index_ptr->IsNestedIndex()) return element_bitset;
return array_offsets->ForEachRowElementRange(...);   // exec 做 ∃ 聚合
```

`array_offsets` 由 exec 自己从 segment 取（`JsonContainsExpr.cpp:2462` 的 `segment_->GetArrayOffsets(...)`），索引全程不认识它。两种聚合策略各有专门机制：

| 场景 | 机制 | 聚合发生在 |
|---|---|---|
| 相关元素谓词（struct array） | `PhyElementFilterBitsNode`：取 offsets 存进 `QueryContext`、元素级求值、`set_bitset_is_element_level(true)`（`ElementFilterBitsNode.cpp:102,134`） | 最晚——`ProjectNode` 的 `find_first_n_element`、`VectorSearchNode.cpp:129,147`、`ExecPlanNodeVisitor.cpp:74,332` 这些边界算子 |
| 不相关谓词（`ContainsAll`） | `result &= query_in(...)`（`JsonContainsExpr.cpp:2497-2500`）：每个值先 ∃ 聚合到行，再行级相与 | 最早——进 AND 之前 |

exec 侧已把这个选择做成每次调用的开关：`ProcessIndexChunksWithRowLevel`（`func_returns_row_level = true`，调用方 lambda 自己聚合）对应前者之外的一切；默认路径的 `need_element_slicing`（`Expr.h:2181`）把元素级 bitmap 按元素区间切片后原样往下传（`Expr.h:2198-2214`），完全不聚合。

聚合之后 `arr == [1,2,3]` 仍需 exec 侧 `is_same_array` 精确验证（存在量词 ≠ 精确相等），即候选类型的常规形态，与 geometry / ngram 同构。

#### 本阶段要统一的：把聚合规整成一个显式算子

聚合今天散在三处、粒度各不相同：`JsonContainsExpr.cpp:2469` 用 `ForEachRowElementRange` 逐行聚合、`UnaryExpr.cpp:743` 用 `ElementIDToRowID` 逐元素反查、`Expr.h:2205` 用 `ElementIDRangeOfRow` 做区间切片。三份实现、三种写法。

要统一的不是"把聚合塞进 reader"，而是把它规整成 exec 侧一个显式的元素到行的聚合算子，由 plan 决定放在哪一步。这条属于 [阶段 4 query 三分](README.md#7-阶段计划)，阶段 1 只需保证索引侧交付的是干净的元素级结果。

#### 元素→行映射的所属组件：列的派生物，索引不持有

映射体是 `ArrayOffsetsSealed` 的前缀和（`std::vector<int32_t>`，4 字节/行）。理由：

1. **由列加载产出**：`ArrayOffsetsSealed::BuildFromColumn(*column, field_meta, num_rows)`（`ChunkedSegmentSealedImpl.cpp:6963`），存进 segment 的 runtime state。
2. **按 struct 共享**：`struct_to_array_offsets[struct_name]`（`:6957-6966`）——同一 struct 下所有 array 字段共用一个实例，`array_offsets_map[field_id]` 只是第二把索引。
3. **随列换代**：字段释放/reopen 时 erase（`:4003,4018`），reopen 后重建为新对象；COW 换代时拷进新 runtime snapshot（`:1029,1425`）。

> **一条外部依赖，但不卡索引侧。** [宽表建模](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「六、查询节点的数据表达」（内存 / mmap / Vortex 如何表示嵌套、如何只加载单根子列）仍是 TODO。它影响的是 `ColumnInterface` 能不能提前冻结，不影响本节接口——索引只需知道自己建在哪一层，输出始终是最内层元素坐标系的 bitmap。登记在[总览 §9 第 5 条](README.md#9-待定问题)。

它是列的派生物。索引侧不持有、不注入、不需要知道它存在——这也让 reader 保持 [§5](#5-查询接口) 声明的"由 `Seal()`/`Open()` 产生后即不可变"，无需任何加载后装配或 reopen 重绑。

> **若将来推翻本结论（确有某类查询必须在 reader 内聚合），备案是"pin 时随句柄携带"**：offsets 由 segcore 在 `PinCells` 返回 `Pinned<>` 句柄时一并附上，reader 本身仍不持有。曾评估过的另两条路都不要走回：`Loader::Open` 的 `LoadOptions` 注入会凭空引入"列先于索引加载"的约束（毁掉 index-only 段的冷取优化），且 reader 捕获的 `shared_ptr` 在列 reopen 后变成过期而不悬垂的旧代——静默错误结果；加载后由 segcore 二次装配则要求列的加载路径知道索引存在（跨模块不变式，漏一处即静默错），并给一个声明"不可变、可无锁并发读"的对象开了可变中间态。

由此排除一个看似最省的做法：postings 里直接存 row id（构建期聚合）。它把聚合固化在构建期，等于让索引替 plan 做了决定，struct array 的相关元素谓词从此永远查不对；省下这部分空间会使一类查询返回错误结果。

#### 多层嵌套：不是 roadmap，是子列的固定形态

[宽表建模设计文档](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「二、Schema 系统」把多层嵌套定为硬约束：

> 每一个具体的子列由一个物理列表示，**类型固定为 `Array<Array<...<T>>>`，深度由路径中的 `Array` 数量决定**，路径中的 `Struct` 不影响深度。**每层包括一个 `offsets`，每个 nullable 层包括一个 `validity` bitmap**。

即：多层不是将来要加的能力，而是嵌套子列的通用形态，单层只是深度 = 1 的特例。三条直接后果：

1. **元素→行的映射不是"一个 `IArrayOffsets`"，是一条 offsets 链**（每层一个）。聚合是沿链逐层复合的前缀和，而非单次 `ElementIDToRowID`。所属组件不变——链在 exec / columnar-format 侧，索引只在它建索引的那一层元素坐标系里工作。
2. **"索引自带一份前缀和"被彻底排除**：该文档「五、存储层的更新」明确 *Array of struct 里的 offset 共享*，索引内复制一份既与存储层的共享方向正面冲突，层数越多复制代价越大，而且它不该做聚合。
3. **本节查询接口按 N 层写、N=1 退化**，不得出现单层假设。对外形态不变——reader 始终在最内层元素坐标系输出 bitmap，`CoordDomain()` 只区分行 / 元素、不对外提供层数；变化被限制在 builder 侧与 exec 聚合链的复杂度上。

**节奏依赖（不由本阶段决定）**：该文档「二十、后续功能」把标量索引列为嵌套建模的后续项。因此阶段 1 在嵌套接口上只定形状、不定实现，实现节奏由宽表建模的推进牵引。聚合归 exec 之后，index 侧对该文档「六、查询节点的数据表达」的依赖弱化：索引只需知道自己建在哪一层，不需要知道链怎么表示。

## 6. 构建接口与加载接口

两个接口的分界是输入不同，不是阶段先后：

| | 输入 | 输出 | 谁调用 | 在哪个进程 |
|---|---|---|---|---|
| **Builder** | 列数据（数组或列游标） | Artifact | indexbuilder 服务 / segcore load 加载时构建 | 建索引任务 / querynode（interim） |
| **Loader** | 落盘字节 | Reader | segcore load | querynode |

看起来重叠的只有一处：序列化被放在 `Artifact::Serialize` 而不是持久化接口自己身上。这是有意的不对称，三条理由：

1. **写方向没有独立调用点**。序列化永远紧接 `Seal()` 发生，从不单独发生；反序列化则发生在另一个时间、通常另一个进程，那时没有 Builder。一个同时提供 Serialize/Deserialize 的对称 Codec，将是一个没有任何调用点同时用到两个方法的类——对称只是形式上的。
2. **文件式索引类型的写方向无法再分一层**。tantivy 系（inverted / text / bson）在构建过程中就直接往本地目录写字节，字节布局是 builder 的内部实现；事后再交给一个编解码器去"编码"是虚构的抽象，这些索引类型的 `Serialize` 实质只是把已经落在本地的文件集交出去。
3. **Artifact 知道自己的物化形态**。它可能是内存结构（bitmap、marisa、有序数组），也可能是本地文件集（tantivy 系）；由它自己实现 `Serialize` 只需一次分派，由外部组件实现则要按索引类型再分派一次。

所以这个接口实际是只读方向的组件，命名为 `IndexLoader`：在没有 Builder 在场时，把落盘产物变回可读对象。它与 Builder 的唯一耦合是格式约定（同一索引类型的 Builder 写、Loader 读，必须一致），这个耦合靠同类型同目录 + 往返测试约束，不靠共用一个类。

### 6.1 Builder

```cpp
// index/contracts/IndexBuilder.h
// 两类索引共用一个接口。T 是值类型而非索引类型：变长值用 view 类型表示
// （std::string_view / ArrayView / SparseRow view），dense vector 用 float
// 且 dim 在构造期已知。因此没有 Scalar/Vector 前缀。
template <typename T>
class IndexBuilder {
 public:
    virtual ~IndexBuilder() = default;

    // 静态能力描述：调用方据此决定怎么输入（§6.1.2）
    virtual BuilderInputSpec InputSpec() const = 0;

    // 唯一的数据输入接口：push。调用方驱动。
    virtual void Add(size_t n, const T* values, const bool* valid) = 0;

    // 仅 form == LocalFile 的索引类型（DiskANN）：数据已由物化器落到本地文件
    virtual void SetSourceFile(const std::string& path) {}

    virtual IndexArtifactPtr Seal() && = 0;
};

// 产物：内存 reader 或文件集合，由索引类型自己决定物化形态
class IndexArtifact {
 public:
    virtual std::shared_ptr<IndexReaderBase> OpenReader() const = 0;   // 直接使用
    virtual void Serialize(storage::FileSink&) const = 0;              // 交给持久化
};
```

> **为什么没有 `Consume(ScanCursor&)`（pull 接口）**。两条构建路径的源与模式本就不同：离线构建（indexbuilder，生产主路径）的源是远端 manifest/binlog，且已经是 push——`IterateFieldDataFromManifest(..., const std::function<void(FieldDataPtr)>& consumer, max_inflight_bytes)`（`storage/Util.h:352`）逐 batch 回调、后台池解码、按输入字节限流，那里根本没有 segment、没有列对象、没有 `ScanCursor`；加载时构建的源才是已加载的列（`generate_interim_index` 收 `ChunkedColumnInterface`，`ChunkedSegmentSealedImpl.h:1385`）。把 pull 摊成 push 只是调用方写个循环，把 push 包装成 pull 要加线程/协程或缓冲反转。所以统一到 push。
>
> 连带结论：builder 的输入数据类型是裸数组，不是任何组件的对象——它不认识列、不认识游标、不认识存储格式，`index → columnar-format` 在 builder 侧降到 0；reader 侧同样降到 0（[§5.8](#58-nested元素级索引坐标与投影)：元素→行的聚合不属于索引），因此这条依赖在接口拆分后整体消失。

各索引类型的 builder 是这个接口的实现而非新的接口：text 类型的分词器配置、json 类型的 path 配置都是构造参数。


#### 6.1.1 输入形态：五档，两类索引交错

"标量流式、向量全量"是错的分界——标量自己就横跨三档，向量落在其中两档。实测：

| 档 | 输入形态 | 谁 | 证据 |
|---|---|---|---|
| **A** 真流式 | 逐片送入引擎，索引类不缓冲 | tantivy 系（inverted / text / ngram / json flat / bson） | `wrapper_->add_data<T>(ptr, n, offset)` 逐 slice（`InvertedIndexTantivy.cpp:686`） |
| **B** 全量驻留 | 输入可单遍，但全量必须在内存里才能构建完成 | `ScalarIndexSort`、`BitmapIndex`、`StringIndexMarisa`、`FMIndex`、`RTreeIndex` | marisa 收满 keyset 再 `trie_.build()`，然后再走一遍回填 `str_ids_`（`StringIndexMarisa.cpp:173-205`）；FM 拼接全部 docs 交 libsais（`FMIndex.cpp:172`）；RTree `bulk_load_from_field_data`（`RTreeIndex.cpp:366`） |
| **B+** 连续缓冲 | 全量驻留且要求单块连续内存 | knowhere 内存索引 | `CacheRawDataToMemory` → 拼成单个 tensor → 一次 `index_.Build(dataset)`（`VectorMemIndex.cpp:533-600`） |
| **C** 需先验统计 | 选型前要先看数据，两遍 | `HybridScalarIndex` | `SelectBuildTypeForPrimitiveType` 先扫求基数（有早退，distinct 达上限即 `break`，`HybridScalarIndex.cpp:167`），再 `GetInternalIndex()->BuildWithFieldData` |
| **D** 本地文件 | 原始数据落成本地文件，按路径交付 | DiskANN | `CacheRawDataToDisk<T>` → `DISK_ANN_RAW_DATA_PATH`（`VectorDiskIndex.cpp:460-462`）；它明确不要数据在内存里 |

> **今天"看起来所有索引类型都全量驻留"是入口造成的假象**：共同入口 `storage::CacheRawDataAndFillMissing` / `CacheRawDataToMemory` 先把整个字段拉进内存，与索引类型本身的需求无关。A 档并不需要——改成游标输入后 A 档能真省内存，B/B+ 省不了。

#### 6.1.2 处理：接口统一，差异收进声明

不给每个索引类型开一个 Builder 接口，也不硬塞进单一签名。把差异从接口挪到数据：

```cpp
struct BuilderInputSpec {
    enum Form { Streaming, Contiguous, LocalFile } form = Streaming;
    bool needs_second_pass = false;       // C 档：Seek(0) 重扫；第一遍可早退
    std::vector<FieldId> side_inputs;     // 非本列的构建输入，见下
};

// 每个索引类型的 builder 静态描述自己，调用方据此决定怎么输入
virtual BuilderInputSpec InputSpec() const = 0;
```

- Builder 接口仍只有一个：`Add(n, values, valid)` + `Seal() → Artifact`。`Add` 表示的是输入方式，不是"能流式构建"；内部缓冲多少、要不要第二遍是实现细节。
- 三种物化方式（不物化 / 连续 buffer / 本地文件）由一个共享物化器实现一次，不是每个索引类型抄一遍。这与 [§4.1](#41-能力描述符) 的 `ReaderCaps` 是同一手法，只是在构建侧。
- **多遍必须写进接口**：C 档要求 `ScanCursor::Seek(0)` 重扫可用（[#51504](https://github.com/milvus-io/milvus/pull/51504) 的 `ScanCursor` 有 `Seek(position)`，表示得了），代价是冷列二次 IO。
- **`side_inputs` 不是预留**：`VectorMemIndex::Build` 读 `VEC_OPT_FIELDS` 并调 `CacheOptFieldToMemory`（`VectorMemIndex.cpp:539-547`），partition key isolation 需要另一个字段的数据作构建输入。单游标签名装不下——Builder 的输入不总是"本列"。

连带收益：今天三个几乎同义的入口 `CacheRawDataAndFillMissing`（标量）/ `CacheRawDataToMemory`（向量内存）/ `CacheRawDataToDisk`（DiskANN）散在各索引类型里，且都绑死 `FileManager`。统一成"声明 + 共享物化器"后，`FileManagerContext` 从 builder 上彻底消失——这正是 [§3 原则 6](#3-设计原则) 的实现路径。


### 6.2 Loader 与 IO 注入

```cpp
class IndexLoader {
 public:
    virtual ~IndexLoader() = default;
    virtual std::string Family() const = 0;   // "inverted" / "bitmap" / "text" / "json_stats" / ...

    // 与 IndexArtifact::OpenReader() 是通向 Reader 的两个入口：
    // 前者从落盘产物打开，后者从刚构建完的产物直接打开。
    virtual std::shared_ptr<IndexReaderBase>
    Open(storage::FileSource&, const LoadOptions& opts) = 0;   // opts: mmap、warmup
};
```

方法名用 `Open` 而非 `Deserialize`：mmap 形态下并不发生"反序列化"（不完整物化），且 `Open` 与 `Artifact::OpenReader()` 成对，读出两个入口的同构关系。

`FileSink`/`FileSource` 是 storage 提供的最小接口（本地暂存 + 远端读写），取代索引类持有 `FileManagerContext`。`Serialize`/`Load`/`Upload`/`LoadUnified`/`UploadUnified` 从索引类上全部移除：序列化在 Artifact，打开在 Loader，上传编排在 indexbuilder 服务，加载编排在 segcore load。

### 6.3 Hybrid 变为 Builder 策略

`HybridScalarIndex` 的"按基数选 bitmap 或 inverted"是构建期决策：Builder 在 `Seal()` 时选型并把选择记进 artifact 元数据，`Loader::Open` 直接返回选中的具体 reader。运行时转发类删除。

## 7. Growing 接口

growing 标量索引是已有生产事实：`TextMatchIndex` 的 growing 构造（commit interval + background merge + `AddTextsGrowing` + `Commit`/`Reload` + reader 快照，`TextMatchIndex.h:31-37`）就是一个 growing 增量索引；segcore 的 `ScalarFieldIndexing<T>`（`FieldIndexing.h:141`）是另一套并行机制。本设计把这个模式提炼为统一接口：

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

    // 已提交行数：快照覆盖到哪一行。单调不减。
    virtual int64_t CommittedRows() const = 0;
};
```

三条关键语义：

1. **快照 + 已提交行数，不承诺实时**。tantivy 的 commit/reload 天然是这个模型；今天 text match 的 commit 时滞是隐式的，接口把它变成显式的已提交行数。
2. **覆盖不足的桥接不在本接口内**。`[CommittedRows(), insert_barrier)` 这段未入索引的行怎么办是 segcore/exec 的执行策略，索引只报告已提交行数。这与 §5.5 反查接口的"决策在消费者"是同一原则。

   策略按索引类型分：text match 允许滞后（不补齐），其余类型回退列扫描补齐。策略表放 segcore、按索引类型 key；索引侧不设任何表示该策略的位——否则 `ReaderCaps` 会开始负责产品语义。
3. **"加载时构建"归 Builder 接口，不归本接口**。sealed 段加载时无索引则在加载时构建——今天有两处：`generate_interim_index`（`ChunkedSegmentSealedImpl.h:1385`，收 `ChunkedColumnInterface`，vector 专属：float/fp16/bf16/sparse 且无正式索引时建临时 binlog index）与 `CreateTextIndex`/`CreateTextIndexWithSchema`（`:252,2023`，text 从原始数据在加载时构建）。它们走 [§6.1](#61-builder) 的 `IndexBuilder<T>::Add`，由调用方逐 chunk 从已加载的列输入，不再是 growing 类的另一个构造分支——`TextMatchIndex` 的四构造函数问题就此消解。

   > **加载时构建 ≠ growing**。两者都"在 segcore 里建索引"，但分属两个接口：加载时构建是 Builder（一次性、独占、以 Artifact 结束，输入是一根已加载的完整列）；growing 是 Appender（长期存活、与写并发、只产按已提交行数截取的快照）。混淆这两者正是今天 `TextMatchIndex` 把 growing writer 和 sealed 加载时构建塞进同一个类的原因（§2.2）。

`GrowingTextIndex`（Append 文本、快照给出 `TextMatchReader`）同型。segcore 的 `GrowingIndexSet` 持有这些 appender，`FieldIndexing` 的分散机制退役。

### 7.1 vector 的 Appender 接口：今天已存在，且带着与 `IndexBase` 同构的问题

Appender 不是标量专属。`segcore/FieldIndexing.h` 里两类索引今天都实现了 append：`VectorFieldIndexing::AppendSegmentIndexDense`/`Sparse`（`:281,287`）建 growing interim 向量索引，`ScalarFieldIndexing::AppendSegmentIndex`（`:171,177`）建 growing 标量索引。但共享基类 `FieldIndexing`（`:51`）是两类接口的并集，5 个纯虚里 3 个 vector 专属、2 个 scalar 专属，两个子类各自 throw 掉对方那一半（§2.2）。

接口与索引类型的关系：

> **四种接口对两类索引统一；每种接口内部的方法按索引类型分。** 接口是按调用方切的（谁在用），索引类型是按数据与算法切的（用什么结构）——两者正交。`FieldIndexing` 的错误正是把"两类索引共有一个接口"误当成"两类索引共有一组方法"。

因此 growing 接口的形状是：`GrowingScalarIndex<T>` 与 `GrowingVectorIndex` 并列，共享语义——快照 + 已提交行数。两类索引 `Append` 的签名差异很小：现状那份 dense/sparse 二分不是索引类型的本质差异。

> **`AppendSegmentIndexDense`/`Sparse` 的二分不该保留，而原因不在签名——在于这两个方法各自内部同时是 Builder 和 Appender。**
>
> 对比两个实现（`FieldIndexing.cpp:268` sparse / `:418` dense）：处理流程同形——凑够 `build_threshold` 行连续内存 → `knowhere::GenDataSet` → id-map validity → `BuildWithDataset` → 此后 `AddWithDataset` 增量。真实差异只有三点：dense 的 `dim` 来自 schema、sparse 的 `dim` 是每批次传入的 `new_data_dim`；dense 定宽可 `FastMemcpy`、sparse 是 `SparseRow` 对象须逐元素赋值；sparse 多一个 `SetIsSparse(true)`。三点都不构成两个接口的理由。
>
> 关键在分支：`!built_` 分支（`:292-336` / `:444-489`）把 `[0, build_threshold)` 从整根 `ConcurrentVector` 全量 gather 出来做 `BuildWithDataset`——这是冷启动全量建，正是 [§6.1.1](#611-输入形态五档两类索引交错) 的 form B+（连续缓冲），属于 Builder 接口；`built_` 分支（`:340-414` / `:492-567`）只用本批 `data_source` 做 `AddWithDataset`，这才是 Appender。
>
> 签名里那个 `const VectorBase*`（整根列）只为第一个分支存在。第二个分支对它的唯一用途是取本批 validity，等价于一个 `const bool* valid`。
>
> 相关代码在 `FieldIndexing.cpp:75-95`——`field_meta.is_nullable() && field_raw_data->is_mapping_storage()` 时取 `field_raw_data->get_valid_count()`，再与 `get_build_threshold()` 比较决定走哪个分支。validity 只需本批一段。
>
> 因此顺序是：先把 `!built_` 冷启动分支迁到 Builder 接口（segcore 在越过阈值时从列 gather 一次输入 `IndexBuilder`，产物交给 appender 作起点），`Append` 即退化为与标量同形的单个方法，`VectorBase*` 从签名消失，dense/sparse 只剩一个 `dim` 参数的区别（见 [§11.3](#113-vector-按四种接口重新划分不重设计)）。
>
> `FieldIndexing` 签名畸形的直接原因，就是两个接口混在同一个方法体里——这印证了 §7 第 3 条「加载时构建 ≠ growing」。

> 两处现状说明：
>
> 1. **vector growing 今天没有换代，也没有快照。** 本节表里"快照不可变、knowhere interim index 换代"是目标而非现状：`index_` 是一个活对象，`AddWithDataset` 就地修改，`get_segment_indexing()` 把裸指针直接交给查询（`FieldIndexing.h:332-335`），`sync_with_index_` 只是个 bool。所以 §7 的快照 + 已提交行数模型对 vector 是真实的行为变更要求，不是把现状形式化。
> 2. **"growing 标量索引是已有生产事实"要缩小范围。** 真正在跑的只有 GEOMETRY：其余标量类型的 `ScalarFieldIndexing::AppendSegmentIndex` 直接 `ThrowInfo(Unsupported, "... not implemented for non-geometry scalar fields")`（`FieldIndexing.cpp:706-711,745-750`），而 `recreate_index` 仍为它们造出 marisa/sort 对象（`:659-662`）——造完不再写入、也不读取，`ScalarFieldIndexing::data_` 全仓零写入，`get_chunk_indexing` 会索引到空 vector。加上 `TextMatchIndex` 的 growing 构造，生产事实是"文本 + 空间两类"，不是"标量各类型普遍"。这不改变设计结论（统一接口仍要做），但改变风险性质：`GrowingScalarIndex` 对绝大多数索引类型是新增能力而非重构既有能力，[§13.1](#13-被本重构触发的已有缺陷) 的 bitmap 越界因此必然发生，不是可能发生。

两类索引在这三点上一致：

| 语义 | scalar | vector |
|---|---|---|
| 快照不可变、并发共享 | tantivy commit/reload | knowhere interim index 换代 |
| 已提交行数（快照覆盖到哪一行） | `CommittedRows()` | 同；今天隐含在 `sync_data_with_index()` 里 |
| 阈值前无快照 | 无（`ScalarFieldIndexing::get_build_threshold()` 返回 0，`:197`） | 有（`VectorFieldIndexing::get_build_threshold()` 取配置，`:321`）——`ReaderSnapshot()` 返回空即表示 |

阈值差异不构成分歧：接口已用"返回空表示尚无可读快照"表示，scalar 只是阈值恒为 0 的退化情形。

**连带的 `IndexBase` 退役项**：`FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 返回 `PinWrapper<index::IndexBase*>`（`:128,131`）——growing 侧同样以 `IndexBase` 作类型擦除句柄。[§11.2 第 3 条](#112-处理决定)的"`IndexBase` 在阶段 1 内退役"必须把这两个出口一并算进去，否则 sealed 侧删干净了、growing 侧还留着一个引用。

## 8. 现有实现类 → 新接口映射

| 现类 | 查询接口 | 构建/growing | 备注 |
|---|---|---|---|
| `InvertedIndexTantivy<T>` | `ScalarPredicateReader<T>` + `PatternMatchReader` | Builder | tantivy 封装降为内部引擎，不再是基类 |
| `BitmapIndex<T>` / `ScalarIndexSort<T>` / `StringIndexMarisa` | `ScalarPredicateReader<T>` + `ScalarValueReader<T>`；`BitmapIndex<std::string>` 与 `StringIndexMarisa` 另 + `PatternMatchReader` | Builder | `is_nested_index_` 模式位保留，对外表示为 `CoordDomain() == Element`（§5.8）。`BitmapIndex` 有完整 LIKE 系列（`BitmapIndex.h:219` `SupportPatternMatch`、`:224` `PatternMatch`、`:281` `PatternQuery` 用 `LikePatternMatcher`），删掉它会删掉在用的代码 |
| `HybridScalarIndex<T>` | 消失 | Builder 选型策略 | §6.3 |
| `StringIndexSort` | `ScalarPredicateReader<std::string>` + `PatternMatchReader` + `ScalarValueReader` | Builder | 它不是类型别名（576 + 1860 行，自带 pImpl 层次与自己的带版本二进制格式，存在的原因是 `ScalarIndexSort<T>` `static_assert(is_arithmetic_v<T>)` 装不下 string）。它也有完整 LIKE 系列（`StringIndexSort.h:131,136`，三处实现覆写）。与 `ScalarIndexSort` 合并是真实工作量，不是随迁 |
| `BoolIndex` | 同 bitmap | Builder | 这个是类型别名（32 行、无类），随迁 |
| `FMIndex` | `PatternMatchReader`，仅此谓词接口 | Builder | `SegcoreConfig` 依赖改构造参数注入。原因是全局可变配置放在 segcore 里，于是所有要读配置的模块都被迫依赖它（`FMIndex.h:30` include `segcore/SegcoreConfig.h`、`:227` 读 `default_config()`） |
| `TextMatchIndex` | `TextMatchReader` | `IndexBuilder<std::string_view>` 的 text 实现 + `GrowingTextIndex` | 四构造函数拆到四个接口 |
| `NgramInvertedIndex` | `NgramReader` | Builder | Phase2 删除，`index → exec` 依赖消失 |
| `JsonFlatIndex` (+ QueryExecutor) | `JsonIndexReader` | `IndexBuilder<std::string_view>` 的 json 实现 | |
| json path cast index | `ScalarPredicateReader<T>` | Builder | 索引清单按 (field, path) 注册 |
| `JsonKeyStats` | **迁出 index**。阶段 1 内移到 `segcore/json_stats/`（断继承 + `git mv`）；终局子列升格 columnar-format、layout 目录留 segcore | 构建仍是离线任务，暂不接 L1 产物的构建与加载流程 | `NotImplemented` 泛滥消失；对 segcore 的 5 处 include 当场降到 0；`BsonInvertedIndex` 一并迁走。终局与过渡见 [§1](#1-范围) |
| `RTreeIndex` / geometry | `SpatialReader`（§5.6） | Builder | `Candidates` 成为唯一的谓词查询接口；点谓词重载（未实现桩，`RTreeIndex.cpp:412,420,427,464,474,481,489,500`）与 `Reverse_Lookup` 移除，但 `IsNull`/`IsNotNull` 保留、归 `NullReader`；`Query(DatasetPtr)` 是在用的入口不是未实现桩，删的是包装、行为搬进 `Candidates`（见 §5.6）；GIS proto 枚举换 native `SpatialOp` |
| `SkipIndex` | **移出 index 接口** | | 归 columnar-format（zone-map/`CellSkipPredicate`） |

> 表中不逐行重复 `NullReader`：七种标量索引全员真实现、零 throw，是无条件可用的公共接口（见 [§5 开头](#5-查询接口)）。
| `VectorMemIndex<T>` / `VectorDiskIndex<T>` | `VectorSearchReader` + `VectorValueReader`（§11.3） | `VectorIndexBuilder` + growing（接替 `VectorFieldIndexing`） | knowhere 交互原样内移，不重设计 |

## 9. 消费者对接

| 消费者 | 现状 | 目标 |
|---|---|---|
| exec 表达式 | `PinIndex` 拿 `IndexBase*` 后 `dynamic_cast` 到具体类型；能力探测靠 `Support*` + try | pin 出口给 typed reader；路径决策只读 `ReaderCaps`；`dynamic_cast` 降到 0（阶段 1 验收标准） |
| exec ngram | `ExecutePhase1/2`，Phase2 传 `exec::SegmentExpr*` | Phase1 = `NgramReader::Candidates`；Phase2 = exec 用 columnar-format `Scan`/`Take` 取值后自行求值 |
| exec geometry | `QueryCandidates` + `PhyGISCoarseConjunctExpr`/`PhyGISRefineConjunctExpr`——切分已正确 | 仅换接口：`SpatialReader::Candidates` + native `SpatialOp` + bitmap 输出；粗筛/精化的处理流程不动，与 ngram 统一为同一候选类型的处理流程 |
| exec ARRAY 相等 | `ExecArrayEqualForIndex`：`InApplyCallback` 逐元素回调 → `unordered_set` 求交 → `to_row_offset` 转坐标 → `is_same_array` 精确验证 | 索引给元素级 `In()` bitmap（§5.8）；exec 侧逐元素 `inplace_and` + 1% 提前退出，再经统一的聚合算子聚合到行做 `is_same_array` 验证。`InApplyCallback` 与 `unordered_set` 消失，`to_row_offset` lambda 并入该算子 |
| indexbuilder | `ScalarIndexCreator` 调 `CreateIndex/Build/Serialize/Upload` | Builder + `Artifact::Serialize` + storage sink；Creator 变薄封装 |
| segcore load | `Load`/`LoadUnified` + cachinglayer 计费实现在索引里 | `Loader::Open` + 计费在 load 侧 translator |
| segcore growing | `FieldIndexing`/`ScalarFieldIndexing` 分散机制 | `GrowingScalarIndex`/`GrowingTextIndex`，由 `GrowingIndexSet` 持有 |
| reduce 物化 | `ReverseDataFromIndex`（`segcore/Utils.h:151`） | `ScalarValueReader::Gather` |

## 10. 硬性规则（lint）

1. `index/` 不得 include `segcore/`、`exec/`、`query/`（现状违规：`NgramInvertedIndex` 2 处、`FMIndex` 1 处、`JsonKeyStats` 5 处——全部本阶段降到 0，其中 `JsonKeyStats` 随目录迁往 `segcore/json_stats/` 而消失，见 §1 过渡处理）。
2. `FileManagerContext`/`DiskFileManagerImpl` 不得出现在任何 Reader/Builder/Appender 的签名与成员中，只允许出现在各索引类型的 Loader/Artifact 实现文件与 indexbuilder 上传编排内。
3. 实现类之间禁止继承（`grep ": public.*Index" `，只允许继承接口）；查询接口不得继承 `IndexReaderBase`（纯 mixin，§4 注）。
3b. `ReaderCaps` 的查询期来源必须是索引清单缓存的纯数据；lint 检查路径决策代码（`DetermineExecPath` 一类）不出现 pin 调用（§4.3）。
4. 能力缺失禁止用 `ThrowInfo(Unsupported)` 表示——lint 检查接口实现中不出现该模式。
5. cachinglayer 类型不得出现在任何接口签名（计费/pin 在 segcore load 侧）。
6. `knowhere` 头不得出现在共享基类与标量索引类型的接口及实现中，仅 vector 索引类型及其 Loader/Artifact 可见（§11.2 第 5 条）。现状违规基线：标量索引类型的头文件自身 knowhere 计数为 0，违规全部来自传递链——`index/Index.h`（共享基类，3 处直接引用）、`index/Utils.h` → `common/QueryInfo.h` → `knowhere/config.h`（几乎所有标量索引类型的实现文件都 include `index/Utils.h`），以及最大的一条：`common/Types.h` 自己（`:27-34` 直接 include `knowhere/binaryset.h`、`comp/index_param.h`、`dataset.h`、`operands.h` 与 `pb/plan.pb.h`、`pb/schema.pb.h`、`pb/segcore.pb.h`）。而 `TargetBitmap`/`DataType`/`FieldId` 只在 `Types.h` 里 alias——任何接口头只要用 `TargetBitmap` 就传递性拉进 knowhere 与 pb。见 [§12.1(a)](#121-vector-查询接口的细化)。

## 11. 标量/向量公共接口：清单与处理

### 11.1 共享范围清单

| 共享物 | 事实 | 性质 |
|---|---|---|
| 类型基类 | `ScalarIndex<T>` 与 `VectorIndex` 都继承 `IndexBase`；两边都对它 Liskov 违约（vector 的 `BuildWithRawDataForUT` throw，scalar 的 `BuildWithDataset` throw）；基类上还有 json 专属的 `GetCastType`/`Exists`，vector 被迫继承 | 不当共享 |
| 统一句柄 | `CacheIndexBasePtr = CacheIndexPtr<IndexBase>`（`Index.h:167`）；segcore 的 `scalar_indexings` 与 `SealedIndexingRecord` 持同一句柄，load 路径对两类索引一视同仁 | 合理共享（索引清单需要） |
| 生命周期/持久化接口 | `Serialize→BinarySet`、`Load`×2、`Upload→IndexStatsPtr`、index_files 约定、mmap 标志、`CellByteSize` 计费——"构建→序列化→上传；下载→加载→pin→计费"两类索引完全同构 | 合理共享（收益所在） |
| storage 流程 | `FileManagerContext`/`Mem`/`DiskFileManager` 被 17 个 index 头引用 | 合理共享 |
| 工厂 | `IndexFactory::CreateIndex` 单点分派两类索引，`CreateIndexInfo` 参数袋职责混杂 | 不当共享 |
| 被动头文件共享 | `Index.h` 顶层头文件 include knowhere 三个头 + `cachinglayer/CacheSlot.h`——每个标量索引 TU 都在编译 knowhere；`BinarySet = knowhere::BinarySet`（`common/Types.h:681`），标量索引的序列化数据类型也是 knowhere 类型 | 不当共享（纯历史） |
| 查询接口 | `In/Range/bitmap` vs `Query(dataset, SearchInfo)/VectorIterators` | 不共享 |
| growing appender 基类 | `segcore/FieldIndexing.h:51` 是两类接口的并集，两个子类各自 throw 掉对方那一半（§2.2、[§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)）；`get_chunk_indexing`/`get_segment_indexing` 又把 `IndexBase` 泄到 growing 侧（`:128,131`） | 不当共享（与 `IndexBase` 同构，只是在 growing 侧） |
| growing 语义（快照 + 已提交行数 + 阈值前无快照） | tantivy commit/reload 与 knowhere interim 换代是同一模型；阈值差异由"返回空快照"吸收 | 合理共享（语义共享，签名不共享） |

### 11.2 处理决定

1. **共享基类收缩为生命周期基类，且生命周期那半下移到 L1**。切成两段：

   | 段 | 内容 | 层 | 谁用 |
   |---|---|---|---|
   | 产物的构建与加载流程 | `Artifact`（`Serialize(FileSink&)`）、`ArtifactLoader`（`Open(FileSource&, LoadOptions&)`）、`FileSink`/`FileSource`/`LoadOptions`/`ArtifactStats`、最小基类 `LoadedArtifact`（负责 `CellByteSize()` 计费） | L1 | index 各索引类型（下层依赖）、columnar-format 的 shredded 布局（同层） |
   | 索引查询基类 | `IndexReaderBase : LoadedArtifact`（能力描述、`Count()`、类型擦除） | L2 | 仅 index |

   下移的理由：这套流程没有一处是索引专属的。JSON shredded 布局（[§1](#1-范围)）同样是「离线构建、落盘、按需加载、参与缓存计费的派生产物」，需要同一套东西，而它在 L1；把流程留在 L2 就只剩两条路——要么 L1→L2 违反分层方向的依赖，要么 `BsonInvertedIndex` 那样把 `AddRecord`/`BuildIndex`/`LoadIndex`/`UploadIndex`/`CellByteSize` 再手写一遍。下移后两者都不必。`ArtifactLoader::Open` 返回 `shared_ptr<LoadedArtifact>`，各层自己 downcast——与 §4.2 的类型擦除基类同一手法，只是下移一层。

   设计验收标准：三种物化形态都装得下——knowhere `BinarySet`（内存 blob 集合）、DiskANN（本地大文件、流式）、mmap。连带：`IndexArtifact`/`IndexLoader`/`IndexStats` 的 `Index` 前缀随下移失效（命名待定，见 [§12.2](#122-产物的构建与加载流程放在哪个组件叫什么)）。`CellByteSize` 的返回类型不再触碰硬规则 4——`ResourceUsage` 从 cachinglayer 移到 milvus-common 的 `common/ResourceUsage.h`（`namespace milvus`），L1 与 segcore 共用同一个类型、无需边界转换；这是一条跨仓前置改动，须先于流程下移实现。

   > **验收结果：第一版 `FileSink`/`FileSource` 三种形态里只装下了一种半，且出现了第四种形态。** 这是把接口写成代码后跑出来的结果：
   >
   > | 形态 | 结论 | 依据 |
   > |---|---|---|
   > | knowhere `BinarySet` | 装得下，但有前提 | `index_.Serialize(BinarySet)` 产出具名内存 blob，对上 `WriteEntry` 是字面匹配。前提是 `Disassemble`/`Assemble`（超过 `FILE_SLICE_SIZE` 的 blob 切成 `name_0..name_k` + `INDEX_FILE_SLICE_META`）必须移进 sink/source 内部——否则 `EntryNames()` 返回的是物理切片名，物理布局泄进 Loader，且每个索引类型抄一遍 |
   > | DiskANN 本地大文件 | 写侧装得下，读侧要补一条承诺 | 写侧 `WriteEntryFromLocalFile` 正合适；读侧是"下载到本地目录 + knowhere 自己按 `DISK_ANN_PREFIX_PATH` 开文件"，`ReadEntriesToLocalDir` 能表示，但接口必须补上今天没有的一条：本地文件名等于文件项 basename |
   > | mmap | 装不下 | `VectorMemIndex::LoadFromFile` 把 n 个远端文件项流式合并成 1 个本地文件（`storage::FileWriter`），knowhere mmap 的正是这个合并文件。`FileSource` 只有 1→内存、1→1 文件、n→n 文件，没有 n→1 |
   > | **DiskANN streaming（第四形态）** | 描述不了 | `GetCacheFilesForDiskIndexLoad(index_files, index_.LoadIndexWithStream())`（`index/VectorIndexValidDataUtils.h:98`）：streaming 时只下载 valid-data 切片，索引字节不经过这套流程，引擎自己去远端读。流程不是这些字节的读者，`ReadEntry`/`ReadEntryToLocalFile`/`ReadEntriesToLocalDir` 任何组合都表示不了 |
   >
   > 前两条与 mmap 那条是同一个解：把 slice 层移进 sink/source，于是"一个逻辑文件项"本身就是合并结果，`ReadEntryToLocalFile` 直接够用。代价是读写两侧必须一起定，不能先冻结写侧。第四形态是新问题——它要求这套流程承认一种"不搬运字节，只搬运位置"的产物，这在当前 `Artifact`/`Loader` 的语义里没有位置。
   >
   > 另有一处没有所属组件：`CleanLocalData`（`indexbuilder/VecIndexCreator.cpp:123` 调用）——`FileSink` 到 `Finish()` 为止不知道 staging 目录的存在。

2. **查询接口不共享，明确写出**：vector 查询接口与 scalar 各索引类型并列（§11.3），不设计任何跨索引类型的查询接口。
3. **`IndexBase` 在阶段 1 内退役**：vector 同阶段迁移，不需要 adapter 过渡。顺序：立共享基类与 Loader → scalar 各索引类型迁移 → vector 重新划分 → 删 `IndexBase`。`CacheIndexBasePtr` 的句柄角色由 `IndexReaderBase` 接替。出口清单必须含 growing 侧：`FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 也返回 `PinWrapper<index::IndexBase*>`（`FieldIndexing.h:128,131`），以及第三个出口 `FieldIndexing::has_raw_data()`（`FieldIndexing.h:174`，直接调 `IndexBase::HasRawData()`，经 `IndexingRecord::HasRawData` 被 `SegmentGrowingImpl.cpp:409,764,1032,1997,2078` 五处消费），见 [§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)。
4. **工厂按索引类型拆分**：`CreateIndexInfo` 拆散，索引类型级的 loader/builder registry 取代 `IndexFactory` 的巨型分派 switch。
5. **knowhere 逐出标量路径**：标量各索引类型的接口与实现零 knowhere include；`BinarySet` 只出现在 vector 索引类型的 Loader/Artifact。收益：标量索引编译隔离、knowhere 升级不再重编全部标量索引。
6. **storage 流程收到 Loader/Artifact 边界之后**（两类索引同规则），17 个头的 `FileManager` 引用缩到各索引类型的 Loader/Artifact 实现文件内。

### 11.3 vector 按四种接口重新划分（不重设计）

vector 纳入阶段 1 的范围是按接口重新划分：把现有 `VectorIndex` 的 virtual 方法按四种接口重新安放，knowhere 交互逻辑原样搬进实现，不改行为，基准照跑。对象不止 sealed 侧的 `VectorIndex`，还包括 growing 侧的 `VectorFieldIndexing`（[§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)）。

```cpp
// 查询接口（与 scalar 各索引类型并列）
class VectorSearchReader {
 public:
    virtual void Search(const DatasetPtr&, const SearchInfo&,
                        const BitsetView&, OpContext*, SearchResult&) const = 0;
    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr&, const knowhere::Json&, const BitsetView&,
              OpContext*) const = 0;
    virtual bool RefineEnabled() const = 0;
};

// 反查接口（GetVector / HasRawData 对应物）
class VectorValueReader { /* GetVector 系 */ };

// Appender 接口（growing interim 索引）：与 GrowingScalarIndex<T> 并列，
// 语义相同（快照 + 已提交行数），签名同形——类型间只差一个 dim（dense 恒为 schema dim，
// sparse 为本批次 dim）。dense/sparse 不再是两个方法，理由见 §7.1。
class GrowingVectorIndex {
 public:
    virtual void Append(int64_t reserved_offset, size_t n,
                        const void* data, int64_t dim, const bool* valid) = 0;
    virtual std::shared_ptr<const VectorSearchReader> ReaderSnapshot() const = 0;
    virtual int64_t CommittedRows() const = 0;   // 阈值未达时快照为空
};
```

四种接口对两类索引统一（Reader / Appender / Builder / Loader），差别只在接口内的方法按索引类型分：

| 接口 | scalar | vector | 关系 |
|---|---|---|---|
| Reader | §5 的各查询接口 | `VectorSearchReader` / `VectorValueReader` | 形态同、内容不共享（§11.2 第 2 条） |
| Appender | `GrowingScalarIndex<T>` | `GrowingVectorIndex` | 语义共享（快照 + 已提交行数），签名同形、仅差一个 `dim` 位；前提是冷启动全量建先迁出到 Builder 接口（[§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)） |
| Builder | `IndexBuilder<T>::Add` + `Seal()` | 同一个接口；knowhere 内存索引落 B+ 档、DiskANN 落 D 档 | 接口统一，差异收进 `BuilderInputSpec`（[§6.1.1/6.1.2](#611-输入形态五档两类索引交错)） |
| Loader | 同一套 | 同一套 | 完全同构 |

> **Builder 接口为什么可以共享，而 Reader 接口不能**。Reader 接口不共享是因为查询语义完全不同（`In/Range/bitmap` vs `Search(dataset, SearchInfo)`），没有可共享的抽象；Builder 接口共享是因为"输入数据 → 构建完成 → 产出 Artifact"两类索引确实同构，真实差异只在输入形态，而输入形态横切两类索引（[§6.1.1](#611-输入形态五档两类索引交错) 的五档里，标量自己就占三档），因此按索引类型切是错的切法，按形态声明才是对的。
>
> 实现时先验一条：`BuilderInputSpec` 的三种 form（Streaming / Contiguous / LocalFile）加 `needs_second_pass` 与 `side_inputs`，是否装得下 knowhere 全部索引类型。装不下就补声明位，而不是给 vector 开一个独立的 Builder 接口。

## 12. 待定问题

每条给出四项：问题（不确定的究竟是什么）、选项、判断标准（拿什么证据结案）、决定时限（什么时候必须定、晚了代价是什么）。

### 12.1 vector 查询接口的细化

三个彼此独立的子问题，其中 (b) 可能是真实缺陷，应最先查。

**(a) `SearchInfo` 不用拆——已结案，剩下的是一条 include 链。** 判断标准（列出 `index/` 实际读了哪些字段）执行后的结果：

`index/` 的全部引用只读四个字段——`search_params_`、`metric_type_`、`topk_`、`trace_ctx_`（`VectorIndex.h:173-186` 的 `PrepareSearchParams`、`VectorMemIndex.cpp:732`、`VectorDiskIndex.cpp:685,720`、`index/Utils.cpp:518,530,538`）。它从不读 `array_offsets_`、`active_count_`、`group_by_field_ids_`、`iterative_filter_execution`、`iterator_v2_info_`、refine 比例等任何一项。

两个连带结论：

- "索引认识 segment 与 executor"这条危害是潜在的、不是实际的。结构体宽不等于依赖宽——它是按 `const&` 传的纯数据，多余字段既不建立依赖边也不被读。
- 同时证伪了一个更值得担心的猜测：既然索引从不读 `array_offsets_`，元素级到行级的聚合今天就不在索引内部发生，与 [§5.8](#58-nested元素级索引坐标与投影) 的判断一致，不存在冲突。

因此不做"按语义劈成两个结构体"这种改动——收益只剩防御性的（防止将来有人在索引里读 `active_count_`），代价是每个调用点构造两个对象并分别穿线。正确形态是本文档在别处一直用的接口缩小范围：vector 查询接口声明自己的最小参数类型（那四个字段），`SearchInfo` 保持为 exec 自己的聚合体，在调用点投影过去。这不是"拆 SearchInfo"，是"接口自带参数类型"。

**但有一条真实成本，且它来自 include 而不是字段职责混杂**：`common/QueryInfo.h:26` 包含 `knowhere/config.h`，而 `index/Utils.h` 包含 `QueryInfo.h`，**几乎所有标量索引类型的实现文件都包含 `index/Utils.h`**（`BitmapIndex.cpp`、`ScalarIndexSort.cpp`、`StringIndexMarisa.cpp`、`StringIndexSort.cpp`、`InvertedIndexTantivy.cpp`、`FMIndex.cpp`、`RTreeIndex.cpp`、`NgramInvertedIndex.cpp`、`HybridScalarIndex.cpp`、`ScalarIndex.cpp`、`bson_inverted.cpp` 等）。所以 [§11.2 第 5 条](#112-处理决定)"标量索引类型零 knowhere include"今天是被这条链破坏的，而不是被某个标量索引直接 include 破坏的（标量索引类型的头文件自身 knowhere 计数全为 0，只有 `index/Index.h` 有 3 处）。

修法是断链，不是拆结构体：最小参数类型（含 `knowhere::Json`）声明在 vector 索引类型自己的头里——按 §11.2 第 5 条 vector 索引类型本就可见 knowhere；`common/QueryInfo.h` 与 `index/Utils.h` 都不再需要 knowhere。

**但断这条链不足以达成"标量索引类型零 knowhere"，还有一条更大的洞：`common/Types.h` 自己。** 它在 `:27-34` 直接 include 了 knowhere 四个头与 pb 三个头，而 `TargetBitmap`/`DataType`/`FieldId` 只在这里 alias——任何接口头只要用 `TargetBitmap`，就传递性拉进 knowhere 与 pb。这意味着 §10 规则 6 在头文件层面不拆 `common/Types.h` 就无法真正达成，而拆它被[总览 §9 第 4 条](README.md#9-待定问题)明确推到"阶段 3 后独立评估"（33 个 include、被 195 个生产文件 include，触碰全仓）。

这是设计里一处未解决的矛盾：阶段 1 能做到的是"标量索引类型不新增、不直接 include knowhere，且断掉 `Index.h` 与 `Utils.h` 两条自造链"；"零 knowhere"这个措辞在 `Types.h` 拆分前只对直接 include 成立。规则 6 的验收定义按这个缩小，不写成传递闭包为零——那是做不到的承诺。

**(b) iterator 生命周期——已确认是现状问题，阶段 1 不修。**

链路逐段核实过：

1. `SearchOnSealedIndex` 中 `auto accessor = SemiInlineGet(entry.indexing_->PinCells(op_context, {0}))` 是函数局部（`query/SearchOnSealed.cpp:88`）。
2. `PrepareVectorIteratorsFromIndex` 取出 `knowhere::IndexNode::IteratorPtr`，并取 `&index.GetOffsetMapping()`——一个指向索引对象内部的裸指针（`exec/operator/Utils.h:129-134`）。
3. 两者一起装进 `SearchResult::vector_iterators_`（`common/QueryResult.h:305`）；`ChunkMergeIterator::offset_mapping_` 就是那个裸指针（`QueryResult.h:252`），没有任何 pin 随之保存。
4. 消费发生在后续算子：`SearchGroupByNode.cpp:89`、`IterativeFilterNode.cpp:127`、`IterativeElementFilterNode.cpp:117`。

**判断：这条完全由现状代码构成，不由本重构引入或放大，因此阶段 1 不动它。** vector 按 [§11.3](#113-vector-按四种接口重新划分不重设计) 的"原样搬移"原则重新划分，iterator 接口保持现状形态（裸 `IteratorPtr`），不预先改成自带 pin 的句柄——那等于在没有确认缺陷的前提下先付接口复杂度。

仍需记住两点：其一，是否真会悬垂取决于 cachinglayer 在 pin 释放后能否在同一查询内淘汰该 cell，这个审计没做，所以本文档不断言它是缺陷；其二，仓库里有同类先例——`SearchResult::PinBitset`（`QueryResult.h:309`）与 `query/SearchOnSealedIndexBitsetLifetimeTest.cpp` 就是为 bitset 的同类问题补的锚。若将来审计确认可淘汰，届时的修法是给 iterator 补同样的锚，而不是回头重开接口形态。

**(c) knowhere 类型能否出现在 vector 接口里——已决定：允许。**

`VectorIndex::VectorIterators` 的返回类型是 `knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>`（`index/VectorIndex.h:78`），直接穿透到 `query/`。决定是允许它继续穿透：vector 索引类型与 knowhere 的绑定是既成事实，包装一层只在"换引擎或多引擎并存"时才有价值，而那个需求今天不存在，不为它付包装成本。

因此 [§11.2 第 5 条](#112-处理决定)的边界就是最终边界：knowhere 类型在 vector 索引类型内自由出现，在共享基类与标量索引类型内一处也不许有。这条同时确认了 [§11.3](#113-vector-按四种接口重新划分不重设计) 的接口框架够用——vector 不需要独立的 `02-vector-index.md`，除非实现时出现别的结构性问题。

### 12.2 产物的构建与加载流程放在哪个组件、叫什么

**先把名词说清楚。** 索引建完之后有一套固定流程：把索引落成文件 → 上传到对象存储；查询节点再把文件下载下来 → 打开成可用对象 → 把它占的内存/文件字节数报给缓存层做计费与淘汰。这套"落盘 → 上传 → 下载 → 打开 → 计费"就是下文说的流程；被搬来搬去的那个东西（一组文件，或一份内存结构）就是产物（artifact）。今天这套流程实现在 `IndexBase` 上（`Serialize`/`Load`×2/`Upload`/`LoadUnified`/`UploadUnified`/`CellByteSize`），每个索引类都得实现一遍——这正是 [§2.1](#21-结构事实) 说的"构建、持久化、查询、缓存计费混在基类里"。

L1 指[总览 §3 目标分层图](README.md#3-目标分层图)里的第 1 层，也就是 `storage` 与 `columnar-format` 所在的那层（L0 基础设施 → L1 字节与列 → L2 index → L3 segcore → L4 plan/exec → L5 应用服务 → L6 capi）。"移到 L1"的意思是：把这套流程从索引组件（L2）挪到字节与列那层（L1），因为它不是索引专属的——JSON shredded 布局同样是"离线构建、落盘、按需加载、参与缓存计费的派生产物"，需要同一套东西，而它在 L1。留在 L2 就只剩两条路：要么让 L1 反过来依赖 L2（分层规则禁止），要么让每个非索引使用者把这套逻辑手写一遍（`BsonInvertedIndex` 今天就是这么干的）。

**放在哪个组件。** 选项 ① 放进 `storage`；② 新立一个 L1 小组件（如 `artifact/`），只装 `Artifact`/`ArtifactLoader`/`LoadedArtifact`/`ArtifactStats`。

① 的有利事实：索引产物的字节侧今天就已经在 `storage/`——`IndexData.{h,cpp}`、`IndexEntryReader/Writer.h`、`IndexEntryDirectStreamWriter`、`IndexEntryEncryptedLocalWriter`、`MemFileManagerImpl`、`DiskFileManagerImpl` 全在那里（`storage/` 约 25.5k 行生产代码）。下移不是搬入新目录，是把已在那里的东西补齐成一套接口。

- 顾虑：storage 变成第二个没有明确职责的组件。
- 判断标准不看行数，用 [README §6.3](README.md#63-app-不成为无明确职责组件的判断标准) 给 app 定的同一条：是否按数据形态选算法。`Artifact`/`Loader` 是"谁在什么时候把字节变成对象"的编排，不按数据形态分支 ⇒ 进 storage 合理。反过来，若第一版接口写出来发现这套流程必须认识"这是 tantivy 目录 / knowhere BinarySet / DiskANN 大文件"才能工作，说明它不属于纯字节世界，改走 ②。这个判断标准在写第一版 `Artifact` 接口时即可验证。

**阶段 1 内验不了第二个消费者。** 下移的核心论据是"这套流程不是索引专属的，JSON shredded 布局要用同一套"（[§11.2 第 1 条](#112-处理决定)）。但 [§1 的过渡处理](#1-范围)已定：阶段 1 内 `JsonKeyStats` 迁到 `segcore/json_stats/` 并明确不接 L1 产物的构建与加载流程，继续手写自己的 `Build`/`Serialize`/`Upload`/`Load`。也就是说阶段 1 结束时这套流程只有 index 一个真实消费者，"两个消费者"的论据要到宽表建模实现后才能被验证。这不推翻判断（终局所属组件没变），但意味着接口形状不能只按 index 的需要收拢——第一版就要拿 shredded 布局的加载路径（`ManifestGroupTranslator` → `ChunkedColumnGroup`）在第二个使用者出现之前先按接口文本做一次对照，确认它装得进 `Artifact`/`Loader`，否则将来第二个消费者接入时会发现这套流程是照索引的形状建的。

**这条前置若不能实现，硬规则 4 必须改口。** [README §5 规则 4](README.md#5-全局硬规则) 现在写的是"不得依赖 cachinglayer 的类型"，而 `LoadedArtifact::CellByteSize()` 的返回类型正是 `ResourceUsage`。只有在它真的挪出 cachinglayer 之后，这句话才与本设计自洽；挪不动就必须缩小那句措辞的范围，否则 lint 与设计自相矛盾。另外改动范围目前只量了文本出现次数（milvus-common 10 文件 / 123 处，milvus 36 文件 / 111 处），没有编译验证拆头之后 `StorageType`/`FormatBytes` 的连带范围——排期前要补这一步。

**命名。** 所属组件定了再一次改完，避免两轮。`Index` 前缀下移后失效（JSON shredded 布局要用同一套，而它不是索引）。候选 `Artifact`/`ArtifactLoader`/`ArtifactStats`；但要连带看 `storage/` 里既有的 `IndexData`/`IndexEntry*` 命名，否则会出现 `Artifact` 与 `IndexEntryWriter` 并存的命名不一致，等于只改了一半。

**跨仓前置（阶段 1 时间线上唯一一条不在本仓内的）。** `ResourceUsage` 今天定义在 `cachinglayer/Utils.h`，而 cachinglayer 属于外部仓 milvus-common。[§11.2](#112-处理决定) 已决定它移到 milvus-common 的 `common/ResourceUsage.h`（`namespace milvus`），必须先于流程下移实现，需单独排期与跨仓协调。

### 12.3 `cell_size_` 的计量方式没有定义

问题不是"有两个访问器不整洁"，而是同一个字段被不同索引类型按不同计量方式填，而缓存层拿它做准入与淘汰。

`cell_size_`（`Index.h:156`，经 `SetCellSize` 灌入、`CellByteSize()` 读出）是 cachinglayer 唯一的计费依据。它今天的来源有两种，含义完全不同：

| 填法 | 值的含义 | 谁这么填 |
|---|---|---|
| `SetCellSize({index_load_info_.index_size, 0})` | 压缩前的索引文件大小（`segcore/Types.h:58` 的注释原话：_It's the size of index file before compressing_） | `V1SealedIndexTranslator.cpp:157-161,198-204`、`SealedIndexTranslator.cpp:199` —— 绝大多数索引类型走这条 |
| `SetCellSize({index->ByteSize(), 0})` | 实测的常驻内存占用（子类 `ComputeByteSize()` 算出） | `TextMatchIndexTranslator.cpp:125,127`；`FMIndex.cpp:713-714` 取回估算、把 memory 半边换成实测、保留 file 半边再塞回去 |

于是：大多数标量索引类型报给缓存层的是远端文件大小，text match 与 FMIndex 报的是实测内存，而 `RTreeIndex` 一次都没调过 `SetCellSize`——它的 `cell_size_` 恒为 `{0,0}`，等于向缓存层报告自己不占资源。所以计量方式不是两套，是三套，第三套是"没有"。对 marisa trie、roaring bitmap、tantivy 这类序列化形态与常驻形态差别很大的索引类型，前者系统性地偏；而两个分支只是把同一个数字放进 memory 半边还是 file 半边（看 `enable_mmap`），并没有换算。计量方式按索引类型不同，意味着内存核算的偏差方向也按索引类型不同——这不是整洁问题，是缓存准入与淘汰的准确性问题。

`ByteSize()` 也不是一个独立的公开概念：全仓 5 处非测试消费者里，4 处是为了算 cell size（`TextMatchIndex.h:129-131`、`TextMatchIndexTranslator`、`FMIndex`）或索引类型内部聚合子索引（`HybridScalarIndex.h:172`、`StringIndexSort.cpp:570`、`RTreeIndex.h:170`），唯一独立的消费者是 `SegmentGrowingImpl.cpp:602` 的 growing 内存上报——而那正好是 `Index.h:130-135` 注明"growing 不更新缓存值"的场景，见 [§13.3](#13-被本重构触发的已有缺陷)。

- 因此要做的不是"合成一个访问器"，而是给 `cell_size_` 下一个定义，并让所有索引类型按同一计量方式填。选项：① 计量方式 = 实测常驻占用（`ByteSize()` 成为它的实现，文件大小只在"还没加载完、必须先报预算"的时刻用，做成 translator 侧的自由函数 `EstimateBytes(load_meta)`，不上索引对象）；② 计量方式 = 分级存储的占用量（内存 + 文件两半各有定义），文件大小与实测各填各的半边，但必须写成显式规则而不是按索引类型人工核对。
- 判断标准：cachinglayer 到底按这个数做什么。若只做已加载对象的淘汰权重，① 足够；若还用于加载前的准入/预留，那"加载前的估算"必须存在，但它属于 translator 的输入元数据、不属于索引自述，① 依然成立。
- 决定时限：不阻塞阶段 1 开工，但必须先于产物的构建与加载流程下移（[§12.2](#122-产物的构建与加载流程放在哪个组件叫什么)）。`LoadedArtifact` 要负责 `CellByteSize()`，把一个没有定义的计量方式搬进 L1 之后再改，动的就是 L1 的公共接口、且波及 columnar-format。

### 12.4 JSON path cast 的类型词汇表

[§1 判断](#1-范围)把 shredding 整体划归 columnar-format 后，寻址单位已由 [§5.7](#57-jsonindexreaderpath-寻址的谓词索引) 定为 `(field, path)`：逐 path 的 cast index 以 `(field, path)` 为键注册在索引清单里，`Resolve` 返回类型擦除基类、消费者自行做跨继承树的 `dynamic_cast`——非 json 索引类型的接口也可以经这条路返回，不需要改签名。剩下的是一个更具体、且今天就能验的问题：cast 的类型词汇表装不下将来的目标类型。

事实：今天的 cast 词汇表是 `JsonCastType`（`common/JsonCastType.h:25`），一个闭合的六值枚举 `UNKNOWN / BOOL / DOUBLE / VARCHAR / ARRAY / JSON`，外加 ARRAY 的一层 `element_type_`。它有 `ToTantivyType()`——这个词汇表是按 tantivy 的类型系统裁的，不是按 Milvus 的 `DataType` 裁的（后者有 `GEOMETRY = 24`，`common/Types.h:85`）。

而[宽表建模](https://zilliverse.feishu.cn/wiki/G9RIwzFwwiYdm4k1WlGcciBSnff)「二十、后续功能」第 6 条设想 cast 成 geo / timestamptz / ref-mode LOB。geo 的 cast 没有对应的 tantivy 类型——它该落到 RTree，不是倒排。所以这不是给枚举加两个值的事，它推翻了"cast type ⇒ tantivy type"这条隐含前提。

- 一个已经存在的不一致：[§5.7](#57-jsonindexreaderpath-寻址的谓词索引) 的 `Resolve(std::string_view path, DataType cast_type)` 写的是 `milvus::DataType`，而现状代码（`IndexBase::GetCastType`）用的是 `JsonCastType`。两者不等价——前者能表示 GEOMETRY，后者不能。写 `DataType` 等于已经选了下面的选项 ①。
- 选项：① 词汇表升级为 `milvus::DataType`（或其子集），cast 目标与普通列的类型系统统一，"哪些 cast 有索引支持"由各索引类型的 builder registry 回答；② 保留 `JsonCastType` 作为"倒排可索引的 cast"专用词汇表，geo 之类走另一条路（按 path 建独立的空间索引，不走 cast 概念）。
- 判断标准：cast 出来的子列是不是一等列——有自己的 FieldId、能独立加载、能像普通列一样建索引。若是，① 成立，且 `(field, path)` 这条路对这些 path 不再需要（它就是根普通列，走普通索引注册）；若 cast 结果仍依附在 JSON 字段下、只在查询时按 path 解析，② 更省事。这个问题属于宽表建模，与[总览 §9 第 5 条](README.md#9-待定问题)（嵌套的查询节点数据表示）同源，不是本文档能单方面决定的。
- 决定时限：不阻塞阶段 1（今天没有 geo cast）。但阶段 1 内只要动到 `Resolve` 的签名就必须先答，否则是把一个未定的类型系统写进接口；写 `JsonCastType` 是保守取值，写 `DataType` 是提前选定。

### 12.5 exec 聚合算子的所属阶段

[§5.8](#58-nested元素级索引坐标与投影) 把"把聚合规整成一个显式的元素到行的聚合算子"判给了 [阶段 4](README.md#7-阶段计划)，阶段 1 只保证索引侧交付干净的元素级结果。风险在于：该算子是阶段 1 索引侧改动的直接下游，而 exec 今天是三份分散实现（`JsonContainsExpr.cpp:2469` 逐行聚合 / `UnaryExpr.cpp:743` 逐元素反查 / `Expr.h:2205` 区间切片）。

- 风险的实际大小取决于一件事：阶段 1 是否让更多索引类型产出元素级结果。若只是把现状的 `is_nested_index_` 模式位形式化为 `CoordDomain()`、不新增 nested 索引类型，那么阶段 1 到阶段 4 之间没有新的不一致，判给阶段 4 是对的；若阶段 1 顺手把某些索引类型改成元素级输出，三份分散的聚合实现就会各自遇到没覆盖过的组合。
- 判断标准：阶段 1 的索引类型清单里有没有"新获得元素级输出"的类型。这个在 [§8 映射表](#8-现有实现类--新接口映射)定稿时就能答。
- 决定时限：阶段 1 的索引类型清单定稿时。答案是"有"就把该算子提前到阶段 1 末尾，不等阶段 4。

### 12.6 growing 已提交行数滞后策略的索引类型清单

[§7](#7-growing-接口) 已定策略按索引类型分、策略表放 segcore：text match 允许滞后（不补齐），其余类型回退列扫描补齐。剩下的是清单本身——`NgramReader` 与 `JsonFlatIndex` 归哪边没定。

两者都是 tantivy 系、都有和 text match 相同的 commit/reload 时滞，从机制上更像"允许滞后"那一档；但它们服务的是普通谓词（`LIKE`、JSON path 比较），用户对"刚写入的数据立刻可查"的预期与全文检索不同。这是产品语义问题，不是技术问题——机制上两种都做得到。

- 判断标准：产品侧对 `LIKE` / JSON path 谓词在 growing 段上的可见性承诺。
- 决定时限：阶段 1 实现 `GrowingScalarIndex` 时。不定的后果不是设计返工（索引侧不设任何表示该策略的位），而是 segcore 的策略表少两行、这两类默认落到"回退列扫描"——保守但可能浪费。

## 13. 被本重构触发的已有缺陷

与 [§12](#12-待定问题) 性质不同：这些不是待决定的设计问题，而是现状已经存在、且被本重构放大或直接触发的缺陷。它们必须进阶段 1 的验收标准——重构可以不修其中某一条，但不能在不知情的情况下把它放大。

**13.1 growing 标量索引会触发 `size_per_chunk_` 越界（issue #51237 同型）。**

`Expr.h:2239-2253` 的注释已经明确写出触发条件（有两个触发条件，不是一个：注释原话是 _a scalar field gains an interim index on growing, or geometry is routed through `ProcessIndexChunks`_——而 geometry 恰好就是今天唯一在跑的 growing 标量索引，见 [§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)）：缓存的 index bitmap 是段全局的（标量索引恒为单 chunk），而 `size_per_chunk_` 是原始数据的 chunk 粒度（`segcore.chunkRows`），两者无关；sealed 段上二者恰好相等，所以至今没有触发，而今天只有 sealed 段能走到这里，因为 growing 段上 `HasIndex()` 只对 vector/geometry 为真。注释原话：_The moment a scalar field gains an interim index on growing … `size_per_chunk_` would over-run the bitmap exactly as in issue #51237._

[§7](#7-growing-接口) 的 `GrowingScalarIndex` 统一接口正是那个触发条件，本阶段直接触发它。阶段 1 实现 growing 标量索引的同一个 PR 里必须先修这处边界，或至少让 growing 标量索引走一条不经过该分支的路径。

**13.2 vector iterator 的 pin 悬垂（见 [§12.1(b)](#121-vector-查询接口的细化)）。**

放在这里是因为它同属一类：现状已存在的疑似缺陷，而接口形状取决于它的结论（裸 `IteratorPtr` vs 自带 pin 的 `Pinned<VectorIterator>`）。审计结论若是"是缺陷"，它应当独立于重构先修，再按修好的形态写进接口。

**13.3 growing 段的字节计费用的是过期缓存值。**

`Index.h:130-135` 明写 `ByteSize()` 返回的是 `ComputeByteSize()` 算出的缓存值、growing 段持续插入时不会自动更新，并声明该方法"仅为 sealed 段设计"。但 growing 索引同样参与 cachinglayer 计费。[§7](#7-growing-接口) 把 growing 索引统一成一等接口后，走这条路径的索引类型会变多，误计费的范围随之扩大。

这与 [§12.3](#123-cell_size_-的计量方式没有定义) 是同一处代码的两个不同问题：12.3 是"计量方式没有定义"的设计问题，13.3 是"其中一套在 growing 上本来就不准"的正确性问题。统一计量方式时必须同时解决后者。
