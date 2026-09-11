# 阶段 1：Index 接口拆分（标量各索引类型 + 标量/向量公共接口）

> **状态：设计提案，讨论中。** 返回 [总览](README.md)。
> 现状事实基于 master `e255009e01`。本文档定义 index 组件的目标接口：标量索引各类型的完整接口定义，
> 以及标量/向量公共接口（Artifact/Reader、静态 family loader、工厂、storage 流程）的处理。向量索引按接口重新划分
> 也在阶段 1 范围（§11），但其 knowhere 交互不做重设计。

## 1. 范围

- **纳入**：常规标量索引（inverted / bitmap / sort / marisa / hybrid）、模式匹配（FMIndex、inverted 的 LIKE 系列）、TextMatch（全文）、Ngram、Geometry/RTree（空间谓词，见 §5.6）、JSON path 索引类型（json path cast index、`JsonFlatIndex`）、growing 增量索引；标量/向量公共接口的清理与向量索引按四种接口重新划分（§11）——Artifact/Reader 分离、静态 family loader/IO、工厂按索引类型拆分、`IndexBase` 阶段内退役。已确认无生产构造者的 Tantivy growing scalar 实现已删除，不把删除项重新列为当前能力。
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
   | **Builder** | indexbuilder 服务、segcore 加载时构建 | 独立对象 | 一次 `Build(input)` 返回完成的 Artifact |
   | **Loader** | segcore load | family 注册的静态 `DeriveCaps`/`Open` 函数对 | 无状态，不创建 loader 对象 |

   任何一类调用方拿不到其他接口的方法。Builder 与 Appender 是两个接口而非一个：前者一次性、独占、以产出 Artifact 结束；后者长期存活、与读并发、只产出按已提交行数截取的快照而不直接持久化（growing 的持久化走 flush 路径）。把两者合一正是 `TextMatchIndex` 四个构造函数挤在一个类的原因（§2.2）。
2. **组合替代继承**。tantivy 封装、marisa、FM 结构是被组合的引擎，不是基类。实现类之间禁止继承。

   > **仓库里已有正例**：`BsonInvertedIndex`（`json_stats/bson_inverted.h:42`）是个裸类，不继承 `IndexBase`/`ScalarIndex`/`InvertedIndexTantivy`，直接持有 `shared_ptr<TantivyIndexWrapper>` 并调 `term_query_i64`。同一个 tantivy 引擎，`TextMatchIndex`/`Ngram`/`JsonFlat` 靠继承复用、它靠组合——后者才是本阶段要推广的形态。代价是它把生命周期接口（`AddRecord`/`BuildIndex`/`LoadIndex`/`UploadIndex`/`CellByteSize`）手写了一遍；接口拆分后这部分由共享 Builder 与静态 family loader 机制承接，组合的好处保留、重复消失。
3. **能力描述，不许 throw**。每个 reader 携带能力描述符；不支持的操作在类型上就不存在，或经 `std::optional` 表示。
4. **模板留在热路径，类型擦除只在管理接口**。exec 的表达式本就按值类型模板化，typed reader 无装箱开销；索引清单持类型擦除基类，`IndexReaderBase` 到查询接口的 downcast 每个表达式节点一次（随 pin 获取），不在 batch 或行的粒度上发生（§4.3）。
5. **不认识 segment 与 executor**。构建输入是完整的原生数据视图或已准备的本地文件；查询输出是 bitmap / 值，坐标一律落在索引自己的坐标系里——行级索引给行号，元素级（nested）索引给元素号。元素→行的投影不属于索引：元素级到行级的聚合时机由查询语义决定，只有 plan 知道，见 [§5.8](#58-nested元素级索引坐标与投影)。
6. **IO 注入**。索引类不持有 `FileManagerContext`；文件读写经注入的 sink/source 接口，只在 family 静态 `Open` 与 Artifact 序列化实现内出现。

## 4. 接口总览

`index/contracts` 按职责分为 `query/`、`build/`、`load/`、`growing/`；注册入口 `Registry.h` 保留在根目录。只调整文件组织，不增加 namespace 层级，也不保留旧路径转发头。生产代码的注释和组件 README 自包含，不引用这组辅助设计文档；注释保留借用输入、pin、后备文件等非显然约束，不复述 `unique_ptr` 本身的独占语义。

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

ArtifactBuilder<Input>（以完整输入类型模板化）          → Build(input) → Artifact
GrowingIndex + Appendable<Batch>                       → typed Append + 同代 GrowingIndexSnapshotPin
StaticLoaderProvider::DeriveCaps/Open                  → metadata caps / 文件 → Reader，IO 注入
```

一个实现类可以同时提供多个查询接口（inverted 同时是 `ScalarPredicateReader<T>` 和 `PatternMatchReader`），通过接口多继承声明，而非通过实现继承获得。

> **查询接口不继承 `IndexReaderBase`**。若每个查询接口都 `virtual public IndexReaderBase`，多继承时为保证基类子对象唯一必须用虚继承，代价是实现类访问基类成员多一层间接、且 `IndexReaderBase` 到查询接口只能走虚基类的 `dynamic_cast`。改为纯 mixin 后：实现类非虚继承 `IndexReaderBase` + 各查询接口，`IndexReaderBase` 到查询接口是一次跨继承树的 `dynamic_cast`，查询接口本身也不需要基类的元数据（那些由索引清单持有，见 §4.3）。

### 4.1 能力描述符

```cpp
// index/contracts/query/ReaderCaps.h
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

exec 的执行路径决策（`DetermineExecPath`）只消费这个结构，不 pin、不 try-catch。选定条目并取得根 Reader 后，消费者再 checked-cast 实际需要的查询接口；metadata 能力与 Reader 实际接口不一致属于错误或明确 fallback，不能靠预 pin 的描述符替代接口检查。segcore 的 `FieldIndexCapability`（segment 级“某 field 有什么索引”）由索引清单聚合各索引的 `ReaderCaps` 得到——单索引能力描述归 index，segment 级聚合归 segcore。

> **聚合是条目列表，不是按位 OR。** 同一 field 上可以同时存在 ngram（`exact = false`）与 inverted（`exact = true`），把两者的 `ReaderCaps` 按位或起来会得到一个不对应任何真实索引的描述符——exec 据此选执行路径就会拿着"精确"的假设去用候选类型的结果。正确形状是每个索引一条条目、各带自己的 `ReaderCaps`，选执行路径时先选条目再读 `ReaderCaps`。

> **执行路径选择规则：有可用索引就走索引，没有才回退列扫描。** shredding 迁出 index 后，"同一 path 既有 path 索引又有 shredded typed 子列"不再是两个索引之间的选择，而是索引 vs 列扫描的普通判定。typed 子列扫得快不快只影响 columnar-format 内部怎么扫，不上升为 exec 的执行路径选择输入——因此列侧 `ColumnCaps` 与 `FieldIndexCapability` 的合流不是阶段 1 的前置。
>
> 已知的次优情形：分级存储下拉起一个未加载的索引 cell 可能贵过扫一根已在内存的 typed 子列。这属于 cost model 层面的后续优化，不改执行路径选择的接口。

> **`ReaderCaps` 必须能在不 pin 的前提下读到**，因此它由加载期元数据（索引类型 + 构建参数，如"VARCHAR 上的 inverted"⇒ predicate + pattern_match + value_lookup）算出，作为纯数据存进索引清单条目，不是必须持有索引对象才能调用的虚函数。理由见 §4.3：路径决策发生在 pin 之前，若读 `ReaderCaps` 需要对象，分级存储里未加载的索引会被无谓拉起。
>
> reader 上仍保留 `Caps()` 用于能力描述（growing 快照、单测需要），但它是一致性校验对象而非查询期来源：pin 后的 `reader->Caps()` 必须等于索引清单缓存的 `ReaderCaps`，这条写成断言与测试。

### 4.2 类型擦除基类

```cpp
// index/contracts/query/IndexReader.h
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
    virtual int64_t     MemoryUsage() const = 0;
    virtual cachinglayer::ResourceUsage CellByteSize() const = 0;
};

}  // namespace milvus::index
```

`CellByteSize()` 留在 Reader 基类，报告该已打开 Reader 实际拥有或明确标注为 unavailable-zero 的 heap/file-backed 资源；`MemoryUsage()` 保留现有查询侧语义。加载前 admission、translator 的 final estimate 与 fallback 仍属于各自 translator，不能塞进 Reader 或与已加载资源混为一谈。旧 `SetCellSize`/`ComputeByteSize` 可变管道与没有生产写入者的 `loaded_resource_estimate` 已删除；`Upload` 也不在 Reader 上，上传属于 indexbuilder 服务。

### 4.3 对象模型：谁创建、谁持有、何时 pin

> Reader 接口按查询语义拆成若干查询接口（`ScalarPredicateReader<T>`、`PatternMatchReader` 等纯 mixin）。sealed 侧只暴露一个根 `IndexPin`；消费者在 pin 的生命周期内从根 Reader checked-cast 所需 mixin，不为每个接口复制 pin 类型或出口。

查询接口不是每次查询新建的 proxy——它就是索引对象本身的一个接口视图。三类东西的创建时机完全不同：

| 对象 | 创建时机 | 生命周期 | 成本 |
|---|---|---|---|
| 实现类实例（`InvertedIndex<int64_t>` 等） | family 静态 `Open()`，即索引加载时 | 长期存活；仅在被 cachinglayer 淘汰后重新加载时重建 | 一次加载 |
| `ReaderCaps` | 索引清单构建时由加载期元数据算出 | 与索引清单条目同寿 | 纯数据拷贝 |
| `IndexPin` 句柄 | 取用时 | 栈上 move-only RAII：共享既有 `CellAccessor` lease + 借用根 Reader 指针 | 一个 shared accessor + 一个指针 |

索引清单持有的是 `CacheSlot`，不是索引对象。现状即 `CacheIndexBasePtr = shared_ptr<CacheSlot<IndexBase>>`（`Index.h:165`），阶段 1 后为 `CacheSlot<IndexReaderBase>`。Segment 的清单与运行状态快照可以通过 `shared_ptr` 保持 slot 的生命周期；slot 内部以 `unique_ptr<IndexReaderBase>` 独占 Reader。family `Open` 将唯一所有权移交给 slot，查询使用者只拿 pin/accessor 与类型化非拥有指针。索引对象活在 slot 内、可被淘汰，因此取查询接口必须先 pin 后 cast——pin 之前对象可能根本不存在。

**不增加 `IndexReaderCell` 或虚 `PinLifetime`。** 外部缓存的 Translator 接收 `unique_ptr<CellT>`，Reader 工厂直接满足这个所有权契约；cell 唯一拥有 Reader。`IndexPin` 共享已经存在的 `CellAccessor<IndexReaderBase>` 控制块并借用 Reader，不建立 Reader 的共享所有权。move 后源 pin 为空；deferred consumer 调用 `IntoSharedLifetime()` 时转移的仍是同一个 accessor，不再分配一层 wrapper。用户持有 pin 时缓存不能淘汰对应 Reader；仅持有一个 Reader 的 `shared_ptr` 不能替代 pin，否则对象仍存活而缓存已退还计费的情况会被隐藏。

**Reader 外壳与底层数据的所有权分开。** Artifact 默认只暴露 Serialize 接口，具体模式支持由 family 决定；只有 sealed `TextIndexArtifact` 与 `VectorMemArtifact` 实现公开的 `ReaderConvertible::IntoReader() &&`，把 Artifact 的底层状态一次性移交给 Reader，不复制索引 payload。转换必须由持有 `ArtifactPtr` 的调用方通过 `ConsumeIndexArtifact` 显式消费，成功、能力缺失或抛错后原 Artifact 都已销毁，不能重试；仅当该产物模式支持持久化且调用方同时需要持久化与直接查询时，才必须先 Serialize/Publish、再转换。其余 Artifact 只能经该 family 注册的静态 `Open` 从支持的持久化产物获得 Reader，能力缺失不得偷偷回退 IO。加载路径只保存 Reader；若 Reader 之后交给缓存，用 move 移交。缓存计费仍需覆盖真实存活的底层资源，不能借共享绕过 pin 管理。

查询期流程：

```text
1. caps = provider.Capability(field_id)              // 纯数据读：不 pin
2. exec 按 caps 决定执行路径
3. pin = provider.PinIndex(op_ctx, selected_key)      // 根 pin，每个表达式节点一次
       内部：slot->PinCells() → IndexReaderBase*
             返回 IndexPin{same CellAccessor, borrowed root reader}
4. selection 检查 selected_reader 是否暴露所需 typed interface
5. typed getter 按调用从 selected_reader dynamic_cast，再调用 reader->In(...)
```

根 pin 与条目选择按表达式保存；selection-time 会验证 metadata 声明的能力确实存在。`IndexInventory` 只提供根 `PinIndex`，JSON path 先从父 Reader 解析出借用 view，再按实际 cast type 检查接口。具体 `PredicateReader<T>()`、`ValueReader<T>()` 等 getter 不缓存 typed pointer，而是每次从当前根/JSON reader 做 `dynamic_cast`；调用频率由各执行路径决定，不能承诺每 batch 零 cast。不存在九路 `ReaderVariant` 或 typed pin 出口。

> **本轮目标是去掉具体实现 cast 与九路类型容器，不是消除查询接口 cast。** 部分索引结果仍会整段缓存后按 batch 切片，但 by-offsets、value lookup 等路径可在每次 typed getter 调用时 cast。性能收益不能从删除 `ReaderVariant` 直接推出。
>
> selection-time 验证负责及早拒绝 metadata/实际接口不一致；typed getter 的后续 cast 负责返回当前需要的 sibling interface。两者职责不同，不能为减少 cast 而重新增加 typed pin 出口或缓存一组接口指针。

**第 1 步不得 pin 是硬约束。** 现状代码注释已写明理由："短路路径（TextIndex/PkIndex/JsonStats）与 RawData 路径永不调用它，标量索引 cell 在分级存储里保持冷态"。若把 `ReaderCaps` 做成必须持有对象才能调的虚函数，路径决策就会把未加载的索引全部拉起——这是线上多余冷加载的直接来源。因此"`Capability()` 不触发 pin"是必测项：用计数型 fake `CacheSlot` 断言 pin 次数为 0——这是一个今天完全没被测到、而线上会造成多余冷加载的行为。

**growing 侧的生命周期**：GrowingIndexSet 唯一持有长寿命 appender；查询取得 GrowingIndexSnapshotPin，将同一版本的 reader、行覆盖边界和生命周期绑定在一起。内部版本唯一拥有 reader，pin 仅保留版本存活。每次发布创建版本，不是每次查询创建 reader；sealed family `Open` 同样维持 unique_ptr 返回。

两侧因此都不存在"每查询一个 proxy"：sealed 是 pin 一个长期对象，growing 是共享一个 commit 期快照。

## 5. 查询接口

sealed reader 由可选的 consuming Artifact 转换（仅 `TextIndexArtifact` 与 `VectorMemArtifact`）或 family registry 的静态 `Open()` 得到，之后不可变、可并发查询。growing 发布固定同代 Reader、覆盖范围和解释元数据；Tantivy/RTree 保持不可变查询视图，Knowhere 只固定逻辑前缀而继续共享单活引擎。bitmap 语义统一：1 = 命中；bitmap 尺寸 = `Count()`。

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
// index/contracts/query/NullReader.h
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

**同算法的类型复用。** 同一 family 内若数值与字符串共享查询生命周期和算法流程，使用一个按值类型模板化的 Reader 外壳；字符串专有的 `PatternMatchReader` 通过 `PatternMatchReaderAdapter<Derived, T, DelegateShouldUseForOp>` 无状态条件 mixin 提供，不复制 Reader 状态，数值实例没有该接口，`Caps()` 与实际继承一致。第三个 bool 只让 Inverted 委托真实的 `ShouldUseForOp` guard，不改变允许的 pattern op。Bitmap、Inverted 与 Sorted 已采用该模式；Bitmap 的公开头统一为 canonical `scalar/bitmap/BitmapIndexReader.h`，不再有 `Internal` 变体。数值 pair 与字符串 dictionary/posting 的持久化布局、读取和计费差异保留在 type-specific storage view，不复制查询外壳，也不改变文件格式或查询热路径。Bitmap 的 posting key 仍为 `owned_t<T>`；字符串查找使用透明比较，`Lookup` 返回拥有的 string，`Gather` 的 string_view 只在同步回调期间有效。

该规则不跨算法 family 建立有状态具体 Reader 基类。Marisa、FM、Text、Ngram 与 RTree 分别保留 trie、FM、全文匹配、候选生成与空间查询状态。JsonFlat 已把 field state、path、NULL 与资源逻辑收进共享 path-view 基类；bool 使用原生布尔范围，numeric 同时实现 int64/double 谓词并处理跨数值域精度，string 负责输入 ownership 与 pattern routing，因此这三种 path view 不再用 policy 模板强行合并。

**参数只共享相同规则。** `index/ParamUtils.h` 统一 numeric `DataType` 解码、string 参数、nested aliases 与冲突检查；`ScalarIndexUtils.h` 统一 scalar value-type 等价、ARRAY element type 协调与 C++ value type 映射。family parser 仍决定 missing/null/default、支持类型及错误上下文；FM 的 null-tolerant 可选参数不强行套 strict helper。只被单个 builder/loader 使用的短 parser 留在实际 consumer，不为单函数另建 Params 头或新的通用参数对象。

```cpp
// index/contracts/query/ScalarPredicateReader.h
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

scalar 与 JSON NGRAM 共用 `NgramIndexBuilder<T>` 的构建外壳：`T = std::string_view` 表示普通 scalar validity，`T = JsonProjectedString` 同时表达 field-null、path missing 与 present value。两者只复用 typed batch 遍历和 writer core；JSON projection、non-exist sidecar、generation 规则与完整性判定保持独立。V1/V2 JSON NGRAM 仍省略 `json_index_non_exist_offsets`，V3 才保留完整 sidecar，不能因模板复用改变旧格式或把 `LegacyUnknown` 误报为完整能力。

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
// index/contracts/query/SpatialReader.h
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
// index/contracts/query/JsonIndexReader.h
class JsonIndexReader {
 public:
    virtual ~JsonIndexReader() = default;

    // 该 path 上是否有可用的谓词接口；结果 get() 返回非拥有的类型擦除指针，
    // 消费者在父 pin 有效期内按 cast_type 做一次跨继承树的 dynamic_cast。
    // 返回空结果 = 该 path 无索引，exec 回退列扫描（可能落在 shredded 子列上，也可能落在
    // 原始 JSON 列上——那是 columnar-format 的事，本类型不知道也不需要知道）。
    virtual JsonResolvedReader
    Resolve(std::string_view path, DataType cast_type) const = 0;

    // path 存在性。建在值上的倒排能独立回答（json_exist_query），无需回读列。
    virtual TargetBitmap Exists(std::string_view path,
                                JsonValueType type = JsonValueType::Any) const = 0;

    virtual std::vector<DataType> CastTypesOf(std::string_view path) const = 0;
};
```

**路径视图也不能带走缓存 Reader 的所有权。** `JsonResolvedReader` 是 move-only 的路径解析结果，提供 `get()` / 空结果判断，不实现或转发查询方法。它有两种构造形态：借用父 Reader 已有的子 Reader，或以 `unique_ptr<const IndexReaderBase>` 持有一个临时路径视图；二者都不独立持有索引 payload。使用方必须同时保留父 Reader 的 pin/accessor，且路径视图先于父 pin 释放。查询从 `get()` 取得类型化接口后直接调用，不经结果对象逐 batch 转发。

- `JsonFlatIndex` 实现 `JsonIndexReader`：`Resolve` 创建一个绑定 path 的独占查询视图，借用父 Reader 的不可变 state。今天已有按 path 创建 `JsonFlatIndexQueryExecutor<T>` 的形态（`JsonFlatIndex.h:34`），只是靠继承 `InvertedIndexTantivy<T>` 拿到查询接口；改为组合后它直接实现 `ScalarPredicateReader<T>`。此视图不是新加载的索引，不复制 payload，也不通过共享指针延长 payload 生命周期。不为任意查询 path 向父 Reader 添加可变缓存。
- `JsonProjectedIndexReader` 独占其 `inner` Reader；匹配路径时返回借用该对象的结果，不复制 Reader、不重新加载、不额外构造转发代理。
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

它是列的派生物。索引侧不持有、不注入、不需要知道它存在——这也让 reader 保持 [§5](#5-查询接口) 的不可变约束，无需任何加载后装配或 reopen 重绑。

> **若将来推翻本结论（确有某类查询必须在 reader 内聚合），备案是“pin 时随句柄携带”**：offsets 由 segcore 随 concrete `IndexPin` 的同代 accessor 一并锚定，reader 本身仍不持有。曾评估过的另两条路都不要走回：family `Open` 的 `LoadOptions` 注入会凭空引入“列先于索引加载”的约束（毁掉 index-only 段的冷取优化），且 reader 捕获的 `shared_ptr` 在列 reopen 后变成过期而不悬垂的旧代——静默错误结果；加载后由 segcore 二次装配则要求列的加载路径知道索引存在（跨模块不变式，漏一处即静默错），并给一个声明“不可变、可无锁并发读”的对象开了可变中间态。

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

Builder 与静态 family load 函数按输入和结果区分，不是同一对象的两个阶段：

| | 输入 | 输出 | 调用方 |
|---|---|---|---|
| **Builder** | 调用方已准备完整的原生数据视图或本地文件 | 已完成的 Artifact | indexbuilder 服务 / segcore 加载时构建 |
| **Static loader** | 持久化产物与 load options | Reader | segcore load / 直接加载会话 |

Artifact 保留序列化职责。bitmap 等产物持有内存结构，tantivy、DiskANN 等产物可能已经是本地文件集；后者序列化主要是向 sink 交付文件，不需要再引入同时承担读写的 Codec。上传编排不属于 Artifact。

### 6.1 一次性 Builder

```cpp
// index/contracts/build/ArtifactBuilder.h
struct BuilderInputSpec {
    std::vector<FieldId> side_inputs;
};

template <typename Input>
class ArtifactBuilder {
 public:
    virtual ~ArtifactBuilder() = default;
    virtual BuilderInputSpec InputSpec() const = 0;
    virtual storage::ArtifactPtr Build(const Input& input) && = 0;
};
```

模板参数是**完整输入类型**，不是裸元素类型。所有 sealed builder 都以一次同步 `Build(input)` 返回已完成产物；不暴露 `Add`、`Seal`、`SetSourceFile`、pass-complete 或调用方 replay 协议。Builder 可以在内部批量调用引擎，也可以重复遍历同一输入，这些不形成公共生命周期。Growing 的长期 append/publish 是另一套接口。

`InputSpec()` 只在构建前声明实际需要的其他字段，构造后保持稳定。它不声明输入轮次或 Streaming/Contiguous/LocalFile form；输入形状由类型表达。向量额外字段是否需要加载仍取决于引擎能力和 partition isolation 配置，不能仅看到 `VEC_OPT_FIELDS` 就额外读取整列。

#### 6.1.1 完整输入与所有权

| 输入类型 | 数据形态 | 所有权与遍历 |
|---|---|---|
| `ScalarBuildInput<T>` | 多个稳定的 typed batch | 调用方保留 batch 及底层数据；Builder 可重复遍历，不要求额外拼接成连续标量数组 |
| `VectorBuildInput<T>` | 单个完整物理 tensor / sparse-row 数组，附带逻辑行信息与 side inputs | 调用方物化一次；Builder 不再缓存一份完整原始输入 |
| `PreparedVectorBuildFiles<T>` | 完整的 raw 文件及可选 sidecar 路径 | 调用方在 Build 返回或抛错前保留输入文件；Builder 的输出 staging 独立存活到 Artifact/Reader 释放 |

```cpp
template <typename T>
struct ScalarBuildBatch {
    std::span<const T> values;
    ValidityView validity;
};

template <typename T>
struct ScalarBuildInput {
    std::span<const ScalarBuildBatch<T>> batches;
};
```

标量 values 按逻辑行对齐，nullable 行也占位置；行数由各 batch 长度求和，不维护第二份计数。复用 `common/ValidityView.h`，可借用 packed 或 expanded validity，空 view 表示全有效；读取时使用 `!validity || validity[i]`。字符串用 `string_view`，数组用 `ArrayView`。不仅 span 指向的数组，连同其字符串、数组内容和 validity 后备内存，都必须稳定到同步 Build 结束。Builder 和返回的 Artifact 不得保留这些借用输入。

原始字段读取、缺失列填充、解码、JSON/ARRAY 投影和物化属于调用方；Builder 不接收 Segment、列对象、cursor 或远端存储 reader。投影所得元素坐标不因此变成行坐标。构建生成的字典、posting、trie keyset、FM corpus 或引擎文件是实际索引状态，不属于应删除的重复原始输入缓存。

**基线依据。** 重构前普通标量、Hybrid 与内存向量的生产入口已经先取得完整字段，再交给索引构建；manifest IO 即使逐 batch 解码，其完整收集入口仍保留全部数据。DiskANN 则先形成完整 raw 文件。此次不把“改造成流式输入”作为目标，也不把潜在的未来流式节省内存描述成已实现收益。

#### 6.1.2 向量输入的完整语义

`VectorBuildInput<T>` 分开表示 `logical_rows`、`physical_rows`、`dim`、`physical_values`、`parent_validity`、可选 `embedding_offsets` 和 `scalar_fields`：

- 普通 nullable 向量的 tensor 只包含有效行，parent validity 保留逻辑行域；不提供 embedding offsets。
- VECTOR_ARRAY 的 tensor 展平所有 embedding，offsets 长度为有效父行数加一。重复 offsets 保留有效空列表；全 null 的数组仍带 `{0}` offsets，不能与普通向量“不存在 offsets”混淆。
- `T` 是引擎分派类型，不能一概视为实际存储元素。dense span 使用 `T`，元素数由存储类型、维度和物理行数决定；sparse 标签 `sparse_u32_f32` 对应的 span 元素是稳定的 `knowhere::sparse::SparseRow<float>` 行对象，不是标签对象，也不按 dense 分量解释。普通物化保持行对象的拥有式复制，直接输入与 interim chunks 借用真实行对象，后备拥有者均保留到同步 Build 返回。
- 额外标量字段显式交付字段 ID、分类组和物理向量行号。外层没有字段，与“存在字段但没有分类组”有区别，保留不同存储版本的缺失输入语义。当前不支持的多字段或 VECTOR_ARRAY side-input 组合不因新增容器而自动获得支持。

`PreparedVectorBuildFiles<T>` 包含 raw、validity、embedding-offset 和 scalar-info 路径。validity/offset 路径存在时必须非空；scalar-info 保留三态：未交付、已交付但无文件、实际文件路径。输入 generation 的生命周期覆盖整个 Build，包括异常退出；输出目录与输入目录分开。引擎对输入的借用必须在输入 generation 释放前结束，不能让返回的 Artifact 偷借输入文件。

### 6.2 静态 family loader 与 IO 注入

不再存在 `IndexLoader`、`ArtifactLoader` 或 `LoadedArtifact` 对象层次。每个 family 提供静态 `DeriveCaps(const Config&) -> ReaderCaps` 与 `Open(FileSource&, const LoadOptions&) -> IndexReaderBasePtr`；`StaticLoaderProvider` concept 在注册点验证这对签名，registry 只保存两个函数指针。`DeriveCaps` 只看加载元数据，不能打开 payload；`Open` 唯一拥有并返回 Reader。刚构建产物直接查询是另一项可选能力：仅 `TextIndexArtifact` 与 `VectorMemArtifact` 实现公开的 `ReaderConvertible::IntoReader() &&`，由 `ConsumeIndexArtifact(ArtifactPtr)` 拿到 Artifact 所有权后消费并转换为 Reader。

只保留三条真实流程：

1. 构建 → Artifact → Serialize → 上传。
2. 构建 → Text/VectorMem Artifact → `ConsumeIndexArtifact` → Reader → 查询。
3. 持久化产物 → family `Open` → Reader → 查询。

删除 `OpenForRewrite`、`RehydratedIndex` 以及仅为“加载后重新发布”存在的构造函数、状态和分支。生产基线不要求该路径；直接加载会话只保存 Reader，不能把它包装为可发布 Artifact。Artifact 与 Reader 仍是不同角色；只有 `TextIndexArtifact` 与 `VectorMemArtifact` 可把刚构建的底层状态一次性移交给 Reader。所有产物保留 Serialize 接口，具体模式支持由 family 决定；普通 scalar、Hybrid、JSON 与 VectorDisk 不提供该转换，能力缺失返回 Unsupported，不做 IO fallback。仅当产物模式支持持久化且调用方同时需要上传与直接查询时，才必须先 Serialize/Publish、再 consume；consume 成功、Unsupported 或抛错都销毁 Artifact，不能重试。mmap 等后备资源的正常持有仍由具体 Reader/family `Open` 保证。

`FileSource`/`FileSink` 注入持久化 IO；Builder 只接收已准备的数据或文件，不持有 `FileManagerContext`。V1/V2 的 Index/DiskFiles source 可通过 `OpenDiskEngineFiles` 返回拥有完整 storage 依赖的 `DiskEngineFileHandle`，其中封装 manager、local prefix 与首个 storage failure；其它 source 保持 Unsupported。索引层不再取得 source 的 `Context` 或 `RemotePaths`。调用方负责下载、上传和结束 sink，索引 family 负责自身格式。输入 source 在打开期间借用，打开后的 Reader 或 `KnowhereEngine` 必须持有查询真正依赖的 handle、文件或引擎状态。

### 6.3 Hybrid 构建策略

标量策略名称统一为 `Hybrid`：目录 `scalar/hybrid`、`HybridIndexBuilder`、`HybridIndexArtifact` 和 family key `hybrid`。不保留名为 Auto 的兼容转发层；平台配置中的 AUTOINDEX 概念不因此改名。

Hybrid 在一次 Build 内对稳定输入统计基数，按已有低/高基数配置选择具体 family（包括 bitmap、inverted、sort、marisa），再将**同一份完整输入**交给选中 builder。默认选择及版本参数语义保持不变。允许探测提前结束，不要求调用方重读远端列，也不保存额外完整原始列。`HybridIndexArtifact` 记录实际选型，family `Open` 直接打开具体 Reader，不保留查询期 Hybrid 转发对象。

`HybridIndexArtifact` 与 `JsonProjectedIndexArtifact` 保持两个显式 wrapper。它们可以复用确实相同的 inner 委托、经 family 选择并校验后的文件写入、owned 临时目录和窄 RAII helper，但不引入无差别 `DecoratedArtifact{inner, extra}` 或强制共同基类；family 格式、metadata/sidecar 顺序、selector、path completeness、non-exist 与 owner 生命周期不能被通用装饰器抹平。

### 6.4 保持各 family 的持久化格式

统一 FileSink 不代表统一旧产物的封装方式。V1/V2 的 BinarySet entry 使用逻辑文件名，超过切片阈值才拆成数字后缀文件并写 `SLICE_META`；DiskFileManager 的目录文件则使用自己的数字切片规则。Marisa 的旧产物属于前者，不能因构建时先写临时文件就改用后者。V3 仍沿用各 family 的既有 entry 布局。

JSON NGRAM 的旧 V1/V2 格式没有 `json_index_non_exist_offsets`，不得通过通用 JSON projection 包装新增该文件。生成 Artifact 时仍收集路径缺失信息，只有序列化为 V1/V2 NGRAM 时省略；V3 保留完整信息。没有该完整性标记时加载路径保持 `LegacyUnknown`，不宣称完整 JSON-path 能力，缺失能力由消费者回退原始列；不能猜测 `Exists` 结果，也不改变 NGRAM 的 Count、field-null 或候选生成。

Numeric Sorted 的 NULL reverse-offset 槽没有查询含义。普通旧 V3 writer 通常留下零初始化的 `0`，旧 JSON 构建还可能因 null_count 与 validity 不一致而把越界循环中的值写入该槽；因此不能把旧 NULL 槽限制为 `0` 或新 writer 使用的 `-1`。加载路径对非 nested 的 NULL 行不读取或解释 reverse offset，包括全 NULL、posting 数为零的产物。非 NULL 行的反向映射、posting 指向 NULL、重复 posting 和 nested 完整性检查保持严格；String Sorted 不套用这个 numeric 专属规则。Reader 取值先检查 validity，不解引用 NULL 行占位。

### 6.5 indexbuilder 会话与 wire/native 边界

`BuildSession` 继续是有状态的一次性会话，保留 `BuildProduction`、`BuildDirect`、`LoadPhysical(milvus::NamedBufferSet)`、`SerializePhysical() -> const milvus::NamedBufferSet&`、`Publish`、`IsDirect`、`SourceType` 与 `DirectDimension`。删除纯状态透传的 `GetState` 和 no-op `CleanLocalData`；不恢复已无职责的 `Run`/`GetKind` 或 Service/FieldAdapter 墙。wire adapter 只负责把 C 输入规范化成 native request/field data，builder、staging、输入 owner 与失败状态仍由同一 session 持有；`SetShard` 行为和公开入口不变。

中性内存表示统一为 `common/NamedBuffer.h` 中的 `milvus::NamedBuffer` 与基于 `std::map` 的 `milvus::NamedBufferSet`。它只统一 name→bytes 容器，不抹平 C wire 上“payload bytes”与“disk descriptor/path”的不同含义；边界仍按对应模式验证大小、指针和所有权。

`storage::Artifact` 保留单参纯虚 `Serialize(FileSink&)`，并增加 `Serialize(FileSink&, ArtifactSerializationMode)`；默认双参重载表示 full-artifact 与 legacy 逻辑投影等价并委托单参，有差异的 family 必须显式覆盖，当前由 `RTreeIndexArtifact` 覆盖。`ArtifactSerializationMode` 只有 `FullArtifact` 与 `LegacyBinarySet`：生产 `IndexBuildService::Publish` 显式使用 FullArtifact，`BuildSession` 的 BinarySet 投影显式使用 LegacyBinarySet。`FullArtifact` 不表示 V3，外层 generation 仍由具体 `FileSink` 决定。由此替代对 RTree concrete type 的 downcast，不改变 full/legacy 文件内容、metadata/sidecar 顺序或其它 family 的默认序列化。

## 7. Growing 接口

Growing 生命周期、Append 输入和 Reader 查询能力是三个正交维度。不能把快照类型固定成某一种查询接口；同一个 reader 可以同时支持谓词、模式匹配、反查等能力，而不增加新的 Growing 公共接口。


```cpp
class GrowingIndexSnapshotPin {
 public:
    explicit operator bool() const noexcept;
    const IndexReaderBase& Reader() const;
    int64_t CoveredRowEnd() const noexcept;
};

class GrowingIndex {
 public:
    virtual ~GrowingIndex() = default;
    virtual void CommitIfNeeded() {}
    virtual void Flush() = 0;
    GrowingIndexSnapshotPin PinSnapshot() const;
    virtual DataType ValueType() const = 0;
    virtual std::string Family() const = 0;

 protected:
    void PublishSnapshot(std::unique_ptr<const IndexReaderBase> reader,
                         int64_t covered_row_end);
};

template <typename Batch>
class Appendable {
 public:
    virtual ~Appendable() = default;
    virtual void Append(int64_t row_begin, const Batch& batch) = 0;
};

// 实现类，不是为每种查询 Reader 新建公共 Growing 接口。
class TantivyGrowingTextIndex : public GrowingIndex,
                                public Appendable<TextBatch> { /* ... */ };
```

**同代一致性。** `PinSnapshot()` 一次取得 reader 和覆盖边界，二者来自同一次发布。删除独立的查询用已提交行数 getter：分别给 reader/watermark getter 加锁，仍可能取得覆盖 100 行的旧 reader 和 200 行的新 watermark，导致补扫漏掉中间 100 行。公共基类实现固定发布协议，具体引擎不重写读侧 pin 获取。

`GrowingIndexSet` 在 pin 前调用 owner 的 `CommitIfNeeded()`：interval writer 因而能在写入停止后由下一次查询提交已到期的最后一批；同步发布型实现使用默认 no-op。提交或发布错误原样传播，不能返回旧 pin 掩盖失败。`PinSnapshot()` 本身仍只取得已经发布的固定版本，不执行 family-specific commit，也不向 Reader 暴露 writer。

`Flush()` 是加载与 Reopen 使用的强制发布边界，不以提交间隔是否到期决定是否完成。它不等于 Artifact 序列化，也不要求向量引擎复制出新版本；各 family 仍遵守既有构建阈值和原始列回退策略。

向量首次 Build 失败时，仅在尚无任何已发布引擎且完整原始列仍保留的情况下，沿用空 pin/raw fallback：记录原错、重置未发布引擎，Insert 和 Load/Flush 不因此新增失败。已构建引擎的 Add 失败则终止该 owner 后续写入并传播错误，不重复 Add，不推进 feed/查询可见 ack，也不回收当前输入；不能在历史原始列已释放后重建。Add 成功后的 Reader/发布记录分配失败只允许重试发布，不属于 Build 回退或 Add 重试。

**覆盖边界。** `CoveredRowEnd()` 是该版本完整覆盖的 Segment 行前缀 `[0, end)`，不是最大已完成 reserved offset，也不是累计 Append 数量。如果后一个范围先完成，前面的空洞关闭前不能越过它发布；null 行也计入覆盖。Reader 的 `Count()` 是其本地坐标系基数，可能是元素数量，不能代替行 watermark。当前 typed batch 表达平坦行；嵌套输入需明确的 offsets/validity 视图，不能把元素数量冒充 row_count。

**所有权与 pin。** Segment 的 `GrowingIndexSet` 唯一持有 `GrowingIndex`。每次发布的内部不可变记录唯一持有 `unique_ptr<const IndexReaderBase>` 并绑定行覆盖边界。`GrowingIndexSnapshotPin` 内部保留该记录，消费者不能取出 reader 的拥有指针。发布和 pin 获取在同一把短锁下完成，查询与引擎构建不持有该锁。新版本发布后，旧版本仍活到最后一个 pin 释放；已获取 pin 可以比 GrowingIndex 活得更久，但调用 PinSnapshot 本身仍需 Segment 生命周期保护。pin 可复制、移动，移出者为空；引用和能力指针不得超过所属 pin 生命周期。

**发布前置条件。** 写入/commit 由写侧串行化，或由引擎提供等价的完成范围协议。所有 family 都固定发布记录、Count、行覆盖范围及其解释所需的元数据，并保留查询依赖的生命周期。Tantivy/RTree 继续发布不可变引擎查询视图；向量采用下述逻辑前缀模型，不要求冻结整个 Knowhere 引擎。`const` 本身既不提供并发安全，也不提供引擎隔离。发布拒绝空 reader、负边界和边界回退，内存分配失败或检查失败保持旧版本。Append 部分失败时，不仅不推进 watermark，还必须在以后发布前处理引擎残留的部分写入；单纯截短 Count 不足以恢复一致性。

**向量逻辑前缀（2026-09-08 已确认）。** 目标是结构重构，不改变向量算法。旧 pin 固定 Count、CoveredRowEnd、validity、offset mapping 和生命周期，底层仍由同一个 Knowhere 引擎接收 Add，并发执行 Search/RangeSearch/Iterator。新写入可能影响 ANN 的内部遍历和近似命中；不承诺旧 pin 的 ANN 结果不变。消费者和 Reader 必须在所有查询、迭代器、取值与 refine 路径限制可见 ID，使用查询可见范围与 pin 覆盖范围的交集，并按同代 nullable 映射转换为物理前缀。不能只在 top-k 之后丢掉新增行。延迟迭代器必须保留同一 pin 及其数据依赖。

此模型不增加 Knowhere 真快照/COW，不逐次 Serialize/Deserialize 复制索引，也不拆成多个 ANN 子索引再合并。已有向量值与逻辑/物理映射前缀保持稳定，元数据的发布成本需单独计量，不能用“不复制引擎”宣称发布没有成本。

**输入能力。** `ScalarBatch<T>{row_count, values, valid}` 是标量输入视图，`TextBatch` 为 string_view 特化，WKB 空间输入复用同样的物理字节视图。`VectorBatch<T>{row_count, values, dim, valid}` 对 dense 使用物理元素类型、对 sparse 使用 sparse-row 类型。输入借用内存只能在 Append 调用期间使用，异步保留需复制。Append 成功表示接受输入，不保证立即发布。插入路径按 schema 绑定一次正确的 `Appendable<Batch>` 能力；查询路径从 pin 中按所需查询能力做 checked sibling cast。不存在 `shared_ptr<void>` 或按查询接口分裂的 Growing 持有分支。

**Segment feed 只做机械复用。** protobuf vector batch 通过同步 `WithProtoVectorValues`、loaded `FieldData` 通过同步 `WithLoadedVectorValues` 复用 typed delivery 与 nullable slicing；raw fallback 只在 `FeedGrowingIndexRange` 内保留一个 `EngineType`/`RawTrait` switch。proto、loaded、raw 三条 source/owner 分支仍然独立，不引入 `FeedTicket` 或第二套协议对象。现有 serialized feed/flush/ACK 仍是唯一协议：raw-ready 只推进连续前缀；pending batch end 在一次失败后固定；`required_flush_end` 只做 max 合并；全部输入 owner 保持到对应时点。raw 列回收发生在主 ACK 之前，普通 pending 输入在 ACK 之后释放。Reopen 的私有新 owner 只回填当前可见前缀，依次 Flush、Register 并更新 schema，不进入普通 pending 队列，也不推进主 ACK。loaded source 保留每 part 边界；临时 sparse decode owner、借用 loaded pending owner 与 pinned raw fallback 的生命期不混用；4096 前缀的 validity/mapping 保持原样。

**失败顺序不变。** 首次 cold Build 只有在没有已发布引擎且完整 raw 仍可用时才安全回退；已构建引擎的 Add 失败保持 poison 并传播；Add 已成功而发布分配失败只重试 publish，不重复 Add，也不提前改变 `PublishAccepted` 的时机。

**消费者职责。** 空 growing snapshot pin 表示尚无可读版本（默认覆盖 0 行）；成功发布的空 reader 与之不同。sealed 根 `IndexPin` 只有索引条目缺失时为空，接口缺失由消费者在 checked cast 或 JSON resolve 后决定 fallback，不能表述为根 pin 自己因接口缺失而为空。`[CoveredRowEnd(), query_barrier)` 的未覆盖部分由 segcore/exec 决定是否补扫，text match 可允许滞后，其他类型按策略回退。若快照比 query_barrier 更新，仍需查询可见性过滤；watermark 不替代 MVCC。

**Builder 边界不变。** sealed 加载时构建归一次性 Builder。growing 越过阈值时的冷启动由 GrowingIndex 初始化/首次提交直接建立引擎，不经过 Artifact；冷启动与后续增量处理在实现内分开，仍保留原 Train/Add 算法。Appender 不承担 Serialize/Upload。GrowingCommitPolicy 只管理写侧提交节奏，不再维护第二份公开覆盖状态。

**Reader 复用。** Growing publisher 只负责准备 family 对应的查询状态和发布记录，不为同一查询能力另建重复 Reader 实现。RTree growing 在稳定状态可用后直接发布现有 `RTreeIndexReader` 与 `RTreeIndexState`；Tantivy 与向量同样复用各自 Reader 查询逻辑，后备存储、冻结的元数据与写侧生命周期差异留在引擎状态组合中。

**实现状态。** 公共发布/pin 协议与 live Growing 实现的签名已统一；Tantivy Text 保留 native rollback、连续 Append、commit 与独立 manual reader 发布，RTree 保留不可变 shard 发布，向量采用已确认的逻辑前缀方案。本轮只抽取上述 typed delivery 与 nullable slicing 的机械重复，不改变 readiness、inventory、`HasIndex`、field-key 选择或发布协议。本批尚无新的生产构建或运行证据，不能用先前 POC 的构建/e2e 证明此次精简后的生产补扫、故障恢复或端到端可用性。关闭条件见 [PENDING_ISSUES.md](PENDING_ISSUES.md) 的 PI-003。

### 7.1 vector 的 Appender 接口：今天已存在，且带着与 `IndexBase` 同构的问题

Appender 不是标量专属。`segcore/FieldIndexing.h` 里两类索引今天都实现了 append：`VectorFieldIndexing::AppendSegmentIndexDense`/`Sparse`（`:281,287`）建 growing interim 向量索引，`ScalarFieldIndexing::AppendSegmentIndex`（`:171,177`）建 growing 标量索引。但共享基类 `FieldIndexing`（`:51`）是两类接口的并集，5 个纯虚里 3 个 vector 专属、2 个 scalar 专属，两个子类各自 throw 掉对方那一半（§2.2）。

接口与索引类型的关系：

> **四种接口对两类索引统一；每种接口内部的方法按索引类型分。** 接口是按调用方切的（谁在用），索引类型是按数据与算法切的（用什么结构）——两者正交。`FieldIndexing` 的错误正是把"两类索引共有一个接口"误当成"两类索引共有一组方法"。

因此所有实现共用 GrowingIndex 的发布与 pin 协议，按输入实现 Appendable<Batch>，不再按标量/文本/向量查询接口分裂 Growing contract。dense/sparse 的物理差异由 typed VectorBatch<T> 描述，不要求两个查询生命周期接口。

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
> 因此顺序是：先把 `!built_` 冷启动分支迁到 GrowingIndex 的初始化/首次 commit，由 growing owner 从列 gather 一次输入并直接建立引擎与固定逻辑前缀的 Reader，不经过 Artifact 或 consuming 转换；`Append` 随后退化为与标量同形的单个方法，`VectorBase*` 从签名消失，dense/sparse 只剩一个 `dim` 参数的区别（见 [§11.3](#113-vector-按四种接口重新划分不重设计)）。
>
> `FieldIndexing` 签名畸形的直接原因，就是两个接口混在同一个方法体里——这印证了 §7 第 3 条「加载时构建 ≠ growing」。

> 两处现状说明：
>
> 1. **原 vector growing 没有不可变引擎快照。** `index_` 是一个活对象，`AddWithDataset` 就地修改，`get_segment_indexing()` 把裸指针直接交给查询（`FieldIndexing.h:332-335`），`sync_with_index_` 只是个 bool。2026-09-08 确认保留该单活引擎算法，新增的 pin 明确绑定可见前缀、元数据与生命周期；不再要求 Knowhere 引擎换代或旧 pin 的 ANN 命中不变。
> 2. **"growing 标量索引是已有生产事实"要缩小范围。** 真正在跑的只有 GEOMETRY：其余标量类型的 `ScalarFieldIndexing::AppendSegmentIndex` 直接 `ThrowInfo(Unsupported, "... not implemented for non-geometry scalar fields")`（`FieldIndexing.cpp:706-711,745-750`），而 `recreate_index` 仍为它们造出 marisa/sort 对象（`:659-662`）——造完不再写入、也不读取，`ScalarFieldIndexing::data_` 全仓零写入，`get_chunk_indexing` 会索引到空 vector。加上 `TextMatchIndex` 的 growing 构造，生产事实是"文本 + 空间两类"，不是"标量各类型普遍"。这不改变设计结论（统一接口仍要做），但改变风险性质：growing 标量实现 对绝大多数索引类型是新增能力而非重构既有能力，[§13.1](#13-被本重构触发的已有缺陷) 的 bitmap 越界因此必然发生，不是可能发生。

两类索引在这三点上一致：

| 语义 | scalar | vector |
|---|---|---|
| 固定发布记录、并发查询 | tantivy 独立 reader / RTree 不可变状态 | 固定逻辑前缀与元数据，Knowhere 活引擎继续 Add/Search；ANN 命中可变化 |
| 已提交行数（快照覆盖到哪一行） | `GrowingIndexSnapshotPin::CoveredRowEnd()` | 同；今天隐含在 `sync_data_with_index()` 里 |
| 阈值前无快照 | 无（`ScalarFieldIndexing::get_build_threshold()` 返回 0，`:197`） | 有（`VectorFieldIndexing::get_build_threshold()` 取配置，`:321`）——`PinSnapshot()` 返回空即表示 |

阈值差异不构成分歧：接口已用"返回空表示尚无可读快照"表示，scalar 只是阈值恒为 0 的退化情形。

**连带的 `IndexBase` 退役项**：`FieldIndexing::get_chunk_indexing`/`get_segment_indexing` 返回 `PinWrapper<index::IndexBase*>`（`:128,131`）——growing 侧同样以 `IndexBase` 作类型擦除句柄。[§11.2 第 3 条](#112-处理决定)的"`IndexBase` 在阶段 1 内退役"必须把这两个出口一并算进去，否则 sealed 侧删干净了、growing 侧还留着一个引用。

## 8. 现有实现类 → 新接口映射

| 现类 | 查询接口 | 构建/growing | 备注 |
|---|---|---|---|
| `InvertedIndexTantivy<T>` | `ScalarPredicateReader<T>` + `PatternMatchReader` | Builder | tantivy 封装降为内部引擎，不再是基类 |
| `BitmapIndex<T>` / `ScalarIndexSort<T>` / `StringIndexMarisa` | `ScalarPredicateReader<T>` + `ScalarValueReader<T>`；`BitmapIndex<std::string>` 与 `StringIndexMarisa` 另 + `PatternMatchReader` | Builder | `is_nested_index_` 模式位保留，对外表示为 `CoordDomain() == Element`（§5.8）。`BitmapIndex` 有完整 LIKE 系列（`BitmapIndex.h:219` `SupportPatternMatch`、`:224` `PatternMatch`、`:281` `PatternQuery` 用 `LikePatternMatcher`），删掉它会删掉在用的代码 |
| `HybridScalarIndex<T>` | 消失 | Builder 选型策略 | §6.3 |
| `StringIndexSort` | `SortedIndexReader<std::string_view>`：`ScalarPredicateReader<std::string_view>` + `PatternMatchReader` + `ScalarValueReader<std::string_view>` | Builder | 旧实现不是类型别名（576 + 1860 行，自带 pImpl 层次与带版本格式），且有完整 LIKE 系列（`StringIndexSort.h:131,136`，三处实现覆写）。终局与数值 `SortedIndexReader<T>` 共用查询外壳和条件 pattern mixin；原字符串 dictionary/posting 与数值 pair 的 storage view、文件格式和热路径分别保留 |
| `BoolIndex` | 同 bitmap | Builder | 这个是类型别名（32 行、无类），随迁 |
| `FMIndex` | `PatternMatchReader`，仅此谓词接口 | Builder | `SegcoreConfig` 依赖改构造参数注入。原因是全局可变配置放在 segcore 里，于是所有要读配置的模块都被迫依赖它（`FMIndex.h:30` include `segcore/SegcoreConfig.h`、`:227` 读 `default_config()`） |
| `TextMatchIndex` | `TextMatchReader` | `ArtifactBuilder<ScalarBuildInput<std::string_view>>` 的 text 实现 + `TantivyGrowingTextIndex` | 四构造函数拆到四个接口 |
| `NgramInvertedIndex` | `NgramReader` | Builder | Phase2 删除，`index → exec` 依赖消失 |
| `JsonFlatIndex` (+ QueryExecutor) | `JsonIndexReader` | `ArtifactBuilder<ScalarBuildInput<std::string_view>>` 的 json 实现 | |
| json path cast index | `ScalarPredicateReader<T>` | Builder | 索引清单按 (field, path) 注册 |
| `JsonKeyStats` | **迁出 index**。阶段 1 内移到 `segcore/json_stats/`（断继承 + `git mv`）；终局子列升格 columnar-format、layout 目录留 segcore | 构建仍是离线任务，暂不接 L1 产物的构建与加载流程 | `NotImplemented` 泛滥消失；对 segcore 的 5 处 include 当场降到 0；`BsonInvertedIndex` 一并迁走。终局与过渡见 [§1](#1-范围) |
| `RTreeIndex` / geometry | `SpatialReader`（§5.6） | Builder | `Candidates` 成为唯一的谓词查询接口；点谓词重载（未实现桩，`RTreeIndex.cpp:412,420,427,464,474,481,489,500`）与 `Reverse_Lookup` 移除，但 `IsNull`/`IsNotNull` 保留、归 `NullReader`；`Query(DatasetPtr)` 是在用的入口不是未实现桩，删的是包装、行为搬进 `Candidates`（见 §5.6）；GIS proto 枚举换 native `SpatialOp` |
| `SkipIndex` | **移出 index 接口** | | 归 columnar-format（zone-map/`CellSkipPredicate`） |

> 表中不逐行重复 `NullReader`：七种标量索引全员真实现、零 throw，是无条件可用的公共接口（见 [§5 开头](#5-查询接口)）。
| `VectorMemIndex<T>` / `VectorDiskIndex<T>` | 非模板 `VectorIndexReader` 实现统一的 `VectorReader` 18 方法契约（§11.3） | `ArtifactBuilder` + growing（接替 `VectorFieldIndexing`） | `KnowhereEngine` 拥有 native state 与 backing owner；共用 metadata/nullable/iterator/refine/raw/dense/embedding 逻辑，不保留 Raw/pass-through 或 Mem/Disk backend policy；仅 `VectorMemArtifact` 提供 consuming 转换 |

## 9. 消费者对接

| 消费者 | 现状 | 目标 |
|---|---|---|
| exec 表达式 | `PinIndex` 拿 `IndexBase*` 后 `dynamic_cast` 到具体实现；能力探测靠 `Support*` + try | 路径决策只读 metadata `ReaderCaps`；选定条目后取得根 `IndexPin`，再 checked-cast 所需查询接口。删除 typed pin 出口与九路 `ReaderVariant`，不以“零 dynamic_cast”为目标；JSON resolve/fallback 与数值 type-alias 差异保留 |
| exec ngram | `ExecutePhase1/2`，Phase2 传 `exec::SegmentExpr*` | Phase1 = `NgramReader::Candidates`；Phase2 = exec 用 columnar-format `Scan`/`Take` 取值后自行求值 |
| exec geometry | `QueryCandidates` + `PhyGISCoarseConjunctExpr`/`PhyGISRefineConjunctExpr`——切分已正确 | 仅换接口：`SpatialReader::Candidates` + native `SpatialOp` + bitmap 输出；粗筛/精化的处理流程不动，与 ngram 统一为同一候选类型的处理流程 |
| exec ARRAY 相等 | `ExecArrayEqualForIndex`：`InApplyCallback` 逐元素回调 → `unordered_set` 求交 → `to_row_offset` 转坐标 → `is_same_array` 精确验证 | 索引给元素级 `In()` bitmap（§5.8）；exec 侧逐元素 `inplace_and` + 1% 提前退出，再经统一的聚合算子聚合到行做 `is_same_array` 验证。`InApplyCallback` 与 `unordered_set` 消失，`to_row_offset` lambda 并入该算子 |
| indexbuilder | `ScalarIndexCreator` 调 `CreateIndex/Build/Serialize/Upload` | `ArtifactBuilder` + `Artifact::Serialize` + storage sink；Creator 变薄封装 |
| segcore load | `Load`/`LoadUnified` + cachinglayer 计费实现在索引里 | registry 静态 `DeriveCaps`/`Open` + Reader `CellByteSize`；admission/final fallback 仍在各 load translator |
| segcore growing | `FieldIndexing`/`ScalarFieldIndexing` 分散机制 | `GrowingIndex` + `Appendable<Batch>`，由 `GrowingIndexSet` 持有 |
| reduce 物化 | `ReverseDataFromIndex`（`segcore/Utils.h:151`） | `ScalarValueReader::Gather` |

## 10. 硬性规则（lint）

1. `index/` 不得 include `segcore/`、`exec/`、`query/`（现状违规：`NgramInvertedIndex` 2 处、`FMIndex` 1 处、`JsonKeyStats` 5 处——全部本阶段降到 0，其中 `JsonKeyStats` 随目录迁往 `segcore/json_stats/` 而消失，见 §1 过渡处理）。
2. `FileManagerContext`/`DiskFileManagerImpl` 不得出现在任何 Reader/Builder/Appender 的签名与成员中，只允许在 family 静态 `Open`/Artifact 实现和 indexbuilder 上传编排内使用；disk reader 只保留 storage-owned `DiskEngineFileHandle` backing owner。
3. 实现类之间禁止继承（`grep ": public.*Index" `，只允许继承接口）；scalar 查询接口保持不继承 `IndexReaderBase` 的纯 mixin。统一向量查询契约 `VectorReader` 是明确例外，直接继承 `IndexReaderBase`，具体 `VectorIndexReader` 只继承这一个查询接口。
3b. `ReaderCaps` 的查询期来源必须是索引清单缓存的纯数据；lint 检查路径决策代码（`DetermineExecPath` 一类）不出现 pin 调用（§4.3）。
4. 能力缺失禁止用 `ThrowInfo(Unsupported)` 表示——lint 检查接口实现中不出现该模式。
5. cachinglayer 的 cache ownership、pin 实现与策略类型不得泄出 segcore/load 边界；Reader 不能索取 cache 所有权或控制淘汰。`IndexReaderBase::CellByteSize()` 返回 `cachinglayer::ResourceUsage` 是已确认的计费值类型例外，不表示 Reader 持有或操作 cache。
6. `knowhere` 头不得出现在共享基类与标量索引类型的接口及实现中，仅 vector 索引类型及其 static Open/Artifact 可见（§11.2 第 5 条）。现状违规基线：标量索引类型的头文件自身 knowhere 计数为 0，违规全部来自传递链——`index/Index.h`（共享基类，3 处直接引用）、`index/Utils.h` → `common/QueryInfo.h` → `knowhere/config.h`（几乎所有标量索引类型的实现文件都 include `index/Utils.h`），以及最大的一条：`common/Types.h` 自己（`:27-34` 直接 include `knowhere/binaryset.h`、`comp/index_param.h`、`dataset.h`、`operands.h` 与 `pb/plan.pb.h`、`pb/schema.pb.h`、`pb/segcore.pb.h`）。而 `TargetBitmap`/`DataType`/`FieldId` 只在 `Types.h` 里 alias——任何接口头只要用 `TargetBitmap` 就传递性拉进 knowhere 与 pb。见 [§12.1(a)](#121-vector-查询接口的细化)。

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
| growing 语义（发布记录 + 覆盖前缀 + 阈值前无快照） | 共用生命周期和前缀协议；Tantivy/RTree 引擎状态不可变，Knowhere 仅固定逻辑前缀，不承诺相同 ANN 命中 | 合理共享（公共保障共享，引擎隔离不强行统一） |

### 11.2 处理决定

1. **共享基类收缩为生命周期基类，且生命周期那半下移到 L1**。切成两段：

   | 段 | 内容 | 层 | 谁用 |
   |---|---|---|---|
   | 产物与 IO 机制 | `Artifact::Serialize(FileSink&)`、`FileSink`/`FileSource`/`LoadOptions`/`ArtifactStats`，以及 owned `LocalDirectory`、窄 `WriteAll`/fd/entry/mmap guard | L1 | index 各索引 family；后续 columnar-format 派生产物可复用同样机制 |
   | 索引加载与查询 | family 静态 `DeriveCaps`/`Open` 函数对；`IndexReaderBase` 负责能力、坐标、`Count()` 与 `CellByteSize()` | L2 | 仅 index |

   下移的理由：文件、目录、分片、原子发布与资源 guard 不是索引算法。JSON shredded 布局（[§1](#1-范围)）同样可能复用这些 L1 机制；但这不要求 L1 暴露一个同时表示“加载后对象”的 `LoadedArtifact`。已实现边界是 Artifact 与 Reader 分离：L1 Artifact 只负责 Serialize，L2 family `Open` 直接返回 `unique_ptr<IndexReaderBase>`。可直接查询的 Text/内存向量 Artifact 经拥有式 `ConsumeIndexArtifact` 转换；其它 Artifact 不提供转换，也不走隐式 IO fallback。

   已落定的命名是 `storage::Artifact`/`ArtifactStats` 与 index family static loader；没有 `IndexLoader`、`ArtifactLoader`、`LoadedArtifact` 兼容层。`CellByteSize` 因此仍位于 L2 `IndexReaderBase`，不要求为了 L1 Artifact 迁移 `ResourceUsage`。物化形态仍覆盖 named memory buffers、DiskANN 本地/stream handle 与 mmap；实现复用机制但不强制把三种 `FileSource` concrete classes 合成一个 mega-class。

   > **第一版接口暴露的问题与最终处理。** 三种形态及第四种 streaming 形态曾得到以下结论；表中是设计演进依据，不是当前接口缺口：
   >
   > | 形态 | 结论 | 依据 |
   > |---|---|---|
   > | knowhere `BinarySet` | 装得下，但有前提 | `index_.Serialize(BinarySet)` 产出具名内存 blob，对上 `WriteEntry` 是字面匹配。前提是 `Disassemble`/`Assemble`（超过 `FILE_SLICE_SIZE` 的 blob 切成 `name_0..name_k` + `INDEX_FILE_SLICE_META`）必须移进 sink/source 内部——否则 `EntryNames()` 返回的是物理切片名，物理布局泄进 Loader，且每个索引类型抄一遍 |
   > | DiskANN 本地大文件 | 写侧装得下，读侧要补一条承诺 | 写侧 `WriteEntryFromLocalFile` 正合适；读侧是"下载到本地目录 + knowhere 自己按 `DISK_ANN_PREFIX_PATH` 开文件"，`ReadEntriesToLocalDir` 能表示，但接口必须补上今天没有的一条：本地文件名等于文件项 basename |
   > | mmap | 装不下 | `VectorMemIndex::LoadFromFile` 把 n 个远端文件项流式合并成 1 个本地文件（`storage::FileWriter`），knowhere mmap 的正是这个合并文件。`FileSource` 只有 1→内存、1→1 文件、n→n 文件，没有 n→1 |
   > | **DiskANN streaming（第四形态）** | 描述不了 | `GetCacheFilesForDiskIndexLoad(index_files, index_.LoadIndexWithStream())`（`index/VectorIndexValidDataUtils.h:98`）：streaming 时只下载 valid-data 切片，索引字节不经过这套流程，引擎自己去远端读。流程不是这些字节的读者，`ReadEntry`/`ReadEntryToLocalFile`/`ReadEntriesToLocalDir` 任何组合都表示不了 |
   >
   > slice、拼接、目录落地和原子发布已归 `FileSink`/`FileSource` 的窄方法；共享的 `LocalDirectory`、`WriteAll`、`FileDescriptorGuard`、`LocalEntryGuard` 与 `MappedRegionGuard` 只复用相同资源机制，不改 family 格式、zero-size 或错误类别。DiskANN streaming 不通过 `Context()`/`RemotePaths()` 泄露 source 状态，而由 storage-owned `DiskEngineFileHandle` 持有 manager、local prefix、首个 storage failure 和完整 owner；只有 V1/V2 Index/DiskFiles source 支持，其余 source 保持 Unsupported。原 `ReadRawEntryToLocalFile` 已删除。
   >
   > staging 已由 owner 生命周期负责，`CleanLocalData` 的 C++/C/Go no-op 接口与调用方一致删除；不另造外层清理 framework。

2. **查询接口不共享，明确写出**：vector 查询接口与 scalar 各索引类型并列（§11.3），不设计任何跨索引类型的查询接口。
3. **`IndexBase` 在阶段 1 内退役**：vector 同阶段迁移，不保留 adapter。sealed cache cell 唯一拥有 `IndexReaderBase`，查询只经 concrete `IndexPin` 保留 accessor lease；growing 继续使用独立的 `GrowingIndexSnapshotPin`/`Snapshot`/`Appendable`/`GrowingCommitPolicy`，不与 sealed pin 合并。
4. **工厂按索引类型拆分**：`CreateIndexInfo` 拆散；input-typed builder registry 与由 concept 约束的 family `DeriveCaps`/`Open` 静态函数对取代 loader 对象和 `IndexFactory` 巨型分派 switch。
5. **knowhere 逐出标量路径**：标量各索引类型的接口与实现零直接 knowhere include；`BinarySet` 只出现在 vector 索引类型的 static Open/Artifact。收益：标量索引编译隔离、knowhere 升级不再重编全部标量索引。
6. **storage 流程收到 family Open/Artifact 边界之后**（两类索引同规则），`FileManager` 引用缩到各索引类型的加载/Artifact 实现文件内。五个 scalar 目录 wrapper 与两个 vector 目录 wrapper 统一为共享拥有的 `storage::LocalDirectory`；只共享相同的创建、路径所有权和 RAII 清理，不用 `LocalChunkManager` 改写现有语义。相同的写满 loop、fd、entry 与 mmap guard 机械复用，各 family 的 open flags、zero-size、错误类别、格式和 sidecar 顺序不变。
7. **cache translator 只共享最小 contract。** `storagev1translator::IndexReaderTranslator` 仅保存 `Family()`、`ValueType()`、`Caps()` 并提供 warmup policy 转换；`SealedIndexTranslator`、`TextMatchIndexTranslator` 与 `InterimSealedIndexTranslator` 的资源估算、admission、final fallback、打开与生命周期逻辑保持各自所有，不合并成通用加载框架。

### 11.3 vector 按四种接口重新划分（不重设计）

vector 纳入阶段 1 的范围是按接口重新划分：把现有 `VectorIndex` 的查询面完整收进一个 `VectorReader`，knowhere 的算法、原生格式、物理类型、disk beamwidth 与 nullable/prefix 语义不变。对象不止 sealed 侧，还包括 growing 侧的 owner（[§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)）。

```cpp
// 一个统一查询接口，共 18 个方法；以下按职责压缩列出。
class VectorReader : public IndexReaderBase {
 public:
    // Search, Iterators, RefineEnabled
    // HasRawData, GetVector, GetSparseVector
    // Metric, KnowhereIndexType, Dim, PrepareSearchParams
    // HasValidData, ValidCount, IsRowValid,
    // PhysicalOffset, LogicalOffset, OffsetMapping
    // CalcDistByIDs, GetEmbListByIds
};

// sealed memory、disk 与 growing generation 共用同一个非模板 Reader。
class VectorIndexReader final : public VectorReader {
 private:
    KnowhereEngine engine_;
    VectorValidData valid_;
    /* generation-specific query state */
};

// 所有 growing 实现共用 GrowingIndex；输入类型独立于查询能力。
template <typename T>
class KnowhereGrowingVectorIndex : public GrowingIndex,
                                  public Appendable<VectorBatch<T>> { /* ... */ };
```

四种接口对两类索引统一（Reader / Appender / Builder / Loader），差别只在接口内的方法按索引类型分：

| 接口 | scalar | vector | 关系 |
|---|---|---|---|
| Reader | §5 的各查询接口 | 一个 `VectorReader`（18 方法） | 形态同、查询内容不与 scalar 共享（§11.2 第 2 条） |
| Appender | `Appendable<ScalarBatch<T>>` | `Appendable<VectorBatch<T>>` | 共用 `GrowingIndex` 与 `GrowingIndexSnapshotPin`；输入类型与查询能力正交，冷启动由 growing owner 直接初始化，不经过 Artifact |
| Builder | `ArtifactBuilder<ScalarBuildInput<T>>` | `ArtifactBuilder<VectorBuildInput<T>>` / `ArtifactBuilder<PreparedVectorBuildFiles<T>>` | 同一个 `Build(input) -> Artifact` 契约，输入形状由类型表达 |
| Static loader | `DeriveCaps`/`Open` | `DeriveCaps`/`Open` | 同一 function-pair contract，无 loader 对象 |

Builder 共用的是“一次完整输入 → 已完成 Artifact”的生命周期，不是标量和向量相同的数据布局。`BuilderInputSpec` 只声明实际 side inputs；值、validity、offsets 和额外字段须沿具体输入类型交付，不能靠新增声明位代替数据通道。Reader 则保留各自的查询语义（`In/Range/bitmap` 与 `Search(dataset, SearchInfo)`）。

`VectorIndexReader` 非模板地直接实现统一查询接口，不经有状态具体 Reader 基类，也不保留 `VectorMemReader<T>`/`VectorDiskReader<T>` alias、Raw pass-through 或 Mem/Disk `BackendPolicy`。`KnowhereEngine` 直接拥有 native handle、物理类型和 backing owner；成员/析构顺序保证 native state 先释放，文件、mmap 或 manager owner 随后释放。共用层保留 metadata、nullable mapping、iterator、refine、raw/dense 与 embedding-list 取回；backend 的真实 Search/sparse、disk beamwidth 和错误分支仍在具体实现内，不能因去掉 policy 改算法或格式。

indexbuilder 的一次性构建边界也只去除转发层，不增加通用生命周期框架。Disk materializer 在构造时建立一个六路 typed `unique_ptr` builder variant，`Build()` 只消费一次；resident `VectorBuildMaterializer` 使用“typed builder + owned values”成对 variant，并以 `monostate` 区分 moved-from 与已 consumed 对象。公共 fields、cached `InputSpec`、borrowed contiguous owner、layout 与 scalar side input 留在外层；move 显式清空源对象，已 consumed 但未 move 的对象仍可读取缓存 `InputSpec`。builder build 失败时仍先销毁 engine-side borrower，再释放输入 owner/view。`VectorScalarInfoAccumulator` 的各 typed 实现是真实类型分派，不随这项 pimpl 清理删除。

当前代码结构已迁移 vector `ReaderCaps`、growing owner/snapshot pin、Segment/exec/indexbuilder 消费者以及 sealed Translator。`storagev1translator::IndexReaderTranslator` 只共享 `Family`、`ValueType`、`Caps` 与 warmup policy 转换；Sealed、TextMatch、Interim 的资源估算、admission、final fallback 与加载时序仍各自负责。资源计账保留 live estimate 并明确 unavailable-zero，精确原生统计仍是后续工作。

运行证据必须单独计算。标准 V1/V2 legacy 33 个 supported case、V3 36 个 supported case、基础 growing/load/reopen、native/add TEXT、普通 INVERTED、Tantivy/Marisa 与 SCANN 的结果均来自当前精简之前的 POC；它们是历史对照，不证明本批构建或运行通过。V1/V2 文件格式继续受支持，删除的是退休 C `AppendIndex`/`V1SealedIndexTranslator` API，不是格式。旧 pin 跨 Add/owner release、deferred iterator、部分失败与发布失败仍主要依赖静态追踪；细节见 [PENDING ISSUES PI-003/004](PENDING_ISSUES.md#pi-003--growing-读视图与生产接线)。

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

**(b) iterator 生命周期——已提供同一 accessor 的锚，运行验收仍 OPEN。**

链路逐段核实过：

1. `SearchOnSealedIndex` 中 `auto accessor = SemiInlineGet(entry.indexing_->PinCells(op_context, {0}))` 是函数局部（`query/SearchOnSealed.cpp:88`）。
2. `PrepareVectorIteratorsFromIndex` 取出 `knowhere::IndexNode::IteratorPtr`，并取 `&index.GetOffsetMapping()`——一个指向索引对象内部的裸指针（`exec/operator/Utils.h:129-134`）。
3. 两者一起装进 `SearchResult::vector_iterators_`（`common/QueryResult.h:305`）；`ChunkMergeIterator::offset_mapping_` 就是那个裸指针（`QueryResult.h:252`），没有任何 pin 随之保存。
4. 消费发生在后续算子：`SearchGroupByNode.cpp:89`、`IterativeFilterNode.cpp:127`、`IterativeElementFilterNode.cpp:117`。

**当前处理。** iterator 返回类型保持裸 `IteratorPtr`，不为 knowhere 包一层新 iterator 类型；消费方把 sealed `IndexPin` 经 `IntoSharedLifetime()` 转入 deferred result。该操作转移的是原有 `CellAccessor` shared control block，不分配新的 Reader wrapper，Reader 仍由 cache cell 唯一拥有。iterator、offset mapping 与 Reader 借用都不得超过该 accessor lifetime。

这解决的是结构上的 lifetime anchor，不等于旧 pin 跨 owner release、cache 淘汰与 deferred consumption 已经经过生产并发/失败验证；动态关闭条件仍在 PI-003。growing 使用自己的 `GrowingIndexSnapshotPin`，不与 sealed accessor 合并。

**(c) knowhere 类型能否出现在 vector 接口里——已决定：允许。**

`VectorIndex::VectorIterators` 的返回类型是 `knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>`（`index/VectorIndex.h:78`），直接穿透到 `query/`。决定是允许它继续穿透：vector 索引类型与 knowhere 的绑定是既成事实，包装一层只在"换引擎或多引擎并存"时才有价值，而那个需求今天不存在，不为它付包装成本。

因此 [§11.2 第 5 条](#112-处理决定)的边界就是最终边界：knowhere 类型在 vector 索引类型内自由出现，在共享基类与标量索引类型内一处也不许有。这条同时确认了 [§11.3](#113-vector-按四种接口重新划分不重设计) 的接口框架够用——vector 不需要独立的 `02-vector-index.md`，除非实现时出现别的结构性问题。

### 12.2 产物的构建与加载流程放在哪个组件、叫什么

**已决定并实现的边界。** 字节与文件机制位于 L1 `storage/artifact`：`Artifact`、`ArtifactStats`、`FileSink`、`FileSource`、`LoadOptions`、`LocalDirectory` 与窄资源 guard。索引语义仍在 L2：`ArtifactBuilder<Input>`、family 静态 `DeriveCaps`/`Open`、`IndexReaderBase` 及查询 mixin。不存在 `ArtifactLoader`、`LoadedArtifact` 或 `IndexLoader` 兼容层；`CellByteSize()` 保留在 `IndexReaderBase`，因此不需要为 L1 Artifact 引入 cachinglayer 资源基类。

**IO 能力按真实消费者收窄。** memory entries、目录/文件物化、n→1 mmap 拼接分别由 `FileSource` 的窄方法表示；DiskANN 的 local/stream 两种 native backing 由 `DiskEngineFileHandle` 封装 manager、local prefix、首个 storage exception 与完整 owner。只有 V1/V2 Index/DiskFiles source 支持该 typed handle，其余 source 返回 Unsupported。`ReadRawEntryToLocalFile`、source `Context()` 与 `RemotePaths()` 已删除；但 V1/V2 文件格式、普通 source generation、`SealedIndexTranslator` 与 live resource estimate 路径仍保留。

**复用的单位是机制，不是 family 对象模型。** `LocalDirectory::CreateOwned`、validated `WriteAll`、fd/entry/mapping guard 及 staging/commit helper 可以跨 family 使用；各 family 的格式、metadata/sidecar 顺序、zero-size 策略和 owner 生命周期仍独立。Hybrid 与 JSON projected artifact 不合成 `DecoratedArtifact`，三种 source 也不强制合成一个 final mega-class。suspected V1 double-write 与最终 FileSource concrete-class merger 均为 follow-up。

**第二消费者仍是 follow-up。** JSON shredded layout 的终局仍属于 columnar-format，但本阶段没有把其整个构建/加载流程迁到上述 Artifact 边界。L1 机制应可被它复用，不据此提前增加共同基类或把 index family 语义下沉到 storage。

### 12.3 `cell_size_` 的计量方式没有定义

**本轮范围决定。** `IndexReaderBase::CellByteSize()` 报告打开后的 Reader 自有 heap/file-backed 资源；无法从 native backend 得到可靠值时允许明确的 unavailable-zero。加载前 admission estimate、`SealedIndexTranslator` 的 final estimate/fallback、`IndexLoadResource`、`LoadOptions::estimated_bytes`、interim 公式和 growing Segment 公式仍保留各自语义，不统一塞入 Reader。

旧 `V1SealedIndexTranslator` 与退休 C `AppendIndex` 已删除；这不表示删除 V1/V2 文件格式或现行 `SealedIndexTranslator`。随之失去唯一生产写入者的 `loaded_resource_estimate` 管道也已删除，不能继续把旧的“V1 post-load index_size 注入 Reader”描述成当前行为。删除死管道不等于加载后已有精确 native 计账，也不等于所有 post-load fallback accounting 都已删除。

问题仍然 OPEN：Knowhere `IndexNode::Size()` 允许近似且覆盖不完整；远端文件长度不是常驻 heap；mmap/file-backed、共享 backing owner 与同一 native node 被多个发布记录引用时都需要明确归属。应以实际 ownership 与资源事件时序定义统计，不能按 pin 次数重复收费，也不能在最后一个 owner 释放前提前减账。具体关闭条件见 [PI-004](PENDING_ISSUES.md#pi-004向量实际资源计账缺少统一的原生统计依据)。

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

### 12.6 growing 滞后不保留独立策略 API

未消费的 `GrowingIndexSet::LagPolicy`/`LagPolicyOf` 已删除。live owner 的提交与补扫行为由 `GrowingCommitPolicy`、snapshot 覆盖前缀和消费者 fallback 共同决定，不再维护第二份按 family 查询的 lag policy 表。Text 仍可按既有 interval 滞后；加载/Reopen 通过 Flush 保证对应边界。Ngram/JSON 当前没有因此获得新的 growing owner 或产品语义。

若以后新增这两类 growing 能力，必须先明确 LIKE/JSON path 的即时可见性承诺，再落进同一 feed/snapshot/fallback 协议；不为假设能力恢复已删除的策略查询 API。

## 13. 被本重构触发的已有缺陷

与 [§12](#12-待定问题) 性质不同：这些不是待决定的设计问题，而是现状已经存在、且被本重构放大或直接触发的缺陷。它们必须进阶段 1 的验收标准——重构可以不修其中某一条，但不能在不知情的情况下把它放大。

**13.1 growing 标量索引会触发 `size_per_chunk_` 越界（issue #51237 同型）。**

`Expr.h:2239-2253` 的注释已经明确写出触发条件（有两个触发条件，不是一个：注释原话是 _a scalar field gains an interim index on growing, or geometry is routed through `ProcessIndexChunks`_——而 geometry 恰好就是今天唯一在跑的 growing 标量索引，见 [§7.1](#71-vector-的-appender-接口今天已存在且带着与-indexbase-同构的问题)）：缓存的 index bitmap 是段全局的（标量索引恒为单 chunk），而 `size_per_chunk_` 是原始数据的 chunk 粒度（`segcore.chunkRows`），两者无关；sealed 段上二者恰好相等，所以至今没有触发，而今天只有 sealed 段能走到这里，因为 growing 段上 `HasIndex()` 只对 vector/geometry 为真。注释原话：_The moment a scalar field gains an interim index on growing … `size_per_chunk_` would over-run the bitmap exactly as in issue #51237._

[§7](#7-growing-接口) 的 growing 标量实现 统一接口正是那个触发条件，本阶段直接触发它。阶段 1 实现 growing 标量索引的同一个 PR 里必须先修这处边界，或至少让 growing 标量索引走一条不经过该分支的路径。

**13.2 vector iterator 的 pin 生命周期（见 [§12.1(b)](#121-vector-查询接口的细化)）。**

历史路径把裸 `IteratorPtr` 与指向 Reader 内部 offset mapping 的指针放进 deferred result，却只在准备函数内持有 accessor。当前由 consumer 保存 `IndexPin::IntoSharedLifetime()` 转移出的同一 accessor lifetime，不改变 iterator 接口，也不建立 Reader 共享所有权。生产并发、owner release、cache 淘汰与故障路径仍按 PI-003 验收。

**13.3 growing 段的字节计费用的是过期缓存值。**

`Index.h:130-135` 明写 `ByteSize()` 返回的是 `ComputeByteSize()` 算出的缓存值、growing 段持续插入时不会自动更新，并声明该方法"仅为 sealed 段设计"。但 growing 索引同样参与 cachinglayer 计费。[§7](#7-growing-接口) 把 growing 索引统一成一等接口后，走这条路径的索引类型会变多，误计费的范围随之扩大。

这与 [§12.3](#123-cell_size_-的计量方式没有定义) 是同一处代码的两个不同问题：12.3 是"计量方式没有定义"的设计问题，13.3 是"其中一套在 growing 上本来就不准"的正确性问题。统一计量方式时必须同时解决后者。
