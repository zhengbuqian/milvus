# `index/growing/` —— Appender 面（growing 增量索引）

W1 的 growing 部分。规格来源是
[01-scalar-index.md §7 / §7.1](../../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md)，
合同在 [`index/contracts/GrowingIndex.h`](../contracts/GrowingIndex.h)。本 README 只做导读与对照。

**growing 不是未来假设，是已有生产事实**（§7）。本目录收编两套现状机制：
`TextMatchIndex` 的 growing 构造（commit interval + background merge + `AddTextsGrowing` +
`Commit`/`Reload` + reader 快照）与 segcore 的 `ScalarFieldIndexing<T>` / `VectorFieldIndexing`
（`segcore/FieldIndexing.h`）。segcore 侧的退役由另一个 agent 做，本目录只提供索引侧的实现。

> `index/` 的行号指向 W1 前的树（master `e255009e01`）；`segcore/FieldIndexing.*` 的行号指向当前树。

## 1. 文件

| 文件 | 一句话 |
|---|---|
| `GrowingAppenderBase.h` | **契约缺口**：Appender 面没有类型擦除根。本文件按 `IndexReaderBase` 对称补一个（`ValueType()` / `Family()` / `CommittedRows()` / `ReaderSnapshotErased()`），并说明为什么不接受 `shared_ptr<void>`；同时列出 growing 侧 `IndexBase` 的**三个**出口 |
| `GrowingCommitPolicy.h` | commit 节奏 + 水位记账，被各族**组合**（从 `TextMatchIndex` 的 `commit_interval_in_ms_`/`shouldTriggerCommit` 提炼，并补上今天缺失的水位本身） |
| `GrowingSpatialIndexFace.h` | **契约缺口**：今天唯一真在跑的 growing 标量索引是 GEOMETRY，而它的面是 `SpatialReader`，装不进 `GrowingScalarIndex<T>`（后者把快照面写死为 `ScalarPredicateReader<T>`） |
| `TantivyGrowingScalarIndex.h/.cpp` | tantivy 支撑的 `GrowingScalarIndex<T>` 实现；快照**复用 sealed 的 `InvertedIndexReader<T>`**。**§13.1 的醒目警告在这个头文件顶部** |
| `TantivyGrowingTextIndex.h/.cpp` | `GrowingTextIndex` 实现；`TextMatchIndex` 四个构造函数里的第 1 个归到这里，其余三个归 Builder/Loader；快照复用 sealed 的 `TextIndexReader` |
| `RTreeGrowingSpatialIndex.h/.cpp` | GEOMETRY 的 growing 实现（今天唯一可用的 growing 标量索引），实现上面那个族内声明的面 |
| `KnowhereGrowingVectorIndex.h/.cpp` | `GrowingVectorIndex` 实现，收编 `VectorFieldIndexing`；dense/sparse 合一，冷启动全量建切给 Builder 面 |

## 2. 三条关键语义（§7），以及现状与它们的距离

| 语义 | 合同要求 | 现状 |
|---|---|---|
| ① **快照 + 水位，不承诺实时** | `ReaderSnapshot()` 给不可变快照，`CommittedRows()` 给显式水位 | text：tantivy commit/reload 天然如此，但**水位不存在**（没人记录快照覆盖到哪一行）；vector：`index_` 是一个**边写边读的活对象**，没有换代（`FieldIndexing.h:332-335` 直接返回裸指针）；geometry：**完全没有快照**，`AddGeometry` 直接写活索引（`FieldIndexing.cpp:790-811`） |
| ② **覆盖不足的桥接不在本合同内** | `[CommittedRows(), insert_barrier)` 归 segcore/exec；索引侧不设任何表达该策略的位 | 现状用 `sync_data_with_index()` 一个 bool 表达，粒度只有"全同步/没同步" |
| ③ **就地建 ≠ growing** | 就地建是 Builder（一次性、独占、终结于 Artifact）；growing 是 Appender（长期存活、与写并发、只产水位快照） | 两者**混在同一个方法体里**：`AppendSegmentIndexDense/Sparse` 的 `!built_` 分支是 form B+ 冷启动全量建，`built_` 分支才是 append。这正是 `VectorBase*`（整根列）留在 append 签名里的唯一原因 |

**策略按族分、表放 segcore**（§7 语义 2、§12.6）：text match 允许滞后不补齐，其余族回退列扫描。
本目录**不设任何表达该策略的位**——否则 `ReaderCaps` 会开始承载产品语义。segcore 用
`GrowingAppenderBase::Family()` 查表。

## 3. `IndexBase` 在 growing 侧的出口（退役清单）

§7.1 点了两个，第三个来自 segcore 消费者接线：

| 出口 | 现状返回 | W1 后 |
|---|---|---|
| `FieldIndexing::get_chunk_indexing(chunk_id)` | `PinWrapper<index::IndexBase*>` | `ReaderSnapshotErased()` |
| `FieldIndexing::get_segment_indexing()` | `PinWrapper<index::IndexBase*>` | `ReaderSnapshotErased()` |
| `FieldIndexing::has_raw_data()` | 调 `index_->HasRawData()`（`IndexBase` 上的 virtual），消费者 `SegmentGrowingImpl.cpp:409,764,1032,1997,2078` | 问**快照**：vector 走 `VectorValueReader::HasRawData()`，scalar 走 `ReaderCaps::value_lookup`（§5.5/§4.1） |

## 4. 快照 = 复用 sealed 的 reader

三个族的快照都**不是新写的类**，就是 sealed 侧那个 reader：

| 族 | 快照类型 | 来源 |
|---|---|---|
| inverted | `InvertedIndexReader<T>` | `index/scalar/inverted/` |
| text | `TextIndexReader` | `index/scalar/text/` |
| vector | `VectorMemReader<T>` | `index/vector/` |

这正是设计自己的主张落到代码上：**读实现两侧共享，只有写面按调用方分开**（§3 原则 1——面是按**调用方**切的，不是按段状态切的）。
一次 commit 产出一个新 reader，所有并发查询经 `shared_ptr` 共享同一份（§4.3）。

但复用带出两条必须先解决的事：

1. **`InvertedIndexReader<T>` 的 `Count()` 会跑到快照前面**。它的构造函数是
   `(engine, null_offsets, is_nested_index)`，没有显式 count，只能问引擎——而引擎在快照背后继续长。
   §5 规定 bitmap 尺寸恒为 `Count()`，§7 规定 growing 快照的覆盖是 `CommittedRows()`；两者不一致就会
   把 bitmap 开到快照根本没索引的行上，**与 §13.1 同型**。同目录的 `TextIndexReader` 已经显式收 `count`，
   两个 reader 自己就不一致。属于标量 agent 的文件，只报告不改。
2. **bitset 回调必须是 growing 那个**。它绑在**引擎**上（`create_reader(SetBitsetFn)`），
   所以持有 wrapper 的 appender 才是安装 `SetBitsetGrowing` 的人；growing 段上 tantivy 可能返回超出
   查询已分配 bitset 的 doc id，而 sealed 版本的注释明说这不可能发生。

## 5. 已知缺口与真行为变更（骨架里就地标注，别当成已解决）

1. **`GrowingScalarIndex<T>` 没有类型擦除根** → `GrowingAppenderBase.h`（本目录先补，形状见该文件；契约层要不要收由契约 owner 定）。
2. **快照面写死为单个 face** → 一个 growing inverted 拿不到 `PatternMatchReader`/`NullReader`，
   一个 growing RTree 根本装不进去。建议 `ReaderSnapshot()` 返回类型擦除根、消费者 sibling cast，
   与 sealed 侧同规则（§4.3）、与 `JsonIndexReader::Resolve` 同手法（§5.7）。
3. **"不可变快照"对 vector 与 geometry 是真行为变更，不是现状描述**。两族今天都没有换代机制。
   geometry 更具体：拆分后的 `RTreeBuildEngine::Finish()` 是**写盘**（`<path>.bgi` + `meta.json`），
   `RTreeQueryEngine::Load()` 是**读盘**，所以"每次 commit 发布一个快照"照现有引擎写法就是**每次 commit 一趟磁盘**。
   三个选项（内存树拷贝 / 双代合并 / 查询期读锁）写在 `RTreeGrowingSpatialIndex.h` 里，都不免费。
4. **`IndexReaderBase : storage::LoadedArtifact`** 让一个从未落盘、从未加载的 growing 快照
   也必须回答 `CellByteSize()`。§13.3 说 growing 索引确实在参与 cachinglayer 计费，所以这个数不是无意义的，
   但它的口径未定（§12.3）、在 growing 上还必然过期。
5. **§13.1**：`TantivyGrowingScalarIndex` 落地即触发 `Expr.h:2239-2253` 注释点名的越界
   （issue #51237 同型）。修它要动 `exec/`，不在本轮范围，但骨架必须让它可见。
