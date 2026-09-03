# `index/contracts/` —— index 组件四种接口的定义

本目录是 [阶段 1：Index 接口拆分](../../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md)
的接口层框架，**只有接口，没有实现**。设计文档是唯一规格来源；本 README 只做导读与对照，
不重复论证。

配套的 L1 产物构建与加载流程在 [`storage/artifact/`](../../storage/artifact/)（§11.2 第 1 条：
"落盘 → 上传 → 下载 → 打开 → 计费"这套流程不是索引专属的，移到 L1）。

## 1. 四种接口：持有者与生命周期

来自 §3 原则 1。**接口是按调用方切的**，任何一类调用方拿不到其他接口的方法。

| 接口 | 持有者 | 形态 | 生命周期 | 本目录中的文件 |
|---|---|---|---|---|
| **Reader** | exec（查询期） | 索引对象上的接口 | 不可变，长期存活 | `IndexReader.h` + 各查询接口 |
| **Appender** | segcore growing insert 路径 | 索引对象上的接口 | 长期存活，与读并发 | `GrowingIndex.h` |
| **Builder** | indexbuilder 服务、segcore 加载时构建 | 独立对象 | 一次性，`Seal()` 后结束 | `IndexBuilder.h` |
| **Loader** | segcore load | 独立对象（每种索引类型一个） | 无状态 | `IndexLoader.h`、`Registry.h` |

**Builder 与 Appender 是两个接口而非一个**：前者一次性、独占，以产出 Artifact 结束；后者长期存活、
与读并发、只产出按已提交行数截取的快照。把两者合一正是 `TextMatchIndex` 四个构造函数挤在一个类的原因（§2.2、§7 第 3 点）。

Reader 接口按查询语义拆成若干查询接口（`ScalarPredicateReader<T>`、`PatternMatchReader` 等纯 mixin）。

## 2. 文件 → 文档小节对照

### 共享基类与能力描述

| 文件 | 文档小节 | 内容 |
|---|---|---|
| `ReaderCaps.h` | §4.1 | 能力描述符。**纯数据**、由加载期元数据算出、存进 `IndexInventory`；`Caps()` 只是一致性校验对象 |
| `IndexReader.h` | §4.2（并见 §4 关于查询接口不继承基类的注） | 类型擦除基类 `IndexReaderBase : storage::LoadedArtifact`，含 `Domain` / `CoordDomain()` / `Count()` |

### 查询接口（全部为纯 mixin，**不继承** `IndexReaderBase`）

| 文件 | 文档小节 | 提供者（§8 映射表） |
|---|---|---|
| `NullReader.h` | §5 开头「跨索引类型的公共接口」 | 七种标量索引类型全员，**无条件可用、不设 `ReaderCaps` 位** |
| `ScalarPredicateReader.h` | §5.1 | inverted / bitmap / sort / marisa / json path cast index |
| `PatternMatchReader.h` | §5.2 | tantivy inverted、marisa（prefix）、FMIndex（**仅此谓词接口**） |
| `TextMatchReader.h` | §5.3 | TextMatchIndex（组合 tantivy 快照，**不继承 inverted**） |
| `NgramReader.h` | §5.4 | NgramInvertedIndex。候选类型，`ReaderCaps.exact = false`，**Phase2 从索引中删除** |
| `SpatialReader.h` | §5.6 | RTreeIndex。候选类型；查询接口是 `SpatialReader` + `NullReader` |
| `ScalarValueReader.h` | §5.5 | bitmap / sort / marisa。反查接口 |
| `JsonIndexReader.h` | §5.7（并见 §12.4） | JsonFlatIndex。只做 **path 路由**，不定义新的查询语义 |

nested（元素级）不是独立的查询接口，是**实现类上的一个模式位**，对外表示为 `CoordDomain() == Domain::Element`（§5.8）。

### 构建 / 加载 / growing

| 文件 | 文档小节 | 内容 |
|---|---|---|
| `IndexBuilder.h` | §6、§6.1、§6.1.1、§6.1.2、§6.3、§11.3 | `IndexBuilder<T>`（**标量与向量共用一个接口**）+ `BuilderInputSpec` |
| `IndexLoader.h` | §6.2、§11.2 第 1 条 | `IndexLoader : storage::ArtifactLoader`，`Family()` + `DeriveCaps()` + 打开 |
| `GrowingIndex.h` | §7、§7.1、§11.3、§13.1、§13.3 | `GrowingScalarIndex<T>` / `GrowingTextIndex` / `GrowingVectorIndex` |
| `VectorReaders.h` | §11.3、§12.1(a)(b)(c) | `VectorSearchParams` / `VectorSearchReader` / `VectorValueReader` |
| `Registry.h` | §11.2 第 4 条 | 按索引类型划分的 loader / builder registry，取代 `IndexFactory` 的巨型分派 switch |

## 3. 不变式

1. **查询接口是纯 mixin，不继承 `IndexReaderBase`**（§4 注、§10 规则 3）。
   实现类**非虚**继承 `IndexReaderBase` + 各查询接口，`IndexReaderBase` 到查询接口是一次跨继承树的 `dynamic_cast`。
2. **能力描述，不许 throw**（§3 原则 3、§10 规则 4）。不支持的操作在类型上就不存在，或经
   `std::optional` 表示。本目录中**没有任何** `ThrowInfo(NotImplemented/Unsupported)` 形态的默认实现。
3. **输出统一是 `TargetBitmap`**（§5 开头）。选择率是查询的运行时属性，不是索引类型的静态属性。
4. **坐标一律由 `CoordDomain()` 描述**（§5.8）。元素级索引在元素坐标系输出，
   **索引不做元素级到行级的聚合**——聚合到行的位置由 plan 决定。
5. **不认识 segment 与 executor**（§3 原则 5、§10 规则 1）。本目录不得出现 `segcore/`、`exec/`、
   `query/` 的任何 include。
6. **不持有 `FileManagerContext` / `DiskFileManagerImpl`**（§3 原则 6、§10 规则 2）。
   IO 一律经注入的 `storage::FileSink` / `storage::FileSource`。
7. **标量索引类型的接口定义零 knowhere include**（§10 规则 6、§11.2 第 5 条）。knowhere 只允许出现在
   `VectorReaders.h` 及 vector 索引类型的实现里（§12.1(c) 已决定允许）。
   → 因此 `GrowingIndex.h` 对 `VectorSearchReader` 用的是**前向声明**而不是 include。
8. **pb 类型不进接口定义**（README §5 规则 2）。native 枚举在本层定义，proto→native 的映射在 plan/exec 侧。
9. **cachinglayer 类型不进接口签名**（README §5 规则 4、§10 规则 5）。
   `LoadOptions::warmup` 用本层的 native `WarmupPolicy`，不是 `CacheWarmupPolicy`。

## 4. 接口框架实现时与文档的偏离

| 位置 | 文档写法 | 代码写法 | 理由 |
|---|---|---|---|
| `ScalarPredicateReader::Range` | `OpType op` | native `CompareOp` | `milvus::OpType` 就是 `proto::plan::OpType`（`common/Types.h:106`）。README §5 规则 2 禁止 pb 出现在接口签名上，§5.6 对 GIS 算子给的正是"定义 native enum + 边界映射"这个解法，此处同理 |
| `PatternMatchReader::PatternMatch` / `NgramReader` | `OpType op` | native `PatternOp` | 同上。取值集就是 §5.2 列出的 `{Match, PrefixMatch, PostfixMatch, InnerMatch, RegexMatch}` |
| `JsonIndexReader::Resolve` / `CastTypesOf` | `DataType cast_type` | `JsonCastType` | §12.4 明说"写 `DataType` 等于提前选定，写 `JsonCastType` 是保守取值"。本轮取保守取值 |
| `JsonIndexReader::Exists` 的 `JsonValueType` | 未说明来源 | 本层 native enum | 现状 `index::JsonValueType` 是 tantivy 引擎枚举 `::JsonExistValueType` 的别名（`JsonFlatIndex.h:31`）。引擎类型不进接口签名 |
| `IndexLoader::Open` | `Open(...) -> shared_ptr<IndexReaderBase>` | `OpenIndex(...)` + `final` 转发的 `Open` | C++ 的协变返回只对裸指针/引用成立，对 `shared_ptr` 不成立，无法把 `ArtifactLoader::Open` 的返回类型改成更窄的类型。语义与配对关系不变 |
| `IndexArtifact` | 在 §6.1 的片段里 | `storage::Artifact` | §11.2 第 1 条把产物的构建与加载流程整体移到 L1 并去掉 `Index` 前缀 |
| `IndexBuilder<T>::Seal()` | `IndexArtifactPtr` | `storage::ArtifactPtr` | 同上 |
| `IndexLoader::DeriveCaps` | 文档无此方法 | 新增 | §4.1 要求 `ReaderCaps`"由加载期元数据算出、不 pin 就能读"，而按索引类型划分的知识只有 Loader 有。这是 §4.1 的直接后果，不是新增设计 |
| `SpatialOp` 取值集 | §5.6 未列举 | `GISOp` 去掉 `Invalid` 与 `STIsValid`、保留 `DWithin` | `STIsValid` 现状明确不走索引（`GISFunctionFilterExpr.cpp:201-202`）；`DWithin` 走索引但 distance 在 exec 侧先转成 bbox（`:448-455`），所以本接口不需要 distance 参数 |
| `ScalarValueReader` 的 `owned_t<T>` | 直接使用 | 在本文件里定义 | 仓库里没有这个 trait；按 §5.5 原话（`owned_t<string_view> = string`，其余 = `T`）实现，不多不少 |

**未采用的方案**：在 `IndexBuilder.h` 里声明 `ScalarIndexBuilder<T>` / `TextIndexBuilder` /
`JsonIndexBuilder` 三个接口。理由：§6.1 的 C++ 片段与正文明确写的是**标量与向量共用一个
`IndexBuilder<T>`**、"各索引类型的 builder 是这个接口的**实现**而非新的接口"，§11.3 的表也把
Builder 一行标为"接口统一"。另：`index/JsonIndexBuilder.h` 这个文件名在仓库里已被占用。

## 5. 明确不在本目录的东西

| 东西 | 所属组件 | 依据 |
|---|---|---|
| `SkipIndex`（zone-map） | columnar-format (L1) | §1 排除项；#51504 已把它用作 `CellSkipPredicate` |
| `JsonKeyStats` / JSON shredding | 阶段 1 内移到 `segcore/json_stats/`；终局子列归 columnar-format、layout 目录归 segcore | §1 的判断与过渡处理 |
| 元素→行的 offsets 链 | 列的派生物，exec / columnar-format 持有 | §5.8「元素→行映射的所属组件」 |
| 缓存计费（pin、准入、淘汰） | segcore load 侧 translator | §4.2、§10 规则 5 |
| 上传编排 | indexbuilder 服务 | §6.2 |
| `ReaderCaps` 的 segment 级聚合（`FieldIndexCapability`） | segcore | §4.1 |
| 元素到行的聚合算子 | exec（阶段 4，可能提前，见 §12.5） | §5.8 |

## 6. 已知待定项（接口框架里以注释形式标注在原地）

- **§12.3 `cell_size_` 的计量方式没有定义**。`storage::LoadedArtifact::CellByteSize()` 负责它，
  但今天同一个字段被不同索引类型按两种计量方式填（压缩前文件大小 vs 实测常驻内存），而缓存层拿它做准入与淘汰。
  §12.3 要求**先于产物的构建与加载流程移到 L1** 定义完。接口框架只记录这个问题。
- **§12.2 放在哪个组件与命名待定**。`Artifact` / `ArtifactLoader` / `ArtifactStats` / `LoadedArtifact`
  是暂定名；`storage/` 里既有的 `IndexData` / `IndexEntry*` 命名系列要连带处理。
- **跨仓前置**：`ResourceUsage` 今天在 milvus-common 的 `cachinglayer/Utils.h`，
  §11.2 要求先移到 `common/ResourceUsage.h`（`namespace milvus`），否则硬规则 4 与本设计自相矛盾。
- **§12.6 growing 已提交行数滞后策略的索引类型清单**：`NgramReader` 与 `JsonFlatIndex` 归哪边待定。
  索引侧不设任何表示该策略的位，所以不影响本层接口。
- **§13.1**：实现 `GrowingScalarIndex` 会直接触发 `size_per_chunk_` 越界（`Expr.h:2240-2252` 的注释
  已把触发条件明确写出）。同一个 PR 里必须先修。
