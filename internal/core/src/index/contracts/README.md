# `index/contracts/` —— index 组件的四面窄合同

本目录是 [W1：Index 四面化](../../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md)
的接口层骨架，**只有接口，没有实现**。设计文档是唯一规格来源；本 README 只做导读与对照，
不重复论证。

配套的 L1 产物管道在 [`storage/artifact/`](../../storage/artifact/)（§11.2 第 1 条：
"落盘 → 上传 → 下载 → 打开 → 计费"这套管道不是索引专属的，下沉到 L1）。

## 1. 四个面：持有者与生命周期

来自 §3 原则 1。**面是按调用方切的**，任何一类调用方拿不到其他面的方法。

| 面 | 持有者 | 形态 | 生命周期 | 本目录中的文件 |
|---|---|---|---|---|
| **Reader** | exec（查询期） | 索引对象上的接口 | 不可变，长期存活 | `IndexReader.h` + 各查询 face |
| **Appender** | segcore growing insert 路径 | 索引对象上的接口 | 长期存活，与读并发 | `GrowingIndex.h` |
| **Builder** | indexbuilder 服务、segcore load 就地建 | 独立协作者 | 一次性，`Seal()` 后终结 | `IndexBuilder.h` |
| **Loader** | segcore load | 独立协作者（每族一个） | 无状态 | `IndexLoader.h`、`Registry.h` |

**Builder 与 Appender 是两个面而非一个**：前者一次性、独占、终结于产出 Artifact；后者长期存活、
与读并发、只产出水位快照。把两者合一正是 `TextMatchIndex` 四个构造函数挤在一个类的成因（§2.2、§7 第 3 点）。

### 术语：**面** ≠ **face**

- **面**（四面）= 按**调用方**切的四组职责：Reader / Appender / Builder / Loader。
- **face**（查询 face）= **Reader 这一面内部**按查询语义再切的纯 mixin 接口，即 face ⊂ Reader 面。

见 §4.3 开头的术语说明。下文"查询 face"一律指后者。

## 2. 文件 → 文档小节对照

### 共享根与能力自述

| 文件 | 文档小节 | 内容 |
|---|---|---|
| `ReaderCaps.h` | §4.1 | 能力描述符。**纯数据**、由加载期元数据算出、存进 inventory；`Caps()` 只是一致性校验对象 |
| `IndexReader.h` | §4.2（并见 §4 关于 face 不继承 root 的注） | 类型擦除根 `IndexReaderBase : storage::LoadedArtifact`，含 `Domain` / `CoordDomain()` / `Count()` |

### 查询 face（全部为纯 mixin，**不继承** `IndexReaderBase`）

| 文件 | 文档小节 | 提供者（§8 映射表） |
|---|---|---|
| `NullReader.h` | §5 开头「跨族共有面」 | 标量七族全员，**无条件可用、不设 caps 位** |
| `ScalarPredicateReader.h` | §5.1 | inverted / bitmap / sort / marisa / json path cast index |
| `PatternMatchReader.h` | §5.2 | tantivy inverted、marisa（prefix）、FMIndex（**仅此谓词面**） |
| `TextMatchReader.h` | §5.3 | TextMatchIndex（组合 tantivy 快照，**不继承 inverted**） |
| `NgramReader.h` | §5.4 | NgramInvertedIndex。候选族，`caps.exact = false`，**Phase2 从索引中删除** |
| `SpatialReader.h` | §5.6 | RTreeIndex。候选族；face 是 `SpatialReader` + `NullReader` |
| `ScalarValueReader.h` | §5.5 | bitmap / sort / marisa。取值面（反查） |
| `JsonIndexReader.h` | §5.7（并见 §12.4） | JsonFlatIndex。只做 **path 路由**，不定义新的查询语义 |

nested（元素级）不是独立的 face，是**实现类上的一个模式位**，对外表达为 `CoordDomain() == Domain::Element`（§5.8）。

### 构建 / 加载 / growing

| 文件 | 文档小节 | 内容 |
|---|---|---|
| `IndexBuilder.h` | §6、§6.1、§6.1.1、§6.1.2、§6.3、§11.3 | `IndexBuilder<T>`（**两族共用一个面**）+ `BuilderInputSpec` |
| `IndexLoader.h` | §6.2、§11.2 第 1 条 | `IndexLoader : storage::ArtifactLoader`，`Family()` + `DeriveCaps()` + 打开 |
| `GrowingIndex.h` | §7、§7.1、§11.3、§13.1、§13.3 | `GrowingScalarIndex<T>` / `GrowingTextIndex` / `GrowingVectorIndex` |
| `VectorFaces.h` | §11.3、§12.1(a)(b)(c) | `VectorSearchParams` / `VectorSearchReader` / `VectorValueReader` |
| `Registry.h` | §11.2 第 4 条 | 族级 loader / builder registry，取代 `IndexFactory` 的 God switch |

## 3. 不变式（违反了这层骨架就失去意义）

1. **查询 face 是纯 mixin，不继承 `IndexReaderBase`**（§4 注、§10 规则 3）。
   实现类**非虚**继承 root + 各 face，root→face 是一次 sibling cast。
2. **能力自述，不许 throw**（§3 原则 3、§10 规则 4）。不支持的操作在类型上就不存在，或经
   `std::optional` 表达。本目录中**没有任何** `ThrowInfo(NotImplemented/Unsupported)` 形态的默认实现。
3. **输出统一是 `TargetBitmap`**（§5 开头）。选择率是查询的运行时属性，不是族的静态属性。
4. **坐标一律由 `CoordDomain()` 自述**（§5.8）。元素级索引在元素坐标系输出，
   **索引不做元素→行的折叠**——折叠点由 plan 决定。
5. **不认识 segment 与 executor**（§3 原则 5、§10 规则 1）。本目录不得出现 `segcore/`、`exec/`、
   `query/` 的任何 include。
6. **不持有 `FileManagerContext` / `DiskFileManagerImpl`**（§3 原则 6、§10 规则 2）。
   IO 一律经注入的 `storage::FileSink` / `storage::FileSource`。
7. **标量族契约零 knowhere include**（§10 规则 6、§11.2 第 5 条）。knowhere 只允许出现在
   `VectorFaces.h` 及 vector 族实现里（§12.1(c) 已裁决允许）。
   → 因此 `GrowingIndex.h` 对 `VectorSearchReader` 用的是**前向声明**而不是 include。
8. **pb 类型不进契约**（README §5 规则 2）。native 枚举在本层定义，proto→native 的映射在 plan/exec 侧。
9. **cachinglayer 类型不进契约签名**（README §5 规则 4、§10 规则 5）。
   `LoadOptions::warmup` 用本层的 native `WarmupPolicy`，不是 `CacheWarmupPolicy`。

## 4. 骨架落地时与文档的偏离（每条都有理由，别当成笔误）

| 位置 | 文档写法 | 骨架写法 | 理由 |
|---|---|---|---|
| `ScalarPredicateReader::Range` | `OpType op` | native `CompareOp` | `milvus::OpType` 就是 `proto::plan::OpType`（`common/Types.h:106`）。README §5 规则 2 禁止 pb 上契约签名，§5.6 对 GIS 算子给的正是"定义 native enum + 边界映射"这个解法，此处同理 |
| `PatternMatchReader::PatternMatch` / `NgramReader` | `OpType op` | native `PatternOp` | 同上。取值集就是 §5.2 列出的 `{Match, PrefixMatch, PostfixMatch, InnerMatch}` |
| `JsonIndexReader::Resolve` / `CastTypesOf` | `DataType cast_type` | `JsonCastType` | §12.4 明说"写 `DataType` 等于提前下注，写 `JsonCastType` 是保守选择"。本轮取保守 |
| `JsonIndexReader::Exists` 的 `JsonValueType` | 未说明来源 | 本层 native enum | 现状 `index::JsonValueType` 是 tantivy 引擎枚举 `::JsonExistValueType` 的别名（`JsonFlatIndex.h:31`）。引擎类型不上契约签名 |
| `IndexLoader::Open` | `Open(...) -> shared_ptr<IndexReaderBase>` | `OpenIndex(...)` + `final` 转发的 `Open` | C++ 的协变返回只对裸指针/引用成立，对 `shared_ptr` 不成立，无法窄化 `ArtifactLoader::Open` 的返回类型。语义与配对关系不变 |
| `IndexArtifact` | 在 §6.1 的片段里 | `storage::Artifact` | §11.2 第 1 条把产物管道整体下沉 L1 并去掉 `Index` 前缀 |
| `IndexBuilder<T>::Seal()` | `IndexArtifactPtr` | `storage::ArtifactPtr` | 同上 |
| `IndexLoader::DeriveCaps` | 文档无此方法 | 新增 | §4.1 要求 caps"由加载期元数据算出、不 pin 就能读"，而族级知识只有 Loader 有。这是 §4.1 的直接后果，不是新设计 |
| `PatternOp` 取值集 | §5.2 列 `{Match, PrefixMatch, PostfixMatch, InnerMatch}` | 多一个 `RegexMatch` | 现状 `RegexMatch` 真的走进 `InvertedIndexTantivy::PatternMatch`（`InvertedIndexTantivy.h:261,301`）与 `NgramInvertedIndex::ExecutePhase1`（`NgramInvertedIndex.cpp:818,937`）。按文档写会静默删掉一条活路径 |
| `SpatialOp` 取值集 | §5.6 未列举 | `GISOp` 去掉 `Invalid` 与 `STIsValid`、保留 `DWithin` | `STIsValid` 现状明确不走索引（`GISFunctionFilterExpr.cpp:201-202`）；`DWithin` 走索引但 distance 在 exec 侧先转成 bbox（`:448-455`），所以本面不需要 distance 参数 |
| `ScalarValueReader` 的 `owned_t<T>` | 直接使用 | 在本文件里定义 | 仓库里没有这个 trait；按 §5.5 原话（`owned_t<string_view> = string`，其余 = `T`）实现，不多不少 |

**本轮没有采纳的一处任务指示**：任务描述要求 `IndexBuilder.h` 声明
`ScalarIndexBuilder<T>` / `TextIndexBuilder` / `JsonIndexBuilder` 三个面，但 §6.1 的 C++ 片段与
正文明确写的是**两族共用一个 `IndexBuilder<T>`**、"各族的 builder 是这个面的**实现**而非新的面"，
§11.3 的表也把 Builder 一行标为"面统一"。骨架按文档走。
（另：`index/JsonIndexBuilder.h` 这个文件名在仓库里已被占用。）

## 5. 明确不在本目录的东西

| 东西 | 归属 | 依据 |
|---|---|---|
| `SkipIndex`（zone-map） | columnar-format (L1) | §1 排除项；#51504 已把它用作 `CellSkipPredicate` |
| `JsonKeyStats` / JSON shredding | W1 内落 `segcore/json_stats/`；终局子列归 columnar-format、layout 目录归 segcore | §1 裁决与过渡处理 |
| 元素→行的 offsets 链 | 列的派生物，exec / columnar-format 持有 | §5.8「元素→行映射的归属」 |
| 缓存计费（pin、准入、淘汰） | segcore load 侧 translator | §4.2、§10 规则 5 |
| 上传编排 | indexbuilder 服务 | §6.2 |
| `ReaderCaps` 的 segment 级聚合（`FieldIndexCapability`） | segcore | §4.1 |
| 元素→行折叠算子 | exec（W4，可能提前，见 §12.5） | §5.8 |

## 6. 已知未决项（骨架里以注释形式钉在原地，别当成已定）

- **§12.3 `cell_size_` 的口径没有定义**。`storage::LoadedArtifact::CellByteSize()` 承载它，
  但今天同一个字段被不同族按两种口径填（压缩前文件大小 vs 实测常驻内存），而缓存层拿它做准入与淘汰。
  §12.3 要求**先于产物管道下沉**定义完。骨架只记录这个洞，不假装它已经定好。
- **§12.2 落点与命名未决**。`Artifact` / `ArtifactLoader` / `ArtifactStats` / `LoadedArtifact`
  是暂定名；`storage/` 里既有的 `IndexData` / `IndexEntry*` 命名族要连带处理。
- **跨仓前置**：`ResourceUsage` 今天在 milvus-common 的 `cachinglayer/Utils.h`，
  §11.2 要求先移到 `common/ResourceUsage.h`（`namespace milvus`），否则硬规则 4 与本设计自相矛盾。
- **§12.6 growing 水位滞后策略的族清单**：`NgramReader` 与 `JsonFlatIndex` 归哪边未定。
  索引侧不设任何表达该策略的位，所以不影响本层接口。
- **§13.1**：落地 `GrowingScalarIndex` 会直接激活 `size_per_chunk_` 越界（`Expr.h:2240-2252` 的注释
  已把触发条件写死）。同一个 PR 里必须先修。
