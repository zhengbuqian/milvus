# `index/vector/` —— vector 索引类型按四种接口重新划分

阶段 1 的 vector 部分：把 `VectorIndex` / `VectorMemIndex<T>` / `VectorDiskAnnIndex<T>` 的 virtual 接口
按四种接口重新安放。规格来源是
[01-scalar-index.md §11.3](../../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md)，
本 README 只做导读与迁移对照。

**这是按接口重新划分，不是重设计**（§11.3）：knowhere 交互逻辑原样搬移，行为不变，基准照跑。
接口框架里每个方法体都是一行 `// TODO: move existing logic here (see <file>:<lines>)`——
**这份接口框架同时是一张迁移地图**。

> **行号指向阶段 1 前的树（master `e255009e01`）**。`index/VectorIndex.h`、
> `index/VectorMemIndex.{h,cpp}`、`index/VectorDiskIndex.{h,cpp}` 被本次改动删除——那正是"按接口重新划分"的含义。
> 取回被引用的实现：`git show e255009e01:internal/core/src/index/VectorMemIndex.cpp`。

## 1. 文件

| 文件 | 职责 |
|---|---|
| `VectorFamilyReaders.h` | **接口定义层没有覆盖的四个查询接口**（meta / nullable / refine / emb-list），每个都记着逼出它的生产调用点——是缺口清单，不是已定的接口定义 |
| `KnowhereEngine.h/.cpp` | 被**组合**的 knowhere 引擎：index handle + index type/metric/dim/version + `PrepareSearchParams`/`CheckCompatible`/`MmapSupported`；今天 `VectorIndex` 基类那一半 |
| `VectorValidData.h` | nullable 向量的 logical→physical 偏移映射状态；今天 `VectorIndex::offset_mapping_` 那一半，按接口拆开（Append 归 Appender、Build 归 Builder、其余归 Reader） |
| `VectorIndexValidDataUtils.h` | 从 `index/` 平移进来的 valid-data 序列化/解析工具；只改了两处签名（`VectorIndex*` → `VectorValidData&`） |
| `VectorMemReader.h/.cpp` | Reader 接口：knowhere 内存索引类型（`VectorMemIndex<T>` 的查询半边） |
| `VectorMemBuilder.h/.cpp` | Builder 接口：form **B+**（连续缓冲，一次 `index_.Build(dataset)`） |
| `VectorMemArtifact.h/.cpp` | Artifact：`BinarySet` 形态（一组具名内存 blob）→ `FileSink::WriteEntry` |
| `VectorMemLoader.h/.cpp` | Loader 接口：materialize 与 mmap 两条装载路径 |
| `VectorDiskReader.h/.cpp` | Reader 接口：DiskANN |
| `VectorDiskBuilder.h/.cpp` | Builder 接口：form **D**（本地文件按路径交付，明确不要数据在内存里） |
| `VectorDiskArtifact.h/.cpp` | Artifact：本地大文件 → `FileSink::WriteEntryFromLocalFile` |
| `VectorDiskLoader.h/.cpp` | Loader 接口：DiskANN 的目录约定装载 |
| `RangeSearchParams.h/.cpp` | 从 `index/Utils.h` 接过来的 `CheckAndUpdateKnowhereRangeSearchParam`——**vector-only 的函数**，却是 `index/Utils.h` include `common/QueryInfo.h` 的唯一理由；挪走即断掉 §12.1(a) 那条 knowhere include 链的下半截 |
| `VectorFamilies.h/.cpp` | 按索引类型注册（`LoaderRegistry` / `BuilderRegistry<T>`，self-registering TU），取代 `IndexFactory` 的巨型分派 switch |

growing（Appender 接口）的向量实现不在本目录，在
[`index/growing/KnowhereGrowingVectorIndex.h`](../growing/KnowhereGrowingVectorIndex.h)（§7.1）。

## 2. 迁移对照：旧的公开接口 → 新的所属组件

| 今天（`e255009e01`） | 新的所属组件 | 备注 |
|---|---|---|
| `VectorIndex::Query` | `VectorSearchReader::Search` | `SearchInfo` 换成 §12.1(a) 的窄参数类型 |
| `VectorIndex::VectorIterators` | `VectorSearchReader::Iterators` | **形态保持现状**（裸 `IteratorPtr`），§12.1(b) 的判断 |
| `VectorIndex::IsIndexRefineEnabled` | `VectorSearchReader::RefineEnabled` | |
| `VectorIndex::HasRawData` / `GetVector` / `GetSparseVector` | `VectorValueReader` | DiskANN 的 `GetSparseVector` 是 throw 的未实现桩，随原样搬移并标注 |
| `VectorIndex::CalcDistByIDs` | `VectorRefineReader`（**接口定义缺口**） | |
| `VectorIndex::GetEmbListByIds` | `EmbeddingListReader`（**接口定义缺口**） | 基类上是 `ThrowInfo(NotImplemented)`，接口拆分后消失 |
| `GetIndexType` / `GetMetricType` / `GetDim` / `PrepareSearchParams` | `VectorIndexMetaReader`（**接口定义缺口**）+ `KnowhereEngine` | |
| `GetOffsetMapping` / `HasValidData` / `GetValidCount` / `IsRowValid` / `GetPhysical(Logical)Offset` | `VectorNullableReader`（**接口定义缺口**）+ `VectorValidData` | |
| `UpdateValidData` | `VectorValidData::Append`，由 Appender 调用 | |
| `BuildValidData` | `VectorValidData::Build`，由 Builder 调用 | |
| `CheckCompatible` / `IsMmapSupported` | `KnowhereEngine`；mmap 的**决策**上移到 Loader/`DeriveCaps` | 现状在构造出对象之后才问（`SealedIndexTranslator.cpp:202`），与 §4.1「不 pin 就能读」相悖 |
| `CleanLocalData` | `VectorDiskArtifact::ReleaseLocalStaging`（**流程缺口**） | `FileSink` 接口里没有 staging 目录的概念 |
| `Build(Config)` / `BuildWithDataset` | `IndexBuilder<T>::Add` + `Seal()` | `CacheRawDataToMemory` / `CacheRawDataToDisk` 归共享物化器 |
| `AddWithDataset` | `GrowingVectorIndex::Append` | 加载时构建 ≠ growing（§7 第 3 点） |
| `Serialize` | `storage::Artifact::Serialize(FileSink&)` | |
| `Upload` / `UploadUnified` | indexbuilder 服务 + `FileSink::Finish() → ArtifactStats` | |
| `Load(BinarySet)` / `Load(TraceContext, Config)` / `LoadFromFile` | `IndexLoader::OpenIndex(FileSource&, LoadOptions&)` | |
| `Count` | `IndexReaderBase::Count` | |
| `CellByteSize` / `SetCellSize` / `ComputeByteSize` | `storage::LoadedArtifact::CellByteSize`（**计量方式待定，§12.3**） | |
| `BuildWithRawDataForUT` | **删除** | 基类上就是 `ThrowInfo(Unsupported)` |
| `FileManagerContext` 成员 | **删除** | 只允许出现在 Loader/Artifact 实现文件（§10 规则 2） |

## 3. 三条不变式（本目录自查）

1. **实现类之间零继承**（§3 原则 2、§10 规则 3）。`VectorMemReader` 与 `VectorDiskReader` 之间没有共同基类；
   共享的东西是**成员**（`KnowhereEngine`、`VectorValidData`）。这是 `VectorIndex` 基类消失后唯一合法的形态。
2. **查询接口是纯 mixin**（§4 注）。实现类**非虚**多继承 `IndexReaderBase` + 各查询接口，
   `IndexReaderBase` 到查询接口是一次跨继承树的 `dynamic_cast`。
3. **knowhere 类型在本目录自由出现，且只在本目录**（§12.1(c) 已决定允许、§10 规则 6 划边界）。不做包装。

## 4. 已知缺口（接口框架里已在对应位置标注）

- **接口定义覆盖不足**：`contracts/VectorReaders.h` 只有 `VectorSearchReader` + `VectorValueReader`，
  上表四行「接口定义缺口」是生产调用点逼出来的。本目录先在本索引类型自己的目录里声明并标注，**没有改接口定义**。
- **`ReaderCaps` 没有向量位**：十个字段全是标量语义，向量 reader 只能全填 false。
  而 exec/segcore 在选择执行路径时真正会问的两件事（`HasRawData`、refine 是否开）都是加载期可推导的。
- **`VectorValueReader` 逼出一个 throw**：DiskANN 没有稀疏支持（`VectorDiskIndex.h:263-267`）。
- **`IndexBuilder<T>` 没有 side-input 的传入路径**：`BuilderInputSpec::side_inputs` 声明得了，交付不了
  （partition key isolation 的 `SCALAR_INFO`、emb-list 的 offsets）。
- **产物的构建与加载流程有三处形状问题**：见 `VectorMemArtifact.h`（slice 层归哪个组件）、
  `VectorMemLoader.h`（mmap 需要 n→1 本地文件）、`VectorDiskLoader.h`（`LoadIndexWithStream` 时字节根本不经过这套流程）。
- **`CleanLocalData` 在这套流程里没有所属组件**：`VectorDiskArtifact::ReleaseLocalStaging` 先接住，
  `FileSink` 接口里没有 staging 目录的概念。

## 5. growing（Appender 接口）

vector 的 Appender 实现在 [`index/growing/KnowhereGrowingVectorIndex.h`](../growing/KnowhereGrowingVectorIndex.h)：
`VectorFieldIndexing` 的 dense/sparse 两个方法合成一个 `Append`，冷启动全量构建（form B+）切给
`VectorMemBuilder`，产物经 `AdoptBuiltIndex` 交给 appender 作起点（§7.1、§7 第 3 点）。
快照复用 `VectorMemReader<T>`——**读实现两侧共享、写接口分开**，正是 §3 原则 1 想要的切法。
