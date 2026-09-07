# PENDING ISSUES

本文记录本次重构尚未解决的设计问题。选择当前实现方案不等于关闭问题，也不代表整体 POC 已完成。

## PI-001：Go 加载信息调用的 native 线程生命周期

状态：**OPEN**。本轮选择恢复 `NewLoadIndexInfo`、`FinishLoadIndexInfo`、`DeleteLoadIndexInfo` 的受控 dynamic pool 执行，并等待每个 concrete closure 的结果；不再采用 2026-09-07 暂选的同步 C 调用。

### 已选实现边界

此前 Go 包装提交任务后忽略 `Await()` 错误。线程池拒绝提交时 C 回调没有执行，零初始化的 CStatus 却可能被当作成功；删除回调未执行则可能漏释放 native 句柄。公开 `newLoadIndexInfo`、`(*LoadIndexInfo).appendLoadIndexInfo`、void `deleteLoadIndexInfo` 不变，也不新增通用 helper：三者各自使用 `GetDynamicPool().Submit(...).Await()` 的 concrete closure。

- New 先处理 Await error；若 closure 已产出 handle，则先删除该 handle，再用 `merr.Wrap` 保留原错误。Await 成功后只处理一次 CStatus；CStatus 失败同样先清理 handle。
- Finish 在提交前准备 data/length（长度 0 时 data 为 nil），Await 后先 `runtime.KeepAlive` 输入，再依次处理 Await error 与唯一一次 CStatus，不重试 native 调用。
- Delete 先复制 handle 并清空 wrapper 字段；closure 返回执行 marker。marker 存在时绝不重复删除；marker 缺失才记录包装后的原 Await error，并同步调用一次 C Delete fallback。

这保证已移交 handle 在这些顺序路径上恰好释放一次且原始错误不被清理覆盖，但不把同一包装对象的并发 close/use 定义为安全，也不为三项操作建立通用 completion/error framework。

涉及代码：`internal/querynodev2/segments/load_index_info.go`，调用方 `GetCLoadInfoWithFunc` 位于同目录 `segment.go`；native 实现位于 `internal/core/src/segcore/load_index_c.cpp`。native New 的输出预置 null、全异常 CStatus，以及 Finish 的空指针/长度/parse 校验沿用现状。

### 为什么代码恢复不足以关闭问题

- **native TLS 与线程退出清理尚未验证。** dynamic pool 的 prehandler 会 `runtime.LockOSThread()`，代码注明用于 cgo thread disposal；仍需证明真实 worker 退出、复用与依赖库线程局部状态的清理结果。
- **线程亲和性与并发关闭尚未完成审计。** 不假设对象的创建和销毁必须同线程，也不假设它们一定可以任意线程执行；不同 pool 任务也不保证落在同一个线程。并发 close/use、提交成功后取消或 shutdown 的所有权仍需实际验证。
- **阻塞成本与并发约束需要生产数据。** 调用方等待结果不代表高并发加载、反复创建/释放时的延迟、线程数和 native 内存已经可接受。
- **C ABI 异常安全与错误分类是另一层问题。** pool + Await 不会自动接住越过 C ABI 的 C++ 异常，也不会修正底层错误码。

### 关闭条件

追踪构造/填充/析构及依赖的线程状态要求；用真实生产构建覆盖 pool 提交失败、CStatus 失败、并发加载、重复加载/释放、worker 退出和关闭竞争，记录线程数/native 内存及 exactly-once 清理结果。只有这些生产并发、失败与线程生命周期证据齐备后才能关闭；恢复 pool、静态检查或单独编译成功都不足以关闭本问题。

## PI-002：资源估算 C ABI 错误迁移尚未完成验收

状态：**OPEN，独立于 PI-001**。

原 `EstimateLoadIndexResource` 按值返回 `LoadResourceRequest`，catch 分支仍 `ThrowInfo`；Go 的 `segment_loader.go` 两处资源估算调用也忽略 `Await()` 错误。这可能把未执行的估算当作零资源，C++ 异常也没有被正常投影为 CStatus。PI-001 恢复 New/Finish/Delete 的 pool + Await 不解决这条独立估算路径。

已选实现改为 `CStatus + out LoadResourceRequest`，并迁移两个 Go 生产调用方来传播 `Await()` 与 CStatus 错误；磁盘分类也采用状态与输出分离，失败结果不写入属性缓存。成功前不得消费输出，admission 估算仍只依赖元数据，不为取得错误或能力信息加载索引 payload。

异常转换移除 catch 中隐式构造 `std::string` 的路径：类型化异常保留错误码，普通异常保留现有观测钩子，OOM/未知异常用不抛出的字符指针转换；错误文本分配失败仍返回非零状态。此前完整生产 C++/Go 构建只属于精简前的历史证据；当前仍需验证异常构造点到 Go 消费者的传播与真实失败路径。编译成功本来也不足以关闭本问题。

手工追踪已核对 CStatus 成功后才读取估算结果、Await 失败上返、失败结果不进入磁盘分类缓存及 load-info 的 defer 清理；这些是静态分支证据，不是故障注入结果。仍有明确边界：线程池提交错误是原始 ants error，不能宣称已统一类型化；scalar estimator 对未知 index type 的既有分支仍返回零成本成功，不能宣称所有坏 metadata 都会产生 CStatus failure。`C.IsLoadWithDisk` 有 Go helper 调用，但该 helper（`GetIndexResourceUsage`）当前没有非测试生产调用者，因此这一缓存路径尚不能用正常加载 e2e 证明执行。OOM、未知异常、错误文本分配失败和提交失败尚未做真实注入，PI-001 的线程生命周期问题也不因此关闭。

## PI-003 — Growing 读视图与生产接线

状态：**OPEN**。接口与生产接线已有静态实现；本轮 mechanical feed 复用没有新的构建、运行或故障注入证据，生命周期和故障边界仍未完成动态验收。

已选定 GrowingIndex + Appendable<Batch> + GrowingIndexSnapshotPin；公共发布记录唯一持有 reader，并原子绑定 CoveredRowEnd。接口层不再分开返回 reader 与 watermark。

Growing 的初始化、append、commit 与 reader 发布不消费 sealed 构建 Artifact。Text 与内存向量 Artifact 的可选消费式转换只服务一次性 sealed 构建后直接查询，不替代 GrowingIndex 自己的稳定版本协议。

当前保留 Tantivy Text 的独立 reader generation、追加失败恢复，以及 RTree 的不可变 shard 发布；无生产构造者的旧 scalar growing 实现不再作为能力或验收对象。Text 的 NULL 信息沿既有 sidecar/validity 保留到同代 Reader，VARCHAR/TEXT 类型不混用。GrowingIndexSet 到 Segment/exec 的生产接线和 Knowhere 逻辑可见前缀已经落入代码；不增加不可变引擎快照。

精简前取得的生产 e2e 包括基础 growing 插入/查询、首次发布、load/reopen、nullable/NULL 和查询可见前缀裁剪，以及 native/add TEXT 在 V1/V2、V3 的双向路径；标准 V1/V2 legacy 33 个 supported case 与 V3 36 个 supported case也曾取得新旧双向构建/读取结果。这些仅是历史成功路径证据，不验证当前精简，也不证明旧 pin 跨 Add 或 owner release 后仍存活、deferred iterator 的完整生命周期、部分失败、发布分配失败、Add poison 或乱序完成。向量精确计账按 PI-004 留待后续。

关闭条件：所有旧 pin 在新写入/提交后保持 Count、覆盖范围、validity、offset mapping 与生命周期保障；Tantivy Text/RTree 保持旧引擎查询视图，Knowhere 允许 ANN 近似命中变化但不得返回前缀外 ID；旧 pin 可跨发布与 owner 释放存活；乱序完成/中间失败不越过行空洞；失败不得发布未被完整接受的输入，某次发布构造失败不得替换当前版本（不排除较早已接受的代完成发布）；query_barrier 与 nested 元素数量不混用；RTree shard、向量 logical/physical mapping 与多代资源计账正确。用真实引擎集成与生产路径验收，不以接口编译代替；沿用本任务不新增/运行单元测试的约束。

### Segment 输入完成与查询可见性

原始列写入完成不等于这次 Insert 可以对查询发布。Go 的 Segment Insert 每次先重新 reserve offset；当前上层对非 released 的插入错误直接 panic，并没有按原 offset 重试整个 Insert 的接口。因此不能先推进查询可见的 ack，再让索引追加错误上返，也不能把索引内部的同范围幂等描述为整个 Insert 幂等。

Segment 使用独立的 raw-ready 连续前缀收集已完成原始列写入的范围。持有 feed 锁后捕获一个完整目标，分有界窗口送入所有启用的 owner；全部窗口接受成功后，先回收已经安全移交的 raw 数据，再一次推进现有的查询可见主 ACK。pending 输入 owner 只能在 ACK 后释放。feed cursor 可记录已被所有 owner 接受的窗口，但暂时领先查询可见 ACK；某个 owner 已发布的 reader 也可能领先查询可见行数，消费者必须裁到查询可见范围与 pin 覆盖范围的交集。

失败窗口的 end 必须保留至所有 owner 接受成功，不能在 raw-ready 前缀继续增长后扩大原窗口，导致已接受该窗口的 owner 收到部分重叠的重试；`required_flush_end` 只按 max 合并。Reopen 的私有新 owner 只回填当前可见前缀，再按 Flush → Register/schema 的顺序安装，不进入普通 pending 队列，也不推进主 ACK；其余仍由同一个保留失败窗口边界的 pump 处理。该内部协议不增加外部重试 API，不吞索引错误，也不改变现有上层失败处理。

本轮只以同步 `WithProtoVectorValues`/`WithLoadedVectorValues` 复用 protobuf 与 loaded typed delivery/nullable slicing，并在 `FeedGrowingIndexRange` 内保留一个 raw `EngineType`/`RawTrait` switch；三条 source/owner 分支仍独立。既有 serialized Feed/Flush/ACK 保持唯一协议，没有新增 `FeedTicket`。此前正常 growing 查询曾覆盖查询可见前缀裁剪和新增 nullable TEXT 字段的基本路径，但本轮未重新运行。乱序完成、跨字段部分失败、尾部短窗口重试和失败后的新增字段恢复仍只有静态追踪，尚需生产故障路径验证。

### 定时提交与加载完成不是同一个边界

普通 Insert 允许 text 索引按既有间隔滞后；但旧加载及 Reopen 路径会在返回前强制 commit/reload 完整回填。仅使用 CommitIfNeeded 的时间判断会丢掉后一个保证，首个查询也可能在尚无 Reader 时错误落入原始文本谓词路径。

采用两个明确操作：CommitIfNeeded 保留查询触发的提交节奏，Text 首次已接受数据必须能发布首代；Flush 同步保证已构建 owner 返回时全部已接受输入都有已发布 Reader，错误原样传播。向量未达到原构建阈值时仍可保持空 pin/raw fallback，不为 Flush 提前构建。Reopen 在注册 staged owner 前 Flush；加载的强制发布要求按目标行边界保留，只有完整 feed/flush 覆盖该边界才能清除，不能在后续 Insert 中绕过上次失败。强制发布也必须在查询可见 ack 推进之前完成。

向量初次 Build 失败另保留旧的安全回退：仅当尚未发布任何引擎记录且完整原始列仍在时，记录原错误、丢弃未发布引擎，继续空 pin/raw fallback；Insert 和 Load/Flush 均不因为此次结构重构而强制失败。已构建引擎的 Add 失败不满足这一条件：标记 owner 不可继续写入并传播原错误，不重复 Add、不推进 feed/可见 ack、不回收当前输入，不能在历史原始列已经回收后假装可以重建。Reader/发布记录分配失败也不能误判为初次 Build 的回退条件；已成功接受的输入必须能仅重试发布，不能重复写入引擎。

此修正不新增持久化格式。TEXT 入口区分 V3 远端 LOB 引用与旧加载路径的原始文本；后者有界编码为本地引用。NULL 行先按 validity 跳过引用解码，新增 nullable TEXT 字段的历史 NULL 行也有界回填；这不提供 TEXT 的非 NULL 默认值能力。基础 load/reopen、首次发布、native/add TEXT 与 NULL 行为只有精简前的历史集成证据；强制发布和恢复失败路径仍只有静态证据。

### Knowhere 逻辑前缀方案已确认，实施验收仍 OPEN

当前依赖的 `knowhere::Index` 拷贝构造只对同一个 node 增加引用计数，
`IndexNode` 的公共接口没有 Clone/Snapshot；Add 的契约允许与 Search、
RangeSearch、AnnIterator 并发。这支持活引擎的并发使用，不等于固定引擎版本。

2026-09-08 用户明确选择固定逻辑可见范围，目标是结构重构、不改变向量算法。方案比较及边界如下：

- **严格固定引擎（未选）。** 若要求旧 pin 不受后续写入影响，需要在 Knowhere 各 growing
  backend 实现可共享的不可变/COW 查询版本；这超出 Milvus 侧 Reader 接口拆分。
  通过发布时 Serialize/Deserialize 复制出独立引擎也能隔离写入，但每次发布
  都有 O(完整索引大小) 的处理和临时内存，并同时驻留 writer、新 reader 及仍被
  pin 的旧 reader。不能默认把这个昂贵复制方案当成完成结构重构的捷径。
- **固定逻辑可见范围（已选）。** 固定 Count、CoveredRowEnd、validity、offset mapping
  与生命周期，并在所有 search/range/iterator/value/refine 路径限制可见 ID。
  引擎仍可继续 Add，因此 ANN 内部遍历和近似结果可能变化。公共契约不再要求
  向量旧 pin 的 ANN 命中不变。必须在搜索阶段限制可见物理 ID，不能仅在 top-k
  后丢弃新行；迭代器及 refine 必须保留同一 pin、过滤范围和后备数据生命周期。
  保留原构建阈值、Train/Add 和原始列回退，不以每次发布全量复制引擎来实现。
- **不可变分片（未选）。** 分别冻结多个向量子索引并合并查询，可共享历史版本，但需要
  新的训练、召回、迭代器和 compaction 规则，不能视为向量算法不变的直接替换。

实现与生产消费者迁移已有静态证据。基础 growing、NULL 物理映射和逻辑可见前缀的生产 e2e，以及 SCANN 全阶段新旧对照，均是当前精简前的历史证据：BF16 在 4096/6144/8192/final-prefix 的 recall 为 1/.3/.3/.3，FLOAT 为 1/.6/.5/.6；两版 expected/actual IDs 相同，top-k 唯一性及 NULL/ID 范围硬检查通过。两边仍因严格 0.6 recall 阈值各有两个 case 失败并以非零状态结束，因此这不是 SCANN 测试通过，也没有修改算法或阈值。
剩余验收包括各可达 growing backend 的并发边界、追加失败状态、旧 pin 跨 Add/owner release、deferred iterator 生命周期、元数据发布成本，以及尚未完成的 GPU 路径。完整 POC 仍未完成。

## PI-004：向量实际资源计账缺少统一的原生统计依据

状态：**OPEN**。用户选择保留现有 live estimate、admission/final fallback 与 growing 公式，不在本轮补齐各原生 backend 的精确统计。旧构建和基本加载 e2e 没有直接观测计账事件时序，也不是当前精简的运行证据；这与逻辑前缀 pin 是两个独立决定。

Knowhere 的 `IndexNode::Size()` 明确允许近似结果，且并非所有 backend 都完整实现。不能直接将其包装成所有向量 Reader 的精确已拥有内存，也不能用远端索引文件长度替代实际驻留内存。向量 `MemoryUsage`/`CellByteSize` 可返回明确说明的 unavailable-zero；这不构成精确原生资源统计的完成证据。

范围决定：

1. **本轮采用。** 保留重构前可用的估算口径，并在契约与具体实现中注明估算或未知，不把它们描述成精确常驻资源。迁移前须核对旧路径的实际来源，不能假定所有旧向量计账都来自 Knowhere `Size()`。
2. **后续工作。** 补齐每个可达原生 backend 的统计能力，分别定义 heap、mmap/file-backed 资源和共享原始数据的归属；本轮不实施这项原生统计改造。

**删除的死管道。** 退休 C `AppendIndex` 及其唯一构造的 `V1SealedIndexTranslator` 已删除；这不是 V1/V2 文件格式退役。失去唯一生产写入者的 `loaded_resource_estimate` plumbing 随后删除，不再把 `index_size` 作为 typed post-load estimate 注入 Reader。普通 `SealedIndexTranslator` 的 admission/final resource estimate 与 fallback、`IndexLoadResource`、`LoadOptions::estimated_bytes`、interim 公式和 growing Segment 级估算都仍是 live 路径；不能把死字段删除描述为所有 post-load accounting 消失。

无论选择哪种方案，同一个活 Knowhere node 被多个发布记录引用时不能按 pin 数重复收费；仍被旧 pin 或数据视图持有的资源，也不能因当前 owner 释放引用便从统计中消失。加载前的 admission 估算和加载后的实际资源计账必须保持语义区分。

关闭仍需按可达 native backend 区分 heap、mmap/file-backed 与共享 owner，观测 admission、最终 charge、打开失败、旧 pin/owner 释放及最后一个引用消失时的资源事件，并注入估算与加载失败。此前正常加载与重开 e2e 未直接观测这些事件；故障注入也尚未覆盖。精确计账问题继续保留。
