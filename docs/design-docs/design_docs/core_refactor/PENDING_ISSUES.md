# PENDING ISSUES

本文记录本次重构尚未解决的设计问题。选择当前实现方案不等于关闭问题，也不代表整体 POC 已完成。

## PI-001：Go 同步加载信息调用的 native 线程生命周期

状态：**OPEN**。2026-09-07 决定先采用方案 B：`NewLoadIndexInfo`、`FinishLoadIndexInfo`、`DeleteLoadIndexInfo` 在调用方直接同步执行，不再提交 dynamic pool 后立即 `Await()`。

### 为什么先选 B

此前 Go 包装忽略 `Await()` 的错误。线程池拒绝提交时 C 回调没有执行，零初始化的 CStatus 却可能被当作成功；删除回调未执行则可能漏释放 native 句柄。同步调用移除这几个操作的任务提交失败通道：创建/填充立即检查真实 CStatus，释放直接执行，并清空已移交的句柄。重复顺序清理可为空操作，但这不把同一包装对象的并发 close/use 定义为安全。

涉及代码：`internal/querynodev2/segments/load_index_info.go`，调用方 `GetCLoadInfoWithFunc` 位于同目录 `segment.go`；native 实现位于 `internal/core/src/segcore/load_index_c.cpp`。

### 为什么 B 不是最终方案

- **native TLS 与线程退出清理尚未证明等价。** 现有 dynamic pool 的 prehandler 会 `runtime.LockOSThread()`，代码注明用于 cgo thread disposal。直接 cgo 调用不会提供相同的 worker 线程生命周期；不能因为当前 Delete 看起来只是 `delete`，就断言构造、销毁、计时或依赖库没有线程局部状态。
- **线程亲和性要求未完成审计。** 不假设对象的创建和销毁必须同线程，也不假设它们一定可以任意线程执行；需要核查实际依赖。旧 pool 的不同任务也不保证落在同一个线程。
- **阻塞成本与并发约束改变。** 调用方原先也会 Await，但移除 pool 同时移除了它对执行并发及线程复用的约束；需要实测高并发加载、反复创建/释放下的延迟、线程数和 native 内存。
- **C ABI 异常安全是另一层问题。** 同步调用不能接住越过 C ABI 的 C++ 异常，也不会自动修正底层错误码。不要将本方案描述为修复所有加载错误。

### 后续候选方案

1. 保留同步执行，明确并验证这些元数据操作无特殊 native 线程生命周期要求，或提供有证据支持的线程状态清理。
2. 恢复受控执行器，但完整定义提交失败、CStatus 失败和清理责任。只有确认清理回调未开始时才能回退执行；已开始的任务不能盲目重复 delete。
3. 建立统一 native 资源管理执行器，集中处理线程退出、释放调度和关闭顺序。只有多个实际调用链需要时才引入，不为这三个操作预先增加通用框架。

### 关闭条件

追踪构造/填充/析构及依赖的线程状态要求；用真实生产构建进行并发加载、重复加载/释放与失败场景验证，记录线程数/native 内存和清理结果；据此决定长期方案。当前不写、不运行单元测试，后续验证采用生产构建和集成/e2e。完成同步改写或单独编译成功均不足以关闭本问题。

## PI-002：资源估算 C ABI 尚未完成错误迁移

状态：**OPEN，独立于 PI-001**。

`EstimateLoadIndexResource` 当前按值返回 `LoadResourceRequest`，catch 分支仍 `ThrowInfo`；Go 的 `segment_loader.go` 两处资源估算调用也忽略 `Await()` 错误。这可能把未执行的估算当作零资源，C++ 异常也没有被正常投影为 CStatus。PI-001 的同步 New/Finish/Delete 不解决这条路径。

后续需要将 C 接口改为 `CStatus + out LoadResourceRequest`，同步迁移所有生产调用方，成功前不得消费输出；保留类型化错误码，并验证异常构造点到 Go 消费者的传播。维持 admission 估算仅依赖元数据的约束，不能为取得错误或能力信息而加载索引 payload。完整 C++/Go 构建与真实失败路径验证仍是验收要求。
