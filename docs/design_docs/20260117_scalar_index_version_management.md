# 标量索引版本管理与兼容性设计

## 1. 背景与动机

### 1.1 问题概述

Milvus 的标量索引格式经历了多次演进，当前版本引入了"统一标量索引格式"（Unified Scalar Index Format），将多个索引文件打包成单一文件，以减少 S3 请求成本和 etcd 压力。然而，在实现过程中发现**版本号传递链路存在断裂**，导致：

1. **新格式索引无法正确加载**：QueryNode 加载索引时获取不到正确的 `scalar_index_engine_version`
2. **版本信息不完整**：部分 Proto 定义缺失 `current_scalar_index_version` 字段
3. **向后兼容性风险**：滚动升级场景下，新旧节点混合运行可能导致索引加载失败

### 1.2 版本号的双重含义

Milvus 中存在两套索引版本号，语义完全不同：

| 版本字段 | 适用范围 | 来源 | 含义 |
|---------|---------|------|------|
| `current_index_version` | 向量索引 | knowhere 库 | 向量索引算法/序列化格式版本 |
| `current_scalar_index_version` | 标量索引 | Milvus 定义 | 标量索引存储格式版本 |

**关键认知**：一个索引要么是向量索引，要么是标量索引，不可能同时是两者。但由于历史原因，Proto 定义中通常只有 `current_index_version`，没有 `current_scalar_index_version`。

### 1.3 标量索引版本演进

| 版本 | 格式描述 | 引入时间 |
|-----|---------|---------|
| 0 | 单 Segment 内 Tantivy 索引 | 早期版本 |
| 1 | 多文件分片格式（16MB 分片） | - |
| 2 | 多文件格式，Tantivy v7 | - |
| **3** | **统一单文件格式** | PR #44 |

---

## 2. 现状分析

### 2.1 系统架构中的索引流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Index Build Flow                                   │
│  DataCoord → IndexNode → Storage (S3/MinIO) + etcd (SegmentIndex)           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Index Load Flow                                    │
│  QueryCoord → QueryNode → Read from Storage + Load Index                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Build 阶段（正常工作）

#### 2.2.1 Go 侧版本传递

```go
// internal/datacoord/task_index.go
req := &workerpb.CreateJobRequest{
    CurrentIndexVersion:       currentVecIndexVersion,
    CurrentScalarIndexVersion: it.indexEngineVersionManager.GetCurrentScalarIndexEngineVersion(),
    // ...
}
```

#### 2.2.2 Proto 定义（正确）

```protobuf
// pkg/proto/worker.proto
message CreateJobRequest {
    int32 current_index_version = 12;
    int32 current_scalar_index_version = 27;  // ✅ 存在
}

// pkg/proto/index_cgo_msg.proto  
message BuildIndexInfo {
    int32 current_index_version = 7;
    int32 current_scalar_index_version = 21;  // ✅ 存在
}
```

#### 2.2.3 C++ 侧处理（正确）

```cpp
// internal/core/src/indexbuilder/index_c.cpp
auto scalar_index_engine_version = build_index_info->current_scalar_index_version();
config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] = scalar_index_engine_version;
```

**结论**：Build 阶段的版本传递链路完整，索引可以正确使用新格式构建。

### 2.3 元数据存储（正确）

```go
// internal/metastore/model/segment_index.go
type SegmentIndex struct {
    CurrentIndexVersion       int32  // 向量索引版本
    CurrentScalarIndexVersion int32  // 标量索引版本 ✅
}
```

etcd 中的 `SegmentIndex` 正确存储了两个版本号。

### 2.4 Load 阶段（问题所在）

#### 2.4.1 Proto 定义缺失

```protobuf
// pkg/proto/query_coord.proto
message FieldIndexInfo {
    int64 fieldID = 1;
    // ...
    int32 current_index_version = 11;
    // ❌ 缺失: current_scalar_index_version
}

// pkg/proto/segcore.proto
message FieldIndexInfo {
    // ...
    int32 current_index_version = 11;
    // ❌ 缺失: current_scalar_index_version
}

// pkg/proto/cgo_msg.proto
message LoadIndexInfo {
    // ...
    int32 index_engine_version = 15;
    // ❌ 缺失: current_scalar_index_version
}
```

#### 2.4.2 Go 侧传递断裂

```go
// internal/querynodev2/segments/segment.go
indexInfoProto := &cgopb.LoadIndexInfo{
    IndexEngineVersion: indexInfo.GetCurrentIndexVersion(),  // 只传了向量版本！
    IndexParams:        indexParams,                         // string → string map
    // ❌ 没有传递 CurrentScalarIndexVersion
}
```

#### 2.4.3 C++ 侧获取失败

```cpp
// internal/core/src/segcore/Utils.cpp
index_info.scalar_index_engine_version =
    milvus::index::GetValueFromConfig<int32_t>(
        config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
        .value_or(1);  // ❌ 找不到，默认为 1！
```

### 2.5 问题总结

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        版本传递链路对比                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│ Build 阶段:                                                                   │
│   CreateJobRequest.current_scalar_index_version                              │
│     → BuildIndexInfo.current_scalar_index_version                            │
│       → config[SCALAR_INDEX_ENGINE_VERSION]                                  │
│         → CreateIndexInfo.scalar_index_engine_version                        │
│           → Index 实现使用 ✅                                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│ Load 阶段:                                                                    │
│   SegmentIndex.CurrentScalarIndexVersion (etcd)                              │
│     → FieldIndexInfo.??? ❌ 缺失字段                                          │
│       → LoadIndexInfo.??? ❌ 缺失字段                                         │
│         → config[SCALAR_INDEX_ENGINE_VERSION] ❌ 未设置                       │
│           → Index::Load() 使用默认值 1 或 2 ❌                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 兼容性场景分析

### 3.1 滚动升级场景

| 场景 | IndexNode 版本 | QueryNode 版本 | 索引格式 | 能否加载 |
|-----|---------------|----------------|---------|---------|
| A | 旧 (v2) | 旧 (v2) | 多文件 | ✅ |
| B | 新 (v3) | 新 (v3) | 单文件 | ❌ (当前问题) |
| C | 旧 (v2) | 新 (v3) | 多文件 | ✅ |
| D | 新 (v3) | 旧 (v2) | 单文件 | ❌ (需向后兼容) |

### 3.2 兼容性要求

1. **向后兼容**：新版 QueryNode 必须能加载旧版 IndexNode 创建的索引
2. **向前兼容**：旧版 QueryNode 无法加载新格式索引时，应优雅降级或拒绝
3. **滚动升级**：支持混合版本集群运行

---

## 4. 解决方案设计

### 4.1 设计原则

1. **显式优于隐式**：版本号应通过专用字段传递，而非依赖 `index_params` 字符串解析
2. **向后兼容**：缺失版本号时使用安全的默认值
3. **单一数据源**：版本号从 etcd 元数据获取，而非推断
4. **最小改动**：尽量复用现有字段和机制

### 4.2 Proto 修改

#### 4.2.1 query_coord.proto

```protobuf
message FieldIndexInfo {
    int64 fieldID = 1;
    bool enable_index = 2 [deprecated = true];
    string index_name = 3;
    int64 indexID = 4;
    int64 buildID = 5;
    repeated common.KeyValuePair index_params = 6;
    repeated string index_file_paths = 7;
    int64 index_size = 8;
    int64 index_version = 9;
    int64 num_rows = 10;
    int32 current_index_version = 11;
    int64 index_store_version = 12;
    int32 current_scalar_index_version = 13;  // 新增
}
```

#### 4.2.2 segcore.proto

```protobuf
message FieldIndexInfo {
    int64 fieldID = 1;
    bool enable_index = 2 [deprecated = true];
    string index_name = 3;
    int64 indexID = 4;
    int64 buildID = 5;
    repeated common.KeyValuePair index_params = 6;
    repeated string index_file_paths = 7;
    int64 index_size = 8;
    int64 index_version = 9;
    int64 num_rows = 10;
    int32 current_index_version = 11;
    int64 index_store_version = 12;
    int32 current_scalar_index_version = 13;  // 新增
}
```

#### 4.2.3 cgo_msg.proto

```protobuf
message LoadIndexInfo {
    int64 collectionID = 1;
    int64 partitionID = 2;
    int64 segmentID = 3;
    schema.FieldSchema field = 5;
    bool enable_mmap = 6;
    string mmap_dir_path = 7;
    int64 indexID = 8;
    int64 index_buildID = 9;
    int64 index_version = 10;
    map<string, string> index_params = 11;
    repeated string index_files = 12;
    string uri = 13;
    int64 index_store_version = 14;
    int32 index_engine_version = 15;
    int64 index_file_size = 16;
    int64 num_rows = 17;
    int32 current_scalar_index_version = 18;  // 新增
}
```

### 4.3 Go 侧修改

#### 4.3.1 QueryCoord 填充 FieldIndexInfo

```go
// internal/querycoordv2/handlers/loadfield.go 或相关位置
func buildFieldIndexInfo(segIndex *model.SegmentIndex) *querypb.FieldIndexInfo {
    return &querypb.FieldIndexInfo{
        FieldID:                   segIndex.FieldID,
        IndexID:                   segIndex.IndexID,
        BuildID:                   segIndex.BuildID,
        IndexVersion:              segIndex.IndexVersion,
        CurrentIndexVersion:       segIndex.CurrentIndexVersion,
        CurrentScalarIndexVersion: segIndex.CurrentScalarIndexVersion,  // 新增
        // ...
    }
}
```

#### 4.3.2 QueryNode 传递版本

```go
// internal/querynodev2/segments/segment.go
indexInfoProto := &cgopb.LoadIndexInfo{
    CollectionID:              loadInfo.GetCollectionID(),
    PartitionID:               loadInfo.GetPartitionID(),
    SegmentID:                 loadInfo.GetSegmentID(),
    // ...
    IndexEngineVersion:        indexInfo.GetCurrentIndexVersion(),
    CurrentScalarIndexVersion: indexInfo.GetCurrentScalarIndexVersion(),  // 新增
}
```

### 4.4 C++ 侧修改

#### 4.4.1 FinishLoadIndexInfo

```cpp
// internal/core/src/segcore/load_index_c.cpp
CStatus
FinishLoadIndexInfo(CLoadIndexInfo c_load_index_info,
                    const uint8_t* serialized_load_index_info,
                    const uint64_t len) {
    // ... 现有代码 ...
    
    load_index_info->index_engine_version = info_proto->index_engine_version();
    
    // 新增：将 scalar 版本注入到 index_params
    auto scalar_version = info_proto->current_scalar_index_version();
    if (scalar_version > 0) {
        load_index_info->index_params[milvus::index::SCALAR_INDEX_ENGINE_VERSION] = 
            std::to_string(scalar_version);
    }
    
    // ... 其余代码 ...
}
```

#### 4.4.2 版本回退逻辑

```cpp
// 在各 Index::Load() 方法中
auto version = GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
    .value_or(milvus::kLastScalarIndexEngineVersionWithoutMeta);  // 默认 2

// 尝试从文件名推断版本（作为备用）
if (version == milvus::kLastScalarIndexEngineVersionWithoutMeta) {
    for (const auto& [filename, _] : index_datas) {
        std::string token;
        int32_t parsed_ver = 0;
        if (TryParsePackedIndexFileName(filename, &token, &parsed_ver)) {
            version = parsed_ver;
            break;
        }
    }
}
```

### 4.5 版本常量定义

#### Go 侧

```go
// pkg/common/common.go
const (
    MinimalScalarIndexEngineVersion     = int32(0)
    CurrentScalarIndexEngineVersion     = int32(3)
    LastScalarIndexEngineVersionWithoutMeta = int32(2)
    UnifiedScalarIndexVersion           = int32(3)
)
```

#### C++ 侧

```cpp
// internal/core/src/common/Pack.h
constexpr int32_t kUnifiedScalarIndexVersion = 3;
constexpr int32_t kLastScalarIndexEngineVersionWithoutMeta = 2;
```

### 4.6 版本解析优先级

在 Load 阶段，版本号的获取优先级为：

1. **Proto 字段**：`LoadIndexInfo.current_scalar_index_version`（最高优先级）
2. **Config/index_params**：`config[SCALAR_INDEX_ENGINE_VERSION]`（备用）
3. **文件名推断**：解析 `packed_<type>_v<ver>` 格式（降级方案）
4. **默认值**：`kLastScalarIndexEngineVersionWithoutMeta = 2`（最终回退）

---

## 5. 兼容性保障

### 5.1 旧索引加载

| 索引版本 | 文件格式 | version 获取方式 | 加载逻辑 |
|---------|---------|-----------------|---------|
| v0/v1 | 多文件分片 | 默认值 1 | 使用旧解析逻辑 |
| v2 | 多文件 | 默认值 2 | 使用旧解析逻辑 |
| v3 | 单文件 `packed_xxx_v3` | Proto 字段 | 使用新解包逻辑 |

### 5.2 滚动升级策略

**关键机制**：`GetCurrentScalarIndexEngineVersion()` 返回所有 QN 的 **MIN** 版本，
确保新构建的索引能被所有 QN 加载。

```
阶段 1: 升级 QueryNode（必须先升级！）
  - 新 QN 注册 CurrentScalarIndexVersion=3
  - 但只要有一个旧 QN，GetCurrentScalarIndexEngineVersion() 仍返回 2
  - 新构建的索引仍使用 v2 格式

阶段 2: 所有 QueryNode 升级完成
  - GetCurrentScalarIndexEngineVersion() 返回 3
  - 此后新构建的索引使用 v3 格式

阶段 3: 升级 DataCoord + QueryCoord
  - 确保 current_scalar_index_version 正确传递到 Load 路径

阶段 4: 升级 IndexNode（可选，通常与 DataNode 一起）
  - IndexNode 本身不决定版本，版本由 DataCoord 通过 GetCurrentScalarIndexEngineVersion() 决定

回滚策略:
  - 如需回滚 QueryNode，新 v3 格式索引无法被旧 QN 加载
  - 需等待 v3 索引自然过期或手动触发重建
  - 建议：升级窗口期避免大规模索引重建
```

**为什么 QueryNode 必须先升级？**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ DataCoord 调用 GetCurrentScalarIndexEngineVersion() 决定构建版本                 │
│                                                                                 │
│ 场景 A: QN 先升级                                                                │
│   QN-1 (新, v3), QN-2 (新, v3), QN-3 (旧, v2)                                   │
│   GetCurrentScalarIndexEngineVersion() = MIN(3, 3, 2) = 2                       │
│   → 仍构建 v2 格式，所有 QN 都能加载 ✅                                           │
│                                                                                 │
│ 场景 B: IndexNode 先升级（假设能强制使用 v3）                                     │
│   构建 v3 格式索引                                                               │
│   旧 QN 无法加载 ❌                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 版本兼容矩阵

| QueryNode 版本 | 可加载的索引版本 |
|---------------|-----------------|
| 旧 (不支持 v3) | v0, v1, v2 |
| 新 (支持 v3) | v0, v1, v2, v3 |

---

## 6. 实施计划

### 6.1 Phase 1: Proto 和基础设施

- [x] 修改 `query_coord.proto::FieldIndexInfo` - 添加 `current_scalar_index_version = 13`
- [x] 修改 `segcore.proto::FieldIndexInfo` - 添加 `current_scalar_index_version = 13`
- [x] 修改 `cgo_msg.proto::LoadIndexInfo` - 添加 `current_scalar_index_version = 18`
- [x] 生成 protobuf 代码 (Go + C++)

### 6.2 Phase 2: Go 侧修改

- [x] QueryCoord: 填充 `FieldIndexInfo.current_scalar_index_version`
  - 修改: `internal/querycoordv2/meta/coordinator_broker.go::GetIndexInfo()`
- [x] QueryNode: 传递版本到 `LoadIndexInfo`
  - 修改: `internal/querynodev2/segments/segment.go`
- [ ] 单元测试

### 6.3 Phase 3: C++ 侧修改

- [x] `FinishLoadIndexInfo`: 解析并注入版本到 `index_params`
  - 修改: `internal/core/src/segcore/load_index_c.cpp`
- [x] 各 Index::Load() 方法已使用 `GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)` 获取版本
- [ ] C++ 单元测试

### 6.4 Phase 4: 集成测试

- [ ] 新索引构建和加载测试
- [ ] 旧索引兼容性测试
- [ ] 滚动升级测试

---

## 7. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| Proto 变更导致不兼容 | 升级失败 | 新增字段使用 optional 语义，默认值安全 |
| 版本推断错误 | 索引加载失败 | 多级回退机制，优先使用显式版本 |
| 滚动升级期间索引不可用 | 查询失败 | 建议升级窗口期避免重建索引 |

---

## 8. 附录

### 8.1 相关代码位置

| 组件 | 文件 | 关键函数/结构 |
|-----|------|-------------|
| Proto | `pkg/proto/query_coord.proto` | `FieldIndexInfo` |
| Proto | `pkg/proto/segcore.proto` | `FieldIndexInfo` |
| Proto | `pkg/proto/cgo_msg.proto` | `LoadIndexInfo` |
| Go | `internal/querynodev2/segments/segment.go` | `LoadIndex` |
| C++ | `internal/core/src/segcore/load_index_c.cpp` | `FinishLoadIndexInfo` |
| C++ | `internal/core/src/segcore/Utils.cpp` | `LoadIndexData` |
| C++ | `internal/core/src/index/*.cpp` | 各 Index 实现 |

### 8.2 相关 PR 和 Issue

- PR #44: 统一标量索引格式
- BUGFIX_TODO_unified_scalar_index.md: 问题追踪清单

### 8.3 测试用例

```cpp
// 测试版本传递
TEST(ScalarIndexVersion, LoadWithExplicitVersion) {
    // 设置 current_scalar_index_version = 3
    // 验证索引使用新格式加载
}

TEST(ScalarIndexVersion, LoadLegacyIndexWithoutVersion) {
    // 不设置 current_scalar_index_version
    // 验证索引使用默认值 2 并正确加载旧格式
}

TEST(ScalarIndexVersion, LoadWithVersionInference) {
    // 设置文件名为 packed_inverted_v3
    // 验证能从文件名推断版本
}
```
