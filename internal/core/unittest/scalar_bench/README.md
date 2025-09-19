# Milvus Scalar Filter Benchmark Framework

## 概述

这是一个用于测试 Milvus segcore 层标量过滤性能的基准测试框架，实现了四级嵌套的测试结构。

## 目录结构

```
scalar_bench/
├── scalar_filter_benchmark.h    # 主框架头文件
├── scalar_filter_benchmark.cpp  # 主框架实现
├── data_generator.h             # 数据生成器头文件
├── data_generator.cpp           # 数据生成器实现
├── segment_data.h               # 数据包装类头文件
├── segment_data.cpp             # 数据包装类实现
├── segment_wrapper.h            # Segment封装头文件
├── segment_wrapper.cpp          # Segment封装实现
├── index_wrapper.h              # 索引封装头文件
├── index_wrapper.cpp            # 索引封装实现
├── bench_paths.h                # 测试路径配置
├── main.cpp                     # 主程序入口
├── CMakeLists.txt              # CMake配置
└── README.md                   # 本文档
```

## 核心特性

### 四级嵌套测试结构

1. **第一级：数据配置** - 生成不同特征的测试数据（只生成一次）
2. **第二级：索引配置** - 在同一数据上构建不同类型的索引
3. **第三级：表达式模板** - 测试不同类型的查询表达式
4. **第四级：参数值** - 测试不同的查询参数和选择率

### 支持的数据类型

- 整数（INT8/16/32/64）
- 浮点数（FLOAT/DOUBLE）
- 字符串（VARCHAR）
- 布尔值（BOOL）

### 支持的数据分布

- UNIFORM - 均匀分布
- NORMAL - 正态分布
- ZIPF - 幂律分布
- SEQUENTIAL - 顺序分布

### 支持的索引类型

- NONE - 无索引（暴力搜索）
- BITMAP - 位图索引
- STL_SORT - 排序索引
- INVERTED - 倒排索引
- TRIE - 前缀树索引
- HYBRID - 混合索引

### 字符串生成模式

- **UUID_LIKE** - 类UUID格式字符串
  - 示例：`a3f4d5e6-b7c8-9d0e-1f2g-3h4i5j6k7l8m`

- **TEMPLATE** - 模板格式（前缀+数字+后缀）
  - 示例：`user-0000001_data`

- **SENTENCE** - 随机英文句子
  - 示例：`The quick data system processes requests.`

- **MIXED** - 混合模式（1/3 UUID + 1/3 模板 + 1/3 句子）

## 编译方法

```bash
# 在 internal/core 目录下
cd /home/zilliz/milvus/internal/core

# 创建构建目录
mkdir -p build
cd build

# 配置
cmake ..

# 编译
make scalar_filter_bench
```

## 使用方法

### 1. 运行默认测试

```bash
./scalar_filter_bench
```

### 2. 演示数据生成器

```bash
./scalar_filter_bench --demo
```

### 3. 使用自定义配置文件

```bash
./scalar_filter_bench config.yaml
```

## 测试配置示例

```cpp
// 创建测试配置
BenchmarkConfig config;

// 数据配置
config.data_configs.push_back({
    .name = "uniform_int64_1m",
    .segment_size = 1000000,
    .data_type = "INT64",
    .distribution = Distribution::UNIFORM,
    .cardinality = 100000,
    .null_ratio = 0.0
});

// 索引配置
config.index_configs.push_back({
    .name = "bitmap",
    .type = ScalarIndexType::BITMAP,
    .params = {{"chunk_size", "8192"}}
});

// 表达式模板
config.expr_templates.push_back({
    .name = "greater_than",
    .expr_template = "field > {value}",
    .type = ExpressionTemplate::Type::COMPARISON
});

// 查询值（控制选择率）
config.query_values.push_back({
    .name = "selectivity_10p",
    .values = {{"value", 900000}},
    .expected_selectivity = 0.1
});
```

## 输出结果

### 1. 控制台输出

```
========================================
Level 1: Data Config - uniform_int64_high_card
  Size: 100000, Type: INT64, Cardinality: 90000
========================================
✓ Data generation completed in 45.23 ms

  ----------------------------------------
  Level 2: Index - bitmap
  ----------------------------------------
  ✓ Index built in 12.45 ms
    Testing: field > 90000 (expected selectivity: 10%)
      → P50: 1.23ms, P99: 2.45ms, Matched: 10000/100000 (10.00%)
```

### 2. CSV文件输出

结果保存到 `benchmark_results.csv`，包含：
- 数据配置、索引配置、表达式信息
- P50、P90、P99延迟
- 匹配行数和选择率
- 索引构建时间和内存使用

## 性能指标

- **延迟指标**: P50, P90, P99, P999, 平均值, 最小值, 最大值
- **吞吐量指标**: QPS
- **资源指标**: 索引内存, 执行内存峰值, CPU使用率
- **准确性指标**: 匹配行数, 实际选择率 vs 预期选择率

## 扩展开发

### 添加新的数据类型

在 `data_generator.cpp` 中实现新的生成方法：

```cpp
std::vector<YourType> GenerateYourTypeData(
    int64_t size,
    const YourConfig& config);
```

### 添加新的索引类型

1. 在 `ScalarIndexType` 枚举中添加新类型
2. 实现索引构建逻辑
3. 更新兼容性检查

### 添加新的表达式类型

1. 在 `ExpressionTemplate::Type` 中添加新类型
2. 实现表达式格式化逻辑
3. 添加测试用例

## 最近更新

### 2024-12 更新内容
1. **完成真实索引构建** - 所有索引类型现在使用真实的Milvus索引实现
2. **修复API兼容性** - 更新到最新的Milvus API（CreateIndexInfo、BuildWithRawDataForUT等）
3. **解决索引冲突** - 添加DropIndex机制，避免重复加载索引导致的断言错误
4. **改进数据访问** - 使用chunk_data方法正确访问Segment中的数据

## 当前实现状态

### ✅ 已完成功能
1. **真实数据生成** - 支持INT64、FLOAT、VARCHAR、BOOL类型，多种分布
2. **真实Segment创建** - 数据已加载到Milvus SegmentSealed，包含系统字段(row id, timestamp)
3. **Schema构建** - 自动根据数据配置生成Schema
4. **真实索引构建** - Bitmap、Inverted、STL_SORT索引已完全实现
5. **索引管理** - IndexManager统一管理索引构建和加载
6. **索引加载优化** - 使用基类统一实现LoadToSegment，减少代码重复
7. **全局初始化** - MmapManager、ChunkManager等已正确初始化
8. **索引冲突处理** - 实现DropIndex机制避免重复加载
9. **Text Proto查询** - 使用Text Proto格式直接定义查询表达式，避免C++表达式解析
10. **真实查询执行** - 通过QueryExecutor执行真实的Segment查询
11. **性能测量** - 测量真实的查询延迟(P50/P99)和选择率
12. **结果报告** - 生成格式化的性能报告，支持排序和CSV导出

### 🚧 下一步优化
1. **性能监控增强** - 更精确的内存和CPU使用率监控
2. **并发测试** - 实现多线程并发查询测试
3. **更多索引类型** - 支持Trie、Hybrid等高级索引

### ⏳ 可选功能
1. **YAML配置** - 集成yaml-cpp支持配置文件
2. **HTML报告** - 生成可视化的HTML性能报告
3. **高级索引特性** - 索引序列化、持久化等

## 注意事项

1. 程序需要初始化MmapManager等全局管理器才能运行
2. 索引构建已使用真实实现，但查询执行仍是模拟的
3. 大规模测试时注意内存使用，建议分批执行
4. 每个字段同时只能有一个索引（需使用DropIndex清理旧索引）

## 完整实现路线图

### 第一阶段：集成Segcore基础设施 ✅ **已完成**

#### Step 1.1: 创建Segment数据结构封装 ✅
```cpp
// segment_wrapper.h
class SegmentWrapper {
    Schema schema_;
    std::shared_ptr<SegmentSealed> sealed_segment_;
    std::map<FieldId, std::vector<std::any>> field_data_;
};
```
- [x] 创建 `segment_wrapper.h/cpp`
- [x] 定义Schema结构（字段ID、类型、名称映射）
- [x] 实现InsertData方法（批量插入）
- **测试程序可运行**：使用wrapper包装真实Segment

#### Step 1.2: 实现真实数据写入Segment ✅
- [x] 在 `GenerateSegment` 中创建真实的 `SegmentSealed` 对象
- [x] 使用 `LoadFieldData` API 写入生成的数据
- [x] 处理不同数据类型的转换（INT64, VARCHAR, FLOAT, BOOL）
- **测试程序可运行**：数据已写入真实segment

#### Step 1.3: 集成Schema定义 ✅
- [x] 创建 `SchemaBuilder` 辅助构建Schema
- [x] 根据DataConfig自动生成对应的Schema
- [x] 添加主键字段和标量字段定义
- **测试程序可运行**：Schema正确，segment数据真实

### 第二阶段：实现索引构建 ✅ **已完成**

#### Step 2.1: 集成标量索引基类 ✅
```cpp
// index_wrapper.h
class IndexWrapperBase {
    virtual IndexBuildResult Build(...);
    virtual bool LoadToSegment(...);
};
```
- [x] 创建 `index_wrapper.h/cpp`
- [x] 封装不同索引类型的创建逻辑
- [x] 实现索引构建接口框架
- **测试程序可运行**：索引框架已创建

#### Step 2.2: 实现具体索引构建 ✅
- [x] BitmapIndexWrapper类已创建并实现真实索引构建
- [x] InvertedIndexWrapper类已创建并实现真实索引构建
- [x] STLSortIndexWrapper类已创建并实现真实索引构建
- [x] 使用`BuildWithRawDataForUT`方法构建真实索引
- [x] 从Segment正确提取原始数据（使用`chunk_data<T>`方法）
- **测试程序可运行**：真实索引已构建成功

#### Step 2.3: 索引管理和加载 ✅
- [x] 实现IndexManager统一管理索引构建
- [x] 实现索引加载到Segment（LoadToSegment方法）
- [x] 解决索引加载冲突问题（添加DropIndex机制）
- [x] 测量索引构建时间（真实计时）
- **当前状态**：索引构建和管理已完全实现

### 第三阶段：表达式解析和执行 ✅ **已完成**

#### Step 3.1: 使用Text Proto定义查询表达式 ✅
```cpp
// 在main.cpp中直接使用text proto格式的查询表达式
config.expr_templates = {
    {.name = "equal_5000",
     .expr_template = R"(
output_field_ids: 101
query {
  predicates {
    unary_range_expr {
      column_info {
        field_id: 101
        data_type: Int64
      }
      op: Equal
      value { int64_val: 5000 }
    }
  }
})",
     .type = ExpressionTemplate::Type::COMPARISON}
};
```
- [x] 实现text proto字符串读取和解析
- [x] 使用google::protobuf::TextFormat反序列化
- [x] 支持各种表达式类型的text proto模板
- **测试程序已运行**：表达式通过text proto定义

#### Step 3.2: 执行真实查询 ✅
```cpp
// query_executor.cpp中实现的真实查询执行
auto plan = BuildPlan(text_proto_plan);
auto retrieve_result = segment->Retrieve(
    nullptr,  // RetrieveContext
    plan.get(),
    MAX_TIMESTAMP,
    limit > 0 ? limit : DEFAULT_MAX_OUTPUT_SIZE,
    false  // ignore_non_pk
);
```
- [x] 使用ProtoParser::CreateRetrievePlan创建查询计划
- [x] 调用segment->Retrieve执行真实查询
- [x] 处理返回的查询结果
- [x] 测量查询执行时间和匹配行数
- **测试程序已运行**：返回真实查询结果

#### Step 3.3: 支持多种表达式类型的Text Proto模板 ✅
- [x] UnaryRangeExpr: 比较运算 (>, <, ==, !=, >=, <=)
- [x] TermExpr: IN/NOT IN操作
- [x] BinaryRangeExpr: 范围查询 (BETWEEN)
- [x] CompareExpr: 字段间比较（已在测试中实现）
- [x] BinaryExpr/UnaryExpr: 逻辑运算 (AND, OR, NOT)（支持框架已有）
- [ ] NullExpr: NULL值处理（可选）
- **测试程序已运行**：主要表达式类型都通过text proto定义并执行

### 第四阶段：性能测量和优化（可运行，真实性能数据）

#### Step 4.1: 集成性能监控
```cpp
// metrics_collector.h
class MetricsCollector {
    void RecordLatency(double ms);
    void RecordMemory(size_t bytes);
    void RecordCPU(double percent);
};
```
- [ ] 创建 `metrics_collector.h/cpp`
- [ ] 集成内存使用统计（RSS, VMS）
- [ ] 集成CPU使用率监控
- [ ] 实现精确的时间测量
- **测试程序仍可运行**：显示真实的性能指标

#### Step 4.2: 实现并发测试
- [ ] 创建线程池执行器
- [ ] 实现并发查询调度
- [ ] 统计并发下的QPS和延迟
- [ ] 处理线程安全问题
- **测试程序仍可运行**：支持并发测试模式

#### Step 4.3: 缓存和预热优化
- [ ] 实现数据缓存预热
- [ ] 实现索引缓存管理
- [ ] 优化内存访问模式
- **测试程序仍可运行**：性能数据更稳定

### 第五阶段：配置和报告增强（可运行，完整功能）

#### Step 5.1: YAML配置支持
```yaml
# config.yaml
data_configs:
  - name: test_data
    segment_size: 1000000
    # ...
```
- [ ] 集成yaml-cpp库
- [ ] 实现配置文件解析
- [ ] 支持配置验证和默认值
- **测试程序仍可运行**：支持配置文件驱动

#### Step 5.2: 报告生成增强
- [ ] 生成HTML格式报告
- [ ] 添加性能对比图表
- [ ] 实现结果数据库存储
- [ ] 支持历史结果对比
- **测试程序仍可运行**：生成详细的分析报告

#### Step 5.3: 测试用例管理
- [ ] 实现测试用例库
- [ ] 支持测试用例导入/导出
- [ ] 添加回归测试支持
- [ ] 实现CI/CD集成
- **测试程序仍可运行**：完整的测试管理功能

### 第六阶段：特定场景优化（可运行，生产就绪）

#### Step 6.1: 慢查询场景测试
- [ ] 实现大规模OR条件测试（800+条件）
- [ ] 实现超大NOT IN测试（300+元素）
- [ ] 实现复杂LIKE组合测试
- [ ] 实现深度JSON路径访问测试
- **测试程序仍可运行**：覆盖所有已知慢查询场景

#### Step 6.2: 极端场景处理
- [ ] 处理全NULL数据
- [ ] 处理超高基数数据（每行唯一）
- [ ] 处理极度倾斜数据（99%相同值）
- [ ] 处理超大数据集（100M+行）
- **测试程序仍可运行**：稳定处理极端情况

#### Step 6.3: 生产环境适配
- [ ] 添加资源限制控制
- [ ] 实现优雅退出机制
- [ ] 添加日志和调试支持
- [ ] 实现分布式测试支持
- **测试程序完成**：可用于生产环境性能测试

## 实现顺序建议

1. **优先级高**（1-2周）：
   - Step 1.1-1.3: Segment基础集成
   - Step 3.1-3.2: 表达式解析和基本执行

2. **优先级中**（2-3周）：
   - Step 2.1-2.3: 索引构建
   - Step 3.3: 完善表达式支持
   - Step 4.1: 性能监控

3. **优先级低**（后续迭代）：
   - Step 4.2-4.3: 并发和优化
   - Step 5.1-5.3: 配置和报告
   - Step 6.1-6.3: 特定场景

## 关键集成点

### 需要引入的Milvus组件
```cpp
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "index/ScalarIndex.h"
#include "index/BitmapIndex.h"
#include "index/InvertedIndex.h"
#include "parser/PlanParser.h"
#include "exec/expression/Expr.h"
#include "common/Schema.h"
```

### 关键API调用示例
```cpp
// 创建Segment
auto segment = std::make_shared<SegmentGrowing>(schema, index_meta);

// 插入数据
segment->Insert(offset, size, field_id, data_ptr, timestamps);

// 构建索引
auto index = IndexFactory::CreateScalarIndex(type, dtype);
index->Build(size, data_ptr);

// 执行查询
auto plan = parser.CreatePlan(expr_str);
auto result = segment->Search(plan, placeholder_group);
```

## 调试和验证

每个步骤完成后的验证方法：

1. **数据正确性**：对比生成的数据和segment中的数据
2. **索引正确性**：验证索引查询结果与暴力搜索一致
3. **性能合理性**：确保索引查询快于暴力搜索
4. **内存合理性**：监控内存使用不超过预期
5. **结果一致性**：多次运行结果稳定

## 预期最终效果

完成所有步骤后，运行命令：
```bash
./scalar_filter_bench config.yaml
```

将会：
1. 根据配置生成真实的测试数据
2. 构建真实的标量索引
3. 执行真实的过滤查询
4. 输出真实的性能数据
5. 生成可用于决策的分析报告