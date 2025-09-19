// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <chrono>
#include <any>

namespace milvus {
namespace scalar_bench {

// 前向声明
class SegmentWrapper;
class SegmentData;

// 数据分布类型
enum class Distribution {
    UNIFORM,    // 均匀分布
    NORMAL,     // 正态分布
    ZIPF,       // 幂律分布
    SEQUENTIAL  // 顺序分布
};

// 标量索引类型
enum class ScalarIndexType {
    NONE,       // 无索引（暴力搜索）
    BITMAP,     // 位图索引
    STL_SORT,   // 排序索引
    INVERTED,   // 倒排索引
    TRIE,       // 前缀树索引
    HYBRID      // 混合索引
};

// 数据配置
struct DataConfig {
    std::string name;
    int64_t segment_size;
    std::string data_type;  // INT64, FLOAT, VARCHAR等
    Distribution distribution;
    int64_t cardinality;
    double null_ratio;

    struct {
        double skewness = 0.0;
        double sparsity = 0.0;
        int64_t unique_values = 0;
    } characteristics;
};

// 索引配置
struct IndexConfig {
    std::string name;
    ScalarIndexType type;
    std::map<std::string, std::string> params;
};

// 表达式模板
struct ExpressionTemplate {
    std::string name;
    std::string expr_template;
    enum Type {
        COMPARISON,
        RANGE,
        SET_OPERATION,
        STRING_MATCH,
        ARRAY_OP,
        LOGICAL,
        NULL_CHECK
    } type;
};

// 查询参数值
struct QueryValue {
    std::string name;
    std::map<std::string, std::any> values;
    double expected_selectivity;
};

// 测试参数
struct TestParams {
    int warmup_iterations = 10;
    int test_iterations = 100;
    bool verify_correctness = true;
    bool collect_memory_stats = true;
};

// 基准测试配置
struct BenchmarkConfig {
    std::vector<DataConfig> data_configs;
    std::vector<IndexConfig> index_configs;
    std::vector<ExpressionTemplate> expr_templates;
    std::vector<QueryValue> query_values;
    TestParams test_params;
};

// 基准测试结果
struct BenchmarkResult {
    // 测试标识
    std::string data_config_name;
    std::string index_config_name;
    std::string expr_template_name;
    std::string query_value_name;
    std::string actual_expression;

    // 延迟指标
    double latency_p50_ms;
    double latency_p90_ms;
    double latency_p99_ms;
    double latency_p999_ms;
    double latency_avg_ms;
    double latency_min_ms;
    double latency_max_ms;

    // 吞吐量指标
    double qps;

    // 资源指标
    int64_t index_memory_bytes;
    int64_t exec_memory_peak_bytes;
    double cpu_usage_percent;

    // 结果指标
    int64_t matched_rows;
    int64_t total_rows;
    double actual_selectivity;
    double expected_selectivity;

    // 索引指标
    double index_build_time_ms;
    int64_t index_size_bytes;

    // 正确性验证
    bool correctness_verified;
    std::string error_message;
};

// 数据和segment的组合
struct SegmentBundle {
    std::shared_ptr<SegmentWrapper> wrapper;
    std::shared_ptr<SegmentData> data;
};

// 索引信息的组合
struct IndexBundle {
    std::shared_ptr<void> wrapper;  // 可以是 IndexWrapperBase 或其他索引对象
    IndexConfig config;
};

// 主测试框架类
class ScalarFilterBenchmark {
public:
    ScalarFilterBenchmark() = default;
    ~ScalarFilterBenchmark() = default;

    // 运行基准测试
    std::vector<BenchmarkResult> RunBenchmark(const BenchmarkConfig& config);

    // 生成报告
    void GenerateReport(const std::vector<BenchmarkResult>& results);

    // 从YAML加载配置
    static BenchmarkConfig LoadConfig(const std::string& yaml_file);

protected:
    // 生成测试数据
    virtual std::shared_ptr<SegmentBundle> GenerateSegment(const DataConfig& config);

    // 构建索引
    virtual std::shared_ptr<IndexBundle> BuildIndex(
        const std::shared_ptr<SegmentBundle>& segment,
        const IndexConfig& config);

    // 执行单个测试
    virtual BenchmarkResult ExecuteSingleBenchmark(
        const std::shared_ptr<SegmentBundle>& segment,
        const std::shared_ptr<IndexBundle>& index,
        const std::string& expression,
        const TestParams& params);

    // 辅助方法
    bool IsIndexApplicable(const IndexConfig& index, const DataConfig& data);
    bool IsExpressionApplicable(const ExpressionTemplate& expr, const DataConfig& data);

private:
    // 性能统计
    BenchmarkResult CalculateStatistics(
        const std::vector<double>& latencies,
        const std::vector<int64_t>& matches,
        int64_t total_rows);
};

} // namespace scalar_bench
} // namespace milvus