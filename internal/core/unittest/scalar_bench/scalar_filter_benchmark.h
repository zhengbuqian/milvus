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
#include <chrono>

#include "config/benchmark_config.h"

namespace milvus {
namespace scalar_bench {

// 前向声明
class SegmentWrapper;
class SegmentData;

// 基准测试结果
struct BenchmarkResult {
    // 运行标识
    int64_t run_id;           // 整个运行的时间戳（毫秒）
    int64_t case_run_id;      // 每个测试用例的时间戳（毫秒）

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
        const TestParams& params,
        int64_t case_run_id = 0,
        const std::string& results_dir = "");

    // 辅助方法
    bool IsIndexApplicable(const IndexConfig& index, const DataConfig& data);
    bool IsExpressionApplicable(const ExpressionTemplate& expr, const DataConfig& data);

    // Resolve field name placeholders in expression template
    std::string ResolveFieldPlaceholders(const std::string& expr_template,
                                         const SegmentWrapper& segment);

    // Validate field references in expression template
    bool ValidateFieldReferences(const std::string& expr_template,
                                 const SegmentWrapper& segment,
                                 std::string& error_msg);

private:
    // 性能统计
    BenchmarkResult CalculateStatistics(
        const std::vector<double>& latencies,
        const std::vector<int64_t>& matches,
        int64_t total_rows);
};

} // namespace scalar_bench
} // namespace milvus
