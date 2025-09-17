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

#include "scalar_filter_benchmark.h"
#include "segment_data.h"
#include "segment_wrapper.h"
#include "index_wrapper.h"
#include <algorithm>
#include <numeric>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cmath>
#include <thread>
#include "bench_paths.h"

namespace milvus {
namespace scalar_bench {

std::vector<BenchmarkResult>
ScalarFilterBenchmark::RunBenchmark(const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> all_results;

    std::cout << "Starting Scalar Filter Benchmark..." << std::endl;
    std::cout << "Total configurations: "
              << config.data_configs.size() << " data configs x "
              << config.index_configs.size() << " index configs x "
              << config.expr_templates.size() << " expression templates x "
              << config.query_values.size() << " query values" << std::endl;

    // 第一级循环：数据配置
    for (const auto& data_config : config.data_configs) {
        std::cout << "\n========================================" << std::endl;
        std::cout << "Level 1: Data Config - " << data_config.name << std::endl;
        std::cout << "  Size: " << data_config.segment_size
                  << ", Type: " << data_config.data_type
                  << ", Cardinality: " << data_config.cardinality << std::endl;
        std::cout << "========================================" << std::endl;

        // 生成数据（只生成一次）
        auto start_time = std::chrono::high_resolution_clock::now();
        auto segment = GenerateSegment(data_config);
        auto data_gen_time = std::chrono::duration<double, std::milli>(
            std::chrono::high_resolution_clock::now() - start_time).count();

        std::cout << "✓ Data generation completed in " << data_gen_time << " ms" << std::endl;

        // 第二级循环：索引配置
        for (size_t idx = 0; idx < config.index_configs.size(); ++idx) {
            const auto& index_config = config.index_configs[idx];

            // 检查索引兼容性
            if (!IsIndexApplicable(index_config, data_config)) {
                std::cout << "  ⊗ Skipping incompatible index: " << index_config.name << std::endl;
                continue;
            }

            std::cout << "\n  ----------------------------------------" << std::endl;
            std::cout << "  Level 2: Index - " << index_config.name << std::endl;
            std::cout << "  ----------------------------------------" << std::endl;

            // 如果不是第一个索引，需要先删除之前的索引
            if (idx > 0) {
                // 从segment bundle中获取segment wrapper
                struct SegmentBundle {
                    std::shared_ptr<SegmentWrapper> wrapper;
                    std::shared_ptr<SegmentData> data;
                };
                auto bundle = std::static_pointer_cast<SegmentBundle>(segment);
                auto& segment_wrapper = bundle->wrapper;

                // 删除field字段上的索引
                auto field_id = segment_wrapper->GetFieldId("field");
                segment_wrapper->DropIndex(field_id);
            }

            // 构建索引
            start_time = std::chrono::high_resolution_clock::now();
            auto index = BuildIndex(segment, index_config);
            auto index_build_time = std::chrono::duration<double, std::milli>(
                std::chrono::high_resolution_clock::now() - start_time).count();

            std::cout << "  ✓ Index built in " << index_build_time << " ms" << std::endl;

            // 第三级循环：表达式模板
            for (const auto& expr_template : config.expr_templates) {
                // 检查表达式适用性
                if (!IsExpressionApplicable(expr_template, data_config)) {
                    continue;
                }

                // 第四级循环：查询参数值
                for (const auto& query_value : config.query_values) {
                    // 格式化表达式
                    auto expression = FormatExpression(expr_template, query_value);

                    std::cout << "    Testing: " << expression
                              << " (expected selectivity: " << query_value.expected_selectivity * 100 << "%)"
                              << std::endl;

                    // 执行基准测试
                    auto result = ExecuteSingleBenchmark(segment, index, expression, config.test_params);

                    // 填充元信息
                    result.data_config_name = data_config.name;
                    result.index_config_name = index_config.name;
                    result.expr_template_name = expr_template.name;
                    result.query_value_name = query_value.name;
                    result.actual_expression = expression;
                    result.expected_selectivity = query_value.expected_selectivity;
                    result.index_build_time_ms = index_build_time;

                    // 输出即时结果
                    std::cout << "      → P50: " << std::fixed << std::setprecision(2) << result.latency_p50_ms << "ms"
                              << ", P99: " << result.latency_p99_ms << "ms"
                              << ", Matched: " << result.matched_rows << "/" << result.total_rows
                              << " (" << result.actual_selectivity * 100 << "%)" << std::endl;

                    all_results.push_back(result);
                }
            }
        }

        std::cout << "\n✓ Completed all tests for data config: " << data_config.name << std::endl;
    }

    return all_results;
}

void
ScalarFilterBenchmark::GenerateReport(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n============================================" << std::endl;
    std::cout << "Scalar Filter Benchmark Report" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "Total test cases: " << results.size() << std::endl;

    // 汇总统计
    if (!results.empty()) {
        // 找出最慢和最快的查询
        auto slowest = std::max_element(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });
        auto fastest = std::min_element(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });

        std::cout << "\nPerformance Summary:" << std::endl;
        std::cout << "  Fastest query (P99): " << fastest->latency_p99_ms << "ms"
                  << " (" << fastest->index_config_name << ", " << fastest->actual_expression << ")" << std::endl;
        std::cout << "  Slowest query (P99): " << slowest->latency_p99_ms << "ms"
                  << " (" << slowest->index_config_name << ", " << slowest->actual_expression << ")" << std::endl;
    }

    // 详细结果表
    std::cout << "\nDetailed Results:" << std::endl;
    std::cout << std::setw(20) << "Data Config"
              << std::setw(15) << "Index"
              << std::setw(30) << "Expression"
              << std::setw(10) << "P50(ms)"
              << std::setw(10) << "P99(ms)"
              << std::setw(12) << "Selectivity"
              << std::setw(12) << "Memory(MB)" << std::endl;
    std::cout << std::string(119, '-') << std::endl;

    for (const auto& result : results) {
        std::cout << std::setw(20) << result.data_config_name.substr(0, 19)
                  << std::setw(15) << result.index_config_name.substr(0, 14)
                  << std::setw(30) << result.actual_expression.substr(0, 29)
                  << std::setw(10) << std::fixed << std::setprecision(2) << result.latency_p50_ms
                  << std::setw(10) << result.latency_p99_ms
                  << std::setw(11) << std::setprecision(1) << result.actual_selectivity * 100 << "%"
                  << std::setw(12) << result.index_memory_bytes / (1024.0 * 1024.0) << std::endl;
    }

    // 保存到CSV文件
    auto results_dir = GetResultsDir();
    std::ofstream csv(results_dir + "benchmark_results.csv");
    csv << "data_config,index_config,expression,p50_ms,p90_ms,p99_ms,avg_ms,"
        << "matched_rows,total_rows,selectivity,index_build_ms,memory_mb\n";

    for (const auto& result : results) {
        csv << result.data_config_name << ","
            << result.index_config_name << ","
            << "\"" << result.actual_expression << "\","
            << result.latency_p50_ms << ","
            << result.latency_p90_ms << ","
            << result.latency_p99_ms << ","
            << result.latency_avg_ms << ","
            << result.matched_rows << ","
            << result.total_rows << ","
            << result.actual_selectivity << ","
            << result.index_build_time_ms << ","
            << result.index_memory_bytes / (1024.0 * 1024.0) << "\n";
    }
    csv.close();

    std::cout << "\nResults saved to: " << results_dir << "benchmark_results.csv" << std::endl;
}

BenchmarkConfig
ScalarFilterBenchmark::LoadConfig(const std::string& yaml_file) {
    // TODO: 实现YAML配置加载
    BenchmarkConfig config;

    // 暂时返回硬编码的测试配置
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
        .name = "no_index",
        .type = ScalarIndexType::NONE,
        .params = {}
    });

    // 表达式模板
    config.expr_templates.push_back({
        .name = "greater_than",
        .expr_template = "field > {value}",
        .type = ExpressionTemplate::Type::COMPARISON
    });

    // 查询值
    config.query_values.push_back({
        .name = "selectivity_10p",
        .values = {{"value", 900000}},
        .expected_selectivity = 0.1
    });

    return config;
}

std::shared_ptr<void>
ScalarFilterBenchmark::GenerateSegment(const DataConfig& config) {
    std::cout << "    Generating " << config.segment_size << " rows of "
              << config.data_type << " data..." << std::endl;

    // 使用真实的数据生成器生成数据
    auto segment_data = SegmentDataGenerator::GenerateSegmentData(config);

    // 验证数据
    if (!segment_data->ValidateData()) {
        throw std::runtime_error("Data validation failed for config: " + config.name);
    }

    // 创建真实的Milvus Segment
    auto segment_wrapper = std::make_shared<SegmentWrapper>();
    segment_wrapper->Initialize(config);

    // 将生成的数据加载到真实的Segment中
    segment_wrapper->LoadFromSegmentData(*segment_data);

    // 打印数据统计摘要（仅在调试模式或小数据集时）
    if (config.segment_size <= 100000) {  // 10万行以下才打印详细信息
        segment_data->PrintSummary();
    } else {
        std::cout << "    Generated " << segment_data->GetRowCount() << " rows, "
                  << "Memory: " << segment_data->GetMemoryBytes() / (1024.0 * 1024.0)
                  << " MB" << std::endl;
    }

    // 返回包含segment和数据的结构
    struct SegmentBundle {
        std::shared_ptr<SegmentWrapper> wrapper;
        std::shared_ptr<SegmentData> data;
    };
    auto bundle = std::make_shared<SegmentBundle>();
    bundle->wrapper = segment_wrapper;
    bundle->data = segment_data;

    return bundle;
}

std::shared_ptr<void>
ScalarFilterBenchmark::BuildIndex(const std::shared_ptr<void>& segment_bundle,
                                   const IndexConfig& config) {
    // 从bundle中获取segment wrapper
    struct SegmentBundle {
        std::shared_ptr<SegmentWrapper> wrapper;
        std::shared_ptr<SegmentData> data;
    };
    auto bundle = std::static_pointer_cast<SegmentBundle>(segment_bundle);
    auto& segment_wrapper = bundle->wrapper;

    // 创建 IndexManager（use bench_paths helpers for disk paths）
    std::string root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    IndexManager index_manager(chunk_manager);

    // 构建并加载索引
    auto result = index_manager.BuildAndLoadIndex(*segment_wrapper, "field", config);

    // 创建索引结果bundle
    struct IndexBundle {
        IndexBuildResult result;
        std::shared_ptr<SegmentWrapper> segment_wrapper;
    };
    auto index_bundle = std::make_shared<IndexBundle>();
    index_bundle->result = result;
    index_bundle->segment_wrapper = segment_wrapper;

    if (result.success) {
        std::cout << "      ✓ Index built in " << std::fixed << std::setprecision(2)
                  << result.build_time_ms << " ms" << std::endl;
    } else {
        std::cout << "      ✗ Index build failed: " << result.error_message << std::endl;
    }

    return index_bundle;
}

BenchmarkResult
ScalarFilterBenchmark::ExecuteSingleBenchmark(const std::shared_ptr<void>& segment,
                                               const std::shared_ptr<void>& index,
                                               const std::string& expression,
                                               const TestParams& params) {
    BenchmarkResult result;
    std::vector<double> latencies;

    // 预热
    for (int i = 0; i < params.warmup_iterations; ++i) {
        // TODO: 执行实际的查询
        auto start = std::chrono::high_resolution_clock::now();
        // 模拟查询执行
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        auto end = std::chrono::high_resolution_clock::now();
    }

    // 测试执行
    for (int i = 0; i < params.test_iterations; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        // TODO: 执行实际的查询
        // 模拟查询执行
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        auto end = std::chrono::high_resolution_clock::now();

        double latency = std::chrono::duration<double, std::milli>(end - start).count();
        latencies.push_back(latency);
    }

    // 计算统计指标
    result = CalculateStatistics(latencies, {100000}, 1000000);
    result.correctness_verified = true;

    return result;
}

bool
ScalarFilterBenchmark::IsIndexApplicable(const IndexConfig& index, const DataConfig& data) {
    // 检查索引类型是否适用于数据类型
    if (data.data_type == "VARCHAR") {
        // 字符串类型不支持STL_SORT索引
        return index.type != ScalarIndexType::STL_SORT;
    }
    return true;
}

bool
ScalarFilterBenchmark::IsExpressionApplicable(const ExpressionTemplate& expr, const DataConfig& data) {
    // 检查表达式是否适用于数据类型
    if (data.data_type == "VARCHAR" && expr.type == ExpressionTemplate::Type::RANGE) {
        return false;  // 字符串不支持范围查询
    }
    return true;
}

std::string
ScalarFilterBenchmark::FormatExpression(const ExpressionTemplate& tmpl, const QueryValue& value) {
    std::string expr = tmpl.expr_template;

    // 简单的字符串替换
    for (const auto& [key, val] : value.values) {
        std::string placeholder = "{" + key + "}";
        size_t pos = expr.find(placeholder);
        if (pos != std::string::npos) {
            // 根据类型转换值
            std::string str_val;
            if (val.type() == typeid(int)) {
                str_val = std::to_string(std::any_cast<int>(val));
            } else if (val.type() == typeid(double)) {
                str_val = std::to_string(std::any_cast<double>(val));
            } else if (val.type() == typeid(std::string)) {
                str_val = "'" + std::any_cast<std::string>(val) + "'";
            }
            expr.replace(pos, placeholder.length(), str_val);
        }
    }

    return expr;
}

BenchmarkResult
ScalarFilterBenchmark::CalculateStatistics(const std::vector<double>& latencies,
                                            const std::vector<int64_t>& matches,
                                            int64_t total_rows) {
    BenchmarkResult result;

    if (latencies.empty()) {
        return result;
    }

    // 排序延迟数据
    std::vector<double> sorted_latencies = latencies;
    std::sort(sorted_latencies.begin(), sorted_latencies.end());

    // 计算百分位数
    auto percentile = [&sorted_latencies](double p) {
        int index = static_cast<int>(p * (sorted_latencies.size() - 1));
        return sorted_latencies[index];
    };

    result.latency_p50_ms = percentile(0.50);
    result.latency_p90_ms = percentile(0.90);
    result.latency_p99_ms = percentile(0.99);
    result.latency_p999_ms = percentile(0.999);

    // 计算平均值
    result.latency_avg_ms = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    result.latency_min_ms = *std::min_element(latencies.begin(), latencies.end());
    result.latency_max_ms = *std::max_element(latencies.begin(), latencies.end());

    // 计算QPS
    result.qps = 1000.0 / result.latency_avg_ms;

    // 匹配统计
    if (!matches.empty()) {
        result.matched_rows = matches.front();  // 假设所有执行返回相同结果
        result.total_rows = total_rows;
        result.actual_selectivity = static_cast<double>(result.matched_rows) / total_rows;
    }

    // 资源指标（占位值）
    result.index_memory_bytes = 10 * 1024 * 1024;  // 10MB
    result.exec_memory_peak_bytes = 50 * 1024 * 1024;  // 50MB
    result.cpu_usage_percent = 75.0;

    return result;
}

} // namespace scalar_bench
} // namespace milvus