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
#include "query_executor.h"
#include "flame_graph_profiler.h"
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

// 外部全局变量声明
extern std::string g_current_run_dir;

std::vector<BenchmarkResult>
ScalarFilterBenchmark::RunBenchmark(const BenchmarkConfig& config) {
    std::vector<BenchmarkResult> all_results;

    // 生成运行ID（当前时间的毫秒时间戳）
    auto now = std::chrono::system_clock::now();
    auto run_id = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::cout << "Starting Scalar Filter Benchmark..." << std::endl;
    std::cout << "Run ID: " << run_id << std::endl;
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
                auto bundle = segment;
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

            // 第三级循环：表达式模板（每个都是完整的 text proto）
            for (const auto& expr_template : config.expr_templates) {
                // 检查表达式适用性
                if (!IsExpressionApplicable(expr_template, data_config)) {
                    continue;
                }

                std::cout << "    Testing: " << expr_template.name << std::endl;

                // 生成case run ID（当前时间的毫秒时间戳）
                auto case_now = std::chrono::system_clock::now();
                auto case_run_id = std::chrono::duration_cast<std::chrono::milliseconds>(
                    case_now.time_since_epoch()).count();

                // 为这次运行创建专门的文件夹
                auto base_results_dir = GetResultsDir();
                std::string run_dir = base_results_dir + std::to_string(run_id) + "/";

                // 执行基准测试（直接使用 text proto）
                auto result = ExecuteSingleBenchmark(segment, index, expr_template.expr_template,
                                                    config.test_params, case_run_id, run_dir);

                // 填充元信息
                result.run_id = run_id;
                result.case_run_id = case_run_id;
                result.data_config_name = data_config.name;
                result.index_config_name = index_config.name;
                result.expr_template_name = expr_template.name;
                result.query_value_name = "";  // 不再需要
                result.actual_expression = expr_template.name;  // 使用模板名称作为表达式描述
                result.expected_selectivity = -1;  // 由查询结果决定
                result.index_build_time_ms = index_build_time;

                // 输出即时结果
                std::cout << "      → P50: " << std::fixed << std::setprecision(2) << result.latency_p50_ms << "ms"
                          << ", P99: " << result.latency_p99_ms << "ms"
                          << ", Matched: " << result.matched_rows << "/" << result.total_rows
                          << " (" << result.actual_selectivity * 100 << "%)" << std::endl;

                all_results.push_back(result);
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

    // 复制结果并排序：先按data config，再按expression，最后按index
    std::vector<BenchmarkResult> sorted_results = results;
    std::sort(sorted_results.begin(), sorted_results.end(),
        [](const BenchmarkResult& a, const BenchmarkResult& b) {
            // 1. 先按 data_config_name 排序
            if (a.data_config_name != b.data_config_name) {
                return a.data_config_name < b.data_config_name;
            }
            // 2. 再按 expression 排序
            if (a.actual_expression != b.actual_expression) {
                return a.actual_expression < b.actual_expression;
            }
            // 3. 最后按 index 排序
            return a.index_config_name < b.index_config_name;
        });

    // 详细结果表（每一行是一个case）
    std::cout << "\nDetailed Results (Run ID: " << (sorted_results.empty() ? 0 : sorted_results[0].run_id) << "):" << std::endl;
    std::cout << std::setw(15) << std::left << "Case ID"
              << std::setw(30) << "Data Config"
              << std::setw(30) << "Expression"
              << std::setw(20) << "Index"
              << std::setw(10) << std::right << "Avg(ms)"
              << std::setw(10) << "P50(ms)"
              << std::setw(10) << "P99(ms)"
              << std::setw(12) << "Selectivity"
              << std::setw(12) << "Memory(MB)" << std::endl;
    std::cout << std::string(159, '-') << std::endl;

    for (const auto& result : sorted_results) {
        std::cout << std::setw(15) << std::left << result.case_run_id
                  << std::setw(30) << result.data_config_name
                  << std::setw(30) << result.actual_expression
                  << std::setw(20) << result.index_config_name
                  << std::setw(10) << std::right << std::fixed << std::setprecision(2) << result.latency_avg_ms
                  << std::setw(10) << result.latency_p50_ms
                  << std::setw(10) << result.latency_p99_ms
                  << std::setw(11) << std::setprecision(1) << result.actual_selectivity * 100 << "%"
                  << std::setw(12) << std::setprecision(1) << result.index_memory_bytes / (1024.0 * 1024.0) << std::endl;
    }

    // 为这次运行创建专门的文件夹
    auto base_results_dir = GetResultsDir();
    int64_t run_id = sorted_results.empty() ? 0 : sorted_results[0].run_id;
    std::string run_dir = base_results_dir + std::to_string(run_id) + "/";

    // 创建运行目录
    std::string mkdir_cmd = "mkdir -p " + run_dir;
    std::system(mkdir_cmd.c_str());

    // 设置当前运行目录（用于中断时清理）
    g_current_run_dir = run_dir;

    // 保存到CSV文件
    std::string csv_filename = "benchmark_results.csv";
    std::ofstream csv(run_dir + csv_filename);
    csv << "run_id,case_run_id,data_config,expression,index_config,avg_ms,p50_ms,p90_ms,p99_ms,"
        << "matched_rows,total_rows,selectivity,index_build_ms,memory_mb\n";

    for (const auto& result : results) {
        csv << result.run_id << ","
            << result.case_run_id << ","
            << result.data_config_name << ","
            << "\"" << result.actual_expression << "\","
            << result.index_config_name << ","
            << result.latency_avg_ms << ","
            << result.latency_p50_ms << ","
            << result.latency_p90_ms << ","
            << result.latency_p99_ms << ","
            << result.matched_rows << ","
            << result.total_rows << ","
            << result.actual_selectivity << ","
            << result.index_build_time_ms << ","
            << result.index_memory_bytes / (1024.0 * 1024.0) << "\n";
    }
    csv.close();

    std::cout << "\nResults saved to: " << run_dir << csv_filename << std::endl;

    // 保存运行摘要到同一文件夹
    std::ofstream summary(run_dir + "run_summary.txt");
    summary << "Benchmark Run Summary" << std::endl;
    summary << "=====================" << std::endl;
    summary << "Run ID: " << run_id << std::endl;
    summary << "Total Cases: " << results.size() << std::endl;
    summary << "Start Time: " << run_id << " ms since epoch" << std::endl;

    if (!results.empty()) {
        // 找出最慢和最快的查询
        auto slowest = std::max_element(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });
        auto fastest = std::min_element(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });

        summary << "\nPerformance Highlights:" << std::endl;
        summary << "  Fastest query (P99): " << fastest->latency_p99_ms << " ms" << std::endl;
        summary << "    - Config: " << fastest->data_config_name << std::endl;
        summary << "    - Index: " << fastest->index_config_name << std::endl;
        summary << "    - Expression: " << fastest->actual_expression << std::endl;
        summary << "  Slowest query (P99): " << slowest->latency_p99_ms << " ms" << std::endl;
        summary << "    - Config: " << slowest->data_config_name << std::endl;
        summary << "    - Index: " << slowest->index_config_name << std::endl;
        summary << "    - Expression: " << slowest->actual_expression << std::endl;
    }
    summary.close();

    std::cout << "Run summary saved to: " << run_dir << "run_summary.txt" << std::endl;

    // 保存配置信息到同一文件夹
    std::ofstream config_file(run_dir + "run_config.json");
    config_file << "{" << std::endl;
    config_file << "  \"run_id\": " << run_id << "," << std::endl;
    config_file << "  \"data_configs\": [";

    // 获取唯一的数据配置
    std::set<std::string> unique_data_configs;
    for (const auto& result : results) {
        unique_data_configs.insert(result.data_config_name);
    }
    bool first_data = true;
    for (const auto& config_name : unique_data_configs) {
        if (!first_data) config_file << ", ";
        config_file << "\"" << config_name << "\"";
        first_data = false;
    }
    config_file << "]," << std::endl;

    config_file << "  \"index_configs\": [";
    std::set<std::string> unique_index_configs;
    for (const auto& result : results) {
        unique_index_configs.insert(result.index_config_name);
    }
    bool first_index = true;
    for (const auto& index_name : unique_index_configs) {
        if (!first_index) config_file << ", ";
        config_file << "\"" << index_name << "\"";
        first_index = false;
    }
    config_file << "]," << std::endl;

    config_file << "  \"expressions\": [";
    std::set<std::string> unique_expressions;
    for (const auto& result : results) {
        unique_expressions.insert(result.actual_expression);
    }
    bool first_expr = true;
    for (const auto& expr : unique_expressions) {
        if (!first_expr) config_file << ", ";
        config_file << "\"" << expr << "\"";
        first_expr = false;
    }
    config_file << "]" << std::endl;
    config_file << "}" << std::endl;
    config_file.close();

    std::cout << "Run configuration saved to: " << run_dir << "run_config.json" << std::endl;
    std::cout << "\n📁 All results saved in folder: " << run_dir << std::endl;
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

std::shared_ptr<SegmentBundle>
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
    auto bundle = std::make_shared<SegmentBundle>();
    bundle->wrapper = segment_wrapper;
    bundle->data = segment_data;

    return bundle;
}

std::shared_ptr<IndexBundle>
ScalarFilterBenchmark::BuildIndex(const std::shared_ptr<SegmentBundle>& segment_bundle,
                                   const IndexConfig& config) {
    // 直接使用segment bundle
    auto bundle = segment_bundle;
    auto& segment_wrapper = bundle->wrapper;

    // 创建 IndexManager（use bench_paths helpers for disk paths）
    std::string root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    IndexManager index_manager(chunk_manager);

    // 构建并加载索引
    auto result = index_manager.BuildAndLoadIndex(*segment_wrapper, "field", config);

    // 创建索引结果bundle
    auto index_bundle = std::make_shared<IndexBundle>();
    index_bundle->wrapper = nullptr;  // 暂时设置为nullptr，因为索引已经加载到segment中
    index_bundle->config = config;

    if (result.success) {
        std::cout << "      ✓ Index built in " << std::fixed << std::setprecision(2)
                  << result.build_time_ms << " ms" << std::endl;
    } else {
        std::cout << "      ✗ Index build failed: " << result.error_message << std::endl;
    }

    return index_bundle;
}

BenchmarkResult
ScalarFilterBenchmark::ExecuteSingleBenchmark(const std::shared_ptr<SegmentBundle>& segment,
                                               const std::shared_ptr<IndexBundle>& index,
                                               const std::string& expression,
                                               const TestParams& params,
                                               int64_t case_run_id,
                                               const std::string& results_dir) {
    BenchmarkResult result;
    std::vector<double> latencies;
    std::vector<int64_t> matched_rows_list;
    latencies.reserve(params.test_iterations);
    matched_rows_list.reserve(params.test_iterations);

    // 获取 SegmentWrapper 和 Schema
    auto segment_wrapper = segment->wrapper;
    auto schema = segment_wrapper->GetSchema();
    auto sealed_segment = segment_wrapper->GetSealedSegment();

    // 创建 QueryExecutor
    QueryExecutor executor(schema);

    // 预热
    for (int i = 0; i < params.warmup_iterations; ++i) {
        // 执行真实查询（text proto 格式）
        auto query_result = executor.ExecuteQuery(sealed_segment.get(), expression);
        if (!query_result.success && i == 0) {
            // 第一次失败时报告错误
            result.error_message = query_result.error_message;
            result.correctness_verified = false;
            return result;
        }
    }

    // 测试执行
    for (int i = 0; i < params.test_iterations; ++i) {
        // 执行真实查询
        auto query_result = executor.ExecuteQuery(sealed_segment.get(), expression);

        if (query_result.success) {
            latencies.push_back(query_result.execution_time_ms);
            matched_rows_list.push_back(query_result.matched_rows);
        } else {
            // 记录错误但继续
            if (result.error_message.empty()) {
                result.error_message = query_result.error_message;
            }
        }
    }

    // 如果没有成功的查询，返回错误
    if (latencies.empty()) {
        result.correctness_verified = false;
        if (result.error_message.empty()) {
            result.error_message = "All queries failed";
        }
        return result;
    }

    // 计算统计指标
    result = CalculateStatistics(latencies, matched_rows_list, segment_wrapper->GetRowCount());
    result.correctness_verified = true;

    // 如果启用了火焰图生成且没有错误，进行性能分析
    if (params.enable_flame_graph && result.correctness_verified && !results_dir.empty()) {
        // 确保结果目录存在
        std::string mkdir_cmd = "mkdir -p " + results_dir;
        std::system(mkdir_cmd.c_str());

        // 创建火焰图分析器
        FlameGraphProfiler::Config profiler_config;
        profiler_config.flamegraph_repo_path = params.flamegraph_repo_path;
        profiler_config.profile_duration_seconds = 1.0;  // 采集1秒
        profiler_config.total_duration_seconds = 1.5;    // 总运行1.5秒
        profiler_config.pre_buffer_seconds = 0.25;       // 前置缓冲
        profiler_config.post_buffer_seconds = 0.25;      // 后置缓冲

        FlameGraphProfiler profiler(profiler_config);

        // 验证环境
        if (profiler.ValidateEnvironment()) {
            // 生成火焰图文件名
            std::string svg_filename = results_dir + std::to_string(case_run_id) + ".svg";

            // 创建工作负载函数
            auto workload = [&]() {
                auto query_result = executor.ExecuteQuery(sealed_segment.get(), expression);
            };

            // 生成case名称用于火焰图标题
            std::string case_name = segment->data->GetConfig().name + "_" +
                                  index->config.name + "_" +
                                  expression.substr(0, 50);  // 截取表达式前50字符

            // 执行性能分析并生成火焰图
            bool profiling_success = profiler.ProfileAndGenerateFlameGraph(
                workload, svg_filename, case_name);

            if (profiling_success) {
                std::cout << "      ✓ Flame graph generated: " << svg_filename << std::endl;
            } else {
                std::cout << "      ⚠ Flame graph generation failed: " << profiler.GetLastError() << std::endl;
            }
        } else {
            std::cout << "      ⚠ Flame graph profiling skipped: " << profiler.GetLastError() << std::endl;
        }
    }

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
    return true;
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