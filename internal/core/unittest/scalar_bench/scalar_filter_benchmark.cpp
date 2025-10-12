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
#include "result_writers.h"
#include "core/segment_data.h"
#include "core/segment_wrapper.h"
#include "core/index_wrapper.h"
#include "core/query_executor.h"
#include "utils/flame_graph_profiler.h"
#include <algorithm>
#include <numeric>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cmath>
#include <set>
#include <string>
#include <regex>
#include <cctype>
#include "utils/bench_paths.h"

#include "config/benchmark_config_loader.h"

namespace milvus {
namespace scalar_bench {

// 外部全局变量声明
extern std::string g_current_run_dir;

std::vector<BenchmarkResult>
ScalarFilterBenchmark::RunBenchmark(const BenchmarkConfig& config, const std::string& config_file) {
    std::cout << "Starting Scalar Filter Benchmark..." << std::endl;

    // 生成 bundle ID（一次性的时间戳）
    auto now = std::chrono::system_clock::now();
    int64_t bundle_id = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now.time_since_epoch())
                            .count();

    std::cout << "\nBundle ID: " << bundle_id << std::endl;
    std::cout << "Config file: " << config_file << std::endl;

    // 创建 bundle 目录
    std::string bundle_dir = CreateBundleDirectory(bundle_id, config_file);
    g_current_run_dir = bundle_dir;

    std::cout << "Output directory: " << bundle_dir << std::endl;

    // 准备 bundle metadata
    BundleMetadata bundle_meta;
    bundle_meta.bundle_id = bundle_id;
    bundle_meta.config_file = config_file;
    bundle_meta.timestamp_ms = bundle_id;
    bundle_meta.test_params = config.test_params;

    // 读取 config 文件内容
    std::ifstream config_stream(config_file);
    if (config_stream.good()) {
        std::stringstream buffer;
        buffer << config_stream.rdbuf();
        bundle_meta.config_content = buffer.str();
    }

    // 存储所有 bundle 的结果
    std::vector<BenchmarkResult> all_results;

    // 遍历每个 case
    for (size_t case_index = 0; case_index < config.cases.size(); ++case_index) {
        const auto& benchmark_case = config.cases[case_index];

        std::cout << "\n╔═══════════════════════════════════════════════════════════╗" << std::endl;
        std::cout << "║ Case: " << std::left << std::setw(50) << benchmark_case.name << "║" << std::endl;
        std::cout << "╚═══════════════════════════════════════════════════════════╝" << std::endl;

        // 生成 case ID
        std::string case_id = std::to_string(bundle_id) + "_" + std::to_string(case_index);

        // 创建 case 目录
        std::string case_dir = CreateCaseDirectory(bundle_dir, case_id, benchmark_case.name);

        std::cout << "Case ID: " << case_id << std::endl;
        std::cout << "Case directory: " << case_dir << std::endl;

        // 收集这个 case 的所有结果
        std::vector<BenchmarkResult> case_results;

        // 遍历所有 suites
        for (const auto& suite : benchmark_case.suites) {
            std::cout << "\n=== Suite: " << (suite.name.empty() ? "default" : suite.name) << " ===" << std::endl;

            // 遍历所有 data configs
            for (const auto& data_config_name : suite.data_config_names) {
                std::cout << "\n----------------------------------------" << std::endl;
                std::cout << "Data Config: " << data_config_name << std::endl;
                std::cout << "----------------------------------------" << std::endl;

                // 解析 data config
                DataConfig data_config;
                try {
                    data_config = ResolveDataConfig(data_config_name, config);
                } catch (const std::exception& e) {
                    std::cerr << "Error resolving data config '" << data_config_name << "': " << e.what() << std::endl;
                    continue;
                }

                std::cout << "  Segment Size: " << data_config.segment_size
                          << ", Fields: " << data_config.fields.size() << std::endl;

                // 生成数据（只生成一次）
                auto start_time = std::chrono::high_resolution_clock::now();
                auto segment = GenerateSegment(data_config);
                auto data_gen_time =
                    std::chrono::duration<double, std::milli>(
                        std::chrono::high_resolution_clock::now() - start_time)
                        .count();

                std::cout << "  ✓ Data generation completed in " << data_gen_time << " ms" << std::endl;

                // 遍历所有 index configs
                for (size_t idx = 0; idx < suite.index_config_names.size(); ++idx) {
                    const auto& index_config_name = suite.index_config_names[idx];

                    // 解析 index config
                    IndexConfig index_config;
                    try {
                        index_config = ResolveIndexConfig(index_config_name, config);
                    } catch (const std::exception& e) {
                        std::cerr << "Error resolving index config '" << index_config_name << "': " << e.what() << std::endl;
                        continue;
                    }

                    // 检查索引兼容性
                    if (!IsIndexApplicable(index_config, data_config)) {
                        std::cout << "    ⊗ Skipping incompatible index: " << index_config.name << std::endl;
                        continue;
                    }

                    std::cout << "\n  Index: " << index_config.name << std::endl;

                    // 如果不是第一个索引，需要先删除之前的索引
                    if (idx > 0) {
                        auto bundle = segment;
                        auto& segment_wrapper = bundle->wrapper;

                        // Get the previous index config to know which fields had indexes
                        if (idx > 0 && idx - 1 < suite.index_config_names.size()) {
                            IndexConfig prev_index_config;
                            try {
                                prev_index_config = ResolveIndexConfig(suite.index_config_names[idx - 1], config);
                                for (const auto& [field_name, field_index_config] : prev_index_config.field_configs) {
                                    if (field_index_config.type != ScalarIndexType::NONE) {
                                        try {
                                            auto field_id = segment_wrapper->GetFieldId(field_name);
                                            segment_wrapper->DropIndex(field_id);
                                        } catch (const std::exception& e) {
                                            std::cerr << "    Warning: Could not drop index for field " << field_name
                                                      << ": " << e.what() << std::endl;
                                        }
                                    }
                                }
                            } catch (const std::exception& e) {
                                // Previous index config resolution failed, skip cleanup
                            }
                        }
                    }

                    // 构建索引
                    start_time = std::chrono::high_resolution_clock::now();
                    auto index = BuildIndex(segment, index_config);
                    auto index_build_time =
                        std::chrono::duration<double, std::milli>(
                            std::chrono::high_resolution_clock::now() - start_time)
                            .count();

                    std::cout << "    ✓ Index built in " << index_build_time << " ms" << std::endl;

                    // 遍历所有 expression templates
                    for (const auto& expr_template : suite.expr_templates) {
                        // 检查表达式适用性
                        if (!IsExpressionApplicable(expr_template, data_config)) {
                            continue;
                        }

                        std::cout << "      Testing: " << expr_template.name << std::endl;

                        // 生成 test run ID
                        auto test_now = std::chrono::system_clock::now();
                        auto test_run_id =
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                test_now.time_since_epoch())
                                .count();

                        // Validate field references before resolving
                        std::string validation_error;
                        if (!ValidateFieldReferences(expr_template.expr_template,
                                                     *segment->wrapper,
                                                     validation_error)) {
                            std::cerr << "        ⚠ Warning: Invalid field references in template '"
                                      << expr_template.name << "': " << validation_error << std::endl;
                            continue;
                        }

                        // Resolve field placeholders in expression template
                        std::string resolved_expression = ResolveFieldPlaceholders(
                            expr_template.expr_template, *segment->wrapper);

                        // 执行基准测试
                        auto result = ExecuteSingleBenchmark(segment,
                                                             index,
                                                             resolved_expression,
                                                             config.test_params,
                                                             test_run_id,
                                                             case_dir);

                        // 填充 bundle 和 case 信息
                        result.bundle_id = bundle_id;
                        result.config_file = config_file;
                        result.case_name = benchmark_case.name;
                        result.case_id = case_id;

                        // 填充其他元信息（保留兼容性）
                        result.run_id = bundle_id;
                        result.case_run_id = test_run_id;
                        result.suite_name = suite.name;
                        result.data_config_name = data_config.name;
                        result.index_config_name = index_config.name;
                        result.expr_template_name = expr_template.name;
                        result.query_value_name = "";
                        result.actual_expression = resolved_expression;
                        result.expected_selectivity = -1;
                        result.index_build_time_ms = index_build_time;

                        // 输出即时结果
                        std::cout << "        → P50: " << std::fixed << std::setprecision(2)
                                  << result.latency_p50_ms << "ms"
                                  << ", P99: " << result.latency_p99_ms << "ms"
                                  << ", Matched: " << result.matched_rows << "/"
                                  << result.total_rows << " ("
                                  << result.actual_selectivity * 100 << "%)" << std::endl;

                        case_results.push_back(result);
                    }
                }

                std::cout << "\n  ✓ Completed all tests for data config: " << data_config.name << std::endl;
            }
        }

        // 准备 case metadata
        CaseMetadata case_meta;
        case_meta.case_id = case_id;
        case_meta.case_name = benchmark_case.name;
        case_meta.bundle_id = bundle_id;
        case_meta.total_tests = case_results.size();

        // 检查是否有火焰图
        bool has_flamegraphs = false;
        for (const auto& r : case_results) {
            if (r.has_flamegraph) {
                has_flamegraphs = true;
                break;
            }
        }
        case_meta.has_flamegraphs = has_flamegraphs;

        // 收集 suite 信息
        std::map<std::string, CaseMetadata::SuiteInfo> suite_map;
        for (const auto& r : case_results) {
            std::string suite_name = r.suite_name.empty() ? "default" : r.suite_name;
            auto& suite_info = suite_map[suite_name];
            suite_info.suite_name = suite_name;

            // 去重添加配置
            if (std::find(suite_info.data_configs.begin(), suite_info.data_configs.end(), r.data_config_name) == suite_info.data_configs.end()) {
                suite_info.data_configs.push_back(r.data_config_name);
            }
            if (std::find(suite_info.index_configs.begin(), suite_info.index_configs.end(), r.index_config_name) == suite_info.index_configs.end()) {
                suite_info.index_configs.push_back(r.index_config_name);
            }
            if (std::find(suite_info.expr_templates.begin(), suite_info.expr_templates.end(), r.expr_template_name) == suite_info.expr_templates.end()) {
                suite_info.expr_templates.push_back(r.expr_template_name);
            }
        }

        for (const auto& [name, info] : suite_map) {
            case_meta.suites.push_back(info);
        }

        // 写入 case 文件
        if (!case_results.empty()) {
            WriteCaseMeta(case_dir, case_meta);
            WriteCaseMetrics(case_dir, case_results);
            WriteCaseSummary(case_dir, case_meta, case_results);
            std::cout << "  ✓ Case metadata, metrics, and summary written" << std::endl;
        }

        // 添加到 bundle metadata
        BundleMetadata::CaseInfo bundle_case_info;
        bundle_case_info.case_name = benchmark_case.name;
        bundle_case_info.case_id = case_id;
        for (const auto& s : case_meta.suites) {
            bundle_case_info.suites.push_back(s.suite_name);
        }
        bundle_case_info.total_tests = case_results.size();
        bundle_case_info.has_flamegraphs = has_flamegraphs;
        bundle_meta.cases.push_back(bundle_case_info);

        // 添加到总结果
        all_results.insert(all_results.end(), case_results.begin(), case_results.end());

        std::cout << "\n✓ Completed case: " << benchmark_case.name << std::endl;
        std::cout << "  Results saved to: " << case_dir << std::endl;
    }

    // 写入 bundle 级别文件
    WriteBundleMeta(bundle_dir, bundle_meta);
    WriteBundleSummary(bundle_dir, bundle_meta, all_results);

    // 生成 Bundle Info 并写入 index.json
    BundleInfo bundle_info = CreateBundleInfo(bundle_meta);
    std::vector<BundleInfo> bundles = {bundle_info};
    WriteIndexJson(GetResultsDir(), bundles);

    std::cout << "\n✅ Bundle completed!" << std::endl;
    std::cout << "  Bundle ID: " << bundle_id << std::endl;
    std::cout << "  Total cases: " << config.cases.size() << std::endl;
    std::cout << "  Total tests: " << all_results.size() << std::endl;
    std::cout << "  Output directory: " << bundle_dir << std::endl;

    return all_results;
}

void
ScalarFilterBenchmark::GenerateReport(
    const std::vector<BenchmarkResult>& results) {
    std::cout << "\n============================================" << std::endl;
    std::cout << "Scalar Filter Benchmark Report" << std::endl;
    std::cout << "============================================" << std::endl;
    std::cout << "Total test cases: " << results.size() << std::endl;

    // 排序：先按data config，再按expression，最后按index
    std::vector<BenchmarkResult> sorted_results = results;
    std::sort(sorted_results.begin(),
              sorted_results.end(),
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
    std::cout << "\nDetailed Results (Run ID: "
              << (sorted_results.empty() ? 0 : sorted_results[0].run_id)
              << "):" << std::endl;
    std::cout << std::setw(15) << std::left << "Case ID" << std::setw(20)
              << "Suite" << std::setw(30) << "Data Config" << std::setw(30)
              << "Expression" << std::setw(20) << "Index" << std::setw(10)
              << std::right << "Avg(ms)" << std::setw(10) << "P50(ms)"
              << std::setw(10) << "P99(ms)" << std::setw(12)
              << "Selectivity"
              //   << std::setw(12) << "Memory(MB)"
              << std::endl;
    std::cout << std::string(159, '-') << std::endl;

    for (const auto& result : sorted_results) {
        std::cout
            << std::setw(15) << std::left << result.case_run_id << std::setw(20)
            << (result.suite_name.empty() ? std::string("default")
                                          : result.suite_name)
            << std::setw(30) << result.data_config_name << std::setw(30)
            << result.expr_template_name << std::setw(20)
            << result.index_config_name << std::setw(10) << std::right
            << std::fixed << std::setprecision(2) << result.latency_avg_ms
            << std::setw(10) << result.latency_p50_ms << std::setw(10)
            << result.latency_p99_ms << std::setw(11) << std::setprecision(4)
            << result.actual_selectivity * 100
            << "%"
            //   << std::setw(12) << std::setprecision(1) << result.index_memory_bytes / (1024.0 * 1024.0)
            << std::endl;
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
    csv << "run_id,case_run_id,suite,data_config,expression,index_config,avg_"
           "ms,p50_ms,p90_ms,p99_ms,"
        << "matched_rows,total_rows,selectivity,index_build_ms,memory_mb\n";

    for (const auto& result : results) {
        const std::string expr_name = result.expr_template_name;
        csv << result.run_id << "," << result.case_run_id << ","
            << (result.suite_name.empty() ? std::string("default")
                                          : result.suite_name)
            << "," << result.data_config_name << "," << expr_name << ","
            << result.index_config_name << "," << result.latency_avg_ms << ","
            << result.latency_p50_ms << "," << result.latency_p90_ms << ","
            << result.latency_p99_ms << "," << result.matched_rows << ","
            << result.total_rows << "," << result.actual_selectivity << ","
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
        auto slowest = std::max_element(
            results.begin(), results.end(), [](const auto& a, const auto& b) {
                return a.latency_p99_ms < b.latency_p99_ms;
            });
        auto fastest = std::min_element(
            results.begin(), results.end(), [](const auto& a, const auto& b) {
                return a.latency_p99_ms < b.latency_p99_ms;
            });

        summary << "\nPerformance Highlights:" << std::endl;
        summary << "  Fastest query (P99): " << fastest->latency_p99_ms << " ms"
                << std::endl;
        summary << "    - Config: " << fastest->data_config_name << std::endl;
        summary << "    - Index: " << fastest->index_config_name << std::endl;
        summary << "    - Expression: " << fastest->actual_expression
                << std::endl;
        summary << "  Slowest query (P99): " << slowest->latency_p99_ms << " ms"
                << std::endl;
        summary << "    - Config: " << slowest->data_config_name << std::endl;
        summary << "    - Index: " << slowest->index_config_name << std::endl;
        summary << "    - Expression: " << slowest->actual_expression
                << std::endl;
    }
    summary.close();

    std::cout << "Run summary saved to: " << run_dir << "run_summary.txt"
              << std::endl;

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
        if (!first_data)
            config_file << ", ";
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
        if (!first_index)
            config_file << ", ";
        config_file << "\"" << index_name << "\"";
        first_index = false;
    }
    config_file << "]," << std::endl;

    config_file << "  \"expressions\": [";
    std::set<std::string> unique_expressions;
    for (const auto& result : results) {
        unique_expressions.insert(result.expr_template_name);
    }
    bool first_expr = true;
    for (const auto& expr : unique_expressions) {
        const std::string e = expr;
        if (!first_expr)
            config_file << ", ";
        config_file << "\"" << e << "\"";
        first_expr = false;
    }
    config_file << "]" << std::endl;
    config_file << "}" << std::endl;
    config_file.close();

    std::cout << "Run configuration saved to: " << run_dir << "run_config.json"
              << std::endl;
    std::cout << "\n📁 All results saved in folder: " << run_dir << std::endl;

    // 生成 meta.json：run 元信息与去重后的配置清单
    {
        std::set<std::string> unique_suites2;
        std::set<std::string> unique_data_configs2;
        std::set<std::string> unique_index_configs2;
        std::set<std::string> unique_expressions2;
        for (const auto& r : results) {
            unique_suites2.insert(r.suite_name.empty() ? std::string("default")
                                                       : r.suite_name);
            unique_data_configs2.insert(r.data_config_name);
            unique_index_configs2.insert(r.index_config_name);
            unique_expressions2.insert(r.expr_template_name);
        }

        std::ofstream meta(run_dir + "meta.json");
        meta << "{\n";
        meta << "  \"id\": \"" << run_id << "\",\n";
        meta << "  \"timestamp_ms\": " << run_id << ",\n";
        meta << "  \"label\": \"\",\n";
        // 统计是否有火焰图
        bool any_flame = false;
        for (const auto& r : results) {
            if (r.has_flamegraph) {
                any_flame = true;
                break;
            }
        }
        meta << "  \"summary\": { \"total_cases\": " << results.size()
             << ", \"has_flamegraphs\": " << (any_flame ? "true" : "false")
             << " },\n";
        meta << "  \"suites\": [";
        bool first_suite = true;
        for (const auto& s : unique_suites2) {
            if (!first_suite)
                meta << ", ";
            meta << "\"" << s << "\"";
            first_suite = false;
        }
        meta << "],\n";
        meta << "  \"data_configs\": [";
        bool first = true;
        for (const auto& s : unique_data_configs2) {
            if (!first)
                meta << ", ";
            meta << "\"" << s << "\"";
            first = false;
        }
        meta << "],\n";
        meta << "  \"index_configs\": [";
        first = true;
        for (const auto& s : unique_index_configs2) {
            if (!first)
                meta << ", ";
            meta << "\"" << s << "\"";
            first = false;
        }
        meta << "],\n";
        meta << "  \"expressions\": [";
        first = true;
        for (const auto& s : unique_expressions2) {
            if (!first)
                meta << ", ";
            meta << "\"" << s << "\"";
            first = false;
        }
        meta << "]\n";
        meta << "}\n";
    }

    // 生成 metrics.json：按 case_run_id 索引的详细指标
    {
        std::ofstream metrics(run_dir + "metrics.json");
        metrics << "{\n  \"cases\": {\n";
        bool first_case = true;
        for (const auto& r : results) {
            if (!first_case)
                metrics << ",\n";
            first_case = false;
            metrics << "    \"" << r.case_run_id << "\": {\n";
            metrics << "      \"data_config\": \"" << r.data_config_name
                    << "\",\n";
            metrics << "      \"index_config\": \"" << r.index_config_name
                    << "\",\n";
            metrics << "      \"expression\": \"" << r.expr_template_name
                    << "\",\n";
            metrics << "      \"latency_ms\": { \"avg\": " << r.latency_avg_ms
                    << ", \"p50\": " << r.latency_p50_ms
                    << ", \"p90\": " << r.latency_p90_ms
                    << ", \"p99\": " << r.latency_p99_ms
                    << ", \"p999\": " << r.latency_p999_ms
                    << ", \"min\": " << r.latency_min_ms
                    << ", \"max\": " << r.latency_max_ms << " },\n";
            metrics << "      \"qps\": " << r.qps << ",\n";
            metrics << "      \"matched_rows\": " << r.matched_rows << ",\n";
            metrics << "      \"total_rows\": " << r.total_rows << ",\n";
            metrics << "      \"selectivity\": " << r.actual_selectivity
                    << ",\n";
            metrics << "      \"index_build_ms\": " << r.index_build_time_ms
                    << ",\n";
            metrics << "      \"memory\": { \"index_mb\": "
                    << (r.index_memory_bytes / (1024.0 * 1024.0))
                    << ", \"exec_peak_mb\": "
                    << (r.exec_memory_peak_bytes / (1024.0 * 1024.0))
                    << " },\n";
            metrics << "      \"cpu_pct\": " << r.cpu_usage_percent << ",\n";
            if (r.has_flamegraph && !r.flamegraph_path.empty()) {
                metrics << "      \"flamegraph\": \"" << r.flamegraph_path
                        << "\"\n";
            } else {
                metrics << "      \"flamegraph\": null\n";
            }
            metrics << "    }";
        }
        metrics << "\n  }\n}\n";
    }

    // 顶层 index.json：读取-合并-去重-再写入（按 run_id 合并摘要）
    {
        auto results_root = GetResultsDir();
        std::string index_path = results_root + "index.json";

        // 构造当前 run 的条目
        std::string new_entry;
        new_entry += "    {\n";
        new_entry += "      \"id\": \"" + std::to_string(run_id) + "\",\n";
        new_entry +=
            "      \"timestamp_ms\": " + std::to_string(run_id) + ",\n";
        new_entry += "      \"label\": \"\",\n";
        bool any_flame2 = false;
        for (const auto& r : results) {
            if (r.has_flamegraph) {
                any_flame2 = true;
                break;
            }
        }
        new_entry += "      \"summary\": { \"total_cases\": " +
                     std::to_string(results.size()) +
                     ", \"has_flamegraphs\": " +
                     std::string(any_flame2 ? "true" : "false") + " }\n";
        new_entry += "    }";

        // 读取现有 index.json
        std::string existing;
        {
            std::ifstream in(index_path);
            if (in.good()) {
                std::string line;
                while (std::getline(in, line)) {
                    existing += line;
                    existing += "\n";
                }
            }
        }

        auto has_non_ws = [](const std::string& s) {
            for (char c : s) {
                if (c != ' ' && c != '\n' && c != '\t' && c != '\r')
                    return true;
            }
            return false;
        };

        std::string merged_body;
        if (!existing.empty()) {
            // 寻找 runs 数组内容
            size_t runs_pos = existing.find("\"runs\"");
            if (runs_pos != std::string::npos) {
                size_t lb = existing.find('[', runs_pos);
                size_t rb = (lb != std::string::npos) ? existing.find(']', lb)
                                                      : std::string::npos;
                if (lb != std::string::npos && rb != std::string::npos &&
                    rb > lb) {
                    std::string body = existing.substr(lb + 1, rb - lb - 1);
                    std::string id_key =
                        "\"id\": \"" + std::to_string(run_id) + "\"";
                    if (body.find(id_key) == std::string::npos) {
                        if (has_non_ws(body)) {
                            merged_body = body + ",\n" + new_entry;
                        } else {
                            merged_body = new_entry;
                        }
                    } else {
                        // 已存在该 run，保持原内容
                        merged_body = body;
                    }
                }
            }
        }

        if (merged_body.empty()) {
            merged_body = new_entry;  // 无历史或解析失败，写入当前 run
        }

        std::ofstream out(index_path, std::ios::out | std::ios::trunc);
        if (!out.good()) {
            std::cerr << "Failed to write index.json at: " << index_path
                      << std::endl;
        } else {
            out << "{\n  \"runs\": [\n";
            out << merged_body << "\n";
            out << "  ]\n}";
        }
    }
}

BenchmarkConfig
ScalarFilterBenchmark::LoadConfig(const std::string& yaml_file) {
    return BenchmarkConfigLoader::FromYamlFile(yaml_file);
}

std::shared_ptr<SegmentBundle>
ScalarFilterBenchmark::GenerateSegment(const DataConfig& config) {
    std::cout << "    Generating " << config.segment_size << " rows with "
              << config.fields.size() << " fields..." << std::endl;

    // 使用真实的数据生成器生成数据
    auto segment_data = SegmentDataGenerator::GenerateSegmentData(config);

    // 验证数据
    if (!segment_data->ValidateData()) {
        throw std::runtime_error("Data validation failed for config: " +
                                 config.name);
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
        std::cout << "    Generated " << segment_data->GetRowCount()
                  << " rows, "
                  << "Memory: "
                  << segment_data->GetMemoryBytes() / (1024.0 * 1024.0) << " MB"
                  << std::endl;
    }

    // 返回包含segment和数据的结构
    auto bundle = std::make_shared<SegmentBundle>();
    bundle->wrapper = segment_wrapper;
    bundle->data = segment_data;

    return bundle;
}

std::shared_ptr<IndexBundle>
ScalarFilterBenchmark::BuildIndex(
    const std::shared_ptr<SegmentBundle>& segment_bundle,
    const IndexConfig& config) {
    // 直接使用segment bundle
    auto bundle = segment_bundle;
    auto& segment_wrapper = bundle->wrapper;

    // 创建 IndexManager（use bench_paths helpers for disk paths）
    std::string root_path = GetSegmentsDir();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    IndexManager index_manager(chunk_manager);

    // Check if this is a per-field index configuration
    if (!config.field_configs.empty()) {
        std::cout << "    Building indexes for " << config.field_configs.size()
                  << " fields:" << std::endl;
        // Build indexes for each configured field
        for (const auto& [field_name, field_index_config] :
             config.field_configs) {
            if (field_index_config.type != ScalarIndexType::NONE) {
                std::cout << "      Building index for field: " << field_name
                          << " with type: "
                          << static_cast<int>(field_index_config.type)
                          << std::endl;
                auto result = index_manager.BuildAndLoadIndexForField(
                    *segment_wrapper, field_name, field_index_config);
            }
        }
    } else {
        // No field-specific index configs, indices remain unbuilt
        std::cout << "    No field-specific index configurations found."
                  << std::endl;
    }

    // 创建索引结果bundle
    auto index_bundle = std::make_shared<IndexBundle>();
    index_bundle->wrapper =
        nullptr;  // 暂时设置为nullptr，因为索引已经加载到segment中
    index_bundle->config = config;

    return index_bundle;
}

BenchmarkResult
ScalarFilterBenchmark::ExecuteSingleBenchmark(
    const std::shared_ptr<SegmentBundle>& segment,
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
        // 执行真实查询（expr 通过 Go parser 转 PlanNode）
        auto query_result = executor.ExecuteQueryExpr(
            sealed_segment.get(), expression, true, -1);
        if (!query_result.success && i == 0) {
            // 第一次失败时报告错误
            result.error_message = query_result.error_message;
            result.correctness_verified = false;
            return result;
        }
    }

    // 测试执行
    for (int i = 0; i < params.test_iterations; ++i) {
        // 执行真实查询（expr）
        auto query_result = executor.ExecuteQueryExpr(
            sealed_segment.get(), expression, true, -1);

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
    result = CalculateStatistics(
        latencies, matched_rows_list, segment_wrapper->GetRowCount());
    result.correctness_verified = true;

    // 如果启用了火焰图生成且没有错误，进行性能分析
    if (params.enable_flame_graph && result.correctness_verified &&
        !results_dir.empty()) {
        // 确保结果目录存在
        std::string mkdir_cmd = "mkdir -p " + results_dir + "flamegraphs";
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
            std::string svg_filename = results_dir + "flamegraphs/" +
                                       std::to_string(case_run_id) + ".svg";

            // 创建工作负载函数
            auto workload = [&]() {
                auto query_result = executor.ExecuteQueryExpr(
                    sealed_segment.get(), expression, true, -1);
            };

            // 生成case名称用于火焰图标题
            std::string case_name =
                segment->data->GetConfig().name + "_" + index->config.name +
                "_" + expression.substr(0, 50);  // 截取表达式前50字符

            // 执行性能分析并生成火焰图
            bool profiling_success = profiler.ProfileAndGenerateFlameGraph(
                workload, svg_filename, case_name);

            if (profiling_success) {
                std::cout << "      ✓ Flame graph generated: " << svg_filename
                          << std::endl;
                result.has_flamegraph = true;
                result.flamegraph_path =
                    "flamegraphs/" + std::to_string(case_run_id) + ".svg";
            } else {
                std::cout << "      ⚠ Flame graph generation failed: "
                          << profiler.GetLastError() << std::endl;
                result.has_flamegraph = false;
            }
        } else {
            std::cout << "      ⚠ Flame graph profiling skipped: "
                      << profiler.GetLastError() << std::endl;
            result.has_flamegraph = false;
        }
    }

    return result;
}

bool
ScalarFilterBenchmark::IsIndexApplicable(const IndexConfig& index,
                                         const DataConfig& data) {
    // With multi-field support, index applicability is checked per field
    // This method returns true as the actual validation happens at field level
    return true;
}

bool
ScalarFilterBenchmark::IsExpressionApplicable(const ExpressionTemplate& expr,
                                              const DataConfig& data) {
    // Expression applicability is determined by field availability during placeholder resolution
    // This method returns true as the actual validation happens during query execution
    return true;
}

std::string
ScalarFilterBenchmark::ResolveFieldPlaceholders(
    const std::string& expr_template, const SegmentWrapper& segment) {
    std::string result = expr_template;

    // Pattern to match placeholders like {field_id:name} or {field_type:name}
    std::regex placeholder_pattern(R"(\{(field_id|field_type):([^}]+)\})");
    std::smatch match;

    // Keep replacing until no more placeholders are found
    while (std::regex_search(result, match, placeholder_pattern)) {
        std::string placeholder = match[0];
        std::string placeholder_type = match[1];
        std::string field_name = match[2];

        try {
            if (placeholder_type == "field_id") {
                // Get the field ID for the given field name
                auto field_id = segment.GetFieldId(field_name);
                // Replace placeholder with just the numeric field ID
                std::string replacement = std::to_string(field_id.get());
                result = std::regex_replace(
                    result,
                    std::regex(std::regex_replace(
                        placeholder,
                        std::regex(R"([\[\]\{\}\(\)\*\+\?\.\|\^\$])"),
                        R"(\$&)")),
                    replacement);
            } else if (placeholder_type == "field_type") {
                // For field_type, just replace with the field name directly
                // This is used for expressions that reference fields by name
                result = std::regex_replace(
                    result,
                    std::regex(std::regex_replace(
                        placeholder,
                        std::regex(R"([\[\]\{\}\(\)\*\+\?\.\|\^\$])"),
                        R"(\$&)")),
                    field_name);
            }
        } catch (const std::exception& e) {
            // If field not found, log warning and leave placeholder as is
            std::cerr << "Warning: Could not resolve placeholder "
                      << placeholder << ": " << e.what() << std::endl;
            // Move past this placeholder to avoid infinite loop
            size_t pos = result.find(placeholder);
            if (pos != std::string::npos) {
                result = result.substr(0, pos) + "[UNRESOLVED:" + placeholder +
                         "]" + result.substr(pos + placeholder.length());
            }
        }
    }

    return result;
}

bool
ScalarFilterBenchmark::ValidateFieldReferences(const std::string& expr_template,
                                               const SegmentWrapper& segment,
                                               std::string& error_msg) {
    // Pattern to match placeholders like {field_id:name} or {field_type:name}
    std::regex placeholder_pattern(R"(\{(field_id|field_type):([^}]+)\})");
    std::string::const_iterator search_start(expr_template.cbegin());
    std::smatch match;

    error_msg.clear();
    bool all_valid = true;
    std::set<std::string> checked_fields;

    while (std::regex_search(
        search_start, expr_template.cend(), match, placeholder_pattern)) {
        std::string placeholder_type = match[1];
        std::string field_name = match[2];

        // Check if we've already validated this field
        if (checked_fields.find(field_name) == checked_fields.end()) {
            checked_fields.insert(field_name);

            try {
                // Try to get the field ID to validate it exists
                auto field_id = segment.GetFieldId(field_name);
                // Field exists, continue
            } catch (const std::exception& e) {
                // Field doesn't exist
                if (!error_msg.empty()) {
                    error_msg += "; ";
                }
                error_msg += "Field '" + field_name + "' not found in schema";
                all_valid = false;
            }
        }

        search_start = match.suffix().first;
    }

    return all_valid;
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
    result.latency_avg_ms =
        std::accumulate(latencies.begin(), latencies.end(), 0.0) /
        latencies.size();
    result.latency_min_ms =
        *std::min_element(latencies.begin(), latencies.end());
    result.latency_max_ms =
        *std::max_element(latencies.begin(), latencies.end());

    // 计算QPS
    result.qps = 1000.0 / result.latency_avg_ms;

    // 匹配统计: 计算平均的 selectivity
    if (!matches.empty() && total_rows > 0) {
        int64_t total_matched = std::accumulate(matches.begin(), matches.end(), int64_t(0));
        result.matched_rows = total_matched / matches.size();  // 平均匹配行数
        result.total_rows = total_rows;
        result.actual_selectivity =
            static_cast<double>(total_matched) / (total_rows * matches.size());
    }

    // 资源指标（占位值）
    result.index_memory_bytes = 10 * 1024 * 1024;      // 10MB
    result.exec_memory_peak_bytes = 50 * 1024 * 1024;  // 50MB
    result.cpu_usage_percent = 75.0;

    return result;
}

DataConfig
ScalarFilterBenchmark::ResolveDataConfig(const std::string& preset_name,
                                         const BenchmarkConfig& config) {
    // Find the preset by name
    for (const auto& preset : config.preset_data_configs) {
        if (preset.name == preset_name) {
            // Load the data config from the file
            auto resolved_path = BenchmarkConfigLoader::ResolvePath(preset.path);

            // Use the helper function that handles overrides
            return BenchmarkConfigLoader::LoadDataConfigWithOverride(resolved_path, preset.override_node);
        }
    }

    throw std::runtime_error("Data config preset not found: " + preset_name);
}

IndexConfig
ScalarFilterBenchmark::ResolveIndexConfig(const std::string& preset_name,
                                          const BenchmarkConfig& config) {
    // Find the preset by name
    for (const auto& preset : config.preset_index_configs) {
        if (preset.name == preset_name) {
            // Construct an IndexConfig from the preset
            IndexConfig index_config;
            index_config.name = preset.name;
            index_config.field_configs = preset.field_configs;
            return index_config;
        }
    }

    throw std::runtime_error("Index config preset not found: " + preset_name);
}

std::string
ScalarFilterBenchmark::CreateBundleDirectory(int64_t bundle_id,
                                             const std::string& config_file) {
    auto base_results_dir = GetResultsDir();
    std::string bundle_dir = base_results_dir + std::to_string(bundle_id) + "/";

    // 创建 bundle 目录
    std::string mkdir_cmd = "mkdir -p " + bundle_dir;
    std::system(mkdir_cmd.c_str());

    // 创建 cases 子目录
    mkdir_cmd = "mkdir -p " + bundle_dir + "cases/";
    std::system(mkdir_cmd.c_str());

    return bundle_dir;
}

std::string
ScalarFilterBenchmark::CreateCaseDirectory(const std::string& bundle_dir,
                                           const std::string& case_id,
                                           const std::string& case_name) {
    std::string case_dir = bundle_dir + "cases/" + case_id + "/";

    // 创建 case 目录
    std::string mkdir_cmd = "mkdir -p " + case_dir;
    std::system(mkdir_cmd.c_str());

    // 创建 flamegraphs 子目录
    mkdir_cmd = "mkdir -p " + case_dir + "flamegraphs/";
    std::system(mkdir_cmd.c_str());

    return case_dir;
}

}  // namespace scalar_bench
}  // namespace milvus
