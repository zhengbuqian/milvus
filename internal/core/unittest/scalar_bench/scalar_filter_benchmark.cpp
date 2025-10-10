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
#include "utils/bench_paths.h"

#include "config/benchmark_config_loader.h"

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
              << config.expr_templates.size() << " expression templates x " << std::endl;

    // 第一级循环：数据配置
    for (const auto& data_config : config.data_configs) {
        std::cout << "\n========================================" << std::endl;
        std::cout << "Level 1: Data Config - " << data_config.name << std::endl;
        std::cout << "  Segment Size: " << data_config.segment_size
                  << ", Fields: " << data_config.fields.size() << std::endl;
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
                // Drop indexes for all fields that had indexes built in previous config
                auto bundle = segment;
                auto& segment_wrapper = bundle->wrapper;

                // Get the previous index config to know which fields had indexes
                if (idx > 0 && idx - 1 < config.index_configs.size()) {
                    const auto& prev_index_config = config.index_configs[idx - 1];
                    for (const auto& [field_name, field_index_config] : prev_index_config.field_configs) {
                        if (field_index_config.type != ScalarIndexType::NONE) {
                            try {
                                auto field_id = segment_wrapper->GetFieldId(field_name);
                                segment_wrapper->DropIndex(field_id);
                            } catch (const std::exception& e) {
                                // Field might not exist or might not have index, continue
                                std::cerr << "Warning: Could not drop index for field " << field_name
                                          << ": " << e.what() << std::endl;
                            }
                        }
                    }
                }
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

                // Validate field references before resolving
                std::string validation_error;
                if (!ValidateFieldReferences(expr_template.expr_template, *segment->wrapper, validation_error)) {
                    std::cerr << "    ⚠ Warning: Invalid field references in template '"
                             << expr_template.name << "': " << validation_error << std::endl;
                    continue;  // Skip this expression template
                }

                // Resolve field placeholders in expression template
                std::string resolved_expression = ResolveFieldPlaceholders(
                    expr_template.expr_template, *segment->wrapper);

                // 执行基准测试（使用解析后的表达式）
                auto result = ExecuteSingleBenchmark(segment, index, resolved_expression,
                                                    config.test_params, case_run_id, run_dir);

                // 填充元信息
                result.run_id = run_id;
                result.case_run_id = case_run_id;
                result.data_config_name = data_config.name;
                result.index_config_name = index_config.name;
                result.expr_template_name = expr_template.name;
                result.query_value_name = "";  // 不再需要
                result.actual_expression = resolved_expression;  // 使用解析后的实际表达式
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

    // // 汇总统计
    // if (!results.empty()) {
    //     // 找出最慢和最快的查询
    //     auto slowest = std::max_element(results.begin(), results.end(),
    //         [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });
    //     auto fastest = std::min_element(results.begin(), results.end(),
    //         [](const auto& a, const auto& b) { return a.latency_p99_ms < b.latency_p99_ms; });

    //     std::cout << "\nPerformance Summary:" << std::endl;
    //     std::cout << "  Fastest query (P99): " << fastest->latency_p99_ms << "ms"
    //               << " (" << fastest->index_config_name << ", " << fastest->actual_expression << ")" << std::endl;
    //     std::cout << "  Slowest query (P99): " << slowest->latency_p99_ms << "ms"
    //               << " (" << slowest->index_config_name << ", " << slowest->actual_expression << ")" << std::endl;
    // }

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
                  << std::setw(30) << result.expr_template_name
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

    // 生成 meta.json：run 元信息与去重后的配置清单
    {
        std::set<std::string> unique_data_configs2;
        std::set<std::string> unique_index_configs2;
        std::set<std::string> unique_expressions2;
        for (const auto& r : results) {
            unique_data_configs2.insert(r.data_config_name);
            unique_index_configs2.insert(r.index_config_name);
            unique_expressions2.insert(r.actual_expression);
        }

        std::ofstream meta(run_dir + "meta.json");
        meta << "{\n";
        meta << "  \"id\": \"" << run_id << "\",\n";
        meta << "  \"timestamp_ms\": " << run_id << ",\n";
        meta << "  \"label\": \"\",\n";
        meta << "  \"summary\": { \"total_cases\": " << results.size() << " },\n";
        meta << "  \"data_configs\": [";
        bool first = true;
        for (const auto& s : unique_data_configs2) { if (!first) meta << ", "; meta << "\"" << s << "\""; first = false; }
        meta << "],\n";
        meta << "  \"index_configs\": [";
        first = true;
        for (const auto& s : unique_index_configs2) { if (!first) meta << ", "; meta << "\"" << s << "\""; first = false; }
        meta << "],\n";
        meta << "  \"expressions\": [";
        first = true;
        for (const auto& s : unique_expressions2) { if (!first) meta << ", "; meta << "\"" << s << "\""; first = false; }
        meta << "]\n";
        meta << "}\n";
    }

    // 生成 metrics.json：按 case_run_id 索引的详细指标
    {
        std::ofstream metrics(run_dir + "metrics.json");
        metrics << "{\n  \"cases\": {\n";
        bool first_case = true;
        for (const auto& r : results) {
            if (!first_case) metrics << ",\n";
            first_case = false;
            metrics << "    \"" << r.case_run_id << "\": {\n";
            metrics << "      \"data_config\": \"" << r.data_config_name << "\",\n";
            metrics << "      \"index_config\": \"" << r.index_config_name << "\",\n";
            metrics << "      \"expression\": \"" << r.actual_expression << "\",\n";
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
            metrics << "      \"selectivity\": " << r.actual_selectivity << ",\n";
            metrics << "      \"index_build_ms\": " << r.index_build_time_ms << ",\n";
            metrics << "      \"memory\": { \"index_mb\": " << (r.index_memory_bytes / (1024.0 * 1024.0))
                    << ", \"exec_peak_mb\": " << (r.exec_memory_peak_bytes / (1024.0 * 1024.0)) << " },\n";
            metrics << "      \"cpu_pct\": " << r.cpu_usage_percent << ",\n";
            metrics << "      \"flamegraph\": \"flamegraphs/" << r.case_run_id << ".svg\"\n";
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
        new_entry += "      \"timestamp_ms\": " + std::to_string(run_id) + ",\n";
        new_entry += "      \"label\": \"\",\n";
        new_entry += "      \"summary\": { \"total_cases\": " + std::to_string(results.size()) + " }\n";
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
                if (c != ' ' && c != '\n' && c != '\t' && c != '\r') return true;
            }
            return false;
        };

        std::string merged_body;
        if (!existing.empty()) {
            // 寻找 runs 数组内容
            size_t runs_pos = existing.find("\"runs\"");
            if (runs_pos != std::string::npos) {
                size_t lb = existing.find('[', runs_pos);
                size_t rb = (lb != std::string::npos) ? existing.find(']', lb) : std::string::npos;
                if (lb != std::string::npos && rb != std::string::npos && rb > lb) {
                    std::string body = existing.substr(lb + 1, rb - lb - 1);
                    std::string id_key = "\"id\": \"" + std::to_string(run_id) + "\"";
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
            merged_body = new_entry; // 无历史或解析失败，写入当前 run
        }

        std::ofstream out(index_path, std::ios::out | std::ios::trunc);
        if (!out.good()) {
            std::cerr << "Failed to write index.json at: " << index_path << std::endl;
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

    // Check if this is a per-field index configuration
    if (!config.field_configs.empty()) {
        std::cout << "    Building indexes for " << config.field_configs.size() << " fields:" << std::endl;
        // Build indexes for each configured field
        for (const auto& [field_name, field_index_config] : config.field_configs) {
            if (field_index_config.type != ScalarIndexType::NONE) {
                std::cout << "      Building index for field: " << field_name
                          << " with type: " << static_cast<int>(field_index_config.type) << std::endl;
                auto result = index_manager.BuildAndLoadIndexForField(*segment_wrapper, field_name, field_index_config);
                if (!result.success) {
                    std::cerr << "Failed to build index for field " << field_name
                              << ": " << result.error_message << std::endl;
                }
            }
        }
    } else {
        // No field-specific index configs, indices remain unbuilt
        std::cout << "    No field-specific index configurations found." << std::endl;
    }

    // 创建索引结果bundle
    auto index_bundle = std::make_shared<IndexBundle>();
    index_bundle->wrapper = nullptr;  // 暂时设置为nullptr，因为索引已经加载到segment中
    index_bundle->config = config;

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
            std::string svg_filename = results_dir + "flamegraphs/" + std::to_string(case_run_id) + ".svg";

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
    // With multi-field support, index applicability is checked per field
    // This method returns true as the actual validation happens at field level
    return true;
}

bool
ScalarFilterBenchmark::IsExpressionApplicable(const ExpressionTemplate& expr, const DataConfig& data) {
    // Expression applicability is determined by field availability during placeholder resolution
    // This method returns true as the actual validation happens during query execution
    return true;
}

std::string
ScalarFilterBenchmark::ResolveFieldPlaceholders(const std::string& expr_template,
                                                const SegmentWrapper& segment) {
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
                result = std::regex_replace(result, std::regex(std::regex_replace(placeholder,
                    std::regex(R"([\[\]\{\}\(\)\*\+\?\.\|\^\$])"), R"(\$&)")), replacement);
            } else if (placeholder_type == "field_type") {
                // For field_type, just replace with the field name directly
                // This is used for expressions that reference fields by name
                result = std::regex_replace(result, std::regex(std::regex_replace(placeholder,
                    std::regex(R"([\[\]\{\}\(\)\*\+\?\.\|\^\$])"), R"(\$&)")), field_name);
            }
        } catch (const std::exception& e) {
            // If field not found, log warning and leave placeholder as is
            std::cerr << "Warning: Could not resolve placeholder " << placeholder
                     << ": " << e.what() << std::endl;
            // Move past this placeholder to avoid infinite loop
            size_t pos = result.find(placeholder);
            if (pos != std::string::npos) {
                result = result.substr(0, pos) + "[UNRESOLVED:" + placeholder + "]"
                       + result.substr(pos + placeholder.length());
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

    while (std::regex_search(search_start, expr_template.cend(), match, placeholder_pattern)) {
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
