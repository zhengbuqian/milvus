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

#include "result_writers.h"
#include <fstream>
#include <iostream>
#include <iomanip>
#include <set>
#include <map>
#include <algorithm>
#include <chrono>
#include <ctime>

namespace milvus {
namespace scalar_bench {

// Helper function to escape JSON strings
static std::string escapeJson(const std::string& input) {
    std::string output;
    output.reserve(input.size());
    for (char c : input) {
        switch (c) {
            case '"':  output += "\\\""; break;
            case '\\': output += "\\\\"; break;
            case '\b': output += "\\b"; break;
            case '\f': output += "\\f"; break;
            case '\n': output += "\\n"; break;
            case '\r': output += "\\r"; break;
            case '\t': output += "\\t"; break;
            default:
                if (c < 32 || c > 126) {
                    // Escape non-printable characters
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    output += buf;
                } else {
                    output += c;
                }
        }
    }
    return output;
}

void
WriteBundleMeta(const std::string& bundle_dir,
                const BundleMetadata& meta) {
    std::ofstream file(bundle_dir + "bundle_meta.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open bundle_meta.json for writing" << std::endl;
        return;
    }

    file << "{\n";
    file << "  \"bundle_id\": \"" << meta.bundle_id << "\",\n";
    file << "  \"config_file\": \"" << escapeJson(meta.config_file) << "\",\n";
    file << "  \"config_content\": \"" << escapeJson(meta.config_content) << "\",\n";
    file << "  \"timestamp_ms\": " << meta.timestamp_ms << ",\n";

    // Test params
    file << "  \"test_params\": {\n";
    file << "    \"warmup_iterations\": " << meta.test_params.warmup_iterations << ",\n";
    file << "    \"test_iterations\": " << meta.test_params.test_iterations << ",\n";
    file << "    \"collect_memory_stats\": " << (meta.test_params.collect_memory_stats ? "true" : "false") << ",\n";
    file << "    \"enable_flame_graph\": " << (meta.test_params.enable_flame_graph ? "true" : "false") << ",\n";
    file << "    \"flamegraph_repo_path\": \"" << escapeJson(meta.test_params.flamegraph_repo_path) << "\"\n";
    file << "  },\n";

    // Cases
    file << "  \"cases\": [\n";
    for (size_t i = 0; i < meta.cases.size(); ++i) {
        const auto& c = meta.cases[i];
        file << "    {\n";
        file << "      \"case_name\": \"" << escapeJson(c.case_name) << "\",\n";
        file << "      \"case_id\": \"" << escapeJson(c.case_id) << "\",\n";
        file << "      \"suites\": [";
        for (size_t j = 0; j < c.suites.size(); ++j) {
            file << "\"" << escapeJson(c.suites[j]) << "\"";
            if (j + 1 < c.suites.size()) file << ", ";
        }
        file << "],\n";
        file << "      \"total_tests\": " << c.total_tests << ",\n";
        file << "      \"has_flamegraphs\": " << (c.has_flamegraphs ? "true" : "false") << "\n";
        file << "    }";
        if (i + 1 < meta.cases.size()) file << ",";
        file << "\n";
    }
    file << "  ]\n";
    file << "}\n";

    file.close();
}

void
WriteCaseMeta(const std::string& case_dir,
              const CaseMetadata& meta) {
    std::ofstream file(case_dir + "case_meta.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open case_meta.json for writing" << std::endl;
        return;
    }

    file << "{\n";
    file << "  \"case_id\": \"" << escapeJson(meta.case_id) << "\",\n";
    file << "  \"case_name\": \"" << escapeJson(meta.case_name) << "\",\n";
    file << "  \"bundle_id\": \"" << meta.bundle_id << "\",\n";

    // Suites
    file << "  \"suites\": [\n";
    for (size_t i = 0; i < meta.suites.size(); ++i) {
        const auto& s = meta.suites[i];
        file << "    {\n";
        file << "      \"suite_name\": \"" << escapeJson(s.suite_name) << "\",\n";

        file << "      \"data_configs\": [";
        for (size_t j = 0; j < s.data_configs.size(); ++j) {
            file << "\"" << escapeJson(s.data_configs[j]) << "\"";
            if (j + 1 < s.data_configs.size()) file << ", ";
        }
        file << "],\n";

        file << "      \"index_configs\": [";
        for (size_t j = 0; j < s.index_configs.size(); ++j) {
            file << "\"" << escapeJson(s.index_configs[j]) << "\"";
            if (j + 1 < s.index_configs.size()) file << ", ";
        }
        file << "],\n";

        file << "      \"expr_templates\": [";
        for (size_t j = 0; j < s.expr_templates.size(); ++j) {
            file << "\"" << escapeJson(s.expr_templates[j]) << "\"";
            if (j + 1 < s.expr_templates.size()) file << ", ";
        }
        file << "]\n";

        file << "    }";
        if (i + 1 < meta.suites.size()) file << ",";
        file << "\n";
    }
    file << "  ],\n";

    file << "  \"total_tests\": " << meta.total_tests << ",\n";
    file << "  \"has_flamegraphs\": " << (meta.has_flamegraphs ? "true" : "false") << "\n";
    file << "}\n";

    file.close();
}

void
WriteCaseMetrics(const std::string& case_dir,
                 const std::vector<BenchmarkResult>& results) {
    std::ofstream file(case_dir + "case_metrics.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open case_metrics.json for writing" << std::endl;
        return;
    }

    file << "{\n";
    file << "  \"tests\": [\n";

    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        file << "    {\n";
        file << "      \"test_id\": \"" << std::setw(4) << std::setfill('0') << (i + 1) << "\",\n";
        file << "      \"suite_name\": \"" << escapeJson(r.suite_name.empty() ? "default" : r.suite_name) << "\",\n";
        file << "      \"data_config\": \"" << escapeJson(r.data_config_name) << "\",\n";
        file << "      \"index_config\": \"" << escapeJson(r.index_config_name) << "\",\n";
        file << "      \"expression\": \"" << escapeJson(r.expr_template_name) << "\",\n";
        file << "      \"actual_expression\": \"" << escapeJson(r.actual_expression) << "\",\n";
        file << "      \"qps\": " << r.qps << ",\n";

        file << "      \"latency_ms\": {\n";
        file << "        \"avg\": " << r.latency_avg_ms << ",\n";
        file << "        \"p50\": " << r.latency_p50_ms << ",\n";
        file << "        \"p90\": " << r.latency_p90_ms << ",\n";
        file << "        \"p99\": " << r.latency_p99_ms << ",\n";
        file << "        \"p999\": " << r.latency_p999_ms << ",\n";
        file << "        \"min\": " << r.latency_min_ms << ",\n";
        file << "        \"max\": " << r.latency_max_ms << "\n";
        file << "      },\n";

        file << "      \"matched_rows\": " << r.matched_rows << ",\n";
        file << "      \"total_rows\": " << r.total_rows << ",\n";
        file << "      \"selectivity\": " << r.actual_selectivity << ",\n";
        file << "      \"index_build_ms\": " << r.index_build_time_ms << ",\n";

        file << "      \"memory\": {\n";
        file << "        \"index_mb\": " << (r.index_memory_bytes / (1024.0 * 1024.0)) << ",\n";
        file << "        \"exec_peak_mb\": " << (r.exec_memory_peak_bytes / (1024.0 * 1024.0)) << "\n";
        file << "      },\n";

        file << "      \"cpu_pct\": " << r.cpu_usage_percent << ",\n";

        if (r.has_flamegraph && !r.flamegraph_path.empty()) {
            file << "      \"flamegraph\": \"" << escapeJson(r.flamegraph_path) << "\"\n";
        } else {
            file << "      \"flamegraph\": null\n";
        }

        file << "    }";
        if (i + 1 < results.size()) file << ",";
        file << "\n";
    }

    file << "  ]\n";
    file << "}\n";

    file.close();
}

void
WriteIndexJson(const std::string& results_base_dir,
               const std::vector<BundleInfo>& bundles) {
    std::string index_path = results_base_dir + "index.json";

    // 读取现有的 index.json 如果存在
    std::set<int64_t> existing_bundle_ids;
    std::vector<BundleInfo> merged_bundles;

    {
        std::ifstream in(index_path);
        if (in.good()) {
            // 简单解析现有的 bundle IDs（这里简化处理，生产环境应使用 JSON 库）
            std::string line;
            while (std::getline(in, line)) {
                size_t pos = line.find("\"bundle_id\"");
                if (pos != std::string::npos) {
                    size_t colon = line.find(":", pos);
                    if (colon != std::string::npos) {
                        size_t start = line.find("\"", colon) + 1;
                        size_t end = line.find("\"", start);
                        if (start != std::string::npos && end != std::string::npos) {
                            std::string id_str = line.substr(start, end - start);
                            try {
                                existing_bundle_ids.insert(std::stoll(id_str));
                            } catch (...) {}
                        }
                    }
                }
            }
        }
    }

    // 合并新的 bundles，去重
    merged_bundles = bundles;
    // TODO: 完整实现需要解析现有 bundles 并合并

    // 写入 index.json
    std::ofstream file(index_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open index.json for writing" << std::endl;
        return;
    }

    file << "{\n";
    file << "  \"bundles\": [\n";

    for (size_t i = 0; i < merged_bundles.size(); ++i) {
        const auto& b = merged_bundles[i];
        file << "    {\n";
        file << "      \"bundle_id\": \"" << b.bundle_id << "\",\n";
        file << "      \"config_file\": \"" << escapeJson(b.config_file) << "\",\n";
        file << "      \"timestamp_ms\": " << b.timestamp_ms << ",\n";
        file << "      \"label\": \"" << escapeJson(b.label) << "\",\n";
        file << "      \"cases\": [";
        for (size_t j = 0; j < b.cases.size(); ++j) {
            file << "\"" << escapeJson(b.cases[j]) << "\"";
            if (j + 1 < b.cases.size()) file << ", ";
        }
        file << "],\n";
        file << "      \"total_tests\": " << b.total_tests << "\n";
        file << "    }";
        if (i + 1 < merged_bundles.size()) file << ",";
        file << "\n";
    }

    file << "  ]\n";
    file << "}\n";

    file.close();
}

BundleInfo
CreateBundleInfo(const BundleMetadata& meta) {
    BundleInfo info;
    info.bundle_id = meta.bundle_id;
    info.config_file = meta.config_file;
    info.timestamp_ms = meta.timestamp_ms;
    info.label = "";  // 可以从 config 或其他地方获取

    int total_tests = 0;
    for (const auto& c : meta.cases) {
        info.cases.push_back(c.case_name);
        total_tests += c.total_tests;
    }
    info.total_tests = total_tests;

    return info;
}

void
WriteCaseSummary(const std::string& case_dir,
                 const CaseMetadata& meta,
                 const std::vector<BenchmarkResult>& results) {
    std::ofstream file(case_dir + "case_summary.txt");
    if (!file.is_open()) {
        std::cerr << "Failed to open case_summary.txt for writing" << std::endl;
        return;
    }

    file << "====================================\n";
    file << "Case Summary\n";
    file << "====================================\n\n";

    file << "Case ID:    " << meta.case_id << "\n";
    file << "Case Name:  " << meta.case_name << "\n";
    file << "Bundle ID:  " << meta.bundle_id << "\n";
    file << "Total Tests: " << meta.total_tests << "\n";
    file << "Has Flamegraphs: " << (meta.has_flamegraphs ? "Yes" : "No") << "\n\n";

    // Suites summary
    file << "------------------------------------\n";
    file << "Suites: " << meta.suites.size() << "\n";
    file << "------------------------------------\n";
    for (const auto& suite : meta.suites) {
        file << "  Suite: " << suite.suite_name << "\n";
        file << "    Data Configs:  " << suite.data_configs.size() << "\n";
        file << "    Index Configs: " << suite.index_configs.size() << "\n";
        file << "    Expressions:   " << suite.expr_templates.size() << "\n";
    }
    file << "\n";

    // Test results summary
    if (!results.empty()) {
        file << "------------------------------------\n";
        file << "Test Results Summary\n";
        file << "------------------------------------\n\n";

        // Calculate aggregate statistics
        double total_qps = 0.0;
        double total_latency_avg = 0.0;
        double min_latency = results[0].latency_min_ms;
        double max_latency = results[0].latency_max_ms;

        for (const auto& r : results) {
            total_qps += r.qps;
            total_latency_avg += r.latency_avg_ms;
            min_latency = std::min(min_latency, r.latency_min_ms);
            max_latency = std::max(max_latency, r.latency_max_ms);
        }

        file << "Average QPS:     " << std::fixed << std::setprecision(2)
             << (total_qps / results.size()) << "\n";
        file << "Average Latency: " << std::fixed << std::setprecision(3)
             << (total_latency_avg / results.size()) << " ms\n";
        file << "Min Latency:     " << std::fixed << std::setprecision(3)
             << min_latency << " ms\n";
        file << "Max Latency:     " << std::fixed << std::setprecision(3)
             << max_latency << " ms\n\n";

        // Top 10 results by QPS
        file << "------------------------------------\n";
        file << "Top 10 Results by QPS\n";
        file << "------------------------------------\n";

        std::vector<BenchmarkResult> sorted_results = results;
        std::sort(sorted_results.begin(), sorted_results.end(),
                  [](const BenchmarkResult& a, const BenchmarkResult& b) {
                      return a.qps > b.qps;
                  });

        int count = 0;
        for (const auto& r : sorted_results) {
            if (count++ >= 10) break;

            file << "\n" << count << ". Suite: " << r.suite_name << "\n";
            file << "   Data: " << r.data_config_name << "\n";
            file << "   Index: " << r.index_config_name << "\n";
            file << "   Expression: " << r.expr_template_name << "\n";
            file << "   QPS: " << std::fixed << std::setprecision(2) << r.qps << "\n";
            file << "   Avg Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_avg_ms << " ms\n";
            file << "   P99 Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_p99_ms << " ms\n";
        }
        file << "\n";
    }

    file << "====================================\n";
    file << "End of Case Summary\n";
    file << "====================================\n";

    file.close();
}

void
WriteBundleSummary(const std::string& bundle_dir,
                   const BundleMetadata& meta,
                   const std::vector<BenchmarkResult>& all_results) {
    std::ofstream file(bundle_dir + "bundle_summary.txt");
    if (!file.is_open()) {
        std::cerr << "Failed to open bundle_summary.txt for writing" << std::endl;
        return;
    }

    file << "============================================\n";
    file << "Bundle Summary\n";
    file << "============================================\n\n";

    file << "Bundle ID:    " << meta.bundle_id << "\n";
    file << "Config File:  " << meta.config_file << "\n";

    // Convert timestamp to readable format
    auto time_t = std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::time_point(std::chrono::milliseconds(meta.timestamp_ms)));
    char time_str[100];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", std::localtime(&time_t));
    file << "Timestamp:    " << time_str << "\n\n";

    // Test parameters
    file << "--------------------------------------------\n";
    file << "Test Parameters\n";
    file << "--------------------------------------------\n";
    file << "Warmup Iterations: " << meta.test_params.warmup_iterations << "\n";
    file << "Test Iterations:   " << meta.test_params.test_iterations << "\n";
    file << "Collect Memory:    " << (meta.test_params.collect_memory_stats ? "Yes" : "No") << "\n";
    file << "Enable Flamegraph: " << (meta.test_params.enable_flame_graph ? "Yes" : "No") << "\n\n";

    // Cases summary
    file << "--------------------------------------------\n";
    file << "Cases: " << meta.cases.size() << "\n";
    file << "--------------------------------------------\n";
    int total_tests = 0;
    for (const auto& c : meta.cases) {
        file << "  Case: " << c.case_name << "\n";
        file << "    Case ID:    " << c.case_id << "\n";
        file << "    Suites:     " << c.suites.size() << "\n";
        file << "    Tests:      " << c.total_tests << "\n";
        file << "    Flamegraphs: " << (c.has_flamegraphs ? "Yes" : "No") << "\n\n";
        total_tests += c.total_tests;
    }
    file << "Total Tests Across All Cases: " << total_tests << "\n\n";

    // Overall results summary
    if (!all_results.empty()) {
        file << "--------------------------------------------\n";
        file << "Overall Results Summary\n";
        file << "--------------------------------------------\n\n";

        // Calculate aggregate statistics
        double total_qps = 0.0;
        double total_latency_avg = 0.0;
        double min_latency = all_results[0].latency_min_ms;
        double max_latency = all_results[0].latency_max_ms;
        double total_index_build_time = 0.0;
        int64_t total_matched_rows = 0;
        int64_t total_rows = 0;

        for (const auto& r : all_results) {
            total_qps += r.qps;
            total_latency_avg += r.latency_avg_ms;
            min_latency = std::min(min_latency, r.latency_min_ms);
            max_latency = std::max(max_latency, r.latency_max_ms);
            total_index_build_time += r.index_build_time_ms;
            total_matched_rows += r.matched_rows;
            total_rows += r.total_rows;
        }

        file << "Total Tests:          " << all_results.size() << "\n";
        file << "Average QPS:          " << std::fixed << std::setprecision(2)
             << (total_qps / all_results.size()) << "\n";
        file << "Average Latency:      " << std::fixed << std::setprecision(3)
             << (total_latency_avg / all_results.size()) << " ms\n";
        file << "Min Latency:          " << std::fixed << std::setprecision(3)
             << min_latency << " ms\n";
        file << "Max Latency:          " << std::fixed << std::setprecision(3)
             << max_latency << " ms\n";
        file << "Total Index Build Time: " << std::fixed << std::setprecision(2)
             << total_index_build_time << " ms\n\n";

        // Best results by case
        file << "--------------------------------------------\n";
        file << "Best Result by Case (by QPS)\n";
        file << "--------------------------------------------\n";

        std::map<std::string, BenchmarkResult> best_by_case;
        for (const auto& r : all_results) {
            auto it = best_by_case.find(r.case_name);
            if (it == best_by_case.end() || r.qps > it->second.qps) {
                best_by_case[r.case_name] = r;
            }
        }

        for (const auto& [case_name, r] : best_by_case) {
            file << "\nCase: " << case_name << "\n";
            file << "  Suite: " << r.suite_name << "\n";
            file << "  Data: " << r.data_config_name << "\n";
            file << "  Index: " << r.index_config_name << "\n";
            file << "  Expression: " << r.expr_template_name << "\n";
            file << "  QPS: " << std::fixed << std::setprecision(2) << r.qps << "\n";
            file << "  Avg Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_avg_ms << " ms\n";
            file << "  P99 Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_p99_ms << " ms\n";
        }
        file << "\n";

        // Top 20 overall results by QPS
        file << "--------------------------------------------\n";
        file << "Top 20 Overall Results by QPS\n";
        file << "--------------------------------------------\n";

        std::vector<BenchmarkResult> sorted_results = all_results;
        std::sort(sorted_results.begin(), sorted_results.end(),
                  [](const BenchmarkResult& a, const BenchmarkResult& b) {
                      return a.qps > b.qps;
                  });

        int count = 0;
        for (const auto& r : sorted_results) {
            if (count++ >= 20) break;

            file << "\n" << count << ". Case: " << r.case_name
                 << " | Suite: " << r.suite_name << "\n";
            file << "   Data: " << r.data_config_name << "\n";
            file << "   Index: " << r.index_config_name << "\n";
            file << "   Expression: " << r.expr_template_name << "\n";
            file << "   QPS: " << std::fixed << std::setprecision(2) << r.qps << "\n";
            file << "   Avg Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_avg_ms << " ms\n";
            file << "   P99 Latency: " << std::fixed << std::setprecision(3)
                 << r.latency_p99_ms << " ms\n";
        }
        file << "\n";
    }

    file << "============================================\n";
    file << "End of Bundle Summary\n";
    file << "============================================\n";

    file.close();
}

}  // namespace scalar_bench
}  // namespace milvus
