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

#include <string>
#include <vector>
#include "config/benchmark_config.h"
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// Bundle 元数据结构
struct BundleMetadata {
    int64_t bundle_id;
    std::string config_file;
    std::string config_content;  // 完整的 YAML 内容
    int64_t timestamp_ms;
    TestParams test_params;

    struct CaseInfo {
        std::string case_name;
        std::string case_id;
        std::vector<std::string> suites;
        int total_tests;
        bool has_flamegraphs;
    };

    std::vector<CaseInfo> cases;
};

// Case 元数据结构
struct CaseMetadata {
    std::string case_id;
    std::string case_name;
    int64_t bundle_id;

    struct SuiteInfo {
        std::string suite_name;
        std::vector<std::string> data_configs;
        std::vector<std::string> index_configs;
        std::vector<std::string> expr_templates;
    };

    std::vector<SuiteInfo> suites;
    int total_tests;
    bool has_flamegraphs;
};

// Bundle 简要信息（用于 index.json）
struct BundleInfo {
    int64_t bundle_id;
    std::string config_file;
    int64_t timestamp_ms;
    std::string label;
    std::vector<std::string> cases;  // case names
    int total_tests;
};

// 结果写入函数
void WriteBundleMeta(const std::string& bundle_dir,
                     const BundleMetadata& meta);

void WriteCaseMeta(const std::string& case_dir,
                   const CaseMetadata& meta);

void WriteCaseMetrics(const std::string& case_dir,
                      const std::vector<BenchmarkResult>& results);

void WriteIndexJson(const std::string& results_base_dir,
                    const std::vector<BundleInfo>& bundles);

// 辅助函数：从 results 生成 bundle info
BundleInfo CreateBundleInfo(const BundleMetadata& meta);

// 生成文本摘要文件
void WriteCaseSummary(const std::string& case_dir,
                      const CaseMetadata& meta,
                      const std::vector<BenchmarkResult>& results);

void WriteBundleSummary(const std::string& bundle_dir,
                        const BundleMetadata& meta,
                        const std::vector<BenchmarkResult>& all_results);

}  // namespace scalar_bench
}  // namespace milvus
