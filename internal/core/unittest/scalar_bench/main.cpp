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

#include <iostream>
#include <memory>
#include <signal.h>
#include <atomic>
#include <cstdlib>

// #include "folly/init/Init.h" // not used

#include "scalar_filter_benchmark.h"
#include "data_generator.h"
#include "bench_paths.h"
#include "storage/MmapManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/storage_test_utils.h"
// #include "test_utils/Constants.h" // not used

using namespace milvus::scalar_bench;

// 全局变量用于信号处理
static std::atomic<bool> g_interrupted{false};

namespace milvus {
namespace scalar_bench {
std::string g_current_run_dir;  // 定义在namespace内，与extern声明匹配
}
}

// 信号处理函数
void SignalHandler(int signum) {
    if (signum == SIGINT) {
        std::cout << "\n\n[INTERRUPTED] Caught Ctrl+C signal..." << std::endl;
        g_interrupted = true;

        // 如果有当前运行的结果文件夹，删除它
        if (!milvus::scalar_bench::g_current_run_dir.empty()) {
            std::cout << "[CLEANUP] Removing incomplete results directory: " << milvus::scalar_bench::g_current_run_dir << std::endl;
            std::string rm_cmd = "rm -rf " + milvus::scalar_bench::g_current_run_dir;
            int result = std::system(rm_cmd.c_str());
            if (result == 0) {
                std::cout << "[CLEANUP] Successfully removed " << milvus::scalar_bench::g_current_run_dir << std::endl;
            } else {
                std::cerr << "[ERROR] Failed to remove " << milvus::scalar_bench::g_current_run_dir << std::endl;
            }
        }

        std::cout << "[EXIT] Benchmark interrupted and cleaned up." << std::endl;
        std::exit(1);
    }
}

// 全局初始化函数
void
InitializeGlobals(int argc, char* argv[]) {
    // folly::Init follyInit(&argc, &argv, false);

    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        GetStorageDir());
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    static const int64_t mb = 1024 * 1024;

    milvus::cachinglayer::Manager::ConfigureTieredStorage(
        {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable},
        {1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb},
        false,
        {10, true, 30});
}

BenchmarkConfig
CreateSimpleTestConfig() {
    BenchmarkConfig config;

    // 数据配置：测试不同的数据分布和基数
    config.data_configs = {{.name = "uniform_int64_high_card",
                            .segment_size = 100'000,
                            .data_type = "INT64",
                            .distribution = Distribution::UNIFORM,
                            .cardinality = 70'000,  // 高基数
                            .null_ratio = 0.0,
                            .value_range = {.min = 0, .max = 100'000}},
                           {.name = "zipf_int64_low_card",
                            .segment_size = 100'000,
                            .data_type = "INT64",
                            .distribution = Distribution::ZIPF,
                            .cardinality = 100,  // 低基数，在值范围内（0-999有1000个可能值）
                            .null_ratio = 0.05,
                            .value_range = {.min = 0, .max = 999}}
                        //    {.name = "uniform_varchar_medium_card",
                        //     .segment_size = 100'000,
                        //     .data_type = "VARCHAR",
                        //     .distribution = Distribution::UNIFORM,
                        //     .cardinality = 10'000,  // 中基数
                        //     .null_ratio = 0.0}
                        };

    // 索引配置：测试不同的索引类型
    config.index_configs = {
        {.name = "no_index", .type = ScalarIndexType::NONE, .params = {}},
        {.name = "bitmap",
         .type = ScalarIndexType::BITMAP,
         .params = {{"chunk_size", "8192"}}},
        {.name = "inverted", .type = ScalarIndexType::INVERTED, .params = {}}};

    // 表达式模板：使用 text proto 格式
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
         .type = ExpressionTemplate::Type::COMPARISON},
        {.name = "greater_than_50000",
         .expr_template = R"(
output_field_ids: 101
query {
  predicates {
    unary_range_expr {
      column_info {
        field_id: 101
        data_type: Int64
      }
      op: GreaterThan
      value { int64_val: 50000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::COMPARISON},
        {.name = "range_10000_to_30000",
         .expr_template = R"(
output_field_ids: 101
query {
  predicates {
    binary_range_expr {
      column_info {
        field_id: 101
        data_type: Int64
      }
      lower_inclusive: true
      upper_inclusive: true
      lower_value { int64_val: 10000 }
      upper_value { int64_val: 30000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::RANGE},
        {.name = "in_specific_values",
         .expr_template = R"(
output_field_ids: 101
query {
  predicates {
    term_expr {
      column_info {
        field_id: 101
        data_type: Int64
      }
      values { int64_val: 100 }
      values { int64_val: 200 }
      values { int64_val: 300 }
      values { int64_val: 400 }
      values { int64_val: 500 }
    }
  }
})",
         .type = ExpressionTemplate::Type::SET_OPERATION}};

    // 查询值：不再需要，因为 text proto 已经包含了所有参数
    // 每个 expr_template 都是完整的、独立的查询
    config.query_values = {};

    // 测试参数
    config.test_params.warmup_iterations = 5;
    config.test_params.test_iterations = 200;
    config.test_params.verify_correctness = true;
    config.test_params.collect_memory_stats = true;
    config.test_params.enable_flame_graph = true;
    config.test_params.flamegraph_repo_path = "/home/zilliz/FlameGraph";

    return config;
}

// 演示数据生成器的使用
void
DemoDataGenerator() {
    std::cout << "\n=== Data Generator Demo ===" << std::endl;

    DataGenerator gen;

    // 生成整数数据
    std::cout << "\n1. Generating integer data:" << std::endl;
    auto int_data = gen.GenerateIntData(10, Distribution::UNIFORM, 0, 100, 5);
    std::cout << "   Generated: ";
    for (size_t i = 0; i < std::min(size_t(10), int_data.size()); ++i) {
        std::cout << int_data[i] << " ";
    }
    std::cout << std::endl;

    // 生成字符串数据 - UUID格式
    std::cout << "\n2. Generating UUID-like strings:" << std::endl;
    StringGenConfig uuid_config;
    uuid_config.pattern = StringGenConfig::Pattern::UUID_LIKE;
    auto uuid_data = gen.GenerateStringData(5, uuid_config);
    for (const auto& s : uuid_data) {
        std::cout << "   " << s << std::endl;
    }

    // 生成字符串数据 - 模板格式
    std::cout << "\n3. Generating template strings:" << std::endl;
    StringGenConfig template_config;
    template_config.pattern = StringGenConfig::Pattern::TEMPLATE;
    template_config.template_config.prefix = "user-";
    template_config.template_config.suffix = "_data";
    auto template_data = gen.GenerateStringData(5, template_config);
    for (const auto& s : template_data) {
        std::cout << "   " << s << std::endl;
    }

    // 生成字符串数据 - 句子
    std::cout << "\n4. Generating sentences:" << std::endl;
    StringGenConfig sentence_config;
    sentence_config.pattern = StringGenConfig::Pattern::SENTENCE;
    auto sentence_data = gen.GenerateStringData(3, sentence_config);
    for (const auto& s : sentence_data) {
        std::cout << "   " << s << std::endl;
    }
}

int
main(int argc, char* argv[]) {
    std::cout << "====================================" << std::endl;
    std::cout << "Milvus Scalar Filter Benchmark Tool" << std::endl;
    std::cout << "====================================" << std::endl;

    // 注册信号处理器
    signal(SIGINT, SignalHandler);

    // 初始化全局单例和管理器
    InitializeGlobals(argc, argv);

    if (argc > 1 && std::string(argv[1]) == "--demo") {
        // 演示数据生成器
        DemoDataGenerator();
        return 0;
    }

    // 创建基准测试实例
    auto benchmark = std::make_unique<ScalarFilterBenchmark>();

    // 加载或创建配置
    BenchmarkConfig config;
    if (argc > 1) {
        // 从YAML文件加载配置
        std::cout << "Loading config from: " << argv[1] << std::endl;
        config = ScalarFilterBenchmark::LoadConfig(argv[1]);
    } else {
        // 使用默认的简单配置
        std::cout << "Using default test configuration" << std::endl;
        config = CreateSimpleTestConfig();
    }

    // 运行基准测试
    std::cout << "\nStarting benchmark..." << std::endl;
    auto results = benchmark->RunBenchmark(config);

    // 生成报告
    std::cout << "\nGenerating report..." << std::endl;
    benchmark->GenerateReport(results);

    // 清除当前运行目录（成功完成，不需要删除）
    milvus::scalar_bench::g_current_run_dir.clear();

    std::cout << "\nBenchmark completed successfully!" << std::endl;

    return 0;
}