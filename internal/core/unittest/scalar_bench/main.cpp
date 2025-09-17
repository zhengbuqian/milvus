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
                            .segment_size = 100'000,  // 10万行用于快速测试
                            .data_type = "INT64",
                            .distribution = Distribution::UNIFORM,
                            .cardinality = 90'000,  // 高基数
                            .null_ratio = 0.0},
                           {.name = "zipf_int64_low_card",
                            .segment_size = 100'000,
                            .data_type = "INT64",
                            .distribution = Distribution::ZIPF,
                            .cardinality = 100,  // 低基数
                            .null_ratio = 0.05},
                           {.name = "uniform_varchar_medium_card",
                            .segment_size = 100'000,
                            .data_type = "VARCHAR",
                            .distribution = Distribution::UNIFORM,
                            .cardinality = 10'000,  // 中基数
                            .null_ratio = 0.0}};

    // 索引配置：测试不同的索引类型
    config.index_configs = {
        {.name = "no_index", .type = ScalarIndexType::NONE, .params = {}},
        {.name = "bitmap",
         .type = ScalarIndexType::BITMAP,
         .params = {{"chunk_size", "8192"}}},
        {.name = "inverted", .type = ScalarIndexType::INVERTED, .params = {}}};

    // 表达式模板：测试不同的查询类型
    config.expr_templates = {
        {.name = "equal",
         .expr_template = "field == {value}",
         .type = ExpressionTemplate::Type::COMPARISON},
        {.name = "greater",
         .expr_template = "field > {value}",
         .type = ExpressionTemplate::Type::COMPARISON},
        {.name = "range",
         .expr_template = "field >= {lower} AND field <= {upper}",
         .type = ExpressionTemplate::Type::RANGE},
        {.name = "in_set",
         .expr_template = "field IN {list}",
         .type = ExpressionTemplate::Type::SET_OPERATION}};

    // 查询值：测试不同的选择率
    config.query_values = {
        {.name = "selectivity_1p",
         .values = {{"value", 99'000},
                    {"lower", 99'000},
                    {"upper", 100'000},
                    {"list", std::string("[99001, 99002, 99003]")}},
         .expected_selectivity = 0.01},
        {.name = "selectivity_10p",
         .values = {{"value", 90'000},
                    {"lower", 90'000},
                    {"upper", 100'000},
                    {"list",
                     std::string("[90001, 90002, 90003, 90004, 90005]")}},
         .expected_selectivity = 0.10},
        {.name = "selectivity_50p",
         .values = {{"value", 50'000},
                    {"lower", 50'000},
                    {"upper", 100'000},
                    {"list", std::string("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]")}},
         .expected_selectivity = 0.50}};

    // 测试参数
    config.test_params = {.warmup_iterations = 5,
                          .test_iterations = 20,
                          .verify_correctness = true,
                          .collect_memory_stats = true};

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

    std::cout << "\nBenchmark completed successfully!" << std::endl;

    return 0;
}