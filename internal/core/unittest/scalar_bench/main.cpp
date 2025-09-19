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
#include "benchmark_presets.h"
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

// 打印使用帮助
void
PrintUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --help              Show this help message" << std::endl;
    std::cout << "  --demo              Run data generator demo" << std::endl;
    std::cout << "  --preset <name>     Use a preset configuration (default: simple)" << std::endl;
    std::cout << "  --list-presets      List all available presets" << std::endl;
    std::cout << "  --config <file>     Load configuration from YAML file" << std::endl;
    std::cout << "\nAvailable presets:" << std::endl;
    auto presets = BenchmarkPresets::GetPresetNames();
    for (const auto& name : presets) {
        std::cout << "  - " << name;
        if (name == BenchmarkPresets::GetDefaultPresetName()) {
            std::cout << " (default)";
        }
        std::cout << std::endl;
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

    // 解析命令行参数
    std::string preset_name = BenchmarkPresets::GetDefaultPresetName();
    std::string config_file;
    bool use_config_file = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg == "--demo") {
            // 演示数据生成器
            DemoDataGenerator();
            return 0;
        } else if (arg == "--list-presets") {
            std::cout << "\nAvailable benchmark presets:" << std::endl;
            auto presets = BenchmarkPresets::GetPresetNames();
            for (const auto& name : presets) {
                std::cout << "  - " << name;
                if (name == BenchmarkPresets::GetDefaultPresetName()) {
                    std::cout << " (default)";
                }
                std::cout << std::endl;
            }
            return 0;
        } else if (arg == "--preset") {
            if (i + 1 < argc) {
                preset_name = argv[++i];
                if (!BenchmarkPresets::HasPreset(preset_name)) {
                    std::cerr << "Error: Unknown preset '" << preset_name << "'" << std::endl;
                    std::cerr << "Use --list-presets to see available presets" << std::endl;
                    return 1;
                }
            } else {
                std::cerr << "Error: --preset requires an argument" << std::endl;
                return 1;
            }
        } else if (arg == "--config") {
            if (i + 1 < argc) {
                config_file = argv[++i];
                use_config_file = true;
            } else {
                std::cerr << "Error: --config requires a file path" << std::endl;
                return 1;
            }
        } else if (arg[0] != '-') {
            // 兼容旧的用法：直接提供配置文件路径
            config_file = arg;
            use_config_file = true;
        } else {
            std::cerr << "Error: Unknown option '" << arg << "'" << std::endl;
            std::cerr << "Use --help for usage information" << std::endl;
            return 1;
        }
    }

    // 创建基准测试实例
    auto benchmark = std::make_unique<ScalarFilterBenchmark>();

    // 加载配置
    BenchmarkConfig config;
    if (use_config_file) {
        // 从YAML文件加载配置
        std::cout << "Loading config from: " << config_file << std::endl;
        config = ScalarFilterBenchmark::LoadConfig(config_file);
    } else {
        // 使用预设配置
        std::cout << "Using preset configuration: " << preset_name << std::endl;
        try {
            config = BenchmarkPresets::GetPreset(preset_name);
        } catch (const std::exception& e) {
            std::cerr << "Error loading preset: " << e.what() << std::endl;
            return 1;
        }
    }

    // 显示配置摘要
    std::cout << "\nConfiguration summary:" << std::endl;
    std::cout << "  Data configs: " << config.data_configs.size() << std::endl;
    std::cout << "  Index configs: " << config.index_configs.size() << std::endl;
    std::cout << "  Expression templates: " << config.expr_templates.size() << std::endl;
    std::cout << "  Warmup iterations: " << config.test_params.warmup_iterations << std::endl;
    std::cout << "  Test iterations: " << config.test_params.test_iterations << std::endl;
    std::cout << "  Flame graph enabled: " << (config.test_params.enable_flame_graph ? "yes" : "no") << std::endl;

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