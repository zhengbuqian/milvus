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
#include <filesystem>
#include <vector>
#include <algorithm>
#include <stdexcept>

#include "scalar_filter_benchmark.h"
#include "utils/bench_paths.h"
#include "storage/MmapManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus::scalar_bench;

static std::atomic<bool> g_interrupted{false};

namespace milvus {
namespace scalar_bench {
std::string g_current_run_dir;
}
}

void SignalHandler(int signum) {
    if (signum == SIGINT) {
        std::cout << "\n\n[INTERRUPTED] Caught Ctrl+C signal..." << std::endl;
        g_interrupted = true;

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

void
InitializeGlobals(int argc, char* argv[]) {
    // folly::Init follyInit(&argc, &argv, false);

    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        GetStorageDir());
    StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = GetTestRemotePath();
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        storage_config);
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

namespace {

namespace fs = std::filesystem;

fs::path
GetBenchCasesDirectory() {
    if (const char* env_dir = std::getenv("SCALAR_BENCH_CASES_DIR")) {
        return fs::path(env_dir);
    }
#ifdef SCALAR_BENCH_CASES_DIR
    return fs::path(SCALAR_BENCH_CASES_DIR);
#else
    // Try to find bench_cases relative to executable
    auto exe_path = fs::current_path();
    auto bench_cases_path = exe_path / "bench_cases";
    if (fs::exists(bench_cases_path)) {
        return bench_cases_path;
    }
    // Try parent directory
    bench_cases_path = exe_path.parent_path() / "bench_cases";
    if (fs::exists(bench_cases_path)) {
        return bench_cases_path;
    }
    // Try from source tree
    bench_cases_path = fs::path(__FILE__).parent_path() / "bench_cases";
    if (fs::exists(bench_cases_path)) {
        return bench_cases_path;
    }
    // Default
    return exe_path / "bench_cases";
#endif
}

std::vector<std::string>
CollectBenchmarkCases() {
    std::vector<std::string> cases;
    auto cases_dir = GetBenchCasesDirectory() / "benchmark_cases";
    std::error_code ec;
    if (!fs::exists(cases_dir, ec) || !fs::is_directory(cases_dir, ec)) {
        return cases;
    }

    for (const auto& entry : fs::directory_iterator(cases_dir, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto& path = entry.path();
        auto ext = path.extension().string();
        if (ext == ".yaml" || ext == ".yml") {
            cases.emplace_back(path.filename().string());
        }
    }

    std::sort(cases.begin(), cases.end());
    return cases;
}

void
PrintUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <config_file>" << std::endl;
    std::cout << "       " << program_name << " [options]" << std::endl;
    std::cout << "\nOptions:" << std::endl;
    std::cout << "  --help              Show this help message" << std::endl;
    std::cout << "  --list-cases        List all available benchmark cases" << std::endl;
    std::cout << "  --config <file>     Load configuration from YAML file" << std::endl;
    std::cout << "\nExamples:" << std::endl;

    auto bench_dir = GetBenchCasesDirectory();
    std::cout << "  " << program_name << " " << (bench_dir / "benchmark_cases" / "quick.yaml").string() << std::endl;
    std::cout << "  " << program_name << " --config my_custom_benchmark.yaml" << std::endl;

    std::cout << "\nAvailable benchmark cases in " << (bench_dir / "benchmark_cases").string() << ":" << std::endl;
    auto cases = CollectBenchmarkCases();
    if (cases.empty()) {
        std::cout << "  (no cases found)" << std::endl;
    } else {
        for (const auto& name : cases) {
            std::cout << "  - " << name << std::endl;
        }
    }
}

} // namespace

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
    std::string config_file;

    if (argc == 1) {
        PrintUsage(argv[0]);
        return 1;
    }

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg == "--list-cases") {
            std::cout << "\nAvailable benchmark cases:" << std::endl;
            auto cases = CollectBenchmarkCases();
            auto bench_dir = GetBenchCasesDirectory() / "benchmark_cases";
            if (cases.empty()) {
                std::cout << "  (no cases found in " << bench_dir << ")" << std::endl;
            } else {
                for (const auto& name : cases) {
                    std::cout << "  - " << name << std::endl;
                }
                std::cout << "\nTo run a case, use:" << std::endl;
                if (!cases.empty()) {
                    std::cout << "  " << argv[0] << " " << (bench_dir / cases[0]).string() << std::endl;
                }
            }
            return 0;
        } else if (arg == "--config") {
            if (i + 1 < argc) {
                config_file = argv[++i];
            } else {
                std::cerr << "Error: --config requires a file path" << std::endl;
                return 1;
            }
        } else if (arg.size() < 2 || arg.substr(0, 2) != "--") {
            // Treat as config file path if not an option
            config_file = arg;
        } else {
            std::cerr << "Error: Unknown option '" << arg << "'" << std::endl;
            std::cerr << "Use --help for usage information" << std::endl;
            return 1;
        }
    }

    if (config_file.empty()) {
        std::cerr << "Error: No configuration file specified" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    // Check if file exists
    if (!fs::exists(config_file)) {
        std::cerr << "Error: Configuration file not found: " << config_file << std::endl;

        // Try to find it in benchmark_cases directory
        auto bench_cases_dir = GetBenchCasesDirectory() / "benchmark_cases";
        auto alt_path = bench_cases_dir / config_file;
        if (fs::exists(alt_path)) {
            std::cout << "Found configuration in benchmark_cases directory: " << alt_path << std::endl;
            config_file = alt_path.string();
        } else {
            return 1;
        }
    }

    // 创建基准测试实例
    auto benchmark = std::make_unique<ScalarFilterBenchmark>();

    // 加载配置
    BenchmarkConfig config;
    try {
        config = benchmark->LoadConfig(config_file);
        std::cout << "\nLoaded configuration from: " << config_file << std::endl;

        // Display configuration summary
        std::cout << "\nConfiguration Summary:" << std::endl;
        std::cout << "  Data configs: " << config.data_configs.size() << std::endl;
        std::cout << "  Index configs: " << config.index_configs.size() << std::endl;
        std::cout << "  Expression templates: " << config.expr_templates.size() << std::endl;
        std::cout << "  Test iterations: " << config.test_params.test_iterations << std::endl;
        std::cout << "  Warmup iterations: " << config.test_params.warmup_iterations << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\nError loading configuration: " << e.what() << std::endl;
        return 1;
    }

    // 运行基准测试
    try {
        auto results = benchmark->RunBenchmark(config);

        // 检查是否被中断
        if (g_interrupted) {
            std::cout << "\nBenchmark was interrupted by user" << std::endl;
            return 1;
        }

        // 生成报告
        benchmark->GenerateReport(results);

        std::cout << "\nBenchmark completed successfully!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\nError during benchmark execution: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}