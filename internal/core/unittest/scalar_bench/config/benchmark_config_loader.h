#pragma once

#include <string>
#include <filesystem>

#include "benchmark_config.h"

namespace milvus {
namespace scalar_bench {

class BenchmarkConfigLoader {
public:
    // Main entry point - loads benchmark case file
    static BenchmarkConfig FromYamlFile(const std::string& path);

    // Load a single data config file
    static DataConfig LoadDataConfigFile(const std::string& path);

    // Resolve path relative to bench_cases directory
    static std::string ResolvePath(const std::string& relative_path);

    // Resolve dictionary file path
    static std::string ResolveDictionaryPath(const std::string& path);

private:
    // Get bench_cases base directory
    static std::filesystem::path GetBenchCasesDir();
};

} // namespace scalar_bench
} // namespace milvus
