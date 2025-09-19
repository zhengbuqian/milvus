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

#include "benchmark_presets.h"
#include <stdexcept>
#include <iostream>

namespace milvus {
namespace scalar_bench {

// 静态预设注册表
std::map<std::string, BenchmarkPresets::PresetGenerator>&
BenchmarkPresets::GetPresets() {
    static std::map<std::string, PresetGenerator> presets;
    return presets;
}

void
BenchmarkPresets::RegisterPreset(const std::string& name, PresetGenerator generator) {
    GetPresets()[name] = generator;
}

BenchmarkConfig
BenchmarkPresets::GetPreset(const std::string& name) {
    auto& presets = GetPresets();
    auto it = presets.find(name);
    if (it == presets.end()) {
        throw std::runtime_error("Preset '" + name + "' not found");
    }
    return it->second();
}

std::string
BenchmarkPresets::GetDefaultPresetName() {
    return "simple";
}

std::vector<std::string>
BenchmarkPresets::GetPresetNames() {
    std::vector<std::string> names;
    for (const auto& [name, _] : GetPresets()) {
        names.push_back(name);
    }
    return names;
}

bool
BenchmarkPresets::HasPreset(const std::string& name) {
    return GetPresets().find(name) != GetPresets().end();
}

// 简单测试配置实现
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

// 快速测试配置
BenchmarkConfig
CreateQuickTestConfig() {
    BenchmarkConfig config;

    // 小数据量，快速验证
    config.data_configs = {{.name = "quick_uniform_int64",
                            .segment_size = 10'000,
                            .data_type = "INT64",
                            .distribution = Distribution::UNIFORM,
                            .cardinality = 5'000,
                            .null_ratio = 0.0,
                            .value_range = {.min = 0, .max = 10'000}}};

    // 只测试无索引和bitmap索引
    config.index_configs = {
        {.name = "no_index", .type = ScalarIndexType::NONE, .params = {}},
        {.name = "bitmap", .type = ScalarIndexType::BITMAP, .params = {{"chunk_size", "4096"}}}};

    // 简单的表达式
    config.expr_templates = {
        {.name = "equal_1000",
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
      value { int64_val: 1000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::COMPARISON}};

    config.query_values = {};

    // 快速测试参数
    config.test_params.warmup_iterations = 2;
    config.test_params.test_iterations = 10;
    config.test_params.verify_correctness = true;
    config.test_params.collect_memory_stats = false;
    config.test_params.enable_flame_graph = false;

    return config;
}

// 全面测试配置
BenchmarkConfig
CreateComprehensiveConfig() {
    BenchmarkConfig config;

    // 多种数据配置
    config.data_configs = {
        // 整数类型
        {.name = "uniform_int64_high",
         .segment_size = 100'000,
         .data_type = "INT64",
         .distribution = Distribution::UNIFORM,
         .cardinality = 80'000,
         .null_ratio = 0.0,
         .value_range = {.min = 0, .max = 100'000}},
        {.name = "normal_int64_med",
         .segment_size = 100'000,
         .data_type = "INT64",
         .distribution = Distribution::NORMAL,
         .cardinality = 10'000,
         .null_ratio = 0.01,
         .value_range = {.min = -50'000, .max = 50'000}},
        {.name = "zipf_int64_low",
         .segment_size = 100'000,
         .data_type = "INT64",
         .distribution = Distribution::ZIPF,
         .cardinality = 100,
         .null_ratio = 0.05,
         .value_range = {.min = 0, .max = 999}},
        // 浮点类型
        {.name = "uniform_float",
         .segment_size = 100'000,
         .data_type = "FLOAT",
         .distribution = Distribution::UNIFORM,
         .cardinality = 50'000,
         .null_ratio = 0.0,
         .value_range = {.min = -1000, .max = 1000}}
    };

    // 所有索引类型
    config.index_configs = {
        {.name = "no_index", .type = ScalarIndexType::NONE, .params = {}},
        {.name = "bitmap", .type = ScalarIndexType::BITMAP, .params = {{"chunk_size", "8192"}}},
        {.name = "stl_sort", .type = ScalarIndexType::STL_SORT, .params = {}},
        {.name = "inverted", .type = ScalarIndexType::INVERTED, .params = {}}
    };

    // 多种表达式类型
    config.expr_templates = {
        // 比较操作
        {.name = "equal",
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
        {.name = "greater_than",
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
        {.name = "less_equal",
         .expr_template = R"(
output_field_ids: 101
query {
  predicates {
    unary_range_expr {
      column_info {
        field_id: 101
        data_type: Int64
      }
      op: LessEqual
      value { int64_val: 30000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::COMPARISON},
        // 范围查询
        {.name = "range_query",
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
      upper_inclusive: false
      lower_value { int64_val: 20000 }
      upper_value { int64_val: 40000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::RANGE},
        // 集合操作
        {.name = "in_values",
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
      values { int64_val: 600 }
      values { int64_val: 700 }
      values { int64_val: 800 }
      values { int64_val: 900 }
      values { int64_val: 1000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::SET_OPERATION}
    };

    config.query_values = {};

    // 全面测试参数
    config.test_params.warmup_iterations = 10;
    config.test_params.test_iterations = 100;
    config.test_params.verify_correctness = true;
    config.test_params.collect_memory_stats = true;
    config.test_params.enable_flame_graph = true;
    config.test_params.flamegraph_repo_path = "/home/zilliz/FlameGraph";

    return config;
}

// 性能测试配置
BenchmarkConfig
CreatePerformanceConfig() {
    BenchmarkConfig config;

    // 大数据量配置
    config.data_configs = {
        {.name = "perf_uniform_1m",
         .segment_size = 1'000'000,
         .data_type = "INT64",
         .distribution = Distribution::UNIFORM,
         .cardinality = 500'000,
         .null_ratio = 0.0,
         .value_range = {.min = 0, .max = 1'000'000}},
        {.name = "perf_zipf_1m",
         .segment_size = 1'000'000,
         .data_type = "INT64",
         .distribution = Distribution::ZIPF,
         .cardinality = 1'000,
         .null_ratio = 0.0,
         .value_range = {.min = 0, .max = 10'000}}
    };

    // 性能相关的索引
    config.index_configs = {
        {.name = "no_index", .type = ScalarIndexType::NONE, .params = {}},
        {.name = "bitmap", .type = ScalarIndexType::BITMAP, .params = {{"chunk_size", "16384"}}},
        {.name = "inverted", .type = ScalarIndexType::INVERTED, .params = {}}
    };

    // 不同选择率的查询
    config.expr_templates = {
        // 低选择率（~0.01%）
        {.name = "low_selectivity",
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
      value { int64_val: 500000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::COMPARISON},
        // 中选择率（~10%）
        {.name = "medium_selectivity",
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
      lower_value { int64_val: 450000 }
      upper_value { int64_val: 550000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::RANGE},
        // 高选择率（~50%）
        {.name = "high_selectivity",
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
      value { int64_val: 500000 }
    }
  }
})",
         .type = ExpressionTemplate::Type::COMPARISON}
    };

    config.query_values = {};

    // 性能测试参数
    config.test_params.warmup_iterations = 20;
    config.test_params.test_iterations = 500;
    config.test_params.verify_correctness = false;  // 性能测试不验证正确性
    config.test_params.collect_memory_stats = true;
    config.test_params.enable_flame_graph = true;
    config.test_params.flamegraph_repo_path = "/home/zilliz/FlameGraph";

    return config;
}

// 自动注册预设配置的类
class PresetRegistrar {
public:
    PresetRegistrar() {
        BenchmarkPresets::RegisterPreset("simple", CreateSimpleTestConfig);
        BenchmarkPresets::RegisterPreset("quick", CreateQuickTestConfig);
        BenchmarkPresets::RegisterPreset("comprehensive", CreateComprehensiveConfig);
        BenchmarkPresets::RegisterPreset("performance", CreatePerformanceConfig);
    }
};

// 全局实例，确保预设被注册
static PresetRegistrar registrar;

} // namespace scalar_bench
} // namespace milvus