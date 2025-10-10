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

#include <any>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"

namespace milvus {
namespace scalar_bench {

using DataType = milvus::DataType;

// 数据分布类型
// TODO: 需要一个 DistributionConfig 类
enum class Distribution {
    UNIFORM,
    NORMAL,
    ZIPF,
    SEQUENTIAL,
    CUSTOM_HIST
};

// 标量索引类型
enum class ScalarIndexType {
    NONE,
    STL_SORT,
    TRIE,
    INVERTED,
    BITMAP,
    HYBRID,
    NGRAM,
};

enum class FieldGeneratorType {
    CATEGORICAL,
    NUMERIC,
    TIMESTAMP,
    VARCHAR,
    ARRAY,
    BOOLEAN
};

// TODO: 增加一个 stats config，处理 json stats/shredding 等


// Forward declaration
struct FieldConfig;

// Dictionary sources for categorical/token generators (see data_generation_schema.md)
// YAML usage under DataConfig.global_dictionaries:
// Inline dictionary example:
//   global_dictionaries:
//     cities_small:
//       items: ["Beijing", "Shanghai", "Shenzhen"]
// External file dictionary example:
//   global_dictionaries:
//     ecommerce_tags:
//       items_file: datasets/tags.txt  // path relative to bench_cases directory
// Built-in dictionary reference (no declaration needed in global_dictionaries):
//   fields:
//     - field_name: user_id
//       generator: categorical
//       type: VARCHAR
//       max_length: 36
//       values:
//         dictionary: uuid_v4_lower   // e.g. uuid_v4_lower, h3_level8
// Notes:
// - One of items, items_file, or builtin is expected. Built-ins can be referenced
//   directly via field values without declaring under global_dictionaries.
struct DictionaryConfig {
    std::vector<std::string> items;
    std::string items_file;
    std::string builtin;
};

// Value pools for generators that draw from token sets (categorical, varchar RANDOM)
// YAML usage inside a field's `values` block:
// Using a built-in or named dictionary:
//   values:
//     dictionary: uuid_v4_lower   // or a name defined in DataConfig.global_dictionaries
// Using inline items directly:
//   values:
//     inline: ["A", "B", "C"]
// Examples:
//   - field_name: user_id
//     generator: categorical
//     type: VARCHAR
//     values:
//       dictionary: uuid_v4_lower
//   - field_name: search_text
//     generator: text
//     max_length: 256
//     mode: random
//     values:
//       inline: ["laptop", "desktop", "monitor"]
struct ValuePoolConfig {
    std::string dictionary;
    std::vector<std::string> inline_items;
};

// ============== Categorical ==============
struct CategoricalGeneratorConfig {
    DataType type;  // only INT64 and VARCHAR are currently supported
    ValuePoolConfig values;
    std::vector<double> duplication_ratios;
    int max_length = 0;  // for VARCHAR
};

// ============== Numeric ==============
struct NumericBucketConfig {
    double weight = 1.0;
    double min = 0.0;
    double max = 0.0;
};

struct OutlierConfig {
    double ratio = 0.0;
    std::vector<double> values;
};

struct RangeDouble {
    double min = 0.0;
    double max = 0.0;
};

// Numeric generator configuration
// - range: REQUIRED global domain [min, max]. Used for UNIFORM/NORMAL/ZIPF sampling and
//          serves as a global clamp/bound for CUSTOM_HIST as well. Ensure all buckets
//          (when used) fall within this range.
// - distribution: sampling strategy. Buckets are considered ONLY when set to CUSTOM_HIST.
// - buckets: piecewise weighted subranges used exclusively for CUSTOM_HIST. Ignored for
//            UNIFORM/NORMAL/ZIPF. If CUSTOM_HIST is selected but buckets are empty, the
//            implementation falls back to uniform over `range`.
// - outliers: injected AFTER sampling (and after precision rounding for floats/doubles),
//             so outlier values may lie outside `range`/buckets.
// - precision: for FLOAT/DOUBLE, applied BEFORE outliers are injected.
struct NumericGeneratorConfig {
    DataType type;
    RangeDouble range;  // required global domain and clamp
    Distribution distribution = Distribution::UNIFORM;
    // For SEQUENTIAL distribution: increment per step. Defaults to 1.0
    double step = 1.0;
    std::vector<NumericBucketConfig> buckets;  // used only when distribution == CUSTOM_HIST
    OutlierConfig outliers;
    int precision = -1;  // for FLOAT/DOUBLE; rounding applied before outliers
};

// ============== Timestamp ==============
struct TimestampHotspotWindow {
    int64_t start = 0;
    int64_t end = 0;
};

struct TimestampHotspot {
    TimestampHotspotWindow window;
    double weight = 0.0;
};

struct RangeInt64 {
    int64_t start = 0;
    int64_t end = 0;
};

struct TimestampGeneratorConfig {
    RangeInt64 range;
    std::vector<TimestampHotspot> hotspots;
    int64_t jitter = 0;
};

// ============== VARCHAR ==============
enum class VarcharMode { RANDOM, TEMPLATE, CORPUS, SINGLE_UUID, SINGLE_TIMESTAMP };

enum class UuidVersion { V1, V4 };

enum class TimestampStringFormat { UNIX, ISO8601 };

struct TokenCountConfig {
    int min = 0;
    int max = 0;
    Distribution distribution = Distribution::UNIFORM;
};

struct KeywordConfig {
    std::string token;
    double frequency = 0.0;
};

struct VarcharGeneratorConfig {
    int max_length = 0;
    VarcharMode mode = VarcharMode::RANDOM;

    // for RANDOM mode
    ValuePoolConfig values;
    TokenCountConfig token_count;
    std::vector<KeywordConfig> keywords;
    std::vector<std::vector<std::string>> phrase_sets;

    // for TEMPLATE mode
    std::string template_str;
    std::map<std::string, std::vector<std::string>> pools;

    // for CORPUS mode
    std::string corpus_file;

    // for SINGLE_UUID mode
    UuidVersion uuid_version = UuidVersion::V4;
    int uuid_length = 36; // allow trimming

    // for SINGLE_TIMESTAMP mode
    TimestampStringFormat ts_format = TimestampStringFormat::UNIX;
    // embed a timestamp generator configuration
    TimestampGeneratorConfig ts_embedding;
};

// ============== Array ===============
struct LengthConfig {
    int min = 0;
    int max = 0;
    Distribution distribution = Distribution::UNIFORM;
    double avg = 0.0;  // for NORMAL-like approximation
};

struct ArrayContainsRule {
    // TODO: change to only 1 vector, and use a bool to determine if it's include or exclude
    std::vector<std::string> include;
    std::vector<std::string> exclude;
    double probability = 0.0;
};

struct ArrayGeneratorConfig {
    // nested element generator, this determines how the elements are generated
    std::shared_ptr<struct FieldConfig> element;
    // how to generate the length of each array
    LengthConfig length;
    int max_capacity = 0;
    std::vector<ArrayContainsRule> contains;
    bool unique = false;
};

// ============== Boolean ==============
struct BooleanGeneratorConfig {
    double true_ratio = 0.5;
    bool has_true_ratio = false;
};

// ============== Complete Config ==============

struct FieldConfig {
    std::string field_name;
    FieldGeneratorType generator = FieldGeneratorType::CATEGORICAL;
    DataType field_type = DataType::VARCHAR;
    bool nullable = false;
    double null_ratio = 0.0;

    // Generator configs - using new unified schema
    CategoricalGeneratorConfig categorical_config;
    NumericGeneratorConfig numeric_config;
    TimestampGeneratorConfig timestamp_config;
    VarcharGeneratorConfig varchar_config;
    ArrayGeneratorConfig array_config;
    BooleanGeneratorConfig boolean_config;
};

// 数据配置
struct DataConfig {
    std::string name;
    int64_t segment_size = 0;
    int64_t segment_seed = 42;  // For reproducible generation

    // Multi-field schema-based generation
    std::map<std::string, DictionaryConfig> dictionaries;
    std::vector<FieldConfig> fields;
};

// Per-field index configuration
struct FieldIndexConfig {
    ScalarIndexType type = ScalarIndexType::NONE;
    std::map<std::string, std::string> params;
};

// 索引配置
struct IndexConfig {
    std::string name;

    // Per-field index configurations
    // Field names not in this map will use NONE (no index)
    std::map<std::string, FieldIndexConfig> field_configs;
};

// 表达式模板
struct ExpressionTemplate {
    std::string name;
    std::string expr_template;
};

// 查询参数值
struct QueryValue {
    std::string name;
    std::map<std::string, std::any> values;
    double expected_selectivity;
};

// 测试参数
struct TestParams {
    int warmup_iterations = 10;
    int test_iterations = 100;
    bool collect_memory_stats = true;
    // TODO: flamegraph 需要更多配置，合并成 FlameGraphConfig
    bool enable_flame_graph = false;
    std::string flamegraph_repo_path = "~/FlameGraph";
};

// 基准测试配置
struct BenchmarkConfig {
    // std::vector<DataConfig> data_configs;
    // std::vector<IndexConfig> index_configs;
    // std::vector<ExpressionTemplate> expr_templates;
    TestParams test_params;

    // Optional: multiple suites per YAML. If non-empty, runner should iterate suites.
    struct BenchmarkSuite {
        std::string name;
        std::vector<DataConfig> data_configs;
        std::vector<IndexConfig> index_configs;
        std::vector<ExpressionTemplate> expr_templates;
    };
    std::vector<BenchmarkSuite> suites;
};

} // namespace scalar_bench
} // namespace milvus
