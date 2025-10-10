#include "benchmark_config_loader.h"
#include "../dictionaries/dictionary_registry.h"
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <algorithm>

namespace milvus {
namespace scalar_bench {
namespace {

static std::string ToUpper(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::toupper);
    return r;
}

ScalarIndexType ParseIndexType(const std::string& value) {
    auto upper = ToUpper(value);
    if (upper == "NONE") return ScalarIndexType::NONE;
    if (upper == "STL_SORT") return ScalarIndexType::STL_SORT;
    if (upper == "TRIE") return ScalarIndexType::TRIE;
    if (upper == "INVERTED") return ScalarIndexType::INVERTED;
    if (upper == "BITMAP") return ScalarIndexType::BITMAP;
    if (upper == "HYBRID") return ScalarIndexType::HYBRID;
    if (upper == "NGRAM") return ScalarIndexType::NGRAM;
    throw std::runtime_error("Unknown scalar index type: " + value);
}

FieldGeneratorType ParseGeneratorType(const std::string& type_str) {
    auto upper = ToUpper(type_str);
    if (upper == "CATEGORICAL") return FieldGeneratorType::CATEGORICAL;
    if (upper == "NUMERIC") return FieldGeneratorType::NUMERIC;
    if (upper == "TIMESTAMP") return FieldGeneratorType::TIMESTAMP;
    if (upper == "VARCHAR") return FieldGeneratorType::VARCHAR;
    if (upper == "ARRAY") return FieldGeneratorType::ARRAY;
    if (upper == "BOOLEAN" || upper == "BOOL") return FieldGeneratorType::BOOLEAN;
    throw std::runtime_error("Unknown generator type: " + type_str);
}

DataType ParseDataType(const std::string& type_str) {
    proto::schema::DataType type;
    if (proto::schema::DataType_Parse(type_str, &type)) {
        auto res = static_cast<DataType>(type);
        switch (res) {
            case DataType::BOOL:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
            case DataType::VARCHAR:
            case DataType::JSON:
            case DataType::ARRAY:
                return res;
            default:
                throw std::runtime_error("Unsupported data type: " + type_str);
        }
    }
    throw std::runtime_error("Unknown data type: " + type_str);
    throw std::runtime_error("Unknown field data type: " + type_str);
}

VarcharMode ParseVarcharMode(const YAML::Node& node) {
    if (!node || !node.IsScalar()) {
        return VarcharMode::RANDOM;
    }

    auto mode = ToUpper(node.as<std::string>());
    if (mode == "RANDOM") {
        return VarcharMode::RANDOM;
    }
    if (mode == "TEMPLATE") {
        return VarcharMode::TEMPLATE;
    }
    if (mode == "CORPUS") {
        return VarcharMode::CORPUS;
    }
    if (mode == "SINGLE_UUID") {
        return VarcharMode::SINGLE_UUID;
    }
    if (mode == "SINGLE_TIMESTAMP") {
        return VarcharMode::SINGLE_TIMESTAMP;
    }

    throw std::runtime_error("Unknown varchar mode: " + node.as<std::string>());
}

void ParseValuePool(const YAML::Node& node, ValuePoolConfig& config) {
    if (!node || !node.IsMap()) {
        return;
    }

    if (node["dictionary"]) {
        config.dictionary = node["dictionary"].as<std::string>();
    }

    if (node["inline"]) {
        const auto& inline_node = node["inline"];
        if (inline_node.IsSequence()) {
            for (const auto& item : inline_node) {
                config.inline_items.push_back(item.as<std::string>());
            }
        } else if (inline_node.IsScalar()) {
            config.inline_items.push_back(inline_node.as<std::string>());
        }
    }
}

Distribution ParseDistribution(const YAML::Node& node) {
    if (!node || !node.IsScalar()) {
        return Distribution::UNIFORM;
    }

    auto dist = ToUpper(node.as<std::string>());
    if (dist == "UNIFORM") return Distribution::UNIFORM;
    if (dist == "NORMAL") return Distribution::NORMAL;
    if (dist == "ZIPF") return Distribution::ZIPF;
    if (dist == "SEQUENTIAL") return Distribution::SEQUENTIAL;
    if (dist == "CUSTOM_HIST") return Distribution::CUSTOM_HIST;

    throw std::runtime_error("Unknown distribution: " + node.as<std::string>());
}

void ParsePhraseSets(const YAML::Node& node, std::vector<std::vector<std::string>>& dst) {
    if (!node) return;
    if (node.IsSequence()) {
        for (const auto& seq : node) {
            if (seq.IsSequence()) {
                std::vector<std::string> items;
                for (const auto& item : seq) {
                    items.push_back(item.as<std::string>());
                }
                dst.push_back(std::move(items));
            }
        }
    }
}

void ParseAndRegisterDictionary(const std::string& name, const YAML::Node& node) {
    auto& registry = DictionaryRegistry::GetInstance();

    if (node["items"] && node["items"].IsSequence()) {
        // Inline dictionary
        std::vector<std::string> items;
        for (const auto& item : node["items"]) {
            items.push_back(item.as<std::string>());
        }
        registry.RegisterInlineDictionary(name, items);
    } else if (node["items_file"]) {
        // File-based dictionary
        auto path = BenchmarkConfigLoader::ResolveDictionaryPath(
            node["items_file"].as<std::string>());
        registry.RegisterFileDictionary(name, path);
    } else if (node["builtin"]) {
        // Built-in dictionary (already registered by InitializeBuiltins)
        auto builtin_name = node["builtin"].as<std::string>();
        if (!registry.HasDictionary(builtin_name)) {
            throw std::runtime_error("Unknown built-in dictionary: " + builtin_name);
        }
        // Note: no need to register, just validate it exists
    }
}

// Parse field configuration
FieldConfig ParseFieldConfig(const YAML::Node& node, const std::string& default_field_name = "") {
    FieldConfig config;

    // Required fields
    if (!node["field_name"]) {
        if (!default_field_name.empty()) {
            config.field_name = default_field_name;
        } else {
            throw std::runtime_error("Field config missing 'field_name'");
        }
    } else {
        config.field_name = node["field_name"].as<std::string>();
    }

    if (!node["generator"]) {
        throw std::runtime_error("Field config missing 'generator' for field: " + config.field_name);
    }
    config.generator = ParseGeneratorType(node["generator"].as<std::string>());

    // Optional type (can be inferred from generator)
    if (node["type"]) {
        config.field_type = ParseDataType(node["type"].as<std::string>());
    }

    // Nullable & Null ratio
    if (node["nullable"]) {
        config.nullable = node["nullable"].as<bool>();
    }
    if (node["null_ratio"]) {
        config.null_ratio = node["null_ratio"].as<double>();
        if (!config.nullable && config.null_ratio > 0.0) {
            throw std::runtime_error("null_ratio is only allowed when nullable is true for field: " + config.field_name);
        }
    }

    switch (config.generator) {
        case FieldGeneratorType::CATEGORICAL: {
            auto& cat = config.categorical_config;

            cat.type = config.field_type;
            if (cat.type != DataType::VARCHAR && cat.type != DataType::INT64) {
                throw std::runtime_error("Categorical generator only supports VARCHAR and INT64");
            }

            if (node["values"]) {
                ParseValuePool(node["values"], cat.values);
            }

            if (node["duplication_ratios"] && node["duplication_ratios"].IsSequence()) {
                for (const auto& ratio : node["duplication_ratios"]) {
                    cat.duplication_ratios.push_back(ratio.as<double>());
                }
            }

            if (node["max_length"]) {
                cat.max_length = node["max_length"].as<int>();
            }
            break;
        }

        case FieldGeneratorType::NUMERIC: {
            auto& num = config.numeric_config;
            num.type = config.field_type;
            if (num.type != DataType::INT64 &&
                num.type != DataType::FLOAT &&
                num.type != DataType::DOUBLE) {
                throw std::runtime_error("Numeric generator only supports INT64/FLOAT/DOUBLE");
            }

            if (node["range"]) {
                auto range_node = node["range"];
                if (range_node["min"]) {
                    num.range.min = range_node["min"].as<double>();
                }
                if (range_node["max"]) {
                    num.range.max = range_node["max"].as<double>();
                }
            }

            if (node["distribution"]) {
                num.distribution = ParseDistribution(node["distribution"]);
            }

            // Optional step for SEQUENTIAL distribution; defaults to 1.0
            if (node["step"]) {
                num.step = node["step"].as<double>();
            }

            if (node["buckets"] && node["buckets"].IsSequence()) {
                for (const auto& bucket : node["buckets"]) {
                    NumericBucketConfig b;
                    if (bucket["weight"]) {
                        b.weight = bucket["weight"].as<double>();
                    }
                    if (bucket["min"]) {
                        b.min = bucket["min"].as<double>();
                    }
                    if (bucket["max"]) {
                        b.max = bucket["max"].as<double>();
                    }
                    num.buckets.push_back(b);
                }
            }

            if (node["outliers"]) {
                auto outliers_node = node["outliers"];
                if (outliers_node["ratio"]) {
                    num.outliers.ratio = outliers_node["ratio"].as<double>();
                }
                if (outliers_node["values"] && outliers_node["values"].IsSequence()) {
                    for (const auto& val : outliers_node["values"]) {
                        num.outliers.values.push_back(val.as<double>());
                    }
                }
            }

            if (node["precision"]) {
                num.precision = node["precision"].as<int>();
            }
            break;
        }

        case FieldGeneratorType::TIMESTAMP: {
            auto& ts = config.timestamp_config;

            if (node["range"]) {
                auto range_node = node["range"];
                if (range_node["start"]) {
                    ts.range.start = range_node["start"].as<int64_t>();
                }
                if (range_node["end"]) {
                    ts.range.end = range_node["end"].as<int64_t>();
                }
            }

            if (node["start"]) {
                ts.range.start = node["start"].as<int64_t>();
            }
            if (node["end"]) {
                ts.range.end = node["end"].as<int64_t>();
            }

            if (node["jitter"]) {
                ts.jitter = node["jitter"].as<int64_t>();
            }

            if (node["hotspots"] && node["hotspots"].IsSequence()) {
                for (const auto& hotspot_node : node["hotspots"]) {
                    TimestampHotspot hotspot;

                    if (hotspot_node["window"]) {
                        auto window_node = hotspot_node["window"];
                        if (window_node["start"]) {
                            hotspot.window.start = window_node["start"].as<int64_t>();
                        }
                        if (window_node["end"]) {
                            hotspot.window.end = window_node["end"].as<int64_t>();
                        }
                    } else {
                        if (hotspot_node["start"]) {
                            hotspot.window.start = hotspot_node["start"].as<int64_t>();
                        }
                        if (hotspot_node["end"]) {
                            hotspot.window.end = hotspot_node["end"].as<int64_t>();
                        }
                    }

                    if (hotspot_node["weight"]) {
                        hotspot.weight = hotspot_node["weight"].as<double>();
                    }

                    ts.hotspots.push_back(hotspot);
                }
            }
            break;
        }

        case FieldGeneratorType::VARCHAR: {
            auto& varchar = config.varchar_config;

            if (node["max_length"]) {
                varchar.max_length = node["max_length"].as<int>();
            }

            varchar.mode = ParseVarcharMode(node["mode"]);

            if (varchar.mode == VarcharMode::RANDOM) {
                if (node["values"]) {
                    ParseValuePool(node["values"], varchar.values);
                }
                if (node["token_count"]) {
                    auto token_node = node["token_count"];
                    if (token_node["min"]) {
                        varchar.token_count.min = token_node["min"].as<int>();
                    }
                    if (token_node["max"]) {
                        varchar.token_count.max = token_node["max"].as<int>();
                    }
                    if (token_node["distribution"]) {
                        varchar.token_count.distribution = ParseDistribution(token_node["distribution"]);
                    }
                }
                if (node["keywords"] && node["keywords"].IsSequence()) {
                    for (const auto& keyword_node : node["keywords"]) {
                        if (!keyword_node["token"]) {
                            throw std::runtime_error(
                                "Keyword entry missing 'token' for field: " + config.field_name);
                        }
                        KeywordConfig keyword;
                        keyword.token = keyword_node["token"].as<std::string>();
                        if (keyword_node["frequency"]) {
                            keyword.frequency = keyword_node["frequency"].as<double>();
                        }
                        varchar.keywords.push_back(keyword);
                    }
                }
                if (node["phrase_sets"]) {
                    ParsePhraseSets(node["phrase_sets"], varchar.phrase_sets);
                }
            } else if (varchar.mode == VarcharMode::TEMPLATE) {
                if (node["template"]) {
                    varchar.template_str = node["template"].as<std::string>();
                } else if (node["template_str"]) {
                    varchar.template_str = node["template_str"].as<std::string>();
                }
                if (node["pools"] && node["pools"].IsMap()) {
                    for (auto it = node["pools"].begin(); it != node["pools"].end(); ++it) {
                        const auto& pool_name = it->first.as<std::string>();
                        const auto& pool_values = it->second;
                        if (!pool_values.IsSequence()) {
                            throw std::runtime_error(
                                "pools entry must be a sequence for field: " + config.field_name);
                        }

                        auto& dest = varchar.pools[pool_name];
                        for (const auto& value : pool_values) {
                            dest.push_back(value.as<std::string>());
                        }
                    }
                }
            } else if (varchar.mode == VarcharMode::CORPUS) {
                if (node["corpus_file"]) {
                    varchar.corpus_file = node["corpus_file"].as<std::string>();
                }
            } else if (varchar.mode == VarcharMode::SINGLE_UUID) {
                if (node["uuid_version"]) {
                    auto uv = ToUpper(node["uuid_version"].as<std::string>());
                    if (uv == "V1") {
                        varchar.uuid_version = UuidVersion::V1;
                    } else if (uv == "V4") {
                        varchar.uuid_version = UuidVersion::V4;
                    } else {
                        throw std::runtime_error("Unsupported uuid_version: " + uv);
                    }
                }
                if (node["uuid_length"]) {
                    varchar.uuid_length = node["uuid_length"].as<int>();
                }
            } else if (varchar.mode == VarcharMode::SINGLE_TIMESTAMP) {
                if (node["ts_format"]) {
                    auto tf = ToUpper(node["ts_format"].as<std::string>());
                    if (tf == "UNIX") {
                        varchar.ts_format = TimestampStringFormat::UNIX;
                    } else if (tf == "ISO8601") {
                        varchar.ts_format = TimestampStringFormat::ISO8601;
                    } else {
                        throw std::runtime_error("Unsupported ts_format: " + tf);
                    }
                }
                // parse embedded ts generator config under `timestamp`
                auto ts_node = node["timestamp"];
                if (ts_node) {
                    if (ts_node["range"]) {
                        auto range_node = ts_node["range"];
                        if (range_node["start"]) {
                            varchar.ts_embedding.range.start = range_node["start"].as<int64_t>();
                        }
                        if (range_node["end"]) {
                            varchar.ts_embedding.range.end = range_node["end"].as<int64_t>();
                        }
                    }
                    if (ts_node["jitter"]) {
                        varchar.ts_embedding.jitter = ts_node["jitter"].as<int64_t>();
                    }
                    if (ts_node["hotspots"]) {
                        for (const auto& hotspot_node : ts_node["hotspots"]) {
                            TimestampHotspot hotspot;
                            if (hotspot_node["window"]) {
                                auto window_node = hotspot_node["window"];
                                if (window_node["start"]) hotspot.window.start = window_node["start"].as<int64_t>();
                                if (window_node["end"]) hotspot.window.end = window_node["end"].as<int64_t>();
                            } else {
                                if (hotspot_node["start"]) hotspot.window.start = hotspot_node["start"].as<int64_t>();
                                if (hotspot_node["end"]) hotspot.window.end = hotspot_node["end"].as<int64_t>();
                            }
                            if (hotspot_node["weight"]) hotspot.weight = hotspot_node["weight"].as<double>();
                            varchar.ts_embedding.hotspots.push_back(hotspot);
                        }
                    }
                }
            }
            break;
        }

        case FieldGeneratorType::ARRAY: {
            auto& array_config = config.array_config;

            if (!node["element"]) {
                throw std::runtime_error(
                    "Array generator requires 'element' config for field: " + config.field_name);
            }

            if (!node["element"].IsMap()) {
                throw std::runtime_error("Array element must be a map for field: " + config.field_name);
            }

            array_config.element = std::make_shared<FieldConfig>(
                ParseFieldConfig(node["element"], config.field_name + "_element"));

            YAML::Node length_node;
            if (node["length"]) {
                length_node = node["length"];
            } else if (node["length_config"]) {
                length_node = node["length_config"];
            }

            if (length_node) {
                if (length_node["min"]) {
                    array_config.length.min = length_node["min"].as<int>();
                }
                if (length_node["max"]) {
                    array_config.length.max = length_node["max"].as<int>();
                }
                if (length_node["distribution"]) {
                    array_config.length.distribution = ParseDistribution(length_node["distribution"]);
                }
                if (length_node["avg"]) {
                    array_config.length.avg = length_node["avg"].as<double>();
                }
            }

            if (node["max_capacity"]) {
                array_config.max_capacity = node["max_capacity"].as<int>();
            }

            if (node["contains"] && node["contains"].IsSequence()) {
                for (const auto& rule_node : node["contains"]) {
                    ArrayContainsRule rule;
                    if (rule_node["include"]) {
                        const auto& include_node = rule_node["include"];
                        if (include_node.IsSequence()) {
                            for (const auto& value : include_node) {
                                rule.include.push_back(value.as<std::string>());
                            }
                        } else if (include_node.IsScalar()) {
                            rule.include.push_back(include_node.as<std::string>());
                        }
                    }

                    if (rule_node["exclude"]) {
                        const auto& exclude_node = rule_node["exclude"];
                        if (exclude_node.IsSequence()) {
                            for (const auto& value : exclude_node) {
                                rule.exclude.push_back(value.as<std::string>());
                            }
                        } else if (exclude_node.IsScalar()) {
                            rule.exclude.push_back(exclude_node.as<std::string>());
                        }
                    }

                    if (rule_node["probability"]) {
                        rule.probability = rule_node["probability"].as<double>();
                    }

                    array_config.contains.push_back(rule);
                }
            }

            if (node["unique"]) {
                array_config.unique = node["unique"].as<bool>();
            }
            break;
        }

        case FieldGeneratorType::BOOLEAN: {
            auto& bool_gen = config.boolean_config;
            if (node["true_ratio"]) {
                bool_gen.true_ratio = node["true_ratio"].as<double>();
                bool_gen.has_true_ratio = true;
            }
            break;
        }

        default:
            break;
    }

    return config;
}

// Parse data configuration from file
DataConfig ParseDataConfig(const YAML::Node& root, const std::string& source) {
    DataConfig config;

    // Basic fields
    if (!root["name"]) {
        throw std::runtime_error("Data config missing 'name': " + source);
    }
    config.name = root["name"].as<std::string>();

    if (!root["segment_size"]) {
        throw std::runtime_error("Data config missing 'segment_size': " + source);
    }
    config.segment_size = root["segment_size"].as<int64_t>();

    if (root["segment_seed"]) {
        config.segment_seed = root["segment_seed"].as<int64_t>();
    }

    // Parse and register global dictionaries
    if (root["global_dictionaries"] && root["global_dictionaries"].IsMap()) {
        for (auto it = root["global_dictionaries"].begin();
             it != root["global_dictionaries"].end(); ++it) {
            auto dict_name = it->first.as<std::string>();
            ParseAndRegisterDictionary(dict_name, it->second);
            // Keep in config for reference (optional, could be removed later)
            DictionaryConfig dict_cfg;
            if (it->second["items"]) {
                for (const auto& item : it->second["items"]) {
                    dict_cfg.items.push_back(item.as<std::string>());
                }
            }
            if (it->second["items_file"]) {
                dict_cfg.items_file = it->second["items_file"].as<std::string>();
            }
            config.dictionaries[dict_name] = dict_cfg;
        }
    }

    // Parse fields
    if (root["fields"] && root["fields"].IsSequence()) {
        for (const auto& field_node : root["fields"]) {
            config.fields.push_back(ParseFieldConfig(field_node, ""));
        }
    } else {
        throw std::runtime_error("Data configuration must have 'fields' defined");
    }

    return config;
}

// Parse per-field index configuration
FieldIndexConfig ParseFieldIndexConfig(const YAML::Node& node) {
    FieldIndexConfig config;

    if (!node["type"]) {
        throw std::runtime_error("Field index config missing 'type'");
    }
    config.type = ParseIndexType(node["type"].as<std::string>());

    if (node["params"] && node["params"].IsMap()) {
        for (auto it = node["params"].begin(); it != node["params"].end(); ++it) {
            config.params[it->first.as<std::string>()] = it->second.as<std::string>();
        }
    }

    return config;
}

BenchmarkConfig ParseBenchmarkConfig(const YAML::Node& root, const std::string& source) {
    BenchmarkConfig config;
    
    // Parse test params (shared across suites)
    if (auto params = root["test_params"]; params && params.IsMap()) {
        if (params["warmup_iterations"]) {
            config.test_params.warmup_iterations = params["warmup_iterations"].as<int>();
        }
        if (params["test_iterations"]) {
            config.test_params.test_iterations = params["test_iterations"].as<int>();
        }
        if (params["collect_memory_stats"]) {
            config.test_params.collect_memory_stats = params["collect_memory_stats"].as<bool>();
        }
        if (params["enable_flame_graph"]) {
            config.test_params.enable_flame_graph = params["enable_flame_graph"].as<bool>();
        }
        if (params["flamegraph_repo_path"]) {
            config.test_params.flamegraph_repo_path = params["flamegraph_repo_path"].as<std::string>();
        }
    }

    // Suites support (required)
    if (auto suites_node = root["suites"]; suites_node && suites_node.IsSequence()) {
        for (const auto& suite_node : suites_node) {
            BenchmarkConfig::BenchmarkSuite suite;
            if (suite_node["name"]) {
                suite.name = suite_node["name"].as<std::string>();
            } else {
                // Optional: default anonymous suite name
                suite.name = "suite";
            }

            // suite.data_configs
            if (auto data_nodes = suite_node["data_configs"]; data_nodes && data_nodes.IsSequence()) {
                for (const auto& node : data_nodes) {
                    if (node["path"]) {
                        auto path_config = node["path"].as<std::string>();
                        auto resolved_path = BenchmarkConfigLoader::ResolvePath(path_config);
                        auto data_config = BenchmarkConfigLoader::LoadDataConfigFile(resolved_path);
                        suite.data_configs.push_back(data_config);
                    } else {
                        throw std::runtime_error("data_configs entry must have 'path' field in suite: " + suite.name);
                    }
                }
            }

            // suite.index_configs
            if (auto idx_nodes = suite_node["index_configs"]; idx_nodes && idx_nodes.IsSequence()) {
                for (const auto& node : idx_nodes) {
                    IndexConfig ic;
                    if (!node["name"]) {
                        throw std::runtime_error("index_configs entry missing 'name' in suite: " + suite.name);
                    }
                    ic.name = node["name"].as<std::string>();

                    if (node["field_configs"] && node["field_configs"].IsMap()) {
                        for (auto it = node["field_configs"].begin(); it != node["field_configs"].end(); ++it) {
                            auto field_name = it->first.as<std::string>();
                            ic.field_configs[field_name] = ParseFieldIndexConfig(it->second);
                        }
                    } else {
                        throw std::runtime_error("index_configs entry must have 'field_configs': " + ic.name);
                    }
                    suite.index_configs.push_back(ic);
                }
            }

            // suite.expr_templates
            if (auto expr_nodes = suite_node["expr_templates"]; expr_nodes && expr_nodes.IsSequence()) {
                for (const auto& node : expr_nodes) {
                    ExpressionTemplate et;
                    if (!node["name"]) {
                        throw std::runtime_error("expr_templates entry missing 'name' in suite: " + suite.name);
                    }
                    et.name = node["name"].as<std::string>();
                    if (!node["expr_template"]) {
                        throw std::runtime_error("expr_templates entry missing 'expr_template': " + et.name);
                    }
                    et.expr_template = node["expr_template"].as<std::string>();
                    suite.expr_templates.push_back(et);
                }
            }

            // suite validation
            if (suite.data_configs.empty()) {
                throw std::runtime_error("Suite '" + suite.name + "' has no data_configs in YAML: " + source);
            }
            if (suite.index_configs.empty()) {
                throw std::runtime_error("Suite '" + suite.name + "' has no index_configs in YAML: " + source);
            }
            if (suite.expr_templates.empty()) {
                throw std::runtime_error("Suite '" + suite.name + "' has no expr_templates in YAML: " + source);
            }

            config.suites.push_back(std::move(suite));
        }
    } else {
        throw std::runtime_error("No suites defined in benchmark YAML: " + source);
    }

    return config;
}

} // namespace

BenchmarkConfig BenchmarkConfigLoader::FromYamlFile(const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& ex) {
        throw std::runtime_error("Failed to load YAML config '" + path + "': " + ex.what());
    }

    // Always use the new parser (old format no longer supported)
    return ParseBenchmarkConfig(root, path);
}

DataConfig BenchmarkConfigLoader::LoadDataConfigFile(const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& ex) {
        throw std::runtime_error("Failed to load data config '" + path + "': " + ex.what());
    }

    return ParseDataConfig(root, path);
}

std::string BenchmarkConfigLoader::ResolvePath(const std::string& relative_path) {
    auto base_dir = GetBenchCasesDir();
    auto resolved = base_dir / relative_path;
    return resolved.string();
}

std::string BenchmarkConfigLoader::ResolveDictionaryPath(const std::string& path) {
    auto base_dir = GetBenchCasesDir() / "datasets";
    auto resolved = base_dir / path;
    return resolved.string();
}

std::filesystem::path BenchmarkConfigLoader::GetBenchCasesDir() {
    // Get the bench_cases directory relative to the binary location
    // This assumes the binary is run from the build directory
    // You may need to adjust this based on your actual setup
    std::filesystem::path current = std::filesystem::current_path();

    // assuming we run in milvus project root directory
    std::filesystem::path bench_cases = current / "internal/core/unittest/scalar_bench/bench_cases";
    if (std::filesystem::exists(bench_cases)) {
        return bench_cases;
    }

    // Try parent directories
    bench_cases = current.parent_path() / "internal/core/unittest/scalar_bench/bench_cases";
    if (std::filesystem::exists(bench_cases)) {
        return bench_cases;
    }

    // Fallback to relative path from source location
    bench_cases = "/home/zilliz/milvus/internal/core/unittest/scalar_bench/bench_cases";
    if (std::filesystem::exists(bench_cases)) {
        return bench_cases;
    }

    throw std::runtime_error("Could not find bench_cases directory");
}

} // namespace scalar_bench
} // namespace milvus
