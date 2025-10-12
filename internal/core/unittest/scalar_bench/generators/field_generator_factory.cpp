#include "field_generator.h"
#include "categorical_generator.h"
#include "numeric_generator.h"
#include "timestamp_generator.h"
#include "varchar_generator.h"
#include "array_generator.h"
#include "boolean_generator.h"
#include <regex>
#include <stdexcept>
#include <sstream>

namespace milvus {
namespace scalar_bench {

std::unique_ptr<IFieldGenerator>
FieldGeneratorFactory::CreateGenerator(const FieldConfig& config) {
    switch (config.generator) {
        case FieldGeneratorType::CATEGORICAL:
            return std::make_unique<CategoricalGenerator>(config);

        case FieldGeneratorType::NUMERIC:
            return std::make_unique<NumericGenerator>(config);

        case FieldGeneratorType::TIMESTAMP:
            return std::make_unique<TimestampGenerator>(config);

        case FieldGeneratorType::VARCHAR:
            return std::make_unique<VarcharGenerator>(config);

        case FieldGeneratorType::ARRAY:
            return std::make_unique<ArrayGenerator>(config);

        case FieldGeneratorType::BOOLEAN:
            return std::make_unique<BooleanGenerator>(config);

        case FieldGeneratorType::JSON:
            // Inline lightweight JSON generator without separate class to avoid boilerplate
            class JsonInlineGenerator : public IFieldGenerator {
            public:
                explicit JsonInlineGenerator(const FieldConfig& cfg) : cfg_(cfg) {}
                DataArray Generate(size_t num_rows, RandomContext& ctx) override {
                    const auto& jcfg = cfg_.json_config;
                    // Resolve candidate JSON documents
                    std::vector<std::string> candidates;
                    if (!jcfg.values.dictionary.empty()) {
                        auto& registry = DictionaryRegistry::GetInstance();
                        candidates = registry.GetDictionary(jcfg.values.dictionary, 0);
                        // Apply candidate sub-selection
                        if (jcfg.values.pick > 0 && (size_t)jcfg.values.pick < candidates.size()) {
                            candidates.resize((size_t)jcfg.values.pick);
                        } else if (jcfg.values.random_pick > 0 && candidates.size() > (size_t)jcfg.values.random_pick) {
                            std::vector<size_t> indices(candidates.size());
                            std::iota(indices.begin(), indices.end(), 0);
                            std::shuffle(indices.begin(), indices.end(), ctx.GetRNG());
                            size_t take = (size_t)jcfg.values.random_pick;
                            std::vector<std::string> picked; picked.reserve(take);
                            for (size_t i = 0; i < take; ++i) picked.push_back(candidates[indices[i]]);
                            candidates.swap(picked);
                        }
                    } else if (!jcfg.values.inline_items.empty()) {
                        candidates = jcfg.values.inline_items;
                    } else {
                        // Provide minimal valid JSON defaults
                        candidates = {"{}", "{\"a\":1}", "{\"b\":\"x\"}", "{\"arr\":[1,2,3]}"};
                    }
                    if (candidates.empty()) {
                        throw std::runtime_error("JSON generator has no candidate values");
                    }
                    // Validate candidates are JSON-looking strings (lightweight check)
                    auto looks_like_json = [](const std::string& s) {
                        if (s.empty()) return false;
                        char c = s.front(); char e = s.back();
                        return (c == '{' && e == '}') || (c == '[' && e == ']');
                    };
                    for (const auto& v : candidates) {
                        if (!looks_like_json(v)) {
                            throw std::runtime_error("JSON candidate is not an object/array string: " + v);
                        }
                    }

                    // Prepare duplication ratios cumulative distribution if provided
                    std::vector<double> cumulative;
                    if (!jcfg.duplication_ratios.empty()) {
                        double sum = 0; cumulative.reserve(candidates.size());
                        size_t i = 0;
                        for (; i < jcfg.duplication_ratios.size() && i < candidates.size(); ++i) {
                            sum += jcfg.duplication_ratios[i];
                            cumulative.push_back(sum);
                        }
                        double remaining = 1.0 - sum;
                        if (remaining < -1e-6) {
                            throw std::runtime_error("duplication_ratios must sum to <= 1.0");
                        }
                        size_t remain_cnt = candidates.size() - i;
                        for (size_t k = 0; k < remain_cnt; ++k) {
                            sum += remain_cnt ? (remaining / (double)remain_cnt) : 0.0;
                            cumulative.push_back(sum);
                        }
                        if (!cumulative.empty()) cumulative.back() = 1.0;
                    }

                    DataArray data_array;
                    data_array.set_type(milvus::proto::schema::DataType::JSON);
                    data_array.set_field_name(cfg_.field_name);
                    data_array.set_is_dynamic(false);
                    auto* json_array = data_array.mutable_scalars()->mutable_json_data();
                    json_array->mutable_data()->Reserve(num_rows);

                    bool* null_mask = nullptr;
                    if (cfg_.nullable && cfg_.null_ratio > 0.0) {
                        auto* vd = data_array.mutable_valid_data();
                        vd->Reserve(num_rows);
                        null_mask = vd->mutable_data();
                    }

                    auto pick_index = [&](RandomContext& ctx)->size_t {
                        if (cumulative.empty()) {
                            return (size_t)ctx.UniformInt(0, (int64_t)candidates.size() - 1);
                        }
                        double r = ctx.UniformReal(0.0, 1.0);
                        auto it = std::lower_bound(cumulative.begin(), cumulative.end(), r);
                        size_t idx = (it == cumulative.end()) ? cumulative.size() - 1 : (size_t)std::distance(cumulative.begin(), it);
                        return idx;
                    };

                    for (size_t i = 0; i < num_rows; ++i) {
                        bool is_valid = true;
                        std::string value;
                        if (cfg_.nullable && cfg_.null_ratio > 0.0 && ctx.Bernoulli(cfg_.null_ratio)) {
                            is_valid = false;
                        } else {
                            value = candidates[pick_index(ctx)];
                        }
                        json_array->add_data(std::move(value));
                        if (null_mask) null_mask[i] = is_valid;
                    }
                    return data_array;
                }
                const FieldConfig& GetConfig() const override { return cfg_; }
            private:
                FieldConfig cfg_;
            };
            return std::make_unique<JsonInlineGenerator>(config);

        default:
            throw std::runtime_error("Unknown generator type");
    }
}

bool FieldGeneratorFactory::ValidateConfig(const FieldConfig& config, std::string& error_msg) {
    try {
        // Basic validation
        if (config.field_name.empty()) {
            error_msg = "Field name is required";
            return false;
        }

        // Generator-specific validation
        switch (config.generator) {
            case FieldGeneratorType::CATEGORICAL: {
                const auto& cat_config = config.categorical_config;
                if (cat_config.values.dictionary.empty() &&
                    cat_config.values.inline_items.empty()) {
                    error_msg = "Categorical generator requires dictionary or inline values";
                    return false;
                }

                // Validate duplication ratios
                if (!cat_config.duplication_ratios.empty()) {
                    double sum = 0;
                    for (double ratio : cat_config.duplication_ratios) {
                        sum += ratio;
                    }
                    if (std::abs(sum - 1.0) > 0.01) {
                        error_msg = "Duplication ratios must sum to 1.0";
                        return false;
                    }
                }
                break;
            }
            case FieldGeneratorType::JSON: {
                const auto& jcfg = config.json_config;
                // Validate candidate pools
                if (jcfg.values.dictionary.empty() && jcfg.values.inline_items.empty()) {
                    error_msg = "JSON generator requires dictionary or inline values";
                    return false;
                }
                if (!jcfg.duplication_ratios.empty()) {
                    double sum = 0; for (double r : jcfg.duplication_ratios) sum += r;
                    if (sum > 1.0 + 1e-2) { // allow small tolerance
                        error_msg = "duplication_ratios must sum to <= 1.0";
                        return false;
                    }
                    for (double r : jcfg.duplication_ratios) {
                        if (r < 0) {
                            error_msg = "duplication_ratios must be non-negative";
                            return false;
                        }
                    }
                }
                break;
            }

            case FieldGeneratorType::NUMERIC: {
                const auto& num_config = config.numeric_config;
                if (num_config.range.min >= num_config.range.max) {
                    error_msg = "Invalid numeric range: min must be less than max";
                    return false;
                }

                // Validate buckets for custom histogram
                if (num_config.distribution == Distribution::CUSTOM_HIST) {
                    if (num_config.buckets.empty()) {
                        error_msg = "CUSTOM_HIST distribution requires buckets";
                        return false;
                    }
                    for (const auto& bucket : num_config.buckets) {
                        if (bucket.min >= bucket.max) {
                            error_msg = "Invalid bucket range";
                            return false;
                        }
                        if (bucket.weight <= 0) {
                            error_msg = "Bucket weights must be positive";
                            return false;
                        }
                    }
                }
                break;
            }

            case FieldGeneratorType::TIMESTAMP: {
                const auto& ts_config = config.timestamp_config;
                if (ts_config.range.start >= ts_config.range.end) {
                    error_msg = "Invalid timestamp range: start must be less than end";
                    return false;
                }

                // Validate hotspots
                for (const auto& hotspot : ts_config.hotspots) {
                    if (hotspot.window.start >= hotspot.window.end) {
                        error_msg = "Invalid hotspot window";
                        return false;
                    }
                    if (hotspot.weight <= 0) {
                        error_msg = "Hotspot weights must be positive";
                        return false;
                    }
                }
                break;
            }

            case FieldGeneratorType::VARCHAR: {
                const auto& varchar_config = config.varchar_config;
                if (varchar_config.max_length <= 0) {
                    error_msg = "varchar max_length must be positive";
                    return false;
                }

                // Mode-specific validation
                switch (varchar_config.mode) {
                    case VarcharMode::TEMPLATE:
                        if (varchar_config.template_str.empty()) {
                            error_msg = "Template mode requires a template string";
                            return false;
                        }
                        break;
                    case VarcharMode::CORPUS:
                        if (varchar_config.corpus_file.empty()) {
                            error_msg = "Corpus mode requires a corpus file";
                            return false;
                        }
                        break;
                    case VarcharMode::RANDOM:
                        if (varchar_config.token_count.min > varchar_config.token_count.max) {
                            error_msg = "Invalid token count range";
                            return false;
                        }
                        break;
                }
                break;
            }

            case FieldGeneratorType::ARRAY: {
                const auto& array_config = config.array_config;
                if (array_config.length.min > array_config.length.max) {
                    error_msg = "Invalid array length range";
                    return false;
                }
                if (array_config.max_capacity > 0 &&
                    array_config.length.max > array_config.max_capacity) {
                    error_msg = "Array length.max exceeds max_capacity";
                    return false;
                }
                if (!array_config.element) {
                    error_msg = "Array generator requires element configuration";
                    return false;
                }
                break;
            }

            case FieldGeneratorType::BOOLEAN: {
                const auto& bool_config = config.boolean_config;
                const bool has_explicit_true_ratio = bool_config.has_true_ratio ||
                    bool_config.true_ratio != 0.5;
                const double true_ratio = has_explicit_true_ratio ? bool_config.true_ratio : 0.5;
                if (true_ratio < 0.0 || true_ratio > 1.0) {
                    error_msg = "Boolean true_ratio must be within [0, 1]";
                    return false;
                }
                break;
            }

            default:
                error_msg = "Unknown generator type";
                return false;
        }

        return true;

    } catch (const std::exception& e) {
        error_msg = std::string("Validation error: ") + e.what();
        return false;
    }
}

} // namespace scalar_bench
} // namespace milvus
