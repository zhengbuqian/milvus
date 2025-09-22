#include "field_generator.h"
#include "categorical_generator.h"
#include "numeric_generator.h"
#include "timestamp_generator.h"
#include "varchar_generator.h"
#include "array_generator.h"
#include "boolean_generator.h"
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
