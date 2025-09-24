#include "boolean_generator.h"
#include <stdexcept>

namespace milvus {
namespace scalar_bench {

BooleanGenerator::BooleanGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::BOOLEAN) {
        throw std::runtime_error("Invalid generator type for BooleanGenerator");
    }
}

DataArray BooleanGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    const auto& bool_config = config_.boolean_config;
    std::vector<bool> result;
    result.reserve(num_rows);
    std::vector<bool> null_mask;

    // Calculate actual true probability
    const bool has_explicit_true_ratio = bool_config.has_true_ratio ||
        bool_config.true_ratio != 0.5;
    double true_prob = has_explicit_true_ratio ? bool_config.true_ratio : 0.5;

    // Generate boolean values
    null_mask.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        bool val = ctx.Bernoulli(true_prob);
        bool is_valid = true;
        if (config_.nullable && config_.null_ratio > 0.0 && ctx.Bernoulli(config_.null_ratio)) {
            is_valid = false;
            val = false;
        }
        result.push_back(val);
        if (config_.nullable && config_.null_ratio > 0.0) null_mask.push_back(is_valid);
    }

    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Bool);
    data_array.set_field_name(config_.field_name);
    data_array.set_is_dynamic(false);
    auto* bool_array = data_array.mutable_scalars()->mutable_bool_data();
    bool_array->mutable_data()->Reserve(result.size());
    for (bool v : result) {
        bool_array->add_data(v);
    }
    if (!null_mask.empty()) {
        auto* vd = data_array.mutable_valid_data();
        vd->mutable_data()->Reserve(null_mask.size());
        for (auto b : null_mask) vd->add_data(b);
    }
    return data_array;
}

} // namespace scalar_bench
} // namespace milvus
