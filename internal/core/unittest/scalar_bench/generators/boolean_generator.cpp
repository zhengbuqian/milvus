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

    // Calculate actual true probability
    const bool has_explicit_true_ratio = bool_config.has_true_ratio ||
        bool_config.true_ratio != 0.5;
    double true_prob = has_explicit_true_ratio ? bool_config.true_ratio : 0.5;

    // Generate boolean values
    for (size_t i = 0; i < num_rows; i++) {
        result.push_back(ctx.Bernoulli(true_prob));
    }

    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Bool);
    auto* bool_array = data_array.mutable_scalars()->mutable_bool_data();
    bool_array->mutable_data()->Reserve(result.size());
    for (bool v : result) {
        bool_array->add_data(v);
    }
    return data_array;
}

} // namespace scalar_bench
} // namespace milvus
