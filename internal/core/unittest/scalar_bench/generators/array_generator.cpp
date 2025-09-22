#include "array_generator.h"
#include <stdexcept>
#include <unordered_set>
#include <algorithm>

namespace milvus {
namespace scalar_bench {

ArrayGenerator::ArrayGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::ARRAY) {
        throw std::runtime_error("Invalid generator type for ArrayGenerator");
    }
    InitializeElementGenerator();
}

void ArrayGenerator::InitializeElementGenerator() {
    const auto& array_config = config_.array_config;

    // Create element generator based on element configuration
    if (!array_config.element) {
        throw std::runtime_error("Array generator requires element configuration");
    }

    // Note: In a full implementation, we'd use FieldGeneratorFactory here
    // For now, we'll create a simplified element generator
    // element_generator_ = FieldGeneratorFactory::CreateGenerator(*array_config.element);
}

FieldColumn ArrayGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    // TODO: Implement array generation once ArrayVal header dependencies are resolved
    // For now, return empty vector as placeholder
    std::vector<int64_t> placeholder;
    placeholder.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        // Generate placeholder data
        placeholder.push_back(ctx.UniformInt(0, 100));
    }
    return placeholder;
}

size_t ArrayGenerator::DetermineArrayLength(RandomContext& ctx) {
    const auto& array_config = config_.array_config;
    const auto& length = array_config.length;

    size_t array_length;

    if (length.min == length.max) {
        array_length = length.min;
    } else {
        // LengthConfig.distribution is string in current header; interpret basic cases
        if (length.distribution == Distribution::UNIFORM) {
            array_length = ctx.UniformInt(length.min, length.max);
        } else if (length.distribution == Distribution::ZIPF) {
            array_length = length.min + ctx.Zipf(length.max - length.min + 1);
        } else if (length.distribution == Distribution::NORMAL) {
            double avg = length.avg > 0 ? length.avg : (length.min + length.max) / 2.0;
            double stddev = (length.max - length.min) / 6.0;
            double val = ctx.Normal(avg, stddev);
            array_length = static_cast<size_t>(std::max(
                static_cast<double>(length.min),
                std::min(static_cast<double>(length.max), val)));
        } else {
            array_length = ctx.UniformInt(length.min, length.max);
        }
    }

    // Apply max_capacity limit
    if (array_config.max_capacity > 0) {
        array_length = std::min(array_length, static_cast<size_t>(array_config.max_capacity));
    }

    return array_length;
}

void ArrayGenerator::ApplyContainsRules(void* array, RandomContext& ctx) {
    // TODO: Implement once ArrayVal is available
    // Placeholder implementation
}

void ArrayGenerator::EnsureUniqueness(void* array) {
    // TODO: Implement once ArrayVal is available
    // Placeholder implementation
}

} // namespace scalar_bench
} // namespace milvus