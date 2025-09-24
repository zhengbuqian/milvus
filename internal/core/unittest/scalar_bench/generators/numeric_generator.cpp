#include "numeric_generator.h"
#include <stdexcept>
#include <cmath>
#include <algorithm>

namespace milvus {
namespace scalar_bench {

NumericGenerator::NumericGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::NUMERIC) {
        throw std::runtime_error("Invalid generator type for NumericGenerator");
    }
}

DataArray
NumericGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;

    // Generate based on type and distribution
    if (num_config.type == DataType::INT64) {
        std::vector<int64_t> result;

        switch (num_config.distribution) {
            case Distribution::UNIFORM:
                result = GenerateUniform<int64_t>(num_rows, ctx);
                break;
            case Distribution::NORMAL:
                result = GenerateNormal<int64_t>(num_rows, ctx);
                break;
            case Distribution::ZIPF:
                result = GenerateZipf<int64_t>(num_rows, ctx);
                break;
            case Distribution::CUSTOM_HIST:
                result = GenerateCustomHist<int64_t>(num_rows, ctx);
                break;
            case Distribution::SEQUENTIAL:
                result = GenerateSequential<int64_t>(num_rows, ctx);
                break;
            default:
                throw std::runtime_error("Unsupported distribution");
        }

        ApplyOutliers(result, ctx);
        DataArray data_array;
        data_array.set_type(milvus::proto::schema::DataType::Int64);
        data_array.set_field_name(config_.field_name);
        data_array.set_is_dynamic(false);
        auto* long_array = data_array.mutable_scalars()->mutable_long_data();
        long_array->mutable_data()->Reserve(result.size());
        for (auto v : result) {
            long_array->add_data(v);
        }
        return data_array;

    } else if (num_config.type == DataType::FLOAT) {
        std::vector<float> result;

        switch (num_config.distribution) {
            case Distribution::UNIFORM:
                result = GenerateUniform<float>(num_rows, ctx);
                break;
            case Distribution::NORMAL:
                result = GenerateNormal<float>(num_rows, ctx);
                break;
            case Distribution::ZIPF:
                result = GenerateZipf<float>(num_rows, ctx);
                break;
            case Distribution::CUSTOM_HIST:
                result = GenerateCustomHist<float>(num_rows, ctx);
                break;
            case Distribution::SEQUENTIAL:
                throw std::runtime_error(
                    "SEQUENTIAL distribution only supports integer types");
            default:
                throw std::runtime_error("Unsupported distribution");
        }

        ApplyPrecision(result);
        ApplyOutliers(result, ctx);
        DataArray data_array;
        data_array.set_type(milvus::proto::schema::DataType::Float);
        data_array.set_field_name(config_.field_name);
        data_array.set_is_dynamic(false);
        auto* float_array = data_array.mutable_scalars()->mutable_float_data();
        float_array->mutable_data()->Reserve(result.size());
        for (auto v : result) {
            float_array->add_data(v);
        }
        return data_array;

    } else if (num_config.type == DataType::DOUBLE) {
        std::vector<double> result;

        switch (num_config.distribution) {
            case Distribution::UNIFORM:
                result = GenerateUniform<double>(num_rows, ctx);
                break;
            case Distribution::NORMAL:
                result = GenerateNormal<double>(num_rows, ctx);
                break;
            case Distribution::ZIPF:
                result = GenerateZipf<double>(num_rows, ctx);
                break;
            case Distribution::CUSTOM_HIST:
                result = GenerateCustomHist<double>(num_rows, ctx);
                break;
            case Distribution::SEQUENTIAL:
                throw std::runtime_error(
                    "SEQUENTIAL distribution only supports integer types");
            default:
                throw std::runtime_error("Unsupported distribution");
        }

        ApplyPrecision(result);
        ApplyOutliers(result, ctx);
        DataArray data_array;
        data_array.set_type(milvus::proto::schema::DataType::Double);
        data_array.set_field_name(config_.field_name);
        data_array.set_is_dynamic(false);
        auto* double_array = data_array.mutable_scalars()->mutable_double_data();
        double_array->mutable_data()->Reserve(result.size());
        for (auto v : result) {
            double_array->add_data(v);
        }
        return data_array;

    } else {
        throw std::runtime_error("Unsupported numeric type.");
    }
}

template <typename T>
std::vector<T>
NumericGenerator::GenerateUniform(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    std::vector<T> result;
    result.reserve(num_rows);

    double min = num_config.range.min;
    double max = num_config.range.max;

    for (size_t i = 0; i < num_rows; i++) {
        if constexpr (std::is_integral_v<T>) {
            result.push_back(static_cast<T>(ctx.UniformInt(
                static_cast<int64_t>(min), static_cast<int64_t>(max))));
        } else {
            result.push_back(static_cast<T>(ctx.UniformReal(min, max)));
        }
    }

    return result;
}

template <typename T>
std::vector<T>
NumericGenerator::GenerateNormal(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    std::vector<T> result;
    result.reserve(num_rows);

    double min = num_config.range.min;
    double max = num_config.range.max;
    double mean = (min + max) / 2.0;
    double stddev = (max - min) / 6.0;  // 99.7% within range

    for (size_t i = 0; i < num_rows; i++) {
        double value = ctx.Normal(mean, stddev);
        // Clamp to range
        value = std::max(min, std::min(max, value));
        result.push_back(static_cast<T>(value));
    }

    return result;
}

template <typename T>
std::vector<T>
NumericGenerator::GenerateZipf(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    std::vector<T> result;
    result.reserve(num_rows);

    double min = num_config.range.min;
    double max = num_config.range.max;
    size_t n_values = static_cast<size_t>(max - min + 1);

    for (size_t i = 0; i < num_rows; i++) {
        size_t rank = ctx.Zipf(n_values, 1.0);  // s=1.0 for standard Zipf
        T value = static_cast<T>(min + rank);
        result.push_back(value);
    }

    return result;
}

template <typename T>
std::vector<T>
NumericGenerator::GenerateCustomHist(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    std::vector<T> result;
    result.reserve(num_rows);

    if (num_config.buckets.empty()) {
        // Fall back to uniform if no buckets defined
        return GenerateUniform<T>(num_rows, ctx);
    }

    // Build cumulative weights
    std::vector<double> cumulative_weights;
    double total_weight = 0;
    for (const auto& bucket : num_config.buckets) {
        total_weight += bucket.weight;
        cumulative_weights.push_back(total_weight);
    }

    // Generate values
    for (size_t i = 0; i < num_rows; i++) {
        double r = ctx.UniformReal(0, total_weight);

        // Find bucket
        size_t bucket_idx = 0;
        for (size_t j = 0; j < cumulative_weights.size(); j++) {
            if (r <= cumulative_weights[j]) {
                bucket_idx = j;
                break;
            }
        }

        // Generate value within bucket
        const auto& bucket = num_config.buckets[bucket_idx];
        if constexpr (std::is_integral_v<T>) {
            result.push_back(static_cast<T>(
                ctx.UniformInt(static_cast<int64_t>(bucket.min),
                               static_cast<int64_t>(bucket.max))));
        } else {
            result.push_back(
                static_cast<T>(ctx.UniformReal(bucket.min, bucket.max)));
        }
    }

    return result;
}

template <typename T>
std::vector<T>
NumericGenerator::GenerateSequential(size_t num_rows, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    std::vector<T> result;
    result.reserve(num_rows);

    double min = num_config.range.min;
    double max = num_config.range.max;

    if constexpr (!std::is_integral_v<T>) {
        throw std::runtime_error(
            "SEQUENTIAL distribution only supports integer types");
    }

    if (min > max) {
        throw std::runtime_error(
            "Invalid range for sequential distribution: min greater than max");
    }

    const int64_t start = static_cast<int64_t>(std::floor(min));
    const int64_t end = static_cast<int64_t>(std::floor(max));
    if (start > end) {
        throw std::runtime_error(
            "Sequential distribution requires integer range");
    }

    const int64_t span = end - start + 1;
    if (span <= 0) {
        throw std::runtime_error(
            "Sequential distribution span must be positive");
    }

    int64_t current = start;
    for (size_t i = 0; i < num_rows; ++i) {
        result.push_back(static_cast<T>(current));
        current += 1;
        if (current > end) {
            current = start;
        }
    }

    return result;
}

template <typename T>
void
NumericGenerator::ApplyOutliers(std::vector<T>& data, RandomContext& ctx) {
    const auto& num_config = config_.numeric_config;
    const auto& outliers = num_config.outliers;

    if (outliers.ratio <= 0 || outliers.values.empty()) {
        return;
    }

    for (size_t i = 0; i < data.size(); i++) {
        if (ctx.Bernoulli(outliers.ratio)) {
            // Select a random outlier value
            size_t idx = ctx.UniformInt(0, outliers.values.size() - 1);
            data[i] = static_cast<T>(outliers.values[idx]);
        }
    }
}

template <typename T>
void
NumericGenerator::ApplyPrecision(std::vector<T>& data) {
    if constexpr (!std::is_integral_v<T>) {
        const auto& num_config = config_.numeric_config;
        if (num_config.precision > 0) {
            double multiplier = std::pow(10, num_config.precision);
            for (auto& value : data) {
                value = std::round(value * multiplier) / multiplier;
            }
        }
    }
}

}  // namespace scalar_bench
}  // namespace milvus