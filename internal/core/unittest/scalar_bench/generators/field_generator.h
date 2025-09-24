#pragma once

#include <memory>
#include <vector>
#include <variant>
#include <string>
#include <random>
#include "common/Types.h"
#include "../config/benchmark_config.h"
#include "knowhere/dataset.h"

namespace milvus {
namespace scalar_bench {

// Random context for consistent generation
class RandomContext {
public:
    explicit RandomContext(uint32_t seed) : rng_(seed), seed_(seed) {}

    std::mt19937& GetRNG() { return rng_; }
    uint32_t GetSeed() const { return seed_; }

    // Utility methods for common distributions
    int64_t UniformInt(int64_t min, int64_t max) {
        std::uniform_int_distribution<int64_t> dist(min, max);
        return dist(rng_);
    }

    double UniformReal(double min, double max) {
        std::uniform_real_distribution<double> dist(min, max);
        return dist(rng_);
    }

    bool Bernoulli(double p) {
        std::bernoulli_distribution dist(p);
        return dist(rng_);
    }

    double Normal(double mean, double stddev) {
        std::normal_distribution<double> dist(mean, stddev);
        return dist(rng_);
    }

    // Zipf distribution (simplified implementation)
    size_t Zipf(size_t n, double s = 1.0) {
        // Simple rejection sampling for Zipf distribution
        // For production, consider using a more efficient algorithm
        static std::vector<double> probabilities;
        if (probabilities.size() != n) {
            probabilities.clear();
            double sum = 0;
            for (size_t i = 1; i <= n; i++) {
                sum += 1.0 / std::pow(i, s);
            }
            for (size_t i = 1; i <= n; i++) {
                probabilities.push_back((1.0 / std::pow(i, s)) / sum);
            }
        }

        double r = UniformReal(0, 1);
        double cumsum = 0;
        for (size_t i = 0; i < probabilities.size(); i++) {
            cumsum += probabilities[i];
            if (r <= cumsum) return i;
        }
        return n - 1;
    }

private:
    std::mt19937 rng_;
    uint32_t seed_;
};

// Base interface for field generators
class IFieldGenerator {
public:
    virtual ~IFieldGenerator() = default;

    // Generate data for the specified number of rows
    virtual DataArray Generate(size_t num_rows, RandomContext& ctx) = 0;

    // Get the field configuration
    virtual const FieldConfig& GetConfig() const = 0;
};

// Generator factory
class FieldGeneratorFactory {
public:
    // Create a generator based on the field configuration
    static std::unique_ptr<IFieldGenerator> CreateGenerator(const FieldConfig& config);

    // Validate that a configuration can be used to create a generator
    static bool ValidateConfig(const FieldConfig& config, std::string& error_msg);
};

// Base class with common functionality
template<typename T>
class BaseFieldGenerator : public IFieldGenerator {
public:
    explicit BaseFieldGenerator(const FieldConfig& config) : config_(config) {}

    const FieldConfig& GetConfig() const override {
        return config_;
    }

protected:
    FieldConfig config_;

    // Apply null ratio to generated data
    void ApplyNullMask(std::vector<T>& data, std::vector<bool>& null_mask,
                       double null_ratio, RandomContext& ctx) {
        if (null_ratio <= 0) return;

        null_mask.resize(data.size(), false);
        for (size_t i = 0; i < data.size(); i++) {
            if (ctx.Bernoulli(null_ratio)) {
                null_mask[i] = true;
                // Optionally set to default value
                data[i] = T{};
            }
        }
    }
};

} // namespace scalar_bench
} // namespace milvus