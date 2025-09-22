#pragma once

#include "field_generator.h"

namespace milvus {
namespace scalar_bench {

class TimestampGenerator : public IFieldGenerator {
public:
    explicit TimestampGenerator(const FieldConfig& config);

    FieldColumn Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    double hotspot_total_weight_ = 0.0;

    // Wrapper around numeric generator for epoch values
    std::vector<int64_t> GenerateEpochValues(size_t num_rows, RandomContext& ctx);

    // Apply hotspot windows
    void ApplyHotspots(std::vector<int64_t>& timestamps, RandomContext& ctx);

    // Apply jitter
    void ApplyJitter(std::vector<int64_t>& timestamps, RandomContext& ctx);
};

} // namespace scalar_bench
} // namespace milvus