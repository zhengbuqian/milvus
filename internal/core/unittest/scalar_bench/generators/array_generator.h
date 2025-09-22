#pragma once

#include "field_generator.h"
#include <memory>

namespace milvus {
namespace scalar_bench {

class ArrayGenerator : public IFieldGenerator {
public:
    explicit ArrayGenerator(const FieldConfig& config);

    FieldColumn Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    std::unique_ptr<IFieldGenerator> element_generator_;

    void InitializeElementGenerator();
    size_t DetermineArrayLength(RandomContext& ctx);
    void ApplyContainsRules(void* array, RandomContext& ctx);
    void EnsureUniqueness(void* array);
};

} // namespace scalar_bench
} // namespace milvus