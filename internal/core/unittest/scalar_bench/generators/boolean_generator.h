#pragma once

#include "field_generator.h"

namespace milvus {
namespace scalar_bench {

class BooleanGenerator : public IFieldGenerator {
public:
    explicit BooleanGenerator(const FieldConfig& config);

    DataArray Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
};

} // namespace scalar_bench
} // namespace milvus