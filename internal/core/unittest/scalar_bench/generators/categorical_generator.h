#pragma once

#include "field_generator.h"
#include "../dictionaries/dictionary_registry.h"

namespace milvus {
namespace scalar_bench {

class CategoricalGenerator : public IFieldGenerator {
public:
    explicit CategoricalGenerator(const FieldConfig& config);

    proto::schema::FieldData Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    std::vector<std::string> values_;  // Cached dictionary values
    std::vector<double> cumulative_ratios_;  // For duplication ratios

    void LoadValues();
    void PrepareDuplicationRatios();
    size_t SelectValueIndex(RandomContext& ctx);

    template<typename T>
    std::vector<T> GenerateTyped(size_t num_rows, RandomContext& ctx);
};

} // namespace scalar_bench
} // namespace milvus