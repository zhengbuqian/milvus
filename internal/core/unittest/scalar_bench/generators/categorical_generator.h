#pragma once

#include "field_generator.h"
// forward-declare classes to reduce header dependencies
namespace milvus { namespace scalar_bench { class DictionaryRegistry; }}

namespace milvus {
namespace scalar_bench {

class CategoricalGenerator : public IFieldGenerator {
public:
    explicit CategoricalGenerator(const FieldConfig& config);

    DataArray Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    std::vector<std::string> values_;  // Cached dictionary values
    std::vector<double> cumulative_ratios_;  // For duplication ratios
    bool prepared_ = false;  // whether sub-selection and ratios are prepared

    void LoadValues();
    void EnsurePrepared(RandomContext& ctx);
    void PrepareDuplicationRatios();
    size_t SelectValueIndex(RandomContext& ctx);

    template<typename T>
    std::vector<T> GenerateTyped(size_t num_rows, RandomContext& ctx);
};

} // namespace scalar_bench
} // namespace milvus