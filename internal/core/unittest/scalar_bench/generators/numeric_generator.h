#pragma once

#include "field_generator.h"

namespace milvus {
namespace scalar_bench {

class NumericGenerator : public IFieldGenerator {
 public:
    explicit NumericGenerator(const FieldConfig& config);

    DataArray Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

 private:
    FieldConfig config_;

    template <typename T>
    std::vector<T> GenerateUniform(size_t num_rows, RandomContext& ctx);

    template <typename T>
    std::vector<T> GenerateNormal(size_t num_rows, RandomContext& ctx);

    template <typename T>
    std::vector<T> GenerateZipf(size_t num_rows, RandomContext& ctx);

    template <typename T>
    std::vector<T> GenerateCustomHist(size_t num_rows, RandomContext& ctx);

    template <typename T>
    std::vector<T> GenerateSequential(size_t num_rows, RandomContext& ctx);

    template <typename T>
    void ApplyOutliers(std::vector<T>& data, RandomContext& ctx);

    template <typename T>
    void ApplyPrecision(std::vector<T>& data);
};

}  // namespace scalar_bench
}  // namespace milvus