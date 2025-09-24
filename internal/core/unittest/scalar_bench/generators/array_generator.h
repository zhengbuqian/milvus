#pragma once

#include "field_generator.h"
#include <memory>
#include <stdexcept>
#include <variant>
#include <vector>
#include <unordered_set>

namespace milvus {
namespace scalar_bench {

class ArrayGenerator : public IFieldGenerator {
public:
    explicit ArrayGenerator(const FieldConfig& config);

    DataArray Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    std::unique_ptr<IFieldGenerator> element_generator_;
    DataType element_type_ = DataType::NONE;

    void InitializeElementGenerator();
    size_t DetermineArrayLength(RandomContext& ctx);
    template <typename T>
    std::vector<std::vector<T>> GenerateTyped(size_t num_rows, RandomContext& ctx);
    DataArray GenerateStringArrays(size_t num_rows, RandomContext& ctx);
    DataArray GenerateNumericArrays(size_t num_rows, RandomContext& ctx,
                                      DataType numeric_type);
    DataArray GenerateFloatArrays(size_t num_rows, RandomContext& ctx,
                                    DataType numeric_type);
    DataArray GenerateBooleanArrays(size_t num_rows, RandomContext& ctx);

    template <typename T>
    static std::vector<T> ExtractValuesFromDataArray(const DataArray& column);
    static std::vector<std::string> ExtractStringValuesFromDataArray(const DataArray& column);

    template <typename T>
    void AppendGeneratedElements(std::vector<T>& values, size_t min_count, RandomContext& ctx);
    void AppendGeneratedStringElements(std::vector<std::string>& values,
                                       size_t min_count,
                                       RandomContext& ctx);

    template <typename T>
    void ApplyContainsRules(std::vector<T>& values, RandomContext& ctx);
    void ApplyStringContainsRules(std::vector<std::string>& values,
                                  RandomContext& ctx,
                                  size_t target_length);

    template <typename T>
    void EnsureUniqueness(std::vector<T>& values);
    void EnsureStringUniqueness(std::vector<std::string>& values);

    void ValidateElementGenerator(const FieldConfig& element_config);
};

} // namespace scalar_bench
} // namespace milvus