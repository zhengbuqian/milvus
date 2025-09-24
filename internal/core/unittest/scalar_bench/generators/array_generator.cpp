#include "array_generator.h"
#include <algorithm>
#include <stdexcept>
#include <unordered_set>

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

    ValidateElementGenerator(*array_config.element);
    element_generator_ = FieldGeneratorFactory::CreateGenerator(*array_config.element);
    element_type_ = array_config.element->field_type;
}

DataArray ArrayGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    switch (element_type_) {
        case DataType::BOOL:
            return GenerateBooleanArrays(num_rows, ctx);
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            return GenerateNumericArrays(num_rows, ctx, element_type_);
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return GenerateFloatArrays(num_rows, ctx, element_type_);
        case DataType::VARCHAR:
            return GenerateStringArrays(num_rows, ctx);
        default:
            throw std::runtime_error("Unsupported element type for array generator");
    }
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

template <typename T>
std::vector<std::vector<T>>
ArrayGenerator::GenerateTyped(size_t num_rows, RandomContext& ctx) {
    std::vector<std::vector<T>> arrays;
    arrays.reserve(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        const size_t length = DetermineArrayLength(ctx);
        std::vector<T> elements;
        elements.reserve(length);

        AppendGeneratedElements(elements, length, ctx);
        ApplyContainsRules(elements, ctx);

        if (config_.array_config.unique) {
            size_t previous_size = elements.size();
            int attempts = 0;
            while (elements.size() < length && attempts < 3) {
                EnsureUniqueness(elements);
                if (elements.size() >= length || elements.size() == previous_size) {
                    break;
                }
                previous_size = elements.size();
                AppendGeneratedElements(elements, length, ctx);
                attempts++;
            }
            EnsureUniqueness(elements);
        }

        if (elements.size() > length) {
            elements.resize(length);
        }
        arrays.push_back(std::move(elements));
    }

    return arrays;
}

DataArray ArrayGenerator::GenerateStringArrays(size_t num_rows, RandomContext& ctx) {
    std::vector<std::vector<std::string>> arrays;
    arrays.reserve(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        const size_t length = DetermineArrayLength(ctx);
        std::vector<std::string> elements;
        elements.reserve(length);

        AppendGeneratedStringElements(elements, length, ctx);
        ApplyStringContainsRules(elements, ctx, length);

        if (config_.array_config.unique) {
            size_t previous_size = elements.size();
            int attempts = 0;
            while (elements.size() < length && attempts < 3) {
                EnsureStringUniqueness(elements);
                if (elements.size() >= length || elements.size() == previous_size) {
                    break;
                }
                previous_size = elements.size();
                AppendGeneratedStringElements(elements, length, ctx);
                attempts++;
            }
            EnsureStringUniqueness(elements);
        }

        if (elements.size() > length) {
            elements.resize(length);
        }
        arrays.push_back(std::move(elements));
    }
    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Array);
    auto* array_data = data_array.mutable_scalars()->mutable_array_data();
    array_data->set_element_type(static_cast<milvus::proto::schema::DataType>(milvus::DataType::VARCHAR));
    for (auto& row : arrays) {
        milvus::proto::schema::ScalarField field_data;
        for (auto& s : row) {
            field_data.mutable_string_data()->add_data(std::move(s));
        }
        *(array_data->add_data()) = std::move(field_data);
    }
    return data_array;
}

template <typename T>
void ArrayGenerator::AppendGeneratedElements(std::vector<T>& values,
                                             size_t min_count,
                                             RandomContext& ctx) {
    while (values.size() < min_count) {
        DataArray element_column = element_generator_->Generate(min_count - values.size(), ctx);
        auto batch = ExtractValuesFromDataArray<T>(element_column);
        values.insert(values.end(), batch.begin(), batch.end());
        if (batch.empty()) {
            break;
        }
    }
}

void ArrayGenerator::AppendGeneratedStringElements(std::vector<std::string>& values,
                                                   size_t min_count,
                                                   RandomContext& ctx) {
    while (values.size() < min_count) {
        DataArray element_column = element_generator_->Generate(min_count - values.size(), ctx);
        auto batch = ExtractStringValuesFromDataArray(element_column);
        values.insert(values.end(), batch.begin(), batch.end());
        if (batch.empty()) {
            break;
        }
    }
}

DataArray ArrayGenerator::GenerateNumericArrays(size_t num_rows,
                                               RandomContext& ctx,
                                               DataType numeric_type) {
    std::vector<milvus::proto::schema::ScalarField> rows;
    rows.reserve(num_rows);
    auto fill_numeric = [&](auto tag) {
        using T = decltype(tag);
        auto arrays = GenerateTyped<T>(num_rows, ctx);
        for (auto& row : arrays) {
            milvus::proto::schema::ScalarField field_data;
            for (auto& v : row) {
                if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
                    field_data.mutable_int_data()->add_data(static_cast<int32_t>(v));
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    field_data.mutable_long_data()->add_data(v);
                }
            }
            rows.emplace_back(std::move(field_data));
        }
    };
    switch (numeric_type) {
        case DataType::INT8: fill_numeric(int8_t{}); break;
        case DataType::INT16: fill_numeric(int16_t{}); break;
        case DataType::INT32: fill_numeric(int32_t{}); break;
        case DataType::INT64: fill_numeric(int64_t{}); break;
        default: throw std::runtime_error("Unsupported integer array element type");
    }
    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Array);
    auto* array_data = data_array.mutable_scalars()->mutable_array_data();
    array_data->set_element_type(static_cast<milvus::proto::schema::DataType>(numeric_type));
    for (auto& r : rows) {
        *(array_data->add_data()) = std::move(r);
    }
    return data_array;
}

DataArray ArrayGenerator::GenerateFloatArrays(size_t num_rows,
                                             RandomContext& ctx,
                                             DataType numeric_type) {
    std::vector<milvus::proto::schema::ScalarField> rows;
    rows.reserve(num_rows);
    auto fill_float = [&](auto tag) {
        using T = decltype(tag);
        auto arrays = GenerateTyped<T>(num_rows, ctx);
        for (auto& row : arrays) {
            milvus::proto::schema::ScalarField field_data;
            for (auto& v : row) {
                if constexpr (std::is_same_v<T, float>) {
                    field_data.mutable_float_data()->add_data(v);
                } else if constexpr (std::is_same_v<T, double>) {
                    field_data.mutable_double_data()->add_data(v);
                }
            }
            rows.emplace_back(std::move(field_data));
        }
    };
    switch (numeric_type) {
        case DataType::FLOAT: fill_float(float{}); break;
        case DataType::DOUBLE: fill_float(double{}); break;
        default: throw std::runtime_error("Unsupported float array element type");
    }
    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Array);
    auto* array_data = data_array.mutable_scalars()->mutable_array_data();
    array_data->set_element_type(static_cast<milvus::proto::schema::DataType>(numeric_type));
    for (auto& r : rows) {
        *(array_data->add_data()) = std::move(r);
    }
    return data_array;
}

DataArray ArrayGenerator::GenerateBooleanArrays(size_t num_rows, RandomContext& ctx) {
    auto arrays = GenerateTyped<bool>(num_rows, ctx);
    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::Array);
    auto* array_data = data_array.mutable_scalars()->mutable_array_data();
    array_data->set_element_type(static_cast<milvus::proto::schema::DataType>(milvus::DataType::BOOL));
    for (auto& row : arrays) {
        milvus::proto::schema::ScalarField field_data;
        for (auto v : row) {
            field_data.mutable_bool_data()->add_data(v);
        }
        *(array_data->add_data()) = std::move(field_data);
    }
    return data_array;
}

template <typename T>
std::vector<T>
ArrayGenerator::ExtractValuesFromDataArray(const DataArray& column) {
    std::vector<T> result;
    switch (column.type()) {
        case milvus::proto::schema::DataType::Int8:
        case milvus::proto::schema::DataType::Int16:
        case milvus::proto::schema::DataType::Int32: {
            auto src = column.scalars().int_data().data();
            result.reserve(src.size());
            for (auto v : src) result.push_back(static_cast<T>(v));
            break;
        }
        case milvus::proto::schema::DataType::Int64: {
            auto src = column.scalars().long_data().data();
            result.assign(src.begin(), src.end());
            break;
        }
        case milvus::proto::schema::DataType::Float: {
            auto src = column.scalars().float_data().data();
            result.reserve(src.size());
            for (auto v : src) result.push_back(static_cast<T>(v));
            break;
        }
        case milvus::proto::schema::DataType::Double: {
            auto src = column.scalars().double_data().data();
            result.reserve(src.size());
            for (auto v : src) result.push_back(static_cast<T>(v));
            break;
        }
        case milvus::proto::schema::DataType::Bool: {
            auto src = column.scalars().bool_data().data();
            result.reserve(src.size());
            for (auto v : src) result.push_back(static_cast<T>(v));
            break;
        }
        default:
            throw std::runtime_error("Array element generator returned unexpected type");
    }
    return result;
}

std::vector<std::string>
ArrayGenerator::ExtractStringValuesFromDataArray(const DataArray& column) {
    if (column.type() != milvus::proto::schema::DataType::VarChar &&
        column.type() != milvus::proto::schema::DataType::String) {
        throw std::runtime_error("Array element generator returned unexpected type");
    }
    std::vector<std::string> result;
    auto src = column.scalars().string_data().data();
    result.reserve(src.size());
    for (auto& s : src) result.emplace_back(s);
    return result;
}

template <typename T>
void ArrayGenerator::ApplyContainsRules(std::vector<T>&, RandomContext&) {
    // No contains rules for numeric/bool types
}

void ArrayGenerator::ApplyStringContainsRules(std::vector<std::string>& values,
                                              RandomContext& ctx,
                                              size_t target_length) {
    const auto& contains_rules = config_.array_config.contains;
    if (contains_rules.empty()) {
        return;
    }

    std::unordered_set<std::string> enforced_tokens;

    for (const auto& rule : contains_rules) {
        if (!rule.include.empty() && ctx.Bernoulli(rule.probability)) {
            for (const auto& token : rule.include) {
                if (enforced_tokens.insert(token).second) {
                    if (!values.empty()) {
                        values[ctx.UniformInt(0, values.size() - 1)] = token;
                    } else {
                        values.push_back(token);
                    }
                }
            }
        }

        if (!rule.exclude.empty() && ctx.Bernoulli(rule.probability)) {
            std::unordered_set<std::string> exclusions(rule.exclude.begin(), rule.exclude.end());
            values.erase(std::remove_if(values.begin(), values.end(), [&](const std::string& value) {
                return exclusions.find(value) != exclusions.end();
            }), values.end());
        }
    }

    const size_t max_capacity = config_.array_config.max_capacity > 0
        ? static_cast<size_t>(config_.array_config.max_capacity)
        : target_length;
    if (values.size() > max_capacity) {
        values.resize(max_capacity);
    }
}

template <typename T>
void ArrayGenerator::EnsureUniqueness(std::vector<T>& values) {
    if (!config_.array_config.unique) {
        return;
    }

    std::unordered_set<T> seen;
    auto it = values.begin();
    while (it != values.end()) {
        if (!seen.insert(*it).second) {
            it = values.erase(it);
        } else {
            ++it;
        }
    }
}

void ArrayGenerator::EnsureStringUniqueness(std::vector<std::string>& values) {
    if (!config_.array_config.unique) {
        return;
    }

    std::unordered_set<std::string> seen;
    auto it = values.begin();
    while (it != values.end()) {
        if (!seen.insert(*it).second) {
            it = values.erase(it);
        } else {
            ++it;
        }
    }
}

void ArrayGenerator::ValidateElementGenerator(const FieldConfig& element_config) {
    if (element_config.generator == FieldGeneratorType::ARRAY) {
        throw std::runtime_error("Nested arrays are not supported");
    }

    switch (element_config.field_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
            break;
        default:
            throw std::runtime_error("Unsupported array element type");
    }
}

} // namespace scalar_bench
} // namespace milvus