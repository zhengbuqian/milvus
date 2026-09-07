// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/marisa/MarisaIndexBuilder.h"

#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>

#include <marisa.h>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/marisa/MarisaIndexArtifact.h"

namespace milvus::index {
namespace {

bool
IsMarisaValueType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

DataType
ParseValueType(const Config& params) {
    const char* key = nullptr;
    if (params.is_object() && params.contains("value_type")) {
        key = "value_type";
    } else if (params.is_object() && params.contains("field_type")) {
        key = "field_type";
    } else {
        return DataType::VARCHAR;
    }

    const auto& encoded = params.at(key);
    DataType type = DataType::NONE;
    try {
        if (encoded.is_string()) {
            const auto text = encoded.get<std::string>();
            if (text == "STRING") {
                type = DataType::STRING;
            } else if (text == "VARCHAR") {
                type = DataType::VARCHAR;
            } else if (text == "TEXT") {
                type = DataType::TEXT;
            } else {
                size_t parsed = 0;
                const auto numeric = std::stoi(text, &parsed);
                if (parsed != text.size()) {
                    ThrowInfo(DataTypeInvalid,
                              "unsupported marisa value type {}",
                              text);
                }
                type = static_cast<DataType>(numeric);
            }
        } else if (encoded.is_number_integer() ||
                   encoded.is_number_unsigned()) {
            type = static_cast<DataType>(encoded.get<int32_t>());
        } else {
            ThrowInfo(DataTypeInvalid,
                      "marisa parameter {} must be a data type",
                      key);
        }
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid marisa value type in {}: {}",
                  key,
                  error.what());
    } catch (const std::invalid_argument&) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported marisa value type {}",
                  encoded.dump());
    } catch (const std::out_of_range&) {
        ThrowInfo(DataTypeInvalid,
                  "marisa value type is out of range: {}",
                  encoded.dump());
    }

    if (!IsMarisaValueType(type)) {
        ThrowInfo(DataTypeInvalid,
                  "marisa requires STRING, VARCHAR, or TEXT, got {}",
                  static_cast<int32_t>(type));
    }
    return type;
}

bool
ParseNested(const Config& params) {
    std::optional<bool> nested;
    std::string_view first_key;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        const auto value = GetValueFromConfig<bool>(params, std::string(key));
        if (!value.has_value()) {
            continue;
        }
        if (nested.has_value() && *nested != *value) {
            ThrowInfo(DataTypeInvalid,
                      "marisa nested parameters {} and {} disagree",
                      first_key,
                      key);
        }
        if (!nested.has_value()) {
            nested = *value;
            first_key = key;
        }
    }
    return nested.value_or(false);
}

std::string_view
LegacyCStringValue(std::string_view value) {
    return value.substr(0, value.find('\0'));
}

size_t
LookupKeyId(const marisa::Trie& trie, std::string_view value) {
    marisa::Agent agent;
    agent.set_query(value.data(), value.size());
    if (!trie.lookup(agent)) {
        return MARISA_INVALID_KEY_ID;
    }
    return agent.key().id();
}

void
BuildCsr(const marisa::Trie& trie,
         const std::vector<int64_t>& str_ids,
         std::vector<uint32_t>& csr_index,
         std::vector<uint32_t>& csr_offsets) {
    const auto num_keys = trie.num_keys();
    AssertInfo(str_ids.size() <= std::numeric_limits<uint32_t>::max(),
               "segment row count {} exceeds uint32_t capacity for marisa CSR",
               str_ids.size());
    AssertInfo(num_keys < std::numeric_limits<uint32_t>::max(),
               "marisa trie key count {} exceeds uint32_t capacity for CSR",
               num_keys);

    csr_index.assign(num_keys + 1, 0);
    for (auto str_id : str_ids) {
        if (str_id == static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            continue;
        }
        AssertInfo(str_id >= 0 && static_cast<size_t>(str_id) < num_keys,
                   "invalid marisa key id {} while building CSR",
                   str_id);
        ++csr_index[static_cast<size_t>(str_id) + 1];
    }
    for (size_t i = 1; i < csr_index.size(); ++i) {
        csr_index[i] += csr_index[i - 1];
    }

    csr_offsets.resize(csr_index.back());
    std::vector<uint32_t> write_pos(csr_index.begin(), csr_index.end() - 1);
    for (size_t row = 0; row < str_ids.size(); ++row) {
        const auto str_id = str_ids[row];
        if (str_id == static_cast<int64_t>(MARISA_NULL_KEY_ID)) {
            continue;
        }
        csr_offsets[write_pos[static_cast<size_t>(str_id)]++] =
            static_cast<uint32_t>(row);
    }
}

}  // namespace

MarisaIndexBuilder::MarisaIndexBuilder(DataType value_type)
    : value_type_(value_type) {
    AssertInfo(IsMarisaValueType(value_type_),
               "marisa builder requires STRING, VARCHAR, or TEXT");
}

MarisaIndexBuilder::~MarisaIndexBuilder() = default;

BuilderInputSpec
MarisaIndexBuilder::InputSpec() const {
    return BuilderInputSpec{.form = BuilderInputSpec::Contiguous,
                            .needs_second_pass = false};
}

void
MarisaIndexBuilder::Add(size_t n,
                        const std::string_view* values,
                        const bool* valid) {
    if (sealed_) {
        ThrowInfo(IndexAlreadyBuild, "marisa builder has already been sealed");
    }
    AssertInfo(n == 0 || values != nullptr,
               "marisa builder received a null value array");
    AssertInfo(n <= std::numeric_limits<uint32_t>::max() - buffered_.size(),
               "marisa row count exceeds uint32_t CSR capacity");
    buffered_.reserve(buffered_.size() + n);
    valid_.reserve(valid_.size() + n);
    for (size_t i = 0; i < n; ++i) {
        const bool is_valid = valid == nullptr || valid[i];
        if (is_valid) {
            const auto value = LegacyCStringValue(values[i]);
            AssertInfo(value.size() <= std::numeric_limits<uint32_t>::max(),
                       "marisa key at row {} exceeds uint32_t length capacity",
                       buffered_.size());
            buffered_.emplace_back(value);
        } else {
            buffered_.emplace_back();
        }
        valid_.push_back(is_valid ? 1 : 0);
    }
}

storage::ArtifactPtr
MarisaIndexBuilder::Seal() && {
    if (sealed_) {
        ThrowInfo(IndexAlreadyBuild, "marisa builder has already been sealed");
    }
    sealed_ = true;

    std::vector<std::string> buffered;
    std::vector<uint8_t> valid;
    buffered.swap(buffered_);
    valid.swap(valid_);

    marisa::Keyset keyset;
    for (size_t row = 0; row < buffered.size(); ++row) {
        if (valid[row]) {
            const auto& value = buffered[row];
            keyset.push_back(value.data(), value.size());
        }
    }

    auto trie = std::make_shared<marisa::Trie>();
    trie->build(keyset, MARISA_LABEL_ORDER);

    std::vector<int64_t> str_ids(buffered.size(),
                                 static_cast<int64_t>(MARISA_NULL_KEY_ID));
    for (size_t row = 0; row < buffered.size(); ++row) {
        if (!valid[row]) {
            continue;
        }
        const auto key_id = LookupKeyId(*trie, buffered[row]);
        AssertInfo(key_id != MARISA_INVALID_KEY_ID && key_id < trie->num_keys(),
                   "failed to resolve a key inserted into marisa trie");
        str_ids[row] = static_cast<int64_t>(key_id);
    }

    std::vector<uint32_t> csr_index;
    std::vector<uint32_t> csr_offsets;
    BuildCsr(*trie, str_ids, csr_index, csr_offsets);
    return std::make_unique<MarisaIndexArtifact>(std::move(trie),
                                                 std::move(str_ids),
                                                 std::move(csr_index),
                                                 std::move(csr_offsets),
                                                 value_type_);
}

namespace {

const bool kMarisaBuilderRegistered = [] {
    BuilderRegistry<std::string_view>::Instance().Register(
        families::kMarisa, [](const BuildParams& params) {
            if (ParseNested(params)) {
                ThrowInfo(DataTypeInvalid,
                          "marisa indexes support row-domain strings only");
            }
            return std::make_unique<MarisaIndexBuilder>(ParseValueType(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
