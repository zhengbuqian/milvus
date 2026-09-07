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

#include "index/scalar/bitmap/BitmapIndexArtifact.h"

#include <cstring>
#include <limits>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "common/Consts.h"
#include "index/Meta.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kLegacyNestedKey = "is_nested_index";
constexpr std::string_view kV3NestedKey = "is_nested";

void
CheckedAdd(size_t& total, size_t value, std::string_view label) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(
            DataTypeInvalid, "bitmap serialized {} byte size overflows", label);
    }
    total += value;
}

size_t
PackedValidityBytes(size_t count) {
    return count / 8 + static_cast<size_t>(count % 8 != 0);
}

TargetBitmap
ToBitset(const roaring::Roaring& posting, size_t count) {
    TargetBitmap result(count, false);
    for (auto coordinate : posting) {
        AssertInfo(coordinate < count,
                   "bitmap posting coordinate {} exceeds count {}",
                   coordinate,
                   count);
        result.set(coordinate);
    }
    return result;
}

template <typename T>
size_t
SerializedDataSize(const std::map<T, roaring::Roaring>& postings) {
    size_t size = 0;
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<T, std::string>) {
            CheckedAdd(size, sizeof(size_t), "string key");
            CheckedAdd(size, key.size(), "string key");
        } else {
            CheckedAdd(size, sizeof(T), "numeric key");
        }
        CheckedAdd(size, posting.getSizeInBytes(true), "posting");
    }
    return size;
}

template <typename T>
std::vector<uint8_t>
SerializeData(const std::map<T, roaring::Roaring>& postings) {
    std::vector<uint8_t> output(SerializedDataSize(postings));
    if (output.empty()) {
        return output;
    }
    auto* cursor = output.data();
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<T, std::string>) {
            const auto key_size = key.size();
            std::memcpy(cursor, &key_size, sizeof(key_size));
            cursor += sizeof(key_size);
            std::memcpy(cursor, key.data(), key_size);
            cursor += key_size;
        } else {
            std::memcpy(cursor, &key, sizeof(T));
            cursor += sizeof(T);
        }
        cursor += posting.write(reinterpret_cast<char*>(cursor), true);
    }
    AssertInfo(cursor == output.data() + output.size(),
               "bitmap serialization size mismatch");
    return output;
}

void
AppendBytes(std::vector<uint8_t>& output,
            const void* data,
            size_t size,
            std::string_view label) {
    const auto offset = output.size();
    auto total = offset;
    CheckedAdd(total, size, label);
    output.resize(total);
    if (size != 0) {
        std::memcpy(output.data() + offset, data, size);
    }
}

template <typename T>
void
AppendKey(std::vector<uint8_t>& output, const T& key) {
    if constexpr (std::is_same_v<T, std::string>) {
        const auto key_size = key.size();
        AppendBytes(output, &key_size, sizeof(key_size), "string key");
        AppendBytes(output, key.data(), key_size, "string key");
    } else {
        AppendBytes(output, &key, sizeof(T), "numeric key");
    }
}

template <typename T>
std::vector<uint8_t>
SerializeBitsetData(const std::map<T, TargetBitmap>& postings, size_t count) {
    std::vector<uint8_t> output;
    for (const auto& [key, bitset] : postings) {
        AssertInfo(bitset.size() == count,
                   "bitmap posting size {} does not match row count {}",
                   bitset.size(),
                   count);
        AppendKey(output, key);

        // Keep only one temporary posting live. This is deliberately a
        // single conversion pass rather than precomputing the final encoded
        // size by converting every bitset twice.
        roaring::Roaring posting;
        for (size_t coordinate = 0; coordinate < count; ++coordinate) {
            if (bitset[coordinate]) {
                posting.add(static_cast<uint32_t>(coordinate));
            }
        }
        const auto encoded_size = posting.getSizeInBytes(true);
        const auto offset = output.size();
        auto total = offset;
        CheckedAdd(total, encoded_size, "posting");
        output.resize(total);
        const auto written = posting.write(
            reinterpret_cast<char*>(output.data() + offset), true);
        AssertInfo(written == encoded_size,
                   "bitmap posting serialization size mismatch");
    }
    return output;
}

template <typename T>
size_t
PostingCount(const BitmapPostingStorage<T>& storage) {
    return storage.layout == BitmapLayout::Roaring
               ? storage.roaring_postings.size()
               : storage.bitset_postings.size();
}

std::vector<uint8_t>
SerializeValidity(const TargetBitmap& validity, size_t count) {
    std::vector<uint8_t> output(PackedValidityBytes(count), 0);
    for (size_t i = 0; i < count; ++i) {
        if (validity[i]) {
            output[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return output;
}

}  // namespace

template <typename T>
BitmapIndexArtifact<T>::BitmapIndexArtifact(
    std::map<T, roaring::Roaring> postings,
    TargetBitmap valid_bitset,
    size_t total_num_rows,
    DataType value_type,
    bool nested,
    bool nullable,
    bool value_lookup,
    bool offset_cache)
    : state_(BuilderState{.postings = std::move(postings),
                          .valid_bitset = std::move(valid_bitset),
                          .total_num_rows = total_num_rows,
                          .value_type = value_type,
                          .nested = nested,
                          .nullable = nullable,
                          .value_lookup = value_lookup,
                          .offset_cache = offset_cache}) {
}

template <typename T>
BitmapIndexArtifact<T>::BitmapIndexArtifact(RewriteState state)
    : state_(std::move(state)) {
    const auto& rewrite = std::get<RewriteState>(state_);
    AssertInfo(rewrite.postings != nullptr,
               "bitmap rewrite artifact requires posting storage");
    AssertInfo(rewrite.valid_bitset != nullptr,
               "bitmap rewrite artifact requires a validity bitmap");
    AssertInfo(rewrite.valid_bitset->size() == rewrite.total_num_rows,
               "bitmap rewrite validity size {} does not match row count {}",
               rewrite.valid_bitset->size(),
               rewrite.total_num_rows);
}

template <typename T>
BitmapIndexArtifact<T>::~BitmapIndexArtifact() = default;

template <typename T>
std::shared_ptr<storage::LoadedArtifact>
BitmapIndexArtifact<T>::OpenReader() const {
    RewriteState args;
    if (const auto* rewrite = std::get_if<RewriteState>(&state_)) {
        args = *rewrite;
    } else {
        const auto& builder = std::get<BuilderState>(state_);
        const auto layout =
            builder.postings.size() <
                    static_cast<size_t>(DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND)
                ? BitmapLayout::Bitset
                : BitmapLayout::Roaring;
        auto postings = std::make_shared<BitmapPostingStorage<T>>();
        postings->layout = layout;
        if (layout == BitmapLayout::Roaring) {
            postings->roaring_postings = builder.postings;
        } else {
            for (const auto& [key, posting] : builder.postings) {
                postings->bitset_postings.emplace(
                    key, ToBitset(posting, builder.total_num_rows));
            }
        }
        args.postings = std::move(postings);
        args.valid_bitset =
            std::make_shared<TargetBitmap>(builder.valid_bitset.clone());
        args.total_num_rows = builder.total_num_rows;
        args.nested = builder.nested;
        args.nullable = builder.nullable;
        args.value_lookup = builder.value_lookup;
        args.value_type = builder.value_type;
        args.offset_cache = builder.offset_cache;
    }

    if constexpr (std::is_same_v<T, std::string>) {
        return std::make_shared<BitmapStringIndexReader>(std::move(args));
    } else {
        return std::make_shared<BitmapIndexReader<T>>(std::move(args));
    }
}

template <typename T>
void
BitmapIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    std::vector<uint8_t> data;
    size_t posting_count = 0;
    size_t total_num_rows = 0;
    bool nested = false;
    bool nullable = false;
    const TargetBitmap* valid_bitset = nullptr;

    if (const auto* builder = std::get_if<BuilderState>(&state_)) {
        data = SerializeData(builder->postings);
        posting_count = builder->postings.size();
        total_num_rows = builder->total_num_rows;
        nested = builder->nested;
        nullable = builder->nullable;
        valid_bitset = &builder->valid_bitset;
    } else {
        const auto& rewrite = std::get<RewriteState>(state_);
        AssertInfo(rewrite.postings != nullptr,
                   "bitmap rewrite artifact lost posting storage");
        AssertInfo(rewrite.valid_bitset != nullptr,
                   "bitmap rewrite artifact lost validity bitmap");
        const auto& postings = *rewrite.postings;
        data = postings.layout == BitmapLayout::Roaring
                   ? SerializeData(postings.roaring_postings)
                   : SerializeBitsetData(postings.bitset_postings,
                                         rewrite.total_num_rows);
        posting_count = PostingCount(postings);
        total_num_rows = rewrite.total_num_rows;
        nested = rewrite.nested;
        nullable = rewrite.nullable;
        valid_bitset = rewrite.valid_bitset.get();
    }

    if (sink.Gen() == storage::Generation::V1V2) {
        YAML::Node meta;
        meta[BITMAP_INDEX_LENGTH] = posting_count;
        meta[BITMAP_INDEX_NUM_ROWS] = total_num_rows;
        meta[std::string(kLegacyNestedKey)] = nested;
        std::stringstream stream;
        stream << meta;
        const auto encoded = stream.str();
        sink.WriteEntry(BITMAP_INDEX_META, encoded.data(), encoded.size());
    } else {
        sink.PutMeta(BITMAP_INDEX_LENGTH, nlohmann::json(posting_count));
        sink.PutMeta(BITMAP_INDEX_NUM_ROWS, nlohmann::json(total_num_rows));
        sink.PutMeta(kV3NestedKey, nlohmann::json(nested));
    }

    sink.WriteEntry(BITMAP_INDEX_DATA, data.data(), data.size());
    if (nullable) {
        AssertInfo(valid_bitset != nullptr,
                   "bitmap nullable artifact lost validity bitmap");
        const auto validity = SerializeValidity(*valid_bitset, total_num_rows);
        sink.WriteEntry(
            BITMAP_INDEX_VALID_BITSET, validity.data(), validity.size());
    }
}

#define INSTANTIATE_BITMAP_ARTIFACT(T) template class BitmapIndexArtifact<T>;
INSTANTIATE_BITMAP_ARTIFACT(bool)
INSTANTIATE_BITMAP_ARTIFACT(int8_t)
INSTANTIATE_BITMAP_ARTIFACT(int16_t)
INSTANTIATE_BITMAP_ARTIFACT(int32_t)
INSTANTIATE_BITMAP_ARTIFACT(int64_t)
INSTANTIATE_BITMAP_ARTIFACT(float)
INSTANTIATE_BITMAP_ARTIFACT(double)
INSTANTIATE_BITMAP_ARTIFACT(std::string)
#undef INSTANTIATE_BITMAP_ARTIFACT

}  // namespace milvus::index
