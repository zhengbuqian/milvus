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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <roaring/roaring.hh>

#include "common/Array.h"
#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the bitmap family. §6.1, §6.1.1 (form **B, fully resident** —
// the value->postings map must be complete before it can be laid out), §8.

namespace milvus::index {

struct BitmapBuildParams {
    // Build over ARRAY elements rather than rows (§5.8). Persisted.
    bool nested{false};
    bool nullable{false};
    bool value_lookup{true};
    // Runtime-only reader policy. It is carried through the in-memory
    // artifact so Seal()->OpenReader() and Loader::OpenIndex() expose the
    // same lookup capability, but it is never serialized.
    bool offset_cache{false};
    DataType value_type{DataType::NONE};
};

template <typename T>
struct BitmapStoredType {
    using type = T;
};

template <>
struct BitmapStoredType<std::string_view> {
    using type = std::string;
};

template <typename T>
using bitmap_stored_t = typename BitmapStoredType<T>::type;

template <typename T>
class BitmapIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit BitmapIndexBuilder(BitmapBuildParams params);

    ~BitmapIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // Replaces primitive Build and the already-flattened nested ARRAY path.
    // Ordinary ARRAY rows use BitmapArrayIndexBuilder below because flattening
    // them here would lose the many-elements-to-one-row coordinate mapping.
    void
    Add(size_t n, const T* values, const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

    // Cardinality observed so far. The `auto` family's selection strategy
    // (§6.3) reads it to choose between bitmap and inverted at Seal() time, and
    // it is also what today's `HybridScalarIndex` recomputes with its own scan
    // (HybridScalarIndex.cpp:121-137). Exposing the count the builder already
    // has removes that second pass.
    size_t
    DistinctCount() const;

 private:
    using StoredT = bitmap_stored_t<T>;

    BitmapBuildParams params_;
    std::map<StoredT, roaring::Roaring> postings_;
    std::vector<uint8_t> validity_;
    size_t total_num_rows_{0};
};

// Ordinary ARRAY indexes consume one ArrayView per source row. This keeps the
// posting coordinate in the row domain: every element of one array points to
// the same row, while empty and null arrays still advance the row coordinate.
// Nested ARRAY indexes use the typed builders above after caller-side
// flattening, so their coordinates are consecutive element offsets.
class BitmapArrayIndexBuilder final : public IndexBuilder<ArrayView> {
 public:
    class Impl;

    explicit BitmapArrayIndexBuilder(BitmapBuildParams params);
    ~BitmapArrayIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const ArrayView* values, const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

    size_t
    DistinctCount() const;

 private:
    BitmapBuildParams params_;
    std::unique_ptr<Impl> impl_;
    std::vector<uint8_t> validity_;
    size_t total_num_rows_{0};
};

}  // namespace milvus::index
