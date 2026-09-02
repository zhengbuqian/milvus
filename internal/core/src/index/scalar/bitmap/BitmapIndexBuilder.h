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
#include <string>

#include <roaring/roaring.hh>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the bitmap family. §6.1, §6.1.1 (form **B, fully resident** —
// the value->postings map must be complete before it can be laid out), §8.

namespace milvus::index {

struct BitmapBuildParams {
    // Build over ARRAY elements rather than rows (§5.8). Persisted.
    bool nested{false};
    DataType value_type{DataType::NONE};
};

template <typename T>
class BitmapIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit BitmapIndexBuilder(BitmapBuildParams params);

    ~BitmapIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // Replaces `Build(n, values, valid)` (BitmapIndex.cpp:95-127),
    // `Build(Config)` (:83-93), `BuildWithFieldData` (:147-192),
    // `BuildPrimitiveField` (:129-145), `BuildArrayField` (:194-213) and
    // `BuildArrayFieldNested` (:215-246).
    //
    // The array variants disappear for the same reason as in the inverted
    // family: flattening ARRAY rows into element values is the CALLER's
    // projection. What the builder must still be told is whether those values
    // are elements or rows, because that decides the coordinate system it
    // records in the artifact — and that is `params_.nested`, one bit, not
    // three methods.
    void
    Add(size_t n, const T* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

    // Cardinality observed so far. The `auto` family's selection strategy
    // (§6.3) reads it to choose between bitmap and inverted at Seal() time, and
    // it is also what today's `HybridScalarIndex` recomputes with its own scan
    // (HybridScalarIndex.cpp:121-137). Exposing the count the builder already
    // has removes that second pass.
    size_t
    DistinctCount() const;

 private:
    BitmapBuildParams params_;
    std::map<T, roaring::Roaring> postings_;
    TargetBitmap valid_bitset_;
    size_t total_num_rows_{0};
};

}  // namespace milvus::index
