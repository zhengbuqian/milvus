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
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/scalar/sort/IndexStructure.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the sorted family. §6.1, §6.1.1 (form **B, fully resident** —
// the array must be complete before it can be sorted), §8.

namespace milvus::index {

struct SortedBuildParams {
    bool nested{false};
    DataType value_type{DataType::NONE};
};

// Numeric / bool. Was `ScalarIndexSort<T>`'s build half.
template <typename T>
class SortedIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit SortedIndexBuilder(SortedBuildParams params);

    ~SortedIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // Replaces `Build(n, values, valid)` (ScalarIndexSort.cpp:114-147),
    // `Build(Config)` (:100-112), `BuildWithFieldData` (:149-198) and
    // `BuildWithArrayDataNested` (:200-251).
    void
    Add(size_t n, const T* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    SortedBuildParams params_;
    std::vector<IndexStructure<T>> data_;
    TargetBitmap valid_bitset_;
    size_t total_num_rows_{0};
};

// VARCHAR. Was `StringIndexSort`'s build half (see SortedIndexReader.h for why
// the two are not one class).
class SortedStringIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit SortedStringIndexBuilder(SortedBuildParams params);

    ~SortedStringIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    SortedBuildParams params_;

    // value -> row (or element) ids, built up before the sort. Was
    // `std::map<std::string, PostingList>` inside
    // `StringIndexSortMemoryImpl::BuildFromMap` (StringIndexSort.cpp:769-794).
    std::vector<std::string> unique_values_;
    std::vector<std::vector<uint32_t>> posting_lists_;
    TargetBitmap valid_bitset_;
    size_t total_num_rows_{0};
};

}  // namespace milvus::index
