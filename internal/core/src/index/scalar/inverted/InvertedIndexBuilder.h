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
#include <memory>
#include <string>
#include <vector>

#include "common/Array.h"
#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"
#include "tantivy-wrapper.h"

namespace milvus::index {

class InvertedIndexDirectory;

struct InvertedBuildParams {
    std::string field_name;
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    uint32_t tantivy_index_version{0};
    bool single_segment{false};
    bool nested{false};
    std::string local_dir;
};

template <typename T>
class InvertedIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit InvertedIndexBuilder(InvertedBuildParams params);

    ~InvertedIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const T* values, const bool* valid) override;

    // Nested ARRAY callers flatten only present elements into this typed
    // builder. Each Add coordinate therefore exists: `valid` must be null or
    // all true. Parent-row validity belongs to the separately persisted parent
    // sidecar and must not be passed as an element mask here, since omitting a
    // false element would renumber every later flattened coordinate.

    storage::ArtifactPtr
        Seal() &&
        override;

 private:
    InvertedBuildParams params_;
    // Declared before the engine so the engine is destroyed before its backing
    // directory on constructor failure and ordinary destruction.
    std::shared_ptr<InvertedIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
    bool sealed_{false};
    bool failed_{false};
};

// Ordinary ARRAY inverted indexes are row-domain and add one multi-valued
// Tantivy document per ArrayView. Nested ARRAY callers flatten to the typed
// builder instead.
class InvertedArrayIndexBuilder final : public IndexBuilder<ArrayView> {
 public:
    explicit InvertedArrayIndexBuilder(InvertedBuildParams params);

    ~InvertedArrayIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const ArrayView* values, const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

 private:
    InvertedBuildParams params_;
    std::shared_ptr<InvertedIndexDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
    bool sealed_{false};
    bool failed_{false};
};

}  // namespace milvus::index
