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

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"
#include "tantivy-wrapper.h"

// The BUILDER of the inverted family. §6.1, §6.1.1 (form **A, truly
// streaming**: `wrapper_->add_data<T>(ptr, n, offset)` slice by slice,
// `InvertedIndexTantivy.cpp:686`), §8.

namespace milvus::index {

struct InvertedBuildParams {
    uint32_t tantivy_index_version{0};

    // `scalar_index_engine_version == 0` selected tantivy's single-segment mode
    // (IndexFactory.cpp:348).
    bool single_segment{false};

    // False only for `JsonFlatIndex` (JsonFlatIndex.h:743-744), which lets
    // tantivy assign doc ids.
    bool user_specified_doc_id{true};

    // Build over ARRAY elements rather than rows. Persisted, and surfaced by
    // the reader as `CoordDomain() == Domain::Element` (§5.8).
    bool nested{false};

    // Element type, when building over an ARRAY field.
    DataType element_type{DataType::NONE};

    std::string local_dir;
};

template <typename T>
class InvertedIndexBuilder final : public IndexBuilder<T> {
 public:
    explicit InvertedIndexBuilder(InvertedBuildParams params);

    ~InvertedIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    // ONE entry point replaces the whole build family of today's class:
    //   `Build(Config)`                       InvertedIndexTantivy.cpp:209-215
    //   `BuildWithFieldData`                  :644-734
    //   `BuildWithRawDataForUT`               :551-642
    //   `build_index_for_array` (+string spec) :736-800
    //   `build_index_for_array_nested` (+spec) :802-865
    //   `build_index_for_json` (virtual hook)  InvertedIndexTantivy.h:340-345
    //
    // The array/nested variants collapse because the CALLER now flattens: a
    // nested build is `Add` over element values with `params_.nested` set, so
    // the builder does not need to know what an `Array` is. The json variant
    // moves to the json family's builder, which owns its own path extraction —
    // it was a virtual hook on this class only because inheritance was the
    // reuse mechanism (§3 principle 2).
    void
    Add(size_t n, const T* values, const bool* valid) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    InvertedBuildParams params_;

    // Writer-mode engine. Was created by `InitForBuildIndex`
    // (InvertedIndexTantivy.cpp:72-92).
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    std::vector<size_t> null_offsets_;
    int64_t count_{0};
};

}  // namespace milvus::index
