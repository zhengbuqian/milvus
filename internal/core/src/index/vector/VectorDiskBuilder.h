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
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorValidData.h"
#include "storage/artifact/Artifact.h"

// BUILDER INTERFACE — DiskANN.
//
// See core_refactor/01-scalar-index.md §6.1.1 form D and §6.1.2.
//
// FORM D (LocalFile) — THE ONE FAMILY THAT EXPLICITLY DOES NOT WANT ITS INPUT IN
// MEMORY. Today: `VectorDiskAnnIndex<T>::Build` calls
// `file_manager_->CacheRawDataToDisk<T>(config)` and passes the resulting path
// as `build_config[DISK_ANN_RAW_DATA_PATH]`
// (`index/VectorDiskIndex.cpp:459-460`); knowhere then streams the file itself.
// That is why `BuilderInputSpec::Form::LocalFile` and `SetSourceFile` exist at
// all (§6.1.2), and why the shared materializer needs three strategies rather
// than one.
//
// !! CONTRACT WART, RECORDED NOT FIXED: `IndexBuilder<T>::Add` is PURE VIRTUAL
// while `SetSourceFile` has an empty default body. For a LocalFile family the
// polarity is inverted — `Add` is the meaningless one. The contract's own
// comment says calling `SetSourceFile` on a non-LocalFile family "is a caller
// bug", and the mirror case holds here, so this implementation asserts rather
// than throwing an `Unsupported` capability error (§3 principle 3 governs
// CAPABILITY answers, not programming errors). Cleanest fix would be a
// form-typed builder interface or a defaulted `Add`; contract owner's call. See
// the report.
//
// !! Line references point at the tree before refactor phase 1 (master
// e255009e01).

namespace milvus::index {

template <typename T>
class VectorDiskBuilder final : public IndexBuilder<T> {
 public:
    VectorDiskBuilder(DataType elem_type,
                      IndexType index_type,
                      MetricType metric_type,
                      IndexVersion version,
                      int64_t dim,
                      knowhere::Json build_params);

    ~VectorDiskBuilder() override = default;

    // Form::LocalFile.
    BuilderInputSpec
    InputSpec() const override;

    // Not the input channel for this family — see the wart note above.
    void
    Add(size_t n, const T* values, const bool* valid) override;

    // THE input channel: the shared materializer has already written the raw
    // vectors to `path`, which becomes `DISK_ANN_RAW_DATA_PATH`.
    void
    SetSourceFile(const std::string& path) override;

    storage::ArtifactPtr
    Seal() && override;

    // Embedding-list builds additionally need an offsets FILE next to the raw
    // data file (`EMB_LIST_OFFSETS_PATH`, `VectorDiskIndex.cpp:425-427`), and
    // nullable builds need a validity FILE (`VALID_DATA_PATH_KEY`, `:435-436`)
    // that knowhere writes and the builder reads back
    // (`ReadDiskValidData`, `:441`). Both are extra streams the contract's
    // single `SetSourceFile` cannot express — same shape of gap as
    // `VectorMemBuilder::SetSideInput`. Family-local, flagged.
    void
    SetEmbListOffsetsFile(const std::string& path);

    void
    SetValidDataFile(const std::string& path);

 private:
    KnowhereEngine engine_;
    knowhere::Json build_params_;
    std::string raw_data_path_;
    std::string emb_list_offsets_path_;
    std::string valid_data_path_;
    VectorValidData valid_data_;
};

}  // namespace milvus::index
