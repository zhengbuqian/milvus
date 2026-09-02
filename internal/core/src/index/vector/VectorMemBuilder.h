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
#include <unordered_map>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorValidData.h"
#include "storage/artifact/Artifact.h"

// BUILDER FACE — knowhere in-memory index families.
//
// See core_refactor/01-scalar-index.md §6.1 (one builder face for both
// families), §6.1.1 (form B+), §6.1.2 (`BuilderInputSpec`), §11.3 (the Builder
// face IS shared with scalar; only the Reader face is zero-shared).
//
// WHY THIS SHARES A FACE WITH THE SCALAR FAMILIES WHILE THE READER DOES NOT
// (§11.3, and it is not a double standard): the reader faces have no shareable
// abstraction because the semantics differ completely (`In/Range/bitmap` versus
// `Search(dataset, params)`); the builder faces DO, because "feed data -> take
// shape -> produce an artifact" is genuinely isomorphic, and the real difference
// is INPUT FORM, which cuts across families — of §6.1.1's five measured forms
// the scalar families alone occupy three.
//
// FORM B+ (Contiguous). Today's body is the evidence: `VectorMemIndex<T>::Build`
// (`index/VectorMemIndex.cpp:510-704`) calls `CacheRawDataToMemory`, walks every
// `FieldData` to size the tensor, copies them into ONE contiguous buffer, and
// makes a SINGLE `GenDataset(...) -> BuildWithDataset(...)` call. Feeding it in
// slices via `Add` changes nothing about that — `Add` declares an INPUT CHANNEL,
// not an ability to build incrementally (§6.1.2). The buffering happens here.
//
// WHAT LEAVES THIS FACE, compared with today's `Build(const Config&)`:
//   - `CacheRawDataToMemory` (`VectorMemIndex.cpp:514`): the SHARED MATERIALIZER
//     does that once for all families (§6.1.2), so `FileManagerContext`
//     disappears from every builder (§3 principle 6, §10 rule 2).
//   - `Upload` (`VectorMemIndex.cpp:292-302`): upload orchestration is the
//     indexbuilder service's; the builder ends at `Seal()` (§6.2).
//   - `Serialize` (`VectorMemIndex.cpp:303-325`): the artifact's
//     (VectorMemArtifact.h, §6 reason 3).
//
// !! Line references point at the pre-W1 tree (master e255009e01).

namespace milvus::index {

template <typename T>
class VectorMemBuilder final : public IndexBuilder<T> {
 public:
    // Per-family knobs as CONSTRUCTOR ARGUMENTS, not a shared parameter bag:
    // §11.2 rule 4 breaks `CreateIndexInfo` apart precisely because most of its
    // fields are meaningless to most families. `build_params` is this family's
    // own typed-ish bag (knowhere's build config), parsed by the family factory
    // registered in Registry.h.
    VectorMemBuilder(DataType elem_type,
                     IndexType index_type,
                     MetricType metric_type,
                     IndexVersion version,
                     int64_t dim,
                     knowhere::Json build_params,
                     bool use_knowhere_build_pool = true);

    ~VectorMemBuilder() override = default;

    // Form::Contiguous (B+), no second pass.
    //
    // `side_inputs` IS LOAD-BEARING HERE AND NOWHERE ELSE IN THE VECTOR FAMILY:
    // `VectorMemIndex<T>::Build` reads `VEC_OPT_FIELDS` and calls
    // `CacheOptFieldToMemory` (`VectorMemIndex.cpp:517-523`) — partition-key
    // isolation needs ANOTHER FIELD's data as a build input, which is exactly the
    // case §6.1.2 cites for why a single-cursor signature cannot express a
    // builder's input.
    BuilderInputSpec
    InputSpec() const override;

    // Appends into the contiguous buffer. For the sparse family `T` is the
    // sparse-row view type and the rows are deep-copied, as today
    // (`VectorMemIndex.cpp:673-687` — the TODO there about giving up ownership
    // survives the move unchanged).
    void
    Add(size_t n, const T* values, const bool* valid) override;

    // `Seal()` is rvalue-qualified in the face: sealing CONSUMES the builder
    // (§3 principle 1 — the Builder is one-shot). Produces a
    // `VectorMemArtifact`, which owns the built knowhere index and knows how to
    // serialize itself (§6 reason 3) and how to open a reader in place
    // (§6.2 — the two entrances to a reader).
    storage::ArtifactPtr
    Seal() && override;

    // !! CONTRACT GAP — SIDE INPUTS ARE DECLARED BUT CANNOT BE DELIVERED.
    // `BuilderInputSpec::side_inputs` (contracts/IndexBuilder.h) names the extra
    // FieldIds a builder needs, but the face has exactly two data entrances:
    // `Add(n, values, valid)` for the primary column and `SetSourceFile(path)`
    // for form LocalFile. NOTHING carries the side-input VALUES. Today they
    // arrive as a `std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>`
    // straight out of `MemFileManagerImpl::CacheOptFieldToMemory`
    // (`VectorMemIndex.cpp:522`) and are attached to the dataset as
    // `knowhere::meta::SCALAR_INFO` (`VectorMemIndex.cpp:648`). Declared
    // family-locally here so the skeleton is honest; the fix belongs in the
    // contract (a third `Add`-like entrance keyed by FieldId). See the report.
    void
    SetSideInput(
        std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
            scalar_info);

    // Embedding-list (VECTOR_ARRAY) builds carry an offsets array alongside the
    // vectors — `dataset->Set(knowhere::meta::EMB_LIST_OFFSET, ...)`
    // (`VectorMemIndex.cpp:650-653`). Same gap as `SetSideInput`: the face has no
    // channel for a second, differently shaped input stream.
    void
    SetEmbListOffsets(std::vector<size_t> offsets);

 private:
    KnowhereEngine engine_;
    knowhere::Json build_params_;

    // Form B+ : ONE contiguous block, assembled here, handed to knowhere once.
    std::vector<uint8_t> buffer_;
    int64_t rows_{0};
    int64_t valid_rows_{0};

    // Validity accumulates while feeding and is sealed into the artifact next to
    // the index — today `Build` builds it at the very end via `BuildValidData`
    // (`VectorMemIndex.cpp:655-657,700-702`).
    std::vector<bool> valid_;
    VectorValidData valid_data_;

    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
        scalar_info_;
    std::vector<size_t> emb_list_offsets_;
};

}  // namespace milvus::index
