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

#include <cstdint>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/VectorFaces.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/index/index.h"
#include "knowhere/index/index_node.h"
#include "knowhere/object.h"
#include "knowhere/version.h"

// The knowhere engine, HELD BY COMPOSITION.
//
// See core_refactor/01-scalar-index.md §3 principle 2 ("composition replaces
// inheritance: the tantivy wrapper, marisa and the FM structure are ENGINES that
// get composed, not base classes; implementation classes may not inherit each
// other") and §10 rule 3, which lints exactly that.
//
// WHAT THIS REPLACES. Today `class VectorIndex : public IndexBase`
// (`index/VectorIndex.h:48`) is a base class carrying (a) the knowhere handle's
// shared vocabulary — index type, metric, dim, version compatibility, mmap
// support — and (b) the nullable offset mapping, with `VectorMemIndex<T>` and
// `VectorDiskAnnIndex<T>` inheriting all of it. `IndexBase` retires in W1
// (§11.2 rule 3), and implementation-to-implementation inheritance is banned, so
// the shared part becomes a MEMBER of each face implementation instead of a base
// of it. Nothing about the knowhere interaction changes — this is re-homing
// (§11.3), and the four `ThrowInfo` shells on today's base
// (`BuildWithRawDataForUT`, `AddWithDataset`, `VectorIterators`,
// `GetEmbListByIds`, `index/VectorIndex.h:58-123`) simply have nowhere to live
// once faces are separate, which is the point (§3 principle 3).
//
// The nullable offset mapping half moves to `VectorValidData` (VectorValidData.h)
// rather than here: it is orthogonal state with its own serialization, and the
// growing appender needs it without needing a built knowhere index.

namespace milvus::index {

class KnowhereEngine {
 public:
    // The normal path. `elem_type` is `DataType::NONE` except for embedding-list
    // (VECTOR_ARRAY) indexes — re-homed from `VectorMemIndex<T>`'s ctor
    // (`index/VectorMemIndex.cpp:168-202`) and `VectorDiskAnnIndex<T>`'s
    // (`index/VectorDiskIndex.cpp:263-300`).
    //
    // NOTE WHAT IS ABSENT FROM THE SIGNATURE: `storage::FileManagerContext`.
    // Both of today's ctors take one and store it as `file_manager_`
    // (`VectorMemIndex.h:153`, `VectorDiskIndex.h:297`). §3 principle 6 and §10
    // rule 2 put IO behind an injected `storage::FileSink`/`FileSource` that only
    // the Builder/Artifact/Loader implementations see, never a reader.
    KnowhereEngine(DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   bool use_knowhere_build_pool = true);

    // The knowhere DataView index used by the interim (build-in-place and
    // growing) path: it reads the caller's memory through `view_data` instead of
    // owning a copy, and has no file manager at all. Re-homed from
    // `VectorMemIndex<T>`'s second ctor (`index/VectorMemIndex.cpp:203-233`).
    KnowhereEngine(DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   knowhere::ViewDataOp view_data,
                   bool use_knowhere_build_pool = true);

    // COPYABLE ON PURPOSE, AND IT IS A HANDLE COPY, NOT AN INDEX COPY:
    // `knowhere::Index<T>` is an intrusively ref-counted handle whose copy
    // constructor does `idx.node->IncRef(); node = idx.node;`. That is what makes
    // `storage::Artifact::OpenReader() const` implementable — the artifact keeps
    // its handle and the reader gets one onto the same knowhere node, with no
    // copy of the index data (§6.2's "two entrances to a reader").
    KnowhereEngine(const KnowhereEngine&) = default;
    KnowhereEngine&
    operator=(const KnowhereEngine&) = default;
    KnowhereEngine(KnowhereEngine&&) = default;
    KnowhereEngine&
    operator=(KnowhereEngine&&) = default;

    // The composed handle. Faces reach through it; nobody inherits it.
    knowhere::Index<knowhere::IndexNode>&
    Raw() {
        return index_;
    }

    const knowhere::Index<knowhere::IndexNode>&
    Raw() const {
        return index_;
    }

    // --- self-description ---------------------------------------------------

    IndexType
    KnowhereIndexType() const {
        return index_type_;
    }

    MetricType
    Metric() const {
        return metric_type_;
    }

    int64_t
    Dim() const {
        return dim_;
    }

    void
    SetDim(int64_t dim) {
        dim_ = dim;
    }

    DataType
    ElemType() const {
        return elem_type_;
    }

    bool
    UseBuildPool() const {
        return use_knowhere_build_pool_;
    }

    // Re-home of `VectorIndex::PrepareSearchParams`
    // (`index/VectorIndex.h:171-190`), retyped onto the narrow §12.1(a)
    // parameter struct. Same four fields, same body.
    knowhere::Json
    PrepareSearchParams(const VectorSearchParams& params) const;

    // Re-home of `VectorIndex::IsMmapSupported` (`index/VectorIndex.h:165-169`):
    // `knowhere::IndexFactory::Instance().FeatureCheck(index_type_, MMAP)`.
    // ITS CONSUMER MOVES TOO: `segcore/storagev1translator/
    // SealedIndexTranslator.cpp:202` asks this AFTER constructing the index. In
    // the target it is a LOAD-TIME question answered from metadata by
    // `IndexLoader::DeriveCaps` / the loader's own knowledge, so that mmap-vs-not
    // is decided before anything is opened (§4.1's "no pin to read caps").
    bool
    MmapSupported() const;

    // Re-home of `VectorIndex::CheckCompatible` (`index/VectorIndex.h:153-163`).
    void
    CheckCompatible(IndexVersion version) const;

    // --- pass-throughs the faces need --------------------------------------

    // `index_.Count()`, with the two zero-cases of today's
    // `VectorMemIndex<T>::Count` (`index/VectorMemIndex.h:85-95`) — an all-null
    // nullable field and an empty embedding-list index — left to the caller,
    // because the first one needs `VectorValidData` and the second needs
    // `empty_emb_list_offsets_`.
    int64_t
    RawCount() const;

    bool
    HasRawData() const;

    bool
    RefineEnabled() const;

    // Embedding-list bookkeeping, re-homed from the `empty_emb_list_offsets_`
    // member shared by both of today's index classes
    // (`VectorMemIndex.h:159`, `VectorDiskIndex.h:301`).
    bool
    IsEmptyEmbListIndex() const {
        return elem_type_ != DataType::NONE && !empty_emb_list_offsets_.empty();
    }

    void
    SetEmptyEmbListOffsets(std::vector<size_t> offsets) {
        empty_emb_list_offsets_ = std::move(offsets);
    }

    const std::vector<size_t>&
    EmptyEmbListOffsets() const {
        return empty_emb_list_offsets_;
    }

 private:
    knowhere::Index<knowhere::IndexNode> index_;
    IndexType index_type_;
    MetricType metric_type_;
    int64_t dim_{0};
    IndexVersion version_{0};
    // Non-NONE only for embedding-list (VECTOR_ARRAY) indexes.
    DataType elem_type_{DataType::NONE};
    bool use_knowhere_build_pool_{true};
    std::vector<size_t> empty_emb_list_offsets_;
};

// Two decoders shared by every face that hands raw vectors back. Re-homed
// verbatim from the `protected` statics of today's base class
// (`index/VectorIndex.h:244-276`): they only touch a knowhere DataSet and
// `milvus::fastmem::FastMemcpy`, so they are free functions here rather than
// inherited helpers (§3 principle 2).
template <typename T>
std::vector<uint8_t>
DecodeVectorByIdsResult(const knowhere::DataSetPtr& result);

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult(const knowhere::DataSetPtr& result);

}  // namespace milvus::index
