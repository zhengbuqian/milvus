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
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/contracts/VectorReaders.h"
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
// `VectorDiskAnnIndex<T>` inheriting all of it. `IndexBase` retires in
// refactor phase 1 (§11.2 rule 3), and implementation-to-implementation
// inheritance is banned, so the shared part becomes a MEMBER of each
// interface implementation instead of a base of it. Nothing about the
// knowhere interaction changes — this is re-homing (§11.3), and the four
// `ThrowInfo` shells on today's base
// (`BuildWithRawDataForUT`, `AddWithDataset`, `VectorIterators`,
// `GetEmbListByIds`, `index/VectorIndex.h:58-123`) simply have nowhere to live
// once interfaces are separate, which is the point (§3 principle 3).
//
// The nullable offset mapping half moves to `VectorValidData` (VectorValidData.h)
// rather than here: it is orthogonal state with its own serialization, and the
// growing appender needs it without needing a built knowhere index.

namespace milvus::index {

template <typename T>
constexpr DataType
PhysicalVectorDataType() {
    if constexpr (std::is_same_v<T, float>) {
        return DataType::VECTOR_FLOAT;
    } else if constexpr (std::is_same_v<T, bin1>) {
        return DataType::VECTOR_BINARY;
    } else if constexpr (std::is_same_v<T, float16>) {
        return DataType::VECTOR_FLOAT16;
    } else if constexpr (std::is_same_v<T, bfloat16>) {
        return DataType::VECTOR_BFLOAT16;
    } else if constexpr (std::is_same_v<T, int8>) {
        return DataType::VECTOR_INT8;
    } else if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        return DataType::VECTOR_SPARSE_U32_F32;
    } else {
        static_assert(!sizeof(T), "unsupported physical vector type");
    }
}

class KnowhereEngine {
 public:
    // Plain in-memory creation. `physical_type` selects the Create<T>
    // instantiation after template erasure. `elem_type` is NONE for ordinary
    // vectors and equals physical_type for VECTOR_ARRAY indexes.
    //
    // NOTE WHAT IS ABSENT FROM THE SIGNATURE: `storage::FileManagerContext`.
    // Both of today's ctors take one and store it as `file_manager_`
    // (`VectorMemIndex.h:153`, `VectorDiskIndex.h:297`). §3 principle 6 and §10
    // rule 2 put IO behind an injected `storage::FileSink`/`FileSource` that only
    // the Builder/Artifact/Loader implementations see, never a reader.
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   bool use_knowhere_build_pool = true);

    // The knowhere DataView index used by the interim (build-in-place and
    // growing) path: it reads the caller's memory through `view_data` instead of
    // owning a copy, and has no file manager at all. Re-homed from
    // `VectorMemIndex<T>`'s second ctor (`index/VectorMemIndex.cpp:203-233`).
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   knowhere::ViewDataOp view_data,
                   bool use_knowhere_build_pool = true);

    // Disk-engine creation. `engine_object` is normally a caller-owned
    // `knowhere::Pack<std::shared_ptr<milvus::FileManager>>` and is borrowed
    // only for the synchronous IndexFactory::Create<T> call. This class never
    // retains a FileManagerContext, FileSource, or runtime load context.
    KnowhereEngine(DataType physical_type,
                   DataType elem_type,
                   IndexType index_type,
                   MetricType metric_type,
                   IndexVersion version,
                   const knowhere::Pack<std::shared_ptr<milvus::FileManager>>&
                       engine_object,
                   bool use_knowhere_build_pool = true);

    // COPY-CONSTRUCTIBLE ON PURPOSE: `knowhere::Index<T>` is an intrusively
    // ref-counted handle, and embedding-list offsets are held as a shared const
    // generation. `storage::Artifact::OpenReader() const` can therefore give a
    // reader the same immutable index state without copying index data or its
    // O(rows) offset array (§6.2's "two entrances to a reader").
    KnowhereEngine(const KnowhereEngine&) = default;
    KnowhereEngine&
    operator=(const KnowhereEngine&) = delete;
    KnowhereEngine(KnowhereEngine&& other) = default;
    KnowhereEngine&
    operator=(KnowhereEngine&& other);

    // The composed handle. Interfaces reach through it; nobody inherits it.
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

    DataType
    PhysicalType() const {
        return physical_type_;
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

    // --- pass-throughs the interfaces need ----------------------------------

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
        return elem_type_ != DataType::NONE && !EmptyEmbListOffsets().empty();
    }

    void
    SetEmptyEmbListOffsets(std::vector<size_t> offsets) {
        empty_emb_list_offsets_ =
            std::make_shared<const std::vector<size_t>>(std::move(offsets));
    }

    void
    SetEmptyEmbListOffsets(std::shared_ptr<const std::vector<size_t>> offsets) {
        empty_emb_list_offsets_ = std::move(offsets);
    }

    const std::vector<size_t>&
    EmptyEmbListOffsets() const {
        static const std::vector<size_t> empty;
        return empty_emb_list_offsets_ == nullptr ? empty
                                                  : *empty_emb_list_offsets_;
    }

 private:
    knowhere::Index<knowhere::IndexNode> index_;
    IndexType index_type_;
    MetricType metric_type_;
    int64_t dim_{0};
    IndexVersion version_{0};
    DataType physical_type_{DataType::NONE};
    // Non-NONE only for embedding-list (VECTOR_ARRAY) indexes.
    DataType elem_type_{DataType::NONE};
    bool use_knowhere_build_pool_{true};
    // One immutable generation shared by artifacts/readers that copy this
    // engine handle. A setter publishes a fresh vector and cannot mutate a
    // generation already observed by a reader.
    std::shared_ptr<const std::vector<size_t>> empty_emb_list_offsets_;
};

// Two decoders shared by every interface that hands raw vectors back. Re-homed
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

template <>
std::vector<uint8_t>
DecodeVectorByIdsResult<sparse_u32_f32>(const knowhere::DataSetPtr& result) =
    delete;

template <>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<sparse_u32_f32>(const knowhere::DataSetPtr& result) =
    delete;

}  // namespace milvus::index
