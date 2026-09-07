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
#include <memory>
#include <string>

#include "common/FieldData.h"
#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/contracts/Registry.h"
#include "storage/artifact/Artifact.h"

// Type erasure over `index::IndexBuilder<T>` for the C ABI's benefit.
//
// See core_refactor/01-scalar-index.md §6.1 and §11.3. `IndexBuilder<T>` is a
// template on the VALUE type (variable-length values are expressed as view
// types — `std::string_view` / `ArrayView` / a sparse-row view — and a dense
// vector is `float` with `dim` known at construction), which is why the
// contract has no Scalar/Vector prefix: ONE BUILDER INTERFACE SERVES BOTH
// FAMILIES. The C ABI is not templated, so exactly one type-erasure step is
// needed, and this is it. §3 principle 4 again: templates on the hot path, type
// erasure only on the management plane.
//
// -------------------------------------------------------------------------
// THE INPUT CURRENCY IS A RAW ARRAY. §6.1's note: "the builder's input currency
// is a raw array, not any component's object — it does not know about columns,
// cursors or storage formats, so `index -> columnar-format` goes to zero on the
// builder side." `Feed()` below therefore takes a decoded batch and the driver
// hands `Add(n, values, valid)` the pointers inside it; the builder never sees
// a column or a cursor.
//
// AND THE INPUT IS PUSH, NOT PULL. There is no `Consume(ScanCursor&)` because
// the two build paths differ in source AND mode: the OFFLINE path (indexbuilder,
// the production main path) reads a remote manifest/binlog and IS ALREADY PUSH
// — `storage::IterateFieldDataFromManifest(..., const
// std::function<void(FieldDataPtr)>& consumer, max_inflight_bytes)`
// (storage/Util.h:353) calls back batch by batch, decodes on a background pool
// and throttles on INPUT bytes; there is no segment, no column object and no
// `ScanCursor` anywhere near it. Only the IN-PLACE build (segcore load) starts
// from an already-loaded column (`generate_interim_index` takes a
// `ChunkedColumnInterface`, segcore/ChunkedSegmentSealedImpl.h:1385). Flattening
// a pull into a push is a loop at the call site; wrapping a push into a pull
// needs a thread, a coroutine or a buffer inversion. Hence push.

namespace milvus::indexbuilder {

enum class FeedControl {
    Continue,
    PassComplete,
};

class BuildDriver {
 public:
    virtual ~BuildDriver() = default;

    // Per-pass self-description; the caller decides HOW to feed from it (§6.1.2:
    // "the interface is unified, the differences move into a declaration"). The
    // five input forms of §6.1.1 (A streaming / B resident / B+ contiguous /
    // C needs-a-first-pass / D local file) cut ACROSS the two families —
    // scalar alone occupies three of them — which is why splitting the Builder
    // interface by family would be the wrong cut and declaring the form is the
    // right one. Multi-pass drivers refresh this value after FinishPass().
    virtual const index::BuilderInputSpec&
    InputSpec() const = 0;

    // One decoded batch, pushed. Fixed-width values are forwarded directly;
    // owning strings and arrays are exposed as callback-scoped views before the
    // driver calls `index::IndexBuilder<T>::Add(n, values, valid)`.
    virtual FeedControl
    Feed(const FieldDataPtr& batch) = 0;

    // End the current probe pass. Valid only when InputSpec initially declared
    // needs_second_pass; implementations refresh InputSpec after the builder
    // selects its concrete delegate.
    virtual void
    FinishPass() = 0;

    // Only for `InputSpec().form == LocalFile` (form D, DiskANN): the data was
    // materialised to a local file by the shared materialiser and is handed
    // over by PATH — that family explicitly does not want it in memory
    // (`CacheRawDataToDisk<T>` -> `DISK_ANN_RAW_DATA_PATH`,
    // index/VectorDiskIndex.cpp:460-462).
    virtual void
    SetSourceFile(const std::string& path) = 0;

    // Terminates the driver. §6.1: `Seal() &&` — the Builder interface is
    // one-shot, exclusive, and ends by producing an artifact.
    virtual storage::ArtifactPtr
    Seal() && = 0;
};

using BuildDriverPtr = std::unique_ptr<BuildDriver>;

// The concrete driver for one value type. Holds the family's
// `index::IndexBuilder<T>` obtained from `index::BuilderRegistry<T>`.
//
// This is the one management-plane type-erasure point. Family construction is
// still registry-driven; this class only adapts decoded `FieldData` into the
// builder's typed raw-array currency.
template <typename T>
class TypedBuildDriver : public BuildDriver {
 public:
    TypedBuildDriver(std::unique_ptr<index::IndexBuilder<T>> builder,
                     DataType source_type,
                     DataType array_element_type = DataType::NONE);

    const index::BuilderInputSpec&
    InputSpec() const override;

    FeedControl
    Feed(const FieldDataPtr& batch) override;

    void
    FinishPass() override;

    void
    SetSourceFile(const std::string& path) override;

    storage::ArtifactPtr
        Seal() &&
        override;

 protected:
    FeedControl
    AddProjected(size_t n, const T* values, const bool* valid);

    void
    ValidateFeed(const FieldDataPtr& batch) const;

    const bool*
    UnpackValidity(const FieldDataPtr& batch);

    void
    MarkFailed() noexcept;

    void
    MarkSourceSet();

    void
    AssertOpen(const char* operation) const;

    DataType
    ArrayElementType() const {
        return array_element_type_;
    }

 private:
    enum class State { Open, Failed, Consumed };

    std::unique_ptr<index::IndexBuilder<T>> builder_;
    index::BuilderInputSpec spec_;
    DataType source_type_{DataType::NONE};
    DataType array_element_type_{DataType::NONE};
    std::unique_ptr<bool[]> validity_scratch_;
    size_t validity_capacity_{0};
    State state_{State::Open};
    bool fed_{false};
    bool source_set_{false};
};

// Validates the normalized outer/value/element types, picks `T`, and asks
// `index::BuilderRegistry<T>::Create(family, params)`.
//
// NOTE ON `side_inputs`: not a placeholder. `VectorMemIndex::Build` reads
// `VEC_OPT_FIELDS` and calls `CacheOptFieldToMemory`
// (index/VectorMemIndex.cpp:539-547) — partition-key isolation needs ANOTHER
// FIELD's data as build input, so a single-cursor signature could never have
// held it.
BuildDriverPtr
MakeBuildDriver(DataType value_type,
                const index::IndexFamily& family,
                const index::BuildParams& params);

}  // namespace milvus::indexbuilder
