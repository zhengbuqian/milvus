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
// contract has no Scalar/Vector prefix: ONE BUILDER FACE SERVES BOTH FAMILIES.
// The C ABI is not templated, so exactly one type-erasure step is needed, and
// this is it. §3 principle 4 again: templates on the hot path, type erasure
// only on the management plane.
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

class BuildDriver {
 public:
    virtual ~BuildDriver() = default;

    // Static self-description; the caller decides HOW to feed from it (§6.1.2:
    // "the face is unified, the differences move into a declaration"). The five
    // input forms of §6.1.1 (A streaming / B resident / B+ contiguous /
    // C needs-a-first-pass / D local file) cut ACROSS the two families —
    // scalar alone occupies three of them — which is why splitting the Builder
    // face by family would be the wrong cut and declaring the form is the right
    // one.
    virtual const index::BuilderInputSpec&
    InputSpec() const = 0;

    // One decoded batch, pushed. The driver forwards the batch's raw pointers
    // to `index::IndexBuilder<T>::Add(n, values, valid)`.
    virtual void
    Feed(const FieldDataPtr& batch) = 0;

    // Only for `InputSpec().form == LocalFile` (form D, DiskANN): the data was
    // materialised to a local file by the shared materialiser and is handed
    // over by PATH — that family explicitly does not want it in memory
    // (`CacheRawDataToDisk<T>` -> `DISK_ANN_RAW_DATA_PATH`,
    // index/VectorDiskIndex.cpp:460-462).
    virtual void
    SetSourceFile(const std::string& path) = 0;

    // Terminates the driver. §6.1: `Seal() &&` — the Builder face is
    // one-shot, exclusive, and ends by producing an artifact.
    virtual storage::ArtifactPtr
    Seal() && = 0;
};

using BuildDriverPtr = std::unique_ptr<BuildDriver>;

// The concrete driver for one value type. Holds the family's
// `index::IndexBuilder<T>` obtained from `index::BuilderRegistry<T>`.
//
// TODO: move existing logic here — the per-type dispatch that today lives in
// `index::IndexFactory::CreateIndex`'s switch and in
// `indexbuilder::IndexFactory::CreateIndex` (indexbuilder/IndexFactory.h:46-83).
// §11.2 item 4: the God switch is replaced by per-family loader/builder
// registries, and `CreateIndexInfo` is broken up.
template <typename T>
class TypedBuildDriver : public BuildDriver {
 public:
    TypedBuildDriver(std::unique_ptr<index::IndexBuilder<T>> builder);

    const index::BuilderInputSpec&
    InputSpec() const override;

    void
    Feed(const FieldDataPtr& batch) override;

    void
    SetSourceFile(const std::string& path) override;

    storage::ArtifactPtr
    Seal() && override;

 private:
    std::unique_ptr<index::IndexBuilder<T>> builder_;
    index::BuilderInputSpec spec_;
};

// Picks `T` from the field's `DataType` and asks
// `index::BuilderRegistry<T>::Create(family, params)`.
//
// NOTE ON HYBRID (§6.3): "pick bitmap or inverted by cardinality" is a BUILD
// TIME decision, so it is a Builder strategy, not a runtime forwarding class.
// The builder chooses at `Seal()` and records the choice in the artifact's
// metadata; `IndexLoader::Open` then returns the chosen concrete reader
// directly. `index::HybridScalarIndex` disappears — the caller here never sees
// the choice. Its two-pass need (scan for cardinality, with an early exit once
// the distinct count hits its cap, index/HybridScalarIndex.cpp:167) is what
// `BuilderInputSpec::needs_second_pass` exists to declare (form C).
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
