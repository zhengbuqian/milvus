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

#include "common/Types.h"
#include "index/contracts/ReaderCaps.h"
#include "storage/artifact/LoadedArtifact.h"

// The type-erased root of the Reader face.
//
// See core_refactor/01-scalar-index.md §4.2, and §4 for why the query faces do
// NOT derive from it.
//
// THE QUERY FACES ARE PURE MIXINS. They do not inherit `IndexReaderBase`. If
// each face did `virtual public IndexReaderBase`, then multiple inheritance in
// an implementation class would need VIRTUAL inheritance to keep the root
// subobject unique; the price is an extra indirection on every access to a root
// member from the implementation, and root->face would have to go through a
// virtual-base `dynamic_cast`. As pure mixins instead: an implementation class
// inherits the root and its faces NON-virtually, root->face is a single sibling
// cast, and the faces need none of the root's metadata anyway (the inventory
// holds that, §4.3). §10 rule 3 lints this.
//
// WHAT IS NOT HERE, compared with today's `IndexBase`:
//   - `Serialize` / `Load` x2 / `Upload` / `LoadUnified` / `UploadUnified`:
//     serialization is `storage::Artifact`, opening is `IndexLoader`, upload is
//     the indexbuilder service, load orchestration is segcore load (§6.2).
//   - `Build` x3 / `BuildWithDataset` / `BuildWithRawDataForUT`: the Builder face.
//   - `SetCellSize` / `ComputeByteSize`: cache accounting is the load-side
//     translator's; only `LoadedArtifact::CellByteSize()` survives, at L1.
//   - `GetCastType` / `Exists`: json-only, moved into `JsonIndexReader` (§5.7).

namespace milvus::index {

// The coordinate system an index's results live in. A row-level index yields row
// numbers, an element-level (nested) index yields element numbers (§4.2, §5.8).
enum class Domain {
    Row,
    Element,
};

class IndexReaderBase : public storage::LoadedArtifact {
 public:
    ~IndexReaderBase() override = default;

    // Self-description. The QUERY-TIME path decision does NOT read this — that
    // goes through the inventory's cached caps (§4.1/§4.3). This method exists
    // for growing snapshots, unit tests, and the consistency assertion
    // "pinned reader's Caps() == inventory's cached caps".
    virtual ReaderCaps
    Caps() const = 0;

    // Row (row-level index) or Element (nested index).
    virtual Domain
    CoordDomain() const = 0;

    // The CARDINALITY OF THIS READER'S OWN COORDINATE SYSTEM — not necessarily a
    // row count (§4.2, §5). Every bitmap this reader returns has this size, and
    // the consumer reads it in the unit `CoordDomain()` states. The index NEVER
    // folds across coordinate systems (§5.8).
    virtual int64_t
    Count() const = 0;

    virtual DataType
    ValueType() const = 0;

    // Pure self-description. Cache accounting is the load-side translator's job
    // — see `storage::LoadedArtifact::CellByteSize()` and the open question in
    // §12.3 about what that number even means today.
    virtual int64_t
    MemoryUsage() const = 0;
};

using IndexReaderBasePtr = std::shared_ptr<IndexReaderBase>;

}  // namespace milvus::index
