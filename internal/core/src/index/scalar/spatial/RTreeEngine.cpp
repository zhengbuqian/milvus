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

#include "index/scalar/spatial/RTreeEngine.h"

#include <utility>

namespace milvus::index {

RTreeBuildEngine::RTreeBuildEngine(std::string index_path)
    : index_path_(std::move(index_path)) {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:41-52, build
    // half only). Signature fix while moving: the old ctor took
    // `std::string& path` — a NON-CONST lvalue ref (RTreeIndexWrapper.h:55) —
    // which forced callers to keep a named local (RTreeIndex.cpp:75).
}

RTreeBuildEngine::~RTreeBuildEngine() = default;

void
RTreeBuildEngine::AddGeometry(const uint8_t* wkb, size_t len,
                              int64_t row_offset) {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:56-101).
    // The per-call `GEOS_init_r` + `GEOSWKBReader_create_r` (:66,:72) should
    // become per-builder state while moving — it is per-row today.
}

void
RTreeBuildEngine::BulkLoad(std::vector<rtree_detail::Value> values) {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:105-169).
    // NOTE the signature change: the old method took
    // `const std::vector<shared_ptr<FieldDataBase>>&` and did the WKB decoding
    // itself, which is what tied the ENGINE to Milvus field-data types. The
    // decoding moves up into RTreeIndexBuilder::Add; the engine now takes
    // (MBR, offset) pairs and knows nothing about columns (§6.1: "the builder's
    // input currency is a plain array").
}

void
RTreeBuildEngine::Finish() {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:171-217).
    //
    // BUG TO FIX WHILE MOVING, not to carry over: `RTreeSerializer::saveBinary`
    // returns a bool that RTreeIndexWrapper.cpp:188 DISCARDS. A serialization
    // failure is reported to std::cerr and then treated as success — the meta
    // json is still written and `finished_` is still set. Per CLAUDE.md's C++
    // error-handling rule the category must be decided at this construction
    // site (a write failure is IOError/FileWriteFailed, not silence).
}

int64_t
RTreeBuildEngine::Count() const {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:299-302).
}

const std::string&
RTreeBuildEngine::IndexPath() const {
    return index_path_;
}

RTreeQueryEngine::RTreeQueryEngine(std::string index_path)
    : index_path_(std::move(index_path)) {
}

RTreeQueryEngine::~RTreeQueryEngine() = default;

void
RTreeQueryEngine::Load() {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:219-250).
    // Same discarded-bool bug on `RTreeSerializer::loadBinary` at :241.
}

void
RTreeQueryEngine::QueryCandidates(const GEOSGeometry* query_geom,
                                  GEOSContextHandle_t ctx,
                                  std::vector<int64_t>& candidate_offsets) const {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:252-281):
    // bounding box of the query geometry, then
    // `rtree_.query(bgi::intersects(query_box), ...)`.
    //
    // The `op` parameter is NOT carried down to this level, because today it is
    // read exactly once — in a log line (RTreeIndexWrapper.cpp:278-280) — and
    // every relation runs the same MBR intersect. `SpatialReader::Candidates`
    // keeps `op` on the CONTRACT (exec must be able to say what it wants, and
    // the superset/refine contract is stated per operator), and the reader
    // decides whether the engine can use it. Pushing an ignored parameter one
    // layer further down would only re-hide the fact.
    //
    // The `shared_mutex` the old code took here (RTreeIndexWrapper.cpp:268) is
    // gone: a loaded query engine is immutable, so concurrent reads need no
    // lock (§5). The lock existed only because one class served both modes.
}

int64_t
RTreeQueryEngine::Count() const {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:299-302).
}

int64_t
RTreeQueryEngine::ByteSize() const {
    // TODO: move existing logic here (see RTreeIndexWrapper.cpp:304-319).
    // Carries a hardcoded 18-bytes-per-entry estimate (:313-316) — one of the
    // per-family fudge factors §12.3 is about.
}

}  // namespace milvus::index
