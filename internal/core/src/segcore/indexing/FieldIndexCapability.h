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
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "index/contracts/ReaderCaps.h"

// Segment-level "what indexes does field F have".
//
// See core_refactor/01-scalar-index.md §4.1, last paragraph:
//   "segcore's `FieldIndexCapability` is the AGGREGATION of the per-index
//    `ReaderCaps` held in the inventory — per-index self-description belongs to
//    index, segment-level aggregation belongs to segcore."
//
// EVERYTHING IN THIS HEADER IS PURE DATA AND READABLE WITHOUT A PIN. That is
// the hard constraint of §4.1/§4.3: the execution-path decision runs BEFORE the
// pin, so making capability a virtual on the index object would drag every cold
// index into memory just to decide a path it may not even take. Today's comment
// at `SegmentInterface.h:236` already states the same reason for
// `GetJsonFlatIndexNestedPath()`. §10 rule 3b lints that path-decision code
// contains no pin call.
//
// NOTE ON AGGREGATION SHAPE: this is deliberately NOT a bitwise OR of every
// entry's `ReaderCaps`. `exact` is per-ENTRY (an ngram index is inexact, the
// inverted index on the same field is not), so OR-ing would produce a
// capability descriptor that describes no actual index. The aggregation is a
// LIST of entries; exec picks one entry and then reads that entry's caps.

namespace milvus::segcore {

// Addresses one index inside the segment's inventory.
//
// Whole-field indexes (inverted / bitmap / sort / marisa / fmindex / rtree /
// text / json flat) leave `json_path` empty. Per-path JSON cast indexes are
// registered under `(field, path)` — §5.7: "the per-path cast index needs no
// dedicated contract: it is an ordinary `ScalarPredicateReader<T>` registered
// in the inventory keyed by `(field, path)`".
struct IndexKey {
    FieldId field_id;
    std::string json_path;

    bool
    operator==(const IndexKey& other) const {
        return field_id == other.field_id && json_path == other.json_path;
    }
};

struct IndexKeyHash {
    size_t
    operator()(const IndexKey& key) const {
        return std::hash<int64_t>()(key.field_id.get()) ^
               (std::hash<std::string>()(key.json_path) << 1);
    }
};

// One inventory entry's capability record. Built at LOAD time from load
// metadata (family + build parameters) via `index::IndexLoader::DeriveCaps()`
// — never by touching the index object.
struct IndexCapabilityEntry {
    IndexKey key;

    // "inverted" / "bitmap" / "sort" / "marisa" / "fmindex" / "text" /
    // "ngram" / "rtree" / "json_flat" ... — `index::IndexLoader::Family()`.
    std::string family;

    // The value type the index was built on. For a JSON per-path cast index
    // this is the cast target, not `DataType::JSON`.
    DataType value_type{DataType::NONE};

    // Pure data. Equal to the pinned reader's `Caps()`, and the inventory
    // asserts that equality after every pin (§4.1: "`Caps()` on the reader
    // stays, but as a CONSISTENCY CHECK, not the query-time source").
    index::ReaderCaps caps;
};

class FieldIndexCapability {
 public:
    FieldIndexCapability() = default;

    explicit FieldIndexCapability(std::vector<IndexCapabilityEntry> entries)
        : entries_(std::move(entries)) {
    }

    bool
    empty() const {
        return entries_.empty();
    }

    const std::vector<IndexCapabilityEntry>&
    entries() const {
        return entries_;
    }

    // Face lookups. Each returns the first entry whose caps advertise the face,
    // or null when no index on this field can serve it — in which case exec
    // falls back to a column scan (§4.1: "if there is a usable index, use the
    // index; only otherwise fall back to a column scan").
    //
    // TODO: move existing logic here — today these questions are answered by
    // `SegmentExpr::HasCompatibleScalarIndex()` (exec/expression/Expr.h:2573)
    // plus `SegmentInternalInterface::HasIndex()` / `HasJsonIndex()`, and the
    // per-op refinement is a `dynamic_cast` + `ShouldUseOp()` on the PINNED
    // index (`Expr.h:2702` `CanUseIndexForOp`). Both collapse into caps reads.
    const IndexCapabilityEntry*
    Predicate() const;

    const IndexCapabilityEntry*
    PatternMatch() const;

    const IndexCapabilityEntry*
    TextMatch() const;

    // Candidate family (`caps.exact == false`): exec must re-evaluate on the
    // original values (§5.4).
    const IndexCapabilityEntry*
    Ngram() const;

    // Candidate family (`caps.exact == false`): MBR coarse filter, exec
    // refines (§5.6).
    const IndexCapabilityEntry*
    Spatial() const;

    const IndexCapabilityEntry*
    ValueLookup() const;

    // Path-addressed composite index (`JsonFlatIndex`). §5.7.
    const IndexCapabilityEntry*
    JsonPaths() const;

    // Per-path cast index registered as `(field, path)`.
    const IndexCapabilityEntry*
    PredicateAtPath(const std::string& json_path) const;

 private:
    std::vector<IndexCapabilityEntry> entries_;
};

// Value equality over `index::ReaderCaps`, used for the post-pin consistency
// assertion (§4.1). It lives here rather than in `index/contracts/` because it
// exists only for segcore's inventory check; the contract layer states the
// invariant, segcore enforces it.
bool
SameCaps(const index::ReaderCaps& a, const index::ReaderCaps& b);

}  // namespace milvus::segcore
