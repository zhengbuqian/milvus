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
#include <variant>
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

// Distinguishes persisted indexes from segment-local registrations without
// borrowing one integer namespace for both. The variant preserves the complete
// signed persisted id and unsigned local registration id ranges.
class IndexIdentity {
 public:
    enum class Kind : uint8_t {
        Persisted,
        Local,
    };

    static IndexIdentity
    Persisted(int64_t index_id);

    static IndexIdentity
    Local(uint64_t registration_id);

    Kind
    kind() const;

    bool
    operator==(const IndexIdentity& other) const;

    bool
    operator<(const IndexIdentity& other) const;

 private:
    explicit IndexIdentity(std::variant<int64_t, uint64_t> value);

    std::variant<int64_t, uint64_t> value_;

    friend struct IndexKeyHash;
};

// Addresses one index inside the segment's inventory. JSON path is capability
// metadata, not identity: a persisted index keeps its catalog id across reload,
// while a built-in/interim index receives a segment-local registration id.
struct IndexKey {
    FieldId field_id;
    IndexIdentity identity;

    bool
    operator==(const IndexKey& other) const {
        return field_id == other.field_id && identity == other.identity;
    }
};

struct IndexKeyHash {
    size_t
    operator()(const IndexKey& key) const;
};

// One inventory entry's capability record. Built at LOAD time from load
// metadata (family + build parameters) via `index::IndexLoader::DeriveCaps()`
// — never by touching the index object.
struct IndexCapabilityEntry {
    IndexKey key;

    // Empty for whole-field indexes. For a per-path JSON cast index this is
    // the selected path; it never substitutes for the stable inventory key.
    std::string json_path;

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
    explicit FieldIndexCapability(
        FieldId field_id, std::vector<IndexCapabilityEntry> entries = {});

    FieldId
    field_id() const {
        return field_id_;
    }

    bool
    empty() const {
        return entries_.empty();
    }

    const std::vector<IndexCapabilityEntry>&
    entries() const {
        return entries_;
    }

    // Returns the exact entry, or null when `key` names another field or is not
    // registered. The pointer refers to `entries_`: it remains valid until this
    // capability object is destroyed, moved from, or assigned.
    const IndexCapabilityEntry*
    Find(const IndexKey& key) const;

 private:
    FieldId field_id_;
    std::vector<IndexCapabilityEntry> entries_;
};

// Value equality over `index::ReaderCaps`, used for the post-pin consistency
// assertion (§4.1). It lives here rather than in `index/contracts/` because it
// exists only for segcore's inventory check; the contract layer states the
// invariant, segcore enforces it.
bool
SameCaps(const index::ReaderCaps& a, const index::ReaderCaps& b);

}  // namespace milvus::segcore
