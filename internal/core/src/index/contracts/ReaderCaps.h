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

// Capability descriptor of one index reader.
//
// See core_refactor/01-scalar-index.md §4.1.
//
// This replaces capability probing by exception and by `Support*` predicates.
// §3 principle 3 is a hard rule: an unsupported operation must not exist on the
// type, or must be expressed through `std::optional` — never through
// `ThrowInfo(Unsupported)`. §10 rule 4 lints for that pattern in contract
// implementations.
//
// HARD CONSTRAINT — `ReaderCaps` MUST BE READABLE WITHOUT PINNING.
//
// It is therefore PURE DATA: computed at load time from the index's metadata
// (family + build parameters — e.g. "inverted on a VARCHAR" => predicate +
// pattern_match + value_lookup) and stored in the inventory entry. It is NOT a
// virtual function that requires holding the index object.
//
// The reason is §4.3: the execution-path decision (`DetermineExecPath`) happens
// BEFORE the pin. Today's code comment already spells out why — "the short
// circuit paths (TextIndex/PkIndex/JsonStats) and the RawData path never call
// it, scalar index cells stay cold under tiered storage". Making caps require
// the object would pull every cold index into memory just to decide the path;
// that is a direct source of cold-fetch amplification. §10 rule 3b lints that
// path-decision code contains no pin call.
//
// `IndexReaderBase::Caps()` still exists, but as a CONSISTENCY CHECK, not as the
// query-time source: after a pin, `reader->Caps()` must equal the caps cached in
// the inventory. That equality is an assertion and a test, not a lookup path.
//
// segcore's segment-level `FieldIndexCapability` ("what indexes does field F
// have") is the aggregation of these — per-index self-description belongs to
// index, segment-level aggregation belongs to segcore (§4.1).

namespace milvus::index {

struct ReaderCaps {
    // In / NotIn / Range. Null predicates are NOT represented here: every scalar
    // family really implements them, so `NullReader` is an unconditional face
    // and gets no bit (§5, "cross-family face").
    bool predicate = false;

    // LIKE family: Match / PrefixMatch / PostfixMatch / InnerMatch.
    bool pattern_match = false;

    bool text_match = false;

    // Candidate family: the hit set is a SUPERSET, exec must verify.
    bool ngram_candidates = false;

    // Candidate family: spatial relation predicates (MBR coarse filter).
    bool spatial = false;

    // Element-level (nested) index: hits are element coordinates. The fold to
    // rows is exec's, and WHERE it happens is decided by the plan (§5.8).
    bool nested = false;

    // Can look the original value back up.
    bool value_lookup = false;

    // Per-row reverse lookup costs O(1) / O(log n). Corresponds to today's
    // `SupportFastReverseLookup`. The decision "reverse lookup is too expensive,
    // go back to the raw column" is NOT made here — the reader only states the
    // cost, the consumer chooses (§5.5).
    bool cheap_value_lookup = false;

    // A composite index addressed by path. Since shredding moved out to
    // columnar-format (§1), this means only "this index object is addressed by
    // path" — it no longer hints that a shredded sub-column exists. "This path
    // has a typed sub-column" is the COLUMN's self-description (§5.7).
    bool json_paths = false;

    // false => the hit set is a superset (ngram, spatial, nested ARRAY equality).
    bool exact = true;
};

}  // namespace milvus::index
