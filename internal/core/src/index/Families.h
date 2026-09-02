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

// Canonical family names.
//
// See 01-scalar-index.md §6.2 (`IndexLoader::Family()`) and §11.2 rule 4
// ("factory split per family: a family-level loader/builder registry replaces
// `IndexFactory`'s God switch").
//
// A family name is the KEY OF THE REGISTRY and the value persisted in the
// artifact metadata under `kFamilyMetaKey`. It is the only thing that decides
// which `IndexLoader` reopens a set of bytes, so it is a stable on-disk
// contract: never rename one of these strings without a format migration.
//
// These are NOT the same vocabulary as `index/Meta.h`'s user-facing index type
// names ("INVERTED", "STL_SORT", "Trie", "AUTOINDEX", ...). The user-facing
// names are a plan/proto-level concept with aliases and legacy spellings; the
// mapping from a user-facing name to a family happens once, in the index-type
// adapter, not inside any family.

namespace milvus::index::families {

inline constexpr const char* kInverted = "inverted";
inline constexpr const char* kBitmap = "bitmap";
inline constexpr const char* kSort = "sort";
inline constexpr const char* kMarisa = "marisa";
inline constexpr const char* kFmIndex = "fmindex";
inline constexpr const char* kText = "text";
inline constexpr const char* kNgram = "ngram";
inline constexpr const char* kRTree = "rtree";
inline constexpr const char* kJsonFlat = "json_flat";

// Not a family with a reader of its own: `auto` is a BUILD-TIME choice between
// `bitmap` and `inverted` made at `Seal()` from the observed cardinality
// (§6.3). Its artifact records the family that was actually chosen, and
// `AutoLoader` simply forwards to that family's loader. See
// index/scalar/auto/AutoBuilder.h.
inline constexpr const char* kAuto = "auto";

// Metadata keys written by every family's `Artifact::Serialize` into the
// `storage::FileSink` and read back by `IndexLoader::OpenIndex` /
// `IndexLoader::DeriveCaps`.
//
// `DeriveCaps` must be answerable from LOAD-TIME METADATA ALONE, without
// opening (let alone pinning) the index — §4.1's hard constraint. That is what
// these keys are for.
inline constexpr const char* kFamilyMetaKey = "index.family";
inline constexpr const char* kValueTypeMetaKey = "index.value_type";
inline constexpr const char* kCoordDomainMetaKey = "index.coord_domain";
inline constexpr const char* kCountMetaKey = "index.count";

}  // namespace milvus::index::families
