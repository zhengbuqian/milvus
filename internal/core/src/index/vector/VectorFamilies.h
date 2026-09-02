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

// Family names of the vector side, and where vector registration happens.
//
// See core_refactor/01-scalar-index.md §11.2 rule 4 ("split the factory by
// family: family-level loader/builder registries replace `IndexFactory`'s God
// switch") and §6.2 (`IndexLoader::Family()`).
//
// SAME CONVENTION AS `index/Families.h` (the scalar side): a family name is the
// registry key AND the value persisted in the artifact metadata under
// `families::kFamilyMetaKey`, so it is a stable on-disk contract. The two lists
// should end up in one header; they are apart here only because this change owns
// the vector half. No name collides — the vector keys are knowhere index-type
// spellings.
//
// TWO DIFFERENCES FROM THE SCALAR SIDE, both consequences of vector families
// being MANY and MECHANICALLY SIMILAR rather than few and idiosyncratic:
//
//  1. The vector family list is not hand-written here; it is derived from
//     `knowhere::IndexEnum` at registration time (VectorFamilies.cpp). Writing
//     ~20 constants by hand would guarantee drift the first time knowhere adds
//     an index type.
//  2. Registration is table-driven in ONE translation unit rather than
//     self-registration in each family's .cpp, because every knowhere in-memory
//     family shares one loader/builder pair. The static-registration hazard
//     documented in `index/Registry.cpp` applies to this TU exactly the same
//     way.

namespace milvus::index::families {

// The DiskANN family, called out by name because it is the only vector family
// with its own loader/builder/artifact triple (form D, §6.1.1).
inline constexpr const char* kDiskAnn = "DISKANN";

// Metadata key carrying the vector element type, so `VectorMemLoader::OpenIndex`
// can recover the `T` that the type-erased loader face erased. The scalar side's
// `families::kValueTypeMetaKey` serves the same purpose; see the note in
// VectorMemLoader.cpp about why the loader is not templated.

}  // namespace milvus::index::families
