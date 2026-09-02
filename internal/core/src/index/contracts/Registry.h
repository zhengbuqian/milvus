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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "index/contracts/IndexBuilder.h"
#include "index/contracts/IndexLoader.h"

// Family-level loader / builder registries.
//
// See core_refactor/01-scalar-index.md §11.2 rule 4: "SPLIT THE FACTORY BY
// FAMILY: break `CreateIndexInfo` apart, and let family-level loader/builder
// registries replace `IndexFactory`'s God switch."
//
// WHAT IS BEING REPLACED. `index/IndexFactory.h` today is a singleton with ~15
// `Create*` methods (`CreateIndex`, `CreateVectorIndex`,
// `CreatePrimitiveScalarIndex`, `CreateNgramIndex`,
// `CreateCompositeScalarIndex`, `CreateComplexScalarIndex`, `CreateJsonIndex`,
// `CreateGeometryIndex`, `CreateNestedIndex` + four nested variants,
// `CreateScalarIndex`) plus six `*LoadResource` overloads, EVERY ONE of which
// takes a `storage::FileManagerContext&` and returns an `IndexBasePtr`. Three
// things are wrong with it and each maps onto a rule:
//   - one dispatch point that must know every family (a God switch): fixed by
//     registration, so adding a family touches only that family's directory;
//   - `CreateIndexInfo` is a mixed parameter bag — field type, index type,
//     metric, dim, tantivy version, json cast type, json path, ngram params,
//     fmindex params, is_text_match, analyzer info — where MOST FIELDS ARE
//     MEANINGLESS FOR MOST FAMILIES;
//   - it hands `FileManagerContext` to every index class, which §10 rule 2
//     forbids: IO reaches a family only as an injected `storage::FileSink` /
//     `storage::FileSource`.
//
// HOW `CreateIndexInfo` IS BROKEN APART. The parameters that SELECT an
// implementation become the registry key; everything else belongs to the family
// and never appears in a shared struct. The registry does not interpret the
// family's own knobs — each family's factory parses them into its own typed
// params struct, which for two families ALREADY EXISTS in the current code
// (`NgramParams`, `FMIndexParams` in `index/IndexInfo.h`).

namespace milvus::index {

// "inverted" / "bitmap" / "stl_sort" / "marisa" / "text" / "ngram" /
// "json_flat" / "rtree" / "fmindex" / vector families...
//
// A string, matching `IndexLoader::Family()`, because the family tag is
// PERSISTED (it comes back off disk and out of the index metadata) and a
// persisted vocabulary that a closed enum would have to be recompiled to extend
// is the wrong shape for a registry key. It is NOT `knowhere::IndexType` — that
// alias is a knowhere type and §10 rule 6 keeps knowhere out of the shared root.
using IndexFamily = std::string;

// Family-specific build/load knobs, opaque to the registry.
//
// `Config` is `nlohmann::json` (`common/Types.h:673`). This is deliberately a
// bag AT THE REGISTRY BOUNDARY ONLY: the registry's job is to find the right
// factory, not to understand the knobs, and the factory's first act is to parse
// this into the family's own typed struct. It is NOT a revival of
// `CreateIndexInfo` — the difference is that no field here is read by anyone but
// the one family it belongs to, so no family carries fields meant for another.
using BuildParams = Config;

// ---------------------------------------------------------------------------
// Loader registry
// ---------------------------------------------------------------------------
//
// Loaders are stateless (§3 principle 1), so one instance per family is
// registered and shared. This registry is also where segcore load answers
// "what are this index's caps" WITHOUT PINNING ANYTHING — see
// `IndexLoader::DeriveCaps` and §4.1/§4.3.
class LoaderRegistry {
 public:
    static LoaderRegistry&
    Instance();

    void
    Register(IndexFamily family, IndexLoaderPtr loader);

    // Null when the family is unknown. NOT a throw: an unknown family is a
    // caller-visible condition, and §3 principle 3 keeps capability answers out
    // of the exception channel.
    IndexLoaderPtr
    Lookup(const IndexFamily& family) const;

    std::vector<IndexFamily>
    Families() const;

 private:
    LoaderRegistry() = default;
};

// ---------------------------------------------------------------------------
// Builder registry
// ---------------------------------------------------------------------------
//
// One registry per value type `T`, because the Builder face is typed
// (`IndexBuilder<T>`) and §3 principle 4 keeps templates on the hot path with
// type erasure only on the management face. Registration is per `(T, family)`;
// the registry itself never sees a type-erased builder.
//
// This is also where §6.3's hybrid strategy lands on the BUILD side: "pick
// bitmap or inverted by cardinality" is a build-time decision made by the hybrid
// family's own builder, which records the choice in the artifact metadata. The
// LOAD side then resolves through `LoaderRegistry` to the CHOSEN CONCRETE
// FAMILY's loader — the runtime forwarding class `HybridScalarIndex` disappears
// and never appears in either registry as a reader.
template <typename T>
class BuilderRegistry {
 public:
    using Factory =
        std::function<std::unique_ptr<IndexBuilder<T>>(const BuildParams&)>;

    static BuilderRegistry&
    Instance();

    void
    Register(IndexFamily family, Factory factory);

    bool
    Supports(const IndexFamily& family) const;

    // Null when the family is unknown or does not build this value type.
    std::unique_ptr<IndexBuilder<T>>
    Create(const IndexFamily& family, const BuildParams& params) const;

    std::vector<IndexFamily>
    Families() const;

 private:
    BuilderRegistry() = default;
};

// NOTE — WHAT IS *NOT* IN THIS FILE.
//
// `IndexFactory`'s six `*LoadResource` overloads (`IndexLoadResource`,
// `VecIndexLoadResource`, `ScalarIndexLoadResource`, ...) are not re-homed here.
// They answer "how much memory/disk will loading this cost", which is the LOAD
// SIDE'S BUDGETING question, not an index self-description — §4.2 and §12.3
// both put pre-load estimation in the segcore load translator's input metadata
// ("`EstimateBytes(load_meta)` as a free function on the translator side, not on
// the index object"). Their landing place is decided together with §12.3's
// definition of `cell_size_`, which must be closed BEFORE the artifact pipeline
// sinks to L1. Deliberately left open here rather than silently placed.

}  // namespace milvus::index
