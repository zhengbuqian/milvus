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

// Family-level loader / builder registries. Selection parameters identify a
// family; the selected implementation parses its own remaining parameters.

namespace milvus::index {

// "inverted" / "bitmap" / "stl_sort" / "marisa" / "text" / "ngram" /
// "json_flat" / "rtree" / "fmindex" / vector families...
//
// A string matching `IndexLoader::Family()`. Load planning derives it from
// runtime parameters and, for HYBRID/AUTO, the format's existing selector.
using IndexFamily = std::string;

// Family-specific build/load knobs, opaque to the registry.
//
// `Config` is `nlohmann::json` (`common/Types.h:673`). This is deliberately a
// bag AT THE REGISTRY BOUNDARY ONLY: the registry's job is to find the right
// factory, not to understand the knobs. Each factory parses its own typed
// parameters immediately.
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
// One registry per value type `T`, because the Builder interface is typed
// (`IndexBuilder<T>`) and §3 principle 4 keeps templates on the hot path with
// type erasure only on the management interface. Registration is per `(T,
// family)`; the registry itself never sees a type-erased builder.
//
// The HYBRID build chooses bitmap or inverted by cardinality and writes the
// existing one-byte selector. Load planning reads only that selector and then
// resolves through `LoaderRegistry` to the concrete family's loader.
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

// Pre-load resource estimation is implemented by the free functions in
// index/LoadResource.h; it is not registry or reader state.

}  // namespace milvus::index
