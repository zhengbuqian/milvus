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

#include "index/contracts/Registry.h"

#include <mutex>
#include <string>
#include <unordered_map>

#include "common/EasyAssert.h"

// Implementation of the contract-layer registries (index/contracts/Registry.h).
//
// See 01-scalar-index.md §11.2 rule 4: "factory split per family: a
// family-level loader/builder registry replaces `IndexFactory`'s God switch".
//
// WHY THIS FILE LIVES HERE AND NOT IN contracts/: contracts/ is interface-only
// by construction (see its README). The registry is the one contract type that
// needs a definition somewhere, and the top level of index/ is the only place
// that is above every family and below nothing.
//
// WHAT REPLACED WHAT: `IndexFactory::CreateScalarIndex` / `CreateVectorIndex`
// (IndexFactory.cpp) was a single `switch` over `DataType` x index-type-string
// that had to name every concrete class, i.e. the top level of index/ had a
// compile-time edge to every family. Now each family registers itself from its
// own .cpp and the top level knows nobody.
//
// STATIC-REGISTRATION HAZARD, stated once here so no family has to repeat it:
// self-registration lives in a namespace-scope object in each family's .cpp.
// That works while every family object file is linked into `milvus_core`
// (today's build: `add_library(milvus_index OBJECT ...)` + `$<TARGET_OBJECTS:>`
// into the shared object, so no archive-member stripping applies). If index/
// ever becomes a static archive, these TUs must be kept with
// `--whole-archive` / `/WHOLEARCHIVE`, or registration silently disappears and
// every `Lookup` returns null.

namespace milvus::index {

namespace {

template <typename Value>
class RegistryTable {
 public:
    void
    Put(const IndexFamily& family, Value value) {
        // TODO: insert under `mu_`, and AssertInfo the family is not already
        // registered — a duplicate family name is a build-configuration bug,
        // not a runtime condition.
    }

    Value
    Get(const IndexFamily& family) const {
        // TODO: lookup under `mu_`; return a default-constructed Value when
        // absent. NOT a throw: "no loader for this family" is answered by the
        // caller (segcore load), which has the context to report it.
        return Value{};
    }

    std::vector<IndexFamily>
    Keys() const {
        // TODO: snapshot of the key set, for tests and for the round-trip
        // matrix ("every registered family has both a loader and a builder").
        return {};
    }

 private:
    mutable std::mutex mu_;
    std::unordered_map<IndexFamily, Value> table_;
};

}  // namespace

LoaderRegistry&
LoaderRegistry::Instance() {
    static LoaderRegistry instance;
    return instance;
}

void
LoaderRegistry::Register(IndexFamily family, IndexLoaderPtr loader) {
    // TODO: delegate to the file-local RegistryTable<IndexLoaderPtr>.
}

IndexLoaderPtr
LoaderRegistry::Lookup(const IndexFamily& family) const {
    // TODO: delegate to the file-local RegistryTable<IndexLoaderPtr>.
    //
    // Loaders are STATELESS (§3 principle 1, "Loader: stateless, one per
    // family"), so a single shared instance per family is returned to every
    // caller; there is no per-load state to keep apart.
    return nullptr;
}

std::vector<IndexFamily>
LoaderRegistry::Families() const {
    return {};
}

template <typename T>
BuilderRegistry<T>&
BuilderRegistry<T>::Instance() {
    static BuilderRegistry<T> instance;
    return instance;
}

template <typename T>
void
BuilderRegistry<T>::Register(IndexFamily family, Factory factory) {
    // TODO: delegate to the file-local RegistryTable<Factory>.
}

template <typename T>
bool
BuilderRegistry<T>::Supports(const IndexFamily& family) const {
    return false;
}

template <typename T>
std::unique_ptr<IndexBuilder<T>>
BuilderRegistry<T>::Create(const IndexFamily& family,
                           const BuildParams& params) const {
    // TODO: look the factory up and invoke it with `params`.
    //
    // `params` is the whole build config; each family's factory picks the keys
    // it understands out of it (tokenizer json for text, min/max gram for
    // ngram, cardinality limit for auto, ...). This is §6.1's "each family's
    // builder is an IMPLEMENTATION of the one builder interface, not a new
    // interface": the per-family parameters are constructor arguments, not
    // extra methods.
    return nullptr;
}

// The value types a scalar builder can be instantiated on. Variable-length
// values are expressed as VIEW types (§6.1): `std::string_view` for VARCHAR /
// TEXT / JSON-as-text. `float` covers the dense-vector case that shares this
// same interface (§11.3), which is why there is no Scalar/Vector prefix on
// `IndexBuilder<T>`.
#define INSTANTIATE_BUILDER_REGISTRY(T) template class BuilderRegistry<T>;
INSTANTIATE_BUILDER_REGISTRY(bool)
INSTANTIATE_BUILDER_REGISTRY(int8_t)
INSTANTIATE_BUILDER_REGISTRY(int16_t)
INSTANTIATE_BUILDER_REGISTRY(int32_t)
INSTANTIATE_BUILDER_REGISTRY(int64_t)
INSTANTIATE_BUILDER_REGISTRY(float)
INSTANTIATE_BUILDER_REGISTRY(double)
INSTANTIATE_BUILDER_REGISTRY(std::string_view)
#undef INSTANTIATE_BUILDER_REGISTRY

}  // namespace milvus::index
