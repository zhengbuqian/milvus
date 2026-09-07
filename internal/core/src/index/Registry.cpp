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

#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/Array.h"
#include "common/EasyAssert.h"

// Implementation of the contract-layer registries. Each family registers from
// its own implementation translation unit, so this file has no concrete-family
// implementation dependency. The explicit-instantiation list may name opaque
// family-local input shapes; it neither includes nor calls their family.
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

struct JsonProjectedString;

namespace {

template <typename Value>
class RegistryTable {
 public:
    void
    Put(const IndexFamily& family, Value value) {
        std::lock_guard lock(mu_);
        auto [_, inserted] = table_.emplace(family, std::move(value));
        AssertInfo(inserted, "duplicate index family registration: {}", family);
    }

    Value
    Get(const IndexFamily& family) const {
        std::lock_guard lock(mu_);
        auto it = table_.find(family);
        return it == table_.end() ? Value{} : it->second;
    }

    std::vector<IndexFamily>
    Keys() const {
        std::lock_guard lock(mu_);
        std::vector<IndexFamily> keys;
        keys.reserve(table_.size());
        for (const auto& [family, _] : table_) {
            keys.push_back(family);
        }
        std::sort(keys.begin(), keys.end());
        return keys;
    }

 private:
    mutable std::mutex mu_;
    std::unordered_map<IndexFamily, Value> table_;
};

}  // namespace

RegistryTable<IndexLoaderPtr>&
LoaderTable() {
    static RegistryTable<IndexLoaderPtr> table;
    return table;
}

template <typename T>
RegistryTable<typename BuilderRegistry<T>::Factory>&
BuilderTable() {
    static RegistryTable<typename BuilderRegistry<T>::Factory> table;
    return table;
}

LoaderRegistry&
LoaderRegistry::Instance() {
    static LoaderRegistry instance;
    return instance;
}

void
LoaderRegistry::Register(IndexFamily family, IndexLoaderPtr loader) {
    AssertInfo(loader != nullptr, "cannot register null loader for {}", family);
    LoaderTable().Put(family, std::move(loader));
}

IndexLoaderPtr
LoaderRegistry::Lookup(const IndexFamily& family) const {
    return LoaderTable().Get(family);
}

std::vector<IndexFamily>
LoaderRegistry::Families() const {
    return LoaderTable().Keys();
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
    AssertInfo(static_cast<bool>(factory),
               "cannot register null builder factory for {}",
               family);
    BuilderTable<T>().Put(family, std::move(factory));
}

template <typename T>
bool
BuilderRegistry<T>::Supports(const IndexFamily& family) const {
    return static_cast<bool>(BuilderTable<T>().Get(family));
}

template <typename T>
std::unique_ptr<IndexBuilder<T>>
BuilderRegistry<T>::Create(const IndexFamily& family,
                           const BuildParams& params) const {
    auto factory = BuilderTable<T>().Get(family);
    return factory ? factory(params) : nullptr;
}

template <typename T>
std::vector<IndexFamily>
BuilderRegistry<T>::Families() const {
    return BuilderTable<T>().Keys();
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
INSTANTIATE_BUILDER_REGISTRY(ArrayView)
INSTANTIATE_BUILDER_REGISTRY(JsonProjectedString)
INSTANTIATE_BUILDER_REGISTRY(float16)
INSTANTIATE_BUILDER_REGISTRY(bfloat16)
INSTANTIATE_BUILDER_REGISTRY(bin1)
INSTANTIATE_BUILDER_REGISTRY(sparse_u32_f32)
#undef INSTANTIATE_BUILDER_REGISTRY

}  // namespace milvus::index
