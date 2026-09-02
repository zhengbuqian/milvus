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

#include "index/vector/VectorFamilies.h"

#include <memory>

#include "index/contracts/Registry.h"
#include "index/vector/VectorDiskBuilder.h"
#include "index/vector/VectorDiskLoader.h"
#include "index/vector/VectorMemBuilder.h"
#include "index/vector/VectorMemLoader.h"

// SKELETON — this TU exists to show the USAGE SHAPE of the registries, which is
// what §11.2 rule 4 puts in place of the God factory. Self-registering, same
// convention (and same static-registration hazard) as the scalar families; see
// the long note in `index/Registry.cpp`.
//
// WHAT IT REPLACES: `IndexFactory::CreateVectorIndex` and its mmap sibling — a
// `switch` over `DataType` that news `VectorMemIndex<T>` / `VectorDiskAnnIndex<T>`
// — plus the `VecIndexLoadResource` overloads, every one of which took a
// `storage::FileManagerContext&` and returned an `IndexBasePtr`
// (`index/IndexFactory.h`). After registration, adding or removing a vector
// family touches this directory only, and index/ root knows nobody.

namespace milvus::index {

namespace {

template <typename T>
void
RegisterMemFamily(const IndexFamily& family) {
    // Loaders are stateless (§3 principle 1): one shared instance per family.
    LoaderRegistry::Instance().Register(family,
                                        std::make_shared<VectorMemLoader>());

    // Builders are one-shot, so the registry stores a FACTORY. `params` is this
    // family's own knob bag, parsed by the factory into typed constructor
    // arguments — NOT a revived `CreateIndexInfo`: nothing in it is read by any
    // family other than this one (contracts/Registry.h).
    BuilderRegistry<T>::Instance().Register(
        family, [](const BuildParams& params) {
            // TODO: parse index type, metric, dim, knowhere version, build
            // config and VEC_OPT_FIELDS out of `params`, then construct.
            return std::unique_ptr<IndexBuilder<T>>{};
        });
}

template <typename T>
void
RegisterDiskFamily(const IndexFamily& family) {
    LoaderRegistry::Instance().Register(family,
                                        std::make_shared<VectorDiskLoader>());
    BuilderRegistry<T>::Instance().Register(
        family, [](const BuildParams& params) {
            return std::unique_ptr<IndexBuilder<T>>{};
        });
}

struct VectorFamilyRegistrar {
    VectorFamilyRegistrar() {
        // TODO: walk the knowhere in-memory index types (HNSW, IVF_FLAT,
        // IVF_PQ, IVF_SQ8, SCANN, FLAT, SPARSE_INVERTED_INDEX, SPARSE_WAND, the
        // emb-list variants, ...) x element type and call `RegisterMemFamily<T>`;
        // then `RegisterDiskFamily<T>(families::kDiskAnn)`.
        //
        // NOTE — `BuilderRegistry<T>` IS PER VALUE TYPE, so a family supporting
        // several element types registers into several registries. That is
        // §3 principle 4 made explicit: the builder face is typed (templates on
        // the data path), the loader face is type-erased (management face), and
        // the element type is therefore recovered from artifact metadata inside
        // `VectorMemLoader::OpenIndex`.
    }
};

[[maybe_unused]] const VectorFamilyRegistrar registrar;

}  // namespace

}  // namespace milvus::index
