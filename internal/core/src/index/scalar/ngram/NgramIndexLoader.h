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

#include <memory>
#include <string>

#include "index/contracts/IndexLoader.h"

// The LOADER of the ngram family (§6.2). The historical wire carries neither
// gram bounds nor JSON path/type, so every open and metadata-only capability
// derivation validates the normalized runtime parameters that supply them.

namespace milvus::index {

class NgramIndexLoader final : public IndexLoader {
 public:
    NgramIndexLoader() = default;

    ~NgramIndexLoader() override = default;

    std::string
    Family() const override;

    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::unique_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;

    RehydratedIndex
    OpenForRewrite(storage::FileSource& source,
                   const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
