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

// The LOADER of the inverted family (§6.2).

namespace milvus::index {

class InvertedIndexLoader final : public IndexLoader {
 public:
    InvertedIndexLoader() = default;

    ~InvertedIndexLoader() override = default;

    std::string
    Family() const override;

    // Derived from LOAD-TIME METADATA ONLY — the persisted value type decides
    // `pattern_match` (§4.1's worked example is literally "inverted on a
    // VARCHAR => predicate + pattern_match"), and the persisted nested bit
    // decides `nested`. No index object is opened, so a cold cell under tiered
    // storage stays cold while exec picks its path (§4.3 step 1, §10 rule 3b).
    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
