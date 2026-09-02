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

// The LOADER of the FM-index family (§6.2).

namespace milvus::index {

class FmIndexLoader final : public IndexLoader {
 public:
    // `cost_ratio` is the injected query-time policy value that used to be read
    // from `segcore::SegcoreConfig` inside the index header
    // (`FMIndex.h:226-228`). The loader is the natural place to hold it: it is
    // the one family object that segcore configures, and it hands the value to
    // every reader it opens. See FmIndexReader for the live-update caveat.
    explicit FmIndexLoader(double cost_ratio = 0.001);

    ~FmIndexLoader() override = default;

    std::string
    Family() const override;

    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;

 private:
    double cost_ratio_;
};

}  // namespace milvus::index
