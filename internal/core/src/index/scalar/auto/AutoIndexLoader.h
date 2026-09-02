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

// The LOADER of the `auto` family — a THIN REDIRECT, and ideally a dead one.
//
// §6.3: the builder records the family it actually chose, so a freshly written
// artifact carries `families::kFamilyMetaKey == "bitmap"` or `"inverted"` and
// segcore's inventory reaches that family's loader without ever passing through
// here. This class exists for artifacts whose metadata still says `"auto"`: it
// reads the recorded concrete family and forwards.
//
// IT HAS NO READER AND ADDS NO BEHAVIOUR. `Open` returns the chosen family's
// reader unchanged. That is the difference from `HybridScalarIndex`, which
// stayed in the call path forever, forwarding every query
// (`HybridScalarIndex.h:97-165`).

namespace milvus::index {

class AutoIndexLoader final : public IndexLoader {
 public:
    AutoIndexLoader() = default;

    ~AutoIndexLoader() override = default;

    std::string
    Family() const override;

    // Delegates to the recorded family's `DeriveCaps`. Note that the caps of an
    // `auto` index are NOT knowable without that recorded family — bitmap and
    // inverted differ on `value_lookup`. This is the concrete reason §6.3's
    // "record the choice in the artifact metadata" is load-bearing rather than
    // cosmetic: §4.1 requires caps to be derivable from metadata alone, with no
    // pin, and an unrecorded choice would make that impossible.
    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
