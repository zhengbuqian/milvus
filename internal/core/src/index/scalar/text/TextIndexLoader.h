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

// The LOADER of the text family.
//
// See 01-scalar-index.md §6.2 (read-only direction, IO injected) and §4.1
// (`DeriveCaps` must be answerable from load-time metadata, without pinning).
//
// It replaces `TextMatchIndex`'s fourth constructor
// (`TextMatchIndex.h:50` / `TextMatchIndex.cpp:106-114`, reached from
// `segcore/storagev1translator/TextMatchIndexTranslator.cpp:106`) together with
// `TextMatchIndex::Load` (`TextMatchIndex.cpp:181-253`).
//
// STATELESS (§3 principle 1): one instance serves every load of this family, so
// nothing about a particular index may be stored on it.

namespace milvus::index {

class TextIndexLoader final : public IndexLoader {
 public:
    TextIndexLoader() = default;

    ~TextIndexLoader() override = default;

    std::string
    Family() const override;

    // Pure data, from metadata only. For text this is a constant
    // (`{.text_match = true}`) — but it still goes through the loader because
    // FAMILY KNOWLEDGE IS THE LOADER'S, and the caller (segcore's inventory)
    // must be able to answer `Capability(field_id)` without touching a cold
    // cell (§4.1, §4.3 step 1, §10 rule 3b).
    ReaderCaps
    DeriveCaps(const Config& index_meta) const override;

    // `OpenIndex` rather than `Open`: C++ covariant returns do not apply to
    // `shared_ptr`, so `IndexLoader` declares this and `final`-forwards `Open`
    // (see index/contracts/IndexLoader.h and its README's deviation table).
    std::shared_ptr<IndexReaderBase>
    OpenIndex(storage::FileSource& source,
              const storage::LoadOptions& opts) override;
};

}  // namespace milvus::index
