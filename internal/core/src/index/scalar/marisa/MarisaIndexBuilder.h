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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "index/contracts/IndexBuilder.h"
#include "storage/artifact/Artifact.h"

// The BUILDER of the marisa family.
//
// §6.1.1 cites this family as the canonical **form B (fully resident)** case,
// and with the sharpest evidence: "marisa collects the whole keyset, calls
// `trie_.build()`, and then WALKS THE DATA A SECOND TIME to fill `str_ids_`"
// (`StringIndexMarisa.cpp:173-205`).
//
// That second walk is over the builder's OWN buffered copy, not over the input
// again, so `needs_second_pass` stays false — that flag means "the caller must
// rewind the source" (`ScanCursor::Seek(0)`, §6.1.2), which is a different and
// more expensive thing. Getting these two confused would make every marisa
// build re-read a cold column.

namespace milvus::index {

class MarisaIndexBuilder final : public IndexBuilder<std::string_view> {
 public:
    explicit MarisaIndexBuilder(DataType value_type);

    ~MarisaIndexBuilder() override;

    BuilderInputSpec
    InputSpec() const override;

    void
    Add(size_t n, const std::string_view* values, const bool* valid) override;

    storage::ArtifactPtr
        Seal() &&
        override;

 private:
    DataType value_type_;
    bool sealed_{false};

    // Keep an owning, row-aligned copy. In particular, never retain a view into
    // an input batch until Seal(): callers are allowed to recycle that batch as
    // soon as Add() returns.
    std::vector<std::string> buffered_;
    std::vector<uint8_t> valid_;
};

}  // namespace milvus::index
