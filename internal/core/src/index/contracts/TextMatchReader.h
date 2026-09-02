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

#include <cstdint>
#include <string_view>

#include "common/Types.h"

// Tokenized full-text matching. Pure mixin, does NOT derive from
// `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5.3.
//
// The implementation COMPOSES a tantivy reader snapshot. IT DOES NOT INHERIT the
// inverted index: today `TextMatchIndex : InvertedIndexTantivy<std::string>`,
// which drags along In / Range and the rest of a surface that must never be
// called on it. §3 principle 2 and §10 rule 3 forbid inheritance between
// implementation classes; only contract interfaces may be inherited.
//
// Its four constructors (growing in-memory writer with commit interval and
// background merge / build-in-place at sealed load / the build service with a
// `FileManagerContext` / open an already-built index, `TextMatchIndex.h:31-52`)
// split across four homes: `GrowingTextIndex` (Appender face), the text
// `IndexBuilder` (Builder face, both for the build service and for
// build-in-place — §7 point 3), and the text `IndexLoader` (Loader face). This
// face is only the read side.

namespace milvus::index {

class TextMatchReader {
 public:
    virtual ~TextMatchReader() = default;

    // Output is a `TargetBitmap`, 1 = hit, size == Count() (§5).
    virtual TargetBitmap
    MatchQuery(std::string_view query, uint32_t min_should_match) const = 0;

    virtual TargetBitmap
    PhraseMatchQuery(std::string_view query, uint32_t slop) const = 0;

    virtual TargetBitmap
    FuzzyMatchQuery(std::string_view query,
                    uint32_t max_edit_distance) const = 0;
};

}  // namespace milvus::index
