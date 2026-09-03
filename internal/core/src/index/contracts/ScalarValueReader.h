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
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "common/Types.h"

// The value interface: reverse lookup. Pure mixin, does NOT derive from
// `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5.5.
//
// Corresponds to today's `Reverse_Lookup` + `SupportFastReverseLookup` +
// `HasRawData`. THE DECISION "reverse lookup is too expensive, go back to the
// raw column" IS NOT MADE HERE: `ReaderCaps::cheap_value_lookup` states the
// cost, the consumer (reduce's Materializer, exec) chooses. Same principle as
// §7's growing watermark: the index reports, the consumer decides.

namespace milvus::index {

// The owning counterpart of T, as defined in §5.5:
//   owned_t<std::string_view> == std::string, owned_t<T> == T otherwise.
//
// NOTE: this trait does not exist in the repository today; it is introduced here
// with exactly the semantics §5.5 states, no more.
template <typename T>
struct OwnedType {
    using type = T;
};

template <>
struct OwnedType<std::string_view> {
    using type = std::string;
};

template <typename T>
using owned_t = typename OwnedType<T>::type;

template <typename T>
class ScalarValueReader {
 public:
    virtual ~ScalarValueReader() = default;

    // THE RETURN VALUE OWNS ITS BYTES — `Lookup` cannot return a view, and this
    // is why the value interface does not follow §5.1's `string_view` decision
    // (that decision covers the INPUT side only).
    //
    // `StringIndexMarisa::Reverse_Lookup` (`StringIndexMarisa.cpp:800-811`)
    // returns `std::string(agent.key().ptr(), agent.key().length())`: the bytes
    // live in a `marisa::Agent` local to the function. A trie stores compressed,
    // so THE ORIGINAL VALUE IS RECONSTRUCTED ON THE SPOT — there simply is no
    // resident buffer inside the index to point at, and a returned view would
    // always dangle. Every trie / compressed family has this shape; it is not a
    // marisa implementation wart.
    virtual std::optional<owned_t<T>>
    Lookup(int64_t offset) const = 0;

    // Batch reverse lookup. Two reasons this is a callback rather than a
    // returned container: (1) the implementation may cluster the offsets by its
    // internal layout, and (2) unlike `Lookup`, a view IS fine here — the
    // implementation can keep its agent alive until the callback returns, so
    // `const T*` only has to be valid for the duration of the call. The output
    // is meant to plug into columnar-format's `TakeResult` convention.
    virtual void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const = 0;
};

}  // namespace milvus::index
