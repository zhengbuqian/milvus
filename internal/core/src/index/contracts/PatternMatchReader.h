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

#include <string_view>

#include "common/Types.h"

// The LIKE family. Pure mixin, does NOT derive from `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5.2.
//
// Providers: tantivy inverted, marisa (prefix), and FMIndex — for FMIndex THIS
// IS ITS ONLY PREDICATE INTERFACE. Together with the unconditional `NullReader`
// (`FMIndex.cpp:392` is a real implementation) that is the whole of FMIndex's
// query surface; today it is a `ScalarIndex<std::string>` carrying 20 unrelated
// methods.

namespace milvus::index {

// NATIVE ENUM, deliberately not `milvus::OpType` (== `proto::plan::OpType`,
// `common/Types.h:106`). Same reasoning as `CompareOp` in
// ScalarPredicateReader.h: README §5 rule 2 forbids pb on a contract signature,
// §5.6 prescribes a native enum plus a plan/exec-side mapping.
//
// !! `RegexMatch` IS IN THIS SET EVEN THOUGH §5.2 DOES NOT LIST IT.
// §5.2 enumerates {Match, PrefixMatch, PostfixMatch, InnerMatch}, but the
// current code routes `proto::plan::OpType::RegexMatch` into the very methods
// this interface replaces: `InvertedIndexTantivy::PatternMatch`
// (`InvertedIndexTantivy.h:261,301`, via `PartialRegexMatcher`),
// `NgramInvertedIndex::ExecutePhase1` (`NgramInvertedIndex.cpp:818,937,1084,1143`)
// and `StringIndexMarisa`'s accepted-op check (`StringIndexMarisa.cpp:650-654`).
// Dropping it would silently delete a live path, so the enum follows the code
// and the doc's list is treated as incomplete rather than as a restriction.
// (`FMIndex` declines both Match and RegexMatch — `FMIndexTest.cpp:143,168` —
// but that is a per-index routing decision, not a missing operator.)
enum class PatternOp {
    Match,          // LIKE
    PrefixMatch,    // startsWith
    PostfixMatch,   // endsWith
    InnerMatch,     // substring, "%value%"
    RegexMatch,     // regex substring match, `=~ "pattern"`
};

class PatternMatchReader {
 public:
    virtual ~PatternMatchReader() = default;

    // `pattern` is the raw SQL LIKE text, NOT a regex; implementations convert
    // internally if they need to. Output is a `TargetBitmap`, 1 = hit,
    // size == Count() (§5).
    virtual TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const = 0;
};

}  // namespace milvus::index
