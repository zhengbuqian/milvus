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

#include "common/Types.h"

// Null predicates. A cross-family interface — pure mixin, does NOT derive from
// `IndexReaderBase` (§4).
//
// See core_refactor/01-scalar-index.md §5 ("cross-family interface:
// NullReader").
//
// WHY THIS MUST BE SPLIT OUT OF `ScalarPredicateReader<T>`.
//
// `RTreeIndex`'s In / NotIn / Range are empty shells — all `ThrowInfo(
// NotImplemented)` (`RTreeIndex.cpp:462,534,542`). Once those are deleted it
// can no longer implement `ScalarPredicateReader<T>`: doing so with T =
// std::string would drag point predicates over WKB right back in. But
// `geo_field IS NULL` is a LIVE path — `PhyNullExpr` routes through
// `ProcessChunksForValid` -> `ProcessIndexChunksForValid` (`Expr.h:2457`) into
// the index, and `RTreeIndex::IsNull` / `IsNotNull` (`RTreeIndex.cpp:469,491`)
// are real implementations. Deleting them along with the geo operators would
// break a live path; keeping them on the typed predicate interface would keep
// RTree tied to a predicate family it does not have.
//
// The signal was already visible in the current code: `IsNull` / `IsNotNull`
// are THE ONLY TWO methods of `ScalarIndex<T>` (`ScalarIndex.h:132,135`) that
// do not mention `T`. A method that does not take `T` living on an interface
// templated on `T` is itself an interface-split error.
//
// ALL SEVEN scalar families implement these for real, zero throws
// (`BitmapIndex.cpp:816`, `FMIndex.cpp:392`, `RTreeIndex.cpp:469`,
// `InvertedIndexTantivy.cpp:349`, `ScalarIndexSort.cpp:508`,
// `StringIndexSort.cpp:462`, `StringIndexMarisa.cpp:406`). It is therefore an
// UNCONDITIONALLY AVAILABLE interface and deliberately gets NO `ReaderCaps` bit
// (§5), and it is not repeated per row in the §8 mapping table.

namespace milvus::index {

class NullReader {
 public:
    virtual ~NullReader() = default;

    // 1 = hit. Bitmap size == IndexReaderBase::Count(), in that reader's own
    // coordinate system (§5).
    virtual TargetBitmap
    IsNull() const = 0;

    virtual TargetBitmap
    IsNotNull() const = 0;
};

}  // namespace milvus::index
