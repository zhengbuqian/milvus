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

#include "common/Types.h"
#include "index/contracts/VectorFaces.h"
#include "knowhere/config.h"

// Range-search parameter validation for knowhere. VECTOR-FAMILY ONLY.
//
// MOVED OUT OF `index/Utils.h` (declaration at `:229`, definition at
// `index/Utils.cpp:513`) — see core_refactor/01-scalar-index.md §12.1(a) and
// §10 rule 6.
//
// WHY THE MOVE MATTERS MORE THAN THE FUNCTION DOES. §10 rule 6 says knowhere
// headers may not appear in the shared root or in any scalar family, and
// §12.1(a) traced today's violation to a single INCLUDE CHAIN rather than to any
// scalar index:
//
//   common/QueryInfo.h:26   includes knowhere/config.h
//   index/Utils.h:34        includes common/QueryInfo.h
//   ~11 scalar .cpp files   include index/Utils.h
//     (BitmapIndex, ScalarIndexSort, StringIndexMarisa, StringIndexSort,
//      InvertedIndexTantivy, FMIndex, RTreeIndex, NgramInvertedIndex,
//      HybridScalarIndex, ScalarIndex, bson_inverted, ...)
//
// So every scalar index compiles knowhere today. `index/Utils.h` needs
// `common/QueryInfo.h` FOR THIS ONE FUNCTION AND NOTHING ELSE — it is the only
// declaration in that header mentioning `SearchInfo`. Moving it here breaks the
// lower half of the chain: `index/Utils.h` stops including `QueryInfo.h`, and
// the scalar families stop pulling knowhere in through it. (The upper half —
// `common/QueryInfo.h` including `knowhere/config.h` — is the exec side's to
// break; §12.1(a) says the narrow parameter type declared in the vector family
// is what lets `QueryInfo.h` drop knowhere too.)
//
// `index/Utils.h` itself is the scalar side's file; this change only takes the
// function over.
//
// SIGNATURE CHANGE: `const SearchInfo&` -> `const VectorSearchParams&`. Safe and
// exact — the body reads `search_info.search_params_` and nothing else
// (`index/Utils.cpp:517,530,538`), which is one of the four fields §12.1(a)
// found `index/` ever touches. `topk` and `metric_type` stay separate arguments
// because the one production call site overrides both: it passes the ITERATOR
// BATCH SIZE as topk, not `search_info.topk_`
// (`query/CachedSearchIterator.cpp:66-67`).

namespace milvus::index {

// Returns false when the query has no RADIUS (i.e. it is not a range search) and
// leaves `search_config` untouched; otherwise fills RADIUS / RANGE_SEARCH_K and,
// when present, RANGE_FILTER (validated by `CheckRangeSearchParam`,
// `common/RangeSearchHelper.h` — L0, stays where it is) and
// RETAIN_ITERATOR_ORDER.
bool
CheckAndUpdateKnowhereRangeSearchParam(const VectorSearchParams& params,
                                       int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config);

}  // namespace milvus::index
