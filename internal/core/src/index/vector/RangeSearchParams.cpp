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

#include "index/vector/RangeSearchParams.h"

namespace milvus::index {

bool
CheckAndUpdateKnowhereRangeSearchParam(const VectorSearchParams& params,
                                       int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config) {
    // TODO: move existing logic here VERBATIM (see index/Utils.cpp:512-545 in
    // the pre-W1 tree, master e255009e01) — read RADIUS out of
    // `params.search_params_`, early-return false when absent, then set RADIUS,
    // RANGE_SEARCH_K = topk, and optionally RANGE_FILTER (validated by
    // `CheckRangeSearchParam(radius, range_filter, metric_type)`) and
    // RETAIN_ITERATOR_ORDER from PAGE_RETAIN_ORDER.
    //
    // The only edit is `search_info.search_params_` -> `params.search_params_`.
    return false;
}

}  // namespace milvus::index
